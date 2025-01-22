package capturer

import (
	"fmt"
	"sync"
	"time"

	"github.com/strahe/curio-sentinel/yblogrepl"
	"github.com/yugabyte/pgx/v5/pgtype"
)

type Processor struct {
	relations map[uint32]*yblogrepl.RelationMessage
	typeMap   *pgtype.Map

	logger Logger
	tx     *TransactionTracker
	events chan<- *Event
	mu     sync.RWMutex
}

func NewProcessor(events chan<- *Event, logger Logger) *Processor {
	return &Processor{
		relations: map[uint32]*yblogrepl.RelationMessage{},
		typeMap:   pgtype.NewMap(),
		tx:        &TransactionTracker{},
		events:    events,
		logger:    logger,
	}
}

func (p *Processor) Process(walData []byte) error {
	logicalMsg, err := yblogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("parse logical replication message: %w", err)
	}
	p.logger.Debugf("Process logical replication message: %s", logicalMsg.Type().String())
	switch logicalMsg := logicalMsg.(type) {
	case *yblogrepl.RelationMessage:
		p.relations[logicalMsg.RelationID] = logicalMsg
	case *yblogrepl.BeginMessage:
		return p.handleBegin(logicalMsg)
	case *yblogrepl.CommitMessage:
		return p.handleCommit(logicalMsg)
	case *yblogrepl.InsertMessage:
		return p.handleInsert(logicalMsg)
	case *yblogrepl.UpdateMessage:
		return p.handleUpdate(logicalMsg)
	case *yblogrepl.DeleteMessage:
		return p.handleDelete(logicalMsg)
	case *yblogrepl.TruncateMessage:
	case *yblogrepl.TypeMessage:
	case *yblogrepl.OriginMessage:
	case *yblogrepl.LogicalDecodingMessage:
	default:
		return fmt.Errorf("unknown message type in pgoutput stream: %T", logicalMsg)
	}
	return nil
}

func (p *Processor) handleRelation(msg *yblogrepl.RelationMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.logger.Debugf("Relation message: %s.%s (%d)", msg.Namespace, msg.RelationName, msg.RelationID)

	p.relations[msg.RelationID] = msg
	return nil
}

func (p *Processor) handleBegin(msg *yblogrepl.BeginMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if msg.CommitTime.Before(time.Now().Add(-30 * time.Second)) {
		p.logger.Warnf("Begin transaction is too old: %d (%s)", msg.Xid, time.Since(msg.CommitTime))
	} else {
		p.logger.Debugf("Begin transaction: %d (%s)", msg.Xid, time.Since(msg.CommitTime))
	}
	p.tx.Begin(msg.Xid, msg.FinalLSN, msg.CommitTime)
	return nil
}

func (p *Processor) handleCommit(msg *yblogrepl.CommitMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	events := p.tx.End(msg.CommitLSN, msg.TransactionEndLSN, msg.CommitTime)

	p.logger.Debugf("Commit transaction: %s", msg.CommitLSN)
	for _, event := range events {
		p.events <- event
	}
	return nil
}

func (p *Processor) handleUpdate(msg *yblogrepl.UpdateMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	rel, ok := p.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", msg.RelationID)
	}
	p.logger.Debugf("Update %s.%s", rel.Namespace, rel.RelationName)

	oldValues := map[string]any{}
	if msg.OldTuple != nil {
		for idx, col := range msg.OldTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				oldValues[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(p.typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return fmt.Errorf("error decoding column data: %s", err)
				}
				oldValues[colName] = val
			}
		}
	}

	newValues := map[string]any{}
	if msg.NewTuple != nil {
		for idx, col := range msg.NewTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				newValues[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(p.typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return fmt.Errorf("error decoding column data: %s", err)
				}
				newValues[colName] = val
			}
		}
	}

	p.tx.AddEvent(&Event{
		Type:   Update,
		Schema: rel.Namespace,
		Table:  rel.RelationName,
		Data: map[string]any{
			"before": oldValues,
			"after":  newValues,
		},
	})
	return nil
}

func (p *Processor) handleInsert(msg *yblogrepl.InsertMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	rel, ok := p.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", msg.RelationID)
	}
	p.logger.Debugf("Insert %s.%s", rel.Namespace, rel.RelationName)

	values := map[string]any{}
	for idx, col := range msg.Tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(p.typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return fmt.Errorf("error decoding column data: %s", err)
			}
			values[colName] = val
		}
	}

	p.tx.AddEvent(&Event{
		Type:   Insert,
		Schema: rel.Namespace,
		Table:  rel.RelationName,
		Data:   values,
	})
	return nil
}

func (p *Processor) handleDelete(msg *yblogrepl.DeleteMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	rel, ok := p.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", msg.RelationID)
	}
	p.logger.Debugf("Delete %s.%s", rel.Namespace, rel.RelationName)

	values := map[string]any{}
	for idx, col := range msg.OldTuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(p.typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return fmt.Errorf("error decoding column data: %s", err)
			}
			values[colName] = val
		}
	}

	p.tx.AddEvent(&Event{
		Type:   Delete,
		Schema: rel.Namespace,
		Table:  rel.RelationName,
		Data:   values,
	})
	return nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
