package capturer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"github.com/strahe/curio-sentinel/yblogrepl"
	"github.com/yugabyte/pgx/v5/pgconn"
	"github.com/yugabyte/pgx/v5/pgtype"
)

type RelationWithPK struct {
	*yblogrepl.RelationMessage
	PKColumns []string // primary key columns
}

type Processor struct {
	relations map[uint32]*RelationWithPK
	typeMap   *pgtype.Map

	logger Logger
	tx     *TransactionTracker
	events chan<- *Event
	conn   *pgconn.PgConn
	mu     sync.RWMutex
}

func NewProcessor(events chan<- *Event, logger Logger, conn *pgconn.PgConn) *Processor {
	return &Processor{
		relations: map[uint32]*RelationWithPK{},
		typeMap:   pgtype.NewMap(),
		tx:        &TransactionTracker{},
		events:    events,
		logger:    logger,
		conn:      conn,
	}
}

func (p *Processor) Process(ctx context.Context, walData []byte) error {
	logicalMsg, err := yblogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("parse logical replication message: %w", err)
	}

	p.logger.Debugf("Process logical replication message: %s", logicalMsg.Type().String())
	switch logicalMsg := logicalMsg.(type) {
	case *yblogrepl.RelationMessage:
		return p.handleRelation(ctx, logicalMsg)
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

func (p *Processor) handleRelation(ctx context.Context, msg *yblogrepl.RelationMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.logger.Debugf("Relation message: %s.%s (%d)", msg.Namespace, msg.RelationName, msg.RelationID)

	rp := RelationWithPK{
		RelationMessage: msg,
	}

	pks, err := p.findPkColumns(ctx, &rp)
	if err != nil {
		return fmt.Errorf("find primary key columns: %w", err)
	}
	rp.PKColumns = pks

	p.logger.Debugf("%s.%s Primary key columns: %v", msg.Namespace, msg.RelationName, rp.PKColumns)
	p.relations[msg.RelationID] = &rp
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

	evt := &Event{
		Type:       Update,
		Schema:     rel.Namespace,
		Table:      rel.RelationName,
		Before:     map[string]any{},
		After:      map[string]any{},
		PrimaryKey: map[string]any{},
	}

	if msg.OldTuple != nil {
		for idx, col := range msg.OldTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				evt.Before[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(p.typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return fmt.Errorf("error decoding column data: %s", err)
				}
				evt.Before[colName] = val
			}
			if lo.Contains(rel.PKColumns, colName) {
				evt.PrimaryKey[colName] = evt.Before[colName]
			}
		}
	}

	if msg.NewTuple != nil {
		for idx, col := range msg.NewTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				evt.After[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(p.typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return fmt.Errorf("error decoding column data: %s", err)
				}
				evt.After[colName] = val
			}
		}
	}

	p.tx.AddEvent(evt)
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

	evt := &Event{
		Type:       Insert,
		Schema:     rel.Namespace,
		Table:      rel.RelationName,
		After:      map[string]any{},
		PrimaryKey: map[string]any{},
	}

	for idx, col := range msg.Tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			evt.After[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(p.typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return fmt.Errorf("error decoding column data: %s", err)
			}
			evt.After[colName] = val
		}
		if lo.Contains(rel.PKColumns, colName) {
			evt.PrimaryKey[colName] = evt.After[colName]
		}
	}

	p.tx.AddEvent(evt)
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

	evt := &Event{
		Type:       Delete,
		Schema:     rel.Namespace,
		Table:      rel.RelationName,
		Before:     map[string]any{},
		PrimaryKey: map[string]any{},
	}
	for idx, col := range msg.OldTuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			evt.Before[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(p.typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return fmt.Errorf("error decoding column data: %s", err)
			}
			evt.Before[colName] = val
		}
		if lo.Contains(rel.PKColumns, colName) {
			evt.PrimaryKey[colName] = evt.Before[colName]
		}
	}

	p.tx.AddEvent(evt)
	return nil
}

func (p *Processor) findPkColumns(ctx context.Context, rel *RelationWithPK) ([]string, error) {
	// Return already fetched PK columns if available
	if len(rel.PKColumns) > 0 {
		return rel.PKColumns, nil
	}

	// If no context provided, create one with timeout
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}

	// SQL query to find primary key columns in proper order
	query := `
SELECT
    a.attname AS column_name
FROM
    pg_index i
JOIN
    pg_attribute a ON a.attrelid = i.indrelid
    AND a.attnum = ANY(i.indkey)
WHERE
    i.indrelid = ($1 || '.' || $2)::regclass
    AND i.indisprimary;
    `

	paramValues := [][]byte{
		[]byte(rel.Namespace),
		[]byte(rel.RelationName),
	}

	resultReader := p.conn.ExecParams(ctx, query, paramValues, nil, nil, nil)
	defer resultReader.Close()

	var pkColumns []string

	result := resultReader.Read()

	if result.Err != nil {
		return nil, fmt.Errorf("failed to read result: %w", result.Err)
	}

	for _, row := range result.Rows {
		if len(row) != 1 {
			return nil, fmt.Errorf("expected 1 column in result, got %d", len(row))
		}
		fmt.Println("1111111111111111111111", string(row[0]))
		pkColumns = append(pkColumns, string(row[0]))
	}

	fmt.Println("pkColumns", pkColumns)

	if len(pkColumns) > 0 {
		rel.PKColumns = pkColumns
	}

	return pkColumns, nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
