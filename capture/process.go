package capture

import (
	"fmt"

	"github.com/strahe/curio-sentinel/pkg/log"
	"github.com/strahe/curio-sentinel/yblogrepl"
	"github.com/yugabyte/pgx/v5/pgtype"
)

func processV1(walData []byte, relations map[uint32]*yblogrepl.RelationMessage, typeMap *pgtype.Map) {
	logicalMsg, err := yblogrepl.Parse(walData)
	if err != nil {
		log.Error().Err(err).Msg("Parse logical replication message")
		return
	}
	log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
	switch logicalMsg := logicalMsg.(type) {
	case *yblogrepl.RelationMessage:
		relations[logicalMsg.RelationID] = logicalMsg

	case *yblogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	case *yblogrepl.CommitMessage:

	case *yblogrepl.InsertMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			log.Info().Msg("unknown relation ID")
			return
		}
		values := map[string]any{}
		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Info().Msg("error decoding column data")
					return
				}
				values[colName] = val
			}
		}
		log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)
	case *yblogrepl.UpdateMessage:
		// ...
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			log.Info().Msg("unknown relation ID")
			return
		}
		oldValues := map[string]any{}
		if logicalMsg.OldTuple != nil {
			for idx, col := range logicalMsg.OldTuple.Columns {
				colName := rel.Columns[idx].Name
				switch col.DataType {
				case 'n': // null
					oldValues[colName] = nil
				case 'u': // unchanged toast
					// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
				case 't': //text
					val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
					if err != nil {
						log.Info().Msg("error decoding column data")
						return
					}
					oldValues[colName] = val
				}
			}
		}

		newValues := map[string]any{}
		if logicalMsg.NewTuple != nil {
			for idx, col := range logicalMsg.NewTuple.Columns {
				colName := rel.Columns[idx].Name
				switch col.DataType {
				case 'n': // null
					newValues[colName] = nil
				case 'u': // unchanged toast
					// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
				case 't': //text
					val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
					if err != nil {
						log.Info().Msg("error decoding column data")
						return
					}
					newValues[colName] = val
				}
			}
		}
		log.Printf("UPDATE %s.%s: %v -> %v", rel.Namespace, rel.RelationName, oldValues, newValues)
	case *yblogrepl.DeleteMessage:
		// ...
	case *yblogrepl.TruncateMessage:
		// ...

	case *yblogrepl.TypeMessage:
	case *yblogrepl.OriginMessage:

	case *yblogrepl.LogicalDecodingMessage:
		log.Printf("Logical decoding message: %q, %q", logicalMsg.Prefix, logicalMsg.Content)

	case *yblogrepl.StreamStartMessageV2:
		log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *yblogrepl.StreamStopMessageV2:
		log.Printf("Stream stop message")
	case *yblogrepl.StreamCommitMessageV2:
		log.Printf("Stream commit message: xid %d", logicalMsg.Xid)
	case *yblogrepl.StreamAbortMessageV2:
		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

type Processor struct {
	relations map[uint32]*yblogrepl.RelationMessage
	typeMap   *pgtype.Map
}

func NewProcessor() *Processor {
	return &Processor{
		relations: map[uint32]*yblogrepl.RelationMessage{},
		typeMap:   pgtype.NewMap(),
	}
}

func (p *Processor) Process(walData []byte) error {
	logicalMsg, err := yblogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("parse logical replication message: %w", err)
	}
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
	case *yblogrepl.TruncateMessage:
	case *yblogrepl.TypeMessage:
	case *yblogrepl.OriginMessage:
	case *yblogrepl.LogicalDecodingMessage:
	case *yblogrepl.StreamStartMessageV2:
	case *yblogrepl.StreamStopMessageV2:
	case *yblogrepl.StreamCommitMessageV2:
	case *yblogrepl.StreamAbortMessageV2:
	default:
		return fmt.Errorf("unknown message type in pgoutput stream: %T", logicalMsg)
	}
	return nil
}

func (p *Processor) handleBegin(msg *yblogrepl.BeginMessage) error {
	log.Debug().Uint32("xid", msg.Xid).
		Str("lsn", msg.FinalLSN.String()).
		Time("timestamp", msg.CommitTime).
		Msg("Begin transaction")
	return nil
}

func (p *Processor) handleCommit(msg *yblogrepl.CommitMessage) error {
	log.Debug().Uint8("flag", msg.Flags).
		Str("commit lsn", msg.CommitLSN.String()).
		Str("end lsn", msg.TransactionEndLSN.String()).
		Time("timestamp", msg.CommitTime).
		Msg("Commit transaction")
	return nil
}

func (p *Processor) handleUpdate(msg *yblogrepl.UpdateMessage) error {
	log.Debug().Uint32("relationId", msg.RelationID).Msg("Update message")
	rel, ok := p.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID")
	}
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
	return nil
}

func (p *Processor) handleInsert(msg *yblogrepl.InsertMessage) error {
	log.Debug().Uint32("relationId", msg.RelationID).Msg("Insert message")
	rel, ok := p.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID")
	}
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
	return nil
}
