package capture

import (
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
		values := map[string]interface{}{}
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
		oldValues := map[string]interface{}{}
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
		newValues := map[string]interface{}{}
		for idx, col := range logicalMsg.NewTuple.Columns {
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
				newValues[colName] = val
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
