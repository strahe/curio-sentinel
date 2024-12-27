package yblogrepl

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/yugabyte/pgx/v5/pgconn"
)

type ReplicationSlotInfo struct {
	SlotName          string         `json:"slot_name"`
	Plugin            string         `json:"plugin"`
	SlotType          string         `json:"slot_type"`
	DatOid            uint32         `json:"datoid"`
	Database          string         `json:"database"`
	Temporary         bool           `json:"temporary"`
	Active            bool           `json:"active"`
	ActivePID         sql.NullInt32  `json:"active_pid"`
	Xmin              sql.NullString `json:"xmin"`
	CatalogXmin       sql.NullString `json:"catalog_xmin"`
	RestartLSN        LSN            `json:"restart_lsn"`
	ConfirmedFlushLSN LSN            `json:"confirmed_flush_lsn"`
	YBStreamID        sql.NullString `json:"yb_stream_id,omitempty"`
	YBRestartCommitHT sql.NullInt64  `json:"yb_restart_commit_ht,omitempty"`

	// computed fields
	CreationTime     time.Time     `json:"creation_time,omitempty"`
	LastActiveTime   time.Time     `json:"last_active_time,omitempty"`
	RetainedWALBytes int64         `json:"retained_wal_bytes,omitempty"`
	ReplicationLag   time.Duration `json:"replication_lag,omitempty"`
}

// ListReplicationSlots returns a list of replication slots
func ListReplicationSlots(ctx context.Context, conn *pgconn.PgConn) ([]ReplicationSlotInfo, error) {
	return getReplicationSlot(ctx, conn, nil)
}

func GetReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) (*ReplicationSlotInfo, error) {
	slots, err := getReplicationSlot(ctx, conn, &slotName)
	if err != nil {
		return nil, err
	}

	if len(slots) == 0 {
		return nil, fmt.Errorf("replication slot %s not found", slotName)
	}

	return &slots[0], nil
}

// getReplicationSlot returns the information of a replication slot
// if the slot name not provided, it will return the all the replication slots
// if the slot name provided, it will return the information of the slot
func getReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName *string) ([]ReplicationSlotInfo, error) {
	query := `
		SELECT
			slot_name,
			plugin,
			slot_type,
			datoid,
			database,
			temporary,
			active,
			active_pid,
			xmin::text,
			catalog_xmin::text,
			restart_lsn,
			confirmed_flush_lsn,
			yb_stream_id,
			yb_restart_commit_ht,
			pg_current_wal_lsn() AS current_lsn,
			pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
		FROM
			pg_catalog.pg_replication_slots
	`
	if slotName != nil {
		query += fmt.Sprintf(" WHERE slot_name = '%s'", *slotName)
	}

	result := conn.Exec(ctx, query)
	results, err := result.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to query replication slots: %w", err)
	}

	if len(results) != 1 {
		return nil, fmt.Errorf("expected 1 result set, got %d", len(results))
	}

	slots := make([]ReplicationSlotInfo, 0, len(results[0].Rows))

	for _, row := range results[0].Rows {
		var slot ReplicationSlotInfo
		var restartLSNStr, confirmedFlushLSNStr, currentLSNStr string
		var retainedBytes int64

		if len(row[0]) > 0 {
			slot.SlotName = string(row[0])
		}
		if len(row[1]) > 0 {
			slot.Plugin = string(row[1])
		}
		if len(row[2]) > 0 {
			slot.SlotType = string(row[2])
		}
		if len(row[3]) > 0 {
			datoid, err := strconv.ParseUint(string(row[3]), 10, 32)
			if err == nil {
				slot.DatOid = uint32(datoid)
			}
		}
		if len(row[4]) > 0 {
			slot.Database = string(row[4])
		}
		if len(row[5]) > 0 {
			slot.Temporary = string(row[5]) == "t"
		}
		if len(row[6]) > 0 {
			slot.Active = string(row[6]) == "t"
		}

		if len(row[7]) > 0 {
			pid, err := strconv.ParseInt(string(row[7]), 10, 32)
			if err == nil {
				slot.ActivePID = sql.NullInt32{Int32: int32(pid), Valid: true}
			}
		}
		if len(row[8]) > 0 {
			slot.Xmin = sql.NullString{String: string(row[8]), Valid: true}
		}
		if len(row[9]) > 0 {
			slot.CatalogXmin = sql.NullString{String: string(row[9]), Valid: true}
		}

		if len(row[10]) > 0 {
			restartLSNStr = string(row[10])
			restartLSN, err := ParseLSN(restartLSNStr)
			if err == nil {
				slot.RestartLSN = restartLSN
			}
		}
		if len(row[11]) > 0 {
			confirmedFlushLSNStr = string(row[11])
			confirmedLSN, err := ParseLSN(confirmedFlushLSNStr)
			if err == nil {
				slot.ConfirmedFlushLSN = confirmedLSN
			}
		}

		// yb specific fields
		if len(row[12]) > 0 {
			slot.YBStreamID = sql.NullString{String: string(row[12]), Valid: true}
		}
		if len(row[13]) > 0 {
			ht, err := strconv.ParseInt(string(row[13]), 10, 64)
			if err == nil {
				slot.YBRestartCommitHT = sql.NullInt64{Int64: ht, Valid: true}
			}
		}

		// computed fields
		if len(row[14]) > 0 {
			currentLSNStr = string(row[14])
		}
		if len(row[15]) > 0 {
			retainedBytes, _ = strconv.ParseInt(string(row[15]), 10, 64)
			slot.RetainedWALBytes = retainedBytes
		}

		if currentLSNStr != "" && confirmedFlushLSNStr != "" {
			currentLSN, err := ParseLSN(currentLSNStr)
			if err == nil && slot.ConfirmedFlushLSN > 0 {
				lagBytes := int64(currentLSN - slot.ConfirmedFlushLSN)
				if lagBytes > 0 {
					slot.ReplicationLag = time.Duration(lagBytes/1048576) * time.Second
				}
			}
		}

		slots = append(slots, slot)
	}

	return slots, nil
}

func CheckReplicationSlotExists(ctx context.Context, conn *pgconn.PgConn, slotName string) (bool, error) {

	sql := "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1"
	result := conn.ExecParams(ctx, sql, [][]byte{[]byte(slotName)}, nil, nil, nil)

	cmdTag, err := result.Close()
	if err != nil {
		return false, fmt.Errorf("failed to query replication slot: %w", err)
	}

	return cmdTag.RowsAffected() > 0, nil
}

type CreateReplicationSlotOptions struct {
	OutputPlugin string
	Temporary    bool   // Yugabyte not support temporary replication slot yet
	LSNType      string // "SEQUENCE" or "HYBRID_TIME", default is "SEQUENCE"
}

type CreateReplicationSlotResult struct {
	Name string
	LSN  LSN
}

// CreateLogicalReplicationSlot creates a logical replication slot
func CreateLogicalReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName string, options CreateReplicationSlotOptions) (*CreateReplicationSlotResult, error) {
	sql := fmt.Sprintf("SELECT * FROM pg_create_logical_replication_slot('%s', '%s', %v, '%s');",
		slotName, options.OutputPlugin, options.Temporary, options.LSNType)

	result := conn.Exec(ctx, sql)
	results, err := result.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to create logical replication slot: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, fmt.Errorf("no result returned from pg_create_logical_replication_slot")
	}

	if len(results[0].Rows[0]) < 2 {
		return nil, fmt.Errorf("expected 2 columns in result, got %d", len(results[0].Rows[0]))
	}

	lsn, err := ParseLSN(string(results[0].Rows[0][1]))
	if err != nil {
		return nil, fmt.Errorf("failed to parse LSN: %w", err)
	}

	return &CreateReplicationSlotResult{
		Name: string(results[0].Rows[0][0]),
		LSN:  lsn,
	}, nil
}

// DropReplicationSlot drops a logical replication slot.
func DropReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) error {
	sql := fmt.Sprintf("SELECT pg_drop_replication_slot('%s');", slotName)
	_, err := conn.Exec(ctx, sql).ReadAll()
	return err
}
