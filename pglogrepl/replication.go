package pglogrepl

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/yugabyte/pgx/v5/pgconn"
)

type ReplicationSlotInfo struct {
	// 复制槽名称
	SlotName string `json:"slot_name"`

	// 逻辑复制输出插件名称 (例如 pgoutput)
	// 对于物理复制槽为空
	Plugin string `json:"plugin"`

	// 复制槽类型："physical" 或 "logical"
	SlotType string `json:"slot_type"`

	// 数据库OID
	DatOid uint32 `json:"datoid"`

	// 复制槽关联的数据库名称
	Database string `json:"database"`

	// 是否为临时复制槽
	// 临时槽在会话结束或崩溃时会自动删除
	Temporary bool `json:"temporary"`

	// 复制槽当前是否活跃（有客户端连接使用）
	Active bool `json:"active"`

	// 如果活跃，当前使用此复制槽的进程ID
	ActivePID sql.NullInt32 `json:"active_pid"`

	// 复制槽要求数据库保留的最早事务ID
	Xmin sql.NullString `json:"xmin"`

	// 复制槽要求数据库保留的最早系统目录事务ID
	CatalogXmin sql.NullString `json:"catalog_xmin"`

	// 需要保留的最早WAL位置
	// 从此位置开始的WAL都将保留，不会被回收
	RestartLSN LSN `json:"restart_lsn"`

	// 消费者确认已处理的位置
	ConfirmedFlushLSN LSN `json:"confirmed_flush_lsn"`

	// YugabyteDB特有: 流ID
	YBStreamID sql.NullString `json:"yb_stream_id,omitempty"`

	// YugabyteDB特有: 重启提交混合时间戳
	YBRestartCommitHT sql.NullInt64 `json:"yb_restart_commit_ht,omitempty"`

	// 以下是计算字段，不直接来自表

	// 复制槽创建时间（需要从系统视图或事件中获取）
	CreationTime time.Time `json:"creation_time,omitempty"`

	// 复制槽最后活跃时间
	LastActiveTime time.Time `json:"last_active_time,omitempty"`

	// 复制槽已保留的WAL大小（字节）
	RetainedWALBytes int64 `json:"retained_wal_bytes,omitempty"`

	// 复制延迟（主服务器当前位置与已确认位置的时间差）
	ReplicationLag time.Duration `json:"replication_lag,omitempty"`
}

// ListReplicationSlots 返回所有复制槽的信息
func ListReplicationSlots(ctx context.Context, conn *pgconn.PgConn) ([]ReplicationSlotInfo, error) {
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

		// 基本字段
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

		// 可空字段
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

		// LSN 字段
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

		// YugabyteDB 特有字段
		if len(row[12]) > 0 {
			slot.YBStreamID = sql.NullString{String: string(row[12]), Valid: true}
		}
		if len(row[13]) > 0 {
			ht, err := strconv.ParseInt(string(row[13]), 10, 64)
			if err == nil {
				slot.YBRestartCommitHT = sql.NullInt64{Int64: ht, Valid: true}
			}
		}

		// 计算字段
		if len(row[14]) > 0 {
			currentLSNStr = string(row[14])
		}
		if len(row[15]) > 0 {
			retainedBytes, _ = strconv.ParseInt(string(row[15]), 10, 64)
			slot.RetainedWALBytes = retainedBytes
		}

		// 计算复制滞后
		if currentLSNStr != "" && confirmedFlushLSNStr != "" {
			currentLSN, err := ParseLSN(currentLSNStr)
			if err == nil && slot.ConfirmedFlushLSN > 0 {
				// 根据数据库负载和写入率估算延迟
				// 这只是粗略估计，实际应用中应根据实际WAL生成率计算
				lagBytes := int64(currentLSN - slot.ConfirmedFlushLSN)
				if lagBytes > 0 {
					// 假设平均每秒生成1MB WAL
					slot.ReplicationLag = time.Duration(lagBytes/1048576) * time.Second
				}
			}
		}

		slots = append(slots, slot)
	}

	return slots, nil
}
