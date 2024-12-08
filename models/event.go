package models

import "time"

// OperationType 定义了数据变更操作类型
type OperationType string

const (
	Insert OperationType = "INSERT"
	Update OperationType = "UPDATE"
	Delete OperationType = "DELETE"
)

// Event 表示从数据源捕获的事件
type Event struct {
	// ID 是事件的唯一标识符
	ID string `json:"id"`

	// Type 表示事件类型，如 "insert", "update", "delete", "truncate", "message", 等
	Type string `json:"type"`

	// Schema 是数据库模式（schema）名称
	Schema string `json:"schema,omitempty"`

	// Table 是数据库表名
	Table string `json:"table,omitempty"`

	// Data 包含事件数据
	// - 对于 insert 事件，包含完整的插入行数据
	// - 对于 update 事件，包含 "old" 和 "new" 两个子对象
	// - 对于 delete 事件，包含被删除的行数据
	// - 对于 truncate 事件，包含被截断的表列表
	// - 对于 message 事件，包含消息内容
	Data map[string]any `json:"data"`

	// Timestamp 是事件发生的时间戳
	Timestamp time.Time `json:"timestamp"`

	// Source 标识事件来源，如 "postgres_logical_replication", "mysql_binlog" 等
	Source string `json:"source,omitempty"`

	// Extra 包含与特定事件相关的额外元数据，不同事件类型可能有不同的额外数据
	// 例如事务ID、LSN位置等
	Extra map[string]any `json:"extra,omitempty"`

	// Checkpoint 是事件的检查点信息，用于恢复处理位置
	// 不同的捕获器实现会使用不同的检查点格式
	Checkpoint string `json:"checkpoint,omitempty"`
}
