package models

import "time"

// OperationType 定义了数据变更操作类型
type OperationType string

const (
	Insert OperationType = "INSERT"
	Update OperationType = "UPDATE"
	Delete OperationType = "DELETE"
)

// Event 表示一个数据变更事件
type Event struct {
	ID          string            `json:"id"`
	Operation   OperationType     `json:"operation"`
	Namespace   string            `json:"namespace"`
	Table       string            `json:"table"`
	Timestamp   time.Time         `json:"timestamp"`
	BeforeImage map[string]any    `json:"before_image,omitempty"`
	AfterImage  map[string]any    `json:"after_image,omitempty"`
	PrimaryKey  map[string]any    `json:"primary_key"`
	LSN         string            `json:"lsn"`
	TxID        string            `json:"tx_id"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}
