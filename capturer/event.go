package capturer

import "time"

type OperationType string

const (
	Insert OperationType = "INSERT"
	Update OperationType = "UPDATE"
	Delete OperationType = "DELETE"
)

type Event struct {
	// ID is a unique identifier for the event
	ID string `json:"id"`

	// Type is the type of operation that was performed on the row
	Type OperationType `json:"type"`

	// Schema is the schema of the table
	Schema string `json:"schema,omitempty"`

	// Table is the name of the table
	Table string `json:"table,omitempty"`

	// PrimaryKey is the primary key of the row, used to uniquely identify a row
	PrimaryKey map[string]any `json:"primary_key,omitempty"`

	// Before contains the row values before the operation
	// - UPDATE: contains old values of the updated row
	// - DELETE: contains values of the deleted row
	// - INSERT: will be nil/empty
	Before map[string]any `json:"before,omitempty"`

	// After contains the row values after the operation
	// - INSERT: contains new row values
	// - UPDATE: contains new values of the updated row
	// - DELETE: will be nil/empty
	After map[string]any `json:"after,omitempty"`

	// Timestamp is the time when the event occurred
	Timestamp time.Time `json:"timestamp"`

	// Metadata contains additional information about the event
	Metadata map[string]any `json:"extra,omitempty"`

	// LSN is the Log Sequence Number of the event
	LSN string `json:"lsn,omitempty"`
}
