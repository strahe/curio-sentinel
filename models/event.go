package models

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

	// Data contains the row values based on operation type:
	// - INSERT: new row values
	// - DELETE: values of the deleted row
	// - UPDATE: contains both old and new values of the updated row
	Data map[string]any `json:"data"`

	// Timestamp is the time when the event was generated
	Timestamp time.Time `json:"timestamp"`

	// Extra contains additional information about the event
	Extra map[string]any `json:"extra,omitempty"`

	// LSN is the Log Sequence Number of the event
	LSN string `json:"lsn,omitempty"`
}
