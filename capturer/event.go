package capturer

import (
	"fmt"
	"time"
)

type OperationType string

const (
	OperationTypeInsert OperationType = "INSERT"
	OperationTypeUpdate OperationType = "UPDATE"
	OperationTypeDelete OperationType = "DELETE"
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

func (e *Event) String() string {
	return fmt.Sprintf("%s %s.%s %s", e.Type, e.Schema, e.Table, e.pk())
}

func (e *Event) pk() string {
	parts := make([]string, 0, len(e.PrimaryKey))
	for k, v := range e.PrimaryKey {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	return fmt.Sprintf("<%s>", joinStrings(parts, ", "))
}

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for _, s := range strs[1:] {
		result += sep + s
	}
	return result
}
