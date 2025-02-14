package sink

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/strahe/curio-sentinel/capturer"
)

// ConsoleSink implements the Sink interface to output events to the console in a pretty table format
type ConsoleSink struct {
	// whether to use colored output
	colorEnabled bool
	// unified table style
	tableStyle table.Style
	// max column width for truncation
	maxColumnWidth int
	// how to handle binary data
	binaryFormat string // "hex", "base64", or "escaped"
}

// ConsoleSinkOption defines functional options for ConsoleSink
type ConsoleSinkOption func(*ConsoleSink)

// WithColorOutput enables or disables colored output
func WithColorOutput(enabled bool) ConsoleSinkOption {
	return func(s *ConsoleSink) {
		s.colorEnabled = enabled
	}
}

// WithMaxColumnWidth sets the maximum column width for truncation
func WithMaxColumnWidth(width int) ConsoleSinkOption {
	return func(s *ConsoleSink) {
		s.maxColumnWidth = width
	}
}

// WithBinaryFormat sets the format for binary data display
// Valid values: "hex", "base64", "escaped"
func WithBinaryFormat(format string) ConsoleSinkOption {
	return func(s *ConsoleSink) {
		s.binaryFormat = format
	}
}

// NewConsoleSink creates a new console sink
func NewConsoleSink(options ...ConsoleSinkOption) *ConsoleSink {
	// Create a custom table style for consistent appearance
	customStyle := table.Style{
		Name: "CDC-Custom",
		Box: table.BoxStyle{
			BottomLeft:       "└",
			BottomRight:      "┘",
			BottomSeparator:  "┴",
			Left:             "│",
			LeftSeparator:    "├",
			MiddleHorizontal: "─",
			MiddleSeparator:  "┼",
			MiddleVertical:   "│",
			PaddingLeft:      " ",
			PaddingRight:     " ",
			Right:            "│",
			RightSeparator:   "┤",
			TopLeft:          "┌",
			TopRight:         "┐",
			TopSeparator:     "┬",
			UnfinishedRow:    "...",
		},
		Options: table.Options{
			DrawBorder:      true,
			SeparateColumns: true,
			SeparateFooter:  true,
			SeparateHeader:  true,
			SeparateRows:    false,
		},
		Title: table.TitleOptions{
			Align:  text.AlignCenter,
			Colors: text.Colors{text.FgHiWhite, text.Bold},
		},
		Color: table.ColorOptions{
			Header: text.Colors{text.FgHiWhite, text.Bold},
			Row:    text.Colors{},
			Footer: text.Colors{text.FgHiWhite, text.Bold},
		},
	}

	// Create sink with default values
	sink := &ConsoleSink{
		colorEnabled:   true,
		tableStyle:     customStyle,
		maxColumnWidth: 80,    // Default max width
		binaryFormat:   "hex", // Default binary format
	}

	// Apply options
	for _, option := range options {
		option(sink)
	}

	return sink
}

// Write outputs events to the console
func (s *ConsoleSink) Write(ctx context.Context, events []*capturer.Event) error {
	for _, event := range events {
		s.writeEventTable(event)
	}
	return nil
}

// writeEventTable outputs an event as a nicely formatted table
func (s *ConsoleSink) writeEventTable(event *capturer.Event) {
	// Define colors
	insertColor := color.New(color.FgGreen, color.Bold).SprintFunc()
	updateColor := color.New(color.FgYellow, color.Bold).SprintFunc()
	deleteColor := color.New(color.FgRed, color.Bold).SprintFunc()
	unchangedColor := color.New(color.FgBlue).SprintFunc()
	changeAddedColor := color.New(color.FgGreen).SprintFunc()
	changeRemovedColor := color.New(color.FgRed).SprintFunc()
	changeModifiedColor := color.New(color.FgYellow).SprintFunc()

	if !s.colorEnabled {
		insertColor = fmt.Sprint
		updateColor = fmt.Sprint
		deleteColor = fmt.Sprint
		unchangedColor = fmt.Sprint
		changeAddedColor = fmt.Sprint
		changeRemovedColor = fmt.Sprint
		changeModifiedColor = fmt.Sprint
	}

	// Create a unified output table for the entire event
	eventTable := table.NewWriter()
	eventTable.SetOutputMirror(os.Stdout)

	// Format operation type with color
	var opText string
	switch event.Type {
	case capturer.OperationTypeInsert:
		opText = insertColor("INSERT")
	case capturer.OperationTypeUpdate:
		opText = updateColor("UPDATE")
	case capturer.OperationTypeDelete:
		opText = deleteColor("DELETE")
	default:
		opText = fmt.Sprintf("%v", event.Type)
	}

	// Create summary section
	summaryRows := []table.Row{
		{"Event ID", event.ID},
		{"Operation", opText},
		{"Table", fmt.Sprintf("%s.%s", event.Schema, event.Table)},
		{"Timestamp", event.Timestamp.Format(time.RFC3339)},
	}

	if event.LSN != "" {
		summaryRows = append(summaryRows, table.Row{"LSN", event.LSN})
	}

	// Add primary key information
	if len(event.PrimaryKey) > 0 {
		pkValues := make([]string, 0)

		// Sort keys for consistent output
		keys := getSortedKeys(event.PrimaryKey)

		for _, k := range keys {
			formattedVal := s.formatValue(event.PrimaryKey[k])
			pkValues = append(pkValues, fmt.Sprintf("%s=%s", k, formattedVal))
		}

		summaryRows = append(summaryRows, table.Row{"Primary Key", strings.Join(pkValues, ", ")})
	}

	// Create a summary section
	summaryTable := table.NewWriter()
	for _, row := range summaryRows {
		summaryTable.AppendRow(row)
	}
	summaryTable.SetStyle(s.tableStyle)
	summaryTable.Style().Options.DrawBorder = false
	summaryTable.Style().Options.SeparateRows = false
	summaryTable.Style().Box.PaddingLeft = " "
	summaryTable.Style().Box.PaddingRight = " "

	// Add the summary section to the main event table
	eventTable.AppendRow(table.Row{summaryTable.Render()})

	// Create and add data tables depending on operation type
	var dataTitle string
	var dataTable table.Writer

	switch event.Type {
	case capturer.OperationTypeInsert:
		if len(event.After) > 0 {
			dataTitle = "Inserted Data"
			dataTable = s.createInsertedDataTable(event.After, changeAddedColor)
		}

	case capturer.OperationTypeDelete:
		if len(event.Before) > 0 {
			dataTitle = "Deleted Data"
			dataTable = s.createDeletedDataTable(event.Before, changeRemovedColor)
		}

	case capturer.OperationTypeUpdate:
		if len(event.Before) > 0 || len(event.After) > 0 {
			dataTitle = "Changed Data"
			dataTable = s.createChangedDataTable(
				event.Before,
				event.After,
				changeAddedColor,
				changeRemovedColor,
				changeModifiedColor,
				unchangedColor,
			)
		}
	}

	// Add data table if we have one
	if dataTable != nil {
		dataTableStr := dataTable.Render()

		// Add section title before the data table
		eventTable.AppendRow(table.Row{""})
		eventTable.AppendRow(table.Row{text.Bold.Sprint(dataTitle)})
		eventTable.AppendRow(table.Row{dataTableStr})
	}

	// Add metadata if available
	if len(event.Metadata) > 0 {
		metadataTable := s.createMetadataTable(event.Metadata)
		metadataTableStr := metadataTable.Render()

		// Add section title before the metadata table
		eventTable.AppendRow(table.Row{""})
		eventTable.AppendRow(table.Row{text.Bold.Sprint("Additional Metadata")})
		eventTable.AppendRow(table.Row{metadataTableStr})
	}

	// Apply the unified style and render the entire event table
	eventTable.SetStyle(s.tableStyle)

	// Add title based on event type and table
	var titlePrefix string
	switch event.Type {
	case capturer.OperationTypeInsert:
		titlePrefix = "INSERT INTO"
	case capturer.OperationTypeUpdate:
		titlePrefix = "UPDATE"
	case capturer.OperationTypeDelete:
		titlePrefix = "DELETE FROM"
	default:
		titlePrefix = string(event.Type)
	}

	eventTable.SetTitle(fmt.Sprintf("%s %s.%s", titlePrefix, event.Schema, event.Table))

	// Add a clear separator before each event
	fmt.Println()
	fmt.Println(strings.Repeat("─", 100))

	// Render the entire event as a single table
	eventTable.Render()
	fmt.Println()
}

// createInsertedDataTable creates a table for inserted data
func (s *ConsoleSink) createInsertedDataTable(data map[string]any, valueColor func(a ...interface{}) string) table.Writer {
	dataTable := table.NewWriter()
	dataTable.AppendHeader(table.Row{"Column", "Value"})

	// Sort keys for consistent output
	keys := getSortedKeys(data)

	for _, k := range keys {
		formattedVal := s.formatValue(data[k])
		dataTable.AppendRow(table.Row{
			k,
			valueColor(formattedVal),
		})
	}

	dataTable.SetStyle(s.tableStyle)
	return dataTable
}

// createDeletedDataTable creates a table for deleted data
func (s *ConsoleSink) createDeletedDataTable(data map[string]any, valueColor func(a ...interface{}) string) table.Writer {
	dataTable := table.NewWriter()
	dataTable.AppendHeader(table.Row{"Column", "Value"})

	// Sort keys for consistent output
	keys := getSortedKeys(data)

	for _, k := range keys {
		formattedVal := s.formatValue(data[k])
		dataTable.AppendRow(table.Row{
			k,
			valueColor(formattedVal),
		})
	}

	dataTable.SetStyle(s.tableStyle)
	return dataTable
}

// createChangedDataTable creates a table showing before/after changes
func (s *ConsoleSink) createChangedDataTable(
	before map[string]any,
	after map[string]any,
	addedColor func(a ...interface{}) string,
	removedColor func(a ...interface{}) string,
	modifiedColor func(a ...interface{}) string,
	unchangedColor func(a ...interface{}) string,
) table.Writer {
	dataTable := table.NewWriter()
	dataTable.AppendHeader(table.Row{"Column", "Before", "After", "Change"})

	// Get all keys from both before and after
	allKeys := make(map[string]bool)
	for k := range before {
		allKeys[k] = true
	}
	for k := range after {
		allKeys[k] = true
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(allKeys))
	for k := range allKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Track if we have any changed values
	hasChanges := false

	for _, k := range keys {
		beforeVal, beforeExists := before[k]
		afterVal, afterExists := after[k]

		var beforeStr, afterStr, changeStr string

		if !beforeExists {
			// Column added
			beforeStr = ""
			afterStr = addedColor(s.formatValue(afterVal))
			changeStr = addedColor("ADDED")
			hasChanges = true
		} else if !afterExists {
			// Column removed
			beforeStr = removedColor(s.formatValue(beforeVal))
			afterStr = ""
			changeStr = removedColor("REMOVED")
			hasChanges = true
		} else if !reflect.DeepEqual(beforeVal, afterVal) {
			// Value changed
			beforeStr = removedColor(s.formatValue(beforeVal))
			afterStr = addedColor(s.formatValue(afterVal))
			changeStr = modifiedColor("MODIFIED")
			hasChanges = true
		} else {
			// Value unchanged
			beforeStr = unchangedColor(s.formatValue(beforeVal))
			afterStr = unchangedColor(s.formatValue(afterVal))
			changeStr = unchangedColor("UNCHANGED")
		}

		dataTable.AppendRow(table.Row{
			k,
			beforeStr,
			afterStr,
			changeStr,
		})
	}

	// If there are no changes, add an informational row
	if !hasChanges && len(keys) > 0 {
		dataTable = table.NewWriter()
		dataTable.AppendRow(table.Row{"No changes detected in column values"})
		dataTable.SetStyle(s.tableStyle)
		return dataTable
	}

	dataTable.SetStyle(s.tableStyle)
	return dataTable
}

// createMetadataTable creates a table for additional metadata
func (s *ConsoleSink) createMetadataTable(metadata map[string]any) table.Writer {
	metadataTable := table.NewWriter()
	metadataTable.AppendHeader(table.Row{"Key", "Value"})

	// Sort keys for consistent output
	keys := getSortedKeys(metadata)

	for _, k := range keys {
		formattedVal := s.formatValue(metadata[k])
		metadataTable.AppendRow(table.Row{
			k,
			formattedVal,
		})
	}

	metadataTable.SetStyle(s.tableStyle)
	return metadataTable
}

// formatValue formats a value for display, handling truncation, binary data, and nil values
func (s *ConsoleSink) formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}

	v := reflect.ValueOf(val)

	// Handle different types
	switch v.Kind() {
	case reflect.Slice:
		// Check if it's a byte slice
		if byteSlice, ok := val.([]byte); ok {
			return s.formatByteArray(byteSlice)
		}
		// For other slices
		return s.truncateString(fmt.Sprintf("%v", val))

	case reflect.String:
		return s.truncateString(fmt.Sprintf("%v", val))

	case reflect.Map, reflect.Struct:
		// For complex types, use Go's default formatting but truncate
		return s.truncateString(fmt.Sprintf("%v", val))

	default:
		// For all other types, use normal formatting
		return fmt.Sprintf("%v", val)
	}
}

// formatByteArray formats a byte array according to the configured format
func (s *ConsoleSink) formatByteArray(data []byte) string {
	// For empty arrays
	if len(data) == 0 {
		return "[]"
	}

	var result string
	switch s.binaryFormat {
	case "hex":
		// Format as hexadecimal
		result = "0x" + hex.EncodeToString(data)
	case "base64":
		// Format as base64
		result = "base64:" + base64.StdEncoding.EncodeToString(data)
	case "escaped":
		// Try to format as a string with escaped characters
		if isTextual(data) {
			result = formatEscapedString(data)
		} else {
			// Fall back to hex for binary data
			result = "0x" + hex.EncodeToString(data)
		}
	default:
		// Default to hex
		result = "0x" + hex.EncodeToString(data)
	}

	// Always truncate the result for consistent display
	return s.truncateString(result)
}

// truncateString truncates a string if it's longer than maxColumnWidth
func (s *ConsoleSink) truncateString(str string) string {
	if len(str) <= s.maxColumnWidth {
		return str
	}

	// Truncate and add ellipsis
	return str[:s.maxColumnWidth-3] + "..."
}

// isTextual checks if a byte array is likely to be textual data
func isTextual(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}

	textual := true
	for _, r := range string(data) {
		if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			textual = false
			break
		}
	}
	return textual
}

// formatEscapedString formats a byte array as a string with special characters escaped
func formatEscapedString(data []byte) string {
	s := string(data)
	var result strings.Builder
	result.WriteRune('"')

	for _, r := range s {
		switch r {
		case '\n':
			result.WriteString("\\n")
		case '\r':
			result.WriteString("\\r")
		case '\t':
			result.WriteString("\\t")
		case '\\':
			result.WriteString("\\\\")
		case '"':
			result.WriteString("\\\"")
		default:
			if unicode.IsPrint(r) {
				result.WriteRune(r)
			} else {
				// Use Unicode escape sequence for non-printable characters
				result.WriteString(fmt.Sprintf("\\u%04x", r))
			}
		}
	}

	result.WriteRune('"')
	return result.String()
}

// getSortedKeys returns sorted keys from a map for consistent output
func getSortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Flush implements the Sink interface, no buffering for console output
func (s *ConsoleSink) Flush(ctx context.Context) error {
	return nil
}

// Close implements the Sink interface
func (s *ConsoleSink) Close() error {
	return nil
}

// Type returns the type of this sink
func (s *ConsoleSink) Type() string {
	return "console"
}
