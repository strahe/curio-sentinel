# curio-sentinel

A CLI tool and library for monitoring database changes in Curio clusters.

It build on top of YugabyteDB's CDC feature and provides a simple way to monitor changes in the database.

## Low level usage

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/strahe/curio-sentinel/capturer"
)

func main() {
	// Create a capturer config
	config := capturer.Config{
		Database: capturer.DatabaseConfig{
			Hosts:    []string{"localhost"},
			Port:     5433,
			Username: "yugabyte",
			Password: "yugabyte",
			Database: "yugabyte",
		},
		// Specify tables to monitor, leave empty to monitor all tables
		Tables: []string{"curio.my_table"},
	}

	// Create a capturer
	cap := capturer.NewYugabyteCapturer(config, nil)
	if err := cap.Start(); err != nil {
		log.Fatalf("Failed to start capturer: %v", err)
	}
	defer cap.Stop()

	for event := range cap.Events() {
		fmt.Printf("Event: %s on %s.%s\n", event.Type, event.Schema, event.Table)

		// handle event
		switch event.Type {
		case capturer.Insert, capturer.Delete:
			fmt.Printf("Data: %v\n", event.Data)
		case capturer.Update:
			fmt.Printf("Before: %v\n", event.Data["before"])
			fmt.Printf("After: %v\n", event.Data["after"])
		}
		// Acknowledge the event
		cap.ACK(context.Background(), event.LSN)
	}
}
```
