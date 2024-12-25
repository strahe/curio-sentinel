package capture

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"

	"github.com/strahe/curio-sentinel/yblogrepl"
)

type EventIDGenerator struct {
	currentXid   uint32
	eventCounter int
	currentLSN   yblogrepl.LSN
	mu           sync.Mutex
}

func (g *EventIDGenerator) BeginTransaction(xid uint32, lsn yblogrepl.LSN) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.currentXid = xid
	g.currentLSN = lsn
	g.eventCounter = 0
}

func (g *EventIDGenerator) EndTransaction() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.currentXid = 0
	g.currentLSN = 0
	g.eventCounter = 0
}

func (g *EventIDGenerator) Generate(rel *yblogrepl.RelationMessage) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.eventCounter++
	var eventID string
	if rel != nil {
		eventID = fmt.Sprintf("%d-%d-%s-%d", g.currentXid, g.eventCounter, g.currentLSN, rel.RelationID)
	} else {
		eventID = fmt.Sprintf("%d-%d-%s", g.currentXid, g.eventCounter, g.currentLSN)
	}
	h := fnv.New64a()
	h.Write([]byte(eventID))
	return strconv.FormatUint(h.Sum64(), 16)
}
