package capture

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/strahe/curio-sentinel/models"
	"github.com/strahe/curio-sentinel/yblogrepl"
)

type TransactionTracker struct {
	xid               uint32
	commitLSN         yblogrepl.LSN
	transactionEndLSN yblogrepl.LSN
	commitTime        time.Time
	pendingEvents     []*models.Event
	eventCounter      int
	mu                sync.Mutex
}

func (t *TransactionTracker) Begin(xid uint32, lsn yblogrepl.LSN, commitTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.xid = xid
	t.commitLSN = lsn
	t.commitTime = commitTime
}

// End marks the end of a transaction and returns the events that were captured
func (t *TransactionTracker) End(lsn, endLsn yblogrepl.LSN, commitTime time.Time) []*models.Event {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.transactionEndLSN = endLsn

	penddingEvents := t.pendingEvents

	t.pendingEvents = make([]*models.Event, 0)
	t.eventCounter = 0

	return penddingEvents
}

func (t *TransactionTracker) AddEvent(event *models.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if event.ID == "" {
		event.ID = t.generateEventID(nil)
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = t.commitTime
	}
	if event.LSN == "" {
		event.LSN = t.commitLSN.String()
	}
	t.pendingEvents = append(t.pendingEvents, event)
}

// generateEventID generates a unique event ID based on the transaction ID, event counter, commit LSN and relation ID
// call with lock held
func (t *TransactionTracker) generateEventID(rel *yblogrepl.RelationMessage) string {
	t.eventCounter++
	var eventID string
	if rel != nil {
		eventID = fmt.Sprintf("%d-%d-%s-%d", t.xid, t.eventCounter, t.commitLSN, rel.RelationID)
	} else {
		eventID = fmt.Sprintf("%d-%d-%s", t.xid, t.eventCounter, t.commitLSN)
	}
	h := fnv.New64a()
	h.Write([]byte(eventID))
	return strconv.FormatUint(h.Sum64(), 16)
}
