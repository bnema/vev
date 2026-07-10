package dgram

import (
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

func TestDiagnosticSnapshotSeparatesProgressAges(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	got := make(chan Diagnostic, 1)
	tr := &Transport{
		clock:                   fixedClock{now: now},
		linkState:               ports.LinkStateDegraded,
		lastAuthenticatedPacket: now.Add(-time.Second),
		lastCompleteRecord:      now.Add(-2 * time.Second),
		lastACKProgress:         now.Add(-3 * time.Second),
		retransmits:             64,
		reassemblyInflight:      2,
		pending: map[uint64]*pending{
			1: {frame: ports.Frame{Payload: make([]byte, 7)}},
			2: {frame: ports.Frame{Payload: make([]byte, 11)}},
		},
		diagnosticCh: make(chan Diagnostic, diagnosticBufferSize),
		done:         make(chan struct{}),
	}
	tr.observe = func(d Diagnostic) {
		// This would deadlock if the observer were invoked under Transport.mu.
		_ = tr.LinkState()
		got <- d
	}
	go tr.diagnosticLoop()
	defer close(tr.done)

	tr.emitDiagnostic()
	select {
	case d := <-got:
		if d.At != now || d.State != ports.LinkStateDegraded {
			t.Fatalf("identity = %+v", d)
		}
		if d.SinceAuthenticatedPacket != time.Second || d.SinceCompleteRecord != 2*time.Second || d.SinceACKProgress != 3*time.Second {
			t.Fatalf("ages = packet %v, record %v, ack %v", d.SinceAuthenticatedPacket, d.SinceCompleteRecord, d.SinceACKProgress)
		}
		if d.PendingRecords != 2 || d.PendingBytes != 18 || d.Retransmits != 64 || d.ReassemblyInflight != 2 {
			t.Fatalf("counters = %+v", d)
		}
	case <-time.After(time.Second):
		t.Fatal("diagnostic observer was not called")
	}
}

func TestDiagnosticSnapshotIsLockConsistentDuringConcurrentUpdate(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := &snapshotClock{now: now, sampled: make(chan struct{}), release: make(chan struct{})}
	tr := &Transport{
		clock:                   clock,
		lastAuthenticatedPacket: now.Add(-time.Second),
		lastCompleteRecord:      now.Add(-2 * time.Second),
		lastACKProgress:         now.Add(-3 * time.Second),
		pending:                 make(map[uint64]*pending),
		diagnosticCh:            make(chan Diagnostic, 1),
	}

	go tr.emitDiagnostic()
	select {
	case <-clock.sampled:
	case <-time.After(time.Second):
		t.Fatal("diagnostic did not begin its clock snapshot")
	}
	updated := make(chan struct{})
	go func() {
		tr.mu.Lock()
		tr.lastAuthenticatedPacket = now.Add(time.Second)
		tr.lastCompleteRecord = now.Add(time.Second)
		tr.lastACKProgress = now.Add(time.Second)
		tr.retransmits = 1
		tr.mu.Unlock()
		close(updated)
	}()
	close(clock.release)

	var d Diagnostic
	select {
	case d = <-tr.diagnosticCh:
	case <-time.After(time.Second):
		t.Fatal("diagnostic was not emitted")
	}
	select {
	case <-updated:
	case <-time.After(time.Second):
		t.Fatal("concurrent update was blocked")
	}
	if d.At != now || d.Retransmits != 0 {
		t.Fatalf("mixed diagnostic snapshot: %+v", d)
	}
	if d.SinceAuthenticatedPacket != time.Second || d.SinceCompleteRecord != 2*time.Second || d.SinceACKProgress != 3*time.Second {
		t.Fatalf("inconsistent diagnostic ages: %+v", d)
	}
	if d.SinceAuthenticatedPacket < 0 || d.SinceCompleteRecord < 0 || d.SinceACKProgress < 0 {
		t.Fatalf("negative diagnostic age: %+v", d)
	}
}

type snapshotClock struct {
	mu      sync.Mutex
	now     time.Time
	sampled chan struct{}
	release chan struct{}
}

func (c *snapshotClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.sampled <- struct{}{}:
	default:
	}
	<-c.release
	return c.now
}

func (c *snapshotClock) NewTimer(time.Duration) ports.Timer { panic("not used") }

func TestDiagnosticObserverIsBounded(t *testing.T) {
	tr := &Transport{
		clock:        fixedClock{now: time.Now()},
		pending:      make(map[uint64]*pending),
		diagnosticCh: make(chan Diagnostic, 1),
		done:         make(chan struct{}),
	}
	for range 100 {
		tr.emitDiagnostic()
	}
	if got := len(tr.diagnosticCh); got != 1 {
		t.Fatalf("queued diagnostics = %d, want bounded buffer", got)
	}
	close(tr.done)
}
