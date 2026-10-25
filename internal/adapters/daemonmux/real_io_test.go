package daemonmux

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// realIODeadline is the single bounded failure guard of every real-socket wait.
// The waits below block on notifications (admission events and the pump's
// per-stream watch channel); the guard only turns a lost wakeup into a failure.
const realIODeadline = 10 * time.Second

// admissions receives the physical identity of every Open the daemon-side pump
// admitted, so a real-socket test waits for admission without polling.
type admissions chan PhysicalStreamID

// newObservedPump builds a daemon-side pump over carrier whose admission events
// are published on the returned channel, starts it, and closes it at test end.
// The observer is registered before Start, as the pump requires.
func newObservedPump(t *testing.T, carrier FramedCarrier) (*Pump, admissions) {
	t.Helper()
	pump, err := NewPump(carrier, DirectionClient, DefaultMuxCeilings())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pump.Close() })
	admitted := make(admissions, int(MaxMuxStreams)+1)
	require.NoError(t, pump.OnAdmitted(func(open Open) {
		select {
		case admitted <- open.Ref.Physical:
		default:
		}
	}))
	pump.Start(context.Background())
	return pump, admitted
}

// awaitAdmitted blocks until count admission events arrived.
func awaitAdmitted(t *testing.T, admitted admissions, count int) {
	t.Helper()
	guard := time.After(realIODeadline)
	for i := 0; i < count; i++ {
		select {
		case <-admitted:
		case <-guard:
			t.Fatalf("pump admitted %d of %d streams", i, count)
		}
	}
}

// awaitStatus blocks on the pump's per-stream watch channel until cond holds for
// the stream's status. The watch is taken before the status is read, so a frame
// applied in between is never missed.
func awaitStatus(t *testing.T, pump *Pump, id PhysicalStreamID, cond func(StreamStatus) bool, msg string) StreamStatus {
	t.Helper()
	guard := time.After(realIODeadline)
	for {
		ch := pump.Watch(id)
		status, ok := pump.Engine().Status(id)
		if ok && cond(status) {
			return status
		}
		select {
		case <-ch:
		case <-status.Done:
		case <-guard:
			t.Fatalf("stream %d: %s", id, msg)
		}
	}
}

// awaitTake blocks until one stream's inbound queue yields a chunk.
func awaitTake(t *testing.T, pump *Pump, id PhysicalStreamID) []byte {
	t.Helper()
	guard := time.After(realIODeadline)
	for {
		ch := pump.Watch(id)
		if chunk, ok := pump.Take(id); ok {
			return chunk
		}
		select {
		case <-ch:
		case <-guard:
			t.Fatalf("stream %d: no inbound chunk arrived", id)
		}
	}
}
