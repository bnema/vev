package daemon

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// echoTestClock is a settable clock whose timers fire only when the test says.
type echoTestClock struct {
	mu     sync.Mutex
	now    time.Time
	timers chan echoTestTimer
}

type echoTestTimer struct {
	timer *manualAttentionTimer
	delay time.Duration
}

func newEchoTestClock() *echoTestClock {
	return &echoTestClock{now: time.Unix(100, 0), timers: make(chan echoTestTimer, 16)}
}

func (c *echoTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *echoTestClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *echoTestClock) NewTimer(d time.Duration) ports.Timer {
	t := &manualAttentionTimer{ch: make(chan time.Time, 1)}
	c.timers <- echoTestTimer{timer: t, delay: d}
	return t
}

// nextTimer returns the next echo timer, skipping the send-bound timers the
// ack-only output path creates.
func (c *echoTestClock) nextTimer(t *testing.T) (*manualAttentionTimer, time.Duration) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case next := <-c.timers:
			if next.delay <= echoAckDelay {
				return next.timer, next.delay
			}
		case <-deadline:
			t.Fatal("timed out waiting for echo timer")
			return nil, 0
		}
	}
}

func TestEchoAckWaitsForEchoTimeoutAndSendsAckOnlyOutput(t *testing.T) {
	clock := newEchoTestClock()
	d, _, ac, sends := newManualSessionWithPTYsClock(t, clock, newQuietPTY())

	d.noteInputApplied(ac, 1)
	clock.advance(20 * time.Millisecond)
	d.noteInputApplied(ac, 2)
	timer, delay := clock.nextTimer(t)
	require.Equal(t, echoAckDelay, delay)
	require.Zero(t, ac.echoAck.Load(), "input is not echoed when applied")

	clock.advance(30 * time.Millisecond)
	timer.fire()
	output := decodeServerMessage(t, awaitFrame(t, sends, "Output")).(protocol.Output)
	require.Equal(t, uint64(1), output.Echo)
	require.Zero(t, output.New, "an ack-only output is a side effect")
	require.Empty(t, output.Data)

	// Input 2 is still younger than the timeout: the tracker re-arms for its
	// remaining age.
	timer, delay = clock.nextTimer(t)
	require.Equal(t, 20*time.Millisecond, delay)
	clock.advance(20 * time.Millisecond)
	timer.fire()
	output = decodeServerMessage(t, awaitFrame(t, sends, "Output")).(protocol.Output)
	require.Equal(t, uint64(2), output.Echo)
	require.Equal(t, uint64(2), ac.echoAck.Load())
}

func TestEchoAckIgnoresUnsequencedAndStaleInput(t *testing.T) {
	clock := newEchoTestClock()
	d, _, ac, _ := newManualSessionWithPTYsClock(t, clock, newQuietPTY())
	ac.echoAck.Store(5)

	d.noteInputApplied(ac, 0)
	d.noteInputApplied(ac, 5)
	require.Empty(t, ac.echo.pending)
	require.Empty(t, clock.timers)
}

func TestResetEchoAckDropsPendingInput(t *testing.T) {
	clock := newEchoTestClock()
	d, _, ac, sends := newManualSessionWithPTYsClock(t, clock, newQuietPTY())

	d.noteInputApplied(ac, 7)
	timer, _ := clock.nextTimer(t)
	ac.resetEchoAck()
	clock.advance(echoAckDelay)
	timer.fire()

	require.Zero(t, ac.echoAck.Load())
	require.Empty(t, drainAllFrames(sends), "a reset tracker must not publish the old link's input")
}

func TestEchoAckWaitsWhileOutputWindowIsFull(t *testing.T) {
	clock := newEchoTestClock()
	d, _, ac, sends := newManualSessionWithPTYsClock(t, clock, newQuietPTY())
	ac.output.maxOutstandingAtomic.Store(1)
	ac.output.outstandingAtomic.Store(1)

	d.noteInputApplied(ac, 1)
	timer, _ := clock.nextTimer(t)
	clock.advance(echoAckDelay)
	timer.fire()

	retry, delay := clock.nextTimer(t)
	require.Equal(t, echoAckDelay, delay, "a full window defers the ack")
	require.Equal(t, uint64(1), ac.echoAck.Load(), "the next frame may still carry it")
	require.Empty(t, drainAllFrames(sends), "no ack-only output may overtake withheld frames")

	ac.output.outstandingAtomic.Store(0)
	clock.advance(echoAckDelay)
	retry.fire()
	output := decodeServerMessage(t, awaitFrame(t, sends, "Output")).(protocol.Output)
	require.Equal(t, uint64(1), output.Echo)
	require.Zero(t, output.New)
}
