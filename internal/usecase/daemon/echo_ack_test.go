package daemon

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

// echoTestClock is a settable clock whose timers fire only when the test says.
// It embeds a generated MockClock backed by the recorded state.
type echoTestClock struct {
	*portsmocks.MockClock
	mu     sync.Mutex
	now    time.Time
	timers chan echoTestTimer
}

// echoManualTimer wraps a generated MockTimer with a test-fired channel.
type echoManualTimer struct {
	*portsmocks.MockTimer
	ch chan time.Time
}

func (t *echoManualTimer) fire() { t.ch <- time.Time{} }

type echoTestTimer struct {
	timer *echoManualTimer
	delay time.Duration
}

func newEchoTestClock(tb testing.TB) *echoTestClock {
	tb.Helper()
	c := &echoTestClock{MockClock: portsmocks.NewMockClock(tb), now: time.Unix(100, 0), timers: make(chan echoTestTimer, 16)}
	c.EXPECT().Now().RunAndReturn(func() time.Time {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.now
	}).Maybe()
	c.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(d time.Duration) ports.Timer {
		timer := &echoManualTimer{MockTimer: &portsmocks.MockTimer{}, ch: make(chan time.Time, 1)}
		timer.EXPECT().C().Return((<-chan time.Time)(timer.ch)).Maybe()
		timer.EXPECT().Reset(mock.Anything).Return(true).Maybe()
		timer.EXPECT().Stop().Return(true).Maybe()
		c.timers <- echoTestTimer{timer: timer, delay: d}
		return timer
	}).Maybe()
	return c
}

func (c *echoTestClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// nextTimer returns the next echo timer once the bubble is quiescent, skipping
// the send-bound timers the ack-only output path creates. It must run inside a
// synctest bubble.
func (c *echoTestClock) nextTimer(t *testing.T) (*echoManualTimer, time.Duration) {
	t.Helper()
	synctest.Wait()
	for {
		select {
		case next := <-c.timers:
			if next.delay <= echoAckDelay {
				return next.timer, next.delay
			}
		default:
			t.Fatal("no echo timer was armed")
			return nil, 0
		}
	}
}

func TestEchoAckWaitsForEchoTimeoutAndSendsAckOnlyOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newEchoTestClock(t)
		d, sess, ac, sends := newManualSessionWithPTYsClock(t, clock, newQuietPTY(t))
		stopOverlayTimersAtCleanup(t, ac)

		// Drive the production input path so the acknowledgement wiring is
		// covered, not just the tracker.
		d.handleSequencedInput(sess, ac, 1, []byte("a"))
		clock.advance(20 * time.Millisecond)
		d.handleSequencedInput(sess, ac, 2, []byte("b"))
		timer, delay := clock.nextTimer(t)
		require.Equal(t, echoAckDelay, delay)
		require.Zero(t, ac.echoAck.Load(), "input is not echoed when applied")

		clock.advance(30 * time.Millisecond)
		timer.fire()
		output := decodeServerMessage(t, requireFrame(t, sends, "Output")).(protocol.Output)
		require.Equal(t, uint64(1), output.Echo)
		require.Zero(t, output.New, "an ack-only output is a side effect")
		require.Empty(t, output.Data)

		// Input 2 is still younger than the timeout: the tracker re-arms for its
		// remaining age.
		timer, delay = clock.nextTimer(t)
		require.Equal(t, 20*time.Millisecond, delay)
		clock.advance(20 * time.Millisecond)
		timer.fire()
		output = decodeServerMessage(t, requireFrame(t, sends, "Output")).(protocol.Output)
		require.Equal(t, uint64(2), output.Echo)
		require.Equal(t, uint64(2), ac.echoAck.Load())
	})
}

func TestEchoAckIgnoresUnsequencedAndStaleInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newEchoTestClock(t)
		d, _, ac, _ := newManualSessionWithPTYsClock(t, clock, newQuietPTY(t))
		stopOverlayTimersAtCleanup(t, ac)
		ac.echoAck.Store(5)

		d.noteInputApplied(ac, 0)
		d.noteInputApplied(ac, 5)
		require.Empty(t, ac.echo.pending)
		require.Empty(t, clock.timers)
	})
}

func TestResetEchoAckDropsPendingInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newEchoTestClock(t)
		d, _, ac, sends := newManualSessionWithPTYsClock(t, clock, newQuietPTY(t))
		stopOverlayTimersAtCleanup(t, ac)

		d.noteInputApplied(ac, 7)
		timer, _ := clock.nextTimer(t)
		ac.resetEchoAck()
		clock.advance(echoAckDelay)
		timer.fire()

		require.Zero(t, ac.echoAck.Load())
		require.Empty(t, drainAllFrames(sends), "a reset tracker must not publish the old link's input")
	})
}

func TestEchoAckWaitsWhileOutputWindowIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newEchoTestClock(t)
		d, _, ac, sends := newManualSessionWithPTYsClock(t, clock, newQuietPTY(t))
		stopOverlayTimersAtCleanup(t, ac)
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
		output := decodeServerMessage(t, requireFrame(t, sends, "Output")).(protocol.Output)
		require.Equal(t, uint64(1), output.Echo)
		require.Zero(t, output.New)
	})
}
