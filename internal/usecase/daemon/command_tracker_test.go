package daemon

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

type commandTrackerTestTimer struct {
	ch   chan time.Time
	once sync.Once
}

func (t *commandTrackerTestTimer) C() <-chan time.Time      { return t.ch }
func (t *commandTrackerTestTimer) Reset(time.Duration) bool { return false }
func (t *commandTrackerTestTimer) Stop() bool               { return true }
func (t *commandTrackerTestTimer) fire()                    { t.once.Do(func() { t.ch <- time.Time{} }) }

type commandTrackerTestClock struct {
	timers chan *commandTrackerTestTimer
	delay  time.Duration
}

func newCommandTrackerTestClock() *commandTrackerTestClock {
	return &commandTrackerTestClock{timers: make(chan *commandTrackerTestTimer, 8)}
}

func (c *commandTrackerTestClock) Now() time.Time { return time.Time{} }

func (c *commandTrackerTestClock) NewTimer(delay time.Duration) ports.Timer {
	c.delay = delay
	timer := &commandTrackerTestTimer{ch: make(chan time.Time, 1)}
	c.timers <- timer
	return timer
}

func TestCommandRequestTrackerTimeoutAndGenerationIsolation(t *testing.T) {
	tracker := NewCommandRequestTracker()
	clock := newCommandTrackerTestClock()
	requestID, outcome := tracker.Publish(4)
	waitDone := make(chan error, 1)
	go func() {
		_, err := tracker.Wait(context.Background(), clock, requestID, 4, outcome)
		waitDone <- err
	}()

	timer := <-clock.timers
	require.Equal(t, CommandRequestTimeout, clock.delay)
	timer.fire()
	require.ErrorIs(t, <-waitDone, ErrCommandRequestTimeout)
	require.Zero(t, tracker.PendingCount())

	// Reuse the timed-out wire ID with a new generation. A late result from the
	// old connection must not complete the new request.
	secondOutcome, ok := tracker.Track(requestID, 8)
	require.True(t, ok)
	tracker.Complete(4, ports.CommandResult{RequestID: requestID, OK: true})
	select {
	case <-secondOutcome:
		t.Fatal("late result from the old generation completed a newer request")
	default:
	}
	tracker.Complete(8, ports.CommandResult{RequestID: requestID, OK: true})
	result := <-secondOutcome
	require.True(t, result.Result.OK)
}
