package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
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
	tracker.Complete(4, protocol.CommandResult{RequestID: requestID, OK: true})
	select {
	case <-secondOutcome:
		t.Fatal("late result from the old generation completed a newer request")
	default:
	}
	tracker.Complete(8, protocol.CommandResult{RequestID: requestID, OK: true})
	result := <-secondOutcome
	require.True(t, result.Result.OK)

	t.Run("wait outcomes", func(t *testing.T) {
		failure := errors.New("transport failed")
		tests := []struct {
			name        string
			setup       func(*testing.T, *CommandRequestTracker, uint64) (context.Context, ports.Clock, func())
			wantErr     error
			wantPending int
		}{
			{
				name: "canceled context",
				setup: func(_ *testing.T, _ *CommandRequestTracker, _ uint64) (context.Context, ports.Clock, func()) {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx, newCommandTrackerTestClock(), nil
				},
				wantErr:     context.Canceled,
				wantPending: 0,
			},
			{
				name: "nil clock",
				setup: func(_ *testing.T, _ *CommandRequestTracker, _ uint64) (context.Context, ports.Clock, func()) {
					return context.Background(), nil, nil
				},
				wantErr:     ErrCommandRequestUnavailable,
				wantPending: 1,
			},
			{
				name: "fail",
				setup: func(_ *testing.T, tracker *CommandRequestTracker, requestID uint64) (context.Context, ports.Clock, func()) {
					tracker.Fail(requestID, 1, failure)
					return context.Background(), newCommandTrackerTestClock(), nil
				},
				wantErr:     failure,
				wantPending: 0,
			},
			{
				name: "remove",
				setup: func(_ *testing.T, tracker *CommandRequestTracker, requestID uint64) (context.Context, ports.Clock, func()) {
					tracker.Remove(requestID, 1)
					clock := newCommandTrackerTestClock()
					return context.Background(), clock, func() {
						timer := <-clock.timers
						timer.fire()
					}
				},
				wantErr:     ErrCommandRequestTimeout,
				wantPending: 0,
			},
			{
				name: "duplicate track",
				setup: func(t *testing.T, tracker *CommandRequestTracker, requestID uint64) (context.Context, ports.Clock, func()) {
					duplicate, ok := tracker.Track(requestID, 1)
					require.False(t, ok)
					require.Nil(t, duplicate)
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx, newCommandTrackerTestClock(), nil
				},
				wantErr:     context.Canceled,
				wantPending: 0,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tracker := NewCommandRequestTracker()
				requestID, outcome := tracker.Publish(1)
				ctx, clock, drive := tt.setup(t, tracker, requestID)
				waitDone := make(chan error, 1)
				go func() {
					_, err := tracker.Wait(ctx, clock, requestID, 1, outcome)
					waitDone <- err
				}()
				if drive != nil {
					drive()
				}
				require.ErrorIs(t, <-waitDone, tt.wantErr)
				require.Equal(t, tt.wantPending, tracker.PendingCount())
			})
		}
	})
}
