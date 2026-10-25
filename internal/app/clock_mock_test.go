package app

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/daemon"
)

// sandboxClockState is the shared state behind one newSandboxClockMock: time
// only moves when Advance is called, so idle shutdown is exercised without wall
// time. It implements no interface; the mock closures capture it.
type sandboxClockState struct {
	mu     sync.Mutex
	now    time.Time
	order  uint64
	timers map[*sandboxTimerState]struct{}
	// armed receives one signal each time a timer is armed or reset, so a test
	// advances time on the event that makes it matter instead of polling.
	armed chan struct{}
}

// sandboxTimerState is the state behind one sandbox timer mock.
type sandboxTimerState struct {
	clock  *sandboxClockState
	ch     chan time.Time
	when   time.Time
	order  uint64
	active bool
}

// newSandboxClockMock is a deterministic ports.Clock whose timers fire only
// when the returned state is advanced. Options register overrides first, then
// the defaults are registered with Maybe.
func newSandboxClockMock(t *testing.T, opts ...func(*portsmocks.MockClock, *sandboxClockState)) (*portsmocks.MockClock, *sandboxClockState) {
	t.Helper()
	clk := portsmocks.NewMockClock(t)
	state := &sandboxClockState{now: time.Unix(0, 0), timers: make(map[*sandboxTimerState]struct{}), armed: make(chan struct{}, 1)}
	for _, opt := range opts {
		opt(clk, state)
	}
	clk.EXPECT().Now().RunAndReturn(func() time.Time {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.now
	}).Maybe()
	clk.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(d time.Duration) ports.Timer {
		timer := &sandboxTimerState{clock: state, ch: make(chan time.Time, 1)}
		state.mu.Lock()
		defer state.mu.Unlock()
		timer.arm(d)
		return newSandboxTimerMock(t, timer)
	}).Maybe()
	return clk, state
}

// Advance moves time forward and fires every timer that came due.
func (c *sandboxClockState) Advance(d time.Duration) {
	if d < 0 {
		panic("sandboxClockState.Advance requires a non-negative duration")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	due := make([]*sandboxTimerState, 0, len(c.timers))
	for timer := range c.timers {
		if !timer.when.After(c.now) {
			due = append(due, timer)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].when.Equal(due[j].when) {
			return due[i].order < due[j].order
		}
		return due[i].when.Before(due[j].when)
	})
	for _, timer := range due {
		timer.active = false
		delete(c.timers, timer)
		select {
		case timer.ch <- c.now:
		default:
		}
	}
}

func newSandboxTimerMock(t *testing.T, state *sandboxTimerState) *portsmocks.MockTimer {
	t.Helper()
	timer := portsmocks.NewMockTimer(t)
	timer.EXPECT().C().Return((<-chan time.Time)(state.ch)).Maybe()
	timer.EXPECT().Stop().RunAndReturn(func() bool {
		state.clock.mu.Lock()
		defer state.clock.mu.Unlock()
		if !state.active {
			return false
		}
		state.active = false
		delete(state.clock.timers, state)
		return true
	}).Maybe()
	timer.EXPECT().Reset(mock.Anything).RunAndReturn(func(d time.Duration) bool {
		state.clock.mu.Lock()
		defer state.clock.mu.Unlock()
		wasActive := state.active
		if state.active {
			state.active = false
			delete(state.clock.timers, state)
		}
		select {
		case <-state.ch:
		default:
		}
		state.arm(d)
		return wasActive
	}).Maybe()
	return timer
}

func (t *sandboxTimerState) arm(d time.Duration) {
	t.order = t.clock.order
	t.clock.order++
	t.when = t.clock.now.Add(d)
	t.active = true
	t.clock.timers[t] = struct{}{}
	select {
	case t.clock.armed <- struct{}{}:
	default:
	}
}

// lifecycleTimerState is the state behind one manual snapshot-shutdown timer
// mock: the test fires it by hand.
type lifecycleTimerState struct {
	ch      chan time.Time
	stopped atomic.Bool
}

// fire delivers one tick unless the timer was stopped; a tick already pending
// is never blocked on.
func (s *lifecycleTimerState) fire() {
	if s.stopped.Load() {
		return
	}
	select {
	case s.ch <- time.Now():
	default:
	}
}

// newLifecycleTimerMock is a manual ports.Timer that only accepts the shared
// snapshot shutdown timeout.
func newLifecycleTimerMock(t *testing.T) (*portsmocks.MockTimer, *lifecycleTimerState) {
	t.Helper()
	state := &lifecycleTimerState{ch: make(chan time.Time, 1)}
	timer := portsmocks.NewMockTimer(t)
	timer.EXPECT().C().Return((<-chan time.Time)(state.ch)).Maybe()
	timer.EXPECT().Reset(mock.Anything).RunAndReturn(func(delay time.Duration) bool {
		if delay != daemon.SnapshotShutdownTimeout() {
			t.Errorf("snapshot shutdown timer reset with delay %s, want %s", delay, daemon.SnapshotShutdownTimeout())
		}
		state.stopped.Store(false)
		return false
	}).Maybe()
	timer.EXPECT().Stop().RunAndReturn(func() bool { return !state.stopped.Swap(true) }).Maybe()
	return timer, state
}

// lifecycleShutdownState is the state behind one newLifecycleShutdownClock.
type lifecycleShutdownState struct {
	timers chan *lifecycleTimerState
}

// newLifecycleShutdownClock is a real clock whose snapshot-shutdown timer is
// replaced by a manual one, delivered to the test through nextTimer.
func newLifecycleShutdownClock(t *testing.T) (*portsmocks.MockClock, *lifecycleShutdownState) {
	t.Helper()
	base := clock.New()
	state := &lifecycleShutdownState{timers: make(chan *lifecycleTimerState, 4)}
	clk := portsmocks.NewMockClock(t)
	clk.EXPECT().Now().RunAndReturn(base.Now).Maybe()
	clk.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(delay time.Duration) ports.Timer {
		if delay != daemon.SnapshotShutdownTimeout() {
			return base.NewTimer(delay)
		}
		timer, timerState := newLifecycleTimerMock(t)
		select {
		case state.timers <- timerState:
		default:
			t.Errorf("unexpected additional snapshot shutdown timer with delay %s", delay)
			timerState.fire()
		}
		return timer
	}).Maybe()
	return clk, state
}

func (s *lifecycleShutdownState) nextTimer(t *testing.T) *lifecycleTimerState {
	t.Helper()
	select {
	case timer := <-s.timers:
		return timer
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for shutdown deadline timer")
		return nil
	}
}

// idleSandboxClock is a sandbox clock mock no test advances.
func idleSandboxClock(t *testing.T) *portsmocks.MockClock {
	t.Helper()
	clk, _ := newSandboxClockMock(t)
	return clk
}
