package quicnettest

import (
	"sort"
	"sync"
	"time"
)

// Clock abstracts time so simulations can be driven by a ManualClock and stay
// reproducible without sleeping.
type Clock interface {
	// Now reports the current time.
	Now() time.Time
	// NewTimer returns a stopped-by-default timer. Callers must Stop or Reset
	// it before selecting on C.
	NewTimer(d time.Duration) Timer
}

// Timer is the subset of *time.Timer the proxy scheduler needs. Deadlines are
// absolute so that a stale duration computed before a clock jump cannot delay
// delivery.
type Timer interface {
	// C is the channel the timer fires on.
	C() <-chan time.Time
	// Stop deactivates the timer, reporting whether it was active.
	Stop() bool
	// ResetAt re-arms the timer at the absolute deadline, draining a pending
	// value first. It reports whether the timer was active before the call.
	ResetAt(deadline time.Time) bool
}

// SystemClock is a Clock backed by wall time.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// NewTimer implements Clock.
func (SystemClock) NewTimer(d time.Duration) Timer {
	return &systemTimer{timer: time.NewTimer(d)}
}

type systemTimer struct{ timer *time.Timer }

func (t *systemTimer) C() <-chan time.Time { return t.timer.C }

func (t *systemTimer) Stop() bool { return t.timer.Stop() }

func (t *systemTimer) ResetAt(deadline time.Time) bool {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	return t.timer.Reset(time.Until(deadline))
}

// ManualClock is a deterministic Clock. Time only moves when Advance is called,
// so impairment scheduling can be asserted exactly instead of waiting on wall
// time.
type ManualClock struct {
	mu     sync.Mutex
	now    time.Time
	order  uint64
	timers map[*manualTimer]struct{}
}

// NewManualClock returns a ManualClock positioned at start.
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start, timers: make(map[*manualTimer]struct{})}
}

// Now implements Clock.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer implements Clock.
func (c *ManualClock) NewTimer(d time.Duration) Timer {
	t := &manualTimer{clock: c, ch: make(chan time.Time, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	t.arm(d)
	return t
}

// Advance moves time forward by d and fires every timer that has come due.
// Timers with equal deadlines fire in creation order, keeping the scheduler
// deterministic. Advance panics when d is negative, because a manual clock
// cannot un-fire timers.
func (c *ManualClock) Advance(d time.Duration) {
	if d < 0 {
		panic("quicnettest: ManualClock.Advance requires a non-negative duration")
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	due := make([]*manualTimer, 0, len(c.timers))
	for timer := range c.timers {
		if !timer.when.After(now) {
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
		case timer.ch <- now:
		default:
		}
	}
	c.mu.Unlock()
}

type manualTimer struct {
	clock  *ManualClock
	ch     chan time.Time
	when   time.Time
	order  uint64
	active bool
}

// arm schedules the timer relative to the clock's current time. The caller must
// hold clock.mu.
func (t *manualTimer) arm(d time.Duration) {
	t.when = t.clock.now.Add(d)
	t.order = t.clock.order
	t.clock.order++
	if d <= 0 {
		t.active = false
		select {
		case t.ch <- t.clock.now:
		default:
		}
		return
	}
	t.active = true
	t.clock.timers[t] = struct{}{}
}

func (t *manualTimer) C() <-chan time.Time { return t.ch }

func (t *manualTimer) Stop() bool {
	c := t.clock
	c.mu.Lock()
	defer c.mu.Unlock()
	if !t.active {
		return false
	}
	t.active = false
	delete(c.timers, t)
	return true
}

func (t *manualTimer) ResetAt(deadline time.Time) bool {
	c := t.clock
	c.mu.Lock()
	defer c.mu.Unlock()
	wasActive := t.active
	if t.active {
		t.active = false
		delete(c.timers, t)
	}
	select {
	case <-t.ch:
	default:
	}
	t.arm(deadline.Sub(c.now))
	return wasActive
}
