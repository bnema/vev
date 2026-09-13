package quicnettest

import (
	"testing"
	"time"
)

func TestManualClockFiresTimersInDeadlineOrder(t *testing.T) {
	start := time.Unix(0, 0)
	clock := NewManualClock(start)
	late := clock.NewTimer(10 * time.Millisecond)
	early := clock.NewTimer(5 * time.Millisecond)

	clock.Advance(4 * time.Millisecond)
	expectNoFire(t, early)
	expectNoFire(t, late)

	clock.Advance(time.Millisecond)
	expectFire(t, early, start.Add(5*time.Millisecond))
	expectNoFire(t, late)

	clock.Advance(5 * time.Millisecond)
	expectFire(t, late, start.Add(10*time.Millisecond))
}

func TestManualClockFiresEqualDeadlinesInCreationOrder(t *testing.T) {
	start := time.Unix(0, 0)
	clock := NewManualClock(start)
	first := clock.NewTimer(5 * time.Millisecond)
	second := clock.NewTimer(5 * time.Millisecond)

	clock.Advance(5 * time.Millisecond)
	expectFire(t, first, start.Add(5*time.Millisecond))
	expectFire(t, second, start.Add(5*time.Millisecond))
}

func TestManualClockStopPreventsFire(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	timer := clock.NewTimer(5 * time.Millisecond)
	if !timer.Stop() {
		t.Fatal("first Stop = false, want true")
	}
	clock.Advance(10 * time.Millisecond)
	expectNoFire(t, timer)
	if timer.Stop() {
		t.Fatal("second Stop = true, want false")
	}
}

func TestManualClockResetRearmsAbsoluteDeadline(t *testing.T) {
	start := time.Unix(0, 0)
	clock := NewManualClock(start)
	timer := clock.NewTimer(5 * time.Millisecond)
	clock.Advance(5 * time.Millisecond)
	expectFire(t, timer, start.Add(5*time.Millisecond))

	timer.ResetAt(start.Add(8 * time.Millisecond))
	expectNoFire(t, timer)
	clock.Advance(3 * time.Millisecond)
	expectFire(t, timer, start.Add(8*time.Millisecond))
}

func TestManualClockPastDeadlineFiresImmediately(t *testing.T) {
	start := time.Unix(0, 0)
	clock := NewManualClock(start)
	timer := clock.NewTimer(0)
	expectFire(t, timer, start)
}

func TestManualClockAdvancePanicsOnNegativeDuration(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	defer func() {
		if recover() == nil {
			t.Fatal("Advance(-1) did not panic")
		}
	}()
	clock.Advance(-time.Nanosecond)
}

func expectFire(t *testing.T, timer Timer, want time.Time) {
	t.Helper()
	select {
	case got := <-timer.C():
		if !got.Equal(want) {
			t.Fatalf("timer fired at %s, want %s", got, want)
		}
	default:
		t.Fatalf("timer did not fire, want %s", want)
	}
}

func expectNoFire(t *testing.T, timer Timer) {
	t.Helper()
	select {
	case got := <-timer.C():
		t.Fatalf("timer fired at %s, want no fire", got)
	default:
	}
}
