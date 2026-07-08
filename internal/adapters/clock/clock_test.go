package clock

import (
	"testing"
	"time"
)

func TestRealClockNow(t *testing.T) {
	c := New()
	before := time.Now().Add(-time.Second)
	got := c.Now()
	after := time.Now().Add(time.Second)

	if got.Before(before) || got.After(after) {
		t.Fatalf("Now() = %v, want between %v and %v", got, before, after)
	}
}

func TestRealClockTimerFires(t *testing.T) {
	c := New()
	timer := c.NewTimer(10 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C():
	case <-time.After(5 * time.Second):
		t.Fatal("timer did not fire before deadline")
	}
}

func TestRealClockTimerStopKeepsChannelQuiet(t *testing.T) {
	c := New()
	timer := c.NewTimer(time.Hour)
	_ = timer.Stop()

	select {
	case <-timer.C():
		t.Fatal("stopped timer fired")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestRealClockTimerResetRearms(t *testing.T) {
	c := New()
	timer := c.NewTimer(time.Hour)
	defer timer.Stop()

	_ = timer.Stop()
	timer.Reset(10 * time.Millisecond)

	select {
	case <-timer.C():
	case <-time.After(5 * time.Second):
		t.Fatal("reset timer did not fire before deadline")
	}
}
