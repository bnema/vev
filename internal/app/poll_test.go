package app

import (
	"testing"
	"time"
)

// pollInterval is how often pollUntil re-evaluates its condition.
const pollInterval = 5 * time.Millisecond

// pollUntil is the package's single polling helper. It is reserved for tests
// that cross a real process, socket or filesystem boundary (they run outside
// synctest) and observe state that exposes no event source to wait on: a unix
// socket appearing on disk, a subprocess record file, or a daemon registry
// reached only through a listing request. Anything with a channel, a mock
// closure or a clock is waited on through that event instead, and in-memory
// tests run inside synctest.Test with synctest.Wait.
//
// It evaluates cond immediately, then on every tick, and fails the test with
// the formatted message once timeout elapses.
func pollUntil(t testing.TB, timeout time.Duration, cond func() bool, format string, args ...any) {
	t.Helper()
	if cond() {
		return
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if cond() {
				return
			}
		case <-deadline.C:
			if cond() {
				return
			}
			t.Fatalf(format, args...)
		}
	}
}
