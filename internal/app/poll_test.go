package app

import "time"

// pollInterval is how often pollUntil re-evaluates its condition.
const pollInterval = 5 * time.Millisecond

// pollUntil is the package's single polling helper. It is reserved for tests
// that cross a real process, socket or filesystem boundary (they run outside
// synctest) and observe state that exposes no event source to wait on: a unix
// socket or record file appearing on disk, a process disappearing, or a daemon
// registry reached only through a listing request. Anything with a channel, a
// mock closure or a clock is waited on through that event instead, and
// in-memory tests run inside synctest.Test with synctest.Wait.
//
// It evaluates cond immediately and then on every tick, and reports whether
// cond held before timeout elapsed. Callers assert the result with a message.
func pollUntil(timeout time.Duration, cond func() bool) bool {
	if cond() {
		return true
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if cond() {
				return true
			}
		case <-deadline.C:
			return cond()
		}
	}
}
