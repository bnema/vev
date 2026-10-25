package quic

import (
	"testing"
	"time"
)

// pollUntil is the real-I/O exception for OS state without a notification source.
// A single failure guard bounds the wait; the ticker only schedules observations.
//
//testpolicy:realtime real QUIC over UDP
func pollUntil(t testing.TB, timeout time.Duration, cond func() bool) {
	t.Helper()
	guard := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !cond() {
		select {
		case <-guard:
			t.Fatalf("real-I/O condition did not settle within %s", timeout)
		case <-ticker.C:
		}
	}
}
