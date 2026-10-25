package ipc

import (
	"bytes"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// pollUntil is the real-I/O exception for state without a notification source.
// A single failure guard bounds the wait; the ticker only schedules observations.
func pollUntil(t testing.TB, timeout time.Duration, cond func() bool) {
	t.Helper()
	guard := time.After(timeout)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !cond() {
		select {
		case <-guard:
			t.Fatalf("real-I/O condition did not settle within %s", timeout)
		case <-ticker.C:
		}
	}
}

// waitSendParked blocks until a goroutine is parked in the framer's synchronous
// Send, which can only be waiting on egress admission when the queue is full.
// That is the only observable sign of the wait. It cannot run in a synctest
// bubble because streamframe pools result channels across bubbles.
func waitSendParked(t testing.TB, timeout time.Duration) {
	t.Helper()
	pollUntil(t, timeout, func() bool {
		var buf bytes.Buffer
		if err := pprof.Lookup("goroutine").WriteTo(&buf, 2); err != nil {
			t.Fatalf("goroutine profile: %v", err)
		}
		return strings.Contains(buf.String(), "streamframe.(*Framer).Send")
	})
}
