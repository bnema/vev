package client

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// sentInput joins every Input message a stream carried, in order.
func sentInput(stream *sessionTestStream) string {
	var out strings.Builder
	for _, message := range stream.messages() {
		if input, ok := message.(protocol.Input); ok {
			out.Write(input.Data)
		}
	}
	return out.String()
}

// pushKeys feeds the terminal one byte per read, like a keyboard, and waits
// until the shared reader consumed them all.
func pushKeys(t *testing.T, h *attachTestHarness, keys string) {
	t.Helper()
	for _, b := range []byte(keys) {
		h.reader.push([]byte{b})
	}
	require.Eventually(t, func() bool { return len(h.reader.chunks) == 0 }, 5*time.Second, time.Millisecond)
}

// TestResumeReplaysKeysHeldWhileConnecting types while a lost attachment waits
// for its backoff and while the resume stream opens (including a failed open
// that is retried), then proves the resumed session receives every key, once,
// in order, and the lost attachment none.
func TestResumeReplaysKeysHeldWhileConnecting(t *testing.T) {
	for _, tt := range []struct {
		name        string
		backoffKeys string
		openKeys    string
		failFirst   bool
		retryKeys   string
	}{
		{name: "typed during backoff", backoffKeys: "HELD_ONE"},
		{name: "typed during stream open", openKeys: "HELD_TWO\r"},
		{name: "typed during backoff and open", backoffKeys: "HELD_ONE", openKeys: "HELD_TWO\r"},
		{name: "kept across a failed open", openKeys: "HELD_ONE", failFirst: true, retryKeys: "HELD_TWO\r"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			h := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) { cfg.Jitter = resumeTestJitter })
			first := newSessionTestStream()
			second := newSessionTestStream()
			opened := make(chan int, 4)
			failed := make(chan struct{})
			release := make(chan struct{})
			count := 0
			h.service.setOpenStream(func(ctx context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				count++
				switch {
				case count == 1:
					return first, nil
				case tt.failFirst && count == 2:
					opened <- count
					select {
					case <-failed:
					case <-ctx.Done():
					}
					return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "route down"}
				}
				opened <- count
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return second, nil
			})
			awaitOpen := func() {
				select {
				case <-opened:
				case <-time.After(5 * time.Second):
					t.Fatal("resume did not open")
				}
			}
			picker.commit(sessionTestRequest(false))
			deliverReadyStream(t, first)
			awaitAttachedState(t, h.sup)
			first.fail(testStreamLoss())

			require.Eventually(t, func() bool { return h.sup.State().Presentation == PresentConnecting }, 5*time.Second, time.Millisecond)
			pushKeys(t, h, tt.backoffKeys)
			fireResumeTimerAfter(t, h.clock, 1, func() {})
			awaitOpen()
			pushKeys(t, h, tt.openKeys)
			if tt.failFirst {
				close(failed)
				fireResumeTimerAfter(t, h.clock, 2, func() {})
				awaitOpen()
				pushKeys(t, h, tt.retryKeys)
			}
			close(release)
			deliverReadyStream(t, second)
			awaitAttachedState(t, h.sup)

			want := tt.backoffKeys + tt.openKeys + tt.retryKeys
			require.Eventually(t, func() bool { return sentInput(second) == want }, 5*time.Second, time.Millisecond,
				"resumed session input = %q, want %q", sentInput(second), want)
			require.Empty(t, sentInput(first))
		})
	}
}
