package client

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// sentInput joins every Input message a stream carried, in order.
func sentInput(stream *streamLog) string {
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
	synctest.Wait()
	require.True(t, len(h.reader.chunks) == 0)
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
			synctest.Test(t, func(t *testing.T) {
				pickerMock, picker := newAttachTestPicker(t)
				h := startAttachHarnessConfig(t, pickerMock, func(cfg *SupervisorConfig) { cfg.Jitter = resumeTestJitter })
				first := newSessionStream(t)
				second := newSessionStream(t)
				opened := make(chan int, 4)
				failed := make(chan struct{})
				release := make(chan struct{})
				count := 0
				h.service.setOpenStream(func(ctx context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
					count++
					switch {
					case count == 1:
						return first.conn, nil
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
					return second.conn, nil
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

				synctest.Wait()
				require.True(t, h.sup.State().Presentation == PresentConnecting)
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
				synctest.Wait()
				require.True(t, sentInput(second) == want, "resumed session input = %q, want %q", sentInput(second), want)
				require.Empty(t, sentInput(first))
			})
		})
	}
}
