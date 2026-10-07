package client

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestResumeCancelDuringStreamOpen(t *testing.T) {
	for _, tt := range []struct {
		name, input string
		escape      bool
	}{
		{name: "ctrl-c", input: "\x03"},
		{name: "kitty escape", input: "\x1b[27u"},
		{name: "legacy escape", input: "\x1b", escape: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			h := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) { cfg.Jitter = resumeTestJitter })
			first := newSessionTestStream()
			opened := make(chan struct{})
			cancelled := make(chan struct{})
			count := 0
			h.service.setOpenStream(func(ctx context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				count++
				if count == 1 {
					return first, nil
				}
				close(opened)
				<-ctx.Done()
				close(cancelled)
				return nil, ctx.Err()
			})
			picker.commit(sessionTestRequest(false))
			deliverReadyStream(t, first)
			awaitAttachedState(t, h.sup)
			first.fail(testStreamLoss())
			fireResumeTimerAfter(t, h.clock, 1, func() {})
			select {
			case <-opened:
			case <-time.After(5 * time.Second):
				t.Fatal("resume did not open")
			}
			h.reader.push([]byte(tt.input))
			if tt.escape {
				for {
					timer := h.clock.awaitTimer(t)
					if timer.delay == 50*time.Millisecond {
						timer.fire()
						break
					}
				}
			}
			select {
			case <-cancelled:
			case <-time.After(5 * time.Second):
				t.Fatal("cancel did not reach open context")
			}
			require.Eventually(t, picker.owns, 5*time.Second, time.Millisecond)
			require.Equal(t, 2, len(h.service.openedRequests()))
		})
	}
}
