package client

import (
	"context"
	"testing"
	"testing/synctest"
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
			synctest.Test(t, func(t *testing.T) {
				pickerMock, picker := newAttachTestPicker(t)
				h := startAttachHarnessConfig(t, pickerMock, func(cfg *SupervisorConfig) { cfg.Jitter = resumeTestJitter })
				first := newSessionStream(t)
				opened := make(chan struct{})
				cancelled := make(chan struct{})
				count := 0
				h.service.setOpenStream(func(ctx context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
					count++
					if count == 1 {
						return first.conn, nil
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
				synctest.Wait()
				require.True(t, picker.owns())
				require.Equal(t, 2, len(h.service.openedRequests()))
			})
		})
	}
}

// TestResumeHeldKeysNeverReachThePicker types while a lost attachment
// resumes, then returns to the picker either by cancelling or because the
// resume fails for good, and proves the picker never receives the held keys
// while it still receives keys typed after it took the terminal back.
func TestResumeHeldKeysNeverReachThePicker(t *testing.T) {
	for _, tt := range []struct {
		name   string
		cancel bool
	}{
		{name: "cancelled", cancel: true},
		{name: "resume refused"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				assertHeldKeysDroppedAtPicker(t, tt.cancel)
			})
		})
	}
}

func assertHeldKeysDroppedAtPicker(t *testing.T, cancel bool) {
	pickerMock, picker := newAttachTestPicker(t)
	h := startAttachHarnessConfig(t, pickerMock, func(cfg *SupervisorConfig) { cfg.Jitter = resumeTestJitter })
	first := newSessionStream(t)
	opened := make(chan struct{}, 1)
	refuse := make(chan struct{})
	count := 0
	h.service.setOpenStream(func(ctx context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		count++
		if count == 1 {
			return first.conn, nil
		}
		opened <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-refuse:
			return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Text: "refused"}
		}
	})
	picker.commit(sessionTestRequest(false))
	deliverReadyStream(t, first)
	awaitAttachedState(t, h.sup)
	first.fail(testStreamLoss())
	synctest.Wait()
	require.True(t, h.sup.State().Presentation == PresentConnecting)
	pushKeys(t, h, "HELD\r")
	if cancel {
		h.reader.push([]byte("\x03"))
	} else {
		fireResumeTimerAfter(t, h.clock, 1, func() {})
		synctest.Wait()
		select {
		case <-opened:
		default:
			t.Fatal("resume did not open")
		}
		close(refuse)
	}
	synctest.Wait()
	require.True(t, picker.owns())
	h.reader.push([]byte("j"))
	synctest.Wait()
	require.True(t, func() bool {
		picker.mu.Lock()
		defer picker.mu.Unlock()
		return len(picker.consumedBytes) > 0
	}())
	picker.mu.Lock()
	defer picker.mu.Unlock()
	require.Equal(t, "j", string(picker.consumedBytes), "held resume keys reached the picker")
}
