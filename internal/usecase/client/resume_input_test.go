package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

func TestIsResumeCancelKey(t *testing.T) {
	tests := []struct {
		name string
		data string
		want bool
	}{
		{name: "escape", data: "\x1b", want: true},
		{name: "ctrl+c", data: "\x03", want: true},
		{name: "kitty escape", data: "\x1b[27u", want: true},
		{name: "kitty escape press event", data: "\x1b[27;1:1u", want: true},
		{name: "kitty ctrl+c", data: "\x1b[99;5u", want: true},
		{name: "kitty ctrl+c with base key", data: "\x1b[1089::99;5u", want: true},
		{name: "ctrl+c after typed text", data: "ab\x03", want: true},
		{name: "kitty ctrl+c after typed text", data: "ab\x1b[99;5u", want: true},
		{name: "kitty escape release", data: "\x1b[27;1:3u"},
		{name: "kitty ctrl+shift+c", data: "\x1b[99;6u"},
		{name: "kitty plain c", data: "\x1b[99u"},
		{name: "kitty ctrl+escape", data: "\x1b[27;5u"},
		{name: "arrow key", data: "\x1b[A"},
		{name: "alt+c", data: "\x1bc"},
		{name: "text", data: "abc"},
		{name: "enter", data: "\r"},
		{name: "empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isResumeCancelKey([]byte(tt.data)))
		})
	}
}

// TestTerminalInputPumpResumeWindow pins the supervisor's input ownership
// while it reconnects: every human byte is discarded and only a cancel key
// does anything, and the window closes cleanly.
func TestTerminalInputPumpResumeWindow(t *testing.T) {
	tests := []struct {
		name       string
		inputs     []string
		wantCancel bool
	}{
		{name: "typed text is discarded", inputs: []string{"abc", "\r"}},
		{name: "escape cancels", inputs: []string{"abc", "\x1b"}, wantCancel: true},
		{name: "ctrl+c cancels", inputs: []string{"\x03"}, wantCancel: true},
		{name: "kitty escape cancels", inputs: []string{"\x1b[27u"}, wantCancel: true},
		{name: "kitty ctrl+c cancels", inputs: []string{"\x1b[99;5u"}, wantCancel: true},
		{name: "a lone escape prefix of a sequence does not cancel", inputs: []string{"\x1b[A"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newAttachTestReader()
			pump := newTerminalInputPump(reader)
			pump.start()
			t.Cleanup(func() {
				pump.stop()
				reader.close()
			})
			cancelled := pump.beginResume()
			require.True(t, cancelled == pump.beginResume(), "one window shares one signal")
			for _, input := range tt.inputs {
				reader.push([]byte(input))
			}
			// The pump reads again only after it handled the previous result,
			// so every pushed chunk was discarded or acked by then.
			reader.awaitHandled(t)
			select {
			case <-cancelled:
				require.True(t, tt.wantCancel, "unexpected cancel")
			default:
				require.False(t, tt.wantCancel, "cancel key was not recognised")
			}

			pump.endResume()
			require.False(t, pumpResumeOpen(pump), "the window is closed")
			reader.push([]byte("after"))
			require.Eventually(t, func() bool {
				pump.mu.Lock()
				defer pump.mu.Unlock()
				return pump.pending != nil && string(pump.pending.data) == "after"
			}, 5*time.Second, time.Millisecond, "input reaches the next owner once the window closes")
		})
	}
}

// TestTerminalInputPumpResumeCancelGrace pins that a cancel key repeated right
// after a resume cancel never reaches the picker, whose exit key would quit
// vev, while any other input, the same key once the grace passed, or a key
// meant for another owner is delivered. Every cancel key slides the grace.
func TestTerminalInputPumpResumeCancelGrace(t *testing.T) {
	type step struct {
		key  string
		at   time.Duration // clock reading when the key's read is handled
		read bool          // the pump consults the clock for this read
		drop bool          // the read never reaches an owner
	}
	const (
		ownerNone         = ""
		ownerPicker       = "picker"
		ownerDuringResume = "non-picker claimed during the resume"
		ownerAfterResume  = "non-picker claimed after the resume"
	)
	tests := []struct {
		name string
		// windowCancels are the clock readings of the cancel keys read while
		// the resume window is open; the first one latches the cancel.
		windowCancels []time.Duration
		owner         string
		reopen        bool
		steps         []step
	}{
		{name: "escape inside the grace is dropped", steps: []step{{key: "\x1b", at: resumeCancelGrace - time.Millisecond, read: true, drop: true}}},
		{name: "ctrl+c inside the grace is dropped", steps: []step{{key: "\x03", at: time.Millisecond, read: true, drop: true}}},
		{name: "kitty ctrl+c inside the grace is dropped", steps: []step{{key: "\x1b[99;5u", at: 100 * time.Millisecond, read: true, drop: true}}},
		{name: "kitty escape inside the grace is dropped", steps: []step{{key: "\x1b[27u", at: 100 * time.Millisecond, read: true, drop: true}}},
		{name: "escape at the end of the grace is delivered", steps: []step{{key: "\x1b", at: resumeCancelGrace, read: true}}},
		{name: "ctrl+c after the grace is delivered", steps: []step{{key: "\x03", at: resumeCancelGrace + time.Second, read: true}}},
		{name: "other key inside the grace is delivered", steps: []step{{key: "j", at: time.Millisecond}}},
		{name: "arrow key inside the grace is delivered", steps: []step{{key: "\x1b[A", at: time.Millisecond}}},
		{name: "ctrl+c mixed with typed text is delivered", steps: []step{{key: "j\x03", at: time.Millisecond}}},
		{name: "typed text then escape is delivered", steps: []step{{key: "ab\x1b", at: time.Millisecond}}},
		{name: "kitty ctrl+c mixed with typed text is delivered", steps: []step{{key: "j\x1b[99;5u", at: time.Millisecond}}},
		{name: "two cancel keys in one read are delivered", steps: []step{{key: "\x03\x03", at: time.Millisecond}}},
		{name: "kitty ctrl+c release is delivered", steps: []step{{key: "\x1b[99;5:3u", at: time.Millisecond}}},
		{
			name: "a swallowed cancel slides the grace",
			steps: []step{
				{key: "\x1b", at: 300 * time.Millisecond, read: true, drop: true},
				{key: "\x03", at: 600 * time.Millisecond, read: true, drop: true},
				{key: "\x1b", at: 900 * time.Millisecond, read: true, drop: true},
				{key: "\x1b", at: 900*time.Millisecond + resumeCancelGrace, read: true},
			},
		},
		{
			name: "the slid grace still ends",
			steps: []step{
				{key: "\x1b", at: 300 * time.Millisecond, read: true, drop: true},
				{key: "\x1b", at: 300*time.Millisecond + resumeCancelGrace, read: true},
			},
		},
		{
			name:          "a repeated cancel inside the window slides the grace",
			windowCancels: []time.Duration{0, 300 * time.Millisecond, 600 * time.Millisecond},
			steps:         []step{{key: "\x1b", at: 900 * time.Millisecond, read: true, drop: true}},
		},
		{
			name:   "a new resume window clears the grace",
			reopen: true,
			steps:  []step{{key: "\x1b", at: time.Millisecond}},
		},
		{
			name:  "the picker keeps the grace",
			owner: ownerPicker,
			steps: []step{{key: "\x1b", at: time.Millisecond, read: true, drop: true}},
		},
		{
			name:  "an owner claimed during the resume gets its first cancel",
			owner: ownerDuringResume,
			steps: []step{{key: "\x03", at: time.Millisecond}},
		},
		{
			name:  "an owner claimed after the resume gets its first cancel",
			owner: ownerAfterResume,
			steps: []step{{key: "\x1b", at: time.Millisecond}},
		},
		{
			name:  "an unowned pump keeps the grace",
			owner: ownerNone,
			steps: []step{{key: "\x1b", at: time.Millisecond, read: true, drop: true}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			windowCancels := tt.windowCancels
			if len(windowCancels) == 0 {
				windowCancels = []time.Duration{0}
			}
			clock := portsmocks.NewMockClock(t)
			for _, at := range windowCancels {
				clock.EXPECT().Now().Return(base.Add(at)).Once()
			}
			for _, st := range tt.steps {
				if st.read {
					clock.EXPECT().Now().Return(base.Add(st.at)).Once()
				}
			}

			reader := newAttachTestReader()
			pump := newTerminalInputPump(reader)
			pump.clock = clock
			pump.start()
			t.Cleanup(func() {
				pump.stop()
				reader.close()
			})

			cancelled := pump.beginResume()
			for range windowCancels {
				reader.push([]byte("\x1b"))
				reader.awaitHandled(t)
			}
			select {
			case <-cancelled:
			default:
				require.Fail(t, "the first cancel key must cancel the resume")
			}
			if tt.owner == ownerDuringResume {
				_, ok := pump.tryClaim()
				require.True(t, ok)
			}
			pump.endResume()
			switch tt.owner {
			case ownerPicker:
				_, ok := pump.tryClaimPicker()
				require.True(t, ok)
			case ownerAfterResume:
				_, ok := pump.tryClaim()
				require.True(t, ok)
			}
			if tt.reopen {
				pump.beginResume()
				pump.endResume()
			}

			for i, st := range tt.steps {
				reader.push([]byte(st.key))
				reader.awaitHandled(t)
				pump.mu.Lock()
				pending := pump.pending
				pump.pending = nil
				pump.mu.Unlock()
				if st.drop {
					require.Nil(t, pending, "step %d: the repeated cancel must not reach an owner", i)
					continue
				}
				require.NotNil(t, pending, "step %d: the key must reach the next owner", i)
				require.Equal(t, st.key, string(pending.data), "step %d", i)
				pump.space <- struct{}{}
			}
		})
	}
}

// resumeCancelHarness is one attached supervisor whose attachment was just
// lost, with the open of every resume attempt scripted by the phase.
type resumeCancelHarness struct {
	*attachTestHarness
	picker   *attachTestPicker
	backoff  *supervisorTestTimer
	notices  chan LifecycleNotice
	first    *sessionTestStream
	resumed  chan *sessionTestStream
	entered  chan struct{}
	openCtxs chan context.Context
	phase    resumeCancelPhase
}

type resumeCancelPhase uint8

const (
	// resumeBackoff cancels while the backoff before the first attempt runs.
	resumeBackoff resumeCancelPhase = iota + 1
	// resumeOpening cancels while the broker open of the attempt is blocked.
	resumeOpening
	// resumeHandshake cancels after the stream opened but before the first
	// frame committed.
	resumeHandshake
	// resumeAttaches lets the first attempt attach.
	resumeAttaches
)

func startResumeCancelHarness(t *testing.T, phase resumeCancelPhase) *resumeCancelHarness {
	t.Helper()
	h := &resumeCancelHarness{
		picker:   newAttachTestPicker(),
		notices:  make(chan LifecycleNotice, 16),
		resumed:  make(chan *sessionTestStream, 4),
		entered:  make(chan struct{}, 4),
		openCtxs: make(chan context.Context, 4),
		phase:    phase,
	}
	h.attachTestHarness = startAttachHarnessConfig(t, h.picker, func(cfg *SupervisorConfig) {
		cfg.Jitter = resumeTestJitter
		cfg.NotifyLifecycle = func(notice LifecycleNotice) { h.notices <- notice }
	})
	var opens atomic.Int32
	h.service.setOpenStream(func(ctx context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		if opens.Add(1) == 1 {
			return h.first, nil
		}
		if h.phase == resumeOpening {
			h.openCtxs <- ctx
			h.entered <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		stream := newSessionTestStream()
		h.resumed <- stream
		return stream, nil
	})
	h.first = newSessionTestStream()
	h.picker.commit(sessionTestRequest(false))
	deliverReadyStream(t, h.first)
	awaitAttachedState(t, h.sup)
	h.first.fail(testStreamLoss())
	h.backoff = awaitResumeTimer(t, h.clock, 1)
	return h
}

// reachPhase fires the backoff and advances to where the phase cancels.
func (h *resumeCancelHarness) reachPhase(t *testing.T) (*sessionTestStream, context.Context) {
	t.Helper()
	switch h.phase {
	case resumeOpening:
		h.backoff.fire()
		<-h.entered
		return nil, <-h.openCtxs
	case resumeHandshake, resumeAttaches:
		h.backoff.fire()
		stream := <-h.resumed
		require.Eventually(t, func() bool { return len(stream.messages()) > 0 }, 5*time.Second, time.Millisecond, "the attempt sent its Hello")
		return stream, nil
	}
	return nil, nil
}

func (h *resumeCancelHarness) drainNotices() (kinds []LifecycleNoticeKind) {
	for {
		select {
		case notice := <-h.notices:
			kinds = append(kinds, notice.Kind)
		default:
			return kinds
		}
	}
}

// TestSupervisorResumeCancelKeyReturnsToPicker proves Escape and Ctrl+C,
// raw or kitty-encoded, cancel a reconnect at every stage before the first
// frame commits: they return to the picker with a cancelled notice, cancel the
// in-flight attempt and close its stream, and give the picker its input back.
func TestSupervisorResumeCancelKeyReturnsToPicker(t *testing.T) {
	keys := []struct {
		name string
		data string
	}{
		{name: "escape", data: "\x1b"},
		{name: "ctrl+c", data: "\x03"},
		{name: "kitty escape", data: "\x1b[27u"},
		{name: "kitty ctrl+c", data: "\x1b[99;5u"},
	}
	phases := []struct {
		name  string
		phase resumeCancelPhase
		opens int
	}{
		{name: "during the backoff", phase: resumeBackoff, opens: 1},
		{name: "during a blocked open", phase: resumeOpening, opens: 2},
		{name: "during the handshake", phase: resumeHandshake, opens: 2},
	}
	for _, phase := range phases {
		for _, key := range keys {
			t.Run(phase.name+"/"+key.name, func(t *testing.T) {
				h := startResumeCancelHarness(t, phase.phase)
				require.True(t, h.sup.State().Resuming, "the reconnect presents the cancel hint")
				stream, openCtx := h.reachPhase(t)
				require.True(t, h.sup.State().Resuming)

				h.reader.push([]byte(key.data))
				awaitPickerState(t, h.sup)
				require.Eventually(t, h.picker.owns, 5*time.Second, time.Millisecond, "the picker gets input back")
				require.False(t, h.sup.State().Resuming)
				require.Len(t, h.service.openedRequests(), phase.opens, "a cancelled resume never opens another stream")
				if openCtx != nil {
					require.Error(t, openCtx.Err(), "the in-flight open was cancelled")
				}
				if stream != nil {
					require.Eventually(t, stream.closedNow, 5*time.Second, time.Millisecond, "the in-flight stream was closed")
				}
				require.Equal(t, []LifecycleNoticeKind{LifecycleNoticeResumeCancelled}, h.drainNotices(), "a cancel reports no failure")

				// The picker owns input again: a later key reaches it.
				before := h.picker.consumedCount()
				h.reader.push([]byte("x"))
				require.Eventually(t, func() bool { return h.picker.consumedCount() > before }, 5*time.Second, time.Millisecond)
			})
		}
	}
}

// TestSupervisorResumeDiscardsTypedInput proves keystrokes that are not a
// cancel key are dropped while reconnecting: they neither cancel nor reach
// the session once the resumed attachment commits.
func TestSupervisorResumeDiscardsTypedInput(t *testing.T) {
	tests := []struct {
		name  string
		typed []string
	}{
		{name: "text", typed: []string{"secret"}},
		{name: "enter and arrows", typed: []string{"\r", "\x1b[A", "\x1b[B"}},
		{name: "alt chord", typed: []string{"\x1bc"}},
		{name: "kitty modified keys", typed: []string{"\x1b[27;5u", "\x1b[99;6u"}},
	}
	phases := []struct {
		name  string
		phase resumeCancelPhase
	}{
		{name: "backoff", phase: resumeBackoff},
		{name: "handshake", phase: resumeHandshake},
	}
	for _, phase := range phases {
		for _, tt := range tests {
			t.Run(phase.name+"/"+tt.name, func(t *testing.T) {
				h := startResumeCancelHarness(t, resumeAttaches)
				if phase.phase == resumeBackoff {
					for _, typed := range tt.typed {
						h.reader.push([]byte(typed))
					}
					h.reader.awaitHandled(t)
				}
				resumed, _ := h.reachPhase(t)
				if phase.phase == resumeHandshake {
					for _, typed := range tt.typed {
						h.reader.push([]byte(typed))
					}
					h.reader.awaitHandled(t)
				}
				require.Equal(t, PresentConnecting, h.sup.State().Presentation, "typed keys never cancel")
				deliverReadyStream(t, resumed)
				awaitAttachedState(t, h.sup)
				require.False(t, h.sup.State().Resuming)

				// Input belongs to the session again: it is delivered, and
				// nothing typed during the reconnect came before it.
				h.reader.push([]byte("z"))
				require.Eventually(t, func() bool {
					for _, message := range resumed.messages() {
						if input, ok := message.(protocol.Input); ok && string(input.Data) == "z" {
							return true
						}
					}
					return false
				}, 5*time.Second, time.Millisecond)
				for _, message := range resumed.messages() {
					if input, ok := message.(protocol.Input); ok {
						require.Equal(t, "z", string(input.Data), "keys typed while reconnecting never reach the session")
					}
				}
				require.Zero(t, h.picker.consumedCount(), "nor the picker")
				require.Empty(t, h.drainNotices())
			})
		}
	}
}

// pumpResumeOpen reports whether the pump has a resume input window open.
func pumpResumeOpen(pump *terminalInputPump) bool {
	pump.mu.Lock()
	defer pump.mu.Unlock()
	return pump.resumeActive
}

// resumeWindowClosed proves the supervisor released the resume input window:
// no cancel signal is open on the pump.
func resumeWindowClosed(t *testing.T, sup *Supervisor) {
	t.Helper()
	require.False(t, pumpResumeOpen(sup.attachments.input), "the resume input window is closed")
}

// TestSupervisorResumeExitsCloseInputWindow proves every way the resume loop
// can end other than a cancel key hands input back: the window is closed and
// the next owner receives keys.
func TestSupervisorResumeExitsCloseInputWindow(t *testing.T) {
	tests := []struct {
		name string
		// exit drives the harness, whose attachment was just lost and whose
		// first backoff is armed, to the end of the resume loop. It returns
		// the stream that receives input afterwards, or nil for the picker.
		exit func(t *testing.T, h *resumeCancelHarness) *sessionTestStream
	}{
		{name: "exhausted window returns to the picker", exit: func(t *testing.T, h *resumeCancelHarness) *sessionTestStream {
			advancePickerClock(h.clock, attachmentResumeWindow)
			h.backoff.fire()
			(<-h.resumed).fail(testStreamLoss())
			awaitPickerState(t, h.sup)
			return nil
		}},
		{name: "broker loss leaves the resume", exit: func(t *testing.T, h *resumeCancelHarness) *sessionTestStream {
			h.service.lose(testStreamLoss())
			awaitPickerState(t, h.sup)
			return nil
		}},
		{name: "an overlay swap after the resume attached", exit: func(t *testing.T, h *resumeCancelHarness) *sessionTestStream {
			h.backoff.fire()
			resumed := <-h.resumed
			deliverReadyStream(t, resumed)
			awaitAttachedState(t, h.sup)
			resumed.deliver(navigationOffer(1))
			awaitPresentation(t, h.sup, PresentAttachedPicker)
			other := sessionTestRequest(true)
			other.Target = protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "beta"}
			h.picker.recordOp(pickerOp{commit: true}, "row", other)
			awaitSent(t, resumed, "Detach", isSent[protocol.Detach])
			second := <-h.resumed
			second.deliver(protocol.Welcome{SessionName: "beta"})
			output := sessionTestOutput(1, "\x1b[Hbeta")
			output.Context.Route.Target = other.Target
			second.deliver(output)
			awaitAttachedState(t, h.sup)
			return second
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := startResumeCancelHarness(t, resumeAttaches)
			session := tt.exit(t, h)
			if session == nil {
				// The picker state is presented before the resume loop unwinds;
				// the picker takes input back only after the window closed.
				require.Eventually(t, h.picker.owns, 5*time.Second, time.Millisecond, "the picker gets input back")
				resumeWindowClosed(t, h.sup)
				before := h.picker.consumedCount()
				h.reader.push([]byte("x"))
				require.Eventually(t, func() bool { return h.picker.consumedCount() > before }, 5*time.Second, time.Millisecond)
				return
			}
			resumeWindowClosed(t, h.sup)
			h.reader.push([]byte("x"))
			require.Eventually(t, func() bool {
				for _, message := range session.messages() {
					if input, ok := message.(protocol.Input); ok && string(input.Data) == "x" {
						return true
					}
				}
				return false
			}, 5*time.Second, time.Millisecond, "the session receives input")
		})
	}
}

// TestSupervisorResumeTerminalEOFTerminates proves a terminal EOF during the
// reconnect ends the run instead of resuming.
func TestSupervisorResumeTerminalEOFTerminates(t *testing.T) {
	tests := []struct {
		name  string
		phase resumeCancelPhase
	}{
		{name: "during the backoff", phase: resumeBackoff},
		// A blocked broker open is not interrupted by EOF: it is bounded by the
		// attachment deadline, after which the loop observes the EOF.
		{name: "during the handshake", phase: resumeHandshake},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := startResumeCancelHarness(t, tt.phase)
			stream, _ := h.reachPhase(t)
			h.reader.close()
			require.NoError(t, h.waitRun(t), "an orderly EOF ends the run cleanly")
			require.Equal(t, PresentTerminating, h.sup.State().Presentation)
			if stream != nil {
				require.Eventually(t, stream.closedNow, 5*time.Second, time.Millisecond, "the in-flight stream was closed")
			}
			require.Empty(t, h.drainNotices(), "no cancelled or failure notice")
		})
	}
}
