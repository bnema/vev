package client

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Picker overlay and move picker tests (Plan 003 A1/A2). They drive the real
// supervisor, host, foreground, and worker over the in-package broker fakes
// with the scripted test clock: no sleeps, and every wait is bounded.

// recordOp records one picker decision on the scripted picker, exactly like
// the controller's coalesced wake.
func (p *attachTestPicker) recordOp(op pickerOp, key string, request ports.BrokerOpenStreamRequest) {
	p.mu.Lock()
	p.op = op
	p.key = key
	p.request = request
	p.mu.Unlock()
	select {
	case p.opsReady <- struct{}{}:
	default:
	}
}

// opTaken reports whether the supervisor consumed the recorded decision.
func (p *attachTestPicker) opTaken() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.op == (pickerOp{})
}

// attachLiveSession commits one attachment on the harness and returns its
// stream once the attachment is presented.
func attachLiveSession(t *testing.T, harness *attachTestHarness, picker *attachTestPicker) *sessionTestStream {
	t.Helper()
	stream := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})
	picker.commit(sessionTestRequest(true))
	awaitStreamHello(t, &sync.Mutex{}, &stream)
	deliverReadyStream(t, stream)
	awaitAttachedState(t, harness.sup)
	return stream
}

func awaitPresentation(t *testing.T, sup *Supervisor, want Presentation) {
	t.Helper()
	require.Eventually(t, func() bool { return sup.State().Presentation == want }, 5*time.Second, time.Millisecond, "presentation never became %s", want)
}

// awaitSent waits for the first client message matching match.
func awaitSent(t *testing.T, stream *sessionTestStream, what string, match func(protocol.ClientMessage) bool) protocol.ClientMessage {
	t.Helper()
	var found protocol.ClientMessage
	require.Eventually(t, func() bool {
		for _, message := range stream.messages() {
			if match(message) {
				found = message
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "client never sent %s", what)
	return found
}

func countSent[T protocol.ClientMessage](stream *sessionTestStream) int {
	count := 0
	for _, message := range stream.messages() {
		if _, ok := message.(T); ok {
			count++
		}
	}
	return count
}

func isSent[T protocol.ClientMessage](message protocol.ClientMessage) bool {
	_, ok := message.(T)
	return ok
}

// laterOutput is a full frame on a later epoch, which the attachment accepts
// after its initial publication.
func laterOutput(epoch, publication uint64, data string) protocol.Output {
	output := sessionTestOutput(publication, data)
	output.Epoch = epoch
	return output
}

func navigationOffer(id uint64) protocol.PickerOffer {
	return protocol.PickerOffer{InteractionID: id, Intent: protocol.PickerIntentNavigation, Title: " Sessions · recent "}
}

// TestPickerOverlayReturnsToLiveAttachment pins PA1: SSP composes the picker
// over the live attachment, its input and output stay with the picker, and
// every cancel (and a commit to the attached session) returns to the very same
// attachment without reconnecting, then asks for an authoritative repaint.
func TestPickerOverlayReturnsToLiveAttachment(t *testing.T) {
	tests := []struct {
		name string
		op   pickerOp
		key  string
	}{
		{name: "escape or q cancels", op: pickerOp{close: true}},
		{name: "ctrl+c cancels without exiting", op: pickerOp{close: true, exit: true}},
		{name: "commit to the attached session closes", op: pickerOp{commit: true}, key: "row"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			harness := startAttachHarness(t, picker)
			stream := attachLiveSession(t, harness, picker)
			foreground := harness.sup.attachments.authority.foreground()
			require.NotNil(t, foreground)
			require.False(t, picker.owns())

			stream.deliver(navigationOffer(1))
			awaitPresentation(t, harness.sup, PresentAttachedPicker)
			require.True(t, picker.owns(), "the picker owns input while it is open")

			// Input belongs to the picker: it is consumed, never sent as session
			// Input.
			harness.reader.push([]byte("j"))
			require.Eventually(t, func() bool { return picker.consumedCount() == 1 }, 5*time.Second, time.Millisecond)
			require.Zero(t, countSent[protocol.Input](stream), "picker input must never reach the session")

			// Attachment output is applied and acknowledged, never written over
			// the picker.
			acks := countSent[protocol.Ack](stream)
			stream.deliver(laterOutput(2, 2, "hidden-frame"))
			require.Eventually(t, func() bool { return countSent[protocol.Ack](stream) > acks }, 5*time.Second, time.Millisecond)
			require.NotContains(t, harness.terminal.written(), "hidden-frame")

			picker.recordOp(tt.op, tt.key, sessionTestRequest(true))
			awaitPresentation(t, harness.sup, PresentAttached)
			awaitSent(t, stream, "OutputResetRequest", isSent[protocol.OutputResetRequest])

			require.Same(t, foreground, harness.sup.attachments.authority.foreground(), "cancel must keep the same live attachment")
			require.False(t, stream.closedNow())
			require.Len(t, harness.service.openedRequests(), 1, "cancel must not reconnect")
			require.Zero(t, countSent[protocol.Detach](stream))
			require.False(t, picker.owns())

			// The session owns input again.
			harness.reader.push([]byte("z"))
			input := awaitSent(t, stream, "session Input", isSent[protocol.Input]).(protocol.Input)
			require.Equal(t, []byte("z"), input.Data)
			select {
			case <-harness.runDone:
				t.Fatal("cancelling the overlay ended the process")
			default:
			}
		})
	}
}

// TestPickerOverlayCommitElsewhereSwapsAttachment pins the swap: a commit to
// another session detaches the live attachment cleanly and attaches the
// committed target through Connecting, without passing through the plain
// picker.
func TestPickerOverlayCommitElsewhereSwapsAttachment(t *testing.T) {
	picker := newAttachTestPicker()
	notices := make(chan LifecycleNotice, 8)
	harness := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) {
		cfg.NotifyLifecycle = func(notice LifecycleNotice) { notices <- notice }
	})
	first := attachLiveSession(t, harness, picker)
	first.deliver(navigationOffer(1))
	awaitPresentation(t, harness.sup, PresentAttachedPicker)

	var mu sync.Mutex
	second := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return second, nil
	})
	other := sessionTestRequest(true)
	other.Target = protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "beta"}
	picker.recordOp(pickerOp{commit: true}, "row", other)

	awaitSent(t, first, "Detach", isSent[protocol.Detach])
	awaitStreamHello(t, &mu, &second)
	second.deliver(protocol.Welcome{SessionName: "beta"})
	output := sessionTestOutput(1, "\x1b[Hbeta")
	output.Context.Route.Target = other.Target
	second.deliver(output)
	awaitAttachedState(t, harness.sup)

	opened := harness.service.openedRequests()
	require.Len(t, opened, 2)
	require.Equal(t, other.Target, opened[1].Target)
	require.True(t, first.closedNow(), "the previous attachment is released")
	select {
	case notice := <-notices:
		t.Fatalf("a swap must not report a lifecycle transition to the picker: %+v", notice)
	default:
	}
}

// TestPickerCloseWithoutAttachment pins the unattached decision: with nothing
// to return to, Escape and q keep the picker (and the process) alive; only the
// explicit Ctrl+C exit ends the run.
func TestPickerCloseWithoutAttachment(t *testing.T) {
	tests := []struct {
		name     string
		op       pickerOp
		wantExit bool
	}{
		{name: "escape or q keeps the picker", op: pickerOp{close: true}},
		{name: "ctrl+c exits explicitly", op: pickerOp{close: true, exit: true}, wantExit: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			harness := startAttachHarness(t, picker)
			picker.recordOp(tt.op, "", ports.BrokerOpenStreamRequest{})
			if tt.wantExit {
				require.NoError(t, harness.waitRun(t))
				return
			}
			// Prove the close was consumed, then that the run survived it: a
			// later commit is still admitted.
			require.Eventually(t, picker.opTaken, 5*time.Second, time.Millisecond)
			stream := newSessionTestStream()
			harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				return stream, nil
			})
			picker.commit(sessionTestRequest(true))
			awaitStreamHello(t, &sync.Mutex{}, &stream)
			select {
			case <-harness.runDone:
				t.Fatal("a cancel with no attachment ended the process")
			default:
			}
		})
	}
}

// moveSnapshot is the two-destination publication a move interaction opens
// with; both rows authorise a move.
func moveSnapshot(interaction, revision uint64) protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: interaction, SourceID: "serving", SourceRevision: revision, Status: protocol.PickerSourceOK,
		Lines: []protocol.PickerLine{
			{Key: "aa/first", Kind: protocol.PickerLineSession, Label: "first", Focusable: true, Actions: protocol.PickerCanMove},
			{Key: "bb/second", Kind: protocol.PickerLineSession, Label: "second", Focusable: true, Actions: protocol.PickerCanMove},
		},
		Cursor: protocol.PickerCursor{Key: "aa/first", Index: 0},
	}
}

func moveOffer(interaction uint64) protocol.PickerOffer {
	return protocol.PickerOffer{InteractionID: interaction, Intent: protocol.PickerIntentMovePane, Title: " Move "}
}

// openMovePicker opens one move interaction on a live attachment and waits
// until its box is drawn.
func openMovePicker(t *testing.T, harness *attachTestHarness, stream *sessionTestStream, interaction uint64) {
	t.Helper()
	stream.deliver(moveOffer(interaction))
	stream.deliver(moveSnapshot(interaction, 1))
	require.Eventually(t, func() bool { return strings.Contains(harness.terminal.written(), "second") }, 5*time.Second, time.Millisecond, "the move picker was never drawn")
}

// TestMovePickerOfferIsPresentedAndAnswered pins PA3: the daemon's move offer
// opens a working destination picker whose input never reaches the session,
// and every exit path answers with the typed replies and hands input back.
func TestMovePickerOfferIsPresentedAndAnswered(t *testing.T) {
	tests := []struct {
		name  string
		keys  []string
		check func(t *testing.T, harness *attachTestHarness, stream *sessionTestStream)
	}{
		{
			name: "enter commits the cursor destination",
			keys: []string{"\x1b[B", "\r"},
			check: func(t *testing.T, harness *attachTestHarness, stream *sessionTestStream) {
				selection := awaitSent(t, stream, "PickerSelection", isSent[protocol.PickerSelection]).(protocol.PickerSelection)
				require.Equal(t, uint64(7), selection.InteractionID)
				require.Equal(t, "serving", selection.SourceID)
				require.Equal(t, uint64(1), selection.SourceRevision)
				require.Equal(t, "bb/second", selection.Key)
				require.Equal(t, protocol.PickerActionMove, selection.Action)
				// The daemon closes the interaction; the terminal returns to the
				// attachment with an authoritative repaint.
				stream.deliver(protocol.PickerClosed{InteractionID: 7})
				awaitSent(t, stream, "OutputResetRequest", isSent[protocol.OutputResetRequest])
			},
		},
		{
			name: "q cancels with a typed close",
			keys: []string{"q"},
			check: func(t *testing.T, harness *attachTestHarness, stream *sessionTestStream) {
				closed := awaitSent(t, stream, "PickerClose", isSent[protocol.PickerClose]).(protocol.PickerClose)
				require.Equal(t, uint64(7), closed.InteractionID)
				awaitSent(t, stream, "OutputResetRequest", isSent[protocol.OutputResetRequest])
			},
		},
		{
			name: "a lone escape cancels once its window elapses",
			keys: []string{"\x1b"},
			check: func(t *testing.T, harness *attachTestHarness, stream *sessionTestStream) {
				// The input path first withholds the Escape as a possible
				// DECRQM reply prefix; its ambiguity window hands it on.
				var marker *supervisorTestTimer
				require.Eventually(t, func() bool {
					for _, timer := range harness.clock.snapshotTimers() {
						if timer.delay == paletteMarkerAmbiguityDeadline && !timer.stopped() {
							marker = timer
							return true
						}
					}
					return false
				}, 5*time.Second, time.Millisecond, "the marker ambiguity window was never armed")
				marker.fire()
				var escape *supervisorTestTimer
				require.Eventually(t, func() bool {
					for _, timer := range harness.clock.snapshotTimers() {
						if timer.delay == pickerEscapeDeadline && timer != marker && !timer.stopped() {
							escape = timer
							return true
						}
					}
					return false
				}, 5*time.Second, time.Millisecond, "the escape window was never armed")
				require.Zero(t, countSent[protocol.PickerClose](stream), "a withheld escape must not close early")
				escape.fire()
				closed := awaitSent(t, stream, "PickerClose", isSent[protocol.PickerClose]).(protocol.PickerClose)
				require.Equal(t, uint64(7), closed.InteractionID)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			harness := startAttachHarness(t, picker)
			stream := attachLiveSession(t, harness, picker)
			openMovePicker(t, harness, stream, 7)

			// Output under the box is applied and acknowledged, never written.
			acks := countSent[protocol.Ack](stream)
			stream.deliver(laterOutput(2, 2, "hidden-frame"))
			require.Eventually(t, func() bool { return countSent[protocol.Ack](stream) > acks }, 5*time.Second, time.Millisecond)
			require.NotContains(t, harness.terminal.written(), "hidden-frame")

			for _, key := range tt.keys {
				harness.reader.push([]byte(key))
			}
			tt.check(t, harness, stream)
			require.Zero(t, countSent[protocol.Input](stream), "move picker input must never reach the session")

			// Input is never swallowed after the picker ended: the next key is
			// session Input again.
			harness.reader.push([]byte("z"))
			input := awaitSent(t, stream, "session Input", isSent[protocol.Input]).(protocol.Input)
			require.Equal(t, []byte("z"), input.Data)
			require.Equal(t, PresentAttached, harness.sup.State().Presentation)
		})
	}
}

// TestMovePickerOfferDeclinedUnderSessionPicker pins the exit path when the
// move picker cannot be presented: the daemon's interaction is closed at once
// instead of swallowing input with nothing on screen.
func TestMovePickerOfferDeclinedUnderSessionPicker(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startAttachHarness(t, picker)
	stream := attachLiveSession(t, harness, picker)
	stream.deliver(navigationOffer(1))
	awaitPresentation(t, harness.sup, PresentAttachedPicker)

	stream.deliver(moveOffer(9))
	closed := awaitSent(t, stream, "PickerClose", isSent[protocol.PickerClose]).(protocol.PickerClose)
	require.Equal(t, uint64(9), closed.InteractionID)
	require.Equal(t, PresentAttachedPicker, harness.sup.State().Presentation)
}

// TestSupervisorReducerPickerOverlay pins the Attached+Picker transitions and
// the paint fence: only a committed attachment hosts the overlay, cancel
// returns to Attached, and a settled attachment falls back to the plain
// picker.
func TestSupervisorReducerPickerOverlay(t *testing.T) {
	tests := []struct {
		name      string
		from      Presentation
		event     supervisorEventKind
		want      Presentation
		wantPaint bool
	}{
		{name: "attached opens the overlay", from: PresentAttached, event: supervisorOverlayOpened, want: PresentAttachedPicker, wantPaint: true},
		{name: "connecting cannot host the overlay", from: PresentConnecting, event: supervisorOverlayOpened, want: PresentConnecting},
		{name: "plain picker ignores an overlay request", from: PresentPicker, event: supervisorOverlayOpened, want: PresentPicker, wantPaint: true},
		{name: "cancel returns to the attachment", from: PresentAttachedPicker, event: supervisorOverlayClosed, want: PresentAttached},
		{name: "close outside the overlay is inert", from: PresentAttached, event: supervisorOverlayClosed, want: PresentAttached},
		{name: "attachment end falls back to the picker", from: PresentAttachedPicker, event: supervisorAttachEnded, want: PresentPicker, wantPaint: true},
		{name: "swap goes through connecting", from: PresentAttachedPicker, event: supervisorAttachBegin, want: PresentConnecting},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := reduceSupervisor(State{Presentation: tt.from, Connectivity: ConnectivityReady}, supervisorEvent{kind: tt.event})
			require.Equal(t, tt.want, state.Presentation)
			require.Equal(t, ConnectivityReady, state.Connectivity)
			require.Equal(t, tt.wantPaint, PickerPresentation(state))
		})
	}
}

// TestMovePickerDrawsAFloatingModalOverTheSession ports main's modal
// presentation pin: the move picker is a bordered box over the session, never
// a full-screen takeover, so it neither clears the screen nor spans it.
func TestMovePickerDrawsAFloatingModalOverTheSession(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startAttachHarness(t, picker)
	stream := attachLiveSession(t, harness, picker)
	before := len(harness.terminal.written())
	openMovePicker(t, harness, stream, 7)

	frame := harness.terminal.written()[before:]
	require.Contains(t, frame, "first", "the destination rows must be drawn")
	require.Contains(t, frame, "─", "the modal border must be drawn")
	require.NotContains(t, frame, "\x1b[2J", "the box must not clear the session under it")
	require.Contains(t, harness.terminal.written(), "ready", "the session frame stays under the box")
}

// TestPickerControllerLoneEscapeResolvesOnItsWindow pins that a real Escape
// keypress (one lone byte) cancels once the disambiguation window elapses on
// the controller clock, and that a following byte disarms the window instead.
func TestPickerControllerLoneEscapeResolvesOnItsWindow(t *testing.T) {
	tests := []struct {
		name      string
		follow    []byte
		wantClose bool
	}{
		{name: "window elapses into Escape", wantClose: true},
		{name: "a following byte resolves the prefix itself", follow: []byte("[B")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, clock := pickerTestController(t)
			controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha", "beta"))

			require.True(t, controller.ConsumeTerminalRead([]byte("\x1b")))
			escape := clock.awaitTimer(t)
			require.Equal(t, pickerEscapeDeadline, escape.delay)
			if tt.follow != nil {
				require.True(t, controller.ConsumeTerminalRead(tt.follow))
				require.Eventually(t, escape.stopped, 5*time.Second, time.Millisecond, "a resolving read must disarm the window")
				op, _ := controller.TakeOp()
				require.False(t, op.close)
				return
			}
			escape.fire()
			select {
			case <-controller.OpsReady():
			case <-time.After(5 * time.Second):
				t.Fatal("the elapsed window never woke the supervisor")
			}
			op, _ := controller.TakeOp()
			require.Equal(t, tt.wantClose, op.close)
			require.False(t, op.exit, "Escape is a cancel, never the explicit exit")
		})
	}
}
