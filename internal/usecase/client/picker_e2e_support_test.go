package client

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// newPickerTestTerminal builds the headless terminal backing both the UI
// state and the test's snapshot observations.
func newPickerTestTerminal(t *testing.T, ctx context.Context) (*uiterm.Terminal, *uiterm.Terminal) {
	t.Helper()
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, "")
	require.NoError(t, err)
	t.Cleanup(terminal.Close)
	return terminal, terminal
}

// waitForText waits for the UI to publish a frame containing the fragment
// after the given action committed.
func waitForText(t *testing.T, ctx context.Context, ui *UI, afterAction uint64, fragment string) {
	t.Helper()
	attached := ports.UIStatusAttached
	matched, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), AfterAction: afterAction, Expect: ports.UIExpect{TextContains: &fragment, Status: &attached}})
	require.NoError(t, err)
	require.NotZero(t, matched.Revision)
}

// pickerE2EHarness is one running attach attempt driven through the real
// wire transport, with the first full frame already committed so automation
// batches admit and the foreground owns the terminal.
type pickerE2EHarness struct {
	attempt   *attachAttempt
	transport *attachPaletteTransport
	ui        *UI
	terminal  *uiterm.Terminal
	input     *terminalInputPump
}

// startPickerE2E boots the attach attempt and pushes the first full frame.
func startPickerE2E(t *testing.T) *pickerE2EHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clock := newAttachPaletteClock()
	transport := newAttachPaletteTransport()
	ms := &milestones{}
	state := &terminalThemeState{}
	input := newTerminalInputPump(nil)
	t.Cleanup(input.stop)
	uiTerminal, _ := newPickerTestTerminal(t, ctx)
	ui := NewUI(uiTerminal, systemClock{})
	runner := &Runner{term: uiTerminal, clock: clock, logger: slog.New(slog.DiscardHandler), ledger: newRouteLedger()}
	runner.ui = ui
	source := routeTestCandidate(0, protocol.RouteOriginLocal)
	source.target = protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "fixture"}
	source.presentation.name = "fixture"
	source.request.SessionName = "fixture"
	_, err := runner.ledger.commit(source)
	require.NoError(t, err)
	attempt := &attachAttempt{
		runner: runner, transport: transport, request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "fixture"},
		milestones: ms, themeState: state,
		enterRaw:  func() error { ms.rawEntered = true; return nil },
		reconnect: &reconnectUI{term: uiTerminal, rawEntered: new(bool)},
		terminalInput: func() *terminalInputPump {
			return input
		},
	}
	done := make(chan attachResult, 1)
	go func() { done <- attempt.run(context.Background()) }()
	t.Cleanup(func() {
		transport.detached <- wire.Frame{Type: wire.MsgDetached, Payload: wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach})}
		select {
		case result := <-done:
			require.NoError(t, result.err)
		case <-time.After(2 * time.Second):
			t.Error("attempt did not detach")
		}
	})
	handshakeTimer := <-clock.handshakeTimers
	select {
	case <-handshakeTimer.stopped:
		// Welcome and synchronous client publications commit the handshake.
	case <-time.After(2 * time.Second):
		t.Fatal("handshake deadline still owned the runtime")
	}

	// The synthetic Welcome carries no Output frame, so push one full frame
	// through the wire: it starts the foreground, publishes the UI boundary,
	// and lets automation batches admit.
	view0 := protocol.ViewContext{Publication: 1, Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "fixture"}}, TabID: "t_abc123", FocusedPaneID: "p_def456"}
	output0, err := wire.MarshalOutput(protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: &view0, Data: []byte("\x1b[Hready")})
	require.NoError(t, err)
	transport.detached <- wire.Frame{Type: wire.MsgOutput, Payload: output0}

	return &pickerE2EHarness{attempt: attempt, transport: transport, ui: ui, terminal: uiTerminal, input: input}
}

// pickerSnapshot is the two-row snapshot both scenarios open the picker with.
func pickerSnapshot() protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: 7, Revision: 1, Title: " Sessions ",
		Rows: []protocol.PickerRow{
			{Key: "aa/first", Display: "first"},
			{Key: "bb/second", Display: "second"},
		},
		Cursor:       protocol.PickerCursor{Key: "aa/first", Index: 0},
		BarrierEpoch: 1, BarrierState: 1, SizeEpoch: 1,
	}
}

// awaitWireFrame waits for one client frame of the requested type.
func awaitWireFrame(t *testing.T, transport *attachPaletteTransport, frameType wire.MsgType) []byte {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("frame %d never crossed the wire", frameType)
			return nil
		default:
			transport.mu.Lock()
			var payload []byte
			for _, frame := range transport.frames {
				if frame.Type == frameType {
					payload = frame.Payload
					break
				}
			}
			transport.mu.Unlock()
			if payload != nil {
				return payload
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// awaitPickerSelection waits for the typed commit on the wire transport.
func awaitPickerSelection(t *testing.T, transport *attachPaletteTransport) protocol.PickerSelection {
	t.Helper()
	selection, err := wire.UnmarshalPickerSelection(awaitWireFrame(t, transport, wire.MsgPickerSelection))
	require.NoError(t, err)
	return selection
}

// requireNoPickerInput fails when any MsgInput frame reached the PTY.
func requireNoPickerInput(t *testing.T, transport *attachPaletteTransport) {
	t.Helper()
	select {
	case frame := <-transport.inputCh:
		t.Fatalf("picker bytes leaked to the PTY: %q", frame.Data)
	case <-time.After(200 * time.Millisecond):
	}
}
