package client

import (
	"context"
	"io"
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
	clock     *attachPaletteClock
}

// startPickerE2E boots the attach attempt and pushes the first full frame.
func startPickerE2E(t *testing.T) *pickerE2EHarness {
	return startPickerE2EHarness(t, nil, true)
}

// startPickerE2EWithInput boots the same harness over a caller-owned
// terminal reader, so tests can drive physical input instead of the
// ui-driver automation channel.
func startPickerE2EWithInput(t *testing.T, reader io.Reader) *pickerE2EHarness {
	return startPickerE2EHarness(t, reader, true)
}

// startPickerE2EHeadless boots the harness without the UI-driver
// composition (runner.ui is nil), which is how the ordinary CLI runs. The
// picker must own and complete physical input there too.
func startPickerE2EHeadless(t *testing.T, reader io.Reader) *pickerE2EHarness {
	return startPickerE2EHarness(t, reader, false)
}

func startPickerE2EHarness(t *testing.T, reader io.Reader, withUI bool) *pickerE2EHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clock := newAttachPaletteClock()
	transport := newAttachPaletteTransport()
	ms := &milestones{}
	state := &terminalThemeState{}
	input := newTerminalInputPump(reader)
	if reader != nil {
		// The harness owns this reader: start its loop so physical bytes
		// reach the attach attempt. The nil case keeps the automation-only
		// path used by the ui-driver scenarios.
		input.start()
	}
	t.Cleanup(input.stop)
	uiTerminal, _ := newPickerTestTerminal(t, ctx)
	var ui *UI
	runner := &Runner{term: uiTerminal, clock: clock, logger: slog.New(slog.DiscardHandler), ledger: newRouteLedger()}
	if withUI {
		ui = NewUI(uiTerminal, systemClock{})
		runner.ui = ui
	}
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

	return &pickerE2EHarness{attempt: attempt, transport: transport, ui: ui, terminal: uiTerminal, input: input, clock: clock}
}

// frameCount reports how many client frames crossed the wire so far.
func (t *attachPaletteTransport) frameCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.frames)
}

// fireAmbiguityDeadlines releases every armed 20 ms ambiguity deadline
// within a short window, which is how a withheld escape prefix reaches the
// decoder. Other timers are left alone.
func (h *pickerE2EHarness) fireAmbiguityDeadlines(t *testing.T) {
	t.Helper()
	deadline := time.After(50 * time.Millisecond)
	for {
		select {
		case timer := <-h.clock.timers:
			if timer.duration == pickerEscapeDeadline {
				timer.fire()
			}
		case <-deadline:
			return
		}
	}
}

// fireDecodeDeadline fires the pump timer armed for one delay, skipping
// unrelated timers the attempt owns.
func (h *pickerE2EHarness) fireDecodeDeadline(t *testing.T, delay time.Duration) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case timer := <-h.clock.timers:
			if timer.duration == delay {
				timer.fire()
				return
			}
		case <-deadline:
			t.Fatalf("no %s timer was armed", delay)
			return
		}
	}
}

// awaitAfterAmbiguityDeadlines fires the 20 ms ambiguity timers the pump
// arms until the wanted frame crosses the wire. The palette-marker scanner
// and the picker decoder share that deadline, so an escape withheld as a
// possible marker is flushed into the picker decoder before its own window
// starts. More than a few deadlines for one escape means the window is
// being re-armed instead of expiring, so the helper fails instead of
// firing forever.
func (h *pickerE2EHarness) awaitAfterAmbiguityDeadlines(t *testing.T, transport *attachPaletteTransport, frameType wire.MsgType) {
	t.Helper()
	const maxDeadlines = 4
	hasFrame := func() bool {
		transport.mu.Lock()
		defer transport.mu.Unlock()
		for _, frame := range transport.frames {
			if frame.Type == frameType {
				return true
			}
		}
		return false
	}
	deadline := time.After(3 * time.Second)
	fired := 0
	for {
		select {
		case timer := <-h.clock.timers:
			if timer.duration == pickerEscapeDeadline {
				timer.fire()
				fired++
				if fired > maxDeadlines {
					t.Fatalf("the escape deadline was re-armed %d times for one prefix", fired)
				}
			}
		default:
		}
		if hasFrame() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("frame %d never crossed the wire", frameType)
		case <-time.After(time.Millisecond):
		}
	}
}

// pickerSnapshot returns the two-row snapshot both scenarios open the picker with.
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
