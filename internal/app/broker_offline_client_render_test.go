package app

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/client"
)

// The resize presentation fence at the composition seam. The supervisor asks
// its renderer to repaint whenever a resize invalidation reaches a serialized
// picker wait, and the production offline composition paints only while the
// shared client.PickerPresentation fence admits the state. That fence is exactly
// PresentPicker: an admitted attachment foreground owns the terminal writer
// from Begin, before MarkAttached and the initial publication, so painting the
// pre-attachment connecting transition would overwrite session output with
// picker bytes. Attached and terminating presentations are refused for the same
// reason.

// offlineRenderTerminal is the composition's terminal double: a mutable
// geometry and an output buffer, with the single resize channel the attachment
// geometry collector would consume.
type offlineRenderTerminal struct {
	mu       sync.Mutex
	geometry domain.Geometry
	out      bytes.Buffer
	flushes  int
}

func newOfflineRenderTerminal(geometry domain.Geometry) *offlineRenderTerminal {
	return &offlineRenderTerminal{geometry: geometry}
}

func (t *offlineRenderTerminal) EnterRaw() (func() error, error) {
	return func() error { return nil }, nil
}

func (t *offlineRenderTerminal) Geometry() (domain.Geometry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.geometry, nil
}

func (t *offlineRenderTerminal) ResizeEvents() <-chan domain.Geometry {
	ch := make(chan domain.Geometry)
	close(ch)
	return ch
}

func (t *offlineRenderTerminal) In() io.Reader { return strings.NewReader("") }

func (t *offlineRenderTerminal) Out() io.Writer { return &offlineRenderTerminalWriter{terminal: t} }

func (t *offlineRenderTerminal) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.flushes++
	return nil
}

func (t *offlineRenderTerminal) resize(geometry domain.Geometry) {
	t.mu.Lock()
	t.geometry = geometry
	t.mu.Unlock()
}

// seed writes bytes exactly as an admitted attachment foreground would, so a
// test can prove the composition never paints over session output.
func (t *offlineRenderTerminal) seed(data string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.out.WriteString(data)
}

// written returns the bytes the composition actually wrote and the flush count.
func (t *offlineRenderTerminal) written() (string, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.out.String(), t.flushes
}

// offlineRenderTerminalWriter serializes writes through the terminal's own lock
// so a test reads a consistent buffer, exactly as the composition's writer mutex
// does in production.
type offlineRenderTerminalWriter struct{ terminal *offlineRenderTerminal }

func (w *offlineRenderTerminalWriter) Write(data []byte) (int, error) {
	w.terminal.mu.Lock()
	defer w.terminal.mu.Unlock()
	return w.terminal.out.Write(data)
}

// offlineRenderPicker builds the production picker over one reachable local
// daemon with a single session, so Render produces a real frame at any size.
func offlineRenderPicker(t *testing.T) *client.Picker {
	t.Helper()
	picker := client.NewPicker(nil, 0)
	picker.ApplySnapshot(ports.BrokerSnapshot{
		Epoch:    1,
		Revision: 1,
		Daemons: []ports.BrokerDaemonObservation{{
			Local:         true,
			DisplayOrigin: "local",
			Policy: ports.BrokerPolicy{
				ProtocolVersion:      protocol.Version,
				CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
				EnvironmentPolicy:    protocol.EnvironmentPolicyDaemonOwned,
				Transport:            "quic",
				Trust:                "offline-render-trust",
				Launch:               "offline-render-launch",
				Isolation:            "offline-render-isolation",
			},
			Identity:        "local-daemon",
			Incarnation:     ports.BrokerDaemonIncarnation{1},
			ProtocolVersion: protocol.Version,
			Availability:    domain.RemoteAvailabilityReachable,
			LastSuccess:     time.Now(),
			InventoryKnown:  true,
			Sessions: []catalogue.RemoteCatalogSession{{
				LifecycleID: domain.SessionLifecycleID{1},
				Name:        "offline-render-session",
				State:       catalogue.RemoteCatalogSessionUp,
			}},
		}},
	})
	return picker
}

// TestOfflineClientRenderRefusesNonPickerPresentations proves the composition
// fence refuses every state the shared predicate refuses, even after a resize
// moved the terminal to a new geometry. The buffer already holds the session
// frame an admitted foreground wrote, so a connecting repaint would overwrite
// it. If client.PickerPresentation admitted PresentConnecting, this test fails
// on overpainted session output.
func TestOfflineClientRenderRefusesNonPickerPresentations(t *testing.T) {
	small := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}
	large := domain.Geometry{Size: domain.Size{Cols: 132, Rows: 43}}
	const session = "\x1b[Hsession output"

	terminal := newOfflineRenderTerminal(small)
	terminal.seed(session)
	picker := offlineRenderPicker(t)

	observed := make(chan client.State, 8)
	render := offlineClientRender(terminal, picker, func(state client.State) {
		select {
		case observed <- state:
		default:
		}
	})

	terminal.resize(large)
	for _, presentation := range []client.Presentation{client.PresentConnecting, client.PresentAttached, client.PresentTerminating} {
		t.Run(presentation.String()+" is never painted over", func(t *testing.T) {
			state := client.State{Presentation: presentation, Connectivity: client.ConnectivityReady, Generation: 1}
			render(state)
			written, flushes := terminal.written()
			require.Equal(t, session, written, "%s must leave the session output byte for byte", presentation)
			require.Zero(t, flushes, "%s must not flush the terminal", presentation)
			select {
			case seen := <-observed:
				require.Equal(t, state, seen, "the composition observer still sees the state")
			default:
				t.Fatalf("the composition never reported the %s state", presentation)
			}
		})
	}
}

// TestOfflineClientRenderFenceIsPickerPresentation pins the composition to the
// shared predicate: it writes picker bytes for exactly the states
// client.PickerPresentation admits, so the composition can never widen or
// narrow the fence locally.
func TestOfflineClientRenderFenceIsPickerPresentation(t *testing.T) {
	small := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}
	states := []client.State{
		{Presentation: client.PresentPicker, Connectivity: client.ConnectivityReady, Generation: 1},
		{Presentation: client.PresentConnecting, Connectivity: client.ConnectivityReady, Generation: 1},
		{Presentation: client.PresentAttached, Connectivity: client.ConnectivityReady, Generation: 1},
		{Presentation: client.PresentTerminating, Connectivity: client.ConnectivityDisconnected, Generation: 1},
	}
	for _, state := range states {
		t.Run(state.Presentation.String(), func(t *testing.T) {
			terminal := newOfflineRenderTerminal(small)
			render := offlineClientRender(terminal, offlineRenderPicker(t), nil)

			render(state)

			written, flushes := terminal.written()
			if client.PickerPresentation(state) {
				require.NotEmpty(t, written, "a picker-visible state must paint the picker")
				require.Equal(t, 1, flushes, "a picker-visible state flushes exactly once")
				return
			}
			require.Empty(t, written, "a refused state must write nothing")
			require.Zero(t, flushes, "a refused state must not flush")
		})
	}
}

// TestOfflineClientRenderPaintsPickerResize pins the picker-visible half of the
// fence: a resize invalidation repaints the picker at the terminal's current
// geometry, and the repaint appends after the previous frame instead of
// rewriting it.
func TestOfflineClientRenderPaintsPickerResize(t *testing.T) {
	small := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}
	large := domain.Geometry{Size: domain.Size{Cols: 132, Rows: 43}}

	terminal := newOfflineRenderTerminal(small)
	picker := offlineRenderPicker(t)
	render := offlineClientRender(terminal, picker, nil)

	render(client.State{Presentation: client.PresentPicker, Connectivity: client.ConnectivityReady, Generation: 1})
	initial, flushes := terminal.written()
	require.NotEmpty(t, initial, "the initial picker paint writes the frame at the reported geometry")
	require.Contains(t, initial, "offline-render-session")
	require.Equal(t, 1, flushes)

	terminal.resize(large)
	render(client.State{Presentation: client.PresentPicker, Connectivity: client.ConnectivityReady, Generation: 1})
	resized, flushes := terminal.written()
	require.True(t, strings.HasPrefix(resized, initial), "the resize repaint appends after the initial frame, never rewrites it")
	require.Greater(t, len(resized), len(initial), "the picker resize repaint writes the frame at the new geometry")
	require.Contains(t, strings.TrimPrefix(resized, initial), "offline-render-session", "the appended repaint is the picker frame at the new size")
	require.Equal(t, 2, flushes)
}
