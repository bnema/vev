//go:build linux

package app

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/pkg/vt"
)

// These integration tests drive the real daemon over a real unix socket with a
// real PTY and the real VT/renderer pipeline. They live in internal/app (which
// is allowed to import adapters) because the layering guard forbids
// internal/usecase from importing internal/adapters, even in tests.

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startDaemon binds a real listener in a temp dir and serves in the background.
// The returned served channel receives Serve's result exactly once.
func startDaemon(t *testing.T, opts ...daemon.Option) (dir string, served <-chan error) {
	t.Helper()
	dir = t.TempDir()
	ln, err := ipc.Listen(dir)
	require.NoError(t, err)

	d := daemon.New(pty.NewFactory(), clock.New(), discardLog(), opts...)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- d.Serve(ctx, ln) }()
	t.Cleanup(cancel)
	return dir, ch
}

// pump wires a background Recv pump to a transport so tests can await frames
// with a timeout (ports.Transport has no read deadline).
type pump struct{ ch chan ports.Frame }

func recvPump(tr ports.Transport) *pump {
	p := &pump{ch: make(chan ports.Frame, 128)}
	go func() {
		for {
			f, err := tr.Recv()
			if err != nil {
				close(p.ch)
				return
			}
			p.ch <- f
		}
	}()
	return p
}

// attach dials, handshakes, and returns the transport plus its frame pump.
func attach(t *testing.T, dir string, intent uint8, name string, sz domain.Size) (ports.Transport, *pump) {
	t.Helper()
	tr, err := ipc.Dial(dir)
	require.NoError(t, err)
	hello := ports.Hello{Version: ports.ProtocolVersion, Intent: intent, Name: name, Size: sz, TermEnv: "xterm-256color"}
	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)}))
	p := recvPump(tr)
	select {
	case f, ok := <-p.ch:
		require.True(t, ok, "connection closed before welcome")
		require.Equal(t, ports.MsgWelcome, f.Type)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for welcome")
	}
	return tr, p
}

// awaitText decodes MsgOutput frames into a fresh VT screen and returns once
// the reconstructed grid contains want.
func awaitText(t *testing.T, p *pump, sz domain.Size, want string) {
	t.Helper()
	_ = awaitScreenText(t, p, sz, want)
}

// awaitScreenText is like awaitText, but returns the reconstructed screen text
// at the point the wanted text appears so callers can make additional checks.
func awaitScreenText(t *testing.T, p *pump, sz domain.Size, want string) string {
	t.Helper()
	screen := vt.NewScreen(sz.Cols, sz.Rows)
	timeout := time.After(5 * time.Second)
	for {
		select {
		case f, ok := <-p.ch:
			if !ok {
				t.Fatalf("connection closed before %q appeared; screen=%q", want, screenText(screen))
			}
			if f.Type == ports.MsgOutput {
				o, err := ports.UnmarshalOutput(f.Payload)
				require.NoError(t, err)
				screen.Write(o.Data)
				text := screenText(screen)
				if strings.Contains(text, want) {
					return text
				}
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %q; screen=%q", want, screenText(screen))
		}
	}
}

func assertNoTextAfterInput(t *testing.T, p *pump, sz domain.Size, absent string) {
	t.Helper()
	screen := vt.NewScreen(sz.Cols, sz.Rows)
	timeout := time.After(300 * time.Millisecond)
	for {
		select {
		case f, ok := <-p.ch:
			if !ok {
				return
			}
			if f.Type == ports.MsgOutput {
				o, err := ports.UnmarshalOutput(f.Payload)
				require.NoError(t, err)
				screen.Write(o.Data)
				require.NotContains(t, screenText(screen), absent)
			}
		case <-timeout:
			return
		}
	}
}

func screenText(s *vt.Screen) string {
	var b strings.Builder
	for y := range s.Frame.Height {
		for x := range s.Frame.Width {
			r := s.Frame.At(x, y).Rune
			if r == 0 {
				r = ' '
			}
			b.WriteRune(r)
		}
	}
	return b.String()
}

func TestIntegration_AttachFirstOutput(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf HELLO; sleep 30"}))

	tr, p := attach(t, dir, ports.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()

	awaitText(t, p, sz, "HELLO")
}

func TestIntegration_InputRoundtrip(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/cat", nil))

	tr, p := attach(t, dir, ports.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()

	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("PINGPONG\n")})}))
	awaitText(t, p, sz, "PINGPONG")
}

func TestIntegration_CommandPaletteCreatesTab(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf READY; sleep 30"}))

	tr, p := attach(t, dir, ports.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()
	awaitText(t, p, sz, "READY")

	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("\x1b ")})}))
	awaitText(t, p, sz, "Commands")

	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("CNT\r")})}))
	text := awaitScreenText(t, p, sz, " 1  2 ")
	require.Contains(t, text, " 1  2 ")
}

func TestIntegration_AltCWithoutPaletteDoesNotCreateTab(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf READY; sleep 30"}))

	tr, p := attach(t, dir, ports.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()
	text := awaitScreenText(t, p, sz, "READY")
	require.NotContains(t, text, " 1  2 ")

	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("\x1bc")})}))
	assertNoTextAfterInput(t, p, sz, " 1  2 ")
}

func TestIntegration_EphemeralGoneOnDetach(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, served := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf READY; sleep 30"}))

	tr, p := attach(t, dir, ports.IntentEphemeral, "", sz)
	awaitText(t, p, sz, "READY")

	// Detach by dropping the connection: the ephemeral session must die, which
	// (being the last session) shuts the daemon down.
	require.NoError(t, tr.Close())

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not shut down after the ephemeral session detached")
	}
}

func TestIntegration_NamedSurvivesReattach(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, served := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf MARKER; sleep 30"}))

	tr1, p1 := attach(t, dir, ports.IntentNew, "work", sz)
	awaitText(t, p1, sz, "MARKER")

	// Detach: a named session must survive.
	require.NoError(t, tr1.Close())

	// Daemon must still be running (session alive).
	select {
	case err := <-served:
		t.Fatalf("daemon shut down while a named session was alive: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	// Re-attach: the first paint must reproduce the retained screen state.
	tr2, p2 := attach(t, dir, ports.IntentAttach, "work", sz)
	defer func() { _ = tr2.Close() }()
	awaitText(t, p2, sz, "MARKER")
}

func TestIntegration_KillAllShutsDownDaemon(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, served := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "sleep 30"}))

	tr1, _ := attach(t, dir, ports.IntentNew, "one", sz)
	require.NoError(t, tr1.Close())
	tr2, _ := attach(t, dir, ports.IntentNew, "two", sz)
	require.NoError(t, tr2.Close())

	killTr, err := ipc.Dial(dir)
	require.NoError(t, err)
	defer func() { _ = killTr.Close() }()
	require.NoError(t, killTr.Send(ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{All: true})}))

	_, err = killTr.Recv()
	require.ErrorIs(t, err, io.EOF)
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not shut down after kill all")
	}
}
