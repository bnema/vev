//go:build linux

package app

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/adapters/snapshot"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/internal/usecase/recovery"
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
	return startDaemonInDir(t, filepath.Join(t.TempDir(), "vev"), opts...)
}

func startDaemonInDir(t *testing.T, dir string, opts ...daemon.Option) (string, <-chan error) {
	t.Helper()
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
	return attachWithEnvironment(t, dir, intent, name, sz, nil)
}

func attachWithEnvironment(t *testing.T, dir string, intent uint8, name string, sz domain.Size, env []string) (ports.Transport, *pump) {
	t.Helper()
	tr, err := ipc.DialContext(context.Background(), dir)
	require.NoError(t, err)
	hello := ports.Hello{Version: ports.ProtocolVersion, Intent: intent, Name: name, Size: sz, TermEnv: "xterm-256color", TrueColor: true, Env: env}
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

func listRemoteSessions(t *testing.T, dir string) ports.Sessions {
	t.Helper()
	tr, err := ipc.DialContext(context.Background(), dir)
	require.NoError(t, err)
	defer func() { _ = tr.Close() }()
	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgList, Payload: ports.MarshalList(ports.List{})}))
	f, err := tr.Recv()
	require.NoError(t, err)
	require.Equal(t, ports.MsgSessions, f.Type)
	sessions, err := ports.UnmarshalSessions(f.Payload)
	require.NoError(t, err)
	return sessions
}

func killAll(dir string) error {
	tr, err := ipc.DialContext(context.Background(), dir)
	if err != nil {
		return err
	}
	defer func() { _ = tr.Close() }()
	if err := tr.Send(ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{All: true})}); err != nil {
		return err
	}
	_, err = tr.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
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

func shellFixture(t *testing.T, label string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shell")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\nprintf 'SHELL_COMMAND="+label+"\\n'\nexec /bin/sh\n"), 0o700))
	return path
}

func assertChildEnvironment(t *testing.T, tr ports.Transport, p *pump, sz domain.Size, wantTestEnv, wantShell, wantRuntimeDir, wantWayland string) {
	t.Helper()
	command := "printf '\\033[2J\\033[H'; printf 'VEV_TEST_ENV=%s SHELL=%s XDG_RUNTIME_DIR=%s WAYLAND_DISPLAY=%s TERM=%s COLORTERM=%s TERM_PROGRAM=%s VEV_PREFIX=%.24s\\n' \"$VEV_TEST_ENV\" \"${SHELL##*/}\" \"$XDG_RUNTIME_DIR\" \"$WAYLAND_DISPLAY\" \"$TERM\" \"$COLORTERM\" \"$TERM_PROGRAM\" \"$VEV\"\n"
	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte(command)})}))
	text := awaitScreenText(t, p, sz, "TERM_PROGRAM=vev")
	for _, want := range []string{
		"VEV_TEST_ENV=" + wantTestEnv,
		"SHELL=" + filepath.Base(wantShell),
		"XDG_RUNTIME_DIR=" + wantRuntimeDir,
		"WAYLAND_DISPLAY=" + wantWayland,
		"TERM=xterm-direct",
		"COLORTERM=truecolor",
		"TERM_PROGRAM=vev",
		"VEV_PREFIX=session=environment,tab=",
	} {
		require.Contains(t, text, want)
	}
}

func TestIntegration_AttachEnvironmentRefreshesFuturePTYChildren(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t)
	firstShell := shellFixture(t, "first")
	secondShell := shellFixture(t, "second")
	firstEnv := []string{
		"VEV_TEST_ENV=first", "SHELL=" + firstShell, "XDG_RUNTIME_DIR=/run/first", "WAYLAND_DISPLAY=wayland-first",
		"TERM=client", "COLORTERM=client", "TERM_PROGRAM=client", "VEV=client",
	}

	tr1, p1 := attachWithEnvironment(t, dir, ports.IntentNew, "environment", sz, firstEnv)
	defer func() { _ = tr1.Close() }()
	awaitText(t, p1, sz, "SHELL_COMMAND=first")
	assertChildEnvironment(t, tr1, p1, sz, "first", firstShell, "/run/first", "wayland-first")

	secondEnv := []string{
		"VEV_TEST_ENV=second", "SHELL=" + secondShell, "XDG_RUNTIME_DIR=/run/second", "WAYLAND_DISPLAY=wayland-second",
		"TERM=client", "COLORTERM=client", "TERM_PROGRAM=client", "VEV=client",
	}
	tr2, p2 := attachWithEnvironment(t, dir, ports.IntentAttach, "environment", sz, secondEnv)
	defer func() { _ = tr2.Close() }()

	// The first shell was already running, so it retains its original environment.
	assertChildEnvironment(t, tr2, p2, sz, "first", firstShell, "/run/first", "wayland-first")

	require.NoError(t, tr2.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("\x1b ")})}))
	awaitText(t, p2, sz, "Commands")
	require.NoError(t, tr2.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("CNT\r")})}))
	awaitText(t, p2, sz, "SHELL_COMMAND=second")
	assertChildEnvironment(t, tr2, p2, sz, "second", secondShell, "/run/second", "wayland-second")
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
	// Tab labels are enriched with the focused pane's title; with no process
	// inspector wired in this test, that title falls back to the shell's
	// basename ("sh").
	text := awaitScreenText(t, p, sz, " 1 (sh)  2 (sh) ")
	require.Contains(t, text, " 1 (sh)  2 (sh) ")
}

func TestIntegration_CommandPaletteRenamesEphemeralSession(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf READY; sleep 30"}))

	tr, p := attach(t, dir, ports.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()
	text := awaitScreenText(t, p, sz, "READY")
	require.Contains(t, text, " 0* ")

	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("\x1b ")})}))
	awaitText(t, p, sz, "Commands")

	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("RNS\r")})}))
	awaitText(t, p, sz, "Rename session")

	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("\x7fwork\r")})}))
	text = awaitScreenText(t, p, sz, " work ")
	require.NotContains(t, text, "work*")

	listTr, err := ipc.DialContext(context.Background(), dir)
	require.NoError(t, err)
	defer func() { _ = listTr.Close() }()
	require.NoError(t, listTr.Send(ports.Frame{Type: ports.MsgList, Payload: ports.MarshalList(ports.List{})}))
	f, err := listTr.Recv()
	require.NoError(t, err)
	require.Equal(t, ports.MsgSessions, f.Type)
	sessions, err := ports.UnmarshalSessions(f.Payload)
	require.NoError(t, err)
	require.Len(t, sessions.Sessions, 1)
	require.Equal(t, "work", sessions.Sessions[0].Name)
	require.False(t, sessions.Sessions[0].Ephemeral)
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

func TestIntegration_EphemeralSurvivesDetachAndReattaches(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, served := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf MARKER; sleep 30"}))

	tr1, p1 := attach(t, dir, ports.IntentEphemeral, "", sz)
	awaitText(t, p1, sz, "MARKER")
	require.NoError(t, tr1.Close())

	select {
	case err := <-served:
		t.Fatalf("daemon shut down while an ephemeral session was alive: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	tr2, p2 := attach(t, dir, ports.IntentAttach, "0", sz)
	defer func() { _ = tr2.Close() }()
	awaitText(t, p2, sz, "MARKER")

	require.NoError(t, killAll(dir))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not shut down after kill all")
	}
}

func TestIntegration_EphemeralNotListedAfterDaemonRestart(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	dir := ipc.SocketDir()

	_, served := startDaemonInDir(t, dir, daemon.WithShell("/bin/sh", []string{"-c", "printf TEMP; sleep 30"}))
	tr, p := attach(t, dir, ports.IntentEphemeral, "", sz)
	awaitText(t, p, sz, "TEMP")
	require.NoError(t, tr.Close())

	require.NoError(t, runKill(context.Background(), "", false, true))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop after kill --daemon")
	}

	_, served2 := startDaemonInDir(t, dir, daemon.WithShell("/bin/sh", []string{"-c", "sleep 30"}))
	sessions := listRemoteSessions(t, dir)
	require.Empty(t, sessions.Sessions)

	require.NoError(t, runKill(context.Background(), "", false, true))
	select {
	case err := <-served2:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("restarted daemon did not stop after kill --daemon")
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

func TestMultipleClientsOneLifecycleOwner(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "runtime")
	owner, err := lifecycle.TryAcquire(dir)
	require.NoError(t, err)

	ready := make(chan struct{})
	transport := &integrationTransport{}
	dial := func(context.Context, string) (ports.Transport, error) {
		select {
		case <-ready:
			return transport, nil
		default:
			return nil, os.ErrNotExist
		}
	}
	var spawns atomic.Int32
	cfg := backoffConfig{initial: time.Millisecond, max: 2 * time.Millisecond, total: time.Second}

	const clients = 4
	errs := make(chan error, clients)
	var wg sync.WaitGroup
	for range clients {
		wg.Go(func() {
			_, err := ensureDaemonWithLifecycle(context.Background(), dir, dial, func() error {
				spawns.Add(1)
				return nil
			}, cfg)
			errs <- err
		})
	}
	require.Never(t, func() bool { return spawns.Load() != 0 }, 50*time.Millisecond, time.Millisecond)
	close(ready)
	require.NoError(t, owner.Release())
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, spawns.Load())
}

func TestListWaitsForLifecycleOwner(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
	require.NoError(t, p.Close())
	owner, err := lifecycle.TryAcquire(ipc.SocketDir())
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- runList(context.Background()) }()
	require.Never(t, func() bool { return len(done) != 0 }, 50*time.Millisecond, time.Millisecond)
	require.NoError(t, owner.Release())
	require.NoError(t, <-done)
}

func TestOfflineKillWaitsForLifecycleOwner(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
	now := time.Now().UnixNano()
	require.NoError(t, p.Save(persist.Record{Name: "named", IncarnationID: domain.IncarnationID{1}, CreatedAt: now, UpdatedAt: now}))
	require.NoError(t, p.Close())
	owner, err := lifecycle.TryAcquire(ipc.SocketDir())
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- runKill(context.Background(), "named", false, false) }()
	require.Never(t, func() bool { return len(done) != 0 }, 50*time.Millisecond, time.Millisecond)
	require.NoError(t, owner.Release())
	require.NoError(t, <-done)
}

func TestKillDaemonWaitsForOwnershipTransfer(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	owner, err := lifecycle.TryAcquire(ipc.SocketDir())
	require.NoError(t, err)
	_, served := startDaemonInDir(t, ipc.SocketDir())

	done := make(chan error, 1)
	go func() { done <- requestDaemonStop(context.Background()) }()
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("daemon did not stop")
	}
	require.Never(t, func() bool { return len(done) != 0 }, 50*time.Millisecond, time.Millisecond)
	require.NoError(t, owner.Release())
	require.NoError(t, <-done)
}

func TestLifecycleOwnershipOutlivesMaintenanceWriter(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	stateDir := filepath.Join(t.TempDir(), "state")
	catalogue := &lifecycleObservedCatalogue{
		Catalogue: newTestPersister(t, stateDir),
		closed:    make(chan struct{}),
	}
	require.NoError(t, catalogue.Create(domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}}))
	repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))
	shutdownClock := newLifecycleShutdownClock(t)
	maintenanceRepository := &lifecycleBlockingMaintenanceRepository{
		SnapshotRepository: repository,
		entered:            make(chan struct{}),
		release:            make(chan struct{}),
		returned:           make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var releaseOnce sync.Once
	releaseMaintenance := func() { releaseOnce.Do(func() { close(maintenanceRepository.release) }) }
	t.Cleanup(releaseMaintenance)

	callbackReturned := make(chan struct{})
	wrapperReturned := make(chan error, 1)
	go func() {
		wrapperReturned <- runWithLifecycleOwner(ctx, runtimeDir, stateDir, func(ctx context.Context) error {
			defer close(callbackReturned)
			d := daemon.New(
				pty.NewFactory(),
				shutdownClock,
				discardLog(),
				daemon.WithSnapshotRepository(repository),
				daemon.WithDurableMaintenance(catalogue, maintenanceRepository),
				daemon.WithCatalogue(catalogue, nil),
			)
			if err := d.CollectStartupGarbage(ctx); err != nil {
				return err
			}
			listener, err := ipc.Listen(runtimeDir)
			if err != nil {
				return err
			}
			return d.Serve(ctx, listener)
		})
	}()

	awaitLifecycleStage(t, maintenanceRepository.entered, "pre-publication maintenance repository call")
	cancel()
	select {
	case <-callbackReturned:
		t.Fatal("Serve callback returned after shutdown deadline while maintenance was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	assertLifecycleStagePending(t, catalogue.closed, "catalogue close while maintenance is blocked")
	assertLifecycleResultPending(t, wrapperReturned, "lifecycle wrapper return while maintenance is blocked")
	_, err := lifecycle.TryAcquire(runtimeDir)
	require.ErrorIs(t, err, lifecycle.ErrBusy)

	releaseMaintenance()
	awaitLifecycleStage(t, maintenanceRepository.returned, "maintenance repository return")
	awaitLifecycleStage(t, catalogue.closed, "catalogue close after maintenance")
	awaitLifecycleStage(t, callbackReturned, "Serve callback return after catalogue close")
	require.NoError(t, awaitLifecycleResult(t, wrapperReturned, "lifecycle wrapper return"))
	owner, err := lifecycle.TryAcquire(runtimeDir)
	require.NoError(t, err)
	require.NoError(t, owner.Release())
}

type lifecycleBlockingMaintenanceRepository struct {
	ports.SnapshotRepository
	entered    chan struct{}
	release    chan struct{}
	returned   chan struct{}
	enterOnce  sync.Once
	returnOnce sync.Once
}

func (r *lifecycleBlockingMaintenanceRepository) CollectGarbage(context.Context, map[domain.IncarnationID]domain.CheckpointRef) error {
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.release // Intentionally ignore cancellation: lifecycle ownership must outlive this call.
	r.returnOnce.Do(func() { close(r.returned) })
	return nil
}

type lifecycleObservedCatalogue struct {
	ports.Catalogue
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *lifecycleObservedCatalogue) Close() error {
	err := c.Catalogue.Close()
	c.closeOnce.Do(func() { close(c.closed) })
	return err
}

func TestLifecycleOwnershipOutlivesSnapshotWriter(t *testing.T) {
	result := runBlockedSnapshotWriterShutdown(t)
	assertLifecycleStagePending(t, result.callbackReturned, "Serve callback return while snapshot writer is blocked")
	assertLifecycleResultPending(t, result.wrapperReturned, "lifecycle wrapper return while snapshot writer is blocked")
	_, err := lifecycle.TryAcquire(result.runtimeDir)
	require.ErrorIs(t, err, lifecycle.ErrBusy)

	result.releaseWriter()
	awaitLifecycleStage(t, result.publishReturned, "snapshot writer return")
	require.NoError(t, awaitLifecycleResult(t, result.wrapperReturned, "lifecycle wrapper return"))
	owner, err := lifecycle.TryAcquire(result.runtimeDir)
	require.NoError(t, err)
	require.NoError(t, owner.Release())
}

func TestLifecycleCallbackWaitsForEveryWriter(t *testing.T) {
	result := runBlockedSnapshotWriterShutdown(t)
	assertLifecycleStagePending(t, result.callbackReturned, "Serve callback return while snapshot writer is blocked")

	result.releaseWriter()
	awaitLifecycleStage(t, result.publishReturned, "snapshot writer return")
	awaitLifecycleStage(t, result.callbackReturned, "Serve callback return after snapshot writer")
	require.NoError(t, awaitLifecycleResult(t, result.wrapperReturned, "lifecycle wrapper return"))
}

func TestFinalCheckpointTimeoutKeepsOwner(t *testing.T) {
	result := runBlockedSnapshotWriterShutdown(t)
	assertLifecycleResultPending(t, result.wrapperReturned, "ownership release after checkpoint timeout with writer alive")
	_, err := lifecycle.TryAcquire(result.runtimeDir)
	require.ErrorIs(t, err, lifecycle.ErrBusy)

	result.releaseWriter()
	require.NoError(t, awaitLifecycleResult(t, result.wrapperReturned, "lifecycle wrapper return"))
}

// Startup restoration repairs HEADs, promotes fallbacks, and replaces catalogue
// records. Lifecycle ownership must therefore outlive it: releasing the flock
// while a restoration repository call is still running would let a second
// daemon mutate the same durable state.
func TestLifecycleOwnershipOutlivesRestorationWriter(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))
	checkpointed := publishRestorableCheckpoint(t, stateDir, repository)

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	blocking := &lifecycleBlockingRestore{
		SnapshotRepository: repository,
		entered:            make(chan struct{}),
		release:            make(chan struct{}),
		returned:           make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseRestore := func() { releaseOnce.Do(func() { close(blocking.release) }) }
	t.Cleanup(releaseRestore)

	shutdownClock := newLifecycleShutdownClock(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	listenerReady := make(chan struct{})
	callbackReturned := make(chan struct{})
	wrapperReturned := make(chan error, 1)
	go func() {
		wrapperReturned <- runWithLifecycleOwner(ctx, runtimeDir, stateDir, func(ctx context.Context) error {
			defer close(callbackReturned)
			opened, err := persist.OpenOrCreate(stateDir)
			if err != nil {
				return err
			}
			listener, err := ipc.Listen(runtimeDir)
			if err != nil {
				return errors.Join(err, opened.Catalogue.Close())
			}
			close(listenerReady)
			d := daemon.New(
				pty.NewFactory(),
				shutdownClock,
				discardLog(),
				daemon.WithShell("/bin/cat", nil),
				daemon.WithCatalogue(opened.Catalogue, opened.Records),
				daemon.WithSnapshotRepository(blocking),
			)
			return d.Serve(ctx, listener)
		})
	}()

	awaitLifecycleStage(t, listenerReady, "daemon listener")
	awaitLifecycleStage(t, blocking.entered, "restoration repository call")
	require.Equal(t, checkpointed, blocking.repairedName(), "restoration must target the checkpointed session")

	cancel()
	shutdownClock.nextTimer(t).fire()
	select {
	case <-callbackReturned:
		t.Fatal("Serve callback returned after shutdown deadline while restoration was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	assertLifecycleStagePending(t, callbackReturned, "Serve callback return while restoration is blocked")
	assertLifecycleResultPending(t, wrapperReturned, "lifecycle wrapper return while restoration is blocked")
	_, err := lifecycle.TryAcquire(runtimeDir)
	require.ErrorIs(t, err, lifecycle.ErrBusy)

	releaseRestore()
	awaitLifecycleStage(t, blocking.returned, "restoration repository return")
	awaitLifecycleStage(t, callbackReturned, "Serve callback return after restoration")
	require.NoError(t, awaitLifecycleResult(t, wrapperReturned, "lifecycle wrapper return"))
	owner, err := lifecycle.TryAcquire(runtimeDir)
	require.NoError(t, err)
	require.NoError(t, owner.Release())
}

// publishRestorableCheckpoint runs a complete daemon over the real catalogue and
// repository until one named session owns a committed checkpoint, then shuts it
// down. The returned name is restorable by any later daemon on the same state.
func publishRestorableCheckpoint(t *testing.T, stateDir string, repository *snapshot.Repository) string {
	t.Helper()
	const name = "restorable"
	runtimeDir := filepath.Join(t.TempDir(), "seed-runtime")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opened, err := persist.OpenOrCreate(stateDir)
	require.NoError(t, err)
	coordinator := recovery.NewCoordinator(opened.Catalogue, repository, rand.Reader)
	listener, err := ipc.Listen(runtimeDir)
	require.NoError(t, err)
	d := daemon.New(
		pty.NewFactory(),
		clock.New(),
		discardLog(),
		daemon.WithShell("/bin/cat", nil),
		daemon.WithCatalogue(opened.Catalogue, opened.Records),
		daemon.WithSnapshotRepository(repository),
		daemon.WithRecoveryCoordinator(coordinator),
	)
	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx, listener) }()

	tr, _ := attach(t, runtimeDir, ports.IntentNew, name, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("checkpoint me\n")})}))
	require.Eventually(t, func() bool {
		record, ok, _ := opened.Catalogue.Record(name)
		return ok && record.Committed != nil && record.DegradedReason == ""
	}, 5*time.Second, 10*time.Millisecond, "session never committed a checkpoint")
	require.NoError(t, tr.Close())

	cancel()
	require.NoError(t, awaitLifecycleResult(t, served, "seed daemon shutdown"))
	return name
}

type lifecycleBlockingRestore struct {
	ports.SnapshotRepository
	entered    chan struct{}
	release    chan struct{}
	returned   chan struct{}
	enterOnce  sync.Once
	returnOnce sync.Once
	mu         sync.Mutex
	repaired   string
}

func (r *lifecycleBlockingRestore) LoadCheckpoint(ctx context.Context, id domain.IncarnationID, name string, ref domain.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.mu.Lock()
	r.repaired = name
	r.mu.Unlock()
	return r.SnapshotRepository.LoadCheckpoint(ctx, id, name, ref)
}

func (r *lifecycleBlockingRestore) RepairHEAD(ctx context.Context, id domain.IncarnationID, ref domain.CheckpointRef) error {
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.release // Intentionally ignore cancellation: lifecycle ownership must outlive this call.
	r.returnOnce.Do(func() { close(r.returned) })
	return r.SnapshotRepository.RepairHEAD(ctx, id, ref)
}

func (r *lifecycleBlockingRestore) repairedName() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.repaired
}

type blockedSnapshotWriterShutdown struct {
	runtimeDir       string
	callbackReturned <-chan struct{}
	wrapperReturned  <-chan error
	publishReturned  <-chan struct{}
	releaseWriter    func()
}

func runBlockedSnapshotWriterShutdown(t *testing.T) blockedSnapshotWriterShutdown {
	t.Helper()
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	stateDir := filepath.Join(t.TempDir(), "state")
	shutdownClock := newLifecycleShutdownClock(t)
	repository := newLifecycleBlockingRepository()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	listenerReady := make(chan struct{})
	listenerClosed := make(chan struct{})
	callbackReturned := make(chan struct{})
	wrapperReturned := make(chan error, 1)
	go func() {
		wrapperReturned <- runWithLifecycleOwner(ctx, runtimeDir, stateDir, func(ctx context.Context) error {
			defer close(callbackReturned)
			listener, err := ipc.Listen(runtimeDir)
			if err != nil {
				return err
			}
			observed := &lifecycleObservedListener{Listener: listener, closed: listenerClosed}
			close(listenerReady)
			d := daemon.New(
				pty.NewFactory(),
				shutdownClock,
				discardLog(),
				daemon.WithShell("/bin/cat", nil),
				daemon.WithSnapshotRepository(repository),
			)
			return d.Serve(ctx, observed)
		})
	}()

	awaitLifecycleStage(t, listenerReady, "daemon listener")
	tr, _ := attach(t, runtimeDir, ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("dirty state\n")})}))
	awaitLifecycleStage(t, repository.entered, "snapshot publication")
	require.NoError(t, tr.Close())
	require.Eventually(t, func() bool {
		sessions := listRemoteSessions(t, runtimeDir)
		return len(sessions.Sessions) == 1 && !sessions.Sessions[0].Attached
	}, time.Second, 10*time.Millisecond)
	cancel()
	awaitLifecycleStage(t, listenerClosed, "listener close")

	shutdownClock.nextTimer(t).fire()
	select {
	case <-callbackReturned:
		t.Fatal("Serve callback returned after checkpoint timeout while snapshot writer was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	assertLifecycleStagePending(t, callbackReturned, "Serve callback return after checkpoint timeout")
	assertLifecycleResultPending(t, wrapperReturned, "lifecycle wrapper return after checkpoint timeout")

	var releaseOnce sync.Once
	releaseWriter := func() { releaseOnce.Do(func() { close(repository.release) }) }
	t.Cleanup(releaseWriter)
	return blockedSnapshotWriterShutdown{
		runtimeDir:       runtimeDir,
		callbackReturned: callbackReturned,
		wrapperReturned:  wrapperReturned,
		publishReturned:  repository.returned,
		releaseWriter:    releaseWriter,
	}
}

type lifecycleBlockingRepository struct {
	ports.SnapshotRepository
	entered    chan struct{}
	release    chan struct{}
	returned   chan struct{}
	enterOnce  sync.Once
	returnOnce sync.Once
}

func newLifecycleBlockingRepository() *lifecycleBlockingRepository {
	return &lifecycleBlockingRepository{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
}

func (r *lifecycleBlockingRepository) Publish(context.Context, ports.SnapshotPublication) error {
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.release // Intentionally ignore cancellation: lifecycle ownership must outlive this call.
	r.returnOnce.Do(func() { close(r.returned) })
	return nil
}

type lifecycleShutdownClock struct {
	t      *testing.T
	base   ports.Clock
	timers chan *lifecycleManualTimer
}

func newLifecycleShutdownClock(t *testing.T) *lifecycleShutdownClock {
	t.Helper()
	return &lifecycleShutdownClock{t: t, base: clock.New(), timers: make(chan *lifecycleManualTimer, 4)}
}

func (c *lifecycleShutdownClock) Now() time.Time { return c.base.Now() }
func (c *lifecycleShutdownClock) NewTimer(delay time.Duration) ports.Timer {
	if delay != daemon.SnapshotShutdownTimeout() {
		return c.base.NewTimer(delay)
	}
	timer := &lifecycleManualTimer{t: c.t, ch: make(chan time.Time, 1)}
	select {
	case c.timers <- timer:
	default:
		c.t.Errorf("unexpected additional snapshot shutdown timer with delay %s", delay)
		timer.fire()
	}
	return timer
}

func (c *lifecycleShutdownClock) nextTimer(t *testing.T) *lifecycleManualTimer {
	t.Helper()
	select {
	case timer := <-c.timers:
		return timer
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for shutdown deadline timer")
		return nil
	}
}

type lifecycleManualTimer struct {
	t       *testing.T
	ch      chan time.Time
	stopped atomic.Bool
}

func (t *lifecycleManualTimer) C() <-chan time.Time { return t.ch }
func (t *lifecycleManualTimer) Reset(delay time.Duration) bool {
	if delay != daemon.SnapshotShutdownTimeout() {
		t.t.Errorf("snapshot shutdown timer reset with delay %s, want %s", delay, daemon.SnapshotShutdownTimeout())
	}
	t.stopped.Store(false)
	return false
}
func (t *lifecycleManualTimer) Stop() bool { return !t.stopped.Swap(true) }
func (t *lifecycleManualTimer) fire() {
	if t.stopped.Load() {
		return
	}
	select {
	case t.ch <- time.Now():
	default:
	}
}

func TestLifecycleManualTimerRepeatedFireIsNonBlocking(t *testing.T) {
	timer := &lifecycleManualTimer{t: t, ch: make(chan time.Time, 1)}
	timer.fire()

	returned := make(chan struct{})
	go func() {
		timer.fire()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("repeated manual timer fire blocked on a pending tick")
	}
}

type lifecycleObservedListener struct {
	ports.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *lifecycleObservedListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func TestLifecycleSocketCloseCatalogueRace(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	stateDir := filepath.Join(t.TempDir(), "state")
	oldOwner, err := lifecycle.TryAcquire(runtimeDir)
	require.NoError(t, err)
	oldOpened, err := persist.OpenOrCreate(stateDir)
	require.NoError(t, err)
	oldListener, err := ipc.Listen(runtimeDir)
	require.NoError(t, err)

	socketCloseEntered := make(chan struct{})
	allowSocketClose := make(chan struct{})
	socketClosed := make(chan struct{})
	catalogueCloseEntered := make(chan struct{})
	allowCatalogueClose := make(chan struct{})
	catalogueClosed := make(chan struct{})
	ownerReleaseEntered := make(chan struct{})
	allowOwnerRelease := make(chan struct{})
	ownerReleased := make(chan struct{})
	teardownEvents := make(chan string, 3)

	controlledListener := &lifecycleControlledListener{
		Listener: oldListener,
		entered:  socketCloseEntered,
		proceed:  allowSocketClose,
		closed:   socketClosed,
		events:   teardownEvents,
	}
	controlledCatalogue := &lifecycleControlledCatalogue{
		Catalogue: oldOpened.Catalogue,
		entered:   catalogueCloseEntered,
		proceed:   allowCatalogueClose,
		closed:    catalogueClosed,
		events:    teardownEvents,
	}
	oldDaemon := daemon.New(pty.NewFactory(), clock.New(), discardLog(), daemon.WithCatalogue(controlledCatalogue, oldOpened.Records))
	oldCtx, stopOld := context.WithCancel(context.Background())
	oldDone := make(chan error, 1)
	go func() {
		serveErr := oldDaemon.Serve(oldCtx, controlledListener)
		teardownEvents <- "owner-release"
		close(ownerReleaseEntered)
		<-allowOwnerRelease
		releaseErr := oldOwner.Release()
		close(ownerReleased)
		oldDone <- errors.Join(serveErr, releaseErr)
	}()

	newAcquireAttempted := make(chan struct{})
	newDurableOpen := make(chan struct{})
	newListen := make(chan struct{})
	startupEvents := make(chan string, 3)
	newDone := make(chan error, 1)
	go func() {
		newDone <- runWithLifecycleOwnerDeps(context.Background(), runtimeDir, stateDir, func(ctx context.Context) error {
			close(newDurableOpen)
			opened, err := persist.OpenOrCreate(stateDir)
			if err != nil {
				return err
			}
			startupEvents <- "durable-open"

			close(newListen)
			listener, err := ipc.Listen(runtimeDir)
			if err != nil {
				return errors.Join(err, opened.Catalogue.Close())
			}
			startupEvents <- "listen"
			return errors.Join(listener.Close(), opened.Catalogue.Close())
		}, lifecycleStartupDeps{
			ensurePrivate: func(string) error { return nil },
			acquire: func(ctx context.Context, dir string, retry time.Duration) (lifecycleOwnership, error) {
				close(newAcquireAttempted)
				owner, err := lifecycle.Acquire(ctx, dir, retry)
				if err != nil {
					return nil, err
				}
				<-ownerReleased
				startupEvents <- "owner-acquired"
				return owner, nil
			},
		})
	}()

	awaitLifecycleStage(t, newAcquireAttempted, "new lifecycle acquisition attempt")
	assertLifecycleStagePending(t, newDurableOpen, "new durable open before old teardown")
	assertLifecycleStagePending(t, newListen, "new listen before old teardown")

	stopOld()
	awaitLifecycleStage(t, socketCloseEntered, "old socket close")
	assertNewDaemonStartupPending(t, newDurableOpen, newListen, "socket close")
	close(allowSocketClose)
	awaitLifecycleStage(t, socketClosed, "old socket closed")

	awaitLifecycleStage(t, catalogueCloseEntered, "old catalogue close")
	assertNewDaemonStartupPending(t, newDurableOpen, newListen, "catalogue close")
	close(allowCatalogueClose)
	awaitLifecycleStage(t, catalogueClosed, "old catalogue closed")

	awaitLifecycleStage(t, ownerReleaseEntered, "old lifecycle owner release")
	assertNewDaemonStartupPending(t, newDurableOpen, newListen, "owner release")
	close(allowOwnerRelease)
	awaitLifecycleStage(t, ownerReleased, "old lifecycle owner released")

	require.NoError(t, awaitLifecycleResult(t, oldDone, "old daemon teardown"))
	require.NoError(t, awaitLifecycleResult(t, newDone, "new daemon startup"))
	require.Equal(t, []string{"socket-close", "catalogue-close", "owner-release"}, []string{
		<-teardownEvents,
		<-teardownEvents,
		<-teardownEvents,
	})
	require.Equal(t, []string{"owner-acquired", "durable-open", "listen"}, []string{
		<-startupEvents,
		<-startupEvents,
		<-startupEvents,
	})
}

func awaitLifecycleStage(t *testing.T, stage <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-stage:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func awaitLifecycleResult(t *testing.T, result <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}

func assertLifecycleStagePending(t *testing.T, stage <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-stage:
		t.Fatalf("unexpected %s", name)
	default:
	}
}

func assertLifecycleResultPending(t *testing.T, result <-chan error, name string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("unexpected %s: %v", name, err)
	default:
	}
}

func assertNewDaemonStartupPending(t *testing.T, durableOpen, listen <-chan struct{}, heldAt string) {
	t.Helper()
	assertLifecycleStagePending(t, durableOpen, "new durable open while old daemon held at "+heldAt)
	assertLifecycleStagePending(t, listen, "new listen while old daemon held at "+heldAt)
}

type lifecycleControlledListener struct {
	ports.Listener
	entered chan struct{}
	proceed chan struct{}
	closed  chan struct{}
	events  chan<- string
	once    sync.Once
	err     error
}

func (l *lifecycleControlledListener) Close() error {
	l.once.Do(func() {
		l.events <- "socket-close"
		close(l.entered)
		<-l.proceed
		l.err = l.Listener.Close()
		close(l.closed)
	})
	return l.err
}

type lifecycleControlledCatalogue struct {
	ports.Catalogue
	entered chan struct{}
	proceed chan struct{}
	closed  chan struct{}
	events  chan<- string
	once    sync.Once
	err     error
}

func (c *lifecycleControlledCatalogue) Close() error {
	c.once.Do(func() {
		c.events <- "catalogue-close"
		close(c.entered)
		<-c.proceed
		c.err = c.Catalogue.Close()
		close(c.closed)
	})
	return c.err
}

type integrationTransport struct{}

func (*integrationTransport) Send(ports.Frame) error     { return nil }
func (*integrationTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (*integrationTransport) Close() error               { return nil }
func (*integrationTransport) LocalAddr() net.Addr        { return nil }
func (*integrationTransport) RemoteAddr() net.Addr       { return nil }

func TestIntegration_KillAllShutsDownDaemon(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, served := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "sleep 30"}))

	tr1, _ := attach(t, dir, ports.IntentNew, "one", sz)
	require.NoError(t, tr1.Close())
	tr2, _ := attach(t, dir, ports.IntentNew, "two", sz)
	require.NoError(t, tr2.Close())

	killTr, err := ipc.DialContext(context.Background(), dir)
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
