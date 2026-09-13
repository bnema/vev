package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/stretchr/testify/require"
)

type funcLifecycleProbe struct {
	tryAcquire func(string) (lifecycleOwnership, error)
}

func (p funcLifecycleProbe) TryAcquire(dir string) (lifecycleOwnership, error) {
	return p.tryAcquire(dir)
}

type fakeDaemonFinder struct {
	find func(ctx context.Context, runtimeDir string) (daemonProcess, bool, error)
}

func (f fakeDaemonFinder) Find(ctx context.Context, runtimeDir string) (daemonProcess, bool, error) {
	if f.find == nil {
		return daemonProcess{}, false, nil
	}
	return f.find(ctx, runtimeDir)
}

func freeLifecycleOwnership() lifecycleOwnership {
	return fakeLifecycleOwnership{release: func() error { return nil }}
}

// forceStopFixture drives forceStopDaemon without touching real processes, the
// real terminal, or the wall clock.
type forceStopFixture struct {
	hooks   forceStopHooks
	signals []syscall.Signal
	output  bytes.Buffer
	now     time.Time
	pid     int
}

func newForceStopFixture(t *testing.T, input string) *forceStopFixture {
	t.Helper()
	f := &forceStopFixture{now: time.Now(), pid: 4242}
	f.hooks = forceStopHooks{
		probe: funcLifecycleProbe{tryAcquire: func(string) (lifecycleOwnership, error) {
			return nil, lifecycle.ErrBusy
		}},
		finder: fakeDaemonFinder{find: func(context.Context, string) (daemonProcess, bool, error) {
			return daemonProcess{PID: f.pid, Command: "/proc/self/exe --daemon", Start: "start-1"}, true, nil
		}},
		signal: func(_ int, sig syscall.Signal) error {
			f.signals = append(f.signals, sig)
			return nil
		},
		now:    func() time.Time { return f.now },
		sleep:  func(_ context.Context, d time.Duration) error { f.now = f.now.Add(d); return nil },
		stdin:  strings.NewReader(input),
		stdout: &f.output,
	}
	return f
}

// freeAfterSignals reports busy ownership until signals have been delivered.
func (f *forceStopFixture) freeAfterSignals(count int) {
	f.hooks.probe = funcLifecycleProbe{tryAcquire: func(string) (lifecycleOwnership, error) {
		if len(f.signals) >= count {
			return freeLifecycleOwnership(), nil
		}
		return nil, lifecycle.ErrBusy
	}}
}

// forceStopRuntime builds a runtime directory with the artifacts a force-stop
// may remove, plus durable state it must never touch.
func forceStopRuntime(t *testing.T) (runtimeDir, stateDir string) {
	t.Helper()
	root := t.TempDir()
	runtimeDir = filepath.Join(root, "runtime")
	stateDir = filepath.Join(root, "state")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	require.NoError(t, os.MkdirAll(stateDir, 0o700))
	require.NoError(t, os.WriteFile(ipc.SocketPath(runtimeDir), []byte("stale"), 0o600))
	spawnLock := filepath.Join(runtimeDir, spawnLockName)
	require.NoError(t, os.Mkdir(spawnLock, 0o700))
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(spawnLock, old, old))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "sessions.kv"), []byte("durable"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "config.toml"), []byte("config"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "vev.log"), []byte("log"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(stateDir, "snapshots"), 0o700))
	return runtimeDir, stateDir
}

func assertDurableStateIntact(t *testing.T, stateDir string) {
	t.Helper()
	for name, want := range map[string]string{
		"sessions.kv": "durable",
		"config.toml": "config",
		"vev.log":     "log",
	} {
		got, err := os.ReadFile(filepath.Join(stateDir, name))
		require.NoError(t, err, name)
		require.Equal(t, want, string(got), name)
	}
	info, err := os.Stat(filepath.Join(stateDir, "snapshots"))
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestForceStopNotApplicableWhenOwnershipIsFree(t *testing.T) {
	f := newForceStopFixture(t, "y\n")
	f.hooks.probe = funcLifecycleProbe{tryAcquire: func(string) (lifecycleOwnership, error) {
		return freeLifecycleOwnership(), nil
	}}

	err := forceStopDaemon(context.Background(), t.TempDir(), f.hooks)

	require.ErrorIs(t, err, errForceStopNotApplicable)
	require.Empty(t, f.signals)
	require.NotContains(t, f.output.String(), "[y/N]", "no prompt may be offered without a busy lifecycle")
}

func TestForceStopNotApplicableWithoutVerifiedProcess(t *testing.T) {
	f := newForceStopFixture(t, "y\n")
	f.hooks.finder = fakeDaemonFinder{find: func(context.Context, string) (daemonProcess, bool, error) {
		return daemonProcess{}, false, nil
	}}

	err := forceStopDaemon(context.Background(), t.TempDir(), f.hooks)

	require.ErrorIs(t, err, errForceStopNotApplicable)
	require.Empty(t, f.signals)
	require.NotContains(t, f.output.String(), "[y/N]")
}

func TestForceStopPromptRequiresExplicitYes(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantStopped bool
	}{
		{name: "empty input defaults to refusal", input: ""},
		{name: "blank line defaults to refusal", input: "\n"},
		{name: "n refuses", input: "n\n"},
		{name: "no refuses", input: "no\n"},
		{name: "yeah refuses", input: "yeah\n"},
		{name: "numeric one refuses", input: "1\n"},
		{name: "y accepts", input: "y\n", wantStopped: true},
		{name: "yes accepts", input: "yes\n", wantStopped: true},
		{name: "y without newline accepts", input: "y", wantStopped: true},
		{name: "uppercase Y accepts", input: "Y\n", wantStopped: true},
		{name: "mixed case Yes accepts", input: "Yes\n", wantStopped: true},
		{name: "padded y accepts", input: "  y  \n", wantStopped: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newForceStopFixture(t, tt.input)
			f.freeAfterSignals(1)
			runtimeDir, _ := forceStopRuntime(t)

			err := forceStopDaemon(context.Background(), runtimeDir, f.hooks)

			if tt.wantStopped {
				require.NoError(t, err)
				require.Equal(t, []syscall.Signal{syscall.SIGTERM}, f.signals)
				return
			}
			require.ErrorIs(t, err, errForceStopDeclined)
			require.Empty(t, f.signals)
		})
	}
}

func TestForceStopSendsSIGTERMAndRemovesStaleRuntimeArtifacts(t *testing.T) {
	f := newForceStopFixture(t, "y\n")
	f.freeAfterSignals(1)
	runtimeDir, stateDir := forceStopRuntime(t)

	err := forceStopDaemon(context.Background(), runtimeDir, f.hooks)

	require.NoError(t, err)
	require.Equal(t, []syscall.Signal{syscall.SIGTERM}, f.signals)
	require.Contains(t, f.output.String(), "4242")
	require.Contains(t, f.output.String(), "/proc/self/exe --daemon")
	require.Contains(t, f.output.String(), lifecycle.Path(runtimeDir))
	require.Contains(t, f.output.String(), "stopped daemon process 4242")
	require.NoFileExists(t, ipc.SocketPath(runtimeDir))
	require.NoDirExists(t, filepath.Join(runtimeDir, spawnLockName))
	assertDurableStateIntact(t, stateDir)
}

func TestForceStopKeepsFreshSpawnLock(t *testing.T) {
	f := newForceStopFixture(t, "y\n")
	f.freeAfterSignals(1)
	runtimeDir, _ := forceStopRuntime(t)
	fresh := filepath.Join(runtimeDir, spawnLockName)
	require.NoError(t, os.Chtimes(fresh, f.now, f.now))

	require.NoError(t, forceStopDaemon(context.Background(), runtimeDir, f.hooks))

	require.NoFileExists(t, ipc.SocketPath(runtimeDir))
	require.DirExists(t, fresh, "a concurrent spawner's fresh lock must be preserved")
}

func TestForceStopEscalatesToSIGKILLOnceForTheSameProcess(t *testing.T) {
	f := newForceStopFixture(t, "yes\n")
	f.freeAfterSignals(2)
	runtimeDir, stateDir := forceStopRuntime(t)
	start := f.now

	require.NoError(t, forceStopDaemon(context.Background(), runtimeDir, f.hooks))

	require.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, f.signals)
	require.GreaterOrEqual(t, f.now.Sub(start), forceStopSIGTERMWait,
		"SIGKILL must only follow the bounded SIGTERM wait")
	require.NoFileExists(t, ipc.SocketPath(runtimeDir))
	require.NoDirExists(t, filepath.Join(runtimeDir, spawnLockName))
	assertDurableStateIntact(t, stateDir)
}

func TestForceStopRefusesSIGKILLWhenProcessIdentityChanged(t *testing.T) {
	f := newForceStopFixture(t, "y\n")
	runtimeDir, stateDir := forceStopRuntime(t)
	calls := 0
	f.hooks.finder = fakeDaemonFinder{find: func(context.Context, string) (daemonProcess, bool, error) {
		calls++
		// The confirmed incarnation stays valid through the SIGTERM re-check and
		// disappears only before the SIGKILL escalation.
		if calls <= 2 {
			return daemonProcess{PID: f.pid, Command: "vev --daemon", Start: "start-1"}, true, nil
		}
		// The verified incarnation is gone: the PID now belongs to another process.
		return daemonProcess{PID: f.pid, Command: "unrelated", Start: "start-2"}, true, nil
	}}

	err := forceStopDaemon(context.Background(), runtimeDir, f.hooks)

	require.ErrorIs(t, err, errForceStopTimeout)
	require.Equal(t, []syscall.Signal{syscall.SIGTERM}, f.signals, "a reused PID must never receive SIGKILL")
	require.FileExists(t, ipc.SocketPath(runtimeDir), "artifacts stay until ownership is released")
	assertDurableStateIntact(t, stateDir)
}

// promptChangeReader flips the verified incarnation as soon as the operator's
// answer is consumed, so the identity check that follows confirmation sees a
// PID that was reused while the prompt was open.
type promptChangeReader struct {
	reader io.Reader
	onRead func()
}

func (r promptChangeReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.onRead()
	}
	return n, err
}

func TestForceStopReverifiesIdentityAfterConfirmation(t *testing.T) {
	tests := []struct {
		name   string
		change func(f *forceStopFixture) (daemonProcess, bool, error)
	}{
		{
			name: "pid reused by an unrelated process",
			change: func(f *forceStopFixture) (daemonProcess, bool, error) {
				return daemonProcess{PID: f.pid, Command: "unrelated", Start: "start-2"}, true, nil
			},
		},
		{
			name: "verified process vanished",
			change: func(*forceStopFixture) (daemonProcess, bool, error) {
				return daemonProcess{}, false, nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newForceStopFixture(t, "y\n")
			runtimeDir, stateDir := forceStopRuntime(t)
			reverified := false
			f.hooks.finder = fakeDaemonFinder{find: func(context.Context, string) (daemonProcess, bool, error) {
				if reverified {
					return tt.change(f)
				}
				return daemonProcess{PID: f.pid, Command: "/proc/self/exe --daemon", Start: "start-1"}, true, nil
			}}
			f.hooks.stdin = promptChangeReader{
				reader: strings.NewReader("y\n"),
				onRead: func() { reverified = true },
			}

			err := forceStopDaemon(context.Background(), runtimeDir, f.hooks)

			require.ErrorIs(t, err, errForceStopTimeout)
			require.Empty(t, f.signals, "the confirmed PID must never receive a signal once its identity changed")
			require.FileExists(t, ipc.SocketPath(runtimeDir), "artifacts stay until ownership is released")
			require.DirExists(t, filepath.Join(runtimeDir, spawnLockName))
			assertDurableStateIntact(t, stateDir)
		})
	}
}

func TestForceStopIsBoundedWhenOwnershipNeverTransfers(t *testing.T) {
	f := newForceStopFixture(t, "y\n")
	runtimeDir, _ := forceStopRuntime(t)
	start := f.now

	err := forceStopDaemon(context.Background(), runtimeDir, f.hooks)

	require.ErrorIs(t, err, errForceStopTimeout)
	require.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, f.signals, "escalation never repeats")
	require.GreaterOrEqual(t, f.now.Sub(start), forceStopSIGTERMWait+forceStopSIGKILLWait)
	require.FileExists(t, ipc.SocketPath(runtimeDir))
}

func TestForceStopDeclinedLeavesRuntimeArtifacts(t *testing.T) {
	f := newForceStopFixture(t, "")
	runtimeDir, stateDir := forceStopRuntime(t)

	err := forceStopDaemon(context.Background(), runtimeDir, f.hooks)

	require.ErrorIs(t, err, errForceStopDeclined)
	require.FileExists(t, ipc.SocketPath(runtimeDir))
	require.DirExists(t, filepath.Join(runtimeDir, spawnLockName))
	assertDurableStateIntact(t, stateDir)
}

func TestForceStopFallbackPreservesCauseWhenNotApplicable(t *testing.T) {
	original := forceStopDaemonFn
	t.Cleanup(func() { forceStopDaemonFn = original })
	cause := errors.New("typed kill failed")
	forceStopDaemonFn = func(context.Context) error { return errForceStopNotApplicable }

	require.ErrorIs(t, forceStopDaemonFallback(context.Background(), cause), cause)

	forceStopDaemonFn = func(context.Context) error { return nil }
	require.NoError(t, forceStopDaemonFallback(context.Background(), cause))

	forceStopDaemonFn = func(context.Context) error { return errForceStopDeclined }
	require.ErrorIs(t, forceStopDaemonFallback(context.Background(), cause), errForceStopDeclined)
}

// TestRunKillOffersForceStopOnlyForDaemonScope drives the real runKill path with
// an unreachable daemon whose lifecycle ownership stays busy.
func TestRunKillOffersForceStopOnlyForDaemonScope(t *testing.T) {
	originalBackoff := defaultBackoff
	originalFallback := forceStopDaemonFn
	t.Cleanup(func() {
		defaultBackoff = originalBackoff
		forceStopDaemonFn = originalFallback
	})
	defaultBackoff = backoffConfig{initial: time.Millisecond, max: time.Millisecond, total: 5 * time.Millisecond}

	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	owner, err := lifecycle.TryAcquire(ipc.SocketDir())
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Release()) }()

	t.Run("daemon scope offers the fallback", func(t *testing.T) {
		called := 0
		forceStopDaemonFn = func(context.Context) error { called++; return nil }
		require.NoError(t, runKill(context.Background(), "", false, true))
		require.Equal(t, 1, called)
	})

	t.Run("daemon scope keeps the typed failure when no candidate exists", func(t *testing.T) {
		called := 0
		forceStopDaemonFn = func(context.Context) error { called++; return errForceStopNotApplicable }
		err := runKill(context.Background(), "", false, true)
		require.Equal(t, 1, called)
		require.ErrorIs(t, err, ErrDaemonUnreachable)
	})

	t.Run("all scope never offers the fallback", func(t *testing.T) {
		called := 0
		forceStopDaemonFn = func(context.Context) error { called++; return nil }
		err := runKill(context.Background(), "", true, false)
		require.Zero(t, called)
		require.ErrorIs(t, err, ErrDaemonUnreachable)
	})
}
