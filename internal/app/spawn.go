package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

// ErrDaemonUnreachable is returned when the daemon socket never becomes
// dialable within the retry budget, despite a spawn attempt.
var ErrDaemonUnreachable = errors.New("vev: daemon did not become reachable")

// spawnLockName is the mkdir-based lock directory guarding daemon spawn, so
// concurrent first-clients elect a single spawner instead of racing to
// re-exec multiple daemons.
const spawnLockName = "spawn.lock"

// staleLockAge bounds how long a spawn lock may persist before it is treated
// as abandoned (spawner crashed mid-flight) and forcibly taken over.
const staleLockAge = 10 * time.Second

// dialFunc dials the daemon socket in dir. Injected so tests can drive the
// spawn/backoff logic without a real socket.
type dialFunc func(ctx context.Context, dir string) (ports.Transport, error)

// spawnFunc launches a detached daemon process. Injected so tests never
// re-exec a real binary.
type spawnFunc func() error

type lifecycleProbe interface {
	TryAcquire(string) (lifecycleOwnership, error)
}

type osLifecycleProbe struct{}

func (osLifecycleProbe) TryAcquire(runtimeDir string) (lifecycleOwnership, error) {
	return lifecycle.TryAcquire(runtimeDir)
}

var daemonLifecycleProbe lifecycleProbe = osLifecycleProbe{}

// backoffConfig parameterises retry-dial timing so tests can shrink it.
type backoffConfig struct {
	initial time.Duration
	max     time.Duration
	total   time.Duration
}

// defaultBackoff is the production retry schedule: 10ms, 20ms, 40ms... capped
// at 500ms, for roughly 5s total.
var defaultBackoff = backoffConfig{
	initial: 10 * time.Millisecond,
	max:     500 * time.Millisecond,
	total:   5 * time.Second,
}

func retryAttempts[T any](ctx context.Context, cfg backoffConfig, attempt func() (T, bool, error)) (T, error) {
	deadline := time.Now().Add(cfg.total)
	backoff := cfg.initial
	if backoff <= 0 {
		backoff = time.Millisecond
	}
	for {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}
		result, done, err := attempt()
		if err != nil {
			var zero T
			return zero, err
		}
		if done {
			return result, nil
		}
		if !time.Now().Before(deadline) {
			var zero T
			return zero, ErrDaemonUnreachable
		}
		if err := waitBackoff(ctx, backoff); err != nil {
			var zero T
			return zero, err
		}
		if backoff *= 2; backoff > cfg.max {
			backoff = cfg.max
		}
	}
}

func waitForLifecycleAvailability(ctx context.Context, runtimeDir string, cfg backoffConfig) (lifecycleOwnership, error) {
	return retryAttempts(ctx, cfg, func() (lifecycleOwnership, bool, error) {
		owner, err := daemonLifecycleProbe.TryAcquire(runtimeDir)
		if err == nil {
			return owner, true, nil
		}
		if !errors.Is(err, lifecycle.ErrBusy) {
			return nil, false, err
		}
		return nil, false, nil
	})
}

type daemonOrLifecycle struct {
	transport ports.Transport
	owner     lifecycleOwnership
}

func waitForDaemonOrLifecycle(ctx context.Context, dir string, dial dialFunc, cfg backoffConfig) (ports.Transport, lifecycleOwnership, error) {
	result, err := retryAttempts(ctx, cfg, func() (daemonOrLifecycle, bool, error) {
		if transport, err := dial(ctx, dir); err == nil {
			return daemonOrLifecycle{transport: transport}, true, nil
		}
		owner, err := daemonLifecycleProbe.TryAcquire(dir)
		if err == nil {
			return daemonOrLifecycle{owner: owner}, true, nil
		}
		if !errors.Is(err, lifecycle.ErrBusy) {
			return daemonOrLifecycle{}, false, err
		}
		return daemonOrLifecycle{}, false, nil
	})
	return result.transport, result.owner, err
}

func waitBackoff(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ensureDaemonWithLifecycle never elects a spawner while another lifecycle
// owner may still be initializing or tearing down durable state.
func ensureDaemonWithLifecycle(ctx context.Context, dir string, dial dialFunc, spawn spawnFunc, cfg backoffConfig) (ports.Transport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if transport, err := dial(ctx, dir); err == nil {
		return transport, nil
	}
	transport, owner, err := waitForDaemonOrLifecycle(ctx, dir, dial, cfg)
	if err != nil {
		return nil, err
	}
	if transport != nil {
		return transport, nil
	}
	if err := owner.Release(); err != nil {
		return nil, fmt.Errorf("vev: release lifecycle spawn probe: %w", err)
	}
	return ensureDaemon(ctx, dir, dial, spawn, cfg)
}

// ensureDaemon returns a transport to a running daemon, spawning one if
// necessary after ensureDaemonWithLifecycle has established lifecycle
// availability. Tests also exercise this lower-level spawn election directly.
func ensureDaemon(ctx context.Context, dir string, dial dialFunc, spawn spawnFunc, cfg backoffConfig) (ports.Transport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t, err := dial(ctx, dir); err == nil {
		slog.Debug("daemon already reachable", "socket_dir", dir)
		return t, nil
	}

	release, acquired, err := acquireSpawnLock(dir)
	if err != nil {
		return nil, fmt.Errorf("vev: acquiring spawn lock: %w", err)
	}
	if acquired {
		// We won the election: spawn, and hold the lock until the socket is
		// dialable (or spawn fails) so late-arriving clients keep waiting
		// rather than spawning a second daemon.
		defer release()
		slog.Info("spawning daemon", "socket_dir", dir)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := spawn(); err != nil {
			slog.Error("daemon spawn failed", "err", err)
			return nil, fmt.Errorf("vev: spawning daemon: %w", err)
		}
	} else {
		slog.Debug("waiting for daemon spawned by another process", "socket_dir", dir)
	}

	return retryDial(ctx, dir, dial, cfg)
}

// acquireSpawnLock attempts to create the spawn-lock directory. It returns
// acquired=true with a release closure when it wins, acquired=false (with a
// no-op release) when another process holds a fresh lock, and takes over a
// lock older than staleLockAge (crash resilience).
func acquireSpawnLock(dir string) (release func(), acquired bool, err error) {
	if err := safedir.EnsurePrivate(dir); err != nil {
		return nil, false, err
	}
	path := filepath.Join(dir, spawnLockName)

	if mkErr := os.Mkdir(path, 0o700); mkErr == nil {
		return func() { _ = os.Remove(path) }, true, nil
	} else if !errors.Is(mkErr, os.ErrExist) {
		return nil, false, mkErr
	}

	// Lock exists: take it over only if it looks abandoned.
	if info, statErr := os.Stat(path); statErr == nil && time.Since(info.ModTime()) > staleLockAge {
		age := time.Since(info.ModTime())
		slog.Warn("taking over stale daemon spawn lock", "path", path, "age", age)
		_ = os.Remove(path)
		if mkErr := os.Mkdir(path, 0o700); mkErr == nil {
			return func() { _ = os.Remove(path) }, true, nil
		}
	}

	// A live peer is spawning; wait for it via retry-dial.
	return func() {}, false, nil
}

// retryDial dials repeatedly with exponential backoff until the daemon
// answers, the context is cancelled, or the total budget is exhausted.
func retryDial(ctx context.Context, dir string, dial dialFunc, cfg backoffConfig) (ports.Transport, error) {
	transport, err := retryAttempts(ctx, cfg, func() (ports.Transport, bool, error) {
		transport, err := dial(ctx, dir)
		return transport, err == nil, nil
	})
	if errors.Is(err, ErrDaemonUnreachable) {
		slog.Error("daemon did not become reachable before retry budget expired", "socket_dir", dir, "budget", cfg.total)
	}
	return transport, err
}

// realDial is the production dialer.
func realDial(ctx context.Context, dir string) (ports.Transport, error) {
	return ipc.DialContext(ctx, dir)
}

// realSpawn re-execs this binary through a short-lived launcher. Waiting for
// that launcher to exit ensures the daemon has been re-parented before the
// client continues, keeping process-tree termination from reaching it.
func realSpawn() error {
	exePath, err := selfExePath()
	if err != nil {
		return fmt.Errorf("resolving executable path: %w", err)
	}

	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening %s: %w", os.DevNull, err)
	}
	defer func() { _ = devNull.Close() }()

	var stderr bytes.Buffer
	cmd := exec.Command(exePath, "--daemon-launcher")
	cmd.Env = withoutPerformanceTraceEnv(os.Environ())
	cmd.Dir = platform.DirOrHome("")
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if detail := bytes.TrimSpace(stderr.Bytes()); len(detail) > 0 {
			return fmt.Errorf("%w: %s", err, detail)
		}
		return err
	}
	slog.Info("daemon process detached")
	return nil
}

func withoutPerformanceTraceEnv(env []string) []string {
	filtered := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		switch name {
		case "VEV_PERF_TRACE", "VEV_PERF_PROCESS_ID", "VEV_PERF_SCENARIO", "VEV_PERF_RUN":
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

// runDaemonLauncher is the intermediate half of the double-fork. It starts
// the long-lived daemon in a new session and exits immediately; realSpawn
// waits for this exit so the daemon is no longer descended from the client.
func runDaemonLauncher() error {
	exePath, err := selfExePath()
	if err != nil {
		return fmt.Errorf("resolving executable path: %w", err)
	}

	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening %s: %w", os.DevNull, err)
	}
	defer func() { _ = devNull.Close() }()

	cmd := exec.Command(exePath, "--daemon")
	cmd.Env = withoutPerformanceTraceEnv(os.Environ())
	cmd.Dir = platform.DirOrHome("")
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}
