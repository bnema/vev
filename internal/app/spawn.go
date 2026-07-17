package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/ipc"
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

// ensureDaemon returns a transport to a running daemon, spawning one if
// necessary. It first tries a plain dial; on failure it elects a single
// spawner via an mkdir lock (losers simply wait), then retry-dials with
// backoff until the socket is live or the budget expires.
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
	deadline := time.Now().Add(cfg.total)
	backoff := cfg.initial
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if t, err := dial(ctx, dir); err == nil {
			return t, nil
		}
		if !time.Now().Before(deadline) {
			slog.Error("daemon did not become reachable before retry budget expired", "socket_dir", dir, "budget", cfg.total)
			return nil, ErrDaemonUnreachable
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > cfg.max {
			backoff = cfg.max
		}
	}
}

// realDial is the production dialer.
func realDial(ctx context.Context, dir string) (ports.Transport, error) {
	return ipc.DialContext(ctx, dir)
}

// realSpawn re-execs this binary as a detached daemon: a new session
// (Setsid) with stdio bound to /dev/null so it survives the client's exit
// and never writes to the client's terminal. It logs via slog to the shared
// log file instead.
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

	cmd := exec.Command(exePath, "--daemon")
	cmd.Dir = platform.DirOrHome("")
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	slog.Info("daemon process started", "pid", cmd.Process.Pid)
	// Detach: we neither wait for nor signal the daemon after launch.
	return cmd.Process.Release()
}
