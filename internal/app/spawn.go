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

	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/pkg/safedir"
)

// ErrDaemonUnreachable is returned when the daemon socket never becomes
// dialable within the retry budget, despite a spawn attempt.
var ErrDaemonUnreachable = errors.New("vev: daemon did not become reachable")

// ErrLifecycleHeldByOther reports that another process kept the daemon
// lifecycle lock for the whole start budget without publishing the carriage:
// typically an older vev daemon that predates the broker. Spawning cannot
// recover this; the user has to stop that process.
var ErrLifecycleHeldByOther = errors.New("vev: another vev daemon holds the lifecycle lock but does not answer; stop the old daemon (for example `pkill -f -- '--daemon'`) and retry")

// spawnLockName is the mkdir-based lock directory guarding daemon spawn, so
// concurrent first-clients elect a single spawner instead of racing to
// re-exec multiple daemons.
const spawnLockName = "spawn.lock"

// staleLockAge bounds how long a spawn lock may persist before it is treated
// as abandoned (spawner crashed mid-flight) and forcibly taken over.
const staleLockAge = 10 * time.Second

// dialFunc dials the daemon socket in dir. Injected so tests can drive the
// spawn/backoff logic without a real socket.
type dialFunc func(ctx context.Context, dir string) (wire.Transport, error)

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

// waitForTargetOrLifecycle is the shared election primitive behind every
// connect-or-spawn path: it repeatedly dials the target and probes lifecycle
// ownership in lockDir until one of the two is available, a non-contention
// failure is reported, or the caller's context or the retry budget ends. It is
// generic over the dial result so the session transport and the daemonmux
// carriage obey exactly one election discipline instead of two that could drift
// apart. The dial closure and lockDir stay separate because the daemon's
// lifecycle lock lives in its runtime directory while a session transport and
// a daemonmux carriage are two different endpoints of the same daemon.
func waitForTargetOrLifecycle[T any](ctx context.Context, lockDir string, dial func(ctx context.Context) (T, error), cfg backoffConfig) (T, lifecycleOwnership, error) {
	type targetOrLifecycle struct {
		target T
		owner  lifecycleOwnership
	}
	busy := false
	result, err := retryAttempts(ctx, cfg, func() (targetOrLifecycle, bool, error) {
		if target, err := dial(ctx); err == nil {
			return targetOrLifecycle{target: target}, true, nil
		}
		owner, err := daemonLifecycleProbe.TryAcquire(lockDir)
		if err == nil {
			return targetOrLifecycle{owner: owner}, true, nil
		}
		if !errors.Is(err, lifecycle.ErrBusy) {
			return targetOrLifecycle{}, false, err
		}
		busy = true
		return targetOrLifecycle{}, false, nil
	})
	if busy && errors.Is(err, ErrDaemonUnreachable) {
		// The lock stayed held and the target never answered for the whole
		// budget: a live owner that is not the daemon this binary expects.
		err = fmt.Errorf("%w (%w)", ErrLifecycleHeldByOther, err)
	}
	return result.target, result.owner, err
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
// owner may still be initializing or tearing down durable state. It is the
// session-transport spelling of the shared election primitive.
func ensureDaemonWithLifecycle(ctx context.Context, dir string, dial dialFunc, spawn spawnFunc, cfg backoffConfig) (wire.Transport, error) {
	return ensureTargetWithLifecycle(ctx, dir, func(ctx context.Context) (wire.Transport, error) { return dial(ctx, dir) }, spawn, cfg)
}

// ensureMuxDaemon returns a raw framed carriage to a running daemon, electing
// exactly one spawner under the same lifecycle and spawn-lock discipline the
// public session socket already uses. lockDir owns the daemon's lifecycle and
// spawn locks; carriage is the private daemonmux endpoint the dial closure
// reaches. Its readiness proof is the carriage itself: it never dials a
// session, opens a session control stream, or otherwise asks the daemon a
// session question.
func ensureMuxDaemon(ctx context.Context, lockDir, carriage string, dial func(ctx context.Context, carriage string) (daemonmux.RawFramedTransport, error), spawn spawnFunc, cfg backoffConfig) (daemonmux.RawFramedTransport, error) {
	return ensureTargetWithLifecycle(ctx, lockDir, func(ctx context.Context) (daemonmux.RawFramedTransport, error) { return dial(ctx, carriage) }, spawn, cfg)
}

// ensureTargetWithLifecycle is the shared connect-or-spawn election: one dial,
// then lifecycle availability, then exactly one elected spawner, then a bounded
// redial of the same target. It never spawns while another lifecycle owner may
// still be initializing or tearing down durable state, and it returns the
// dialed value unchanged.
func ensureTargetWithLifecycle[T any](ctx context.Context, lockDir string, dial func(ctx context.Context) (T, error), spawn spawnFunc, cfg backoffConfig) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if target, err := dial(ctx); err == nil {
		return target, nil
	}
	target, owner, err := waitForTargetOrLifecycle(ctx, lockDir, dial, cfg)
	if err != nil {
		return zero, err
	}
	if any(target) != nil {
		// The target is an interface (a session transport or a raw mux
		// carriage), so this is the same nil test the typed spelling used: a
		// non-nil interface value means the dial succeeded.
		return target, nil
	}
	if err := owner.Release(); err != nil {
		return zero, fmt.Errorf("vev: release lifecycle spawn probe: %w", err)
	}
	return ensureTarget(ctx, lockDir, dial, spawn, cfg)
}

// ensureTarget is the shared spawn election: it dials once, then takes the
// spawn lock in lockDir, and only the elected winner spawns. Every dialer —
// the session transport and the daemonmux carriage — therefore observes the
// same single-spawner guarantee.
func ensureTarget[T any](ctx context.Context, lockDir string, dial func(ctx context.Context) (T, error), spawn spawnFunc, cfg backoffConfig) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if t, err := dial(ctx); err == nil {
		slog.Debug("daemon already reachable", "socket_dir", lockDir)
		return t, nil
	}

	release, acquired, err := acquireSpawnLock(lockDir)
	if err != nil {
		return zero, fmt.Errorf("vev: acquiring spawn lock: %w", err)
	}
	if acquired {
		// We won the election: spawn, and hold the lock until the socket is
		// dialable (or spawn fails) so late-arriving clients keep waiting
		// rather than spawning a second daemon.
		defer release()
		slog.Info("spawning daemon", "socket_dir", lockDir)
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if err := spawn(); err != nil {
			slog.Error("daemon spawn failed", "err", err)
			return zero, fmt.Errorf("vev: spawning daemon: %w", err)
		}
	} else {
		slog.Debug("waiting for daemon spawned by another process", "socket_dir", lockDir)
	}

	return retryTarget(ctx, lockDir, dial, cfg)
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

// retryTarget is the shared bounded redial: it repeats the same dial with
// exponential backoff until it answers, the caller's context ends, or the total
// budget is exhausted. It is generic so the daemonmux carriage retries under
// exactly the budget the session transport uses.
func retryTarget[T any](ctx context.Context, lockDir string, dial func(ctx context.Context) (T, error), cfg backoffConfig) (T, error) {
	var lastDialErr error
	target, err := retryAttempts(ctx, cfg, func() (T, bool, error) {
		target, dialErr := dial(ctx)
		if dialErr != nil {
			lastDialErr = dialErr
		}
		return target, dialErr == nil, nil
	})
	if errors.Is(err, ErrDaemonUnreachable) {
		slog.Error("daemon did not become reachable before retry budget expired", "socket_dir", lockDir, "budget", cfg.total, "last_dial_error", lastDialErr)
		if lastDialErr != nil {
			err = fmt.Errorf("%w: last dial: %v", ErrDaemonUnreachable, lastDialErr)
		}
	}
	return target, err
}

// realDial is the production dialer.
func realDial(ctx context.Context, dir string) (wire.Transport, error) {
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
