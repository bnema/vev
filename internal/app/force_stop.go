package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
)

// Force-stopping is the interactive last resort for a daemon that still owns
// lifecycle.lock but cannot be stopped through the typed kill protocol, for
// example when `go install` replaced the binary while a `/proc/self/exe
// --daemon` process kept running under the name "exe". Ownership is never
// inferred from a process name: the candidate must be a same-user process that
// holds lifecycle.lock open and runs as a vev daemon, and its incarnation is
// re-verified before any escalation so a reused PID is never signalled.

const (
	// forceStopSIGTERMWait bounds how long SIGTERM has to release lifecycle
	// ownership before the fallback escalates exactly once to SIGKILL.
	forceStopSIGTERMWait = 5 * time.Second
	// forceStopSIGKILLWait bounds how long SIGKILL has to release lifecycle
	// ownership. Escalation never repeats.
	forceStopSIGKILLWait = 5 * time.Second
	// forceStopPollInterval is the ownership-transfer poll cadence.
	forceStopPollInterval = 50 * time.Millisecond
)

var (
	// errForceStopNotApplicable reports that no force-stop was offered because
	// lifecycle ownership was already free or no verified daemon process holds
	// it. Callers keep their original diagnosis in that case.
	errForceStopNotApplicable = errors.New("no verified daemon process holds lifecycle ownership")
	// errForceStopDeclined reports an explicit or default refusal at the prompt.
	errForceStopDeclined = errors.New("vev: force stop declined; daemon left running")
	// errForceStopTimeout reports that the daemon never released lifecycle
	// ownership within the bounded escalation policy.
	errForceStopTimeout = errors.New("daemon did not release lifecycle ownership")
)

// daemonProcess is a verified, same-user process candidate.
type daemonProcess struct {
	PID     int
	Command string // display only; never used to infer ownership
	// Start is an opaque OS-provided process start marker. It distinguishes
	// incarnations so a reused PID is never signalled.
	Start string
}

// daemonProcessFinder locates the process holding lifecycle ownership. It must
// prove ownership from the process's open file descriptors, not from its name.
type daemonProcessFinder interface {
	// Find returns the same-user process that holds lifecycle.lock open and
	// runs as a vev daemon. ok is false when no such process exists.
	Find(ctx context.Context, runtimeDir string) (daemonProcess, bool, error)
}

// hasDaemonArgument reports whether args contain the exact daemon flag. It
// complements, and never replaces, the verified lock-holder proof.
func hasDaemonArgument(args []string) bool {
	for _, arg := range args {
		if arg == "--daemon" {
			return true
		}
	}
	return false
}

// forceStopHooks carries every side effect so policy tests never signal a real
// process or read the real terminal.
type forceStopHooks struct {
	probe  lifecycleProbe
	finder daemonProcessFinder
	signal func(pid int, sig syscall.Signal) error
	now    func() time.Time
	sleep  func(ctx context.Context, d time.Duration) error
	stdin  io.Reader
	stdout io.Writer
}

func defaultForceStopHooks() forceStopHooks {
	return forceStopHooks{
		probe:  daemonLifecycleProbe,
		finder: platformDaemonProcessFinder{},
		signal: syscall.Kill,
		now:    time.Now,
		sleep:  forceStopSleep,
		stdin:  os.Stdin,
		stdout: os.Stdout,
	}
}

func forceStopSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// forceStopDaemon returns nil once the daemon was stopped through the
// interactive fallback and its stale runtime artifacts were removed. It returns
// errForceStopNotApplicable without any side effect when the fallback does not
// apply, and errForceStopDeclined when the operator refused.
func forceStopDaemon(ctx context.Context, runtimeDir string, hooks forceStopHooks) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Ownership must already be busy: a free lock means there is no daemon to
	// force-stop and the caller's normal path applies.
	owner, err := hooks.probe.TryAcquire(runtimeDir)
	if err == nil {
		return errors.Join(errForceStopNotApplicable, owner.Release())
	}
	if !errors.Is(err, lifecycle.ErrBusy) {
		return fmt.Errorf("vev: probing daemon ownership: %w", err)
	}

	candidate, ok, err := hooks.finder.Find(ctx, runtimeDir)
	if err != nil {
		return fmt.Errorf("vev: identifying daemon process: %w", err)
	}
	if !ok {
		return errForceStopNotApplicable
	}

	confirmed, err := hooks.confirm(fmt.Sprintf(
		"vev: daemon process %d (%s) still owns %s after the typed kill request failed.\n"+
			"vev: it may be incompatible or unreachable. Send SIGTERM to process %d?",
		candidate.PID, candidate.Command, lifecycle.Path(runtimeDir), candidate.PID))
	if err != nil {
		return err
	}
	if !confirmed {
		return errForceStopDeclined
	}

	// The prompt is interactive and may stay open arbitrarily long, so the
	// confirmed incarnation is re-verified before SIGTERM exactly as it is
	// before SIGKILL: a PID reused while the operator was answering is never
	// signalled.
	current, found, findErr := hooks.finder.Find(ctx, runtimeDir)
	if findErr != nil {
		return fmt.Errorf("vev: re-identifying daemon process: %w", findErr)
	}
	if !found || current.PID != candidate.PID || current.Start != candidate.Start {
		return fmt.Errorf("vev: %w (process identity changed before SIGTERM)", errForceStopTimeout)
	}

	if err := hooks.signal(candidate.PID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("vev: sending SIGTERM to process %d: %w", candidate.PID, err)
	}

	owner, err = forceStopAwaitOwnership(ctx, runtimeDir, forceStopSIGTERMWait, hooks)
	if err != nil {
		if !errors.Is(err, errForceStopTimeout) {
			return err
		}
		// Escalate exactly once, and only for the exact incarnation that was
		// verified. A PID reused by another process is never signalled.
		current, found, findErr := hooks.finder.Find(ctx, runtimeDir)
		if findErr != nil {
			return fmt.Errorf("vev: re-identifying daemon process: %w", findErr)
		}
		if !found || current.PID != candidate.PID || current.Start != candidate.Start {
			return fmt.Errorf("vev: %w (process identity changed before SIGKILL)", errForceStopTimeout)
		}
		if err := hooks.signal(candidate.PID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("vev: sending SIGKILL to process %d: %w", candidate.PID, err)
		}
		owner, err = forceStopAwaitOwnership(ctx, runtimeDir, forceStopSIGKILLWait, hooks)
		if err != nil {
			return fmt.Errorf("vev: %w after SIGKILL", err)
		}
	}
	// Holding lifecycle ownership proves no daemon is starting or listening, so
	// removing runtime artifacts cannot race a live daemon.
	defer func() { retErr = errors.Join(retErr, owner.Release()) }()
	if err := removeStaleRuntimeArtifacts(runtimeDir, hooks); err != nil {
		return err
	}
	_, err = fmt.Fprintf(hooks.stdout, "vev: stopped daemon process %d and removed stale runtime artifacts\n", candidate.PID)
	return err
}

// forceStopAwaitOwnership polls until lifecycle ownership is free, then returns
// it so the caller can keep it through cleanup.
func forceStopAwaitOwnership(ctx context.Context, runtimeDir string, wait time.Duration, hooks forceStopHooks) (lifecycleOwnership, error) {
	deadline := hooks.now().Add(wait)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		owner, err := hooks.probe.TryAcquire(runtimeDir)
		if err == nil {
			return owner, nil
		}
		if !errors.Is(err, lifecycle.ErrBusy) {
			return nil, err
		}
		if !hooks.now().Before(deadline) {
			return nil, errForceStopTimeout
		}
		if err := hooks.sleep(ctx, forceStopPollInterval); err != nil {
			return nil, err
		}
	}
}

// removeStaleRuntimeArtifacts deletes only the runtime socket and an abandoned
// spawn lock. It never touches durable state: sessions.kv, snapshots,
// configuration, and logs live under the state directory and are not candidates
// here.
func removeStaleRuntimeArtifacts(runtimeDir string, hooks forceStopHooks) error {
	var errs []error
	if err := os.Remove(ipc.SocketPath(runtimeDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, fmt.Errorf("vev: removing stale daemon socket: %w", err))
	}
	spawnPath := filepath.Join(runtimeDir, spawnLockName)
	info, err := os.Stat(spawnPath)
	switch {
	case err == nil:
		// A fresh spawn lock may belong to a concurrent client that is waiting
		// for a daemon; only abandoned locks are removed.
		if hooks.now().Sub(info.ModTime()) > staleLockAge {
			if err := os.RemoveAll(spawnPath); err != nil {
				errs = append(errs, fmt.Errorf("vev: removing stale spawn lock: %w", err))
			}
		}
	case errors.Is(err, os.ErrNotExist):
	default:
		errs = append(errs, fmt.Errorf("vev: inspecting spawn lock: %w", err))
	}
	return errors.Join(errs...)
}

// confirm prints a y/N prompt and returns true only for an explicit yes. Empty
// input, EOF, and every other reply default to refusal.
func (h forceStopHooks) confirm(question string) (bool, error) {
	if _, err := fmt.Fprintf(h.stdout, "%s [y/N] ", question); err != nil {
		return false, fmt.Errorf("vev: writing force-stop prompt: %w", err)
	}
	line, err := bufio.NewReader(h.stdin).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("vev: reading force-stop confirmation: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

// forceStopDaemonFn is the interactive last-resort hook used by runKill. Tests
// replace it to exercise the fallback wiring without a real daemon process.
var forceStopDaemonFn = func(ctx context.Context) error {
	return forceStopDaemon(ctx, ipc.SocketDir(), defaultForceStopHooks())
}

// forceStopDaemonFallback offers the interactive force-stop after the typed
// KillDaemon protocol failed. When no verified daemon process holds lifecycle
// ownership, the original failure is returned unchanged so the caller keeps its
// own diagnosis.
func forceStopDaemonFallback(ctx context.Context, cause error) error {
	err := forceStopDaemonFn(ctx)
	if errors.Is(err, errForceStopNotApplicable) {
		return cause
	}
	return err
}
