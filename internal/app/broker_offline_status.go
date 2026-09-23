package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

// Detached connect-or-spawn and status.
//
// `_broker-launcher` is the intermediate half of a double fork: it starts one
// hidden `_broker-serve` in a new session and exits immediately, exactly like
// the daemon launcher, so the broker is reparented before the status caller
// continues. `_broker-status` is the client-facing probe: without --ensure it
// is dial-only and renders the observable state, and with --ensure it
// coordinates a race-safe connect-or-spawn.
//
// Both commands are hidden from public help and change no ordinary command,
// path, or factory. They operate only inside the operator-supplied offline
// root; a root that overlaps the production runtime or state directories is
// refused before anything is created.
//
// Race safety rests on two exclusive descriptor-backed locks that are never
// stolen by age and whose inodes are never unlinked:
//
//   - the broker lifetime lock (`layout.Runtime/lifecycle.lock`) is held by the
//     foreground `_broker-serve` for its whole run; a status probe uses it only
//     to detect that a broker already owns the sandbox and therefore must not
//     be duplicated.
//   - the spawn-election lock (`layout.Spawn/lifecycle.lock`) elects exactly
//     one status caller to spawn. Holding it across the readiness wait keeps
//     late callers waiting on the winner instead of racing a second spawn; if
//     the winner dies, the kernel releases the lock, so a waiter can take over
//     without ever unlinking or age-stealing the lock file.
//
// The elected spawner holds the election until the endpoint answers a valid
// broker registration and publishes its initial snapshot, or until the one
// absolute overall deadline expires. A live-but-incompatible endpoint and a
// stalled handshake are diagnosed without spawning.

// productionBrokerLauncherCommand is the hidden intermediate that starts one
// detached broker serve process and exits.
const productionBrokerLauncherCommand = "_broker-production-launcher"

// Bounds for the detached connect-or-spawn path. Every wait is bounded by the
// one absolute overall deadline, and the first dial is bounded on its own so a
// stalled endpoint cannot consume the whole budget before classification.
const (
	brokerStatusDefaultTimeout = 10 * time.Second
	brokerStatusProbeBudget    = 2 * time.Second
	brokerStatusInitialBackoff = 5 * time.Millisecond
	brokerStatusMaxBackoff     = 50 * time.Millisecond
)

var (
	// errBrokerAbsent reports that no broker is bound at the endpoint, so a
	// spawn may recover it. A missing socket and a stale socket left by a dead
	// owner are both absence.
	errBrokerAbsent = errors.New("vev: broker endpoint is absent")
	// errBrokerIncompatible reports that the endpoint exists but is not a
	// compatible broker (a foreign path, a live endpoint that fails the broker
	// handshake, or a handshake that stalls past its bound). It is never an
	// invitation to spawn.
	errBrokerIncompatible = errors.New("vev: broker endpoint is not a compatible broker")
	// errBrokerNotReady reports that --ensure did not observe a ready broker
	// before the overall deadline.
	errBrokerNotReady = errors.New("vev: broker did not become ready")
)

// brokerStatusReport is the bounded JSON status document. It carries exactly
// the ready/offline state, the broker endpoint, and the broker's epoch,
// revision, and host count. Epoch, revision, and host count are zero when the
// broker is offline.
type brokerStatusReport struct {
	Status    string               `json:"status"`
	Endpoint  string               `json:"endpoint"`
	Epoch     ports.BrokerEpoch    `json:"epoch"`
	Revision  ports.BrokerRevision `json:"revision"`
	HostCount int                  `json:"host_count"`
}

// brokerStatusLock is one held exclusive election lock.
type brokerStatusLock interface {
	Release() error
}

// brokerStatusDeps are the injectable seams of `_broker-status`. Every seam has
// a production default; tests substitute them to drive the election, deadline,
// and classification deterministically.
type brokerStatusDeps struct {
	now             func() time.Time
	wait            func(context.Context, time.Duration) error
	probe           func(context.Context, string) (brokerStatusReport, error)
	acquireElection func(string) (brokerStatusLock, error)
	lifetimeOwner   func(string) (bool, error)
	spawn           func(ctx context.Context, root string, grace time.Duration, graceSet bool) error
	stdout          io.Writer
}

// defaultBrokerStatusDeps returns the production seams.
func defaultBrokerStatusDeps() brokerStatusDeps {
	return brokerStatusDeps{
		now:   time.Now,
		wait:  waitBackoff,
		probe: probeBrokerStatus,
		acquireElection: func(dir string) (brokerStatusLock, error) {
			return lifecycle.TryAcquire(dir)
		},
		lifetimeOwner: func(runtimeDir string) (bool, error) {
			owner, err := lifecycle.TryAcquire(runtimeDir)
			if err != nil {
				if errors.Is(err, lifecycle.ErrBusy) {
					return true, nil
				}
				return false, err
			}
			return false, owner.Release()
		},
		spawn: func(ctx context.Context, _ string, _ time.Duration, _ bool) error {
			return spawnProductionBrokerLauncher(ctx)
		},
		stdout: os.Stdout,
	}
}

// parseProductionBrokerLauncherArgs parses the argument-free hidden launcher entry.
func parseProductionBrokerLauncherArgs(args []string) (command, error) {
	if len(args) != 0 {
		return command{}, usagef("`%s` does not accept arguments", productionBrokerLauncherCommand)
	}
	return command{kind: kindProductionBrokerLauncher}, nil
}

// runProductionBrokerLauncherCommand starts one detached broker serve process.
func runProductionBrokerLauncherCommand(context.Context) error {
	return startDetachedBrokerServe([]string{productionBrokerServeCommand}, filepath.Join(productionBrokerLayout().Log, "vev-broker-crash.log"))
}

// startDetachedBrokerServe starts one `_broker-serve` in a new session and
// releases it, so the broker outlives the short-lived launcher. It mirrors the
// daemon launcher's double-fork boundary: no ambient variable is dropped beyond
// the performance-trace inputs, stdio is /dev/null, and the process is
// reparented immediately.
func startDetachedBrokerServe(args []string, crashPath string) error {
	exePath, err := selfExePath()
	if err != nil {
		return fmt.Errorf("vev: resolving executable path: %w", err)
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("vev: opening %s: %w", os.DevNull, err)
	}
	defer func() { _ = devNull.Close() }()

	cmd := exec.Command(exePath, args...)
	cmd.Env = withoutPerformanceTraceEnv(os.Environ())
	cmd.Dir = platform.DirOrHome("")
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	if crashPath == "" {
		cmd.Stderr = devNull
	} else {
		if err := safedir.EnsurePrivate(filepath.Dir(crashPath)); err != nil {
			return fmt.Errorf("vev: secure broker crash log directory: %w", err)
		}
		if err := os.Rename(crashPath, crashPath+".prev"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("vev: preserve previous broker crash log: %w", err)
		}
		crashLog, err := os.OpenFile(crashPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("vev: open broker crash log: %w", err)
		}
		defer func() { _ = crashLog.Close() }()
		cmd.Stderr = crashLog
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("vev: starting detached broker: %w", err)
	}
	return cmd.Process.Release()
}

// spawnBrokerLauncher runs the hidden launcher, bounded by ctx, and waits for
// it to exit, so the broker has been reparented before the caller retries the
// endpoint. exec.CommandContext is deliberate: the overall status deadline or a
// signal kills a launcher that is still waiting, while killing the launcher
// after it has started the detached `_broker-serve` does not touch that serve --
// it is in its own session and was released, so it survives.
func spawnProductionBrokerLauncher(ctx context.Context) error {
	return runBrokerLauncherProcess(ctx, []string{productionBrokerLauncherCommand})
}

func runBrokerLauncherProcess(ctx context.Context, args []string) error {
	exePath, err := selfExePath()
	if err != nil {
		return fmt.Errorf("vev: resolving executable path: %w", err)
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("vev: opening %s: %w", os.DevNull, err)
	}
	defer func() { _ = devNull.Close() }()

	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, exePath, args...)
	cmd.Env = withoutPerformanceTraceEnv(os.Environ())
	cmd.Dir = platform.DirOrHome("")
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if detail := bytes.TrimSpace(stderr.Bytes()); len(detail) > 0 {
			return fmt.Errorf("vev: launching broker: %w: %s", err, detail)
		}
		return fmt.Errorf("vev: launching broker: %w", err)
	}
	return nil
}

// brokerStatusRequest carries one ensure run's fixed inputs.
type brokerStatusRequest struct {
	layout     brokerconfig.Layout
	socketPath string
	deadline   time.Time
	timeout    time.Duration
	root       string
	grace      time.Duration
	graceSet   bool
}

// ensureBrokerReady connects to an already-ready broker or elects one spawner
// and holds the election until the endpoint answers a valid broker registration
// and publishes its initial snapshot, or until the absolute deadline expires.
func ensureBrokerReady(ctx context.Context, req brokerStatusRequest, deps brokerStatusDeps) (brokerStatusReport, error) {
	ctx, cancel := context.WithDeadline(ctx, req.deadline)
	defer cancel()

	// Secure the sandbox paths before any lock is taken: the spawn-election
	// directory lives beneath the runtime directory, and both must be private
	// and symlink-free before they can hold an exclusive lock.
	if err := safedir.EnsurePrivate(req.layout.Spawn); err != nil {
		return brokerStatusReport{}, fmt.Errorf("vev: secure broker sandbox spawn directory: %w", err)
	}
	if err := req.layout.VerifyCreated(); err != nil {
		return brokerStatusReport{}, fmt.Errorf("vev: verify broker sandbox paths: %w", err)
	}

	var held brokerStatusLock
	defer func() {
		if held != nil {
			_ = held.Release()
		}
	}()
	spawned := false
	backoff := brokerStatusInitialBackoff

	for {
		if err := ctx.Err(); err != nil {
			return brokerStatusReport{}, err
		}
		if !deps.now().Before(req.deadline) {
			return brokerStatusReport{}, fmt.Errorf("%w: endpoint %s did not answer Register within %s", errBrokerNotReady, req.socketPath, req.timeout)
		}
		report, err := probeWithin(ctx, deps, req.socketPath, req.deadline)
		if err == nil {
			return report, nil
		}
		if ctx.Err() != nil {
			return brokerStatusReport{}, ctx.Err()
		}
		// A probe that finished after the deadline is a bounded failure, never
		// an absence that would justify one more spawn.
		if !deps.now().Before(req.deadline) {
			return brokerStatusReport{}, fmt.Errorf("%w: endpoint %s did not answer Register within %s", errBrokerNotReady, req.socketPath, req.timeout)
		}
		if errors.Is(err, brokeripc.ErrBrokerRetired) {
			return brokerStatusReport{}, err
		}
		if !errors.Is(err, errBrokerAbsent) {
			return brokerStatusReport{}, fmt.Errorf("vev: broker endpoint %s is live but incompatible: %w; run \"vev kill --all\" and retry", req.socketPath, err)
		}

		if held == nil {
			lock, err := deps.acquireElection(req.layout.Spawn)
			switch {
			case err == nil:
				held = lock
				// Re-dial after winning the election: the previous holder may
				// already have published readiness.
				report, err := probeWithin(ctx, deps, req.socketPath, req.deadline)
				if err == nil {
					return report, nil
				}
				if ctx.Err() != nil {
					return brokerStatusReport{}, ctx.Err()
				}
				if errors.Is(err, brokeripc.ErrBrokerRetired) {
					return brokerStatusReport{}, err
				}
				if !errors.Is(err, errBrokerAbsent) {
					return brokerStatusReport{}, fmt.Errorf("vev: broker endpoint %s is live but incompatible: %w; run \"vev kill --all\" and retry", req.socketPath, err)
				}
			case errors.Is(err, lifecycle.ErrBusy):
				// Another caller is elected and spawning. Only the elected
				// spawner ever probes the broker lifetime lock, so a waiter can
				// never briefly steal ownership from the broker it is waiting for.
				if err := deps.wait(ctx, backoff); err != nil {
					return brokerStatusReport{}, err
				}
				backoff *= 2
				if backoff > brokerStatusMaxBackoff {
					backoff = brokerStatusMaxBackoff
				}
				continue
			default:
				return brokerStatusReport{}, fmt.Errorf("vev: elect broker spawner: %w", err)
			}
		}
		if held != nil && !spawned {
			busy, err := deps.lifetimeOwner(req.layout.Runtime)
			if err != nil {
				return brokerStatusReport{}, fmt.Errorf("vev: inspect broker lifetime owner: %w", err)
			}
			if !busy {
				if err := deps.spawn(ctx, req.root, req.grace, req.graceSet); err != nil {
					return brokerStatusReport{}, err
				}
				spawned = true
			}
		}

		if err := deps.wait(ctx, backoff); err != nil {
			return brokerStatusReport{}, err
		}
		backoff *= 2
		if backoff > brokerStatusMaxBackoff {
			backoff = brokerStatusMaxBackoff
		}
	}
}

// probeWithin bounds one probe by the smaller of the probe budget and the
// remaining overall time.
func probeWithin(ctx context.Context, deps brokerStatusDeps, socketPath string, deadline time.Time) (brokerStatusReport, error) {
	budget := brokerStatusProbeBudget
	if remaining := deadline.Sub(deps.now()); remaining < budget {
		budget = remaining
	}
	if budget <= 0 {
		return brokerStatusReport{}, context.DeadlineExceeded
	}
	probeCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return deps.probe(probeCtx, socketPath)
}

// probeBrokerStatus is the production probe: it classifies endpoint absence
// first, dials with the bounded broker setup, then waits for the broker to
// register the connection and publish its initial snapshot.
func probeBrokerStatus(ctx context.Context, socketPath string) (brokerStatusReport, error) {
	info, err := os.Lstat(socketPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return brokerStatusReport{}, absentError(socketPath)
	case err != nil:
		return brokerStatusReport{}, fmt.Errorf("vev: inspect broker endpoint %s: %w", socketPath, err)
	case info.Mode()&os.ModeSocket == 0:
		return brokerStatusReport{}, incompatibleError(socketPath, errors.New("path is not a Unix socket"))
	}

	service, err := brokeripc.Dial(ctx, socketPath, brokeripc.Config{})
	if errors.Is(err, brokeripc.ErrBrokerRetired) {
		return brokerStatusReport{}, err
	}
	if err != nil {
		if backendAbsent(err) {
			return brokerStatusReport{}, absentError(socketPath)
		}
		if ctx.Err() != nil {
			return brokerStatusReport{}, ctx.Err()
		}
		// The endpoint may have retired between the stat and the dial; re-check
		// so a broker that just shut down is classified as absent, not as an
		// incompatible endpoint that must never be respawned.
		if _, statErr := os.Lstat(socketPath); errors.Is(statErr, fs.ErrNotExist) {
			return brokerStatusReport{}, absentError(socketPath)
		}
		return brokerStatusReport{}, incompatibleError(socketPath, err)
	}
	defer func() { _ = service.Close() }()

	sub, err := service.Subscribe()
	if err != nil {
		return brokerStatusReport{}, incompatibleError(socketPath, err)
	}
	defer sub.Close()
	changed := sub.Changed()
	for {
		if snapshot := service.Snapshot(); snapshot.Revision != 0 {
			return readyBrokerStatus(socketPath, snapshot), nil
		}
		select {
		case <-ctx.Done():
			return brokerStatusReport{}, ctx.Err()
		case _, ok := <-changed:
			if ok {
				continue
			}
			if snapshot := service.Snapshot(); snapshot.Revision != 0 {
				return readyBrokerStatus(socketPath, snapshot), nil
			}
			return brokerStatusReport{}, incompatibleError(socketPath, errors.New("connection ended before the initial snapshot"))
		}
	}
}

// readyBrokerStatus projects one committed snapshot into the ready report.
// HostCount counts configured remote hosts; the local daemon observation is
// not a configured host.
func readyBrokerStatus(socketPath string, snapshot ports.BrokerSnapshot) brokerStatusReport {
	hosts := 0
	for _, daemon := range snapshot.Daemons {
		if !daemon.Local {
			hosts++
		}
	}
	return brokerStatusReport{
		Status:    "ready",
		Endpoint:  socketPath,
		Epoch:     snapshot.Epoch,
		Revision:  snapshot.Revision,
		HostCount: hosts,
	}
}

// backendAbsent reports whether one dial failure means no broker is bound:
// a missing socket or a stale socket whose dead owner left the pathname behind.
// Any other dial failure is a live endpoint that must not be respawned.
func backendAbsent(err error) bool {
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		return false
	}
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED)
}

// absentError wraps one recoverable-absence diagnosis.
func absentError(socketPath string) error {
	return fmt.Errorf("%w: %s", errBrokerAbsent, socketPath)
}

// incompatibleError wraps one live-but-incompatible diagnosis.
func incompatibleError(socketPath string, cause error) error {
	return fmt.Errorf("%w: %s: %w", errBrokerIncompatible, socketPath, cause)
}
