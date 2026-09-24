package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
)

// Real-subprocess coverage for the hidden launcher and status entry points.
//
// These tests re-execute the test binary through the production dispatch path:
// TestMain intercepts the hidden broker commands when brokerHelperEnv is set and
// runs app.Run, so `_broker-status` really elects a spawner, `_broker-launcher`
// really double-forks, and `_broker-serve` really composes the offline broker.
// Every helper records its PID under brokerHelperRecordDirEnv, so a test can
// observe how many processes were spawned and terminate every one of them.

const (
	brokerHelperEnv          = "VEV_BROKER_HELPER"
	brokerHelperRecordDirEnv = "VEV_BROKER_HELPER_RECORD_DIR"
)

// Controlled-launcher test env: when set, the hidden `_broker-launcher` helper
// records its PID and waits for the release marker instead of running the real
// double-fork, so a test can hold a launcher open and prove the spawn wait is
// bounded by its context.
const (
	brokerLauncherBlockRecordEnv  = "VEV_BROKER_LAUNCHER_BLOCK_RECORD"
	brokerLauncherBlockReleaseEnv = "VEV_BROKER_LAUNCHER_BLOCK_RELEASE"
)

// recordBrokerHelperProcess appends one helper process's PID, so a test can
// discover and later terminate a detached broker or launcher.
func recordBrokerHelperProcess(role string) {
	dir := os.Getenv(brokerHelperRecordDirEnv)
	if dir == "" {
		return
	}
	file, err := os.OpenFile(filepath.Join(dir, role+".pids"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer func() { _ = file.Close() }()
	_, _ = fmt.Fprintf(file, "%d\n", os.Getpid())
}

// shortTempDir creates one temporary directory with a short absolute path.
//
// The AF_UNIX pathname limit is about 103 bytes, and t.TempDir embeds the full
// test name; the broker socket lives several components beneath the sandbox
// root, so a long test name would overflow the socket path. A short base keeps
// the real subprocess tests honest. It uses /tmp because macOS TMPDIR is
// already a long /var/folders path.
func shortTempDir(t *testing.T, prefix string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", prefix)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// brokerSubprocessSandbox isolates one real offline root and records every
// spawned helper process.
type brokerSubprocessSandbox struct {
	t         *testing.T
	layout    brokerconfig.Layout
	recordDir string
}

// newBrokerSubprocessSandbox isolates XDG roots, writes one marker-valid empty
// sandbox, and arranges cleanup of every recorded broker and launcher process.
func newBrokerSubprocessSandbox(t *testing.T, idleGrace string) *brokerSubprocessSandbox {
	t.Helper()
	layout := emptyProductionBrokerLayout(t, idleGrace)
	recordDir := filepath.Join(shortTempDir(t, "vevr"), "r")
	require.NoError(t, os.MkdirAll(recordDir, 0o700))
	t.Setenv(brokerHelperEnv, "1")
	t.Setenv(brokerHelperRecordDirEnv, recordDir)
	sandbox := &brokerSubprocessSandbox{t: t, layout: layout, recordDir: recordDir}
	t.Cleanup(sandbox.cleanup)
	return sandbox
}

// ensure uses the production connect-or-spawn entry point and observes the
// registered epoch after the broker publishes its initial snapshot.
func (s *brokerSubprocessSandbox) ensure(timeout time.Duration) (brokerStatusReport, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	service, err := connectProductionBroker(ctx)
	if err != nil {
		return brokerStatusReport{}, err
	}
	defer service.Close()
	return probeBrokerStatus(ctx, s.socketPath())
}

// runBrokerCommand executes one helper through the test binary.
func runBrokerCommand(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = os.Environ()
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run broker command %v: %v", args, err)
		}
		code = exitErr.ExitCode()
	}
	return out.String(), errb.String(), code
}

// socketPath is the broker endpoint inside the sandbox runtime directory.
func (s *brokerSubprocessSandbox) socketPath() string {
	return brokeripc.SocketPath(s.layout.Runtime)
}

// recorded returns every PID a role recorded, in spawn order.
func (s *brokerSubprocessSandbox) recorded(role string) []int {
	s.t.Helper()
	raw, err := os.ReadFile(filepath.Join(s.recordDir, role+".pids"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(s.t, err)
	var pids []int
	for _, field := range strings.Fields(string(raw)) {
		pid, convErr := strconv.Atoi(field)
		require.NoError(s.t, convErr)
		pids = append(pids, pid)
	}
	return pids
}

// live returns the recorded PIDs that still exist.
func (s *brokerSubprocessSandbox) live(role string) []int {
	s.t.Helper()
	var live []int
	for _, pid := range s.recorded(role) {
		if err := syscall.Kill(pid, 0); err == nil || errors.Is(err, syscall.EPERM) {
			live = append(live, pid)
		}
	}
	return live
}

// terminate signals every recorded PID of one role and waits for each exit.
func (s *brokerSubprocessSandbox) terminate(role string, signal syscall.Signal) {
	s.t.Helper()
	for _, pid := range s.recorded(role) {
		if err := syscall.Kill(pid, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
			s.t.Errorf("signal %v pid %d: %v", signal, pid, err)
			continue
		}
		require.NoError(s.t, waitForProcessExit(pid, 10*time.Second))
	}
}

// waitForSocketGone waits until the broker endpoint no longer exists.
func (s *brokerSubprocessSandbox) waitForSocketGone(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Lstat(s.socketPath()); errors.Is(err, os.ErrNotExist) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// cleanup terminates every recorded broker and launcher so no test leaks an
// orphan process.
func (s *brokerSubprocessSandbox) cleanup() {
	for _, role := range []string{productionBrokerServeCommand, productionBrokerLauncherCommand} {
		for _, pid := range s.recorded(role) {
			if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				continue
			}
			_ = waitForProcessExit(pid, 10*time.Second)
		}
	}
}

// TestBrokerStatusSubprocessElectsExactlyOneBroker races several real status
// processes against one socket: exactly one broker process, one lifetime owner,
// and one epoch must result.
func TestBrokerStatusSubprocessElectsExactlyOneBroker(t *testing.T) {
	sandbox := newBrokerSubprocessSandbox(t, "5s")
	const racers = 4
	type outcome struct {
		report brokerStatusReport
		err    error
	}
	results := make([]outcome, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index].report, results[index].err = sandbox.ensure(20 * time.Second)
		}(i)
	}
	wg.Wait()
	epochs := make(map[uint64]struct{}, racers)
	for _, result := range results {
		require.NoError(t, result.err)
		require.Equal(t, "ready", result.report.Status)
		require.Equal(t, sandbox.socketPath(), result.report.Endpoint)
		epochs[uint64(result.report.Epoch)] = struct{}{}
	}
	require.Len(t, epochs, 1)
	require.Len(t, sandbox.recorded(productionBrokerServeCommand), 1)
	require.Len(t, sandbox.live(productionBrokerServeCommand), 1)
}

// TestBrokerStatusSubprocessIdleExitRestartsWithNewEpoch lets the broker idle
// out, then restarts it on demand and proves the new process samples a fresh
// epoch.
func TestBrokerStatusSubprocessIdleExitRestartsWithNewEpoch(t *testing.T) {
	sandbox := newBrokerSubprocessSandbox(t, "2s")

	first, err := sandbox.ensure(20 * time.Second)
	require.NoError(t, err)
	require.Equal(t, "ready", first.Status)
	require.Positive(t, first.Epoch)

	require.True(t, sandbox.waitForSocketGone(15*time.Second), "the idle broker must shut itself down")
	for _, pid := range sandbox.recorded(productionBrokerServeCommand) {
		require.NoError(t, waitForProcessExit(pid, 10*time.Second), "the idle broker must exit")
	}
	require.Empty(t, sandbox.live(productionBrokerServeCommand), "the idle broker must exit")

	second, err := sandbox.ensure(20 * time.Second)
	require.NoError(t, err)
	require.Equal(t, "ready", second.Status)
	require.NotEqual(t, first.Epoch, second.Epoch, "an on-demand restart must sample a new epoch")
	require.Len(t, sandbox.recorded(productionBrokerServeCommand), 2, "one broker per ensure is expected")
	require.Len(t, sandbox.live(productionBrokerServeCommand), 1, "only the restarted broker may remain")
}

// TestBrokerStatusSubprocessRecoversStaleSocket proves a stale socket left by a
// dead owner is recoverable absence, not an incompatible endpoint.
func TestBrokerStatusSubprocessRecoversStaleSocket(t *testing.T) {
	sandbox := newBrokerSubprocessSandbox(t, "3s")
	require.NoError(t, os.MkdirAll(sandbox.layout.Runtime, 0o700))
	bindStaleUnixSocket(t, sandbox.socketPath())

	report, err := sandbox.ensure(20 * time.Second)
	require.NoError(t, err)
	require.Equal(t, "ready", report.Status)
	require.Len(t, sandbox.live(productionBrokerServeCommand), 1)
}

// TestBrokerStatusSubprocessWaitsForLiveSpawnerThenRecovers holds the
// descriptor-backed election lock like a spawner that died mid-startup: the
// waiter must not spawn, and must take over once the lock is released.
func TestBrokerStatusSubprocessWaitsForLiveSpawnerThenRecovers(t *testing.T) {
	sandbox := newBrokerSubprocessSandbox(t, "3s")

	spawner, err := lifecycle.TryAcquire(sandbox.layout.Spawn)
	require.NoError(t, err)

	_, err = sandbox.ensure(500 * time.Millisecond)
	require.Error(t, err)
	require.Empty(t, sandbox.recorded(productionBrokerServeCommand), "a waiter must not spawn while another spawner is live")

	// The kernel releases the spawner's descriptor when it dies; releasing here
	// is exactly that event.
	require.NoError(t, spawner.Release())
	report, err := sandbox.ensure(20 * time.Second)
	require.NoError(t, err)
	require.Equal(t, "ready", report.Status)
	require.Len(t, sandbox.live(productionBrokerServeCommand), 1)
}

// TestBrokerStatusSubprocessSilentEndpointFailsWithoutSpawning proves a live
// endpoint that never answers the broker handshake is diagnosed as
// incompatible under the probe budget, and never respawned over.
func TestBrokerStatusSubprocessSilentEndpointFailsWithoutSpawning(t *testing.T) {
	sandbox := newBrokerSubprocessSandbox(t, "3s")
	require.NoError(t, os.MkdirAll(sandbox.layout.Runtime, 0o700))
	// A listener that accepts and never answers: the broker preamble stalls.
	_ = bindUnixSocket(t, sandbox.socketPath())

	// The timeout must exceed the probe budget so the stall is classified as a
	// live-but-incompatible endpoint instead of exhausting the overall deadline.
	start := time.Now()
	_, err := sandbox.ensure(5 * time.Second)
	require.ErrorContains(t, err, "live but incompatible")
	require.Less(t, time.Since(start), 5*time.Second, "the probe budget must classify a silent endpoint before the overall deadline")
	require.Empty(t, sandbox.recorded(productionBrokerServeCommand), "a live-but-silent endpoint must not be respawned over")
}

// TestBrokerLauncherSpawnWaitIsBoundedByContext proves a controlled blocking
// launcher is terminated when the spawn context is cancelled, so the overall
// deadline/signal bounds the launcher wait. The helper never starts a detached
// serve, so this is purely the bounded-wait half; other subprocess tests prove a
// successful launcher's detached serve survives.
func TestBrokerLauncherSpawnWaitIsBoundedByContext(t *testing.T) {
	record := filepath.Join(shortTempDir(t, "vevl"), "launcher.pid")
	release := filepath.Join(t.TempDir(), "release")
	t.Setenv(brokerHelperEnv, "1")
	t.Setenv(brokerLauncherBlockRecordEnv, record)
	t.Setenv(brokerLauncherBlockReleaseEnv, release)
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- spawnProductionBrokerLauncher(ctx) }()

	pid := waitForProcessRecord(t, record)
	select {
	case err := <-result:
		t.Fatalf("spawnBrokerLauncher returned while the controlled launcher %d was still running: %v", pid, err)
	default:
	}

	cancel()
	select {
	case err := <-result:
		require.Error(t, err, "cancelling the context must abort the launcher wait")
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling the context must bound the launcher spawn wait")
	}
	require.NoError(t, waitForProcessExit(pid, 5*time.Second), "CommandContext must terminate a blocking launcher")
}

// TestBrokerStatusSubprocessLauncherDiagnosesConflicts proves an explicit
// launcher grace that conflicts with the provisioned one is refused before any
// process is spawned, and that a missing configuration fails the launcher.
// A production launcher accepts no idle override; malformed broker.json is
// diagnosed by the detached serve before it can claim the lifetime lock.
func TestBrokerStatusSubprocessLauncherDiagnosesConflicts(t *testing.T) {
	t.Run("unsupported idle override", func(t *testing.T) {
		sandbox := newBrokerSubprocessSandbox(t, "90s")
		_, stderr, code := runBrokerCommand(t, productionBrokerLauncherCommand, "--idle-grace", "1m")
		require.NotZero(t, code)
		require.Contains(t, stderr, "does not accept arguments")
		require.Empty(t, sandbox.recorded(productionBrokerServeCommand))
	})
	t.Run("invalid production configuration", func(t *testing.T) {
		sandbox := newBrokerSubprocessSandbox(t, "")
		require.NoError(t, os.WriteFile(productionBrokerConfigPath(), []byte("not json"), 0o600))
		_, stderr, code := runBrokerCommand(t, productionBrokerServeCommand)
		require.NotZero(t, code)
		require.Contains(t, stderr, brokerconfig.ConfigFileName)
		require.NoFileExists(t, filepath.Join(sandbox.layout.Runtime, "lifecycle.lock"))
	})
}

// TestBrokerStatusSubprocessNoOrphanProcess proves the launcher exits on its
// own, one broker remains, and terminating it leaves no recorded process live.
func TestBrokerStatusSubprocessNoOrphanProcess(t *testing.T) {
	sandbox := newBrokerSubprocessSandbox(t, "5s")

	report, err := sandbox.ensure(20 * time.Second)
	require.NoError(t, err)
	require.Equal(t, "ready", report.Status)

	require.NotEmpty(t, sandbox.recorded(productionBrokerLauncherCommand))
	for _, pid := range sandbox.recorded(productionBrokerLauncherCommand) {
		require.NoError(t, waitForProcessExit(pid, 5*time.Second), "the short-lived launcher must exit")
	}
	require.Len(t, sandbox.live(productionBrokerServeCommand), 1)

	_, err = lifecycle.TryAcquire(sandbox.layout.Runtime)
	require.ErrorIs(t, err, lifecycle.ErrBusy, "the live broker must own the lifetime lock")

	sandbox.terminate(productionBrokerServeCommand, syscall.SIGTERM)
	require.Empty(t, sandbox.live(productionBrokerLauncherCommand), "no launcher may be left behind")
	require.Empty(t, sandbox.live(productionBrokerServeCommand), "no broker may be left behind")
	require.True(t, sandbox.waitForSocketGone(5*time.Second), "an orderly broker shutdown must unlink its socket")
	owner, err := lifecycle.TryAcquire(sandbox.layout.Runtime)
	require.NoError(t, err, "the terminated broker must release the lifetime lock")
	require.NoError(t, owner.Release())
}

// recordedPIDsIn reads one role's PID record from an arbitrary directory.
func recordedPIDsIn(t *testing.T, dir, role string) []int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, role+".pids"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	var pids []int
	for _, field := range strings.Fields(string(raw)) {
		pid, convErr := strconv.Atoi(field)
		require.NoError(t, convErr)
		pids = append(pids, pid)
	}
	return pids
}
