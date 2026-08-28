package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
)

var errDialFailed = errors.New("dial failed")

const (
	spawnTestChildFileEnv    = "VEV_SPAWN_TEST_CHILD_FILE"
	spawnTestLauncherFileEnv = "VEV_SPAWN_TEST_LAUNCHER_FILE"
	spawnTestReleaseFileEnv  = "VEV_SPAWN_TEST_RELEASE_FILE"
	spawnTestTraceFileEnv    = "VEV_SPAWN_TEST_TRACE_FILE"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case "--daemon-launcher":
			launcherFile := os.Getenv(spawnTestLauncherFileEnv)
			if launcherFile != "" {
				if err := writeProcessRecord(launcherFile); err != nil {
					os.Exit(2)
				}
			}
			if err := Run(os.Args[1:]); err != nil {
				os.Exit(2)
			}
			if releaseFile := os.Getenv(spawnTestReleaseFileEnv); releaseFile != "" {
				deadline := time.Now().Add(5 * time.Second)
				for {
					if _, err := os.Stat(releaseFile); err == nil {
						break
					} else if !errors.Is(err, os.ErrNotExist) || !time.Now().Before(deadline) {
						os.Exit(2)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			os.Exit(0)
		case "--daemon":
			path := os.Getenv(spawnTestChildFileEnv)
			if path == "" {
				os.Exit(2)
			}
			if tracePath := os.Getenv(spawnTestTraceFileEnv); tracePath != "" {
				if err := os.WriteFile(tracePath, []byte(os.Getenv("VEV_PERF_TRACE")), 0o600); err != nil {
					os.Exit(2)
				}
			}
			if err := writeProcessRecord(path); err != nil {
				os.Exit(2)
			}
			for {
				time.Sleep(time.Hour)
			}
		}
	}
	os.Exit(m.Run())
}

func TestRealSpawnWaitsForLauncherExitAndDoesNotPropagateTrace(t *testing.T) {
	dir := t.TempDir()
	childFile := filepath.Join(dir, "daemon.pid")
	launcherFile := filepath.Join(dir, "launcher.pid")
	releaseFile := filepath.Join(dir, "release-launcher")
	traceFile := filepath.Join(dir, "daemon.trace-env")
	t.Setenv(spawnTestChildFileEnv, childFile)
	t.Setenv(spawnTestLauncherFileEnv, launcherFile)
	t.Setenv(spawnTestReleaseFileEnv, releaseFile)
	t.Setenv(spawnTestTraceFileEnv, traceFile)
	t.Setenv("VEV_PERF_TRACE", "parent-trace.jsonl")

	// Register cleanup before starting either subprocess. The release marker
	// unblocks a launcher even when setup or an assertion fails, and PID files
	// let cleanup terminate every helper that reached its acknowledgement.
	t.Cleanup(func() {
		_ = os.WriteFile(releaseFile, nil, 0o600)
		for _, path := range []string{launcherFile, childFile} {
			if err := terminateProcessFromFile(path); err != nil {
				t.Errorf("clean up %s: %v", filepath.Base(path), err)
			}
		}
	})

	spawnResult := make(chan error, 1)
	go func() {
		spawnResult <- realSpawn()
	}()

	launcherPID := waitForProcessRecord(t, launcherFile)
	daemonPID := waitForProcessRecord(t, childFile)

	select {
	case err := <-spawnResult:
		t.Fatalf("realSpawn returned while launcher %d was still running: %v", launcherPID, err)
	default:
	}
	if err := syscall.Kill(launcherPID, 0); err != nil {
		t.Fatalf("launcher exited before release acknowledgement: %v", err)
	}

	if err := os.WriteFile(releaseFile, nil, 0o600); err != nil {
		t.Fatalf("release launcher: %v", err)
	}
	select {
	case err := <-spawnResult:
		if err != nil {
			t.Fatalf("realSpawn: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("realSpawn did not return after launcher release")
	}

	if err := waitForProcessExit(launcherPID, 2*time.Second); err != nil {
		t.Fatalf("launcher still exists after realSpawn returned: %v", err)
	}
	if err := syscall.Kill(daemonPID, 0); err != nil {
		t.Fatalf("daemon exited with its launcher: %v", err)
	}
	traceEnv, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("read daemon trace environment: %v", err)
	}
	if got := string(traceEnv); got != "" {
		t.Fatalf("daemon inherited VEV_PERF_TRACE=%q", got)
	}
}

func waitForProcessRecord(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(string(data))
			if err != nil {
				t.Fatalf("parse process record %s: %v", filepath.Base(path), err)
			}
			return pid
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read process record %s: %v", filepath.Base(path), err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process did not write %s", filepath.Base(path))
	return 0
}

func waitForProcessExit(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("process %d did not exit within %s", pid, timeout)
}

func writeProcessRecord(path string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func terminateProcessFromFile(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return err
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return waitForProcessExit(pid, 2*time.Second)
}

// fastBackoff keeps the retry loop snappy for tests.
var fastBackoff = backoffConfig{initial: time.Millisecond, max: 5 * time.Millisecond, total: 100 * time.Millisecond}

func TestAcquireSpawnLockSingleWinnerUnderRace(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "vev")
	const racers = 16

	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	releases := make(chan func(), racers)

	for range racers {
		wg.Go(func() {
			<-start
			release, acquired, err := acquireSpawnLock(dir)
			if err != nil {
				t.Errorf("acquireSpawnLock: %v", err)
				return
			}
			if acquired {
				wins.Add(1)
				releases <- release
			}
		})
	}

	close(start)
	wg.Wait()
	close(releases)

	if got := wins.Load(); got != 1 {
		t.Fatalf("expected exactly one winner, got %d", got)
	}
	for release := range releases {
		release()
	}
	// After release, the lock directory is gone and can be re-acquired.
	release, acquired, err := acquireSpawnLock(dir)
	if err != nil || !acquired {
		t.Fatalf("re-acquire after release: acquired=%v err=%v", acquired, err)
	}
	release()
}

func TestAcquireSpawnLockTakesOverStaleLock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "vev")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("seeding socket dir: %v", err)
	}
	lockPath := filepath.Join(dir, spawnLockName)
	if err := os.Mkdir(lockPath, 0o700); err != nil {
		t.Fatalf("seeding lock: %v", err)
	}

	// A fresh lock must NOT be taken over.
	if _, acquired, err := acquireSpawnLock(dir); err != nil || acquired {
		t.Fatalf("fresh lock taken over: acquired=%v err=%v", acquired, err)
	}

	// Backdate the lock beyond the stale threshold; now it is taken over.
	old := time.Now().Add(-2 * staleLockAge)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatalf("backdating lock: %v", err)
	}
	release, acquired, err := acquireSpawnLock(dir)
	if err != nil || !acquired {
		t.Fatalf("stale lock not taken over: acquired=%v err=%v", acquired, err)
	}
	release()
}

func TestRetryAttemptsChecksOperationErrorBeforeDeadline(t *testing.T) {
	attemptErr := errors.New("attempt failed")
	calls := 0
	_, err := retryAttempts(context.Background(), backoffConfig{}, func() (struct{}, bool, error) {
		calls++
		return struct{}{}, false, attemptErr
	})
	if !errors.Is(err, attemptErr) {
		t.Fatalf("retryAttempts error = %v, want %v", err, attemptErr)
	}
	if calls != 1 {
		t.Fatalf("attempt called %d times, want 1", calls)
	}
}

func TestRetryAttemptsClampsNonPositiveInitialDelay(t *testing.T) {
	started := time.Now()
	calls := 0
	_, err := retryAttempts(context.Background(), backoffConfig{total: time.Second, max: time.Second}, func() (struct{}, bool, error) {
		calls++
		return struct{}{}, calls == 2, nil
	})
	if err != nil {
		t.Fatalf("retryAttempts: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 500*time.Microsecond {
		t.Fatalf("retryAttempts retried after %s, want a positive initial delay", elapsed)
	}
}

func TestRetryDialSucceedsAfterTransientFailures(t *testing.T) {
	want := wiremocks.NewMockTransport(t)
	var calls atomic.Int32
	dial := func(context.Context, string) (wire.Transport, error) {
		if calls.Add(1) <= 3 {
			return nil, errDialFailed
		}
		return want, nil
	}

	got, err := retryDial(context.Background(), t.TempDir(), dial, fastBackoff)
	if err != nil {
		t.Fatalf("retryDial: %v", err)
	}
	if got != want {
		t.Fatalf("retryDial returned unexpected transport")
	}
	if calls.Load() != 4 {
		t.Fatalf("dial called %d times, want 4", calls.Load())
	}
}

func TestRetryDialPermanentFailure(t *testing.T) {
	dial := func(context.Context, string) (wire.Transport, error) { return nil, errDialFailed }
	_, err := retryDial(context.Background(), t.TempDir(), dial, fastBackoff)
	if !errors.Is(err, ErrDaemonUnreachable) {
		t.Fatalf("retryDial error = %v, want ErrDaemonUnreachable", err)
	}
}

func TestRetryDialContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls atomic.Int32
	dial := func(context.Context, string) (wire.Transport, error) {
		calls.Add(1)
		return nil, errDialFailed
	}
	_, err := retryDial(ctx, t.TempDir(), dial, fastBackoff)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retryDial error = %v, want context.Canceled", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("dial called %d times for canceled context, want 0", calls.Load())
	}
}

func TestEnsureDaemonContextCancelledDoesNotDialOrSpawn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dial := func(context.Context, string) (wire.Transport, error) {
		t.Fatal("ensureDaemon dialed with a canceled context")
		return nil, nil
	}
	spawn := func() error {
		t.Fatal("ensureDaemon spawned with a canceled context")
		return nil
	}

	_, err := ensureDaemon(ctx, t.TempDir(), dial, spawn, fastBackoff)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureDaemon error = %v, want context.Canceled", err)
	}
}

func TestEnsureDaemonDoesNotSpawnWhenContextCancelsAfterFailedDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var spawnCalls atomic.Int32
	dial := func(context.Context, string) (wire.Transport, error) {
		cancel()
		return nil, errDialFailed
	}
	spawn := func() error {
		spawnCalls.Add(1)
		return nil
	}

	_, err := ensureDaemon(ctx, filepath.Join(t.TempDir(), "vev"), dial, spawn, fastBackoff)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureDaemon error = %v, want context.Canceled", err)
	}
	if got := spawnCalls.Load(); got != 0 {
		t.Fatalf("spawn called %d times after canceled failed dial, want 0", got)
	}
}

func TestEnsureDaemonSpawnsThenDials(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "vev")
	want := wiremocks.NewMockTransport(t)
	var dialCalls, spawnCalls atomic.Int32
	dial := func(context.Context, string) (wire.Transport, error) {
		// First (pre-spawn) dial fails; the post-spawn retry succeeds.
		if dialCalls.Add(1) == 1 {
			return nil, errDialFailed
		}
		return want, nil
	}
	spawn := func() error {
		spawnCalls.Add(1)
		return nil
	}

	got, err := ensureDaemon(context.Background(), dir, dial, spawn, fastBackoff)
	if err != nil {
		t.Fatalf("ensureDaemon: %v", err)
	}
	if got != want {
		t.Fatalf("ensureDaemon returned unexpected transport")
	}
	if spawnCalls.Load() != 1 {
		t.Fatalf("spawn called %d times, want 1", spawnCalls.Load())
	}
	// The spawn lock must be released once ensureDaemon returns.
	if _, err := os.Stat(filepath.Join(dir, spawnLockName)); !os.IsNotExist(err) {
		t.Fatalf("spawn lock not released: stat err=%v", err)
	}
}

func TestEnsureDaemonReturnsExistingDaemon(t *testing.T) {
	want := wiremocks.NewMockTransport(t)
	var spawnCalls atomic.Int32
	dial := func(context.Context, string) (wire.Transport, error) { return want, nil }
	spawn := func() error { spawnCalls.Add(1); return nil }

	got, err := ensureDaemon(context.Background(), t.TempDir(), dial, spawn, fastBackoff)
	if err != nil {
		t.Fatalf("ensureDaemon: %v", err)
	}
	if got != want {
		t.Fatalf("ensureDaemon returned unexpected transport")
	}
	if spawnCalls.Load() != 0 {
		t.Fatalf("spawn should not be called when daemon already up, got %d", spawnCalls.Load())
	}
}

func TestEnsureDaemonSpawnFailure(t *testing.T) {
	spawnErr := errors.New("exec boom")
	dial := func(context.Context, string) (wire.Transport, error) { return nil, errDialFailed }
	spawn := func() error { return spawnErr }

	_, err := ensureDaemon(context.Background(), filepath.Join(t.TempDir(), "vev"), dial, spawn, fastBackoff)
	if !errors.Is(err, spawnErr) {
		t.Fatalf("ensureDaemon error = %v, want wrapped %v", err, spawnErr)
	}
}
