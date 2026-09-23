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
)

const (
	spawnTestChildFileEnv    = "VEV_SPAWN_TEST_CHILD_FILE"
	spawnTestLauncherFileEnv = "VEV_SPAWN_TEST_LAUNCHER_FILE"
	spawnTestReleaseFileEnv  = "VEV_SPAWN_TEST_RELEASE_FILE"
	spawnTestTraceFileEnv    = "VEV_SPAWN_TEST_TRACE_FILE"
)

func TestMain(m *testing.M) {
	// The hidden offline broker commands are re-executed through this test
	// binary, so a broker test can exercise the real dispatch path in a real
	// subprocess. The helper env var keeps that interception out of ordinary
	// runs.
	if len(os.Args) >= 2 && os.Getenv(brokerHelperEnv) == "1" {
		switch os.Args[1] {
		case brokerMuxStdioCommand, brokerMuxQUICBootstrapCommand, brokerMuxQUICProxyCommand,
			brokerReadyCommand,
			productionBrokerServeCommand, productionBrokerLauncherCommand, "--daemon":
			recordBrokerHelperProcess(os.Args[1])
			if os.Args[1] == productionBrokerLauncherCommand {
				if record := os.Getenv(brokerLauncherBlockRecordEnv); record != "" {
					blockBrokerLauncherHelper(record, os.Getenv(brokerLauncherBlockReleaseEnv))
				}
			}
			if err := Run(os.Args[1:]); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(ExitCode(err))
			}
			os.Exit(0)
		}
	}
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

// blockBrokerLauncherHelper is a controlled, blocking launcher: it records its
// PID and waits for the release marker (or a bounded fallback) so a test can
// hold a launcher open and prove the spawn wait is bounded by its context.
func blockBrokerLauncherHelper(record, release string) {
	if err := writeProcessRecord(record); err != nil {
		os.Exit(2)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, statErr := os.Stat(release)
		if statErr == nil {
			break
		}
		if !errors.Is(statErr, os.ErrNotExist) {
			os.Exit(2)
		}
		time.Sleep(10 * time.Millisecond)
	}
	os.Exit(0)
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
