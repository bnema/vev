package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

var errDialFailed = errors.New("dial failed")

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

func TestRetryDialSucceedsAfterTransientFailures(t *testing.T) {
	want := portsmocks.NewMockTransport(t)
	var calls atomic.Int32
	dial := func(string) (ports.Transport, error) {
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
	dial := func(string) (ports.Transport, error) { return nil, errDialFailed }
	_, err := retryDial(context.Background(), t.TempDir(), dial, fastBackoff)
	if !errors.Is(err, ErrDaemonUnreachable) {
		t.Fatalf("retryDial error = %v, want ErrDaemonUnreachable", err)
	}
}

func TestRetryDialContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dial := func(string) (ports.Transport, error) { return nil, errDialFailed }
	_, err := retryDial(ctx, t.TempDir(), dial, fastBackoff)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retryDial error = %v, want context.Canceled", err)
	}
}

func TestEnsureDaemonSpawnsThenDials(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "vev")
	want := portsmocks.NewMockTransport(t)
	var dialCalls, spawnCalls atomic.Int32
	dial := func(string) (ports.Transport, error) {
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
	want := portsmocks.NewMockTransport(t)
	var spawnCalls atomic.Int32
	dial := func(string) (ports.Transport, error) { return want, nil }
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
	dial := func(string) (ports.Transport, error) { return nil, errDialFailed }
	spawn := func() error { return spawnErr }

	_, err := ensureDaemon(context.Background(), filepath.Join(t.TempDir(), "vev"), dial, spawn, fastBackoff)
	if !errors.Is(err, spawnErr) {
		t.Fatalf("ensureDaemon error = %v, want wrapped %v", err, spawnErr)
	}
}
