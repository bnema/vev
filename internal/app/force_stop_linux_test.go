//go:build linux

package app

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/stretchr/testify/require"
)

// TestLinuxForceStopFinderVerifiesDaemonHoldingEmptyLegacyLock proves ownership
// through a real descriptor: the helper runs as `--daemon`, holds a legacy
// (empty) lifecycle.lock, and is identified without trusting the process name.
func TestLinuxForceStopFinderVerifiesDaemonHoldingEmptyLegacyLock(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	childFile := filepath.Join(t.TempDir(), "daemon.pid")
	t.Setenv(spawnTestChildFileEnv, childFile)
	t.Setenv(spawnTestLockDirEnv, runtimeDir)

	cmd := exec.Command(os.Args[0], "--daemon")
	cmd.Env = os.Environ()
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	daemonPID := waitForProcessRecord(t, childFile)

	finder := platformDaemonProcessFinder{}
	var process daemonProcess
	var ok bool
	require.Eventually(t, func() bool {
		var err error
		process, ok, err = finder.Find(context.Background(), runtimeDir)
		require.NoError(t, err)
		return ok
	}, 5*time.Second, 10*time.Millisecond, "finder did not identify the lock holder")

	require.Equal(t, daemonPID, process.PID)
	require.Contains(t, process.Command, "--daemon")
	require.NotEmpty(t, process.Start)

	lockInfo, err := os.Stat(lifecycle.Path(runtimeDir))
	require.NoError(t, err)
	require.EqualValues(t, 0, lockInfo.Size(), "the legacy lifecycle lock stays empty")
}

// TestLinuxForceStopFinderIgnoresNonDaemonLockHolder proves a same-user process
// that merely holds lifecycle.lock is not a candidate: this test binary owns the
// lock but was not started as a vev daemon.
func TestLinuxForceStopFinderIgnoresNonDaemonLockHolder(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	owner, err := lifecycle.TryAcquire(runtimeDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Release()) }()

	_, ok, err := platformDaemonProcessFinder{}.Find(context.Background(), runtimeDir)

	require.NoError(t, err)
	require.False(t, ok, "ownership must not be inferred from a process name alone")
}

func TestLinuxForceStopFinderIgnoresUnheldLockFile(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	require.NoError(t, os.WriteFile(lifecycle.Path(runtimeDir), nil, 0o600))

	_, ok, err := platformDaemonProcessFinder{}.Find(context.Background(), runtimeDir)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestLinuxForceStopFinderIgnoresMissingLockFile(t *testing.T) {
	_, ok, err := platformDaemonProcessFinder{}.Find(context.Background(), filepath.Join(t.TempDir(), "runtime"))
	require.NoError(t, err)
	require.False(t, ok)
}

// TestLinuxProcessStartMarkerIsStable pins the PID-reuse marker: the kernel
// start time of our own process is stable across reads and distinguishes
// incarnations.
func TestLinuxProcessStartMarkerIsStable(t *testing.T) {
	first, err := procStartMarker(os.Getpid())
	require.NoError(t, err)
	require.NotEmpty(t, first)

	second, err := procStartMarker(os.Getpid())
	require.NoError(t, err)
	require.Equal(t, first, second)

	// Sanity: a different live process has a different marker or at least a
	// parseable one.
	other, err := procStartMarker(1)
	require.NoError(t, err)
	require.NotEmpty(t, other)
}
