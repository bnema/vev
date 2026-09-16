//go:build unix

package lifecycle

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLifecycleLockProcessHelper(t *testing.T) {
	if os.Getenv("VEV_LIFECYCLE_LOCK_HELPER") != "1" {
		t.Skip("helper process")
	}

	owner, err := TryAcquire(os.Getenv("VEV_LIFECYCLE_LOCK_DIR"))
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Release()) }()

	_, err = fmt.Fprintln(os.Stdout, "locked")
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, os.Stdin)
	require.NoError(t, err)
}

func TestLifecycleLock(t *testing.T) {
	t.Run("process contention and reacquire", testLifecycleLockProcessContentionAndReacquire)
	t.Run("acquire cancellation", testLifecycleLockAcquireCancellation)
	t.Run("unavailable runtime directory", testLifecycleLockUnavailableRuntimeDirectory)
	t.Run("lock file is owner-only regular", testLifecycleLockIsOwnerOnlyRegular)
	t.Run("owner-only variant is accepted", testLifecycleLockOwnerOnlyVariantAccepted)
	t.Run("restrictive umask normalizes to owner-only", testLifecycleLockRestrictiveUmaskNormalizes)
	t.Run("symlinked lock path fails closed", testLifecycleLockSymlinkFailsClosed)
	t.Run("nonregular lock path fails closed", testLifecycleLockNonRegularFailsClosed)
	t.Run("loose permissions fail closed", testLifecycleLockLoosePermissionsFailClosed)
}

func testLifecycleLockIsOwnerOnlyRegular(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	owner, err := TryAcquire(runtimeDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Release()) }()

	info, err := os.Lstat(Path(runtimeDir))
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular(), "the lock must be a regular file")
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "the lock must be owner-only")
}

// testLifecycleLockOwnerOnlyVariantAccepted proves a pre-existing owner-only
// lock with a different owner bit pattern is trusted, so an installation whose
// lock was created under a variant umask can still start the daemon.
func testLifecycleLockOwnerOnlyVariantAccepted(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	require.NoError(t, os.WriteFile(Path(runtimeDir), nil, 0o600))
	require.NoError(t, os.Chmod(Path(runtimeDir), 0o700))

	owner, err := TryAcquire(runtimeDir)
	require.NoError(t, err)
	require.NoError(t, owner.Release())
}

// testLifecycleLockRestrictiveUmaskNormalizes proves a newly created lock is
// chmodded to 0600 even when the process umask would have stripped owner bits,
// so the lock is always exactly owner-only after creation.
func testLifecycleLockRestrictiveUmaskNormalizes(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	previous := syscall.Umask(0o400)
	defer syscall.Umask(previous)

	owner, err := TryAcquire(runtimeDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Release()) }()

	info, err := os.Lstat(Path(runtimeDir))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "a newly created lock must be normalized to owner-only")
}

func testLifecycleLockSymlinkFailsClosed(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	target := filepath.Join(t.TempDir(), "target.lock")
	require.NoError(t, os.WriteFile(target, nil, 0o600))
	require.NoError(t, os.Symlink(target, Path(runtimeDir)))

	_, err := TryAcquire(runtimeDir)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrBusy), "a symlinked lock path is an error, not contention")
	require.Contains(t, err.Error(), "symlink")
}

func testLifecycleLockNonRegularFailsClosed(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	require.NoError(t, syscall.Mkfifo(Path(runtimeDir), 0o600))

	_, err := TryAcquire(runtimeDir)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrBusy), "a non-regular lock path is an error, not contention")
	require.Contains(t, err.Error(), "not a regular file")
}

func testLifecycleLockLoosePermissionsFailClosed(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	require.NoError(t, os.WriteFile(Path(runtimeDir), nil, 0o600))
	require.NoError(t, os.Chmod(Path(runtimeDir), 0o644))

	_, err := TryAcquire(runtimeDir)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrBusy), "a loose lock file is an error, not contention")
	require.Contains(t, err.Error(), "permissions")
}

func testLifecycleLockProcessContentionAndReacquire(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLifecycleLockProcessHelper$")
	cmd.Env = append(os.Environ(),
		"VEV_LIFECYCLE_LOCK_HELPER=1",
		"VEV_LIFECYCLE_LOCK_DIR="+runtimeDir,
	)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	}()

	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "locked\n", line)

	_, err = TryAcquire(runtimeDir)
	require.ErrorIs(t, err, ErrBusy)

	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait())

	owner, err := TryAcquire(runtimeDir)
	require.NoError(t, err)
	require.NoError(t, owner.Release())
	require.NoError(t, owner.Release(), "release is idempotent")
}

func testLifecycleLockAcquireCancellation(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	owner, err := TryAcquire(runtimeDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Release()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = Acquire(ctx, runtimeDir, time.Millisecond)
	require.ErrorIs(t, err, context.Canceled)
}

func testLifecycleLockUnavailableRuntimeDirectory(t *testing.T) {
	runtimePath := t.TempDir() + "/not-a-directory"
	require.NoError(t, os.WriteFile(runtimePath, []byte("occupied"), 0o600))

	_, err := TryAcquire(runtimePath)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrBusy))
}
