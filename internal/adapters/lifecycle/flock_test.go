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
