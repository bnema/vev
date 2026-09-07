package sshstdio

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func lookupCommand(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	require.NoError(t, err)
	path, err = filepath.Abs(path)
	require.NoError(t, err)
	return path
}

func TestBuildCommandForRemoteLaunchQuotesBinaryAndEnvironment(t *testing.T) {
	spec := BuildCommandForRemoteLaunch("test@example.com", "/opt/vev with-space", []string{"HOME=/tmp/home", "VALUE=a;b'c"}, "_stdio")
	require.Equal(t, "ssh", spec.Path)
	require.Equal(t, []string{
		"--",
		"test@example.com",
		"'env' '-i' 'HOME=/tmp/home' 'VALUE=a;b'\\''c' '/opt/vev with-space' '_stdio'",
	}, spec.Args)
}

func TestBuildCommandForIsolatedRemoteLaunchQuotesRootAndOwner(t *testing.T) {
	spec := BuildCommandForRemoteLaunchWithRoot("test@example.com", "/tmp/root with-space", "owner;token", "/opt/vev with-space", []string{"HOME=/tmp/home", "VALUE=a;b'c"}, "_stdio")
	require.Equal(t, "ssh", spec.Path)
	require.Equal(t, "test@example.com", spec.Args[1])
	require.Contains(t, spec.Args[2], "'sh' '-c'")
	require.Contains(t, spec.Args[2], "'/tmp/root with-space'")
	require.Contains(t, spec.Args[2], "'owner;token'")
	require.NotContains(t, spec.Args[2], "owner;token' &&")
	require.Contains(t, spec.Args[2], "VALUE=a;b")
}

func TestBuildCommandForRemoteCleanupVerifiesOwnerBeforeRemoval(t *testing.T) {
	spec := BuildCommandForRemoteCleanup("test@example.com", "/tmp/root", "owner", "/opt/vev", []string{"HOME=/tmp/home"}, "_ui-cleanup")
	require.Contains(t, spec.Args[2], "/tmp/root/.vev-ui-driver-owner")
	require.Contains(t, spec.Args[2], "'_ui-cleanup'")
	require.Contains(t, spec.Args[2], "rm -rf \"$root\"")
}

func TestIsolatedLaunchScriptRemovesRootWhenCleanupCommandFails(t *testing.T) {
	truePath := lookupCommand(t, "true")
	falsePath := lookupCommand(t, "false")
	tests := []struct {
		name       string
		mode       string
		cleanup    bool
		wantExists bool
	}{
		{name: "launch", mode: "_stdio", wantExists: true},
		{name: "cleanup", mode: "_ui-cleanup", cleanup: true, wantExists: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "root")
			if test.cleanup {
				require.NoError(t, exec.Command("sh", "-c", isolatedLaunchScript(root, "owner-token", truePath, nil, "_stdio", false)).Run())
			}
			err := exec.Command("sh", "-c", isolatedLaunchScript(root, "owner-token", falsePath, nil, test.mode, test.cleanup)).Run()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr)
			require.Equal(t, 1, exitErr.ExitCode())
			_, statErr := os.Stat(root)
			if test.wantExists {
				require.NoError(t, statErr)
			} else {
				require.ErrorIs(t, statErr, os.ErrNotExist)
			}
		})
	}
}

func TestIsolatedLaunchScriptOwnsAndCleansFreshRoot(t *testing.T) {
	truePath := lookupCommand(t, "true")
	root := filepath.Join(t.TempDir(), "root")
	launch := isolatedLaunchScript(root, "owner-token", truePath, nil, "_stdio", false)
	require.NoError(t, exec.Command("sh", "-c", launch).Run())
	marker := filepath.Join(root, ".vev-ui-driver-owner")
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, []byte("owner-token"), data)

	// The same owner can reconnect to the endpoint, but another token cannot
	// reuse the root created by this invocation.
	require.NoError(t, exec.Command("sh", "-c", isolatedLaunchScript(root, "owner-token", truePath, nil, "_stdio", false)).Run())
	require.Error(t, exec.Command("sh", "-c", isolatedLaunchScript(root, "other-owner", truePath, nil, "_stdio", false)).Run())
	require.NoError(t, exec.Command("sh", "-c", isolatedLaunchScript(root, "owner-token", truePath, nil, "_ui-cleanup", true)).Run())
	_, err = os.Stat(root)
	require.ErrorIs(t, err, os.ErrNotExist)
}
