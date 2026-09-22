package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/platform"
	"github.com/stretchr/testify/require"
)

// Exercise both real detached re-execs, which change cwd to the user's home.
// A long development root must preserve its configuration/state identity while
// all roles independently resolve the same portable, isolated runtime path.
func TestProductionBrokerDevelopmentEnvironmentReexec(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "implicit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("VEV_ENV", "test")
			t.Setenv("VEV_ENV_ROOT", "")
			if explicit {
				t.Setenv("VEV_ENV_ROOT", filepath.Join(t.TempDir(), strings.Repeat("r", 100)))
			}
			for _, key := range []string{"XDG_CONFIG_HOME", "XDG_STATE_HOME", "XDG_RUNTIME_DIR"} {
				t.Setenv(key, "")
			}
			require.NoError(t, platform.ActivateDevelopmentEnvironment())
			root := os.Getenv("VEV_ENV_ROOT")
			require.True(t, filepath.IsAbs(root))
			records := shortTempDir(t, "vevenv")
			t.Setenv(brokerHelperEnv, "1")
			t.Setenv(brokerHelperRecordDirEnv, records)
			sandbox := &brokerSubprocessSandbox{t: t, recordDir: records}
			runtime := ipc.SocketDir()
			require.NotEqual(t, filepath.Join(os.Getenv("XDG_RUNTIME_DIR"), "vev"), runtime)
			t.Cleanup(func() {
				for _, role := range []string{productionBrokerServeCommand, productionBrokerLauncherCommand} {
					for _, pid := range sandbox.recorded(role) {
						_ = syscall.Kill(pid, syscall.SIGKILL)
						_ = waitForProcessExit(pid, 5*time.Second)
					}
				}
				_ = os.RemoveAll(runtime)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			service, err := connectProductionBroker(ctx)
			require.NoError(t, err)
			defer service.Close()
			require.NotZero(t, service.Snapshot().Epoch)
			require.Len(t, sandbox.recorded(productionBrokerLauncherCommand), 1)
			require.Len(t, sandbox.recorded(productionBrokerServeCommand), 1)
			require.FileExists(t, filepath.Join(root, "test", "config", "vev", "broker.json"))
			require.FileExists(t, filepath.Join(root, "test", "state", "vev", "broker", "state", "state.json"))
		})
	}
}

func TestProductionBrokerRejectsInsecureShortRuntimeParent(t *testing.T) {
	for _, kind := range []string{"symlink", "public-directory"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("XDG_CONFIG_HOME", t.TempDir())
			t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), strings.Repeat("x", 110)))
			runtime := ipc.SocketDir()
			t.Cleanup(func() { _ = os.RemoveAll(runtime) })
			if kind == "symlink" {
				require.NoError(t, os.Symlink(t.TempDir(), runtime))
			} else {
				require.NoError(t, os.Mkdir(runtime, 0o700))
				require.NoError(t, os.Chmod(runtime, 0o755))
			}
			service, err := connectProductionBroker(context.Background())
			require.Nil(t, service)
			require.ErrorContains(t, err, "secure broker runtime parent")
			_, err = os.Lstat(filepath.Join(runtime, "broker"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}
