package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/stretchr/testify/require"
)

func TestProductionBrokerHiddenCommandsRejectSandboxOptions(t *testing.T) {
	serve, err := parseArgs([]string{productionBrokerServeCommand})
	require.NoError(t, err)
	require.Equal(t, kindProductionBrokerServe, serve.kind)
	require.True(t, serve.brokerServe.production)

	launcher, err := parseArgs([]string{productionBrokerLauncherCommand})
	require.NoError(t, err)
	require.Equal(t, kindProductionBrokerLauncher, launcher.kind)

	for _, name := range []string{productionBrokerServeCommand, productionBrokerLauncherCommand} {
		_, err = parseArgs([]string{name, "--offline-root", t.TempDir()})
		require.Error(t, err)
	}
}

func TestEnsureProductionBrokerConfigCreatesPrivateEmptyBootstrapWithoutReplacingExisting(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)

	require.NoError(t, ensureProductionBrokerConfig())
	path := productionBrokerConfigPath()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, `{"marker":"vev.broker.offline/v1","registrations":[]}`, string(raw))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	dirInfo, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())

	layout := productionBrokerLayout()
	_, err = brokerconfig.LoadProduction(layout, path, "", localDaemonPolicy(), daemonmux.SocketPath(ipc.SocketDir()))
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("operator-owned"), 0o600))
	require.NoError(t, ensureProductionBrokerConfig())
	raw, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "operator-owned", string(raw))
}

func TestEnsureProductionBrokerConfigSupportsConcurrentFirstUse(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	errs := make(chan error, 16)
	for range cap(errs) {
		go func() { errs <- ensureProductionBrokerConfig() }()
	}
	for range cap(errs) {
		require.NoError(t, <-errs)
	}
	raw, err := os.ReadFile(productionBrokerConfigPath())
	require.NoError(t, err)
	require.JSONEq(t, `{"marker":"vev.broker.offline/v1","registrations":[]}`, string(raw))
}

func TestProductionBrokerLayoutUsesDedicatedRuntimeStateAndConfig(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	layout := productionBrokerLayout()
	require.Equal(t, filepath.Join(layout.Root, "state"), layout.State)
	require.Equal(t, filepath.Join(layout.Runtime, "spawn"), layout.Spawn)
	require.Equal(t, "broker", filepath.Base(layout.Root))
	require.Equal(t, "broker", filepath.Base(layout.Runtime))
	require.Equal(t, "broker.json", filepath.Base(productionBrokerConfigPath()))
	require.NotEqual(t, layout.Root, layout.Runtime)
}
