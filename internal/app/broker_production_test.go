package app

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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
