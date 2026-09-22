package app

import (
	"testing"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/stretchr/testify/require"
)

func TestBrokerPoolLimitsDefaultWarmRetention(t *testing.T) {
	limits := brokerPoolLimits(nil)
	require.Equal(t, brokerconfig.DefaultWarmTransports, limits.Warm)
	require.Zero(t, limits.Idle, "the default matches the removed client cache: no age bound")
	require.LessOrEqual(t, brokerconfig.MaxWarmTransports, limits.Physical)
}
