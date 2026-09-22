package broker

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// TestRegistryLookupHostDeepCopyAndCancel pins the committed-authority read
// seam: a lookup returns a defensive deep copy of durable routing authority (so
// a caller cannot alias the registry's record through the route's argv), a
// cancelled context is refused before authority is consulted, and an unknown
// endpoint reports absence without an error.
func TestRegistryLookupHostDeepCopyAndCancel(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	registry := newTestRegistry(t, 1, newTestStore(), newTestProbe(1), clock)
	reg := registration(t, "user@host:22", 1)
	require.NoError(t, registry.ReplaceHosts(hostRecords(reg)))

	want := hostRecord(reg)
	got, found, err := registry.LookupHost(context.Background(), reg.Endpoint)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, want, got)
	require.Equal(t, canonicalRoute(poolPolicy(), reg.Endpoint), got.Route)

	// Mutating the returned copy touches nothing: the route's argv never aliases
	// registry authority, and neither do the value fields.
	got.Route.Argv[0] = "mutated"
	got.Policy.Trust = "mutated"
	got.Registration.Generation = 99
	got.Identity = "mutated"
	again, found, err := registry.LookupHost(context.Background(), reg.Endpoint)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, want, again, "the returned record is a deep copy of authority")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	absent, found, err := registry.LookupHost(cancelled, reg.Endpoint)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, found)
	require.Equal(t, ports.BrokerHostRecord{}, absent)

	missing, found, err := registry.LookupHost(context.Background(), "user@absent:22")
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, ports.BrokerHostRecord{}, missing)

	// The refused lookups never moved authority.
	require.Equal(t, []ports.BrokerHostRecord{want}, registry.authority.Hosts)
	require.Equal(t, domain.RemoteRegistration{}, missing.Registration)
}
