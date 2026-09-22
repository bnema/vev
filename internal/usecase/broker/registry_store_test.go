package broker

import (
	"errors"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/require"
)

func TestRegistryRestoresAuthoritativeMembership(t *testing.T) {
	for _, mode := range []string{"removed", "replaced", "never observed", "matching"} {
		t.Run(mode, func(t *testing.T) {
			old := registration(t, "host", 1)
			current := old
			store := portsmocks.NewMockBrokerHostStore(t)
			snapshot := ports.BrokerSnapshot{Epoch: 1, Revision: 9, Daemons: []ports.BrokerDaemonObservation{{Endpoint: "host", Registration: old, Availability: domain.RemoteAvailabilityReachable, InventoryKnown: true}}}
			hosts := ports.BrokerHosts{Revision: 4}
			if mode == "replaced" {
				current.Generation++
			}
			if mode != "removed" {
				hosts.Hosts = []ports.BrokerHostRecord{{Registration: current, Pinned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), current.Endpoint)}}
			}
			if mode == "never observed" {
				snapshot = ports.BrokerSnapshot{}
			}
			store.EXPECT().Load().Return(snapshot, nil).Once()
			store.EXPECT().LoadHosts().Return(hosts, nil).Once()
			r, err := NewRegistry(2, store, newTestProbe(1), newManualClock(time.Unix(100, 0)), nil)
			require.NoError(t, err)
			got := r.Snapshot()
			require.Equal(t, ports.BrokerEpoch(2), got.Epoch)
			require.Equal(t, ports.BrokerRevision(1), got.Revision)
			if mode == "removed" {
				require.Empty(t, got.Daemons)
				return
			}
			require.Len(t, got.Daemons, 1)
			require.Equal(t, current, got.Daemons[0].Registration)
			require.Equal(t, mode == "matching", got.Daemons[0].InventoryKnown)
		})
	}
}

// TestRegistryRejectsAuthorityAndLoadFailures covers the negative store seam:
// NewRegistry requires BrokerHostStore explicitly, so a load error, an
// authority error, or authority that fails validation is fatal instead of
// being skipped as an optional capability.
func TestRegistryRejectsAuthorityAndLoadFailures(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	reg := registration(t, "host", 1)
	loadErr := errors.New("snapshot load failed")
	authorityErr := errors.New("authority load failed")
	invalidAuthority := map[string]ports.BrokerHosts{
		"missing revision": {Hosts: []ports.BrokerHostRecord{{Registration: reg, Pinned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), reg.Endpoint)}}},
		"duplicate endpoint": {Revision: 1, Hosts: []ports.BrokerHostRecord{
			{Registration: reg, Pinned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), reg.Endpoint)},
			{Registration: reg, Learned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), reg.Endpoint)},
		}},
		"unanchored record": {Revision: 1, Hosts: []ports.BrokerHostRecord{{Registration: reg, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), reg.Endpoint)}}},
		"zero policy":       {Revision: 1, Hosts: []ports.BrokerHostRecord{{Registration: reg, Pinned: true, Route: canonicalRoute(poolPolicy(), reg.Endpoint)}}},
	}
	for name, hosts := range invalidAuthority {
		t.Run(name, func(t *testing.T) {
			store := portsmocks.NewMockBrokerHostStore(t)
			store.EXPECT().Load().Return(ports.BrokerSnapshot{}, nil).Once()
			store.EXPECT().LoadHosts().Return(hosts, nil).Once()
			registry, err := NewRegistry(1, store, newTestProbe(1), clock, nil)
			require.Error(t, err)
			require.Nil(t, registry)
		})
	}
	t.Run("snapshot load error", func(t *testing.T) {
		store := portsmocks.NewMockBrokerHostStore(t)
		store.EXPECT().Load().Return(ports.BrokerSnapshot{}, loadErr).Once()
		registry, err := NewRegistry(1, store, newTestProbe(1), clock, nil)
		require.ErrorIs(t, err, loadErr)
		require.Nil(t, registry)
	})
	t.Run("authority load error", func(t *testing.T) {
		store := portsmocks.NewMockBrokerHostStore(t)
		store.EXPECT().Load().Return(ports.BrokerSnapshot{}, nil).Once()
		store.EXPECT().LoadHosts().Return(ports.BrokerHosts{}, authorityErr).Once()
		registry, err := NewRegistry(1, store, newTestProbe(1), clock, nil)
		require.ErrorIs(t, err, authorityErr)
		require.Nil(t, registry)
	})
}

// TestRegistryRestoreIsReadOnlyAndPublicationLocal pins the revision series: the
// durable authority revision is store-local and never seeds the publication
// series, and construction writes nothing back.
func TestRegistryRestoreIsReadOnlyAndPublicationLocal(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	reg := registration(t, "host", 1)
	store := portsmocks.NewMockBrokerHostStore(t)
	store.EXPECT().Load().Return(ports.BrokerSnapshot{Epoch: 5, Revision: 4096, Daemons: []ports.BrokerDaemonObservation{{
		Endpoint: reg.Endpoint, Registration: reg, Availability: domain.RemoteAvailabilityReachable, InventoryKnown: true,
	}}}, nil).Once()
	store.EXPECT().LoadHosts().Return(ports.BrokerHosts{Revision: 99, Hosts: []ports.BrokerHostRecord{{Registration: reg, Pinned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), reg.Endpoint)}}}, nil).Once()

	r, err := NewRegistry(6, store, newTestProbe(1), clock, nil)
	require.NoError(t, err)
	snapshot := r.Snapshot()
	require.Equal(t, ports.BrokerRevision(1), snapshot.Revision)
	require.Equal(t, ports.BrokerEpoch(6), snapshot.Epoch)
	// Restoring reuses the matching observation but never republishes it: a
	// fresh process adopts authority and rewrites only when it observes.
	require.Len(t, snapshot.Daemons, 1)
	require.True(t, snapshot.Daemons[0].InventoryKnown)
	require.Equal(t, domain.RemoteAvailabilityReachable, snapshot.Daemons[0].Availability)
}
