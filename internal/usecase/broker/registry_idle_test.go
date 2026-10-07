package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// TestRegistryIdleRefreshPublishesOnceAndSkipsUnchangedPersistence pins the
// idle cost of a watched broker: a routine refresh of a reachable host
// publishes once (its result, no transient Checking publication), and a result
// whose durable content is unchanged does not rewrite the store.
func TestRegistryIdleRefreshPublishesOnceAndSkipsUnchangedPersistence(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	store := newTestStore()
	registry := newTestRegistry(t, 1, store, probe, clock)
	registry.demandFreshForRemote = time.Second
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	answer := func(sessions []catalogue.RemoteCatalogSession) ports.BrokerDaemonObservation {
		t.Helper()
		var call probeCall
		require.Eventually(t, func() bool {
			select {
			case call = <-probe.calls:
				return true
			default:
				clock.Advance(registry.demandFreshForRemote)
				return false
			}
		}, registryTestWait, time.Millisecond)
		call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
			Availability:   domain.RemoteAvailabilityReachable,
			InventoryKnown: true,
			Sessions:       sessions,
		}}
		now := clock.Now()
		return waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
			return !host.Checking && host.LastSuccess.Equal(now)
		})
	}

	first := []catalogue.RemoteCatalogSession{hostSession("work")}
	answer(first)
	stored := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		host, ok := snapshot.Find(reg.Endpoint)
		return ok && len(host.Sessions) == 1
	})
	writes := store.writeCount()
	revision := registry.Snapshot().Revision

	for range 2 {
		answer(first)
		require.Equal(t, revision+1, registry.Snapshot().Revision, "one publication per routine refresh")
		revision++
	}

	// A durable change is still written, and it is the only write since the
	// first one: the idle refreshes never reached the store.
	changed := []catalogue.RemoteCatalogSession{hostSession("work"), hostSession("play")}
	answer(changed)
	stored = waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		host, ok := snapshot.Find(reg.Endpoint)
		return ok && len(host.Sessions) == 2
	})
	require.Equal(t, writes+1, store.writeCount())
	require.Equal(t, registry.Snapshot().Revision, stored.Revision)
}

// TestRegistryExplicitProbeStillPublishesChecking keeps the visible refresh
// state for requested observations and for hosts not confirmed reachable.
func TestRegistryExplicitProbeStillPublishesChecking(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	// Unknown availability: the first scheduled observation is visible.
	call := receiveCall(t, probe)
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool { return host.Checking })
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})

	// Reachable host, explicit request: still visible.
	registry.RequestProbe(reg.Endpoint)
	call = receiveCall(t, probe)
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool { return host.Checking })
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool { return !host.Checking })
}
