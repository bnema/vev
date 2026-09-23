package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestRegistryNoDaemonUsesPassiveCadenceEvenWithDemand(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(2)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	registry.jitter = identityJitter
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	registry.SetDemand(true)
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityNoDaemon}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityNoDaemon
	})
	require.Equal(t, start.Add(defaultFreshFor), host.NextDue)
	clock.Advance(defaultDemandFreshForRemote)
	requireNoProbeWithin(t, probe, probeGrace)
	clock.Advance(defaultFreshFor - defaultDemandFreshForRemote)
	receiveCall(t, probe)
}
