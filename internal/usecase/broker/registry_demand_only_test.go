package broker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestRegistryObservesOnlyUnderSubscriptionOrExplicitRequest(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(4)
	local := newScriptedLocalProbe(4)
	registry := newLocalTestRegistry(t, newTestStore(), probe, local, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	// Start Run directly: the test helper's synthetic subscription must not
	// hide an idle broker probe.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	clock.Advance(time.Hour)
	requireNoProbeWithin(t, probe, probeGrace)
	requireNoLocalProbeWithin(t, local, probeGrace)

	registry.SetDemand(true)
	remoteCall := receiveCall(t, probe)
	localCall := receiveLocalCall(t, local)
	remoteCall.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	localCall.result <- localProbeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnknown}}
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	waitLocal(t, registry, func(host ports.BrokerDaemonObservation) bool { return !host.Checking })
	registry.SetDemand(false)
	clock.Advance(time.Hour)
	requireNoProbeWithin(t, probe, probeGrace)
	requireNoLocalProbeWithin(t, local, probeGrace)

	registry.RequestProbe(reg.Endpoint)
	receiveCall(t, probe)
	requireNoLocalProbeWithin(t, local, probeGrace)
}
