package app

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestBrokerRemoteProbeRequestIsObservationOnly(t *testing.T) {
	registration := domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1}
	policy := ports.BrokerPolicy{ProtocolVersion: protocol.Version, CatalogSchemaVersion: 3, Transport: "stdio", Trust: "ssh", Launch: "explicit", Isolation: "user", EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}
	probe := &brokerRemoteProbe{
		epoch: 7,
		hosts: &routeTestHosts{record: ports.BrokerHostRecord{Registration: registration, Policy: policy}, found: true},
	}

	request, err := probe.request(context.Background(), registration)
	require.NoError(t, err)
	require.NoError(t, request.Validate())
	require.Equal(t, ports.BrokerStreamObservation, request.Purpose)
	require.Zero(t, request.Admission)
	require.Empty(t, request.Env)
	require.Equal(t, registration, request.Registration)
	require.Equal(t, policy, request.Policy)
	// Observation must never start the target: the request carries the
	// existing-only authorization, and a spawn-capable one would be refused.
	require.Equal(t, ports.BrokerDaemonExistingOnly, request.StartMode)
	startable := request
	startable.StartMode = ports.BrokerDaemonStartIfNeeded
	require.Error(t, startable.Validate())
}

func TestBrokerRemoteProbeRejectsUnconfiguredEndpointBeforeDial(t *testing.T) {
	probe := &brokerRemoteProbe{epoch: 7, hosts: &routeTestHosts{}}
	_, err := probe.Probe(context.Background(), domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1})
	require.ErrorContains(t, err, "incomplete broker remote probe")
}
