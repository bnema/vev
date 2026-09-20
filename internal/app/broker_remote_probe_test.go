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
		policy: func(endpoint string) (ports.BrokerPolicy, bool) {
			return policy, endpoint == registration.Endpoint
		},
	}

	request, err := probe.request(registration)
	require.NoError(t, err)
	require.NoError(t, request.Validate())
	require.Equal(t, ports.BrokerStreamObservation, request.Purpose)
	require.Zero(t, request.Admission)
	require.Empty(t, request.Env)
	require.Equal(t, registration, request.Registration)
	require.Equal(t, policy, request.Policy)
}

func TestBrokerRemoteProbeRejectsUnconfiguredEndpointBeforeDial(t *testing.T) {
	probe := &brokerRemoteProbe{epoch: 7, policy: func(string) (ports.BrokerPolicy, bool) { return ports.BrokerPolicy{}, false }}
	_, err := probe.Probe(context.Background(), domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1})
	require.ErrorContains(t, err, "incomplete broker remote probe")
}
