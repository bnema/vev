package broker

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func observationPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion: protocol.Version, CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
		Transport: "ssh-quic", Trust: "ssh", Launch: "explicit", Isolation: "user",
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}
}

func observationCatalogue(t *testing.T) string {
	t.Helper()
	encoded, err := json.Marshal(catalogue.RemoteCatalog{ProtocolVersion: protocol.Version, SchemaVersion: catalogue.RemoteCatalogSchemaVersion, Sessions: []catalogue.RemoteCatalogSession{}})
	require.NoError(t, err)
	return string(encoded) + "\n"
}

// observationHosts answers LookupHost for exactly one configured record.
func observationHosts(t *testing.T, record ports.BrokerHostRecord) ports.BrokerHostAuthorityReader {
	hosts := portsmocks.NewMockBrokerHostAuthorityReader(t)
	hosts.EXPECT().LookupHost(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, endpoint string) (ports.BrokerHostRecord, bool, error) {
		return record, endpoint == record.Registration.Endpoint, ctx.Err()
	}).Maybe()
	return hosts
}

func TestObservationRequestIsObservationOnly(t *testing.T) {
	registration := domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1}
	policy := observationPolicy()
	tests := []struct {
		name  string
		build func(t *testing.T) (ports.BrokerOpenStreamRequest, error)
		check func(t *testing.T, request ports.BrokerOpenStreamRequest)
	}{
		{
			name: "remote",
			build: func(t *testing.T) (ports.BrokerOpenStreamRequest, error) {
				probe := &RemoteProbe{Epoch: 7, Hosts: observationHosts(t, ports.BrokerHostRecord{Registration: registration, Policy: policy})}
				return probe.request(context.Background(), registration)
			},
			check: func(t *testing.T, request ports.BrokerOpenStreamRequest) {
				require.False(t, request.Local)
				require.Equal(t, registration, request.Registration)
				require.Equal(t, registration.Endpoint, request.Endpoint)
			},
		},
		{
			name: "local",
			build: func(t *testing.T) (ports.BrokerOpenStreamRequest, error) {
				probe, err := NewLocalDaemonProbe(7, "local-address", policy, portsmocks.NewMockBrokerEndpointConnector(t), portsmocks.NewMockSessionCodec(t), func() (ports.BrokerDaemonIdentity, error) { return "daemon", nil })
				require.NoError(t, err)
				return probe.request()
			},
			check: func(t *testing.T, request ports.BrokerOpenStreamRequest) {
				require.True(t, request.Local)
				require.Empty(t, request.Endpoint)
				require.Equal(t, domain.RemoteRegistration{}, request.Registration)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := tt.build(t)
			require.NoError(t, err)
			require.NoError(t, request.Validate())
			require.Equal(t, ports.BrokerEpoch(7), request.Epoch)
			require.Equal(t, ports.BrokerStreamObservation, request.Purpose)
			require.Zero(t, request.Admission)
			require.Empty(t, request.Name)
			require.Empty(t, request.Env)
			require.Equal(t, protocol.ExactSessionTarget{}, request.Target)
			require.Equal(t, policy, request.Policy)
			require.NotZero(t, request.Connection)
			require.NotZero(t, request.Stream)
			tt.check(t, request)
			// Observation must never start the target: a spawn-capable request
			// is refused rather than silently narrowed.
			require.Equal(t, ports.BrokerDaemonExistingOnly, request.StartMode)
			startable := request
			startable.StartMode = ports.BrokerDaemonStartIfNeeded
			require.Error(t, startable.Validate())
		})
	}
}

func TestRemoteProbeRejectsStaleRegistrationBeforeDial(t *testing.T) {
	registration := domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1}
	tests := []struct {
		name    string
		probe   *RemoteProbe
		wantErr string
	}{
		{name: "incomplete composition", probe: &RemoteProbe{Epoch: 7}, wantErr: "incomplete broker remote probe"},
		{name: "unknown registration", probe: &RemoteProbe{
			Epoch: 7, Routes: portsmocks.NewMockBrokerRouteAuthority(t), Binder: portsmocks.NewMockBrokerIdentityBinder(t),
			Connector: portsmocks.NewMockBrokerEndpointConnector(t), Hosts: observationHosts(t, ports.BrokerHostRecord{}),
		}, wantErr: "unknown or stale probe registration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.probe.Probe(context.Background(), registration)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestNewLocalDaemonProbeRejectsIncompleteComposition(t *testing.T) {
	policy := observationPolicy()
	loader := func() (ports.BrokerDaemonIdentity, error) { return "daemon", nil }
	connector := portsmocks.NewMockBrokerEndpointConnector(t)
	codec := portsmocks.NewMockSessionCodec(t)
	tests := []struct {
		name      string
		epoch     ports.BrokerEpoch
		address   string
		connector ports.BrokerEndpointConnector
		codec     ports.SessionCodec
		loader    LocalIdentityLoader
		wantErr   string
	}{
		{name: "epoch", address: "a", connector: connector, codec: codec, loader: loader, wantErr: "epoch"},
		{name: "connector", epoch: 1, address: "a", codec: codec, loader: loader, wantErr: "connector"},
		{name: "codec", epoch: 1, address: "a", connector: connector, loader: loader, wantErr: "codec"},
		{name: "loader", epoch: 1, address: "a", connector: connector, codec: codec, wantErr: "identity loader"},
		{name: "address", epoch: 1, connector: connector, codec: codec, loader: loader, wantErr: "address"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewLocalDaemonProbe(tt.epoch, tt.address, policy, tt.connector, tt.codec, tt.loader)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestLocalDaemonProbeWithoutIdentityDoesNotDial(t *testing.T) {
	// No Connect expectation: any dial fails the mock.
	probe, err := NewLocalDaemonProbe(7, "local-address", observationPolicy(), portsmocks.NewMockBrokerEndpointConnector(t), portsmocks.NewMockSessionCodec(t), func() (ports.BrokerDaemonIdentity, error) { return "", nil })
	require.NoError(t, err)
	observation, err := probe.ProbeLocal(context.Background())
	require.NoError(t, err)
	require.Equal(t, domain.RemoteAvailabilityUnknown, observation.Availability)
	require.Zero(t, observation.Identity)
}

func TestLocalDaemonProbeNilReceiverFailsClosed(t *testing.T) {
	var probe *LocalDaemonProbe
	observation, err := probe.ProbeLocal(context.Background())
	require.Error(t, err)
	require.Equal(t, domain.RemoteAvailabilityUnreachable, observation.Availability)
}
