package app

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerstore"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

func TestUpgradeBrokerHostVersions(t *testing.T) {
	tests := []struct {
		name         string
		policy       func() ports.BrokerPolicy
		wantRevision uint64 // also the expected generation advance
	}{
		{
			name: "older build policy is upgraded and identity kept",
			policy: func() ports.BrokerPolicy {
				p := remoteBrokerPolicy("")
				p.ProtocolVersion--
				return p
			},
			wantRevision: 1,
		},
		{
			name:         "current policy is left untouched",
			policy:       func() ports.BrokerPolicy { return remoteBrokerPolicy("") },
			wantRevision: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registration, err := domain.NewRemoteRegistration("user@host", [16]byte{1})
			require.NoError(t, err)
			policy := tt.policy()
			route, err := ports.BrokerRouteForTransport(policy.Transport, registration.Endpoint)
			require.NoError(t, err)
			record := ports.BrokerHostRecord{Registration: registration, Pinned: true, Policy: policy, Route: route, Identity: "daemon-identity"}

			store, err := brokerstore.Open(brokerstore.Options{Dir: filepath.Join(shortTempDir(t, "vevu"), "state"), InitialHosts: []ports.BrokerHostRecord{record}, InitialImportProvided: true})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			before, err := store.LoadHosts()
			require.NoError(t, err)

			require.NoError(t, upgradeBrokerHostVersions(store))

			after, err := store.LoadHosts()
			require.NoError(t, err)
			require.Equal(t, before.Revision+tt.wantRevision, after.Revision)
			require.Len(t, after.Hosts, 1)
			got := after.Hosts[0]
			require.Equal(t, protocol.Version, got.Policy.ProtocolVersion)
			require.Equal(t, catalogue.RemoteCatalogSchemaVersion, got.Policy.CatalogSchemaVersion)
			require.Equal(t, ports.BrokerDaemonIdentity("daemon-identity"), got.Identity)
			require.Equal(t, registration.Endpoint, got.Registration.Endpoint)
			require.Equal(t, registration.Incarnation, got.Registration.Incarnation)
			require.Equal(t, registration.Generation+domain.RemoteGeneration(tt.wantRevision), got.Registration.Generation)
		})
	}
}
