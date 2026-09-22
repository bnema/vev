package broker

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestRegistryBindsAuthenticatedIdentityDurablyAndIdempotently(t *testing.T) {
	store := newMembershipStore()
	registry, err := NewRegistryWithConfig(1, store, nil, newManualClock(time.Unix(1, 0)), nil, RegistryConfig{MembershipMode: MembershipMutable, ObservationDisabled: true})
	require.NoError(t, err)
	policy := poolPolicy()
	registration, err := registry.AddHost(context.Background(), "user@example.test", policy)
	require.NoError(t, err)
	request := ports.BrokerIdentityBindingRequest{Fence: ports.BrokerEndpointFence{Registration: registration}, Policy: policy, Identity: "daemon-1"}

	identity, err := registry.BindAuthenticatedIdentity(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, ports.BrokerDaemonIdentity("daemon-1"), identity)
	require.Equal(t, ports.BrokerDaemonIdentity("daemon-1"), store.hosts.Hosts[0].Identity)
	revision := store.hosts.Revision

	identity, err = registry.BindAuthenticatedIdentity(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, ports.BrokerDaemonIdentity("daemon-1"), identity)
	require.Equal(t, revision, store.hosts.Revision, "idempotent binding must not rewrite authority")

	request.Identity = "daemon-2"
	_, err = registry.BindAuthenticatedIdentity(context.Background(), request)
	require.Error(t, err)
	require.Equal(t, ports.BrokerDaemonIdentity("daemon-1"), store.hosts.Hosts[0].Identity)
}

func TestRegistryRejectsAuthenticatedIdentityAcrossIncompatiblePolicies(t *testing.T) {
	store := newMembershipStore()
	registry, err := NewRegistryWithConfig(1, store, nil, newManualClock(time.Unix(1, 0)), nil, RegistryConfig{MembershipMode: MembershipMutable, ObservationDisabled: true})
	require.NoError(t, err)
	policy := poolPolicy()
	first, err := registry.AddHost(context.Background(), "first.example.test", policy)
	require.NoError(t, err)
	_, err = registry.BindAuthenticatedIdentity(context.Background(), ports.BrokerIdentityBindingRequest{Fence: ports.BrokerEndpointFence{Registration: first}, Policy: policy, Identity: "shared-daemon"})
	require.NoError(t, err)

	incompatible := policy
	incompatible.Isolation = "other-isolation"
	second, err := registry.AddHost(context.Background(), "second.example.test", incompatible)
	require.NoError(t, err)
	_, err = registry.BindAuthenticatedIdentity(context.Background(), ports.BrokerIdentityBindingRequest{Fence: ports.BrokerEndpointFence{Registration: second}, Policy: incompatible, Identity: "shared-daemon"})
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorConflictingPolicy, typed.Code)
}

func TestRegistryIdentityBindingIsFencedAcrossRemoveReadd(t *testing.T) {
	store := newMembershipStore()
	registry, err := NewRegistryWithConfig(1, store, nil, newManualClock(time.Unix(1, 0)), nil, RegistryConfig{MembershipMode: MembershipMutable, ObservationDisabled: true})
	require.NoError(t, err)
	policy := poolPolicy()
	old, err := registry.AddHost(context.Background(), "user@example.test", policy)
	require.NoError(t, err)
	removed, err := registry.RemoveHost(context.Background(), old)
	require.NoError(t, err)
	require.True(t, removed)
	fresh, err := registry.AddHost(context.Background(), old.Endpoint, policy)
	require.NoError(t, err)
	require.False(t, fresh.Equal(old))

	_, err = registry.BindAuthenticatedIdentity(context.Background(), ports.BrokerIdentityBindingRequest{Fence: ports.BrokerEndpointFence{Registration: old}, Policy: policy, Identity: "stale"})
	require.Error(t, err)
	identity, err := registry.BindAuthenticatedIdentity(context.Background(), ports.BrokerIdentityBindingRequest{Fence: ports.BrokerEndpointFence{Registration: fresh}, Policy: policy, Identity: "fresh"})
	require.NoError(t, err)
	require.Equal(t, ports.BrokerDaemonIdentity("fresh"), identity)
}
