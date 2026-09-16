package daemonmux

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// TestServerBindingValidatesAndExposesAuthority proves the immutable binding
// rejects every invalid field once, at construction, and exposes exactly the
// validated authority through its accessors.
func TestServerBindingValidatesAndExposesAuthority(t *testing.T) {
	binding, err := NewServerBinding(testIdentity(), testIncarnation(), testPolicy())
	require.NoError(t, err)
	require.NoError(t, binding.Validate())
	require.Equal(t, testIdentity(), binding.Identity())
	require.Equal(t, testIncarnation(), binding.Incarnation())
	require.Equal(t, testPolicy(), binding.Policy())
}

// TestServerBindingRefusesInvalidAuthority proves an empty identity, a zero
// incarnation, and every invalid policy shape are refused with the binding's
// single sentinel.
func TestServerBindingRefusesInvalidAuthority(t *testing.T) {
	invalidPolicy := testPolicy()
	invalidPolicy.ProtocolVersion = 0

	tests := []struct {
		name        string
		identity    ports.BrokerDaemonIdentity
		incarnation ports.BrokerDaemonIncarnation
		policy      ports.BrokerPolicy
	}{
		{name: "empty identity", identity: "", incarnation: testIncarnation(), policy: testPolicy()},
		{name: "zero incarnation", identity: testIdentity(), incarnation: ports.BrokerDaemonIncarnation{}, policy: testPolicy()},
		{name: "zero policy", identity: testIdentity(), incarnation: testIncarnation(), policy: ports.BrokerPolicy{}},
		{name: "policy without protocol version", identity: testIdentity(), incarnation: testIncarnation(), policy: invalidPolicy},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewServerBinding(test.identity, test.incarnation, test.policy)
			require.ErrorIs(t, err, ErrInvalidBinding)
		})
	}
}

// TestServerBindingAcceptsOnlyExactPolicy proves the binding accepts the exact
// policy and refuses a request differing in a single field.
func TestServerBindingAcceptsOnlyExactPolicy(t *testing.T) {
	binding, err := NewServerBinding(testIdentity(), testIncarnation(), testPolicy())
	require.NoError(t, err)
	require.True(t, binding.Accepts(testPolicy()))

	tests := []struct {
		name   string
		mutate func(*ports.BrokerPolicy)
	}{
		{name: "protocol version", mutate: func(p *ports.BrokerPolicy) { p.ProtocolVersion++ }},
		{name: "catalogue schema version", mutate: func(p *ports.BrokerPolicy) { p.CatalogSchemaVersion++ }},
		{name: "environment policy", mutate: func(p *ports.BrokerPolicy) {
			p.EnvironmentPolicy = protocol.EnvironmentPolicyClientOwned
		}},
		{name: "transport", mutate: func(p *ports.BrokerPolicy) { p.Transport = "stdio" }},
		{name: "trust", mutate: func(p *ports.BrokerPolicy) { p.Trust = "tofu" }},
		{name: "launch", mutate: func(p *ports.BrokerPolicy) { p.Launch = "manual" }},
		{name: "isolation", mutate: func(p *ports.BrokerPolicy) { p.Isolation = "system" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requested := testPolicy()
			test.mutate(&requested)
			require.False(t, binding.Accepts(requested))
		})
	}
}

// TestServerBindingIsImmutable proves the binding copied its inputs at
// construction: mutating the caller's local policy afterwards never changes
// what the binding enforces.
func TestServerBindingIsImmutable(t *testing.T) {
	local := testPolicy()
	binding, err := NewServerBinding(testIdentity(), testIncarnation(), local)
	require.NoError(t, err)

	local.Transport = "mutated"
	local.Trust = "mutated"
	require.Equal(t, testPolicy(), binding.Policy())
	require.True(t, binding.Accepts(testPolicy()))
}
