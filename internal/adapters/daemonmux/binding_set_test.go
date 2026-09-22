package daemonmux

import (
	"sync"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestServerBindingsClosedSetCloneAndDuplicates(t *testing.T) {
	first := testPolicy()
	second := first
	second.Transport = "ssh-stdio"
	entries := []ServerPolicyAdmission{localAcceptance(first), remoteAcceptance(second)}
	binding, err := NewServerBindings(testIdentity(), testIncarnation(), entries)
	require.NoError(t, err)
	entries[0] = localAcceptance(second)
	require.True(t, binding.Accepts(first), "constructor must clone its authority")
	require.True(t, binding.Accepts(second))
	variation := second
	variation.Trust += "-other"
	require.False(t, binding.Accepts(variation))
	_, err = NewServerBindings(testIdentity(), testIncarnation(), []ServerPolicyAdmission{localAcceptance(first), remoteAcceptance(first)})
	require.ErrorIs(t, err, ErrInvalidBinding)
	_, err = NewServerBindings(testIdentity(), testIncarnation(), nil)
	require.ErrorIs(t, err, ErrInvalidBinding)
}

func TestServerBindingsConcurrentAdmission(t *testing.T) {
	policy := testPolicy()
	binding, err := NewServerBindings(testIdentity(), testIncarnation(), []ServerPolicyAdmission{localAcceptance(policy)})
	require.NoError(t, err)
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() { defer wg.Done(); require.True(t, binding.Accepts(policy)) }()
	}
	wg.Wait()
}

// TestServerBindingsSinglePolicyShorthandMatchesSetConstructor proves the
// fixtures-only shorthand is exactly the one-member closed set: same authority,
// same decisions, same exposure through Policy().
func TestServerBindingsSinglePolicyShorthandMatchesSetConstructor(t *testing.T) {
	shorthand, err := NewServerBinding(testIdentity(), testIncarnation(), testPolicy())
	require.NoError(t, err)
	set, err := NewServerBindings(testIdentity(), testIncarnation(), []ServerPolicyAdmission{localAcceptance(testPolicy())})
	require.NoError(t, err)

	require.Equal(t, set.Identity(), shorthand.Identity())
	require.Equal(t, set.Incarnation(), shorthand.Incarnation())
	require.Equal(t, set.Policy(), shorthand.Policy())
	require.NoError(t, shorthand.Validate())
	for _, candidate := range []ports.BrokerPolicy{testPolicy(), func() ports.BrokerPolicy { p := testPolicy(); p.Transport = "other"; return p }()} {
		require.Equal(t, set.Accepts(candidate), shorthand.Accepts(candidate))
	}
}

// TestServerBindingsAcceptedReturnsProvisionedMember proves admission answers
// with the member the configuration provisioned and never with the request: no
// merging, inheritance, echo, or fallback can widen the configured authority.
// The answered entry carries the provisioned locality alongside its policy.
func TestServerBindingsAcceptedReturnsProvisionedMember(t *testing.T) {
	first := testPolicy()
	second := first
	second.Transport = "ssh-stdio"
	second.Trust = "pinned"
	binding, err := NewServerBindings(testIdentity(), testIncarnation(), []ServerPolicyAdmission{localAcceptance(first), remoteAcceptance(second)})
	require.NoError(t, err)

	accepted, ok := binding.Accepted(second)
	require.True(t, ok)
	require.Equal(t, remoteAcceptance(second), accepted, "admission answers the provisioned member, not the request")
	require.Equal(t, ports.SessionOriginRemote, accepted.Origin)

	local, ok := binding.Accepted(first)
	require.True(t, ok)
	require.Equal(t, localAcceptance(first), local)
	require.Equal(t, ports.SessionOriginLocal, local.Origin)

	// A request differing in exactly one field is refused outright: it can
	// never be answered by a member it was not provisioned as.
	variations := []func(*ports.BrokerPolicy){
		func(p *ports.BrokerPolicy) { p.ProtocolVersion++ },
		func(p *ports.BrokerPolicy) { p.CatalogSchemaVersion++ },
		func(p *ports.BrokerPolicy) { p.Transport = "other" },
		func(p *ports.BrokerPolicy) { p.Trust = "other" },
		func(p *ports.BrokerPolicy) { p.Isolation = "other" },
	}
	for _, mutate := range variations {
		requested := second
		mutate(&requested)
		accepted, ok := binding.Accepted(requested)
		require.False(t, ok, "a one-field variation must not be admitted")
		require.Equal(t, ServerPolicyAdmission{}, accepted)
	}
}

// TestServerBindingsPolicyExposesNoMultiMemberAuthority proves Policy() answers
// only a genuine single-member authority: a multi-member binding exposes the
// zero value rather than one of its members, so a caller can never mistake an
// arbitrary member for the authority of the whole daemon.
func TestServerBindingsPolicyExposesNoMultiMemberAuthority(t *testing.T) {
	first := testPolicy()
	second := first
	second.Transport = "ssh-stdio"
	binding, err := NewServerBindings(testIdentity(), testIncarnation(), []ServerPolicyAdmission{localAcceptance(first), remoteAcceptance(second)})
	require.NoError(t, err)
	require.Equal(t, ports.BrokerPolicy{}, binding.Policy())
	require.True(t, binding.Accepts(first))
	require.True(t, binding.Accepts(second))
}

// TestServerBindingsRefusesInvalidMemberAmongValid proves validation is
// all-or-nothing: one invalid member refuses the whole closed set instead of
// publishing a partially valid authority. An entry with an unknown or invalid
// locality is refused the same way, so an accepted member always names a
// provisioned origin.
func TestServerBindingsRefusesInvalidMemberAmongValid(t *testing.T) {
	invalid := testPolicy()
	invalid.ProtocolVersion = 0

	tests := []struct {
		name    string
		entries []ServerPolicyAdmission
	}{
		{name: "invalid member first", entries: []ServerPolicyAdmission{localAcceptance(invalid), remoteAcceptance(testPolicy())}},
		{name: "invalid member last", entries: []ServerPolicyAdmission{localAcceptance(testPolicy()), remoteAcceptance(invalid)}},
		{name: "zero policy member", entries: []ServerPolicyAdmission{localAcceptance(testPolicy()), remoteAcceptance(ports.BrokerPolicy{})}},
		{name: "unknown origin", entries: []ServerPolicyAdmission{{Policy: testPolicy(), Origin: ports.SessionOriginUnknown}}},
		{name: "invalid origin", entries: []ServerPolicyAdmission{{Policy: testPolicy(), Origin: ports.SessionConnectionOrigin(9)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewServerBindings(testIdentity(), testIncarnation(), tt.entries)
			require.ErrorIs(t, err, ErrInvalidBinding)
		})
	}
}

// TestServerBindingsRefusesInvalidAuthority proves an empty identity and a zero
// incarnation are refused regardless of how many valid policies follow.
func TestServerBindingsRefusesInvalidAuthority(t *testing.T) {
	tests := []struct {
		name        string
		identity    ports.BrokerDaemonIdentity
		incarnation ports.BrokerDaemonIncarnation
	}{
		{name: "empty identity", identity: "", incarnation: testIncarnation()},
		{name: "zero incarnation", identity: testIdentity(), incarnation: ports.BrokerDaemonIncarnation{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewServerBindings(tt.identity, tt.incarnation, []ServerPolicyAdmission{localAcceptance(testPolicy())})
			require.ErrorIs(t, err, ErrInvalidBinding)
		})
	}
}

// TestServerBindingsOriginShorthandMatchesSetConstructor proves Origin()
// answers only a genuine single-member authority, while a multi-member binding
// exposes the unknown origin rather than an arbitrary member's locality.
func TestServerBindingsOriginShorthandMatchesSetConstructor(t *testing.T) {
	shorthand, err := NewServerBinding(testIdentity(), testIncarnation(), testPolicy())
	require.NoError(t, err)
	require.Equal(t, ports.SessionOriginLocal, shorthand.Origin())

	first := testPolicy()
	second := first
	second.Transport = "ssh-stdio"
	multi, err := NewServerBindings(testIdentity(), testIncarnation(), []ServerPolicyAdmission{localAcceptance(first), remoteAcceptance(second)})
	require.NoError(t, err)
	require.Equal(t, ports.SessionOriginUnknown, multi.Origin())
}

// TestServerBindingsConcurrentMultiMemberAdmission proves a shared closed set is
// safe to admit against from many handshakes at once, resolving each concurrent
// request to its own provisioned member.
func TestServerBindingsConcurrentMultiMemberAdmission(t *testing.T) {
	first := testPolicy()
	second := first
	second.Transport = "ssh-stdio"
	third := first
	third.Isolation = "system"
	binding, err := NewServerBindings(testIdentity(), testIncarnation(), []ServerPolicyAdmission{localAcceptance(first), remoteAcceptance(second), remoteAcceptance(third)})
	require.NoError(t, err)

	candidates := []ServerPolicyAdmission{localAcceptance(first), remoteAcceptance(second), remoteAcceptance(third)}
	var wg sync.WaitGroup
	for i := range 150 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			want := candidates[index%len(candidates)]
			accepted, ok := binding.Accepted(want.Policy)
			require.True(t, ok)
			require.Equal(t, want, accepted)
		}(i)
	}
	wg.Wait()
}
