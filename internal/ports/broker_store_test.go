package ports

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// TestValidateDurableHostProjectionIdentity pins the durable rule to the live
// observation rule: an identity and incarnation must appear together, and an
// observed identity must carry the protocol version it was authenticated with.
// A restored durable record that violates either rule would otherwise reach a
// publication the wire refuses.
func TestValidateDurableHostProjectionIdentity(t *testing.T) {
	observed := func(o BrokerDaemonObservation) BrokerDaemonObservation {
		o.Identity = BrokerDaemonIdentity("authed-daemon")
		o.Incarnation = BrokerDaemonIncarnation{9}
		o.ProtocolVersion = protocol.Version
		return o
	}
	tests := []struct {
		name            string
		mutate          func(*BrokerDaemonObservation)
		wantErrContains string
	}{
		{
			name:   "no identity is accepted",
			mutate: func(*BrokerDaemonObservation) {},
		},
		{
			name:   "identity and incarnation with a protocol version are accepted",
			mutate: func(o *BrokerDaemonObservation) { *o = observed(*o) },
		},
		{
			name: "identity with zero protocol version is refused",
			mutate: func(o *BrokerDaemonObservation) {
				*o = observed(*o)
				o.ProtocolVersion = 0
			},
			wantErrContains: "carries no protocol version",
		},
		{
			name: "identity without incarnation is refused",
			mutate: func(o *BrokerDaemonObservation) {
				o.Identity = BrokerDaemonIdentity("authed-daemon")
			},
			wantErrContains: "partial identity",
		},
		{
			name: "incarnation without identity is refused",
			mutate: func(o *BrokerDaemonObservation) {
				o.Incarnation = BrokerDaemonIncarnation{9}
			},
			wantErrContains: "partial identity",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obs := testBrokerObservation("user@arch")
			tc.mutate(&obs)
			err := ValidateDurableHostProjection(obs)
			if tc.wantErrContains != "" {
				require.ErrorContains(t, err, tc.wantErrContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestBrokerRouteSpecValidateMatrix pins the durable route contract: an empty
// route is never valid membership, the closed kind set is enforced, unix routes
// carry exactly an absolute clean path, and ssh routes carry a valid target with
// a bounded argv whose entries are non-empty, control-free, and size-bounded.
func TestBrokerRouteSpecValidateMatrix(t *testing.T) {
	stdinArgv := []string{"vev", "_broker-mux-stdio", "--production"}
	quicArgv := []string{"vev", "_broker-mux-quic-bootstrap", "--production"}
	overBoundArgv := make([]string, BrokerMaxRouteArgv+1)
	for i := range overBoundArgv {
		overBoundArgv[i] = "arg"
	}
	tests := []struct {
		name    string
		route   BrokerRouteSpec
		wantErr bool
	}{
		{name: "empty route is never valid", route: BrokerRouteSpec{}, wantErr: true},
		{name: "unix route with an absolute clean path", route: BrokerRouteSpec{Kind: BrokerRouteUnix, Path: "/run/vev/broker.sock"}},
		{name: "unix route with an empty path", route: BrokerRouteSpec{Kind: BrokerRouteUnix}, wantErr: true},
		{name: "unix route with a relative path", route: BrokerRouteSpec{Kind: BrokerRouteUnix, Path: "broker.sock"}, wantErr: true},
		{name: "unix route with an unclean path", route: BrokerRouteSpec{Kind: BrokerRouteUnix, Path: "/run/../vev.sock"}, wantErr: true},
		{name: "unix route with a target", route: BrokerRouteSpec{Kind: BrokerRouteUnix, Path: "/run/vev.sock", Target: "user@host"}, wantErr: true},
		{name: "unix route with argv", route: BrokerRouteSpec{Kind: BrokerRouteUnix, Path: "/run/vev.sock", Argv: []string{"vev"}}, wantErr: true},
		{name: "ssh-stdio route", route: BrokerRouteSpec{Kind: BrokerRouteSSHStdio, Target: "user@host:22", Argv: stdinArgv}},
		{name: "ssh-quic route", route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: "user@host:22", Argv: quicArgv}},
		{name: "ssh route with a path", route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Path: "/run/vev.sock", Target: "user@host", Argv: quicArgv}, wantErr: true},
		{name: "ssh route with an empty target", route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Argv: quicArgv}, wantErr: true},
		{name: "ssh route with no argv", route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: "user@host"}, wantErr: true},
		{name: "ssh route with an over-bound argv", route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: "user@host", Argv: overBoundArgv}, wantErr: true},
		{name: "ssh route with an empty argv entry", route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: "user@host", Argv: []string{"vev", ""}}, wantErr: true},
		{name: "ssh route with a NUL argv entry", route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: "user@host", Argv: []string{"vev\x00"}}, wantErr: true},
		{name: "ssh route with a newline argv entry", route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: "user@host", Argv: []string{"vev\n"}}, wantErr: true},
		{name: "ssh route with an oversize argv entry", route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: "user@host", Argv: []string{strings.Repeat("a", BrokerMaxRouteArgBytes+1)}}, wantErr: true},
		{name: "unknown kind", route: BrokerRouteSpec{Kind: BrokerRouteKind("tcp"), Target: "user@host", Argv: quicArgv}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.route.Validate()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestBrokerRouteForTransportClosedVocabulary pins the mapping from the closed
// legacy policy transport vocabulary to canonical remote routes.
func TestBrokerRouteForTransportClosedVocabulary(t *testing.T) {
	for _, tc := range []struct {
		transport string
		kind      BrokerRouteKind
		bootstrap string
	}{
		{"quic", BrokerRouteSSHQUIC, "_broker-mux-quic-bootstrap"},
		{"ssh-quic", BrokerRouteSSHQUIC, "_broker-mux-quic-bootstrap"},
		{"stdio", BrokerRouteSSHStdio, "_broker-mux-stdio"},
		{"ssh-stdio", BrokerRouteSSHStdio, "_broker-mux-stdio"},
	} {
		t.Run(tc.transport, func(t *testing.T) {
			route, err := BrokerRouteForTransport(tc.transport, "user@host:22")
			require.NoError(t, err)
			require.NoError(t, route.Validate())
			require.Equal(t, tc.kind, route.Kind)
			require.Equal(t, "user@host:22", route.Target)
			require.Equal(t, []string{"vev", tc.bootstrap, "--production"}, route.Argv)
		})
	}
	for _, transport := range []string{"", "unix", "tcp", "ssh"} {
		t.Run("refused/"+transport, func(t *testing.T) {
			_, err := BrokerRouteForTransport(transport, "user@host:22")
			require.Error(t, err)
		})
	}
}

// TestBrokerRouteSpecCloneIsDeep proves cloning a route never shares the argv
// backing array, so a deep copy of durable membership cannot alias authority.
func TestBrokerRouteSpecCloneIsDeep(t *testing.T) {
	original := BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: "user@host:22", Argv: []string{"vev", "_broker-mux-quic-bootstrap", "--production"}}
	clone := original.Clone()
	require.Equal(t, original, clone)
	require.NotSame(t, &original.Argv[0], &clone.Argv[0])
	clone.Argv[0] = "mutated"
	require.Equal(t, "vev", original.Argv[0], "mutating the clone must not touch authority")

	// A route with no argv clones to a nil argv without allocating one.
	unix := BrokerRouteSpec{Kind: BrokerRouteUnix, Path: "/run/vev.sock"}
	require.Nil(t, unix.Clone().Argv)
}

// TestValidateBrokerHostRecordsRequiresARoute proves an empty route is never
// valid membership: a record that otherwise looks well-formed but names no
// canonical route is refused, so an un-routed record can never be published or
// restored as durable authority.
func TestValidateBrokerHostRecordsRequiresARoute(t *testing.T) {
	policy := BrokerPolicy{ProtocolVersion: 1, CatalogSchemaVersion: 1, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, Transport: "quic", Trust: "trusted", Launch: "explicit", Isolation: "user"}
	registration := domain.RemoteRegistration{Endpoint: "user@host:22", Incarnation: [16]byte{1}, Generation: 1}

	routed := BrokerHostRecord{Registration: registration, Pinned: true, Policy: policy, Route: BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: registration.Endpoint, Argv: []string{"vev", "_broker-mux-quic-bootstrap", "--production"}}}
	require.NoError(t, ValidateBrokerHostRecords([]BrokerHostRecord{routed}))

	unrouted := routed
	unrouted.Route = BrokerRouteSpec{}
	require.ErrorContains(t, ValidateBrokerHostRecords([]BrokerHostRecord{unrouted}), "unknown broker route kind")
}

// TestUpgradeBrokerHostRoutesIsClosedAndIdempotent proves the pre-route upgrade
// fills exactly the records without a route, leaves already-routed authority
// untouched, refuses an unknown legacy transport, and never aliases the input.
func TestUpgradeBrokerHostRoutesIsClosedAndIdempotent(t *testing.T) {
	policy := BrokerPolicy{ProtocolVersion: 1, CatalogSchemaVersion: 1, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, Transport: "stdio", Trust: "trusted", Launch: "explicit", Isolation: "user"}
	registration := domain.RemoteRegistration{Endpoint: "user@host:22", Generation: 1}
	input := []BrokerHostRecord{{Registration: registration, Pinned: true, Policy: policy}}

	upgraded, err := UpgradeBrokerHostRoutes(input)
	require.NoError(t, err)
	require.Len(t, upgraded, 1)
	require.Equal(t, BrokerRouteSpec{Kind: BrokerRouteSSHStdio, Target: registration.Endpoint, Argv: []string{"vev", "_broker-mux-stdio", "--production"}}, upgraded[0].Route)
	require.Empty(t, input[0].Route, "the upgrade never aliases or mutates the caller's records")

	// Already-routed records are preserved verbatim and the upgrade is idempotent.
	routed := []BrokerHostRecord{{Registration: registration, Pinned: true, Policy: policy, Route: upgraded[0].Route}}
	again, err := UpgradeBrokerHostRoutes(routed)
	require.NoError(t, err)
	require.Equal(t, routed, again)

	// An unknown legacy transport is refused, so an unapproved route is never
	// invented over durable authority.
	unknown := append([]BrokerHostRecord(nil), input...)
	unknown[0].Policy.Transport = "tcp"
	_, err = UpgradeBrokerHostRoutes(unknown)
	require.ErrorContains(t, err, "unsupported broker transport")
}
