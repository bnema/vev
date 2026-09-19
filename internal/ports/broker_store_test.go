package ports

import (
	"testing"

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
