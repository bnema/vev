package ports

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func acquisitionTestPolicy() BrokerPolicy {
	return BrokerPolicy{ProtocolVersion: 59, CatalogSchemaVersion: 3, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, Transport: "quic", Trust: "openssh", Launch: "explicit", Isolation: "user"}
}

func TestBrokerDialTargetFirstContactAndBoundValidation(t *testing.T) {
	registration := domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1}
	base := BrokerDialTarget{Fence: BrokerEndpointFence{Registration: registration}, Address: "ssh:user@example.test", Policy: acquisitionTestPolicy(), StartMode: BrokerDaemonExistingOnly}
	require.NoError(t, base.Validate())

	bound := base
	bound.ExpectedIdentity = BrokerExpectedIdentity{Bound: true, Identity: "daemon-1"}
	require.NoError(t, bound.Validate())

	malformed := base
	malformed.ExpectedIdentity.Identity = "daemon-without-bound-bit"
	require.Error(t, malformed.Validate())

	// The start mode is explicit and closed: zero is invalid, and both legal
	// values validate.
	missingMode := base
	missingMode.StartMode = 0
	require.ErrorContains(t, missingMode.Validate(), "invalid broker daemon start mode")
	startable := base
	startable.StartMode = BrokerDaemonStartIfNeeded
	require.NoError(t, startable.Validate())

	for _, tc := range []struct {
		name  string
		mode  BrokerDaemonStartMode
		valid bool
	}{
		{"existing only", BrokerDaemonExistingOnly, true},
		{"start if needed", BrokerDaemonStartIfNeeded, true},
		{"zero", 0, false},
		{"unknown", BrokerDaemonStartMode(3), false},
	} {
		t.Run("mode/"+tc.name, func(t *testing.T) {
			require.Equal(t, tc.valid, tc.mode.Validate() == nil)
		})
	}
}

func TestBrokerEndpointFenceSeparatesLocalAndRemoteAuthority(t *testing.T) {
	require.NoError(t, (BrokerEndpointFence{Local: true}).Validate())
	require.Error(t, (BrokerEndpointFence{}).Validate())
	require.Error(t, (BrokerEndpointFence{Local: true, Registration: domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1}}).Validate())
}
