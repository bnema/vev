package sessionwire

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

func wireTestPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: 3,
		EnvironmentPolicy:    protocol.EnvironmentPolicyDaemonOwned,
		Transport:            "unix-mux",
		Trust:                "same-user",
		Launch:               "explicit",
		Isolation:            "per-user",
	}
}

func wireTestAdmission() ports.SessionAdmission {
	return ports.SessionAdmission{
		Origin:    ports.SessionOriginLocal,
		Policy:    wireTestPolicy(),
		Purpose:   ports.BrokerStreamAttachment,
		Admission: ports.BrokerAdmissionExact,
		Target:    protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{4, 5, 6}, SessionName: "work"},
		Env:       []string{"TERM=xterm-256color"},
	}
}

// TestServerConnectionAdmissionProviderPreservedWithoutWireChange proves the
// admission-aware constructor stores the provisioned metadata and exposes it
// through the optional provider, while a successful typed round trip over the
// same connection is byte-for-byte the ordinary session traffic: the admission
// never leaks onto the session wire.
func TestServerConnectionAdmissionProviderPreservedWithoutWireChange(t *testing.T) {
	raw := &scriptedTransport{recv: mustServerPreambleQueue(t, mustEncodeClient(t, protocol.Ping{}))}
	deadline := time.Now().Add(protocol.HandshakeTimeout)
	connection := NewServerConnectionWithAdmission(raw, deadline, wireTestAdmission())

	provider, ok := connection.(ports.SessionAdmissionProvider)
	require.True(t, ok, "the admission-aware connection implements the optional provider")
	admission, ok := provider.SessionAdmission()
	require.True(t, ok)
	require.Equal(t, wireTestAdmission(), admission)
	require.NoError(t, admission.Validate())

	// A typed exchange on the same connection succeeds and the sent application
	// frame is the ordinary Pong encoding: the admission is not part of the
	// session wire. (The preamble response precedes it.)
	message, err := connection.ReceiveClient()
	require.NoError(t, err)
	require.Equal(t, protocol.Ping{}, message)
	require.NoError(t, connection.SendServer(protocol.Pong{}))
	require.Equal(t, 2, raw.sentLen())
	got, err := DecodeServerEnvelope(raw.sentPayload(1))
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, got)
}

// TestServerConnectionAdmissionProviderIsDefensiveCopy proves the provider hands
// out an independent copy on every read: a consumer that mutates what it holds
// never alters the connection's stored admission or a sibling's view.
func TestServerConnectionAdmissionProviderIsDefensiveCopy(t *testing.T) {
	raw := &scriptedTransport{}
	connection := NewServerConnectionWithAdmission(raw, time.Now().Add(protocol.HandshakeTimeout), wireTestAdmission())
	provider := connection.(ports.SessionAdmissionProvider)

	mutated, ok := provider.SessionAdmission()
	require.True(t, ok)
	mutated.Env[0] = "TERM=mutated"
	mutated.Policy.Trust = "mutated"
	mutated.Target.SessionName = "mutated"

	reread, ok := provider.SessionAdmission()
	require.True(t, ok)
	require.Equal(t, wireTestAdmission(), reread)

	left, _ := provider.SessionAdmission()
	right, _ := provider.SessionAdmission()
	left.Env[0] = "TERM=left"
	require.Equal(t, "TERM=xterm-256color", right.Env[0])
}

// TestServerConnectionAdmissionAbsentForLegacyConstructors proves the legacy
// constructors carry no admission metadata: a consumer that asserts the
// provider is told ok=false rather than trusted with a zero admission.
func TestServerConnectionAdmissionAbsentForLegacyConstructors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func() ports.ServerConnection
	}{
		{name: "NewServerConnection", build: func() ports.ServerConnection { return NewServerConnection(&scriptedTransport{}) }},
		{name: "NewServerConnectionWithDeadline", build: func() ports.ServerConnection {
			return NewServerConnectionWithDeadline(&scriptedTransport{}, time.Now().Add(protocol.HandshakeTimeout))
		}},
		{name: "NewServerConnectionWithAdmission zero", build: func() ports.ServerConnection {
			return NewServerConnectionWithAdmission(&scriptedTransport{}, time.Now().Add(protocol.HandshakeTimeout), ports.SessionAdmission{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider, ok := tc.build().(ports.SessionAdmissionProvider)
			require.True(t, ok)
			admission, present := provider.SessionAdmission()
			require.False(t, present)
			require.Equal(t, ports.SessionAdmission{}, admission)
		})
	}
}
