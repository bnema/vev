package daemonmux

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// TestOpenMessageCarriesAttachmentAdmission proves the request-to-open
// conversion preserves the attachment admission variant and its creation name,
// all the way to the wire and back.
//
// The session daemon admits an attachment by the variant the stream declares, so
// a conversion that drops it delivers every attachment as "no admission" and the
// daemon refuses it: one-shot ephemeral creation, named creation, and exact
// attach all fail before a session is ever accepted, which is exactly how it
// presented from a real composition.
func TestOpenMessageCarriesAttachmentAdmission(t *testing.T) {
	base := ports.BrokerOpenStreamRequest{
		Epoch:      1,
		Purpose:    ports.BrokerStreamAttachment,
		Local:      true,
		Connection: ports.BrokerConnectionID{1},
		Stream:     ports.BrokerStreamID(1),
		Policy:     logicalTestPolicy(),
		StartMode:  ports.BrokerDaemonStartIfNeeded,
	}
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "alpha"}

	tests := []struct {
		name      string
		request   ports.BrokerOpenStreamRequest
		admission ports.BrokerStreamAdmission
		createAs  string
	}{
		{
			name: "exact attach keeps its exact target",
			request: func() ports.BrokerOpenStreamRequest {
				r := base
				r.Admission = ports.BrokerAdmissionExact
				r.Target = target
				return r
			}(),
			admission: ports.BrokerAdmissionExact,
		},
		{
			name: "named creation keeps its name",
			request: func() ports.BrokerOpenStreamRequest {
				r := base
				r.Admission = ports.BrokerAdmissionCreateNamed
				r.Name = "work"
				return r
			}(),
			admission: ports.BrokerAdmissionCreateNamed,
			createAs:  "work",
		},
		{
			name: "ephemeral creation keeps its variant",
			request: func() ports.BrokerOpenStreamRequest {
				r := base
				r.Admission = ports.BrokerAdmissionCreateEphemeral
				return r
			}(),
			admission: ports.BrokerAdmissionCreateEphemeral,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			open := openMessage(1, tc.request)
			require.Equal(t, tc.admission, open.Admission)
			require.Equal(t, tc.createAs, open.Name)
			require.Equal(t, tc.request.Target, open.Target)

			// The same values must survive the ceiling-checked wire round trip
			// the daemon actually decodes.
			decoded, err := DecodeClient(mustEncodeClient(t, open), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			admitting, ok := decoded.(Open)
			require.True(t, ok)
			require.Equal(t, tc.admission, admitting.Admission)
			require.Equal(t, tc.createAs, admitting.Name)
		})
	}
}
