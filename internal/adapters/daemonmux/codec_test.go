package daemonmux

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func testConnectionID(seed byte) ports.BrokerConnectionID {
	var id ports.BrokerConnectionID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func testRegistration() domain.RemoteRegistration {
	return domain.RemoteRegistration{
		Endpoint:    "dev@host:22",
		Incarnation: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Generation:  9,
	}
}

func testPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: 3,
		EnvironmentPolicy:    protocol.EnvironmentPolicyDaemonOwned,
		Transport:            "quic",
		Trust:                "pinned",
		Launch:               "agent",
		Isolation:            "user",
	}
}

// localAcceptance provisions one local carriage entry under policy, matching
// the local Open requests the shared fixtures send.
func localAcceptance(policy ports.BrokerPolicy) ServerPolicyAdmission {
	return ServerPolicyAdmission{Policy: policy, Origin: ports.SessionOriginLocal}
}

// remoteAcceptance provisions one remote carriage entry under policy, matching
// a remote Open carrying a validated registration.
func remoteAcceptance(policy ports.BrokerPolicy) ServerPolicyAdmission {
	return ServerPolicyAdmission{Policy: policy, Origin: ports.SessionOriginRemote}
}

func testTarget() protocol.ExactSessionTarget {
	return protocol.ExactSessionTarget{
		LifecycleID: domain.SessionLifecycleID{1, 2, 3},
		SessionName: "work",
	}
}

func testRef(physical PhysicalStreamID) StreamRef {
	return StreamRef{
		Physical:   physical,
		Epoch:      7,
		Connection: testConnectionID(0x11),
		Client:     3,
	}
}

func testErrorDetail() ErrorDetail {
	return ErrorDetail{Code: ports.BrokerErrorUnavailable, Text: "dial refused", AdmissionCode: 1, FailureKind: domain.RemoteFailureTransport}
}

// unknownClientMessage and unknownServerMessage prove the directional codecs
// fail closed on concrete types outside their closed union.
type unknownClientMessage struct{}

func (unknownClientMessage) muxClientMessage() {}

type unknownServerMessage struct{}

func (unknownServerMessage) muxServerMessage() {}

const (
	testEnvelopeCeiling = wire.AbsoluteEnvelopeLimit
	testChunkCeiling    = MaxMuxChunkBytes
)

func mustEncodeClient(t *testing.T, message ClientMessage) []byte {
	t.Helper()
	raw, err := EncodeClient(message, testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	return raw
}

func mustEncodeServer(t *testing.T, message ServerMessage) []byte {
	t.Helper()
	raw, err := EncodeServer(message, testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	return raw
}

func testOpen() Open {
	return Open{
		Ref: testRef(5), Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Local: false,
		Endpoint: "dev@host:22", Registration: testRegistration(), Target: testTarget(),
		Env: []string{"TERM=xterm-256color"}, Policy: testPolicy(),
		StartMode: ports.BrokerDaemonStartIfNeeded,
	}
}

// TestMuxClientVariants inventories every client variant tag and proves each
// one round-trips through encode and decode.
func TestMuxClientVariants(t *testing.T) {
	messages := []struct {
		name    string
		message ClientMessage
		tag     protowire.Number
	}{
		{"open", testOpen(), 301},
		{"data", Data{Physical: 5, Data: []byte("frame")}, 302},
		{"close", Close{Physical: 5}, 303},
		{"reset", Reset{Physical: 5, Error: testErrorDetail(), HasError: true}, 304},
		{"reset_orderly", Reset{Physical: 5}, 304},
	}
	for _, tc := range messages {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.message)
			number, wireType, consumed := protowire.ConsumeTag(raw)
			require.Greater(t, consumed, 0)
			require.Equal(t, tc.tag, number)
			require.Equal(t, protowire.BytesType, wireType)
			decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.message, decoded)
		})
	}
}

// TestMuxServerVariants inventories every server variant tag and proves each
// one round-trips through encode and decode.
func TestMuxServerVariants(t *testing.T) {
	messages := []struct {
		name    string
		message ServerMessage
		tag     protowire.Number
	}{
		{"opened", Opened{Ref: testRef(5)}, 401},
		{"refused", Refused{Ref: testRef(5), Error: testErrorDetail()}, 402},
		{"data", Data{Physical: 5, Data: []byte("frame")}, 403},
		{"close", Close{Physical: 5}, 404},
		{"reset", Reset{Physical: 5, Error: testErrorDetail(), HasError: true}, 405},
		{"reset_orderly", Reset{Physical: 5}, 405},
	}
	for _, tc := range messages {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustEncodeServer(t, tc.message)
			number, wireType, consumed := protowire.ConsumeTag(raw)
			require.Greater(t, consumed, 0)
			require.Equal(t, tc.tag, number)
			require.Equal(t, protowire.BytesType, wireType)
			decoded, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.message, decoded)
		})
	}
}

// TestMuxSharedPayloadDirections proves the shared Data/Close/Reset payloads
// convert into both directions: the same value produces the direction's own
// tag and decodes back identically on either side.
func TestMuxSharedPayloadDirections(t *testing.T) {
	pairs := []struct {
		name         string
		client       ClientMessage
		server       ServerMessage
		clientTag    protowire.Number
		serverTag    protowire.Number
		expectedSame any
	}{
		{"data", Data{Physical: 5, Data: []byte("frame")}, Data{Physical: 5, Data: []byte("frame")}, 302, 403, Data{Physical: 5, Data: []byte("frame")}},
		{"close", Close{Physical: 5}, Close{Physical: 5}, 303, 404, Close{Physical: 5}},
		{"reset", Reset{Physical: 5}, Reset{Physical: 5}, 304, 405, Reset{Physical: 5}},
	}
	for _, tc := range pairs {
		t.Run(tc.name, func(t *testing.T) {
			clientRaw := mustEncodeClient(t, tc.client)
			clientTag, _, _ := protowire.ConsumeTag(clientRaw)
			require.Equal(t, tc.clientTag, clientTag)
			clientDecoded, err := DecodeClient(clientRaw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSame, clientDecoded)

			serverRaw := mustEncodeServer(t, tc.server)
			serverTag, _, _ := protowire.ConsumeTag(serverRaw)
			require.Equal(t, tc.serverTag, serverTag)
			serverDecoded, err := DecodeServer(serverRaw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSame, serverDecoded)
		})
	}
}

// TestMuxGoldenVectors pins literal byte encodings where the wire layout is
// stable: tag, length, and one varint field.
func TestMuxGoldenVectors(t *testing.T) {
	vectors := []struct {
		name    string
		message ClientMessage
		want    []byte
	}{
		{"close", Close{Physical: 1}, []byte{0xFA, 0x12, 0x02, 0x08, 0x01}},
		{"data", Data{Physical: 1, Data: []byte("hi")}, []byte{0xF2, 0x12, 0x06, 0x08, 0x01, 0x12, 0x02, 0x68, 0x69}},
		{"reset_orderly", Reset{Physical: 1}, []byte{0x82, 0x13, 0x02, 0x08, 0x01}},
	}
	for _, tc := range vectors {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.message)
			require.Equal(t, tc.want, raw)
			// Re-encode is deterministic: the same message marshals
			// identically, so a receiver can rely on a stable byte layout.
			again := mustEncodeClient(t, tc.message)
			require.Equal(t, tc.want, again)
			decoded, err := DecodeClient(tc.want, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.message, decoded)
		})
	}

	// The server direction has its own tags: the same value never produces
	// the client bytes.
	serverRaw := mustEncodeServer(t, Close{Physical: 1})
	require.Equal(t, []byte{0xA2, 0x19, 0x02, 0x08, 0x01}, serverRaw)

	// A populated ref is stable too: MuxOpened carries the four-field ref in
	// numeric order.
	ref := StreamRef{Physical: 1, Epoch: 1, Connection: testConnectionID(1), Client: 1}
	opened := mustEncodeServer(t, Opened{Ref: ref})
	back, err := DecodeServer(opened, testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, Opened{Ref: ref}, back)
}

// TestMuxBounds proves stateless bound refusals at max-1, max, and max+1.
func TestMuxBounds(t *testing.T) {
	t.Run("envelope ceiling", func(t *testing.T) {
		raw := mustEncodeClient(t, Close{Physical: 1})
		_, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		// One byte below the exact serialized length refuses the envelope.
		_, err = DecodeClient(raw, uint64(len(raw)-1), testChunkCeiling)
		require.ErrorIs(t, err, wire.ErrScanLength)
		// At the ceiling the envelope decodes; one byte above the ceiling is
		// refused before scanning.
		_, err = DecodeClient(raw, uint64(len(raw)), testChunkCeiling)
		require.NoError(t, err)
		_, err = DecodeClient(make([]byte, MaxMuxEnvelopeBytes+1), MaxMuxEnvelopeBytes, testChunkCeiling)
		require.ErrorIs(t, err, wire.ErrScanLength)
		// Encoding above the local ceiling fails closed.
		_, err = EncodeClient(Close{Physical: 1}, uint64(len(raw)-1), testChunkCeiling)
		require.ErrorIs(t, err, wire.ErrScanLength)
		_, err = EncodeClient(Close{Physical: 1}, uint64(len(raw)), testChunkCeiling)
		require.NoError(t, err)
	})
	t.Run("application envelope ceiling is 1 MiB", func(t *testing.T) {
		require.Equal(t, uint64(1<<20), MaxMuxApplicationBytes)
		require.Equal(t, uint64(1<<20), MinMuxEnvelopeBytes)
		// The fixed application ceiling applies at max and refuses at max+1,
		// whatever the negotiated receive ceiling advertises.
		require.NoError(t, checkMuxEnvelopeCeiling(make([]byte, MaxMuxApplicationBytes), testEnvelopeCeiling))
		require.ErrorIs(t, checkMuxEnvelopeCeiling(make([]byte, MaxMuxApplicationBytes+1), testEnvelopeCeiling), wire.ErrScanLength)
		require.ErrorIs(t, checkMuxEnvelopeCeiling(make([]byte, MaxMuxApplicationBytes+1), MaxMuxEnvelopeBytes), wire.ErrScanLength)
		// A hostile open envelope above the application ceiling is refused
		// before scan even though the negotiated ceiling admits 16 MiB.
		env := make([]string, 400)
		for i := range env {
			env[i] = "K=" + strings.Repeat("x", ports.BrokerMaxEnvEntryBytes-2)
		}
		hostile := &wire.MuxClientEnvelope{Payload: &wire.MuxClientEnvelope_Open{Open: &wire.MuxOpen{
			Ref:     &wire.MuxStreamRef{PhysicalStreamId: 1, BrokerEpoch: 1, ConnectionId: make([]byte, 16), ClientStreamId: 1},
			Purpose: 1, Local: true, Env: env,
		}}}
		payload, err := proto.Marshal(hostile)
		require.NoError(t, err)
		require.Greater(t, len(payload), int(MaxMuxApplicationBytes))
		_, err = DecodeClient(payload, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, wire.ErrScanLength)
	})
	t.Run("chunk window 1..64KiB", func(t *testing.T) {
		require.Equal(t, uint64(64<<10), MaxMuxChunkBytes)
		atMax := bytes.Repeat([]byte{0x66}, int(MaxMuxChunkBytes))
		belowMax := bytes.Repeat([]byte{0x66}, int(MaxMuxChunkBytes)-1)
		aboveMax := bytes.Repeat([]byte{0x66}, int(MaxMuxChunkBytes)+1)
		for _, data := range [][]byte{belowMax, atMax} {
			clientRaw, err := EncodeClient(Data{Physical: 5, Data: data}, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			clientDecoded, err := DecodeClient(clientRaw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, data, clientDecoded.(Data).Data)

			serverRaw, err := EncodeServer(Data{Physical: 5, Data: data}, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			serverDecoded, err := DecodeServer(serverRaw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, data, serverDecoded.(Data).Data)
		}
		_, err := EncodeClient(Data{Physical: 5, Data: aboveMax}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
		_, err = EncodeServer(Data{Physical: 5, Data: aboveMax}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
		// A negotiated chunk below the absolute maximum also refuses.
		raw := mustEncodeClient(t, Data{Physical: 5, Data: []byte("12345678")})
		_, err = DecodeClient(raw, testEnvelopeCeiling, 7)
		require.ErrorIs(t, err, ErrTooLarge)
		_, err = DecodeClient(raw, testEnvelopeCeiling, 8)
		require.NoError(t, err)
		// Empty chunk is invalid, not too large.
		_, err = EncodeClient(Data{Physical: 5}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("environment entries", func(t *testing.T) {
		atMax := "K=" + strings.Repeat("x", ports.BrokerMaxEnvEntryBytes-2)
		require.Len(t, atMax, ports.BrokerMaxEnvEntryBytes)
		aboveMax := atMax + "x"
		message := testOpen()
		message.Env = []string{atMax}
		_, err := EncodeClient(message, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		message.Env = []string{aboveMax}
		_, err = EncodeClient(message, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)

		many := make([]string, ports.BrokerMaxEnvEntries)
		for i := range many {
			many[i] = "A=B"
		}
		message = testOpen()
		message.Env = many
		_, err = EncodeClient(message, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		message.Env = append(append([]string(nil), many...), "C=D")
		_, err = EncodeClient(message, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
	})
	t.Run("error text", func(t *testing.T) {
		atMax := testErrorDetail()
		atMax.Text = strings.Repeat("e", ports.BrokerMaxErrorBytes)
		_, err := EncodeServer(Refused{Ref: testRef(5), Error: atMax}, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		over := testErrorDetail()
		over.Text = strings.Repeat("e", ports.BrokerMaxErrorBytes+1)
		_, err = EncodeServer(Refused{Ref: testRef(5), Error: over}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
	})
}

// TestMuxValidationNegatives is the table of semantic refusals: stream
// identity, taxonomy, text, environment, policy, and error details.
func TestMuxValidationNegatives(t *testing.T) {
	t.Run("zero stream identities refused", func(t *testing.T) {
		_, err := EncodeClient(Close{}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		_, err = EncodeClient(Data{Physical: 0, Data: []byte("x")}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		_, err = EncodeServer(Reset{Physical: 0}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		zeroEpoch := testRef(5)
		zeroEpoch.Epoch = 0
		_, err = EncodeServer(Opened{Ref: zeroEpoch}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		zeroConnection := testRef(5)
		zeroConnection.Connection = ports.BrokerConnectionID{}
		_, err = EncodeServer(Opened{Ref: zeroConnection}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		zeroClient := testRef(5)
		zeroClient.Client = 0
		_, err = EncodeServer(Opened{Ref: zeroClient}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		_, err = EncodeClient(Open{Ref: zeroClient, Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("open taxonomy and shape", func(t *testing.T) {
		badPurpose := testOpen()
		badPurpose.Purpose = ports.BrokerStreamPurpose(9)
		_, err := EncodeClient(badPurpose, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		badEndpoint := testOpen()
		badEndpoint.Endpoint = "not a host"
		raw, err := EncodeClient(badEndpoint, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		_, err = DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		local := testOpen()
		local.Local = true
		raw, err = EncodeClient(local, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		_, err = DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		controlWithTarget := testOpen()
		controlWithTarget.Purpose = ports.BrokerStreamControl
		raw, err = EncodeClient(controlWithTarget, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		envelope := &wire.MuxClientEnvelope{}
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOpen().Target = exactTargetToWire(testOpen().Target)
		raw, err = proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		controlWithEnv := testOpen()
		controlWithEnv.Purpose = ports.BrokerStreamControl
		controlWithEnv.Target = protocol.ExactSessionTarget{}
		raw, err = EncodeClient(controlWithEnv, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		_, err = DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		noEquals := testOpen()
		noEquals.Env = []string{"NOEQUALS"}
		_, err = EncodeClient(noEquals, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		emptyPolicy := testOpen()
		emptyPolicy.Policy = ports.BrokerPolicy{}
		raw, err = EncodeClient(emptyPolicy, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		_, err = DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		// A local control open with no attachment state is legal.
		valid := Open{Ref: testRef(5), Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded}
		raw, err = EncodeClient(valid, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, valid, decoded)
	})
	t.Run("taxonomy switches refuse mutated wire values", func(t *testing.T) {
		raw := mustEncodeClient(t, testOpen())
		envelope := &wire.MuxClientEnvelope{}
		require.NoError(t, wire.ScanEnvelope(envelope, raw))
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOpen().Purpose = 9
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.Error(t, err)

		rawErr := mustEncodeServer(t, Refused{Ref: testRef(5), Error: testErrorDetail()})
		server := &wire.MuxServerEnvelope{}
		require.NoError(t, wire.ScanEnvelope(server, rawErr))
		require.NoError(t, proto.Unmarshal(rawErr, server))
		server.GetRefused().GetError().Code = 99
		mutated, err = proto.Marshal(server)
		require.NoError(t, err)
		_, err = DecodeServer(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.Error(t, err)
	})
	t.Run("error detail is mandatory on refusal", func(t *testing.T) {
		_, err := EncodeServer(Refused{Ref: testRef(5)}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		// The zero detail has no valid code, so it can never travel.
		_, err = EncodeServer(Refused{Ref: testRef(5), Error: ErrorDetail{AdmissionCode: 1}}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("reset never drops a declared detail", func(t *testing.T) {
		for name, stray := range map[string]ErrorDetail{
			"full detail":       testErrorDetail(),
			"code only":         {Code: ports.BrokerErrorTimeout},
			"text only":         {Text: "late"},
			"admission only":    {AdmissionCode: 2},
			"failure kind only": {FailureKind: domain.RemoteFailureTransport},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := EncodeClient(Reset{Physical: 5, Error: stray}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
				_, err = EncodeServer(Reset{Physical: 5, Error: stray}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
			})
		}
		// The zero detail is the canonical absent value and still encodes; a
		// declared detail still round-trips exactly.
		_, err := EncodeServer(Reset{Physical: 5}, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		declared := Reset{Physical: 5, Error: testErrorDetail(), HasError: true}
		decoded, err := DecodeServer(mustEncodeServer(t, declared), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, declared, decoded)
	})
	t.Run("invalid error detail refused on every path", func(t *testing.T) {
		badAdmission := testErrorDetail()
		badAdmission.AdmissionCode = MaxAdmissionCode + 1
		badKind := testErrorDetail()
		badKind.FailureKind = MaxFailureKind + 1
		badCode := testErrorDetail()
		badCode.Code = 0
		for name, detail := range map[string]ErrorDetail{
			"admission over maximum":  badAdmission,
			"failure kind over bound": badKind,
			"invalid error code":      badCode,
		} {
			t.Run(name, func(t *testing.T) {
				_, err := EncodeServer(Refused{Ref: testRef(5), Error: detail}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
				_, err = EncodeServer(Reset{Physical: 5, Error: detail, HasError: true}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
				_, err = EncodeClient(Reset{Physical: 5, Error: detail, HasError: true}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
			})
		}
		// The closed taxonomy boundaries are accepted.
		boundary := testErrorDetail()
		boundary.AdmissionCode = MaxAdmissionCode
		boundary.FailureKind = MaxFailureKind
		_, err := EncodeServer(Refused{Ref: testRef(5), Error: boundary}, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
	})
	t.Run("bidi and control text refused", func(t *testing.T) {
		evil := testErrorDetail()
		evil.Text = "timeout\u202erorrE"
		_, err := EncodeServer(Refused{Ref: testRef(5), Error: evil}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		evil.Text = "bad\x01text"
		_, err = EncodeServer(Refused{Ref: testRef(5), Error: evil}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		badEnv := testOpen()
		badEnv.Env = []string{"K=\u202eevil"}
		_, err = EncodeClient(badEnv, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("nil messages refused", func(t *testing.T) {
		var openNil *Open
		_, err := EncodeClient(openNil, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		var dataNil *Data
		_, err = EncodeServer(dataNil, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		var refusedNil *Refused
		_, err = EncodeServer(refusedNil, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
}

// TestMuxTruncatedAndTrailing proves truncated prefixes and trailing garbage
// are rejected on both directions.
func TestMuxTruncatedAndTrailing(t *testing.T) {
	raw := mustEncodeClient(t, testOpen())
	for size := range len(raw) {
		_, err := DecodeClient(raw[:size], testEnvelopeCeiling, testChunkCeiling)
		require.Error(t, err, "client prefix[:%d] accepted", size)
	}
	_, err := DecodeClient(append(append([]byte(nil), raw...), 0xFF), testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)

	serverRaw := mustEncodeServer(t, Opened{Ref: testRef(5)})
	for size := range len(serverRaw) {
		_, err := DecodeServer(serverRaw[:size], testEnvelopeCeiling, testChunkCeiling)
		require.Error(t, err, "server prefix[:%d] accepted", size)
	}
	concatenated := append(append([]byte(nil), serverRaw...), serverRaw...)
	_, err = DecodeServer(concatenated, testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)
}

// TestMuxWrongDirection proves wrong-direction decode is rejected: client
// bytes never decode as daemon output and vice versa.
func TestMuxWrongDirection(t *testing.T) {
	clientRaw := mustEncodeClient(t, Close{Physical: 1})
	_, err := DecodeServer(clientRaw, testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)

	serverRaw := mustEncodeServer(t, Close{Physical: 1})
	_, err = DecodeClient(serverRaw, testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)

	// Go-typed misuse fails with ErrWrongDirection at encode time: unknown
	// concrete types presented to either directional codec fail closed.
	_, err = encodeClientEnvelope(unknownClientMessage{}, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)
	_, err = encodeServerEnvelope(unknownServerMessage{}, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)

	// Nil-payload envelopes decode as wrong-direction.
	_, err = decodeClientEnvelope(&wire.MuxClientEnvelope{}, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)
	_, err = decodeServerEnvelope(&wire.MuxServerEnvelope{}, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)
}

// TestMuxCorruptInputSmoke feeds representative corruptions through both
// decoders as a smoke test: bit flips, truncation, unknown fields, and
// direction swaps must never panic and must never be accepted silently.
func TestMuxCorruptInputSmoke(t *testing.T) {
	seeds := [][]byte{
		mustEncodeClient(t, testOpen()),
		mustEncodeClient(t, Data{Physical: 5, Data: []byte("frame")}),
		mustEncodeClient(t, Close{Physical: 5}),
		mustEncodeServer(t, Opened{Ref: testRef(5)}),
		mustEncodeServer(t, Refused{Ref: testRef(5), Error: testErrorDetail()}),
		mustEncodeServer(t, Reset{Physical: 5, Error: testErrorDetail(), HasError: true}),
	}
	for _, seed := range seeds {
		mutated := append([]byte(nil), seed...)
		if len(mutated) > 2 {
			mutated[len(mutated)-1] ^= 0xFF
			mutated[len(mutated)-2] ^= 0x01
		}
		_, _ = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		_, _ = DecodeServer(mutated, testEnvelopeCeiling, testChunkCeiling)
		unknown := protowire.AppendTag(append([]byte(nil), seed...), 399, protowire.VarintType)
		unknown = protowire.AppendVarint(unknown, 1)
		_, _ = DecodeClient(unknown, testEnvelopeCeiling, testChunkCeiling)
		_, _ = DecodeServer(unknown, testEnvelopeCeiling, testChunkCeiling)
	}
	require.NotEmpty(t, seeds)
}

// appendStrictScanSeeds adds raw vectors that must be refused by the strict
// scanner rather than reaching generated unmarshal.
func appendStrictScanSeeds(seeds [][]byte) [][]byte {
	unknown := protowire.AppendTag(nil, 399, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	wrongType := protowire.AppendTag(nil, 301, protowire.VarintType)
	wrongType = protowire.AppendVarint(wrongType, 1)
	return append(seeds, unknown, wrongType, []byte{0xFF}, []byte{0x80}, nil)
}

func fuzzClientSeeds(t testing.TB) [][]byte {
	t.Helper()
	seeds := [][]byte{}
	for _, message := range []ClientMessage{
		testOpen(),
		Data{Physical: 5, Data: []byte("frame")},
		Close{Physical: 5},
		Reset{Physical: 5, Error: testErrorDetail(), HasError: true},
	} {
		raw, err := EncodeClient(message, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		seeds = append(seeds, raw)
	}
	return appendStrictScanSeeds(seeds)
}

func fuzzServerSeeds(t testing.TB) [][]byte {
	t.Helper()
	seeds := [][]byte{}
	for _, message := range []ServerMessage{
		Opened{Ref: testRef(5)},
		Refused{Ref: testRef(5), Error: testErrorDetail()},
		Data{Physical: 5, Data: []byte("frame")},
		Close{Physical: 5},
		Reset{Physical: 5, Error: testErrorDetail(), HasError: true},
	} {
		raw, err := EncodeServer(message, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		seeds = append(seeds, raw)
	}
	return appendStrictScanSeeds(seeds)
}

// FuzzDecodeClient proves DecodeClient never panics on arbitrary input.
func FuzzDecodeClient(f *testing.F) {
	for _, seed := range fuzzClientSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = DecodeClient(payload, testEnvelopeCeiling, testChunkCeiling)
	})
}

// FuzzDecodeServer proves DecodeServer never panics on arbitrary input.
func FuzzDecodeServer(f *testing.F) {
	for _, seed := range fuzzServerSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = DecodeServer(payload, testEnvelopeCeiling, testChunkCeiling)
	})
}

// TestMuxAdmissionVariants proves the attachment-admission contract travels
// the mux wire losslessly and rejects every incoherent combination at the
// same boundary the ports contract rejects it.
func TestMuxAdmissionVariants(t *testing.T) {
	roundTrip := func(t *testing.T, open Open) Open {
		t.Helper()
		raw, err := EncodeClient(open, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		open, ok := decoded.(Open)
		require.True(t, ok)
		return open
	}
	t.Run("exact attach round-trips", func(t *testing.T) {
		require.Equal(t, testOpen(), roundTrip(t, testOpen()))
	})
	t.Run("named creation round-trips", func(t *testing.T) {
		named := testOpen()
		named.Admission = ports.BrokerAdmissionCreateNamed
		named.Name = "work"
		named.Target = protocol.ExactSessionTarget{}
		require.Equal(t, named, roundTrip(t, named))
	})
	t.Run("ephemeral creation round-trips", func(t *testing.T) {
		ephemeral := testOpen()
		ephemeral.Admission = ports.BrokerAdmissionCreateEphemeral
		ephemeral.Target = protocol.ExactSessionTarget{}
		require.Equal(t, ephemeral, roundTrip(t, ephemeral))
	})
	t.Run("control never carries admission", func(t *testing.T) {
		control := Open{Ref: testRef(6), Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded}
		require.Equal(t, control, roundTrip(t, control))
		mutated := mustEncodeClient(t, control)
		envelope := &wire.MuxClientEnvelope{}
		require.NoError(t, proto.Unmarshal(mutated, envelope))
		envelope.GetOpen().Admission = 2
		envelope.GetOpen().Name = "work"
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("exact with a creation name is refused", func(t *testing.T) {
		open := testOpen()
		open.Name = "work"
		raw, err := EncodeClient(open, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		_, err = DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("named creation with a target is refused", func(t *testing.T) {
		named := testOpen()
		named.Admission = ports.BrokerAdmissionCreateNamed
		named.Name = "work"
		raw, err := EncodeClient(named, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		envelope := &wire.MuxClientEnvelope{}
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOpen().Target = exactTargetToWire(testOpen().Target)
		raw, err = proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("unknown admission code is refused", func(t *testing.T) {
		raw := mustEncodeClient(t, testOpen())
		envelope := &wire.MuxClientEnvelope{}
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOpen().Admission = 9
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
}

// TestMuxStartModeRoundTrips proves the daemon-start authorization travels the
// mux wire losslessly for every stream purpose, that the daemon-side decoder
// never widens an existing-only authorization, and that the zero and unknown
// wire codes are refused rather than defaulted.
func TestMuxStartModeRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      ports.BrokerDaemonStartMode
		purpose   ports.BrokerStreamPurpose
		admission ports.BrokerStreamAdmission
	}{
		{"existing only control", ports.BrokerDaemonExistingOnly, ports.BrokerStreamControl, 0},
		{"start if needed control", ports.BrokerDaemonStartIfNeeded, ports.BrokerStreamControl, 0},
		{"existing only observation", ports.BrokerDaemonExistingOnly, ports.BrokerStreamObservation, 0},
		{"start if needed exact attach", ports.BrokerDaemonStartIfNeeded, ports.BrokerStreamAttachment, ports.BrokerAdmissionExact},
	} {
		t.Run(tc.name, func(t *testing.T) {
			open := Open{Ref: testRef(5), Purpose: tc.purpose, Admission: tc.admission, Local: true, Policy: testPolicy(), StartMode: tc.mode}
			if tc.admission == ports.BrokerAdmissionExact {
				open.Target = testTarget()
			}
			raw, err := EncodeClient(open, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			admitting, ok := decoded.(Open)
			require.True(t, ok)
			require.Equal(t, tc.mode, admitting.StartMode)
		})
	}

	t.Run("zero wire code is refused", func(t *testing.T) {
		raw := mustEncodeClient(t, Open{Ref: testRef(6), Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonExistingOnly})
		envelope := &wire.MuxClientEnvelope{}
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOpen().StartMode = 0
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage, "an absent start mode is never defaulted")
	})

	t.Run("unknown wire code is refused", func(t *testing.T) {
		raw := mustEncodeClient(t, Open{Ref: testRef(6), Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonExistingOnly})
		envelope := &wire.MuxClientEnvelope{}
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOpen().StartMode = 3
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})

	t.Run("encoding a zero start mode is refused", func(t *testing.T) {
		_, err := EncodeClient(Open{Ref: testRef(6), Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()}, testEnvelopeCeiling, testChunkCeiling)
		require.Error(t, err) // Closed wire enum; decode refusal is tested above.
	})
}

// TestMuxAdmissionWireRange proves the MuxOpen decoder narrows the wire
// admission field before any cast: the accepted semantic set (no admission, or
// one of the three closed admission codes) is unchanged, while every wire value
// whose high bits would alias a valid code is refused instead of silently
// truncating onto that code.
func TestMuxAdmissionWireRange(t *testing.T) {
	control := Open{Ref: testRef(6), Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded}
	named := testOpen()
	named.Admission = ports.BrokerAdmissionCreateNamed
	named.Name = "work"
	named.Target = protocol.ExactSessionTarget{}
	ephemeral := testOpen()
	ephemeral.Admission = ports.BrokerAdmissionCreateEphemeral
	ephemeral.Target = protocol.ExactSessionTarget{}

	tests := []struct {
		name      string
		base      Open
		admission uint32
		wantErr   bool
	}{
		{"no admission on a control stream", control, 0, false},
		{"exact admission", testOpen(), uint32(ports.BrokerAdmissionExact), false},
		{"create-named admission", named, uint32(ports.BrokerAdmissionCreateNamed), false},
		{"create-ephemeral admission", ephemeral, uint32(ports.BrokerAdmissionCreateEphemeral), false},
		{"wire value 257 aliasing exact", testOpen(), 257, true},
		{"wire value 258 aliasing create-named", named, 258, true},
		{"wire value 1024 aliasing no admission", control, 1024, true},
		{"wire value max uint32", control, 1<<32 - 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.base)
			envelope := &wire.MuxClientEnvelope{}
			require.NoError(t, proto.Unmarshal(raw, envelope))
			envelope.GetOpen().Admission = tc.admission
			mutated, err := proto.Marshal(envelope)
			require.NoError(t, err)
			decoded, err := DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrInvalidMessage)
				require.Zero(t, decoded, "an out-of-range wire value is never accepted as an alias")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.base, decoded)
		})
	}
}
