package brokerwire

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
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

func testOperationID(seed byte) ports.BrokerOperationID {
	var id ports.BrokerOperationID
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
		CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
		EnvironmentPolicy:    protocol.EnvironmentPolicyDaemonOwned,
		Transport:            "quic",
		Trust:                "pinned",
		Launch:               "agent",
		Isolation:            "user",
	}
}

func testTarget() protocol.ExactSessionTarget {
	return protocol.ExactSessionTarget{
		LifecycleID: domain.SessionLifecycleID{1, 2, 3},
		SessionName: "work",
	}
}

func testCatalogSession() catalogue.RemoteCatalogSession {
	return catalogue.RemoteCatalogSession{
		LifecycleID: domain.SessionLifecycleID{9, 8, 7},
		Name:        "work",
		State:       catalogue.RemoteCatalogSessionUp,
		Ephemeral:   true,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "shell", Detail: "1 pane", Attention: true}},
		Attached:    true,
		LastUsedSeq: 4,
		ActiveTabID: "tab-1",
		Reason:      domain.RemoteReasonRefreshing,
	}
}

// testDaemonObservation builds one observed, catalogue-valid remote daemon
// projection. Sessions are empty: they travel as separate parts.
func testDaemonObservation() ports.BrokerDaemonObservation {
	return ports.BrokerDaemonObservation{
		Endpoint:        "dev@host:22",
		DisplayOrigin:   "dev@host",
		Rank:            2,
		Registration:    testRegistration(),
		Policy:          testPolicy(),
		Identity:        ports.BrokerDaemonIdentity("authed-daemon"),
		Incarnation:     ports.BrokerDaemonIncarnation{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		ProtocolVersion: protocol.Version,
		Capabilities:    protocol.CapabilityResume | protocol.CapabilityUDP,
		Availability:    domain.RemoteAvailabilityReachable,
		Checking:        true,
		LastAttempt:     time.Unix(1700000000, 1).UTC(),
		LastSuccess:     time.Unix(1700000001, 2).UTC(),
		NextDue:         time.Unix(1700000002, 3).UTC(),
		FailureEpisode:  6,
		LastFailure:     domain.RemoteFailure{Kind: domain.RemoteFailureTransport},
		InventoryKnown:  true,
		Sessions:        []catalogue.RemoteCatalogSession{},
	}
}

// testLocalDaemonObservation builds the local daemon projection: no endpoint,
// no registration, always a valid policy, and an explicit availability.
func testLocalDaemonObservation() ports.BrokerDaemonObservation {
	return ports.BrokerDaemonObservation{
		Local:          true,
		DisplayOrigin:  "local",
		Policy:         testPolicy(),
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{},
	}
}

func testErrorDetail() ErrorDetail {
	return ErrorDetail{Code: ports.BrokerErrorUnavailable, Text: "dial refused", AdmissionCode: 1, FailureKind: domain.RemoteFailureTransport}
}

// unknownClientMessage and unknownServerMessage prove the directional
// codecs fail closed on concrete types outside their closed union.
type unknownClientMessage struct{}

func (unknownClientMessage) brokerClientMessage() {}

type unknownServerMessage struct{}

func (unknownServerMessage) brokerServerMessage() {}

const (
	testEnvelopeCeiling = wire.AbsoluteEnvelopeLimit
	testChunkCeiling    = MaxStreamChunkBytes
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

// TestBrokerPreambleByteLengthHelper proves the exposed 4 KiB byte-length
// helper accepts at the boundary and refuses one byte over, for both encoded
// and received preambles.
func TestBrokerPreambleByteLengthHelper(t *testing.T) {
	require.Equal(t, wire.PreambleLimit, BrokerPreambleLimit)
	require.Equal(t, 4<<10, BrokerPreambleLimit)
	require.NoError(t, CheckPreambleSize(nil))
	require.NoError(t, CheckPreambleSize(make([]byte, BrokerPreambleLimit)))
	require.ErrorIs(t, CheckPreambleSize(make([]byte, BrokerPreambleLimit+1)), ErrPreambleRejected)

	encoded, err := proto.Marshal(EncodePreambleRequest(defaultBrokerCeilings()))
	require.NoError(t, err)
	require.NoError(t, CheckPreambleSize(encoded))
	response, err := proto.Marshal(EncodePreambleResponse(true, defaultBrokerCeilings(), 0))
	require.NoError(t, err)
	require.NoError(t, CheckPreambleSize(response))
}

// TestBrokerPreambleRoles proves the broker preamble uses roles 3/4, the
// existing magic/epoch/exact version, zero capability bits, the 4 KiB
// preamble bound, and precise refusal mapping.
// TestBrokerCeilingsExportedSurface proves the exported ceiling value the
// connection dispatcher consumes stays equivalent to the preamble helpers'
// internal view: defaults are negotiable, minima are element-wise, and a
// non-negotiable advertisement is refused.
func TestBrokerCeilingsExportedSurface(t *testing.T) {
	defaults := DefaultCeilings()
	require.NoError(t, defaults.Validate())
	require.Equal(t, defaultBrokerCeilings(), defaults)

	smaller := Ceilings{MaxReceiveEnvelopeBytes: MinBrokerEnvelopeBytes, StreamChunkLimit: 1024}
	require.NoError(t, smaller.Validate())
	require.Equal(t, smaller, EffectiveCeilings(defaults, smaller))
	require.Equal(t, smaller, EffectiveCeilings(smaller, defaults))
	require.Equal(t, defaults, EffectiveCeilings(defaults, defaults))

	for _, invalid := range []Ceilings{
		{},
		{MaxReceiveEnvelopeBytes: MinBrokerEnvelopeBytes - 1, StreamChunkLimit: MaxStreamChunkBytes},
		{MaxReceiveEnvelopeBytes: MaxBrokerEnvelopeBytes + 1, StreamChunkLimit: MaxStreamChunkBytes},
		{MaxReceiveEnvelopeBytes: MaxBrokerEnvelopeBytes, StreamChunkLimit: 0},
		{MaxReceiveEnvelopeBytes: MaxBrokerEnvelopeBytes, StreamChunkLimit: MaxStreamChunkBytes + 1},
	} {
		require.ErrorIs(t, invalid.Validate(), ErrPreambleRejected)
	}

	// The decoded preamble's exported ceilings drive the codec ceilings.
	request := EncodePreambleRequest(smaller)
	decoded, err := DecodePreambleRequest(request)
	require.NoError(t, err)
	require.Equal(t, smaller, decoded.Ceilings)
	require.NoError(t, decoded.Ceilings.Validate())
}

func TestBrokerPreambleRoles(t *testing.T) {
	t.Run("client role is 3 and server role is 4", func(t *testing.T) {
		request := EncodePreambleRequest(defaultBrokerCeilings())
		require.Equal(t, BrokerRoleClient, request.GetRole().GetRole())
		require.Equal(t, uint32(3), request.GetRole().GetRole())
		response := EncodePreambleResponse(true, defaultBrokerCeilings(), 0)
		require.Equal(t, BrokerRoleServer, response.GetRole().GetRole())
		require.Equal(t, uint32(4), response.GetRole().GetRole())
	})
	t.Run("magic epoch version match session contract", func(t *testing.T) {
		request := EncodePreambleRequest(defaultBrokerCeilings())
		require.Equal(t, wire.PreambleMagic, request.GetMagic())
		require.Equal(t, wire.ProtocolEpoch, request.GetEpoch())
		require.Equal(t, uint32(protocol.Version), request.GetVersion())
		require.Zero(t, request.GetCapabilityBits())
		response := EncodePreambleResponse(true, defaultBrokerCeilings(), 0)
		require.Equal(t, wire.PreambleMagic, response.GetMagic())
		require.Equal(t, wire.ProtocolEpoch, response.GetEpoch())
		require.Equal(t, uint32(protocol.Version), response.GetVersion())
		require.Zero(t, response.GetCapabilityBits())
	})
	t.Run("preamble fits 4KiB", func(t *testing.T) {
		raw, err := proto.Marshal(EncodePreambleRequest(defaultBrokerCeilings()))
		require.NoError(t, err)
		require.LessOrEqual(t, len(raw), BrokerPreambleLimit)
		require.Equal(t, 4<<10, BrokerPreambleLimit)
		raw, err = proto.Marshal(EncodePreambleResponse(true, defaultBrokerCeilings(), 0))
		require.NoError(t, err)
		require.LessOrEqual(t, len(raw), BrokerPreambleLimit)
	})
	t.Run("session roles rejected", func(t *testing.T) {
		request := EncodePreambleRequest(defaultBrokerCeilings())
		request.Role.Role = 1
		_, err := DecodePreambleRequest(request)
		require.ErrorIs(t, err, ErrPreambleRejected)
		require.Equal(t, RejectionWrongRole, RejectionCodeFor(request, err))
		request.Role.Role = 2
		_, err = DecodePreambleRequest(request)
		require.ErrorIs(t, err, ErrPreambleRejected)
		response := EncodePreambleResponse(true, defaultBrokerCeilings(), 0)
		response.Role.Role = 2
		_, err = DecodePreambleResponse(response)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
	t.Run("round trip negotiates minima", func(t *testing.T) {
		local := defaultBrokerCeilings()
		remote := brokerCeilings{MaxReceiveEnvelopeBytes: testEnvelopeCeiling / 2, StreamChunkLimit: 1024}
		decoded, err := DecodePreambleRequest(EncodePreambleRequest(remote))
		require.NoError(t, err)
		require.Equal(t, remote, decoded.Ceilings)
		require.Equal(t, remote, effectiveBrokerCeilings(local, decoded.Ceilings))
		resp, err := DecodePreambleResponse(EncodePreambleResponse(true, remote, 0))
		require.NoError(t, err)
		require.True(t, resp.Accepted)
		require.Equal(t, remote, resp.Ceilings)
	})
	t.Run("advertised envelope window 1MiB..16MiB", func(t *testing.T) {
		require.Equal(t, uint64(1<<20), uint64(MinBrokerEnvelopeBytes))
		require.Equal(t, uint64(16<<20), uint64(MaxBrokerEnvelopeBytes))
		for _, advertised := range []uint64{0, MinBrokerEnvelopeBytes - 1, MaxBrokerEnvelopeBytes + 1} {
			request := EncodePreambleRequest(defaultBrokerCeilings())
			request.MaxReceiveEnvelopeBytes = advertised
			_, err := DecodePreambleRequest(request)
			require.ErrorIs(t, err, ErrPreambleRejected)
			require.Equal(t, RejectionLimitRefused, RejectionCodeFor(request, err))
		}
		for _, advertised := range []uint64{MinBrokerEnvelopeBytes, MaxBrokerEnvelopeBytes} {
			request := EncodePreambleRequest(defaultBrokerCeilings())
			request.MaxReceiveEnvelopeBytes = advertised
			_, err := DecodePreambleRequest(request)
			require.NoError(t, err)
		}
	})
	t.Run("stream chunk window 1..64KiB", func(t *testing.T) {
		require.Equal(t, uint64(64<<10), uint64(MaxStreamChunkBytes))
		for _, advertised := range []uint64{0, MaxStreamChunkBytes + 1} {
			request := EncodePreambleRequest(defaultBrokerCeilings())
			request.OutputDataLimit = advertised
			_, err := DecodePreambleRequest(request)
			require.ErrorIs(t, err, ErrPreambleRejected)
			require.Equal(t, RejectionLimitRefused, RejectionCodeFor(request, err))
		}
		for _, advertised := range []uint64{MinStreamChunkBytes, MaxStreamChunkBytes} {
			request := EncodePreambleRequest(defaultBrokerCeilings())
			request.OutputDataLimit = advertised
			_, err := DecodePreambleRequest(request)
			require.NoError(t, err)
		}
	})
	t.Run("caps zero", func(t *testing.T) {
		request := EncodePreambleRequest(defaultBrokerCeilings())
		request.CapabilityBits = 1
		_, err := DecodePreambleRequest(request)
		require.ErrorIs(t, err, ErrPreambleRejected)
		require.Equal(t, RejectionLimitRefused, RejectionCodeFor(request, err))
		response := EncodePreambleResponse(true, defaultBrokerCeilings(), 0)
		response.CapabilityBits = 7
		_, err = DecodePreambleResponse(response)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
	t.Run("refusal mapping is precise", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(*wire.PreambleRequest)
			want   uint32
		}{
			{"bad magic", func(r *wire.PreambleRequest) { r.Magic = 0xDEAD }, RejectionBadMagic},
			{"epoch", func(r *wire.PreambleRequest) { r.Epoch++ }, RejectionEpochMismatch},
			{"version", func(r *wire.PreambleRequest) { r.Version++ }, RejectionVersionMismatch},
			{"role", func(r *wire.PreambleRequest) { r.Role.Role = 9 }, RejectionWrongRole},
			{"caps", func(r *wire.PreambleRequest) { r.CapabilityBits = 1 }, RejectionLimitRefused},
			{"envelope floor", func(r *wire.PreambleRequest) { r.MaxReceiveEnvelopeBytes = 1 }, RejectionLimitRefused},
			{"chunk", func(r *wire.PreambleRequest) { r.OutputDataLimit = 0 }, RejectionLimitRefused},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				request := EncodePreambleRequest(defaultBrokerCeilings())
				tc.mutate(request)
				_, err := DecodePreambleRequest(request)
				require.ErrorIs(t, err, ErrPreambleRejected)
				require.Equal(t, tc.want, RejectionCodeFor(request, err))
			})
		}
		// Refusal responses decode as rejected with their code.
		response := EncodePreambleResponse(false, defaultBrokerCeilings(), RejectionVersionMismatch)
		require.False(t, response.GetAccepted())
		require.Equal(t, RejectionVersionMismatch, response.GetRejection().GetCode())
		decoded, err := DecodePreambleResponse(response)
		require.ErrorIs(t, err, ErrPreambleRejected)
		require.Equal(t, RejectionVersionMismatch, decoded.Code)
	})
	t.Run("nil preambles rejected", func(t *testing.T) {
		_, err := DecodePreambleRequest(nil)
		require.ErrorIs(t, err, ErrPreambleRejected)
		_, err = DecodePreambleResponse(nil)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
}

// TestBrokerClientVariants inventories every client variant tag.
func TestBrokerClientVariants(t *testing.T) {
	connection := testConnectionID(0x11)
	operation := testOperationID(0x44)
	messages := []struct {
		name    string
		message ClientMessage
		tag     protowire.Number
	}{
		{"register", Register{}, 101},
		{"subscribe", Subscribe{Epoch: 7, Connection: connection, Generation: 1}, 102},
		{"resync", Resync{Epoch: 7, Connection: connection, Generation: 2}, 103},
		{"unsubscribe", Unsubscribe{Epoch: 7, Connection: connection, Generation: 3}, 104},
		{"add_host", AddHost{Epoch: 7, Connection: connection, Operation: operation, Endpoint: "dev@host:22", Policy: testPolicy()}, 105},
		{"remove_host", RemoveHost{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration()}, 106},
		{"reconcile", Reconcile{Epoch: 7, Connection: connection, Registration: testRegistration()}, 107},
		{"open_stream", OpenStream{Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Endpoint: "dev@host:22", Registration: testRegistration(), Target: testTarget(), Env: []string{"TERM=xterm"}, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded}, 108},
		{"client_stream_data", ClientStreamData{Epoch: 7, Connection: connection, Stream: 3, Data: []byte("frame")}, 109},
		{"close_stream", CloseStream{Epoch: 7, Connection: connection, Stream: 3}, 110},
		{"update_host_policy", UpdateHostPolicy{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration(), Policy: testPolicy()}, 111},
		{"start_preview", testStartPreview(connection), 112},
		{"cancel_preview", CancelPreview{Epoch: 7, Connection: connection, Generation: 3}, 113},
	}
	require.Len(t, messages, 13)
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

// TestBrokerOpenStreamAdmissionVariants pins the attachment-admission
// contract: exact, create-named, and create-ephemeral round-trip, control
// carries none, and wrong admission/purpose/name/target combinations are
// refused exactly like ports.BrokerOpenStreamRequest.Validate.
func TestBrokerOpenStreamAdmissionVariants(t *testing.T) {
	connection := testConnectionID(0x11)
	base := func() OpenStream {
		return OpenStream{
			Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamAttachment,
			Endpoint: "dev@host:22", Registration: testRegistration(), Policy: testPolicy(),
			// An attachment or creation always carries the explicit
			// start-if-needed authorization; the zero mode is refused.
			StartMode: ports.BrokerDaemonStartIfNeeded,
		}
	}
	roundTrips := []struct {
		name    string
		message OpenStream
	}{
		{"exact", func() OpenStream {
			message := base()
			message.Admission = ports.BrokerAdmissionExact
			message.Target = testTarget()
			return message
		}()},
		{"create named", func() OpenStream {
			message := base()
			message.Admission = ports.BrokerAdmissionCreateNamed
			message.Name = "work"
			return message
		}()},
		{"create ephemeral", func() OpenStream {
			message := base()
			message.Admission = ports.BrokerAdmissionCreateEphemeral
			return message
		}()},
		{"control carries none", OpenStream{
			Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamControl,
			Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded,
		}},
	}
	for _, tc := range roundTrips {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.message)
			decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.message, decoded)
		})
	}

	t.Run("wrong combinations refused", func(t *testing.T) {
		exact := func() OpenStream {
			message := base()
			message.Admission = ports.BrokerAdmissionExact
			message.Target = testTarget()
			return message
		}
		cases := map[string]OpenStream{
			"attachment without admission": func() OpenStream { m := base(); return m }(),
			"exact with name": func() OpenStream {
				m := exact()
				m.Name = "work"
				return m
			}(),
			"named with target": func() OpenStream {
				m := base()
				m.Admission = ports.BrokerAdmissionCreateNamed
				m.Name = "work"
				m.Target = testTarget()
				return m
			}(),
			"named without name": func() OpenStream {
				m := base()
				m.Admission = ports.BrokerAdmissionCreateNamed
				return m
			}(),
			"ephemeral with name": func() OpenStream {
				m := base()
				m.Admission = ports.BrokerAdmissionCreateEphemeral
				m.Name = "work"
				return m
			}(),
			"control with admission": OpenStream{
				Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamControl,
				Local: true, Policy: testPolicy(), Admission: ports.BrokerAdmissionExact, StartMode: ports.BrokerDaemonStartIfNeeded,
			},
			"attachment with zero start mode": func() OpenStream { m := exact(); m.StartMode = 0; return m }(),
			"attachment with existing-only start mode": func() OpenStream {
				m := exact()
				m.StartMode = ports.BrokerDaemonExistingOnly
				return m
			}(),
			"observation with start-if-needed start mode": OpenStream{
				Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamObservation,
				Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded,
			},
		}
		for name, message := range cases {
			t.Run(name, func(t *testing.T) {
				_, err := EncodeClient(message, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
			})
		}
	})

	t.Run("unknown admission code refused on decode", func(t *testing.T) {
		message := base()
		message.Admission = ports.BrokerAdmissionExact
		message.Target = testTarget()
		raw := mustEncodeClient(t, message)
		envelope := &wire.BrokerClientEnvelope{}
		require.NoError(t, wire.ScanEnvelope(envelope, raw))
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOpenStream().Admission = 9
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})

	t.Run("start mode round-trips and is never defaulted", func(t *testing.T) {
		// The mode round-trips with the purpose it is legal for: an existing-only
		// authorization belongs to control/observation, a start-if-needed one to
		// attachment/control.
		control := func(mode ports.BrokerDaemonStartMode) OpenStream {
			return OpenStream{Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), StartMode: mode}
		}
		for _, message := range []OpenStream{
			control(ports.BrokerDaemonExistingOnly),
			control(ports.BrokerDaemonStartIfNeeded),
			func() OpenStream {
				m := base()
				m.Admission = ports.BrokerAdmissionExact
				m.Target = testTarget()
				m.StartMode = ports.BrokerDaemonStartIfNeeded
				return m
			}(),
		} {
			decoded, err := DecodeClient(mustEncodeClient(t, message), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			admitting, ok := decoded.(OpenStream)
			require.True(t, ok)
			require.Equal(t, message.StartMode, admitting.StartMode)
		}
		// An absent wire mode is refused, never widened into a spawn-capable
		// default, and an unknown code is refused too.
		message := base()
		message.Admission = ports.BrokerAdmissionExact
		message.Target = testTarget()
		raw := mustEncodeClient(t, message)
		envelope := &wire.BrokerClientEnvelope{}
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOpenStream().StartMode = 0
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		envelope.GetOpenStream().StartMode = 3
		mutated, err = proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		// An observation that authorized a spawn is refused rather than narrowed.
		envelope.GetOpenStream().Purpose = 3
		envelope.GetOpenStream().StartMode = 2
		envelope.GetOpenStream().Target = nil
		envelope.GetOpenStream().Admission = 0
		mutated, err = proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
}

// TestBrokerSnapshotDaemonByteForByte proves the daemon and session parts are
// byte-for-byte stable across an encode/decode/re-encode cycle, including an
// observed identity/incarnation/version/capability set, the local daemon, and a
// locally-bound session.
func TestBrokerSnapshotDaemonByteForByte(t *testing.T) {
	connection := testConnectionID(0x11)
	messages := []struct {
		name    string
		message SnapshotPart
	}{
		{"observed remote daemon", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 1, Part: SnapshotDaemonPart{HostIndex: 1, Daemon: testDaemonObservation(), SessionCount: 1}}},
		{"reachable remote without daemon", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 2, Part: SnapshotDaemonPart{HostIndex: 1, Daemon: func() ports.BrokerDaemonObservation {
			daemon := testDaemonObservation()
			daemon.Identity = ""
			daemon.Incarnation = ports.BrokerDaemonIncarnation{}
			daemon.ProtocolVersion = 0
			daemon.Capabilities = 0
			daemon.Availability = domain.RemoteAvailabilityNoDaemon
			daemon.LastFailure = domain.RemoteFailure{}
			daemon.LastSuccess = time.Time{}
			daemon.InventoryKnown = false
			return daemon
		}(), SessionCount: 0}}},
		{"local daemon", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 0, Part: SnapshotDaemonPart{HostIndex: 0, Daemon: testLocalDaemonObservation(), SessionCount: 1}}},
		{"local session", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 2, Part: SnapshotSessionPart{HostIndex: 0, SessionIndex: 0, Local: true, Session: testCatalogSession()}}},
	}
	for _, tc := range messages {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustEncodeServer(t, tc.message)
			decoded, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.message, decoded)
			again, err := EncodeServer(decoded, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, raw, again, "re-encode must be byte-for-byte identical")
		})
	}
}

// TestBrokerServerVariants inventories every server variant tag.
func TestBrokerServerVariants(t *testing.T) {
	connection := testConnectionID(0x11)
	operation := testOperationID(0x77)
	messages := []struct {
		name    string
		message ServerMessage
		tag     protowire.Number
	}{
		{"registered", Registered{Epoch: 7, Connection: connection}, 201},
		{"snapshot_begin", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 0, Part: SnapshotBegin{HostCount: 2, SessionCount: 2, TombstoneCount: 0, LocalPresent: true}}, 202},
		{"snapshot_daemon", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 1, Part: SnapshotDaemonPart{HostIndex: 0, Daemon: testLocalDaemonObservation(), SessionCount: 0}}, 202},
		{"snapshot_daemon_remote", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 2, Part: SnapshotDaemonPart{HostIndex: 1, Daemon: testDaemonObservation(), SessionCount: 1}}, 202},
		{"snapshot_session", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 3, Part: SnapshotSessionPart{HostIndex: 1, SessionIndex: 0, Session: testCatalogSession()}}, 202},
		{"snapshot_session_local", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 4, Part: SnapshotSessionPart{HostIndex: 0, SessionIndex: 0, Local: true, Session: testCatalogSession()}}, 202},
		{"snapshot_tombstone", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 5, Part: SnapshotTombstonePart{TombstoneIndex: 0, Registration: testRegistration(), RetiredRevision: 4}}, 202},
		{"snapshot_end", SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 6, Part: SnapshotEnd{}}, 202},
		{"operation_result", OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeFailed, Error: testErrorDetail(), HasError: true}, 203},
		{"operation_result_add", OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeOK, Registration: testRegistration()}, 203},
		{"operation_result_remove", OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeOK, Removed: true}, 203},
		{"stream_opened", StreamOpened{Epoch: 7, Connection: connection, Stream: 3}, 204},
		{"server_stream_data", ServerStreamData{Epoch: 7, Connection: connection, Stream: 3, Data: []byte("frame")}, 205},
		{"stream_closed", StreamClosed{Epoch: 7, Connection: connection, Stream: 3, Error: testErrorDetail(), HasError: true}, 206},
		{"stream_closed_orderly", StreamClosed{Epoch: 7, Connection: connection, Stream: 4}, 206},
		{"progress", Progress{Epoch: 7, Connection: connection, Stream: 3, Phase: BrokerProgressProbing, Text: "probing"}, 207},
		{"broker_error", BrokerErrorMessage{Epoch: 7, Connection: connection, Error: testErrorDetail()}, 208},
		{"shutdown", Shutdown{Epoch: 7, Connection: connection, Reason: ShutdownTerminating, Text: "stopping"}, 209},
		{"preview_publication", testPreviewPublication(connection), 210},
		{"preview_publication_error", PreviewPublication{Epoch: 7, Connection: connection, Generation: 5, Error: testErrorDetail(), HasError: true}, 210},
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

// TestBrokerDownSessionEmptyTabsRoundTrip proves a down session with an
// empty-present tab list survives a full encode/decode round trip: proto3
// repeated fields carry no presence, so the decoder must map an absent
// field back to an empty-present list rather than refusing it.
func TestBrokerDownSessionEmptyTabsRoundTrip(t *testing.T) {
	connection := testConnectionID(0x11)
	session := catalogue.RemoteCatalogSession{
		LifecycleID: domain.SessionLifecycleID{1, 2, 3},
		Name:        "down",
		State:       catalogue.RemoteCatalogSessionDown,
		Tabs:        []catalogue.RemoteCatalogTab{},
	}
	message := SnapshotPart{
		Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 2,
		Part: SnapshotSessionPart{HostIndex: 0, SessionIndex: 0, Session: session},
	}
	raw := mustEncodeServer(t, message)
	decoded, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, message, decoded)

	part, ok := decoded.(SnapshotPart)
	require.True(t, ok)
	sessionPart, ok := part.Part.(SnapshotSessionPart)
	require.True(t, ok)
	require.NotNil(t, sessionPart.Session.Tabs, "empty tabs decode as empty-present, never nil")
	require.Empty(t, sessionPart.Session.Tabs)
}

// TestBrokerTabIndexBound proves a tab index at the per-session maximum is a
// range refusal, not a semantic one: valid indexes are 0..max-1.
func TestBrokerTabIndexBound(t *testing.T) {
	connection := testConnectionID(0x11)
	envelope := &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_SnapshotPart{SnapshotPart: &wire.SnapshotPart{
		Scope:      &wire.BrokerScope{BrokerEpoch: 7, ConnectionId: connection[:]},
		Generation: 2,
		Revision:   5,
		Index:      2,
		Part: &wire.SnapshotPart_Session{Session: &wire.SnapshotSession{
			HostIndex: 0, SessionIndex: 0,
			Session: &wire.BrokerCatalogSession{
				LifecycleId: []byte{1, 2, 3},
				Name:        "down",
				State:       2,
				Tabs:        []*wire.BrokerCatalogTab{{Id: "tab", Index: catalogue.RemoteCatalogMaxTabsPerSess, Name: "tab"}},
			},
		}},
	}}}
	raw, err := proto.Marshal(envelope)
	require.NoError(t, err)
	_, err = DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
	require.ErrorIs(t, err, errConvertRange)
}

// mutateDaemonIndex re-encodes one daemon snapshot part envelope with its
// daemon index overwritten, so decode can be fed a value encode would never
// emit.
func mutateDaemonIndex(t *testing.T, part SnapshotPart, hostIndex uint32) []byte {
	t.Helper()
	raw := mustEncodeServer(t, part)
	envelope := &wire.BrokerServerEnvelope{}
	require.NoError(t, wire.ScanEnvelope(envelope, raw))
	require.NoError(t, proto.Unmarshal(raw, envelope))
	envelope.GetSnapshotPart().GetDaemon().HostIndex = hostIndex
	mutated, err := proto.Marshal(envelope)
	require.NoError(t, err)
	return mutated
}

// mutateSessionIndexes re-encodes one session snapshot part envelope with its
// host and session indexes overwritten.
func mutateSessionIndexes(t *testing.T, part SnapshotPart, hostIndex, sessionIndex uint32) []byte {
	t.Helper()
	raw := mustEncodeServer(t, part)
	envelope := &wire.BrokerServerEnvelope{}
	require.NoError(t, wire.ScanEnvelope(envelope, raw))
	require.NoError(t, proto.Unmarshal(raw, envelope))
	envelope.GetSnapshotPart().GetSession().HostIndex = hostIndex
	envelope.GetSnapshotPart().GetSession().SessionIndex = sessionIndex
	mutated, err := proto.Marshal(envelope)
	require.NoError(t, err)
	return mutated
}

// TestBrokerSnapshotIndexBounds proves snapshot part indexes are strict
// ranges: max-1 is accepted on both encode and decode, and max is refused as a
// range refusal on both paths. Daemon indexes are bounded by
// BrokerMaxDaemonsPerSnapshot and session indexes by BrokerMaxSessionsPerHost.
func TestBrokerSnapshotIndexBounds(t *testing.T) {
	connection := testConnectionID(0x11)
	daemonPart := func(hostIndex uint32) SnapshotPart {
		return SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 1, Part: SnapshotDaemonPart{HostIndex: hostIndex, Daemon: testDaemonObservation()}}
	}
	sessionPart := func(hostIndex, sessionIndex uint32) SnapshotPart {
		return SnapshotPart{Epoch: 7, Connection: connection, Generation: 2, Revision: 5, Index: 2, Part: SnapshotSessionPart{HostIndex: hostIndex, SessionIndex: sessionIndex, Session: testCatalogSession()}}
	}

	t.Run("daemon part at max-1 accepted", func(t *testing.T) {
		message := daemonPart(ports.BrokerMaxDaemonsPerSnapshot - 1)
		raw := mustEncodeServer(t, message)
		decoded, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, message, decoded)
		decoded, err = DecodeServer(mutateDaemonIndex(t, message, ports.BrokerMaxDaemonsPerSnapshot-1), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, message, decoded)
	})

	t.Run("daemon part at max refused", func(t *testing.T) {
		_, err := EncodeServer(daemonPart(ports.BrokerMaxDaemonsPerSnapshot), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
		_, err = DecodeServer(mutateDaemonIndex(t, daemonPart(0), ports.BrokerMaxDaemonsPerSnapshot), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
	})

	t.Run("daemon part at uint32 maximum refused", func(t *testing.T) {
		_, err := EncodeServer(daemonPart(^uint32(0)), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
		_, err = DecodeServer(mutateDaemonIndex(t, daemonPart(0), ^uint32(0)), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
	})

	t.Run("session part at max-1 accepted", func(t *testing.T) {
		message := sessionPart(ports.BrokerMaxDaemonsPerSnapshot-1, ports.BrokerMaxSessionsPerHost-1)
		raw := mustEncodeServer(t, message)
		decoded, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, message, decoded)
		decoded, err = DecodeServer(mutateSessionIndexes(t, message, ports.BrokerMaxDaemonsPerSnapshot-1, ports.BrokerMaxSessionsPerHost-1), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, message, decoded)
	})

	t.Run("session part host index at max refused", func(t *testing.T) {
		_, err := EncodeServer(sessionPart(ports.BrokerMaxDaemonsPerSnapshot, 0), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
		_, err = DecodeServer(mutateSessionIndexes(t, sessionPart(0, 0), ports.BrokerMaxDaemonsPerSnapshot, 0), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
	})

	t.Run("session part session index at max refused", func(t *testing.T) {
		_, err := EncodeServer(sessionPart(0, ports.BrokerMaxSessionsPerHost), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
		_, err = DecodeServer(mutateSessionIndexes(t, sessionPart(0, 0), 0, ports.BrokerMaxSessionsPerHost), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
	})
}

// TestBrokerGoldenVectors pins byte-for-byte wire encodings.
func TestBrokerGoldenVectors(t *testing.T) {
	connection := testConnectionID(0x11)
	raw := mustEncodeClient(t, Register{})
	decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, Register{}, decoded)
	// Re-encode is deterministic: the same message marshals identically.
	again, err := EncodeClient(Register{}, testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, raw, again)

	sub := Subscribe{Epoch: 7, Connection: connection, Generation: 1 << 40}
	rawSub := mustEncodeClient(t, sub)
	envelope := &wire.BrokerClientEnvelope{}
	require.NoError(t, wire.ScanEnvelope(envelope, rawSub))
	require.NoError(t, proto.Unmarshal(rawSub, envelope))
	require.Equal(t, uint64(7), envelope.GetSubscribe().GetScope().GetBrokerEpoch())
	require.Equal(t, uint64(1<<40), envelope.GetSubscribe().GetGeneration())
	back, err := DecodeClient(rawSub, testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, sub, back)

	// The passive reader flag survives the round trip, and an active
	// subscription encodes without it (proto3 omits false).
	passive := sub
	passive.Passive = true
	rawPassive := mustEncodeClient(t, passive)
	back, err = DecodeClient(rawPassive, testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, passive, back)
	require.Greater(t, len(rawPassive), len(rawSub))
}

// TestBrokerBounds proves stateless bound refusals.
func TestBrokerBounds(t *testing.T) {
	connection := testConnectionID(0x11)
	t.Run("envelope ceiling", func(t *testing.T) {
		raw := mustEncodeClient(t, Register{})
		_, err := DecodeClient(raw, uint64(len(raw)-1), testChunkCeiling)
		require.ErrorIs(t, err, wire.ErrScanLength)
		_, err = DecodeClient(make([]byte, testEnvelopeCeiling+1), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, wire.ErrScanLength)
	})
	t.Run("chunk bound", func(t *testing.T) {
		big := bytes.Repeat([]byte{0x66}, int(MaxStreamChunkBytes)+1)
		_, err := EncodeClient(ClientStreamData{Epoch: 7, Connection: connection, Stream: 3, Data: big}, testEnvelopeCeiling, testEnvelopeCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
		_, err = EncodeServer(ServerStreamData{Epoch: 7, Connection: connection, Stream: 3, Data: big}, testEnvelopeCeiling, testEnvelopeCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
		// A negotiated chunk below the absolute max also refuses.
		raw := mustEncodeClient(t, ClientStreamData{Epoch: 7, Connection: connection, Stream: 3, Data: []byte("12345678")})
		_, err = DecodeClient(raw, testEnvelopeCeiling, 4)
		require.ErrorIs(t, err, ErrTooLarge)
		// Empty chunk is invalid, not too large.
		_, err = EncodeClient(ClientStreamData{Epoch: 7, Connection: connection, Stream: 3}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("env entries", func(t *testing.T) {
		many := make([]string, ports.BrokerMaxEnvEntries+1)
		for i := range many {
			many[i] = "A=B"
		}
		_, err := EncodeClient(OpenStream{Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Endpoint: "dev@host:22", Registration: testRegistration(), Target: testTarget(), Env: many, Policy: testPolicy()}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
		oversize := []string{"K=" + strings.Repeat("x", ports.BrokerMaxEnvEntryBytes)}
		_, err = EncodeClient(OpenStream{Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Endpoint: "dev@host:22", Registration: testRegistration(), Target: testTarget(), Env: oversize, Policy: testPolicy()}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
	})
	t.Run("error text", func(t *testing.T) {
		oversize := testErrorDetail()
		oversize.Text = strings.Repeat("e", ports.BrokerMaxErrorBytes+1)
		_, err := EncodeServer(BrokerErrorMessage{Epoch: 7, Connection: connection, Error: oversize}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
	})
	t.Run("snapshot counts", func(t *testing.T) {
		_, err := EncodeServer(SnapshotPart{Epoch: 7, Connection: connection, Generation: 1, Revision: 1, Index: 0, Part: SnapshotBegin{HostCount: ports.BrokerMaxDaemonsPerSnapshot + 1}}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrTooLarge)
	})
	t.Run("control env carries no attachment state", func(t *testing.T) {
		_, err := EncodeClient(OpenStream{Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), Target: testTarget()}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
}

// TestBrokerValidationNegatives is the table of semantic refusals: IDs,
// UTF-8/control/bidi text, taxonomy, times, env/policy, snapshot parts.
func TestBrokerValidationNegatives(t *testing.T) {
	connection := testConnectionID(0x11)
	t.Run("zero scope", func(t *testing.T) {
		_, err := EncodeClient(Subscribe{Epoch: 0, Connection: connection}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		_, err = EncodeClient(Subscribe{Epoch: 7}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		_, err = EncodeServer(Registered{Epoch: 7}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("zero stream", func(t *testing.T) {
		_, err := EncodeClient(CloseStream{Epoch: 7, Connection: connection}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("error detail never travels without HasError", func(t *testing.T) {
		detail := testErrorDetail()
		// HasError=false is exactly the absent detail, whatever the outcome
		// and whatever field of the detail is nonzero.
		for name, stray := range map[string]ErrorDetail{
			"full detail":       detail,
			"code only":         {Code: ports.BrokerErrorTimeout},
			"text only":         {Text: "late"},
			"admission only":    {AdmissionCode: 2},
			"failure kind only": {FailureKind: domain.RemoteFailureTransport},
		} {
			t.Run(name, func(t *testing.T) {
				for _, outcome := range []ports.BrokerMutationOutcome{ports.BrokerOutcomeOK, ports.BrokerOutcomeFailed, ports.BrokerOutcomeUnknown} {
					_, err := EncodeServer(OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: outcome, Error: stray}, testEnvelopeCeiling, testChunkCeiling)
					require.ErrorIs(t, err, ErrInvalidMessage)
				}
				_, err := EncodeServer(StreamClosed{Epoch: 7, Connection: connection, Stream: 3, Error: stray}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
			})
		}
		// The zero detail is the canonical absent value and still encodes; a
		// declared detail still round-trips exactly.
		_, err := EncodeServer(StreamClosed{Epoch: 7, Connection: connection, Stream: 3}, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		_, err = EncodeServer(OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: ports.BrokerOutcomeOK, Registration: testRegistration()}, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		declared := OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: ports.BrokerOutcomeFailed, Error: detail, HasError: true}
		decoded, err := DecodeServer(mustEncodeServer(t, declared), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, declared, decoded)
	})
	t.Run("invalid error detail refused on every encode path", func(t *testing.T) {
		badAdmission := testErrorDetail()
		badAdmission.AdmissionCode = MaxBrokerAdmissionCode + 1
		badKind := testErrorDetail()
		badKind.FailureKind = MaxBrokerFailureKind + 1
		badCode := testErrorDetail()
		badCode.Code = 0
		for name, detail := range map[string]ErrorDetail{
			"admission over maximum":  badAdmission,
			"failure kind over bound": badKind,
			"invalid error code":      badCode,
		} {
			t.Run(name, func(t *testing.T) {
				_, err := EncodeServer(BrokerErrorMessage{Epoch: 7, Connection: connection, Error: detail}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
				_, err = EncodeServer(OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: ports.BrokerOutcomeFailed, Error: detail, HasError: true}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
				_, err = EncodeServer(StreamClosed{Epoch: 7, Connection: connection, Stream: 3, Error: detail, HasError: true}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
			})
		}
		// The closed taxonomy boundaries are accepted.
		boundary := testErrorDetail()
		boundary.AdmissionCode = MaxBrokerAdmissionCode
		boundary.FailureKind = MaxBrokerFailureKind
		_, err := EncodeServer(BrokerErrorMessage{Epoch: 7, Connection: connection, Error: boundary}, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
	})
	t.Run("membership request authority", func(t *testing.T) {
		base := AddHost{Epoch: 7, Connection: connection, Operation: testOperationID(1), Endpoint: "dev@host:22", Policy: testPolicy()}
		// An absent policy is not authority: it is refused exactly like an
		// invalid one rather than inherited from a default.
		missing := base
		missing.Policy = ports.BrokerPolicy{}
		_, err := EncodeClient(missing, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		// A partial registration is not exact authority either.
		_, err = EncodeClient(RemoveHost{Epoch: 7, Connection: connection, Operation: testOperationID(1)}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		partial := testRegistration()
		partial.Incarnation = [16]byte{}
		_, err = EncodeClient(RemoveHost{Epoch: 7, Connection: connection, Operation: testOperationID(1), Registration: partial}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		partial = testRegistration()
		partial.Generation = 0
		_, err = EncodeClient(RemoveHost{Epoch: 7, Connection: connection, Operation: testOperationID(1), Registration: partial}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		update := UpdateHostPolicy{Epoch: 7, Connection: connection, Operation: testOperationID(1), Registration: testRegistration(), Policy: testPolicy()}
		missingPolicy := update
		missingPolicy.Policy = ports.BrokerPolicy{}
		_, err = EncodeClient(missingPolicy, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		missingRegistration := update
		missingRegistration.Registration = domain.RemoteRegistration{}
		_, err = EncodeClient(missingRegistration, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("membership result presence", func(t *testing.T) {
		// A result may never carry both removal and registration, and a
		// failed or outcome-unknown result may carry neither authority and
		// must carry its typed error.
		both := OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: ports.BrokerOutcomeOK, Removed: true, Registration: testRegistration()}
		_, err := EncodeServer(both, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		for _, outcome := range []ports.BrokerMutationOutcome{ports.BrokerOutcomeFailed, ports.BrokerOutcomeUnknown} {
			_, err = EncodeServer(OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: outcome, Registration: testRegistration(), Error: testErrorDetail(), HasError: true}, testEnvelopeCeiling, testChunkCeiling)
			require.ErrorIs(t, err, ErrInvalidMessage)
			_, err = EncodeServer(OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: outcome, Removed: true, Error: testErrorDetail(), HasError: true}, testEnvelopeCeiling, testChunkCeiling)
			require.ErrorIs(t, err, ErrInvalidMessage)
			// Neither authority without an error is refused too: a failure
			// with no detail would read as an unexplained refusal.
			_, err = EncodeServer(OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: outcome}, testEnvelopeCeiling, testChunkCeiling)
			require.ErrorIs(t, err, ErrInvalidMessage)
		}
		// A success carries no error at all.
		_, err = EncodeServer(OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: ports.BrokerOutcomeOK, Registration: testRegistration(), Error: testErrorDetail(), HasError: true}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		// A partial registration is refused on the result path too.
		partial := testRegistration()
		partial.Incarnation = [16]byte{}
		_, err = EncodeServer(OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: ports.BrokerOutcomeOK, Registration: partial}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("membership result authority by mutation kind", func(t *testing.T) {
		registration := testRegistration()
		add := OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: ports.BrokerOutcomeOK, Registration: registration}
		require.NoError(t, add.ValidateForMutation(MutationKindAddHost))
		require.ErrorIs(t, add.ValidateForMutation(MutationKindRemoveHost), ErrInvalidMessage)
		require.NoError(t, add.ValidateForMutation(MutationKindUpdateHostPolicy))

		remove := OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: ports.BrokerOutcomeOK, Removed: true}
		require.NoError(t, remove.ValidateForMutation(MutationKindRemoveHost))
		require.ErrorIs(t, remove.ValidateForMutation(MutationKindAddHost), ErrInvalidMessage)
		require.ErrorIs(t, remove.ValidateForMutation(MutationKindUpdateHostPolicy), ErrInvalidMessage)

		failed := OperationResult{Epoch: 7, Connection: connection, Operation: testOperationID(1), Outcome: ports.BrokerOutcomeFailed, Error: testErrorDetail(), HasError: true}
		for _, kind := range []RegisterMutationKind{MutationKindAddHost, MutationKindRemoveHost, MutationKindUpdateHostPolicy} {
			require.NoError(t, failed.ValidateForMutation(kind))
		}
		// A kind outside the closed taxonomy is refused, never guessed.
		require.Error(t, add.ValidateForMutation(RegisterMutationKind(9)))
		// The request-side classifier names exactly the three mutations.
		for message, want := range map[ClientMessage]RegisterMutationKind{
			AddHost{}:          MutationKindAddHost,
			RemoveHost{}:       MutationKindRemoveHost,
			UpdateHostPolicy{}: MutationKindUpdateHostPolicy,
			Subscribe{}:        0,
		} {
			got, ok := MutationKindOf(message)
			if want == 0 {
				require.False(t, ok)
				continue
			}
			require.True(t, ok)
			require.Equal(t, want, got)
		}
		require.Equal(t, "add_host", MutationKindAddHost.String())
		require.Equal(t, "remove_host", MutationKindRemoveHost.String())
		require.Equal(t, "update_host_policy", MutationKindUpdateHostPolicy.String())
		require.Equal(t, "unknown", RegisterMutationKind(9).String())
	})
	t.Run("bad endpoint", func(t *testing.T) {
		_, err := EncodeClient(AddHost{Epoch: 7, Connection: connection, Operation: testOperationID(1), Endpoint: "not a host", Policy: testPolicy()}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("taxonomy switches", func(t *testing.T) {
		raw := mustEncodeClient(t, OpenStream{Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Endpoint: "dev@host:22", Registration: testRegistration(), Target: testTarget(), Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded})
		envelope := &wire.BrokerClientEnvelope{}
		require.NoError(t, wire.ScanEnvelope(envelope, raw))
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOpenStream().Purpose = 9
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.Error(t, err)

		rawErr := mustEncodeServer(t, BrokerErrorMessage{Epoch: 7, Connection: connection, Error: testErrorDetail()})
		server := &wire.BrokerServerEnvelope{}
		require.NoError(t, wire.ScanEnvelope(server, rawErr))
		require.NoError(t, proto.Unmarshal(rawErr, server))
		server.GetBrokerErrorMessage().GetError().Code = 99
		mutated, err = proto.Marshal(server)
		require.NoError(t, err)
		_, err = DecodeServer(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.Error(t, err)
	})
	t.Run("bidi and control text refused", func(t *testing.T) {
		evil := testErrorDetail()
		evil.Text = "timeout\u202erorrE"
		_, err := EncodeServer(BrokerErrorMessage{Epoch: 7, Connection: connection, Error: evil}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		evil.Text = "bad\x01text"
		_, err = EncodeServer(BrokerErrorMessage{Epoch: 7, Connection: connection, Error: evil}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		_, err = EncodeServer(Shutdown{Epoch: 7, Connection: connection, Reason: ShutdownIdleExit, Text: "x\x1b"}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("env without equals refused", func(t *testing.T) {
		_, err := EncodeClient(OpenStream{Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Endpoint: "dev@host:22", Registration: testRegistration(), Target: testTarget(), Env: []string{"NOEQUALS"}, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("invalid policy refused", func(t *testing.T) {
		_, err := EncodeClient(OpenStream{Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Endpoint: "dev@host:22", Registration: testRegistration(), Target: testTarget(), Policy: ports.BrokerPolicy{}, StartMode: ports.BrokerDaemonStartIfNeeded}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("snapshot revision zero refused", func(t *testing.T) {
		_, err := EncodeServer(SnapshotPart{Epoch: 7, Connection: connection, Generation: 1, Index: 0, Part: SnapshotEnd{}}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("missing snapshot part refused", func(t *testing.T) {
		_, err := EncodeServer(SnapshotPart{Epoch: 7, Connection: connection, Generation: 1, Revision: 1}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("local open with registration refused", func(t *testing.T) {
		_, err := EncodeClient(OpenStream{Epoch: 7, Connection: connection, Stream: 3, Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Local: true, Registration: testRegistration(), Target: testTarget(), Policy: testPolicy()}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("bad shutdown reason refused", func(t *testing.T) {
		_, err := EncodeServer(Shutdown{Epoch: 7, Connection: connection, Reason: ShutdownReason(9)}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("bad progress phase refused", func(t *testing.T) {
		_, err := EncodeServer(Progress{Epoch: 7, Connection: connection, Stream: 3, Phase: BrokerProgressPhase(9)}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
	t.Run("nil messages refused", func(t *testing.T) {
		var clientNil *Register
		_, err := EncodeClient(clientNil, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		var serverNil *Registered
		_, err = EncodeServer(serverNil, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
}

// TestBrokerTruncatedAndTrailing proves truncated prefixes and trailing
// garbage are rejected on both directions.
func TestBrokerTruncatedAndTrailing(t *testing.T) {
	raw := mustEncodeClient(t, Subscribe{Epoch: 7, Connection: testConnectionID(1), Generation: 3})
	for size := range len(raw) {
		_, err := DecodeClient(raw[:size], testEnvelopeCeiling, testChunkCeiling)
		require.Error(t, err, "client prefix[:%d] accepted", size)
	}
	require.Error(t, func() error {
		_, err := DecodeClient(append(append([]byte(nil), raw...), 0xFF), testEnvelopeCeiling, testChunkCeiling)
		return err
	}())

	serverRaw := mustEncodeServer(t, Registered{Epoch: 7, Connection: testConnectionID(1)})
	for size := range len(serverRaw) {
		_, err := DecodeServer(serverRaw[:size], testEnvelopeCeiling, testChunkCeiling)
		require.Error(t, err, "server prefix[:%d] accepted", size)
	}
	concatenated := append(append([]byte(nil), serverRaw...), serverRaw...)
	_, err := DecodeServer(concatenated, testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)
}

// TestBrokerWrongDirection proves wrong-direction decode is rejected:
// client bytes never decode as server output and vice versa.
func TestBrokerWrongDirection(t *testing.T) {
	clientRaw := mustEncodeClient(t, Register{})
	_, err := DecodeServer(clientRaw, testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)

	serverRaw := mustEncodeServer(t, Registered{Epoch: 7, Connection: testConnectionID(1)})
	_, err = DecodeClient(serverRaw, testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)

	// Go-typed misuse fails with ErrWrongDirection at encode time: unknown
	// concrete types presented to either directional codec fail closed.
	_, err = encodeClientEnvelope(unknownClientMessage{}, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)
	_, err = encodeServerEnvelope(unknownServerMessage{}, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)

	// Nil-payload envelopes decode as wrong-direction.
	_, err = decodeClientEnvelope(&wire.BrokerClientEnvelope{}, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)
	_, err = decodeServerEnvelope(&wire.BrokerServerEnvelope{}, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)
}

// TestBrokerCorruptInputSmoke feeds representative corruptions through both
// decoders as a smoke test: bit flips, truncation, unknown fields, and
// direction swaps must never panic and must never be accepted silently.
func TestBrokerCorruptInputSmoke(t *testing.T) {
	seeds := [][]byte{
		mustEncodeClient(t, Register{}),
		mustEncodeClient(t, AddHost{Epoch: 7, Connection: testConnectionID(2), Operation: testOperationID(0x21), Endpoint: "dev@host:22", Policy: testPolicy()}),
		mustEncodeClient(t, RemoveHost{Epoch: 7, Connection: testConnectionID(2), Operation: testOperationID(0x22), Registration: testRegistration()}),
		mustEncodeClient(t, UpdateHostPolicy{Epoch: 7, Connection: testConnectionID(2), Operation: testOperationID(0x23), Registration: testRegistration(), Policy: testPolicy()}),
		mustEncodeClient(t, OpenStream{Epoch: 7, Connection: testConnectionID(2), Stream: 3, Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded}),
		mustEncodeServer(t, SnapshotPart{Epoch: 7, Connection: testConnectionID(2), Generation: 1, Revision: 1, Index: 0, Part: SnapshotEnd{}}),
		mustEncodeServer(t, OperationResult{Epoch: 7, Connection: testConnectionID(2), Operation: testOperationID(0x24), Outcome: ports.BrokerOutcomeFailed, Error: testErrorDetail(), HasError: true}),
		mustEncodeServer(t, Shutdown{Epoch: 7, Connection: testConnectionID(2), Reason: ShutdownIdleExit}),
	}
	for _, seed := range seeds {
		mutated := append([]byte(nil), seed...)
		if len(mutated) > 2 {
			mutated[len(mutated)-1] ^= 0xFF
			mutated[len(mutated)-2] ^= 0x01
		}
		_, _ = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		_, _ = DecodeServer(mutated, testEnvelopeCeiling, testChunkCeiling)
		unknown := protowire.AppendTag(append([]byte(nil), seed...), 199, protowire.VarintType)
		unknown = protowire.AppendVarint(unknown, 1)
		_, _ = DecodeClient(unknown, testEnvelopeCeiling, testChunkCeiling)
		_, _ = DecodeServer(unknown, testEnvelopeCeiling, testChunkCeiling)
	}
	require.NotEmpty(t, seeds)
}

// fuzzClientSeeds and fuzzServerSeeds are the shared strict-scan seed corpus:
// valid brokered messages plus vectors that exercise ScanEnvelope rejection
// (unknown tag, wrong wire type, truncation, empty, and trailing bytes).
func fuzzClientSeeds(t testing.TB) [][]byte {
	t.Helper()
	seeds := [][]byte{}
	for _, message := range []ClientMessage{
		Register{},
		Subscribe{Epoch: 7, Connection: testConnectionID(0x11), Generation: 1},
		AddHost{Epoch: 7, Connection: testConnectionID(0x11), Operation: testOperationID(0x11), Endpoint: "dev@host:22", Policy: testPolicy()},
		RemoveHost{Epoch: 7, Connection: testConnectionID(0x11), Operation: testOperationID(0x12), Registration: testRegistration()},
		UpdateHostPolicy{Epoch: 7, Connection: testConnectionID(0x11), Operation: testOperationID(0x13), Registration: testRegistration(), Policy: testPolicy()},
		OpenStream{Epoch: 7, Connection: testConnectionID(0x11), Stream: 3, Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Endpoint: "dev@host:22", Registration: testRegistration(), Target: testTarget(), Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded},
		ClientStreamData{Epoch: 7, Connection: testConnectionID(0x11), Stream: 3, Data: []byte("frame")},
		CloseStream{Epoch: 7, Connection: testConnectionID(0x11), Stream: 3},
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
		Registered{Epoch: 7, Connection: testConnectionID(0x11)},
		SnapshotPart{Epoch: 7, Connection: testConnectionID(0x11), Generation: 2, Revision: 5, Index: 0, Part: SnapshotEnd{}},
		SnapshotPart{Epoch: 7, Connection: testConnectionID(0x11), Generation: 2, Revision: 5, Index: 2, Part: SnapshotSessionPart{HostIndex: 0, SessionIndex: 0, Session: testCatalogSession()}},
		OperationResult{Epoch: 7, Connection: testConnectionID(0x11), Operation: testOperationID(0x14), Outcome: ports.BrokerOutcomeOK, Registration: testRegistration()},
		BrokerErrorMessage{Epoch: 7, Connection: testConnectionID(0x11), Error: testErrorDetail()},
		Shutdown{Epoch: 7, Connection: testConnectionID(0x11), Reason: ShutdownTerminating},
	} {
		raw, err := EncodeServer(message, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		seeds = append(seeds, raw)
	}
	return appendStrictScanSeeds(seeds)
}

// appendStrictScanSeeds adds raw vectors that must be refused by the strict
// scanner rather than reaching generated unmarshal.
func appendStrictScanSeeds(seeds [][]byte) [][]byte {
	unknown := protowire.AppendTag(nil, 199, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	wrongType := protowire.AppendTag(nil, 101, protowire.VarintType)
	wrongType = protowire.AppendVarint(wrongType, 1)
	return append(seeds, unknown, wrongType, []byte{0xFF}, []byte{0x80}, nil)
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

func TestBuildRegistrationCodec(t *testing.T) {
	for _, tc := range []struct {
		name     string
		build    string
		retiring bool
	}{
		{"empty", "", false}, {"matching", "release+abc", false}, {"retiring", "release+def", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := Register{Build: tc.build}
			raw := mustEncodeClient(t, request)
			decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, request, decoded)
			want := []byte{0xaa, 0x06, 0x00}
			if tc.build != "" {
				want = append([]byte{0xaa, 0x06, byte(len(tc.build) + 2), 0x0a, byte(len(tc.build))}, []byte(tc.build)...)
			}
			require.Equal(t, want, raw)
			response := Registered{Epoch: 7, Connection: testConnectionID(1), Build: tc.build, Retiring: tc.retiring}
			raw = mustEncodeServer(t, response)
			decodedServer, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, response, decodedServer)
			envelope := &wire.BrokerServerEnvelope{}
			require.NoError(t, proto.Unmarshal(raw, envelope))
			require.Equal(t, tc.build, envelope.GetRegistered().GetBuild())
			require.Equal(t, tc.retiring, envelope.GetRegistered().GetRetiring())
		})
	}
	for _, tc := range []struct {
		name   string
		server bool
	}{{"client", false}, {"server", true}} {
		t.Run("oversize-"+tc.name, func(t *testing.T) {
			build := strings.Repeat("x", 129)
			if tc.server {
				_, err := EncodeServer(Registered{Epoch: 7, Connection: testConnectionID(1), Build: build}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
				raw, err := proto.Marshal(&wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_Registered{Registered: &wire.Registered{Scope: scopeToWire(7, testConnectionID(1)), Build: build}}})
				require.NoError(t, err)
				_, err = DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
			} else {
				_, err := EncodeClient(Register{Build: build}, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
				raw, err := proto.Marshal(&wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_Register{Register: &wire.Register{Build: build}}})
				require.NoError(t, err)
				_, err = DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
				require.ErrorIs(t, err, ErrInvalidMessage)
			}
		})
	}
}
