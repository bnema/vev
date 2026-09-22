package brokerwire

// Preview contract tests (broker preview slice 1).
//
// StartPreview carries tags 112 with scope, generation, the observed
// daemon route (never a stream identity), and the terminal preview request; CancelPreview
// carries tag 113 with scope and generation. PreviewPublication carries
// tag 210 with scope, generation, and exactly one of preview/error via
// its result oneof. These tests pin the stateless codec's half of that
// contract: frozen tags, byte round trips, the strict oneof presence
// rule, exact authority preservation, range-strict narrowing, and the
// ports preview validation parity (routes that do not own the target
// refused).

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func testPreviewTarget() domain.RemoteSessionTarget {
	return domain.RemoteSessionTarget{
		Endpoint: "dev@host:22", DisplayOrigin: "dev@host",
		LifecycleID: domain.SessionLifecycleID{5, 6, 7},
		SessionName: "work", LiveTabID: "tab-1",
	}
}

func testPreviewRequest() protocol.RemotePreviewRequest {
	return protocol.RemotePreviewRequest{
		Version: protocol.RemotePreviewSchemaVersion,
		Target:  testPreviewTarget(),
		Width:   2, Height: 1,
	}
}

func testPreviewRoute() ports.BrokerPreviewRoute {
	return ports.BrokerPreviewRoute{Endpoint: "dev@host:22", Registration: testRegistration(), Policy: testPolicy()}
}

func testStartPreview(connection ports.BrokerConnectionID) StartPreview {
	return StartPreview{
		Epoch: 7, Connection: connection, Generation: 3,
		Route: testPreviewRoute(), Preview: testPreviewRequest(),
	}
}

func testLocalStartPreview(connection ports.BrokerConnectionID) StartPreview {
	m := testStartPreview(connection)
	m.Route = ports.BrokerPreviewRoute{Local: true, Policy: testPolicy()}
	m.Preview.Target.Endpoint = ports.BrokerPreviewLocalEndpoint
	m.Preview.Target.DisplayOrigin = ports.BrokerPreviewLocalEndpoint
	return m
}

func testRemotePreview() protocol.RemotePreview {
	return protocol.RemotePreview{
		Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK,
		LifecycleID: testPreviewTarget().LifecycleID, TabID: testPreviewTarget().LiveTabID,
		Revision: 11, Width: 2, Height: 1,
		Cells: []renderer.Cell{{Rune: 'a'}, {Rune: 'b'}},
	}
}

func testPreviewPublication(connection ports.BrokerConnectionID) PreviewPublication {
	return PreviewPublication{
		Epoch: 7, Connection: connection, Generation: 3,
		Preview: testRemotePreview(),
	}
}

// TestPreviewVariantTags pins the frozen envelope tags: start_preview 112,
// cancel_preview 113, preview_publication 210.
func TestPreviewVariantTags(t *testing.T) {
	connection := testConnectionID(0x61)
	raw := mustEncodeClient(t, testStartPreview(connection))
	number, wireType, consumed := protowire.ConsumeTag(raw)
	require.Greater(t, consumed, 0)
	require.Equal(t, protowire.Number(112), number)
	require.Equal(t, protowire.BytesType, wireType)

	raw = mustEncodeClient(t, CancelPreview{Epoch: 7, Connection: connection, Generation: 3})
	number, _, _ = protowire.ConsumeTag(raw)
	require.Equal(t, protowire.Number(113), number)

	raw = mustEncodeServer(t, testPreviewPublication(connection))
	number, wireType, consumed = protowire.ConsumeTag(raw)
	require.Greater(t, consumed, 0)
	require.Equal(t, protowire.Number(210), number)
	require.Equal(t, protowire.BytesType, wireType)
}

// TestPreviewByteRoundTrip proves all three preview messages survive an
// encode/decode/re-encode cycle byte for byte, in both success and typed
// error publication shapes.
func TestPreviewByteRoundTrip(t *testing.T) {
	connection := testConnectionID(0x62)
	clients := []struct {
		name    string
		message ClientMessage
	}{
		{"start_preview", testStartPreview(connection)},
		{"start_preview_local", testLocalStartPreview(connection)},
		{"cancel_preview", CancelPreview{Epoch: 7, Connection: connection, Generation: 5}},
	}
	for _, tc := range clients {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.message)
			decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.message, decoded)
			again, err := EncodeClient(decoded, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, raw, again, "re-encode must be byte-for-byte identical")
		})
	}

	connection = testConnectionID(0x63)
	servers := []struct {
		name    string
		message PreviewPublication
	}{
		{"success", testPreviewPublication(connection)},
		{"failed", PreviewPublication{
			Epoch: 7, Connection: connection, Generation: 4,
			Error: testErrorDetail(), HasError: true,
		}},
	}
	for _, tc := range servers {
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

// TestPreviewRequestValidationParity proves the codec refuses exactly what
// ports.BrokerPreviewRequest.Validate refuses: a route that does not own the
// target, a remote route without matching registration, a zero generation,
// and an invalid preview request.
func TestPreviewRequestValidationParity(t *testing.T) {
	connection := testConnectionID(0x64)
	base := testStartPreview(connection)
	local := testLocalStartPreview(connection)
	cases := map[string]func() StartPreview{
		"local route for remote target": func() StartPreview {
			m := base
			m.Route = ports.BrokerPreviewRoute{Local: true, Policy: testPolicy()}
			return m
		},
		"remote route for local target": func() StartPreview {
			m := local
			m.Route = testPreviewRoute()
			return m
		},
		"local route carries registration": func() StartPreview {
			m := local
			m.Route.Registration = testRegistration()
			return m
		},
		"remote route without registration": func() StartPreview {
			m := base
			m.Route.Registration = domain.RemoteRegistration{}
			return m
		},
		"remote route endpoint mismatch": func() StartPreview {
			m := base
			m.Route.Endpoint = "other@host:22"
			return m
		},
		"target on another host": func() StartPreview {
			m := base
			m.Preview.Target.Endpoint = "other@host:22"
			return m
		},
		"zero generation": func() StartPreview {
			m := base
			m.Generation = 0
			return m
		},
		"invalid preview dimensions": func() StartPreview {
			m := base
			m.Preview.Width = 0
			return m
		},
		"stopped preview target": func() StartPreview {
			m := base
			m.Preview.Target.Stopped = true
			m.Preview.Target.LiveTabID = ""
			return m
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			// The candidate must already fail the ports gate, so the codec
			// refusal is parity, not invention.
			require.Error(t, previewRequest(mutate()).Validate())
			_, err := EncodeClient(mutate(), testEnvelopeCeiling, testChunkCeiling)
			require.ErrorIs(t, err, ErrInvalidMessage)
		})
	}

	decodeCases := map[string]func(*wire.PreviewRoute){
		"missing route":               func(r *wire.PreviewRoute) { *r = wire.PreviewRoute{} },
		"missing policy":              func(r *wire.PreviewRoute) { r.Policy = nil },
		"local with registration":     func(r *wire.PreviewRoute) { r.Local = true },
		"remote without registration": func(r *wire.PreviewRoute) { r.Registration = nil },
	}
	for name, mutate := range decodeCases {
		t.Run("decode "+name, func(t *testing.T) {
			raw := mustEncodeClient(t, base)
			envelope := &wire.BrokerClientEnvelope{}
			require.NoError(t, wire.ScanEnvelope(envelope, raw))
			require.NoError(t, proto.Unmarshal(raw, envelope))
			mutate(envelope.GetStartPreview().GetRoute())
			mutated, err := proto.Marshal(envelope)
			require.NoError(t, err)
			_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
			require.Error(t, err)
		})
	}
}

// TestPreviewPublicationResultPresence proves exactly one of preview/error
// travels: a publication missing both results, setting a nil result
// member, or carrying a viewport alongside its error is refused, and a
// nonzero ErrorDetail without HasError is refused rather than dropped.
func TestPreviewPublicationResultPresence(t *testing.T) {
	connection := testConnectionID(0x65)

	t.Run("absent result refused", func(t *testing.T) {
		message := &wire.PreviewPublication{
			Scope:      &wire.BrokerScope{BrokerEpoch: 7, ConnectionId: connection[:]},
			Generation: 3,
		}
		raw, err := proto.Marshal(&wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_PreviewPublication{PreviewPublication: message}})
		require.NoError(t, err)
		_, err = DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})

	t.Run("nil result members refused", func(t *testing.T) {
		// A nil oneof member fails structurally before semantic validation
		// selects its error: either refusal proves the payload never decodes.
		for _, message := range []*wire.PreviewPublication{
			{
				Scope:      &wire.BrokerScope{BrokerEpoch: 7, ConnectionId: connection[:]},
				Generation: 3, Result: &wire.PreviewPublication_Preview{},
			},
			{
				Scope:      &wire.BrokerScope{BrokerEpoch: 7, ConnectionId: connection[:]},
				Generation: 3, Result: &wire.PreviewPublication_Error{},
			},
		} {
			raw, err := proto.Marshal(&wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_PreviewPublication{PreviewPublication: message}})
			require.NoError(t, err)
			_, err = DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
			require.Error(t, err)
		}
	})

	t.Run("error with viewport refused", func(t *testing.T) {
		published := PreviewPublication{
			Epoch: 7, Connection: connection, Generation: 3,
			Preview: testRemotePreview(), Error: testErrorDetail(), HasError: true,
		}
		_, err := EncodeServer(published, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})

	t.Run("dropped detail refused", func(t *testing.T) {
		published := testPreviewPublication(connection)
		published.Error = testErrorDetail()
		_, err := EncodeServer(published, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
}

// TestPreviewNarrowingRefused proves every narrowing cast on the preview
// path is range-strict: values above uint16 aliasing a valid enum or
// dimension are refused before any cast.
func TestPreviewNarrowingRefused(t *testing.T) {
	connection := testConnectionID(0x66)
	base := testStartPreview(connection)
	raw := mustEncodeClient(t, base)
	envelope := &wire.BrokerClientEnvelope{}
	require.NoError(t, wire.ScanEnvelope(envelope, raw))
	require.NoError(t, proto.Unmarshal(raw, envelope))

	mutate := func(apply func(*wire.BrokerClientEnvelope)) []byte {
		clone := proto.Clone(envelope).(*wire.BrokerClientEnvelope)
		apply(clone)
		mutated, err := proto.Marshal(clone)
		require.NoError(t, err)
		return mutated
	}
	for _, tc := range []struct {
		name   string
		mutate func(*wire.BrokerClientEnvelope)
	}{
		// 257 aliases RemotePreviewUnavailable (1) under truncation.
		{"preview version", func(e *wire.BrokerClientEnvelope) { e.GetStartPreview().GetPreview().Version = 1<<16 + 1 }},
		{"preview width", func(e *wire.BrokerClientEnvelope) { e.GetStartPreview().GetPreview().Width = 1<<16 + 1 }},
		{"preview height", func(e *wire.BrokerClientEnvelope) { e.GetStartPreview().GetPreview().Height = 1<<16 + 80 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeClient(mutate(tc.mutate), testEnvelopeCeiling, testChunkCeiling)
			require.ErrorIs(t, err, errConvertRange)
		})
	}

	published := mustEncodeServer(t, testPreviewPublication(connection))
	serverEnvelope := &wire.BrokerServerEnvelope{}
	require.NoError(t, wire.ScanEnvelope(serverEnvelope, published))
	require.NoError(t, proto.Unmarshal(published, serverEnvelope))
	serverEnvelope.GetPreviewPublication().GetPreview().Status = 1<<16 + 1
	mutated, err := proto.Marshal(serverEnvelope)
	require.NoError(t, err)
	_, err = DecodeServer(mutated, testEnvelopeCeiling, testChunkCeiling)
	require.ErrorIs(t, err, errConvertRange)

	t.Run("truncated and trailing", func(t *testing.T) {
		for _, message := range []struct {
			name   string
			encode func() []byte
			decode func([]byte) (any, error)
		}{
			{"start", func() []byte { return mustEncodeClient(t, base) }, func(raw []byte) (any, error) {
				return DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
			}},
			{"publication", func() []byte { return mustEncodeServer(t, testPreviewPublication(connection)) }, func(raw []byte) (any, error) {
				return DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
			}},
		} {
			raw := message.encode()
			for size := range len(raw) {
				_, err := message.decode(raw[:size])
				require.Error(t, err, "%s prefix[:%d] accepted", message.name, size)
			}
			_, err := message.decode(append(append([]byte(nil), raw...), 0xFF))
			require.Error(t, err)
		}
	})
}

// TestPreviewWrongDirection proves preview payloads fail closed across
// directions: client payloads never decode as server messages and the
// reverse.
func TestPreviewWrongDirection(t *testing.T) {
	connection := testConnectionID(0x67)
	_, err := EncodeServer(unknownServerMessage{}, testEnvelopeCeiling, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)
	_, err = EncodeClient(unknownClientMessage{}, testEnvelopeCeiling, testChunkCeiling)
	require.ErrorIs(t, err, ErrWrongDirection)

	raw := mustEncodeClient(t, testStartPreview(connection))
	_, err = DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)
	raw = mustEncodeServer(t, testPreviewPublication(connection))
	_, err = DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)
}
