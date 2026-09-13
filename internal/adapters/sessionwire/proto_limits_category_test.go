package sessionwire

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// overControlCap is comfortably above wire.ControlEnvelopeLimit but well
// below wire.AbsoluteEnvelopeLimit, so a category-exempt envelope still
// rides the negotiated ceiling while a control envelope is capped.
const overControlCap = wire.ControlEnvelopeLimit + (1 << 20)

// testOutputRaw builds an Output that never compresses (Full is false), so
// its serialized envelope stays above the control cap.
func testOutputRaw(data []byte) protocol.Output {
	return protocol.Output{Epoch: 1, New: 0, Size: domain.Size{Cols: 80, Rows: 24}, Data: data}
}

func mustMarshalClientEnvelope(t *testing.T, envelope *wire.ClientEnvelope) []byte {
	t.Helper()
	raw, err := proto.Marshal(envelope)
	require.NoError(t, err)
	return raw
}

func mustMarshalServerEnvelope(t *testing.T, envelope *wire.ServerEnvelope) []byte {
	t.Helper()
	raw, err := proto.Marshal(envelope)
	require.NoError(t, err)
	return raw
}

// TestEnvelopeCategoryClassification proves category detection derives from
// the serialized leading tag only: ImagePush/Output are exempt, every other
// variant and every unknown, empty, or malformed payload stays control.
func TestEnvelopeCategoryClassification(t *testing.T) {
	imagePayload := mustMarshalClientEnvelope(t, &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_ImagePush{ImagePush: &wire.ImagePush{Data: []byte("img")}}})
	pingPayload := mustMarshalClientEnvelope(t, &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Ping{Ping: &wire.Ping{}}})
	outputPayload := mustMarshalServerEnvelope(t, &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_Output{Output: &wire.Output{Data: []byte("out")}}})
	errorPayload := mustMarshalServerEnvelope(t, &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_Error{Error: &wire.ErrorMsg{}}})
	unknownField := protowire.AppendBytes(protowire.AppendTag(nil, 99, protowire.BytesType), []byte("x"))

	clientCases := []struct {
		name    string
		payload []byte
		want    envelopeCategory
	}{
		{"image push is exempt", imagePayload, categoryBulk},
		{"control variant", pingPayload, categoryControl},
		{"server envelope is control", outputPayload, categoryControl},
		{"unknown field", unknownField, categoryControl},
		{"empty payload", nil, categoryControl},
		{"truncated tag", []byte{0xFF}, categoryControl},
		{"non-length-delimited tag", protowire.AppendTag(nil, 1, protowire.VarintType), categoryControl},
	}
	for _, tt := range clientCases {
		t.Run("client/"+tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, clientEnvelopeCategory(tt.payload))
		})
	}

	serverCases := []struct {
		name    string
		payload []byte
		want    envelopeCategory
	}{
		{"output is exempt", outputPayload, categoryOutput},
		{"control variant", errorPayload, categoryControl},
		{"client envelope is control", imagePayload, categoryControl},
		{"unknown field", unknownField, categoryControl},
		{"empty payload", nil, categoryControl},
		{"truncated tag", []byte{0xFF}, categoryControl},
	}
	for _, tt := range serverCases {
		t.Run("server/"+tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, serverEnvelopeCategory(tt.payload))
		})
	}
}

// TestClientSendCategoryEnvelopeCeilings proves client sends cap control
// envelopes at wire.ControlEnvelopeLimit while ImagePush rides the negotiated
// ceiling; a lower negotiated ceiling still wins for both categories.
func TestClientSendCategoryEnvelopeCeilings(t *testing.T) {
	inputOver := protocol.Input{InputSeq: 1, Data: bytes.Repeat([]byte{0xAB}, overControlCap)}
	imageOver := protocol.ImagePush{InputSeq: 1, Mime: "image/png", Data: bytes.Repeat([]byte{0xAB}, overControlCap)}
	smallControl := protocol.Input{InputSeq: 1, Data: bytes.Repeat([]byte{0x5A}, 4096)}
	smallLen := len(mustEncodeClient(t, smallControl))

	tests := []struct {
		name     string
		message  protocol.ClientMessage
		ceilings protoCeilings
		wantErr  error
	}{
		{
			name:     "input rides absolute negotiation",
			message:  inputOver,
			ceilings: defaultProtoCeilings(),
		},
		{
			name:     "image rides absolute negotiation",
			message:  imageOver,
			ceilings: defaultProtoCeilings(),
		},
		{
			name:     "image respects lower negotiated ceiling",
			message:  imageOver,
			ceilings: protoCeilings{maxReceiveEnvelopeBytes: wire.ControlEnvelopeLimit, outputDataLimit: uint64(protocol.MaxOutputDataLen)},
			wantErr:  wire.ErrScanLength,
		},
		{
			name:     "control fits negotiated lower ceiling",
			message:  smallControl,
			ceilings: protoCeilings{maxReceiveEnvelopeBytes: uint64(smallLen), outputDataLimit: uint64(protocol.MaxOutputDataLen)},
		},
		{
			name:     "control above negotiated lower ceiling",
			message:  smallControl,
			ceilings: protoCeilings{maxReceiveEnvelopeBytes: uint64(smallLen - 1), outputDataLimit: uint64(protocol.MaxOutputDataLen)},
			wantErr:  wire.ErrScanLength,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{}
			conn := &clientConnection{raw: raw, ceilings: tt.ceilings}
			conn.preambleOnce.Do(func() {})
			err := conn.SendClient(tt.message)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Zero(t, raw.sentLen())
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, raw.sentLen())
		})
	}
}

// TestServerSendCategoryEnvelopeCeilings proves server sends cap control
// envelopes at wire.ControlEnvelopeLimit while Output rides the negotiated
// ceiling; a lower negotiated ceiling still wins for both categories.
func TestServerSendCategoryEnvelopeCeilings(t *testing.T) {
	controlOver := protocol.Sessions{Sessions: []protocol.SessionInfo{{Name: strings.Repeat("x", overControlCap)}}}
	outputOver := testOutputRaw(bytes.Repeat([]byte{0xAB}, overControlCap))
	smallControl := protocol.Sessions{Sessions: []protocol.SessionInfo{{Name: strings.Repeat("x", 4096)}}}
	smallLen := len(mustEncodeServer(t, smallControl))

	tests := []struct {
		name     string
		message  protocol.ServerMessage
		ceilings protoCeilings
		wantErr  error
	}{
		{
			name:     "control capped even at absolute negotiation",
			message:  controlOver,
			ceilings: defaultProtoCeilings(),
			wantErr:  wire.ErrScanLength,
		},
		{
			name:     "output rides absolute negotiation",
			message:  outputOver,
			ceilings: defaultProtoCeilings(),
		},
		{
			name:     "output respects lower negotiated ceiling",
			message:  outputOver,
			ceilings: protoCeilings{maxReceiveEnvelopeBytes: wire.ControlEnvelopeLimit, outputDataLimit: uint64(protocol.MaxOutputDataLen)},
			wantErr:  wire.ErrScanLength,
		},
		{
			name:     "control fits negotiated lower ceiling",
			message:  smallControl,
			ceilings: protoCeilings{maxReceiveEnvelopeBytes: uint64(smallLen), outputDataLimit: uint64(protocol.MaxOutputDataLen)},
		},
		{
			name:     "control above negotiated lower ceiling",
			message:  smallControl,
			ceilings: protoCeilings{maxReceiveEnvelopeBytes: uint64(smallLen - 1), outputDataLimit: uint64(protocol.MaxOutputDataLen)},
			wantErr:  wire.ErrScanLength,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{}
			conn := &serverConnection{raw: raw, ceilings: tt.ceilings}
			err := conn.SendServer(tt.message)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Zero(t, raw.sentLen())
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, raw.sentLen())
		})
	}
}

// TestServerSendModesShareControlCategoryCap proves async and owned
// synchronous server sends enforce the same control cap as the normal path.
func TestServerSendModesShareControlCategoryCap(t *testing.T) {
	controlOver := protocol.Sessions{Sessions: []protocol.SessionInfo{{Name: strings.Repeat("x", overControlCap)}}}
	raw := &asyncScriptedTransport{scriptedTransport: &scriptedTransport{}}
	conn := &serverConnection{raw: raw, ceilings: defaultProtoCeilings()}

	require.ErrorIs(t, conn.SendServerAsync(controlOver), wire.ErrScanLength)
	require.ErrorIs(t, conn.SendServerSynchronous(controlOver), wire.ErrScanLength)
	require.Empty(t, raw.async)
	require.Empty(t, raw.synchronous)
}

// TestReceiveCategoryEnvelopeCeilings proves both typed receive paths apply
// the same category ceilings as sends, including malformed and unknown
// payloads, which must fall back to the strictest control ceiling.
func TestReceiveCategoryEnvelopeCeilings(t *testing.T) {
	serverInputOver := mustEncodeClient(t, protocol.Input{InputSeq: 1, Data: bytes.Repeat([]byte{0xAB}, overControlCap)})
	clientImageOver := mustEncodeClient(t, protocol.ImagePush{InputSeq: 1, Mime: "image/png", Data: bytes.Repeat([]byte{0xAB}, overControlCap)})
	clientControlOver := mustEncodeServer(t, protocol.Sessions{Sessions: []protocol.SessionInfo{{Name: strings.Repeat("x", overControlCap)}}})
	clientOutputOver := mustEncodeServer(t, testOutputRaw(bytes.Repeat([]byte{0xAB}, overControlCap)))

	oversizedMalformed := bytes.Repeat([]byte{0xFF}, overControlCap)
	oversizedUnknown := protowire.AppendBytes(protowire.AppendTag(nil, 99, protowire.BytesType), bytes.Repeat([]byte{0xAB}, overControlCap))
	smallUnknown := protowire.AppendBytes(protowire.AppendTag(nil, 99, protowire.BytesType), []byte("x"))

	lowerControl := protoCeilings{maxReceiveEnvelopeBytes: wire.ControlEnvelopeLimit, outputDataLimit: uint64(protocol.MaxOutputDataLen)}

	tests := []struct {
		name     string
		receiver string
		payload  []byte
		ceilings protoCeilings
		wantErr  error
		wantCat  protocol.DecodeCategory
	}{
		{
			name:     "server receive input rides absolute negotiation",
			receiver: "server",
			payload:  serverInputOver,
			ceilings: defaultProtoCeilings(),
		},
		{
			name:     "server receive image rides absolute negotiation",
			receiver: "server",
			payload:  clientImageOver,
			ceilings: defaultProtoCeilings(),
		},
		{
			name:     "server receive image respects lower negotiated ceiling",
			receiver: "server",
			payload:  clientImageOver,
			ceilings: lowerControl,
			wantErr:  wire.ErrScanLength,
			wantCat:  protocol.DecodeMalformed,
		},
		{
			name:     "server receive oversized malformed defaults to control",
			receiver: "server",
			payload:  oversizedMalformed,
			ceilings: defaultProtoCeilings(),
			wantErr:  wire.ErrScanLength,
			wantCat:  protocol.DecodeMalformed,
		},
		{
			name:     "server receive oversized unknown field defaults to control",
			receiver: "server",
			payload:  oversizedUnknown,
			ceilings: defaultProtoCeilings(),
			wantErr:  wire.ErrScanLength,
			wantCat:  protocol.DecodeMalformed,
		},
		{
			name:     "server receive small unknown field stays unknown type",
			receiver: "server",
			payload:  smallUnknown,
			ceilings: defaultProtoCeilings(),
			wantErr:  wire.ErrScanUnknown,
			wantCat:  protocol.DecodeUnknownType,
		},
		{
			name:     "client receive control capped at absolute negotiation",
			receiver: "client",
			payload:  clientControlOver,
			ceilings: defaultProtoCeilings(),
			wantErr:  wire.ErrScanLength,
			wantCat:  protocol.DecodeMalformed,
		},
		{
			name:     "client receive output rides absolute negotiation",
			receiver: "client",
			payload:  clientOutputOver,
			ceilings: defaultProtoCeilings(),
		},
		{
			name:     "client receive output respects lower negotiated ceiling",
			receiver: "client",
			payload:  clientOutputOver,
			ceilings: lowerControl,
			wantErr:  wire.ErrScanLength,
			wantCat:  protocol.DecodeMalformed,
		},
		{
			name:     "client receive oversized malformed defaults to control",
			receiver: "client",
			payload:  oversizedMalformed,
			ceilings: defaultProtoCeilings(),
			wantErr:  wire.ErrScanLength,
			wantCat:  protocol.DecodeMalformed,
		},
		{
			name:     "client receive small unknown field stays unknown type",
			receiver: "client",
			payload:  smallUnknown,
			ceilings: defaultProtoCeilings(),
			wantErr:  wire.ErrScanUnknown,
			wantCat:  protocol.DecodeUnknownType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: []wire.Envelope{{Payload: tt.payload}}}
			var err error
			if tt.receiver == "server" {
				conn := &serverConnection{raw: raw, ceilings: tt.ceilings}
				conn.preambleOnce.Do(func() {})
				_, err = conn.ReceiveClient()
			} else {
				conn := &clientConnection{raw: raw, ceilings: tt.ceilings}
				conn.preambleOnce.Do(func() {})
				_, err = conn.ReceiveServer()
			}
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			var failure *protocol.DecodeFailure
			require.ErrorAs(t, err, &failure)
			require.ErrorIs(t, failure.Err, tt.wantErr)
			require.Equal(t, tt.wantCat, failure.Category)
		})
	}
}
