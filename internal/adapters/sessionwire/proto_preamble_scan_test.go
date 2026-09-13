package sessionwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func preambleFrame(payload []byte) []byte {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	return append(header[:], payload...)
}

func mustMarshalPreambleRequest(t *testing.T) []byte {
	t.Helper()
	raw, err := proto.Marshal(preambleRequestToWire(defaultProtoCeilings()))
	require.NoError(t, err)
	return raw
}

func mustMarshalPreambleResponse(t *testing.T) []byte {
	t.Helper()
	raw, err := proto.Marshal(preambleResponseToWire(true, defaultProtoCeilings(), 0))
	require.NoError(t, err)
	return raw
}

func withUnknownField(t *testing.T, payload []byte) []byte {
	t.Helper()
	unknown := protowire.AppendTag(append([]byte(nil), payload...), 99, protowire.VarintType)
	return protowire.AppendVarint(unknown, 1)
}

// TestReadPreambleFrameRejectsUnscannedPayloads proves the strict envelope
// scan runs before generated unmarshal on the preamble read path: unknown,
// repeated, and concatenated payloads are refused.
func TestReadPreambleFrameRejectsUnscannedPayloads(t *testing.T) {
	request := mustMarshalPreambleRequest(t)
	response := mustMarshalPreambleResponse(t)
	magic := protowire.Number((&wire.PreambleRequest{}).ProtoReflect().Descriptor().Fields().ByName("magic").Number())

	duplicateMagic := protowire.AppendTag(append([]byte(nil), request...), magic, protowire.VarintType)
	duplicateMagic = protowire.AppendVarint(duplicateMagic, 7)

	tests := []struct {
		name    string
		message proto.Message
		payload []byte
		wantErr error
	}{
		{name: "valid request", message: &wire.PreambleRequest{}, payload: request},
		{name: "valid response", message: &wire.PreambleResponse{}, payload: response},
		{name: "unknown request field", message: &wire.PreambleRequest{}, payload: withUnknownField(t, request), wantErr: wire.ErrScanUnknown},
		{name: "unknown response field", message: &wire.PreambleResponse{}, payload: withUnknownField(t, response), wantErr: wire.ErrScanUnknown},
		{name: "repeated singular request field", message: &wire.PreambleRequest{}, payload: duplicateMagic, wantErr: wire.ErrScanDuplicate},
		{name: "concatenated requests", message: &wire.PreambleRequest{}, payload: append(append([]byte(nil), request...), request...), wantErr: wire.ErrScanDuplicate},
		{name: "concatenated responses", message: &wire.PreambleResponse{}, payload: append(append([]byte(nil), response...), response...), wantErr: wire.ErrScanDuplicate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := readPreambleFrame(bytes.NewReader(preambleFrame(tt.payload)), tt.message)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestProtoPreambleRejectsScannedMalformation proves the live preamble
// exchange refuses unknown and concatenated payloads on both sides before
// unmarshal, with the server sending a typed refusal.
func TestProtoPreambleRejectsScannedMalformation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("server rejects unknown request field", func(t *testing.T) {
		raw := &scriptedTransport{recv: []wire.Envelope{{Payload: withUnknownField(t, mustMarshalPreambleRequest(t))}}}
		_, err := runProtoServerPreamble(ctx, raw, defaultProtoCeilings())
		require.ErrorIs(t, err, wire.ErrScanUnknown)
		require.Equal(t, 1, raw.sentLen())
	})
	t.Run("server rejects concatenated request", func(t *testing.T) {
		request := mustMarshalPreambleRequest(t)
		raw := &scriptedTransport{recv: []wire.Envelope{{Payload: append(append([]byte(nil), request...), request...)}}}
		_, err := runProtoServerPreamble(ctx, raw, defaultProtoCeilings())
		require.ErrorIs(t, err, wire.ErrScanDuplicate)
		require.Equal(t, 1, raw.sentLen())
	})
	t.Run("client rejects unknown response field", func(t *testing.T) {
		raw := &scriptedTransport{recv: []wire.Envelope{{Payload: withUnknownField(t, mustMarshalPreambleResponse(t))}}}
		_, err := runProtoClientPreamble(ctx, raw, defaultProtoCeilings())
		require.ErrorIs(t, err, wire.ErrScanUnknown)
	})
}
