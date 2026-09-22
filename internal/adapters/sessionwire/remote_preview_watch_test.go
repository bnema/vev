package sessionwire

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func testRemotePreviewWatch() protocol.RemotePreviewWatch {
	return protocol.RemotePreviewWatch{
		Request: protocol.RemotePreviewRequest{
			Version: protocol.RemotePreviewSchemaVersion,
			Target: domain.RemoteSessionTarget{
				Endpoint: "local", DisplayOrigin: "local", LifecycleID: domain.SessionLifecycleID{7},
				SessionName: "work", LiveTabID: "tab-2",
			},
			Width: 80, Height: 24,
		},
		MinInterval: 125 * time.Millisecond,
	}
}

// TestRemotePreviewWatchByteRoundTrip pins the watch as a client first
// message: it re-encodes byte-for-byte, rides field 32, and carries its
// interval as whole milliseconds.
func TestRemotePreviewWatchByteRoundTrip(t *testing.T) {
	message := testRemotePreviewWatch()
	raw, err := EncodeClientMessage(message)
	require.NoError(t, err)
	require.NoError(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, raw))

	var envelope wire.ClientEnvelope
	require.NoError(t, proto.Unmarshal(raw, &envelope))
	payload, ok := envelope.Payload.(*wire.ClientEnvelope_RemotePreviewWatch)
	require.True(t, ok, "got %T", envelope.Payload)
	require.Equal(t, uint32(125), payload.RemotePreviewWatch.GetMinIntervalMs())

	decoded, err := DecodeClientEnvelope(raw)
	require.NoError(t, err)
	require.Equal(t, message, decoded)
	reencoded, err := EncodeClientMessage(decoded)
	require.NoError(t, err)
	require.Equal(t, raw, reencoded, "RemotePreviewWatch must re-encode byte-for-byte")
	require.Equal(t, protocol.DecodeMessageRemotePreview, ClientVariantKindForTest(raw))
}

func TestRemotePreviewWatchStrictScan(t *testing.T) {
	valid, err := EncodeClientMessage(testRemotePreviewWatch())
	require.NoError(t, err)
	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "trailing garbage", raw: append(append([]byte(nil), valid...), 0xFF)},
		{name: "duplicate variant", raw: append(append([]byte(nil), valid...), valid...)},
	}
	for cut := 1; cut < len(valid); cut++ {
		tests = append(tests, struct {
			name string
			raw  []byte
		}{name: "truncated prefix", raw: valid[:cut]})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeClientEnvelope(tt.raw)
			require.Error(t, err)
		})
	}
}

func TestRemotePreviewWatchRejectsInvalidValues(t *testing.T) {
	request, err := EncodeClientMessage(testRemotePreviewWatch())
	require.NoError(t, err)
	var envelope wire.ClientEnvelope
	require.NoError(t, proto.Unmarshal(request, &envelope))
	valid := envelope.Payload.(*wire.ClientEnvelope_RemotePreviewWatch).RemotePreviewWatch

	tests := []struct {
		name   string
		mutate func(*wire.RemotePreviewWatch)
	}{
		{name: "zero interval", mutate: func(w *wire.RemotePreviewWatch) { w.MinIntervalMs = 0 }},
		{name: "interval below bound", mutate: func(w *wire.RemotePreviewWatch) { w.MinIntervalMs = 15 }},
		{name: "interval above bound", mutate: func(w *wire.RemotePreviewWatch) { w.MinIntervalMs = 1001 }},
		{name: "oversized interval", mutate: func(w *wire.RemotePreviewWatch) { w.MinIntervalMs = ^uint32(0) }},
		{name: "missing request", mutate: func(w *wire.RemotePreviewWatch) { w.Request = nil }},
		{name: "oversized width", mutate: func(w *wire.RemotePreviewWatch) { w.Request.Width = protocol.RemotePreviewMaxWidth + 1 }},
		{name: "zero height", mutate: func(w *wire.RemotePreviewWatch) { w.Request.Height = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := proto.Clone(valid).(*wire.RemotePreviewWatch)
			tt.mutate(message)
			_, err := remotePreviewWatchFromWire(message)
			require.ErrorIs(t, err, protocol.ErrInvalidRemotePreviewWatch)
		})
	}

	encodeTests := []struct {
		name   string
		mutate func(*protocol.RemotePreviewWatch)
	}{
		{name: "sub-millisecond interval", mutate: func(w *protocol.RemotePreviewWatch) { w.MinInterval += time.Microsecond }},
		{name: "interval above bound", mutate: func(w *protocol.RemotePreviewWatch) { w.MinInterval = 2 * time.Second }},
		{name: "stopped target", mutate: func(w *protocol.RemotePreviewWatch) { w.Request.Target.Stopped, w.Request.Target.LiveTabID = true, "" }},
	}
	for _, tt := range encodeTests {
		t.Run("encode "+tt.name, func(t *testing.T) {
			message := testRemotePreviewWatch()
			tt.mutate(&message)
			_, err := EncodeClientMessage(message)
			require.Error(t, err)
		})
	}
}
