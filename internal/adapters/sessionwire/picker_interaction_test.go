package sessionwire

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func pickerWireSnapshot() protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: 7, Revision: 2, Title: " Sessions ",
		Rows: []protocol.PickerRow{
			{Key: "ab12/work", Display: "work", Detail: "2 tabs"},
			{Key: "cd34/perso", Display: "perso", Stopped: true},
		},
		Cursor:       protocol.PickerCursor{Key: "ab12/work", Index: 0},
		BarrierEpoch: 3, BarrierState: 9, SizeEpoch: 1,
	}
}

func TestPickerClientMessagesEncodeWithTypes(t *testing.T) {
	tests := []struct {
		name    string
		message protocol.ClientMessage
		typeID  wire.MsgType
	}{
		{name: "close", message: protocol.PickerClose{InteractionID: 7, Revision: 2}, typeID: wire.MsgPickerCloseClient},
		{name: "close pointer", message: &protocol.PickerClose{InteractionID: 7, Revision: 2}, typeID: wire.MsgPickerCloseClient},
		{name: "selection", message: protocol.PickerSelection{CauseActionID: 9, InteractionID: 7, Revision: 2, Key: "ab12/work"}, typeID: wire.MsgPickerSelection},
		{name: "selection pointer", message: &protocol.PickerSelection{CauseActionID: 9, InteractionID: 7, Revision: 2, Key: "ab12/work"}, typeID: wire.MsgPickerSelection},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{}
			require.NoError(t, NewClientConnection(raw).SendClient(tt.message))
			require.Equal(t, tt.typeID, raw.sent.Type)
			got, err := decodeClient(raw.sent)
			require.NoError(t, err)
			switch message := tt.message.(type) {
			case *protocol.PickerClose:
				require.Equal(t, *message, got)
			case *protocol.PickerSelection:
				require.Equal(t, *message, got)
			default:
				require.Equal(t, tt.message, got)
			}
		})
	}
}

func TestPickerServerMessagesEncodeWithTypes(t *testing.T) {
	snapshot := pickerWireSnapshot()
	close := protocol.PickerClose{InteractionID: 7, Revision: 2}
	failure := protocol.PickerFailure{CauseActionID: 9, InteractionID: 7, Key: "ab12/work", Code: protocol.PickerRetiredTarget}
	tests := []struct {
		name    string
		message protocol.ServerMessage
		typeID  wire.MsgType
		payload []byte
	}{
		{name: "snapshot", message: snapshot, typeID: wire.MsgPickerSnapshot, payload: wire.MarshalPickerSnapshot(snapshot)},
		{name: "snapshot pointer", message: &snapshot, typeID: wire.MsgPickerSnapshot, payload: wire.MarshalPickerSnapshot(snapshot)},
		{name: "close", message: close, typeID: wire.MsgPickerCloseServer, payload: wire.MarshalPickerClose(close)},
		{name: "close pointer", message: &close, typeID: wire.MsgPickerCloseServer, payload: wire.MarshalPickerClose(close)},
		{name: "failure", message: failure, typeID: wire.MsgPickerFailure, payload: wire.MarshalPickerFailure(failure)},
		{name: "failure pointer", message: &failure, typeID: wire.MsgPickerFailure, payload: wire.MarshalPickerFailure(failure)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotNil(t, tt.payload)
			raw := &scriptedTransport{}
			require.NoError(t, NewServerConnection(raw).SendServer(tt.message))
			require.Equal(t, tt.typeID, raw.sent.Type)
			require.Equal(t, tt.payload, raw.sent.Payload)
		})
	}
}

func TestPickerMessagesRejectWrongDirection(t *testing.T) {
	snapshot := pickerWireSnapshot()
	clientFrames := []wire.Frame{
		{Type: wire.MsgPickerCloseClient, Payload: wire.MarshalPickerClose(protocol.PickerClose{InteractionID: 1})},
		{Type: wire.MsgPickerSelection, Payload: wire.MarshalPickerSelection(protocol.PickerSelection{InteractionID: 1, Revision: 1, Key: "a/b"})},
	}
	serverFrames := []wire.Frame{
		{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(snapshot)},
		{Type: wire.MsgPickerCloseServer, Payload: wire.MarshalPickerClose(protocol.PickerClose{InteractionID: 1})},
		{Type: wire.MsgPickerFailure, Payload: wire.MarshalPickerFailure(protocol.PickerFailure{InteractionID: 1, Code: protocol.PickerUnknownKey})},
	}
	for _, frame := range clientFrames {
		require.NotNil(t, frame.Payload)
		got, err := decodeClient(frame)
		require.NoError(t, err)
		require.NotNil(t, got)
		_, err = decodeServer(frame)
		require.ErrorIs(t, err, ErrWrongDirection)
	}
	for _, frame := range serverFrames {
		require.NotNil(t, frame.Payload)
		got, err := decodeServer(frame)
		require.NoError(t, err)
		require.NotNil(t, got)
		_, err = decodeClient(frame)
		require.ErrorIs(t, err, ErrWrongDirection)
	}
}
