package sessionwire

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func pickerWireOffer() protocol.PickerOffer {
	return protocol.PickerOffer{
		InteractionID: 7, RequestID: 3, Intent: protocol.PickerIntentMoveTab,
		MoveSourceKey: "ab12/work#tab-1", BarrierEpoch: 3, BarrierState: 9, SizeEpoch: 1,
		Title: " Sessions · grouped ",
	}
}

func pickerWireSnapshot() protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: 7, SourceID: "serving", SourceRevision: 2, Status: protocol.PickerSourceOK,
		Lines: []protocol.PickerLine{
			{Kind: protocol.PickerLineSection, Label: "LOCAL", Dim: true},
			{Key: "ab12/work", Kind: protocol.PickerLineSession, Label: "work", Actions: protocol.PickerCanNavigate},
			{Key: "ab12/work#tab-1", Kind: protocol.PickerLineTab, Label: "editor", Detail: " (vim)", Attention: true, Actions: protocol.PickerCanMove},
		},
		Cursor: protocol.PickerCursor{Key: "ab12/work", Index: 1},
	}
}

func pickerWireSelection() protocol.PickerSelection {
	return protocol.PickerSelection{
		CauseActionID: 9, RequestID: 3, InteractionID: 7, SourceID: "serving",
		SourceRevision: 2, Key: "ab12/work", Action: protocol.PickerActionMove,
	}
}

func pickerWirePreviewRequest() protocol.PickerPreviewRequest {
	return protocol.PickerPreviewRequest{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving",
		Key: "ab12/work#tab-1", Width: 80, Height: 24,
	}
}

func pickerWirePreview() protocol.PickerPreview {
	return protocol.PickerPreview{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving",
		Key: "ab12/work#tab-1", Status: protocol.PickerPreviewOK, Width: 2, Height: 1,
		Cells: []renderer.Cell{{Rune: 'o'}, {Rune: 'k'}},
	}
}

func TestPickerClientMessagesEncodeWithTypes(t *testing.T) {
	begin := protocol.PickerBegin{RequestID: 3, Intent: protocol.PickerIntentNavigation}
	closeMessage := protocol.PickerClose{InteractionID: 7, RequestID: 3}
	selection := pickerWireSelection()
	previewRequest := pickerWirePreviewRequest()
	tests := []struct {
		name    string
		message protocol.ClientMessage
		typeID  wire.MsgType
	}{
		{name: "begin", message: begin, typeID: wire.MsgPickerBegin},
		{name: "begin pointer", message: &begin, typeID: wire.MsgPickerBegin},
		{name: "close", message: closeMessage, typeID: wire.MsgPickerCloseClient},
		{name: "close pointer", message: &closeMessage, typeID: wire.MsgPickerCloseClient},
		{name: "selection", message: selection, typeID: wire.MsgPickerSelection},
		{name: "selection pointer", message: &selection, typeID: wire.MsgPickerSelection},
		{name: "preview request", message: previewRequest, typeID: wire.MsgPickerPreviewRequest},
		{name: "preview request pointer", message: &previewRequest, typeID: wire.MsgPickerPreviewRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{}
			require.NoError(t, NewClientConnection(raw).SendClient(tt.message))
			require.Equal(t, tt.typeID, raw.sent.Type)
			got, err := decodeClient(raw.sent)
			require.NoError(t, err)
			switch message := tt.message.(type) {
			case *protocol.PickerBegin:
				require.Equal(t, *message, got)
			case *protocol.PickerClose:
				require.Equal(t, *message, got)
			case *protocol.PickerSelection:
				require.Equal(t, *message, got)
			case *protocol.PickerPreviewRequest:
				require.Equal(t, *message, got)
			default:
				require.Equal(t, tt.message, got)
			}
		})
	}
}

func TestPickerServerMessagesEncodeWithTypes(t *testing.T) {
	offer := pickerWireOffer()
	snapshot := pickerWireSnapshot()
	closed := protocol.PickerClosed{InteractionID: 7, BarrierEpoch: 3, BarrierState: 9}
	result := protocol.PickerResult{CauseActionID: 9, InteractionID: 7, SourceID: "serving", Key: "ab12/work", Action: protocol.PickerActionKill}
	failure := protocol.PickerFailure{CauseActionID: 9, InteractionID: 7, SourceID: "serving", Key: "ab12/work", Action: protocol.PickerActionNavigate, Code: protocol.PickerRetiredTarget}
	preview := pickerWirePreview()
	tests := []struct {
		name    string
		message protocol.ServerMessage
		typeID  wire.MsgType
		payload []byte
	}{
		{name: "offer", message: offer, typeID: wire.MsgPickerOffer, payload: wire.MarshalPickerOffer(offer)},
		{name: "offer pointer", message: &offer, typeID: wire.MsgPickerOffer, payload: wire.MarshalPickerOffer(offer)},
		{name: "snapshot", message: snapshot, typeID: wire.MsgPickerSnapshot, payload: wire.MarshalPickerSnapshot(snapshot)},
		{name: "snapshot pointer", message: &snapshot, typeID: wire.MsgPickerSnapshot, payload: wire.MarshalPickerSnapshot(snapshot)},
		{name: "closed", message: closed, typeID: wire.MsgPickerClosedServer, payload: wire.MarshalPickerClosed(closed)},
		{name: "closed pointer", message: &closed, typeID: wire.MsgPickerClosedServer, payload: wire.MarshalPickerClosed(closed)},
		{name: "result", message: result, typeID: wire.MsgPickerResult, payload: wire.MarshalPickerResult(result)},
		{name: "result pointer", message: &result, typeID: wire.MsgPickerResult, payload: wire.MarshalPickerResult(result)},
		{name: "failure", message: failure, typeID: wire.MsgPickerFailure, payload: wire.MarshalPickerFailure(failure)},
		{name: "failure pointer", message: &failure, typeID: wire.MsgPickerFailure, payload: wire.MarshalPickerFailure(failure)},
		{name: "preview", message: preview, typeID: wire.MsgPickerPreview, payload: wire.MarshalPickerPreview(preview)},
		{name: "preview pointer", message: &preview, typeID: wire.MsgPickerPreview, payload: wire.MarshalPickerPreview(preview)},
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
	offer := pickerWireOffer()
	snapshot := pickerWireSnapshot()
	selection := pickerWireSelection()
	clientFrames := []wire.Frame{
		{Type: wire.MsgPickerBegin, Payload: wire.MarshalPickerBegin(protocol.PickerBegin{RequestID: 1, Intent: protocol.PickerIntentNavigation})},
		{Type: wire.MsgPickerCloseClient, Payload: wire.MarshalPickerClose(protocol.PickerClose{InteractionID: 1})},
		{Type: wire.MsgPickerSelection, Payload: wire.MarshalPickerSelection(selection)},
		{Type: wire.MsgPickerPreviewRequest, Payload: wire.MarshalPickerPreviewRequest(pickerWirePreviewRequest())},
	}
	serverFrames := []wire.Frame{
		{Type: wire.MsgPickerOffer, Payload: wire.MarshalPickerOffer(offer)},
		{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(snapshot)},
		{Type: wire.MsgPickerClosedServer, Payload: wire.MarshalPickerClosed(protocol.PickerClosed{InteractionID: 1})},
		{Type: wire.MsgPickerResult, Payload: wire.MarshalPickerResult(protocol.PickerResult{InteractionID: 1, SourceID: "serving", Key: "a/b", Action: protocol.PickerActionKill})},
		{Type: wire.MsgPickerFailure, Payload: wire.MarshalPickerFailure(protocol.PickerFailure{InteractionID: 1, Action: protocol.PickerActionKill, Code: protocol.PickerUnknownKey})},
		{Type: wire.MsgPickerPreview, Payload: wire.MarshalPickerPreview(pickerWirePreview())},
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
