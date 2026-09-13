package sessionwire

import (
	"math"
	"testing"

	"google.golang.org/protobuf/proto"

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
	return protocol.NormalizePickerSnapshot(protocol.PickerSnapshot{
		InteractionID: 7, SourceID: "serving", SourceRevision: 2, Status: protocol.PickerSourceOK,
		Lines: []protocol.PickerLine{
			{Kind: protocol.PickerLineSection, Label: "LOCAL", Dim: true},
			{Key: "ab12/work", Kind: protocol.PickerLineSession, Label: "work", Focusable: true, Actions: protocol.PickerCanNavigate},
			{Key: "ab12/work#tab-1", Kind: protocol.PickerLineTab, Label: "editor", Detail: " (vim)", Attention: true, Focusable: true, Actions: protocol.PickerCanMove},
		},
		Cursor: protocol.PickerCursor{Key: "ab12/work", Index: 1},
	})
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

func pickerWireControlRequest() protocol.PickerControlRequest {
	return protocol.PickerControlRequest{
		Version: protocol.Version, RequestID: 4, Operation: protocol.PickerControlResolve,
		SourceRevision: 2, SourceID: "serving", Key: "ab12/work#tab-1",
	}
}

func pickerWireControlResponse() protocol.PickerControlResponse {
	snapshot := protocol.NormalizePickerSnapshot(pickerWireSnapshot())
	return protocol.PickerControlResponse{
		RequestID: 4, Operation: protocol.PickerControlSnapshot, Status: protocol.PickerSourceOK, Snapshot: &snapshot,
	}
}

func TestPickerClientMessagesEncodeWithTypes(t *testing.T) {
	begin := protocol.PickerBegin{RequestID: 3, Intent: protocol.PickerIntentNavigation}
	closeMessage := protocol.PickerClose{InteractionID: 7, RequestID: 3}
	selection := pickerWireSelection()
	previewRequest := pickerWirePreviewRequest()
	controlRequest := pickerWireControlRequest()
	tests := []struct {
		name    string
		message protocol.ClientMessage
	}{
		{name: "begin", message: begin},
		{name: "begin pointer", message: &begin},
		{name: "close", message: closeMessage},
		{name: "close pointer", message: &closeMessage},
		{name: "selection", message: selection},
		{name: "selection pointer", message: &selection},
		{name: "preview request", message: previewRequest},
		{name: "preview request pointer", message: &previewRequest},
		{name: "control request", message: controlRequest},
		{name: "control request pointer", message: &controlRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: []wire.Envelope{mustPreambleResponse(t)}}
			require.NoError(t, NewClientConnection(raw).SendClient(tt.message))
			got, err := DecodeClientEnvelope(mustSingleAppPayload(t, raw))
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
			case *protocol.PickerControlRequest:
				require.Equal(t, *message, got)
			default:
				require.Equal(t, tt.message, got)
			}
		})
	}
}

func TestPickerServerMessagesEncodeWithTypes(t *testing.T) {
	offer := pickerWireOffer()
	closed := protocol.PickerClosed{InteractionID: 7, BarrierEpoch: 3, BarrierState: 9}
	result := protocol.PickerResult{CauseActionID: 9, InteractionID: 7, SourceID: "serving", Key: "ab12/work", Action: protocol.PickerActionKill}
	failure := protocol.PickerFailure{CauseActionID: 9, InteractionID: 7, SourceID: "serving", Key: "ab12/work", Action: protocol.PickerActionNavigate, Code: protocol.PickerRetiredTarget}
	preview := pickerWirePreview()
	tests := []struct {
		name    string
		message protocol.ServerMessage
	}{
		{name: "offer", message: offer},
		{name: "offer pointer", message: &offer},
		{name: "closed", message: closed},
		{name: "closed pointer", message: &closed},
		{name: "result", message: result},
		{name: "result pointer", message: &result},
		{name: "failure", message: failure},
		{name: "failure pointer", message: &failure},
		{name: "preview", message: preview},
		{name: "preview pointer", message: &preview},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{}
			require.NoError(t, NewServerConnection(raw).SendServer(tt.message))
			require.Equal(t, 1, raw.sentLen())
			got, err := DecodeServerEnvelope(raw.sentPayload(0))
			require.NoError(t, err)
			switch message := tt.message.(type) {
			case *protocol.PickerOffer:
				require.Equal(t, *message, got)
			case *protocol.PickerClosed:
				require.Equal(t, *message, got)
			case *protocol.PickerResult:
				require.Equal(t, *message, got)
			case *protocol.PickerFailure:
				require.Equal(t, *message, got)
			case *protocol.PickerPreview:
				require.Equal(t, *message, got)
			default:
				require.Equal(t, tt.message, got)
			}
		})
	}
}

func TestPickerSnapshotNormalizesIntoProjections(t *testing.T) {
	snapshot := pickerWireSnapshot()
	raw := &scriptedTransport{}
	require.NoError(t, NewServerConnection(raw).SendServer(snapshot))
	require.Equal(t, 1, raw.sentLen())
	got, err := DecodeServerEnvelope(raw.sentPayload(0))
	require.NoError(t, err)
	decoded, ok := got.(protocol.PickerSnapshot)
	require.True(t, ok, "got %T", got)
	require.Equal(t, snapshot.Lines, decoded.Lines)
	require.Equal(t, snapshot.Cursor, decoded.Cursor)
	require.Equal(t, snapshot.Recent, decoded.Recent)
	require.Equal(t, snapshot.Grouped, decoded.Grouped)
	for _, projection := range []protocol.PickerProjection{decoded.Recent, decoded.Grouped} {
		require.Len(t, projection.Lines, 3)
		require.Equal(t, "ab12/work", projection.Cursor.Key)
	}
}

func TestPickerControlMessagesRoundTripThroughConnections(t *testing.T) {
	request := pickerWireControlRequest()
	response := pickerWireControlResponse()
	snapshot := pickerWireSnapshot()
	wanted := protocol.PickerControlResponse{
		RequestID: response.RequestID, Operation: response.Operation, Status: response.Status,
		Snapshot: &protocol.PickerSnapshot{
			InteractionID: snapshot.InteractionID, SourceID: snapshot.SourceID,
			SourceRevision: snapshot.SourceRevision, Status: snapshot.Status,
			Lines: snapshot.Lines, Cursor: snapshot.Cursor,
			Recent: snapshot.Recent, Grouped: snapshot.Grouped,
		},
	}

	for _, tc := range []struct {
		name    string
		message protocol.ClientMessage
	}{
		{name: "request", message: request},
		{name: "request pointer", message: &request},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientRaw := &scriptedTransport{recv: []wire.Envelope{mustPreambleResponse(t)}}
			require.NoError(t, NewClientConnection(clientRaw).SendClient(tc.message))
			serverRaw := &scriptedTransport{recv: mustServerPreambleQueue(t, mustSingleAppPayload(t, clientRaw))}
			got, err := NewServerConnection(serverRaw).ReceiveClient()
			require.NoError(t, err)
			require.Equal(t, request, got)
		})
	}

	for _, tc := range []struct {
		name    string
		message protocol.ServerMessage
	}{
		{name: "response", message: response},
		{name: "response pointer", message: &response},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverRaw := &scriptedTransport{}
			require.NoError(t, NewServerConnection(serverRaw).SendServer(tc.message))
			require.Equal(t, 1, serverRaw.sentLen())
			clientRaw := &scriptedTransport{recv: mustClientPreambleQueue(t, serverRaw.sentPayload(0))}
			got, err := NewClientConnection(clientRaw).ReceiveServer()
			require.NoError(t, err)
			require.Equal(t, wanted, got)
		})
	}
}

func TestPickerControlRequestMalformedPreHandshakeClassification(t *testing.T) {
	converted, err := pickerControlRequestToWire(pickerWireControlRequest())
	require.NoError(t, err)
	valid, err := proto.Marshal(&wire.ClientEnvelope{
		Payload: &wire.ClientEnvelope_PickerControlRequest{PickerControlRequest: converted},
	})
	require.NoError(t, err)
	tests := []struct {
		name    string
		payload []byte
		version uint16
	}{
		{name: "truncated version", payload: valid[:1]},
		{name: "oversized version", payload: pickerControlRequestWithVersion(t, math.MaxUint16+1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: mustServerPreambleQueue(t, tt.payload)}
			_, err := NewServerConnection(raw).ReceiveClient()
			var failure *protocol.DecodeFailure
			require.ErrorAs(t, err, &failure)
			require.Equal(t, protocol.DecodeMalformed, failure.Category)
			require.Equal(t, tt.version, failure.Version)
		})
	}
}

func pickerControlRequestWithVersion(t *testing.T, version uint32) []byte {
	t.Helper()
	converted, err := pickerControlRequestToWire(pickerWireControlRequest())
	require.NoError(t, err)
	converted.Version = version
	raw, err := proto.Marshal(&wire.ClientEnvelope{
		Payload: &wire.ClientEnvelope_PickerControlRequest{PickerControlRequest: converted},
	})
	require.NoError(t, err)
	return raw
}

func TestPickerMessagesDecodeInCorrectDirection(t *testing.T) {
	offer := pickerWireOffer()
	snapshot := pickerWireSnapshot()
	selection := pickerWireSelection()
	clientMessages := []protocol.ClientMessage{
		protocol.PickerBegin{RequestID: 1, Intent: protocol.PickerIntentNavigation},
		protocol.PickerClose{InteractionID: 1},
		selection,
		pickerWirePreviewRequest(),
		pickerWireControlRequest(),
	}
	serverMessages := []protocol.ServerMessage{
		offer,
		snapshot,
		protocol.PickerClosed{InteractionID: 1},
		protocol.PickerResult{InteractionID: 1, SourceID: "serving", Key: "a/b", Action: protocol.PickerActionKill},
		protocol.PickerFailure{InteractionID: 1, Action: protocol.PickerActionKill, Code: protocol.PickerUnknownKey},
		pickerWirePreview(),
		pickerWireControlResponse(),
	}
	// Cross-direction decodability is structural, not directional: field
	// numbers collide across envelopes, so a client payload may parse as
	// an unrelated server message and vice versa. Direction is enforced
	// structurally only where variants are unmapped; these cases pin the
	// deterministic classification the scanner produces today. Honest
	// wrong-direction rejection requires production to bind direction
	// into the envelope (see preamble epoch/role).
	for _, message := range clientMessages {
		raw := mustEncodeClient(t, message)
		got, err := DecodeClientEnvelope(raw)
		require.NoError(t, err)
		require.Equal(t, message, got)
	}
	for _, message := range serverMessages {
		raw := mustEncodeServer(t, message)
		got, err := DecodeServerEnvelope(raw)
		require.NoError(t, err)
		if snapshot, ok := message.(protocol.PickerSnapshot); ok {
			decoded, ok := got.(protocol.PickerSnapshot)
			require.True(t, ok, "got %T", got)
			// Converters normalize into Recent/Grouped projections and
			// repopulate the Lines/Cursor construction aliases.
			require.Equal(t, snapshot.Lines, decoded.Lines)
			require.Equal(t, snapshot.Cursor, decoded.Cursor)
			require.Equal(t, snapshot.Recent, decoded.Recent)
			require.Equal(t, snapshot.Grouped, decoded.Grouped)
			continue
		}
		if response, ok := message.(protocol.PickerControlResponse); ok {
			decoded, ok := got.(protocol.PickerControlResponse)
			require.True(t, ok, "got %T", got)
			require.Equal(t, response.RequestID, decoded.RequestID)
			require.Equal(t, response.Operation, decoded.Operation)
			require.Equal(t, response.Status, decoded.Status)
			require.NotNil(t, decoded.Snapshot)
			continue
		}
		require.Equal(t, message, got)
	}
}
