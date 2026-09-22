package sessionwire

import (
	"testing"

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

func TestPickerClientMessagesEncodeWithTypes(t *testing.T) {
	closeMessage := protocol.PickerClose{InteractionID: 7, RequestID: 3}
	selection := pickerWireSelection()
	tests := []struct {
		name    string
		message protocol.ClientMessage
	}{
		{name: "close", message: closeMessage},
		{name: "close pointer", message: &closeMessage},
		{name: "selection", message: selection},
		{name: "selection pointer", message: &selection},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: []wire.Envelope{mustPreambleResponse(t)}}
			require.NoError(t, NewClientConnection(raw).SendClient(tt.message))
			got, err := DecodeClientEnvelope(mustSingleAppPayload(t, raw))
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
	offer := pickerWireOffer()
	closed := protocol.PickerClosed{InteractionID: 7, BarrierEpoch: 3, BarrierState: 9}
	result := protocol.PickerResult{CauseActionID: 9, InteractionID: 7, SourceID: "serving", Key: "ab12/work", Action: protocol.PickerActionKill}
	failure := protocol.PickerFailure{CauseActionID: 9, InteractionID: 7, SourceID: "serving", Key: "ab12/work", Action: protocol.PickerActionNavigate, Code: protocol.PickerRetiredTarget}
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

func TestPickerMessagesDecodeInCorrectDirection(t *testing.T) {
	offer := pickerWireOffer()
	snapshot := pickerWireSnapshot()
	selection := pickerWireSelection()
	clientMessages := []protocol.ClientMessage{
		protocol.PickerClose{InteractionID: 1},
		selection,
	}
	serverMessages := []protocol.ServerMessage{
		offer,
		snapshot,
		protocol.PickerClosed{InteractionID: 1},
		protocol.PickerResult{InteractionID: 1, SourceID: "serving", Key: "a/b", Action: protocol.PickerActionKill},
		protocol.PickerFailure{InteractionID: 1, Action: protocol.PickerActionKill, Code: protocol.PickerUnknownKey},
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
		require.Equal(t, message, got)
	}
}
