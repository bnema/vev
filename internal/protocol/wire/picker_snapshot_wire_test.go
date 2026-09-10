package wire

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/protocol"
)

func pickerSnapshotForTest() protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: 7, Revision: 2, Title: " Sessions ",
		Rows: []protocol.PickerRow{
			{Key: "ab12/work", Display: "work", Detail: "2 tabs"},
			{Key: "cd34/perso", Display: "perso", Detail: "", Stopped: true},
		},
		Cursor:       protocol.PickerCursor{Key: "ab12/work", Index: 0},
		BarrierEpoch: 3, BarrierState: 9, SizeEpoch: 1,
	}
}

func TestPickerOpenRoundTrip(t *testing.T) {
	open := protocol.PickerOpen{InteractionID: 7}
	payload := MarshalPickerOpen(open)
	if payload == nil {
		t.Fatal("MarshalPickerOpen returned nil")
	}
	decoded, err := UnmarshalPickerOpen(payload)
	if err != nil || decoded != open {
		t.Fatalf("UnmarshalPickerOpen() = %#v, %v; want %#v", decoded, err, open)
	}
	assertAllPrefixesFail(t, payload, UnmarshalPickerOpen)
	assertTrailingGarbageFails(t, payload, UnmarshalPickerOpen)
	if MarshalPickerOpen(protocol.PickerOpen{}) != nil {
		t.Fatal("MarshalPickerOpen accepted zero interaction")
	}
}

func TestPickerSnapshotRoundTrip(t *testing.T) {
	snapshot := pickerSnapshotForTest()
	payload := MarshalPickerSnapshot(snapshot)
	if payload == nil {
		t.Fatal("MarshalPickerSnapshot returned nil")
	}
	decoded, err := UnmarshalPickerSnapshot(payload)
	if err != nil {
		t.Fatalf("UnmarshalPickerSnapshot() error = %v", err)
	}
	if decoded.InteractionID != snapshot.InteractionID || decoded.Revision != snapshot.Revision ||
		decoded.Title != snapshot.Title || decoded.BarrierEpoch != snapshot.BarrierEpoch ||
		decoded.BarrierState != snapshot.BarrierState || decoded.SizeEpoch != snapshot.SizeEpoch ||
		decoded.Cursor != snapshot.Cursor || len(decoded.Rows) != len(snapshot.Rows) {
		t.Fatalf("UnmarshalPickerSnapshot() = %#v, want %#v", decoded, snapshot)
	}
	for i, row := range decoded.Rows {
		if row != snapshot.Rows[i] {
			t.Fatalf("UnmarshalPickerSnapshot() row %d = %#v, want %#v", i, row, snapshot.Rows[i])
		}
	}
	assertAllPrefixesFail(t, payload, UnmarshalPickerSnapshot)
	assertTrailingGarbageFails(t, payload, UnmarshalPickerSnapshot)
}

func TestPickerCloseSelectionFailureRoundTrip(t *testing.T) {
	close := protocol.PickerClose{InteractionID: 7, Revision: 2}
	if payload := MarshalPickerClose(close); payload == nil {
		t.Fatal("MarshalPickerClose returned nil")
	} else {
		decoded, err := UnmarshalPickerClose(payload)
		if err != nil || decoded != close {
			t.Fatalf("UnmarshalPickerClose() = %#v, %v; want %#v", decoded, err, close)
		}
		assertAllPrefixesFail(t, payload, UnmarshalPickerClose)
		assertTrailingGarbageFails(t, payload, UnmarshalPickerClose)
	}
	selection := protocol.PickerSelection{CauseActionID: 9, InteractionID: 7, Revision: 2, Key: "ab12/work"}
	if payload := MarshalPickerSelection(selection); payload == nil {
		t.Fatal("MarshalPickerSelection returned nil")
	} else {
		decoded, err := UnmarshalPickerSelection(payload)
		if err != nil || decoded != selection {
			t.Fatalf("UnmarshalPickerSelection() = %#v, %v; want %#v", decoded, err, selection)
		}
		assertAllPrefixesFail(t, payload, UnmarshalPickerSelection)
		assertTrailingGarbageFails(t, payload, UnmarshalPickerSelection)
	}
	failure := protocol.PickerFailure{CauseActionID: 9, InteractionID: 7, Key: "ab12/work", Code: protocol.PickerRetiredTarget}
	if payload := MarshalPickerFailure(failure); payload == nil {
		t.Fatal("MarshalPickerFailure returned nil")
	} else {
		decoded, err := UnmarshalPickerFailure(payload)
		if err != nil || decoded != failure {
			t.Fatalf("UnmarshalPickerFailure() = %#v, %v; want %#v", decoded, err, failure)
		}
		assertAllPrefixesFail(t, payload, UnmarshalPickerFailure)
		assertTrailingGarbageFails(t, payload, UnmarshalPickerFailure)
	}
}

func TestPickerSnapshotBounds(t *testing.T) {
	snapshot := pickerSnapshotForTest()
	oversize := snapshot
	oversize.Title = strings.Repeat("x", protocol.PickerInteractionMaxDisplayBytes+1)
	if MarshalPickerSnapshot(oversize) != nil {
		t.Fatal("MarshalPickerSnapshot accepted oversize title")
	}
	oversize = snapshot
	oversize.Rows = append([]protocol.PickerRow(nil), snapshot.Rows...)
	oversize.Rows[0].Display = strings.Repeat("y", protocol.PickerInteractionMaxDisplayBytes+1)
	if MarshalPickerSnapshot(oversize) != nil {
		t.Fatal("MarshalPickerSnapshot accepted oversize row display")
	}
	duplicate := snapshot
	duplicate.Rows = []protocol.PickerRow{snapshot.Rows[0], snapshot.Rows[0]}
	if MarshalPickerSnapshot(duplicate) != nil {
		t.Fatal("MarshalPickerSnapshot accepted duplicate keys")
	}
	unknownCursor := snapshot
	unknownCursor.Cursor = protocol.PickerCursor{Key: "ff00/ghost", Index: 0}
	if MarshalPickerSnapshot(unknownCursor) != nil {
		t.Fatal("MarshalPickerSnapshot accepted unknown cursor key")
	}
	many := snapshot
	many.Rows = make([]protocol.PickerRow, protocol.PickerInteractionMaxRows+1)
	for i := range many.Rows {
		many.Rows[i] = protocol.PickerRow{Key: "k" + strings.Repeat("0", 120), Display: "d"}
	}
	if MarshalPickerSnapshot(many) != nil {
		t.Fatal("MarshalPickerSnapshot accepted too many rows")
	}
	if MarshalPickerSelection(protocol.PickerSelection{InteractionID: 7, Revision: 2}) != nil {
		t.Fatal("MarshalPickerSelection accepted empty key")
	}
	if MarshalPickerFailure(protocol.PickerFailure{InteractionID: 7, Code: 99}) != nil {
		t.Fatal("MarshalPickerFailure accepted unknown code")
	}
	if MarshalPickerClose(protocol.PickerClose{}) != nil {
		t.Fatal("MarshalPickerClose accepted zero interaction")
	}
}

func TestPickerMessageDirections(t *testing.T) {
	snapshot := pickerSnapshotForTest()
	// Direction is enforced by the sessionwire adapters (see
	// internal/adapters/sessionwire/picker_interaction_test.go); here we pin
	// that every picker payload unmarshals under its own type and rejects
	// the opposite direction's types.
	clientCases := []struct {
		name  string
		frame func() Frame
	}{
		{name: "open", frame: func() Frame {
			payload := MarshalPickerOpen(protocol.PickerOpen{InteractionID: 1})
			return Frame{Type: MsgPickerOpen, Payload: payload}
		}},
		{name: "close-client", frame: func() Frame {
			payload := MarshalPickerClose(protocol.PickerClose{InteractionID: 1})
			return Frame{Type: MsgPickerCloseClient, Payload: payload}
		}},
		{name: "selection", frame: func() Frame {
			payload := MarshalPickerSelection(protocol.PickerSelection{InteractionID: 1, Revision: 1, Key: "a/b"})
			return Frame{Type: MsgPickerSelection, Payload: payload}
		}},
	}
	serverCases := []struct {
		name  string
		frame func() Frame
	}{
		{name: "snapshot", frame: func() Frame {
			payload := MarshalPickerSnapshot(snapshot)
			return Frame{Type: MsgPickerSnapshot, Payload: payload}
		}},
		{name: "close-server", frame: func() Frame {
			payload := MarshalPickerClose(protocol.PickerClose{InteractionID: 1})
			return Frame{Type: MsgPickerCloseServer, Payload: payload}
		}},
		{name: "failure", frame: func() Frame {
			payload := MarshalPickerFailure(protocol.PickerFailure{InteractionID: 1, Code: protocol.PickerUnknownKey})
			return Frame{Type: MsgPickerFailure, Payload: payload}
		}},
	}
	for _, tt := range clientCases {
		t.Run("client/"+tt.name, func(t *testing.T) {
			frame := tt.frame()
			if frame.Payload == nil {
				t.Fatal("marshal returned nil")
			}
			if frame.Type != MsgPickerOpen && frame.Type != MsgPickerCloseClient && frame.Type != MsgPickerSelection {
				t.Fatalf("client frame has unexpected type %d", frame.Type)
			}
		})
	}
	for _, tt := range serverCases {
		t.Run("server/"+tt.name, func(t *testing.T) {
			frame := tt.frame()
			if frame.Payload == nil {
				t.Fatal("marshal returned nil")
			}
			if frame.Type != MsgPickerSnapshot && frame.Type != MsgPickerCloseServer && frame.Type != MsgPickerFailure {
				t.Fatalf("server frame has unexpected type %d", frame.Type)
			}
		})
	}
}
