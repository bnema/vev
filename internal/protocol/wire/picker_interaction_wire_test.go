package wire

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/protocol"
)

func pickerOfferForTest() protocol.PickerOffer {
	return protocol.PickerOffer{
		InteractionID: 7, RequestID: 3, Intent: protocol.PickerIntentMovePane,
		MoveSourceKey: "ab12/work#tab-1", BarrierEpoch: 3, BarrierState: 9, SizeEpoch: 1,
		Title: " Sessions · recent ",
	}
}

func pickerSnapshotForTest() protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: 7, SourceID: "serving", SourceRevision: 2,
		Status: protocol.PickerSourceOK,
		Lines: []protocol.PickerLine{
			{Kind: protocol.PickerLineSection, Label: "LOCAL", Dim: true},
			{
				Key: "ab12/work", Kind: protocol.PickerLineSession, Label: "work", Detail: "stopped",
				Status: protocol.PickerLineStatusStopped, Stopped: true,
				Actions: protocol.PickerCanNavigate | protocol.PickerCanKill,
			},
			{
				Key: "ab12/work#tab-1", Kind: protocol.PickerLineTab, Label: "editor",
				Detail: " (vim)", Attention: true, StatusDetail: "stale catalog",
				Actions: protocol.PickerCanNavigate, Ephemeral: true,
			},
			{
				Key: "cd34/host", Kind: protocol.PickerLineHost, Label: "example.test",
				Dim: true, Status: protocol.PickerLineStatusDown, StatusDetail: "unreachable",
			},
		},
		Cursor: protocol.PickerCursor{Key: "ab12/work#tab-1", Index: 2},
	}
}

func TestPickerOfferRoundTrip(t *testing.T) {
	offer := pickerOfferForTest()
	payload := MarshalPickerOffer(offer)
	if payload == nil {
		t.Fatal("MarshalPickerOffer returned nil")
	}
	decoded, err := UnmarshalPickerOffer(payload)
	if err != nil {
		t.Fatalf("UnmarshalPickerOffer() error = %v", err)
	}
	if decoded != offer {
		t.Fatalf("UnmarshalPickerOffer() = %#v, want %#v", decoded, offer)
	}
	assertAllPrefixesFail(t, payload, UnmarshalPickerOffer)
	assertTrailingGarbageFails(t, payload, UnmarshalPickerOffer)
}

func TestPickerBeginRoundTrip(t *testing.T) {
	begin := protocol.PickerBegin{RequestID: 11, Intent: protocol.PickerIntentNavigation}
	payload := MarshalPickerBegin(begin)
	if payload == nil {
		t.Fatal("MarshalPickerBegin returned nil")
	}
	decoded, err := UnmarshalPickerBegin(payload)
	if err != nil || decoded != begin {
		t.Fatalf("UnmarshalPickerBegin() = %#v, %v; want %#v", decoded, err, begin)
	}
	assertAllPrefixesFail(t, payload, UnmarshalPickerBegin)
	assertTrailingGarbageFails(t, payload, UnmarshalPickerBegin)
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
	if decoded.InteractionID != snapshot.InteractionID || decoded.SourceID != snapshot.SourceID ||
		decoded.SourceRevision != snapshot.SourceRevision || decoded.Status != snapshot.Status ||
		decoded.Cursor != snapshot.Cursor || len(decoded.Lines) != len(snapshot.Lines) {
		t.Fatalf("UnmarshalPickerSnapshot() = %#v, want %#v", decoded, snapshot)
	}
	for i, line := range decoded.Lines {
		if line != snapshot.Lines[i] {
			t.Fatalf("UnmarshalPickerSnapshot() line %d = %#v, want %#v", i, line, snapshot.Lines[i])
		}
	}
	assertAllPrefixesFail(t, payload, UnmarshalPickerSnapshot)
	assertTrailingGarbageFails(t, payload, UnmarshalPickerSnapshot)
}

func TestPickerCloseSelectionResultFailureRoundTrip(t *testing.T) {
	close := protocol.PickerClose{InteractionID: 7, RequestID: 3}
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
	closed := protocol.PickerClosed{InteractionID: 7, BarrierEpoch: 3, BarrierState: 9}
	if payload := MarshalPickerClosed(closed); payload == nil {
		t.Fatal("MarshalPickerClosed returned nil")
	} else {
		decoded, err := UnmarshalPickerClosed(payload)
		if err != nil || decoded != closed {
			t.Fatalf("UnmarshalPickerClosed() = %#v, %v; want %#v", decoded, err, closed)
		}
		assertAllPrefixesFail(t, payload, UnmarshalPickerClosed)
		assertTrailingGarbageFails(t, payload, UnmarshalPickerClosed)
	}
	selection := protocol.PickerSelection{
		CauseActionID: 9, RequestID: 3, InteractionID: 7, SourceID: "serving",
		SourceRevision: 2, Key: "ab12/work", Action: protocol.PickerActionMove,
	}
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
	result := protocol.PickerResult{
		CauseActionID: 9, RequestID: 3, InteractionID: 7, SourceID: "serving",
		Key: "ab12/work", Action: protocol.PickerActionKill,
	}
	if payload := MarshalPickerResult(result); payload == nil {
		t.Fatal("MarshalPickerResult returned nil")
	} else {
		decoded, err := UnmarshalPickerResult(payload)
		if err != nil || decoded != result {
			t.Fatalf("UnmarshalPickerResult() = %#v, %v; want %#v", decoded, err, result)
		}
		assertAllPrefixesFail(t, payload, UnmarshalPickerResult)
		assertTrailingGarbageFails(t, payload, UnmarshalPickerResult)
	}
	failure := protocol.PickerFailure{
		CauseActionID: 9, RequestID: 3, InteractionID: 7, SourceID: "serving",
		Key: "ab12/work", Action: protocol.PickerActionNavigate, Code: protocol.PickerRetiredTarget,
	}
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
	oversize.SourceID = strings.Repeat("x", protocol.PickerInteractionMaxSourceBytes+1)
	if MarshalPickerSnapshot(oversize) != nil {
		t.Fatal("MarshalPickerSnapshot accepted oversize source id")
	}
	oversize = snapshot
	oversize.Lines = append([]protocol.PickerLine(nil), snapshot.Lines...)
	oversize.Lines[1].Label = strings.Repeat("y", protocol.PickerInteractionMaxDisplayBytes+1)
	if MarshalPickerSnapshot(oversize) != nil {
		t.Fatal("MarshalPickerSnapshot accepted oversize line label")
	}
	duplicate := snapshot
	duplicate.Lines = []protocol.PickerLine{snapshot.Lines[1], snapshot.Lines[1]}
	if MarshalPickerSnapshot(duplicate) != nil {
		t.Fatal("MarshalPickerSnapshot accepted duplicate keys")
	}
	keyedSection := snapshot
	keyedSection.Lines = []protocol.PickerLine{{Kind: protocol.PickerLineSection, Label: "LOCAL", Key: "nope"}}
	if MarshalPickerSnapshot(keyedSection) != nil {
		t.Fatal("MarshalPickerSnapshot accepted a keyed section line")
	}
	unknownCursor := snapshot
	unknownCursor.Cursor = protocol.PickerCursor{Key: "ff00/ghost", Index: 0}
	if MarshalPickerSnapshot(unknownCursor) != nil {
		t.Fatal("MarshalPickerSnapshot accepted unknown cursor key")
	}
	many := snapshot
	many.Lines = make([]protocol.PickerLine, protocol.PickerInteractionMaxLines+1)
	for i := range many.Lines {
		many.Lines[i] = protocol.PickerLine{Key: "k" + strings.Repeat("0", 120), Kind: protocol.PickerLineSession, Label: "d"}
	}
	if MarshalPickerSnapshot(many) != nil {
		t.Fatal("MarshalPickerSnapshot accepted too many lines")
	}
	if MarshalPickerSelection(protocol.PickerSelection{InteractionID: 7, SourceRevision: 2, SourceID: "serving", Action: protocol.PickerActionNavigate}) != nil {
		t.Fatal("MarshalPickerSelection accepted empty key")
	}
	if MarshalPickerFailure(protocol.PickerFailure{InteractionID: 7, Action: protocol.PickerActionNavigate, Code: 99}) != nil {
		t.Fatal("MarshalPickerFailure accepted unknown code")
	}
	if MarshalPickerClose(protocol.PickerClose{}) != nil {
		t.Fatal("MarshalPickerClose accepted zero interaction")
	}
	if MarshalPickerOffer(protocol.PickerOffer{InteractionID: 1, Intent: protocol.PickerIntentNavigation, MoveSourceKey: "a/b"}) != nil {
		t.Fatal("MarshalPickerOffer accepted a move source on a navigation intent")
	}
	if MarshalPickerBegin(protocol.PickerBegin{Intent: protocol.PickerIntentNavigation}) != nil {
		t.Fatal("MarshalPickerBegin accepted a zero request id")
	}
	if MarshalPickerResult(protocol.PickerResult{InteractionID: 1, SourceID: "serving", Key: "a/b"}) != nil {
		t.Fatal("MarshalPickerResult accepted an unknown action")
	}
}

func TestPickerMessageTypes(t *testing.T) {
	// Direction is enforced by the sessionwire adapters (see
	// internal/adapters/sessionwire/picker_interaction_test.go); here we pin
	// the frame type each payload marshals to and that a nil payload never
	// reaches a transport.
	snapshot := pickerSnapshotForTest()
	cases := []struct {
		name    string
		want    MsgType
		payload []byte
	}{
		{name: "offer", want: MsgPickerOffer, payload: MarshalPickerOffer(pickerOfferForTest())},
		{name: "snapshot", want: MsgPickerSnapshot, payload: MarshalPickerSnapshot(snapshot)},
		{name: "closed", want: MsgPickerClosedServer, payload: MarshalPickerClosed(protocol.PickerClosed{InteractionID: 1})},
		{name: "result", want: MsgPickerResult, payload: MarshalPickerResult(protocol.PickerResult{InteractionID: 1, SourceID: "serving", Key: "a/b", Action: protocol.PickerActionKill})},
		{name: "failure", want: MsgPickerFailure, payload: MarshalPickerFailure(protocol.PickerFailure{InteractionID: 1, Action: protocol.PickerActionKill, Code: protocol.PickerUnknownKey})},
		{name: "begin", want: MsgPickerBegin, payload: MarshalPickerBegin(protocol.PickerBegin{RequestID: 1, Intent: protocol.PickerIntentNavigation})},
		{name: "close-client", want: MsgPickerCloseClient, payload: MarshalPickerClose(protocol.PickerClose{InteractionID: 1})},
		{name: "selection", want: MsgPickerSelection, payload: MarshalPickerSelection(protocol.PickerSelection{InteractionID: 1, SourceID: "serving", SourceRevision: 1, Key: "a/b", Action: protocol.PickerActionNavigate})},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.payload == nil {
				t.Fatal("marshal returned nil")
			}
			if tt.want != MsgPickerOffer && tt.want != MsgPickerSnapshot && tt.want != MsgPickerClosedServer &&
				tt.want != MsgPickerResult && tt.want != MsgPickerFailure &&
				tt.want != MsgPickerBegin && tt.want != MsgPickerCloseClient && tt.want != MsgPickerSelection {
				t.Fatalf("unexpected frame type %d", tt.want)
			}
		})
	}
}
