package ports

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	remotePreviewGoldenStatusOffset         = 2
	remotePreviewGoldenCellsOffset          = 2 + 1 + 16 + 2 + len("tab-1") + 8 + 2 + 2 + 4
	remotePreviewGoldenCellFlagsOffset      = remotePreviewGoldenCellsOffset + 4
	remotePreviewGoldenStyleFlagsOffset     = remotePreviewGoldenCellsOffset + 5
	remotePreviewGoldenStyleAttrsOffset     = remotePreviewGoldenCellsOffset + 6
	remotePreviewGoldenSecondCellOffset     = remotePreviewGoldenCellsOffset + previewCellWireSize
	remotePreviewGoldenUnderlineStyleOffset = remotePreviewGoldenSecondCellOffset + 12
)

func remotePreviewGoldenPayload() []byte {
	return []byte{
		0x00, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x00, 0x05, 't', 'a', 'b', '-', '1',
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		0x00, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x6f, 0x00,
		0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x6b, 0x00,
		0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
}

func TestRemotePreviewCodecGoldenRoundTrip(t *testing.T) {
	golden := remotePreviewGoldenPayload()
	got, err := UnmarshalRemotePreview(golden)
	if err != nil {
		t.Fatalf("UnmarshalRemotePreview() error = %v", err)
	}
	if got.Version != RemotePreviewSchemaVersion || got.Status != RemotePreviewOK {
		t.Fatalf("header = (%d, %d), want (%d, %d)", got.Version, got.Status, RemotePreviewSchemaVersion, RemotePreviewOK)
	}
	var wantLifecycle domain.SessionLifecycleID
	for i := range wantLifecycle {
		wantLifecycle[i] = byte(i + 1)
	}
	if got.LifecycleID != wantLifecycle || got.TabID != "tab-1" || got.Revision != 1 || got.Width != 2 || got.Height != 1 {
		t.Fatalf("metadata = %#v, want lifecycle/tab/revision/size from golden", got)
	}
	if len(got.Cells) != 2 || got.Cells[0].Rune != 'o' || got.Cells[1].Rune != 'k' {
		t.Fatalf("cells = %#v, want [o k]", got.Cells)
	}
	if !bytes.Equal(MarshalRemotePreview(got), golden) {
		t.Fatalf("marshal changed golden payload: %x", MarshalRemotePreview(got))
	}
}

func TestRemotePreviewCodecRejectsInvalidStatusFlagsAndRanges(t *testing.T) {
	golden := remotePreviewGoldenPayload()
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "status", mutate: func(b []byte) { b[remotePreviewGoldenStatusOffset] = 0xff }},
		{name: "cell flags", mutate: func(b []byte) { b[remotePreviewGoldenCellFlagsOffset] = 0x02 }},
		{name: "style flags", mutate: func(b []byte) { b[remotePreviewGoldenStyleFlagsOffset] = 0x80 }},
		{name: "style attrs", mutate: func(b []byte) { b[remotePreviewGoldenStyleAttrsOffset] = 0x80 }},
		{name: "underline range", mutate: func(b []byte) { b[remotePreviewGoldenUnderlineStyleOffset] = byte(renderer.UnderlineDashed + 1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := append([]byte(nil), golden...)
			test.mutate(payload)
			if _, err := UnmarshalRemotePreview(payload); err == nil {
				t.Fatal("malformed payload decoded successfully")
			}
		})
	}
}

func TestRemotePreviewCodecRejectsWideCellBoundaryAndInvalidIDs(t *testing.T) {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 1
	valid := RemotePreview{
		Version: RemotePreviewSchemaVersion, Status: RemotePreviewOK,
		LifecycleID: lifecycle, TabID: "tab-1", Revision: 1, Width: 2, Height: 1,
		Cells: []renderer.Cell{{Rune: '界'}, {Continuation: true}},
	}
	if MarshalRemotePreview(valid) == nil {
		t.Fatal("valid wide-cell preview did not marshal")
	}
	for _, malformed := range []RemotePreview{
		{Version: RemotePreviewSchemaVersion, Status: RemotePreviewOK, LifecycleID: lifecycle, TabID: "tab-1", Revision: 1, Width: 1, Height: 1, Cells: []renderer.Cell{{Rune: '界'}}},
		{Version: RemotePreviewSchemaVersion, Status: RemotePreviewOK, LifecycleID: lifecycle, TabID: "tab-1", Revision: 1, Width: 2, Height: 1, Cells: []renderer.Cell{{Continuation: true}, {Rune: 'x'}}},
		{Version: RemotePreviewSchemaVersion, Status: RemotePreviewOK, LifecycleID: lifecycle, TabID: "tab-1", Revision: 1, Width: 2, Height: 1, Cells: []renderer.Cell{{Rune: '界'}, {Continuation: true, Rune: 'x'}}},
		{Version: RemotePreviewSchemaVersion, Status: RemotePreviewOK, LifecycleID: lifecycle, TabID: "tab-1", Revision: 1, Width: 2, Height: 2, Cells: []renderer.Cell{{Rune: 'x'}, {Rune: '界'}, {Continuation: true}, {Rune: 'y'}}},
	} {
		if MarshalRemotePreview(malformed) != nil {
			t.Fatalf("malformed wide-cell preview marshaled: %#v", malformed.Cells)
		}
	}
	invalidLifecycle := valid
	invalidLifecycle.LifecycleID = domain.SessionLifecycleID{}
	if MarshalRemotePreview(invalidLifecycle) != nil {
		t.Fatal("zero lifecycle ID marshaled")
	}
	invalidTab := valid
	invalidTab.TabID = "tab id"
	if MarshalRemotePreview(invalidTab) != nil {
		t.Fatal("invalid tab ID marshaled")
	}
}

func TestRemotePreviewRequestRejectsOversizedNestedRouteData(t *testing.T) {
	target := previewTargetForTest()
	target.Endpoint = strings.Repeat("h", math.MaxUint16+1)
	request := RemotePreviewRequest{Version: RemotePreviewSchemaVersion, Target: target, Width: 20, Height: 5}
	if MarshalRemotePreviewRequest(request) != nil {
		t.Fatal("oversized endpoint was truncated and marshaled")
	}
}

func TestRemoteTargetWireRejectsInvalidSelectorCombinations(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*domain.RemoteSessionTarget)
	}{
		{name: "zero lifecycle", mutate: func(target *domain.RemoteSessionTarget) { target.LifecycleID = domain.SessionLifecycleID{} }},
		{name: "live and stopped selector", mutate: func(target *domain.RemoteSessionTarget) {
			target.StoppedTab = domain.NewOrdinalTabSelector(0, "main", 1)
		}},
		{name: "stopped with live ID", mutate: func(target *domain.RemoteSessionTarget) {
			target.Stopped = true
			target.LiveTabID = "tab-1"
			target.StoppedTab = domain.NewOrdinalTabSelector(0, "main", 1)
		}},
		{name: "stable selector with ordinal fields", mutate: func(target *domain.RemoteSessionTarget) {
			target.Stopped = true
			target.LiveTabID = ""
			target.StoppedTab = domain.TabSelector{Kind: domain.TabSelectorByStableID, StableID: "tab-1", Ordinal: 1}
		}},
		{name: "ordinal selector with zero count", mutate: func(target *domain.RemoteSessionTarget) {
			target.Stopped = true
			target.LiveTabID = ""
			target.StoppedTab = domain.NewOrdinalTabSelector(0, "main", 0)
		}},
		{name: "oversized tab name", mutate: func(target *domain.RemoteSessionTarget) {
			target.Stopped = true
			target.LiveTabID = ""
			target.StoppedTab = domain.NewOrdinalTabSelector(0, strings.Repeat("n", 257), 1)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			target := *testRemoteTarget(false)
			test.mutate(&target)
			message := AttachTarget{Endpoint: target.Endpoint, Session: target.SessionName, Intent: IntentAttach, RemoteTarget: &target, EnvironmentPolicy: EnvironmentPolicyDaemonOwned}
			if MarshalAttachTarget(message) != nil {
				t.Fatal("invalid target was marshaled")
			}
		})
	}
}

func TestRemoteTargetWireRichGoldenRoundTrip(t *testing.T) {
	target := testRemoteTarget(false)
	got := MarshalAttachTarget(AttachTarget{
		Endpoint: target.Endpoint, Session: target.SessionName, Intent: IntentAttach,
		RemoteTarget: target, EnvironmentPolicy: EnvironmentPolicyDaemonOwned,
	})
	want := []byte{
		0x00, 0x0f, 'b', 'u', 'i', 'l', 'd', '@', 'm', 'u', 'l', 'e', ':', '2', '2', '2', '2',
		0x00, 0x04, 'w', 'o', 'r', 'k', IntentAttach,
		0x01, 0x01,
		0x00, 0x0f, 'b', 'u', 'i', 'l', 'd', '@', 'm', 'u', 'l', 'e', ':', '2', '2', '2', '2',
		0x00, 0x09, 'm', 'u', 'l', 'e', ':', '2', '2', '2', '2',
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x00, 0x04, 'w', 'o', 'r', 'k', 0x00,
		0x00, 0x05, 't', 'a', 'b', '-', '3', 0x00, 0x01,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("rich target bytes = %x, want %x", got, want)
	}
	decoded, err := UnmarshalAttachTarget(got)
	if err != nil {
		t.Fatalf("UnmarshalAttachTarget() error = %v", err)
	}
	if decoded.RemoteTarget == nil || *decoded.RemoteTarget != *target || decoded.EnvironmentPolicy != EnvironmentPolicyDaemonOwned {
		t.Fatalf("decoded target = %#v, policy = %d", decoded.RemoteTarget, decoded.EnvironmentPolicy)
	}
}
