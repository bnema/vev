package wire

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func previewTargetForTest() domain.RemoteSessionTarget {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 9
	return domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
}

func TestRemotePreviewCodecPreservesWideStyledCells(t *testing.T) {
	style := renderer.DefaultStyle()
	style.Bold = true
	style.Attrs = renderer.AttrUnderline
	style.HasForegroundRGB = true
	style.ForegroundRGB = renderer.RGB{R: 1, G: 2, B: 3}
	preview := protocol.RemotePreview{
		Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK,
		LifecycleID: previewTargetForTest().LifecycleID, TabID: "tab-1", Revision: 4, Width: 3, Height: 1,
		Cells: []renderer.Cell{{Rune: '界', Style: style}, {Continuation: true, Style: style}, {Rune: 'x', Style: style}},
	}
	payload := MarshalRemotePreview(preview)
	if payload == nil {
		t.Fatal("MarshalRemotePreview returned nil")
	}
	got, err := UnmarshalRemotePreview(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Cells) != len(preview.Cells) || !got.Cells[0].Equal(preview.Cells[0]) || !got.Cells[1].Equal(preview.Cells[1]) {
		t.Fatalf("cells = %#v, want %#v", got.Cells, preview.Cells)
	}
}

func TestRemotePreviewCodecRejectsMalformedBoundsAndGarbage(t *testing.T) {
	preview := protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK, LifecycleID: previewTargetForTest().LifecycleID, TabID: "tab-1", Revision: 1, Width: 1, Height: 1, Cells: []renderer.Cell{{Rune: 'x'}}}
	payload := MarshalRemotePreview(preview)
	for i := 0; i < len(payload); i++ {
		if _, err := UnmarshalRemotePreview(payload[:i]); err == nil {
			t.Fatalf("prefix %d unexpectedly decoded", i)
		}
	}
	if _, err := UnmarshalRemotePreview(append(append([]byte(nil), payload...), 1)); err == nil {
		t.Fatal("trailing garbage unexpectedly decoded")
	}
	bad := preview
	bad.Width = protocol.RemotePreviewMaxWidth + 1
	if MarshalRemotePreview(bad) != nil {
		t.Fatal("oversized preview marshaled")
	}
}

func TestRemotePreviewRequestRejectsStoppedAndInvalidTargets(t *testing.T) {
	target := previewTargetForTest()
	request := protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 20, Height: 5}
	if MarshalRemotePreviewRequest(request) == nil {
		t.Fatal("valid preview request did not marshal")
	}
	target.Stopped = true
	target.LiveTabID = ""
	target.StoppedTab = domain.NewOrdinalTabSelector(0, "", 1)
	if MarshalRemotePreviewRequest(protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 20, Height: 5}) != nil {
		t.Fatal("stopped preview request marshaled")
	}
}
