package daemon

import (
	"testing"
	"testing/synctest"

	"bnema.dev/vev/internal/domain"
	"bnema.dev/vev/internal/protocol"
	"bnema.dev/vev/internal/usecase/ui"
	"github.com/stretchr/testify/require"
)

func TestUnmarkedTabMoveRefreshesOpenPicker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, source, _, _, releases := newManualTabSession(t, 2)
		for _, release := range releases {
			defer release()
		}
		source.tabs[0].name = ""
		source.tabs[1].name = ""
		source.tabs[1].marked = true
		source.mu.Lock()
		d.publishSessionMarkLocked(source)
		source.mu.Unlock()
		destination, _ := addAgentTestSession(t, d, "destination")
		destination.name = "destination"
		viewer := testAttachment(destination)
		stopOverlayTimersAtCleanup(t, viewer)
		own := destination.tabs[0]
		openMovePickerForTest(t, d, viewer, destination, protocol.PickerIntentMoveTab, moveSourceForSession(destination, viewer, domain.TabStableID(own.stableID), ""))
		labels := func() []string {
			viewer.overlays.pickerMu.Lock()
			defer viewer.overlays.pickerMu.Unlock()
			var out []string
			for _, line := range viewer.overlays.pickerGrouped.Lines {
				out = append(out, line.Label)
			}
			return out
		}
		require.Contains(t, labels(), "2 "+string(ui.NerdGlyphs().Mark))
		moved := source.tabs[0]
		require.NoError(t, d.moveTab(moveTabRequest{Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID), Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation}}))
		require.Contains(t, labels(), "1 "+string(ui.NerdGlyphs().Mark))
		require.NotContains(t, labels(), "2 "+string(ui.NerdGlyphs().Mark))
	})
}
