package daemon

import (
	"strings"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
)

// TestHeldCtrlShowsRecentSessionRanks pins that a reported Ctrl hold prefixes
// the attachment's recent sessions with their Ctrl+N ranks, and its release
// removes them.
func TestHeldCtrlShowsRecentSessionRanks(t *testing.T) {
	tests := []struct {
		name  string
		ctrl  bool
		ranks bool
	}{
		{name: "held shows ranks", ctrl: true, ranks: true},
		{name: "released hides ranks", ctrl: false, ranks: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 1)
			defer releases[0]()
			ac.setRouteSnapshot(testRecentRouteSnapshot())
			effect := beginRecentRoutePaletteEffect(t, d, sess, ac)
			if !tt.ctrl {
				ac.ctrlHeld.Store(true)
			}

			d.applyModifierStateForAttachment(effect, protocol.ModifierState{Ctrl: tt.ctrl})

			require.Equal(t, tt.ctrl, ac.ctrlHeld.Load())
			bars := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})
			require.Equal(t, tt.ranks, bars.showRanks)
			row := renderTestStatusRow(bars)
			require.Equal(t, tt.ranks, strings.Contains(row, "1:recent"), row)
			require.Equal(t, tt.ranks, strings.Contains(row, "2:older"), row)
		})
	}
}

func renderTestStatusRow(state barState) string {
	row := make([]renderer.Cell, 80)
	drawStatusBarState(row, state, resolveStyles(nil))
	return rowText(row)
}
