package daemon

import (
	"fmt"
	"testing"

	"bnema.dev/vev/internal/protocol"

	"bnema.dev/vev/internal/usecase/agentactivity"
	"bnema.dev/vev/internal/usecase/ui"
	"github.com/stretchr/testify/require"
)

func TestPickerAndPaletteSessionMarks(t *testing.T) {
	for _, glyphs := range []ui.GlyphSet{ui.NerdGlyphs(), ui.UnicodeGlyphs(), ui.ASCIIGlyphs()} {
		for _, marked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%t", glyphs.Mode, marked), func(t *testing.T) {
				sess := &session{sessionCore: sessionCore{id: "work", name: "work", incarnation: newTestLifecycle(t)}, tabs: []*tab{{stableID: "bell", name: "bell", attention: true}, {stableID: "marked", name: "marked", marked: marked}}}
				snap := sess.snapshotView(viewOptions{tabDetails: true})
				require.Equal(t, marked, snap.marked)
				require.Equal(t, marked, sess.snapshotView(viewOptions{}).marked, "attention on an earlier tab must not stop aggregation")
				view := snap.pickerView(newPickerMarks(glyphs, glyphs.Spinner), agentactivity.IndicatorDone)
				require.Contains(t, view.Name, string(glyphs.Attention))
				require.Equal(t, "work", view.TargetName)
				if marked {
					require.Contains(t, view.Name, string(glyphs.Mark))
					require.Equal(t, "marked "+string(glyphs.Mark), view.Tabs[1].Name)
				} else {
					require.NotContains(t, view.Name, string(glyphs.Mark))
					require.Equal(t, "marked", view.Tabs[1].Name)
				}
				d := newTestDaemon(t, nil, newStubClock(t))
				d.sessions[sess.id] = sess
				results := d.paletteResultsWithMark(nil, nil, protocol.RecentRouteSnapshot{}, glyphs.Mark)
				require.Len(t, results, 1)
				name, ok := results[0].SessionName()
				require.True(t, ok)
				require.Equal(t, "work", name)
				if marked {
					require.Contains(t, results[0].DisplayText(), string(glyphs.Mark))
				} else {
					require.NotContains(t, results[0].DisplayText(), string(glyphs.Mark))
				}
			})
		}
	}
}
