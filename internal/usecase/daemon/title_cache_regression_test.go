package daemon

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestComposeFramePrunesTitleGenerationsAfterLayoutChurn(t *testing.T) {
	committed := composeCacheInput{}
	scratch := composeCacheInput{}
	first := layout.PaneID("pane-1")

	for i := range 8 {
		id := layout.PaneID(fmt.Sprintf("pane-%d", i+2))
		state := capturedRenderState{
			reset:  i == 0,
			layout: capturedTabLayout{area: domain.Rect{Width: 20, Height: 5}, fingerprint: fmt.Sprintf("layout-%d", i), valid: true},
			panes: []capturedPaneRenderState{
				{id: first, title: "first", titleGeneration: 1, placement: layout.Placement{ID: first, TitleBar: domain.Rect{Width: 20, Height: 1}, Collapsed: true}},
				{id: id, title: "replacement", titleGeneration: 1, placement: layout.Placement{ID: id, TitleBar: domain.Rect{Y: 1, Width: 20, Height: 1}, Collapsed: true}},
			},
		}
		out := composeFrame(state, committed, scratch)
		scratch, committed = committed, out.cache

		require.Equal(t, map[layout.PaneID]uint64{first: 1, id: 1}, committed.titleGenerations)
	}
}
