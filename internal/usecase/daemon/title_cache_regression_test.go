package daemon

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestComposeClientFrameCachePrunesTitleGenerationsAfterLayoutChurn(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 20, Rows: 5})
	first := win.focusedPane()
	var bars barCache
	var composed composedFrameCache
	state := barState{status: (&session{id: "s", name: "work", tabs: []*tab{win}}).statusSegments(true)}

	for i := 0; i < 8; i++ {
		id := layout.PaneID(fmt.Sprintf("pane-%d", i+2))
		replacement := newPane(id, nil, domain.Size{Cols: 20, Rows: 3})
		win.panes = map[layout.PaneID]*pane{first.id: first, id: replacement}
		win.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{
			layout.NewLeaf(first.id),
			layout.NewLeaf(id),
		}, Expanded: id}
		win.tree.Focus = id

		composeClientFrameWithLayoutCached(state, win, i == 0, solveTabLayoutLocked(win), &bars, &composed)

		require.Equal(t, map[layout.PaneID]uint64{
			first.id: composed.titleGenerations[first.id],
			id:       composed.titleGenerations[id],
		}, composed.titleGenerations)
	}
}
