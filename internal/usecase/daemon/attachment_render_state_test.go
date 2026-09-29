package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAttachmentRenderStateTransitions(t *testing.T) {
	kept, dropped := &pane{}, &pane{}
	emitted := composeCacheInput{valid: true}
	tests := []struct {
		name      string
		apply     func(*attachmentRenderState)
		wantCache bool
		wantSpare bool
		wantPanes []*pane
	}{
		{name: "commit keeps previous as spare", apply: func(r *attachmentRenderState) { r.commitComposition(emitted) }, wantCache: true, wantSpare: true, wantPanes: []*pane{kept, dropped}},
		{name: "forget composition keeps panes", apply: (*attachmentRenderState).forgetComposition, wantPanes: []*pane{kept, dropped}},
		{name: "forget one pane", apply: func(r *attachmentRenderState) { r.forgetPanes(dropped) }, wantCache: true, wantPanes: []*pane{kept}},
		{name: "forget all panes keeps composition", apply: (*attachmentRenderState).forgetAllPanes, wantCache: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var state attachmentRenderState
			state.commitComposition(emitted)
			state.storePaneSnapshot(kept, capturedPaneRenderState{})
			state.storePaneSnapshot(dropped, capturedPaneRenderState{})
			tt.apply(&state)
			require.Equal(t, tt.wantCache, state.cache.valid)
			require.Equal(t, tt.wantSpare, state.spare.valid)
			require.Len(t, state.panes, len(tt.wantPanes))
			for _, p := range tt.wantPanes {
				require.Contains(t, state.panes, p)
			}
		})
	}
}
