package daemon

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/layout"
	"github.com/bnema/vev/internal/usecase/agentactivity"
	"github.com/bnema/vev/internal/usecase/ui"
)

// hiddenAgentPane installs the agent pane p in the viewed tab tb and returns
// a function that makes tb draw it.
type hiddenAgentPane func(t *testing.T, r *agentTestRig, tb *tab, p *pane) (show func())

// installFloatingAgentPane makes p tb's floating pane, visible or hidden.
func installFloatingAgentPane(visible bool) hiddenAgentPane {
	return func(t *testing.T, r *agentTestRig, tb *tab, p *pane) func() {
		installTestFloating(tb, p, visible)
		tb.mu.Lock()
		p.mu.Lock()
		p.publishOwnerLocked(r.sess, tb, tb.floating.generation)
		p.mu.Unlock()
		tb.mu.Unlock()
		return func() {
			// Showing the pane resizes the tab, which refreshes the bar cwd.
			r.proc.EXPECT().Cwd(mock.Anything).Return("", errors.New("no cwd")).Maybe()
			require.NoError(t, r.d.toggleFloating(r.sess, r.ac))
			tb.mu.Lock()
			defer tb.mu.Unlock()
			require.Equal(t, floatingVisible, tb.floating.state)
		}
	}
}

// installStackedAgentPane stacks p after tb's focused pane, collapsed.
func installStackedAgentPane(t *testing.T, r *agentTestRig, tb *tab, p *pane) func() {
	tb.mu.Lock()
	first := tb.tree.Focus
	tb.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(first), layout.NewLeaf(p.id)}, Expanded: first}
	tb.panes[p.id] = p
	p.mu.Lock()
	p.publishOwnerLocked(r.sess, tb, 0)
	p.mu.Unlock()
	tb.mu.Unlock()
	return func() {
		tb.mu.Lock()
		require.True(t, focusPlacementLocked(tb, p.id))
		tb.mu.Unlock()
	}
}

func TestAgentActivityUndrawnPaneDoneShownUntilDrawn(t *testing.T) {
	tests := []struct {
		name    string
		install hiddenAgentPane
		// wantDone is whether the finished turn is unseen.
		wantDone bool
		// recheckPaint is whether showing the pane leaves the visibility
		// re-check to the next paint, which the paint after it acknowledges.
		// The floating toggle re-checks at once.
		recheckPaint bool
	}{
		{name: "hidden floating pane in the viewed tab", install: installFloatingAgentPane(false), wantDone: true},
		{name: "visible floating pane in the viewed tab", install: installFloatingAgentPane(true), wantDone: false},
		{name: "collapsed stack member of the viewed tab", install: installStackedAgentPane, wantDone: true, recheckPaint: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 1)
			selectTestAttachmentTab(r.sess, 0)
			tb := r.sess.tabs[0]
			pty := newAgentTestPTY(t)
			pty.pgid.Store(778)
			r.proc.EXPECT().Argv(778).Return([]string{"claude"}, nil).Maybe()
			p := newPane("agent", pty.mock, domain.Size{Cols: 80, Rows: 20})
			p.ctx, p.cancel = tb.ctx, tb.cancel
			show := tt.install(t, r, tb, p)

			output := func(rows ...string) {
				p.mu.Lock()
				top := p.screen.Rows() - len(rows) + 1
				p.screen.Write([]byte(fmt.Sprintf("\x1b[2J\x1b[%d;1H", top) + strings.Join(rows, "\r\n")))
				p.mu.Unlock()
				r.sched.markDirty(p)
				r.advance(0)
				r.settle()
			}
			// paint renders the attachment through the paint path, lets the
			// scheduler apply its acknowledgement, and reports whether the
			// tab bar drew the Done check.
			paint := func() bool {
				_ = drainAllFrames(r.sends)
				r.d.setAttentionFrame(1) // a visible pulse beat
				r.d.paint(r.sess, r.ac, true, nil)
				data := string(mustOutputData(t, r.sends))
				r.advance(0)
				return strings.Contains(data, string(ui.DoneGlyph))
			}
			isDone := func() bool { return tb.agentIndicator() == agentactivity.IndicatorDone }

			output(claudeIdleRows...)
			r.advance(agentactivity.DefaultStartupGrace)
			output(claudeWorkingRows...)
			r.advance(agentactivity.DefaultMinDoneWorking + time.Second)
			output(claudeIdleRows...)
			r.advance(agentactivity.DefaultIdleHoldCap)
			require.Equal(t, tt.wantDone, isDone())
			if !tt.wantDone {
				require.False(t, paint(), "a seen Done is never drawn")
				return
			}

			for range 2 {
				require.True(t, paint(), "the viewed tab draws Done while the pane is not drawn")
				require.True(t, isDone(), "a paint without the pane keeps Done")
			}
			show()
			r.advance(0)
			if tt.recheckPaint {
				require.True(t, paint(), "the first paint with the pane drawn re-checks it")
				require.True(t, isDone())
			}
			require.False(t, paint(), "the viewed tab no longer draws Done once the pane is drawn")
			require.False(t, isDone(), "the paint that showed the pane acknowledged Done")
		})
	}
}
