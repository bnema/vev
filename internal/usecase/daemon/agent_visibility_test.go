package daemon

import (
	"context"
	"errors"
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

// paintDone paints the attachment through the paint path without running the
// scheduler and reports whether the tab bar drew the Done check.
func (r *agentTestRig) paintDone(t *testing.T) bool {
	t.Helper()
	_ = drainAllFrames(r.sends)
	r.d.setAttentionFrame(1) // a visible pulse beat
	r.d.paint(r.sess, r.ac, true, nil)
	return strings.Contains(string(mustOutputData(t, r.sends)), string(ui.DoneGlyph))
}

// newAgentPane returns a pane owned by tb's lifetime that is not yet in tb.
func newAgentPane(t *testing.T, tb *tab, id layout.PaneID) *pane {
	t.Helper()
	p := newPane(id, newAgentTestPTY(t).mock, domain.Size{Cols: 80, Rows: 20})
	p.ctx, p.cancel = tb.ctx, tb.cancel
	return p
}

// stackAgentPane stacks p after tb's focused pane, collapsed, and returns
// functions that expand p or the other member through the layout path.
func (r *agentTestRig) stackAgentPane(tb *tab, p *pane) (expand, collapse func()) {
	tb.mu.Lock()
	first := tb.tree.Focus
	tb.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(first), layout.NewLeaf(p.id)}, Expanded: first}
	tb.panes[p.id] = p
	tb.bumpLayoutGenerationLocked()
	p.mu.Lock()
	p.publishOwnerLocked(r.sess, tb, 0)
	p.mu.Unlock()
	tb.mu.Unlock()
	focus := func(id layout.PaneID) func() {
		return func() {
			tb.mu.Lock()
			changed := focusPlacementLocked(tb, id)
			tb.mu.Unlock()
			if changed {
				r.sess.geometry.applyTabLayout(r.d, r.sess, tb)
			}
		}
	}
	return focus(p.id), focus(first)
}

// floatAgentPane makes p tb's hidden floating pane.
func (r *agentTestRig) floatAgentPane(tb *tab, p *pane) {
	installTestFloating(tb, p, false)
	tb.mu.Lock()
	p.mu.Lock()
	p.publishOwnerLocked(r.sess, tb, tb.floating.generation)
	p.mu.Unlock()
	tb.mu.Unlock()
}

func (r *agentTestRig) paneDone(tb *tab, p *pane) bool {
	tr := r.sched.tabs[tb]
	return tr != nil && tr.panes[p].done
}

func TestAgentActivityCarriedDoneWaitsForNewTabPaint(t *testing.T) {
	r := newAgentTestRig(t, 3)
	selectTestAttachmentTab(r.sess, 0)
	destination := r.sess.tabs[2]
	moved := r.pane(1)
	moved.ctx, moved.cancel = context.WithCancel(r.d.paneProcessCtx)
	t.Cleanup(moved.cancel)
	r.sched.forget(moved)
	r.startClaudeTurn()
	r.endTurn(agentactivity.DefaultMinDoneWorking+time.Second, false)
	require.Equal(t, agentactivity.IndicatorDone, r.sess.tabs[1].agentIndicator())

	require.NoError(t, r.d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: r.sess.id, Incarnation: r.sess.incarnation},
		SourceTabID:      domain.TabStableID(r.sess.tabs[1].stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: r.sess.id, Incarnation: r.sess.incarnation},
		DestinationTabID: domain.TabStableID(destination.stableID),
	}))
	// A paint of the destination captured before the carried Done reached
	// it: its generation is newer than the Done's old shownGen.
	staleGen, seq := r.sched.gen, paneVisibilitySeq(destination)
	r.advance(0)
	r.settle()
	require.Equal(t, agentactivity.IndicatorDone, destination.agentIndicator(), "the Done is carried")

	r.sched.requestAck(destination, staleGen, seq)
	r.advance(0)
	r.d.noteAgentPaneVisibility(destination)
	r.advance(0)
	require.True(t, r.paneDone(destination, moved), "an ack older than the carry keeps the Done")

	// The emptied source tab closed, so the destination is now the last tab.
	selectTestAttachmentTab(r.sess, len(r.sess.tabs)-1)
	require.Equal(t, destination, r.sess.tabForAttachment(r.ac))
	r.paintDone(t)
	r.advance(0)
	require.False(t, r.paneDone(destination, moved), "the destination's paint acknowledges the Done")
}

func TestAgentActivityStaleAckAfterVisibilityChange(t *testing.T) {
	tests := []struct {
		name string
		// setup installs the drawn pane p in the unviewed tab tb, publishes
		// its Done, and returns the hide and show actions.
		setup func(t *testing.T, r *agentTestRig, tb *tab, p *pane) (hide, show func())
	}{
		{
			name: "floating pane hidden, painted, and shown before the ack",
			setup: func(t *testing.T, r *agentTestRig, tb *tab, p *pane) (func(), func()) {
				r.floatAgentPane(tb, p)
				r.proc.EXPECT().Cwd(mock.Anything).Return("", errors.New("no cwd")).Maybe()
				// The CLI toggle acts on the first tab and paints nothing.
				toggle := func() { require.NoError(t, r.d.toggleFloating(r.sess, nil)) }
				toggle()
				r.advance(0)
				seedAgentStatus(r.d, r.sess, tb, p, agentPaneStatus{state: agentactivity.Idle, done: true})
				return toggle, toggle
			},
		},
		{
			name: "stack member collapsed, painted, and expanded before the ack",
			setup: func(t *testing.T, r *agentTestRig, tb *tab, p *pane) (func(), func()) {
				expand, collapse := r.stackAgentPane(tb, p)
				expand()
				r.advance(0)
				seedAgentStatus(r.d, r.sess, tb, p, agentPaneStatus{state: agentactivity.Idle, done: true})
				return collapse, expand
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			selectTestAttachmentTab(r.sess, 1)
			tb := r.sess.tabs[0]
			p := newAgentPane(t, tb, "agent")
			hide, show := tt.setup(t, r, tb, p)
			require.True(t, r.paneDone(tb, p))

			// The scheduler sees none of these steps until the ack: the
			// paint between hide and show does not draw the pane.
			selectTestAttachmentTab(r.sess, 0)
			hide()
			r.paintDone(t)
			show()
			r.advance(0)
			require.True(t, r.paneDone(tb, p), "an ack captured while the pane was hidden keeps Done")

			r.paintDone(t)
			r.advance(0)
			require.False(t, r.paneDone(tb, p), "the paint that drew the pane acknowledges Done")
		})
	}
}

func TestAgentActivityUndrawnPaneDoneSurvivesOtherPanesAck(t *testing.T) {
	r := newAgentTestRig(t, 1)
	selectTestAttachmentTab(r.sess, 0)
	tb := r.sess.tabs[0]
	floating, stacked := newAgentPane(t, tb, "float"), newAgentPane(t, tb, "stacked")
	r.floatAgentPane(tb, floating)
	expand, _ := r.stackAgentPane(tb, stacked)
	seedAgentStatus(r.d, r.sess, tb, floating, agentPaneStatus{state: agentactivity.Idle, done: true, hidden: true})
	seedAgentStatus(r.d, r.sess, tb, stacked, agentPaneStatus{state: agentactivity.Idle, done: true, hidden: true})

	r.proc.EXPECT().Cwd(mock.Anything).Return("", errors.New("no cwd")).Maybe()
	require.NoError(t, r.d.toggleFloating(r.sess, r.ac))
	r.advance(0)
	r.paintDone(t)
	r.advance(0)
	require.False(t, r.paneDone(tb, floating), "the drawn floating pane is acknowledged")
	require.True(t, r.paneDone(tb, stacked), "the collapsed member keeps Done")
	require.True(t, r.paintDone(t), "the viewed tab still draws Done for the collapsed member")
	r.advance(0)
	require.True(t, r.paneDone(tb, stacked))

	expand()
	r.advance(0)
	r.paintDone(t)
	r.advance(0)
	require.False(t, r.paneDone(tb, stacked))
}

func TestAgentActivityCollapsedAfterDoneOnUnviewedTab(t *testing.T) {
	r := newAgentTestRig(t, 2)
	selectTestAttachmentTab(r.sess, 1)
	tb := r.sess.tabs[0]
	p := newAgentPane(t, tb, "agent")
	expand, collapse := r.stackAgentPane(tb, p)
	expand()
	r.advance(0)
	seedAgentStatus(r.d, r.sess, tb, p, agentPaneStatus{state: agentactivity.Idle, done: true})

	collapse()
	r.advance(0)
	selectTestAttachmentTab(r.sess, 0)
	for range 2 {
		require.True(t, r.paintDone(t), "the viewed tab draws Done for the collapsed member")
		r.advance(0)
		require.True(t, r.paneDone(tb, p))
	}
}

func TestAgentActivityAckSweepKeepsUndrawnDone(t *testing.T) {
	r := newAgentTestRig(t, 2)
	selectTestAttachmentTab(r.sess, 0)
	tb, other := r.sess.tabs[0], r.sess.tabs[1]
	hidden, drawn := newAgentPane(t, tb, "hidden"), r.pane(0)
	r.floatAgentPane(tb, hidden)
	seedAgentStatus(r.d, r.sess, tb, hidden, agentPaneStatus{state: agentactivity.Idle, done: true, hidden: true})
	seedAgentStatus(r.d, r.sess, tb, drawn, agentPaneStatus{state: agentactivity.Idle, done: true})
	for len(r.sched.acks) < cap(r.sched.acks) {
		r.sched.acks <- other
	}

	r.paintDone(t)
	require.True(t, r.sched.ackSweep.Load(), "the ack overflowed")
	r.advance(0)
	require.False(t, r.paneDone(tb, drawn), "the sweep acknowledges the drawn pane")
	require.True(t, r.paneDone(tb, hidden), "the sweep keeps the undrawn pane's Done")
}

func TestAgentActivityCLIFloatingToggleKeepsUnseenDone(t *testing.T) {
	r := newAgentTestRig(t, 2)
	// The CLI toggle acts on the first tab, which no attachment views.
	selectTestAttachmentTab(r.sess, 1)
	tb := r.sess.tabs[0]
	p := newAgentPane(t, tb, "agent")
	r.floatAgentPane(tb, p)
	seedAgentStatus(r.d, r.sess, tb, p, agentPaneStatus{state: agentactivity.Idle, done: true, hidden: true})
	r.proc.EXPECT().Cwd(mock.Anything).Return("", errors.New("no cwd")).Maybe()

	for range 3 {
		require.NoError(t, r.d.toggleFloating(r.sess, nil))
		r.advance(0)
		require.True(t, r.paneDone(tb, p), "a CLI toggle does not acknowledge Done")
	}
	tb.mu.Lock()
	require.Equal(t, floatingVisible, tb.floating.state)
	tb.mu.Unlock()

	selectTestAttachmentTab(r.sess, 0)
	r.paintDone(t)
	r.advance(0)
	require.False(t, r.paneDone(tb, p), "the paint that drew the pane acknowledges Done")
}
