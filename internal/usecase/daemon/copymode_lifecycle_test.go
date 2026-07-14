package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestCopyModeLifecycleResizeExitsBeforeGeometryChange(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
	d.enterCopyMode(sess, ac)
	require.True(t, ac.overlays.copyActive())

	d.resize(sess, ac, domain.Size{Cols: 100, Rows: 30})

	require.False(t, ac.overlays.copyActive())
}

func TestCopyModeLifecycleActivateTabClearsRuntime(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 2)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	d.enterCopyMode(sess, ac)
	require.True(t, ac.overlays.copyActive())

	require.True(t, sess.switchTab(1))
	d.activateTab(sess, sess.activeTab())

	require.False(t, ac.overlays.copyActive())
}

func TestCopyModeLifecycleClosePaneClearsRecoveredClientState(t *testing.T) {
	d, sess, _, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	tb := sess.activeTab()
	closingPTY := portsmocks.NewMockPTY(t)
	closingPTY.EXPECT().Close().Return(nil).Once()
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	closing := newPane("pane-2", closingPTY, domain.Size{Cols: 20, Rows: 10})
	tb.panes["pane-2"] = closing
	ac := &attachedClient{output: newOutputStateStream()}
	ac.initOverlays()
	sess.client = ac
	ac.setSession(sess)
	closing.mu.Lock()
	document := scopy.NewSnapshot(closing.history, closing.screen.Frame)
	closing.mu.Unlock()
	ac.overlays.copyMu.Lock()
	ac.overlays.copyPane = closing
	ac.overlays.copySnapshot = &document
	ac.overlays.copyMode = scopy.NewMode(document)
	ac.overlays.copyMu.Unlock()

	require.NoError(t, d.closePane(sess, tb, "pane-2", nil, false))
	require.False(t, ac.overlays.copyActive())
	require.Nil(t, copyTargetPane(ac.overlays))
}

func TestCopyModeLifecycleCloseOnlyPaneInTabClearsRecoveredClientState(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 2)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	tb := sess.activeTab()
	p := tb.focusedPane()
	d.enterCopyMode(sess, ac)
	require.True(t, ac.overlays.copyActive())

	require.NoError(t, d.closePane(sess, tb, p.id, nil, false))
	require.False(t, ac.overlays.copyActive())
	require.Nil(t, copyTargetPane(ac.overlays))
}

func TestCopyModeLifecycleDoesNotRenderCandidateBeforeValidation(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 2)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	tb := sess.activeTab()
	p := tb.focusedPane()
	p.mu.Lock()
	document := scopy.NewSnapshot(p.history, p.screen.Frame)
	p.mu.Unlock()

	// Hold session validation after publication and make the captured target
	// stale. A render snapshot taken in this window must not expose the
	// candidate document.
	sess.mu.Lock()
	sess.active = 1
	result := make(chan bool, 1)
	go func() {
		result <- d.publishCopyMode(sess, ac, tb, p, document, nil)
	}()
	published := assert.Eventually(t, func() bool {
		ac.overlays.copyMu.Lock()
		defer ac.overlays.copyMu.Unlock()
		return ac.overlays.copyPane == p && ac.overlays.copySnapshot != nil
	}, time.Second, time.Millisecond)
	if !published {
		sess.mu.Unlock()
		<-result
		return
	}
	renderSnapshot := ac.overlays.SnapshotForRender()
	candidateWasRenderable := renderSnapshot.copyActive
	renderSnapshot.Unlock()
	sess.mu.Unlock()

	require.False(t, <-result)
	require.False(t, candidateWasRenderable)
	require.False(t, ac.overlays.copyActive())
	require.Nil(t, copyTargetPane(ac.overlays))
}

func TestCopyModeLifecycleRejectsPublicationForInactiveOrRemovedPane(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invalidate func(*session, *tab, *pane)
	}{
		{
			name: "removed pane",
			invalidate: func(_ *session, tb *tab, p *pane) {
				tb.mu.Lock()
				delete(tb.panes, p.id)
				tb.mu.Unlock()
			},
		},
		{
			name: "inactive tab",
			invalidate: func(sess *session, _ *tab, _ *pane) {
				require.True(t, sess.switchTab(1))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 2)
			defer func() {
				for _, release := range releases {
					release()
				}
			}()
			tb := sess.activeTab()
			p := tb.focusedPane()
			p.mu.Lock()
			document := scopy.NewSnapshot(p.history, p.screen.Frame)
			p.mu.Unlock()
			tc.invalidate(sess, tb, p)

			require.False(t, d.publishCopyMode(sess, ac, tb, p, document, nil))
			require.False(t, ac.overlays.copyActive())
			require.Nil(t, copyTargetPane(ac.overlays))
		})
	}
}
