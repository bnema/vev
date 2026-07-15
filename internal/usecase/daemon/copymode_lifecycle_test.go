package daemon

import (
	"fmt"
	"testing"
	"time"

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
	ac.overlays.copyMu.Lock()
	seedCopyInteractionLocked(ac.overlays, ac.overlays.copyPane, ac.overlays.copyDocument)
	ac.overlays.copyMu.Unlock()

	d.resize(sess, ac, domain.Size{Cols: 100, Rows: 30})

	require.False(t, ac.overlays.copyActive())
	ac.overlays.copyMu.Lock()
	require.False(t, ac.overlays.copyPointer.valid)
	require.False(t, ac.overlays.copyClick.valid)
	ac.overlays.copyMu.Unlock()
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
	ac.overlays.copyMu.Lock()
	seedCopyInteractionLocked(ac.overlays, ac.overlays.copyPane, ac.overlays.copyDocument)
	ac.overlays.copyMu.Unlock()

	require.True(t, sess.switchTab(1))
	d.activateTab(sess, sess.activeTab())

	require.False(t, ac.overlays.copyActive())
	ac.overlays.copyMu.Lock()
	require.False(t, ac.overlays.copyPointer.valid)
	require.False(t, ac.overlays.copyClick.valid)
	ac.overlays.copyMu.Unlock()
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
	document := scopy.NewDocument(scopy.NewSnapshot(closing.history, closing.screen.Frame), domain.DefaultWordSeparators)
	closing.mu.Unlock()
	ac.overlays.copyMu.Lock()
	ac.overlays.copyPane = closing
	ac.overlays.copyDocument = document
	ac.overlays.copyMode = scopy.NewMode(document)
	seedCopyInteractionLocked(ac.overlays, closing, document)
	ac.overlays.copyMu.Unlock()

	require.NoError(t, d.closePane(sess, tb, "pane-2", nil, false))
	require.False(t, ac.overlays.copyActive())
	require.Nil(t, copyTargetPane(ac.overlays))
	ac.overlays.copyMu.Lock()
	require.False(t, ac.overlays.copyPointer.valid)
	require.False(t, ac.overlays.copyClick.valid)
	ac.overlays.copyMu.Unlock()
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
	document := scopy.NewDocument(scopy.NewSnapshot(p.history, p.screen.Frame), domain.DefaultWordSeparators)
	p.mu.Unlock()

	// Pause after candidate publication, then make the captured target stale.
	// A render snapshot while paused must not expose the candidate document.
	reached := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan struct{})
	d.beforeCopyModeRevalidate = func() {
		close(reached)
		<-resume
	}
	resumed := false
	defer func() {
		if !resumed {
			close(resume)
		}
		select {
		case <-done:
			d.beforeCopyModeRevalidate = nil
		case <-time.After(time.Second):
			t.Error("copy mode publication did not finish after revalidation gate was released")
		}
	}()

	result := make(chan bool, 1)
	go func() {
		defer close(done)
		result <- d.publishCopyMode(sess, ac, tb, p, document, nil, nil)
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("copy mode publication did not reach revalidation gate")
	}

	renderSnapshot := ac.overlays.SnapshotForRender()
	candidateWasRenderable := renderSnapshot.copyActive
	renderSnapshot.Unlock()
	require.True(t, sess.switchTab(1))
	close(resume)
	resumed = true

	select {
	case published := <-result:
		require.False(t, published)
	case <-time.After(time.Second):
		t.Fatal("copy mode publication did not finish after revalidation gate was released")
	}
	require.False(t, candidateWasRenderable)
	require.False(t, ac.overlays.copyActive())
	require.Nil(t, copyTargetPane(ac.overlays))
}

func TestCopyModeLifecyclePublicationEpochRejectsReleaseAndPaneClose(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invalidate func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, tb *tab, p *pane)
	}{
		{
			name: "release during publication",
			invalidate: func(_ *testing.T, d *Daemon, sess *session, ac *attachedClient, _ *tab, _ *pane) {
				d.handleInput(sess, ac, []byte("\x1b[<0;1;2m"))
			},
		},
		{
			name: "pane close during publication",
			invalidate: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, tb *tab, p *pane) {
				require.NoError(t, d.closePane(sess, tb, p.id, ac, false))
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

			// A real press is required: the motion publication must inherit this
			// pointer epoch, rather than publishing a fresh interaction.
			d.handleInput(sess, ac, []byte("\x1b[<0;1;2M"))
			ac.overlays.copyMu.Lock()
			require.True(t, ac.overlays.copyPointer.valid)
			ac.overlays.copyMu.Unlock()

			reached := make(chan struct{})
			resume := make(chan struct{})
			d.beforeCopyModeRevalidate = func() {
				close(reached)
				<-resume
			}
			done := make(chan struct{})
			go func() {
				d.handleInput(sess, ac, []byte("\x1b[<32;1;3M"))
				close(done)
			}()
			<-reached

			tc.invalidate(t, d, sess, ac, tb, p)
			close(resume)
			<-done

			ac.overlays.copyMu.Lock()
			require.Nil(t, ac.overlays.copyMode, "stale publication must not resurrect copy mode")
			require.Nil(t, ac.overlays.copyCandidate)
			require.False(t, ac.overlays.copyPointer.valid)
			require.False(t, ac.overlays.copyClick.valid)
			ac.overlays.copyMu.Unlock()
		})
	}
}

func TestCopyModeLifecycleFloatingCloseDuringPublicationDoesNotResurrect(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 2)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	tb := sess.activeTab()
	floating := newPane("floating", nil, domain.Size{Cols: 20, Rows: 5})
	installTestFloating(tb, floating, true)
	tb.mu.Lock()
	_, geometry, visible := tb.visibleFloatingSnapshotLocked(d.currentFloatingConfig())
	tb.mu.Unlock()
	require.True(t, visible)
	press := fmt.Appendf(nil, "\x1b[<0;%d;%dM", geometry.Inner.X+1, geometry.Inner.Y+2)
	motion := fmt.Appendf(nil, "\x1b[<32;%d;%dM", geometry.Inner.X+1, geometry.Inner.Y+3)

	d.handleInput(sess, ac, press)
	ac.overlays.copyMu.Lock()
	require.True(t, ac.overlays.copyPointer.valid)
	ac.overlays.copyMu.Unlock()

	reached := make(chan struct{})
	resume := make(chan struct{})
	d.beforeCopyModeRevalidate = func() {
		close(reached)
		<-resume
	}
	done := make(chan struct{})
	go func() {
		d.handleInput(sess, ac, motion)
		close(done)
	}()
	<-reached
	d.teardownFloating(tb, ac)
	close(resume)
	<-done

	ac.overlays.copyMu.Lock()
	require.Nil(t, ac.overlays.copyMode)
	require.Nil(t, ac.overlays.copyCandidate)
	require.False(t, ac.overlays.copyPointer.valid)
	require.False(t, ac.overlays.copyClick.valid)
	ac.overlays.copyMu.Unlock()
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
			document := scopy.NewDocument(scopy.NewSnapshot(p.history, p.screen.Frame), domain.DefaultWordSeparators)
			p.mu.Unlock()
			tc.invalidate(sess, tb, p)

			require.False(t, d.publishCopyMode(sess, ac, tb, p, document, nil, nil))
			require.False(t, ac.overlays.copyActive())
			require.Nil(t, copyTargetPane(ac.overlays))
		})
	}
}
