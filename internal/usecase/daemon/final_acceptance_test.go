package daemon

import (
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/require"
)

func TestAcceptancePaletteNewTabUsesSessionGeometryAfterAttachmentResize(t *testing.T) {
	d, sess, first, _ := newManualSessionWithPTYs(t, newQuietPTY())
	first.clientID[0] = 1
	firstTab := sess.tabs[0]
	firstTab.stableID = "tab-first"
	firstTab.panes[firstTab.focusedPane().id].stableID = "pane-first"

	secondTransport, _ := newCapturingTransport(t)
	second := &attachedClient{
		tr:     secondTransport,
		output: newOutputStateStream(),
		size:   domain.Size{Cols: 80, Rows: 24},
	}
	second.initOverlays()
	second.clientID[0] = 2
	second.setSession(sess)
	secondTab := newTabWithStableID("tab-second", "pane-second", newQuietPTY(), domain.Size{Cols: 80, Rows: 23})
	secondTab.ctx, secondTab.cancel = sess.ctx, func() {}
	publishTiledPaneOwners(sess, secondTab)
	require.NoError(t, sess.runMutation(func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		sess.tabs = append(sess.tabs, secondTab)
		return nil
	}))
	require.True(t, sess.registerAttachment(second))
	require.True(t, sess.selectAttachmentTab(first, domain.TabStableID(firstTab.stableID)))
	require.True(t, sess.selectAttachmentTab(second, domain.TabStableID(secondTab.stableID)))

	d.ptys = newFactory(t, newQuietPTY())
	rc := d.attachCoordinator(sess, nil, first, true)
	d.attachCoordinator(sess, nil, second, true)
	firstToken := sess.attachmentToken(first, first.transport())
	firstToken.lease = rc.attachmentLease(first)
	first.publishAttachmentCapability(firstToken)
	secondToken := sess.attachmentToken(second, second.transport())
	secondToken.lease = rc.attachmentLease(second)
	second.publishAttachmentCapability(secondToken)

	firstView := first.viewSnapshot()
	secondView := second.viewSnapshot()
	first.sendMu.Lock()
	firstEpoch := first.output.currentEpoch()
	first.sendMu.Unlock()
	require.True(t, d.resizeAttachmentForLease(secondToken, domain.Size{Cols: 120, Rows: 40}))

	d.enterPalette(sess, second)
	d.handlePaletteInput(second, []byte("CNT\r"))

	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	require.Len(t, tabs, 3)
	wantContent := domain.Size{Cols: 120, Rows: 38}
	for _, tb := range tabs {
		tb.mu.Lock()
		gotTabSize := tb.size
		pane := tb.focusedPane()
		pane.mu.Lock()
		gotPaneSize := domain.Size{Cols: pane.screen.Frame.Width, Rows: pane.screen.Frame.Height}
		pane.mu.Unlock()
		tb.mu.Unlock()
		require.Equal(t, wantContent, gotTabSize)
		require.Equal(t, wantContent, gotPaneSize)
	}
	require.Equal(t, firstView.tabID, first.viewSnapshot().tabID, "new tab selection crossed attachment boundary")
	require.NotEqual(t, secondView.tabID, second.viewSnapshot().tabID, "palette did not select the created tab")
	first.sendMu.Lock()
	gotFirstEpoch := first.output.currentEpoch()
	first.sendMu.Unlock()
	require.Equal(t, firstEpoch, gotFirstEpoch, "new tab render crossed attachment boundary")
	second.sendMu.Lock()
	gotSecondSize := second.size
	second.sendMu.Unlock()
	require.Equal(t, domain.Size{Cols: 120, Rows: 40}, gotSecondSize)
}

func TestAcceptanceAttachmentStateIsolationAcrossResetResizeAndDetach(t *testing.T) {
	d, sess, first, _ := newManualSessionWithPTYs(t, newQuietPTY())
	first.clientID[0] = 1
	firstTab := sess.tabs[0]
	firstTab.stableID = "tab-first"
	firstTab.panes[firstTab.focusedPane().id].stableID = "pane-first"

	secondTransport, _ := newCapturingTransport(t)
	second := &attachedClient{
		tr:     secondTransport,
		output: newOutputStateStream(),
		size:   domain.Size{Cols: 80, Rows: 24},
	}
	second.initOverlays()
	second.clientID[0] = 2
	second.setSession(sess)

	secondTab := newTabWithStableID("tab-second", "pane-second", newQuietPTY(), domain.Size{Cols: 80, Rows: 23})
	secondTab.ctx, secondTab.cancel = sess.ctx, func() {}
	publishTiledPaneOwners(sess, secondTab)
	require.NoError(t, sess.runMutation(func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		sess.tabs = append(sess.tabs, secondTab)
		return nil
	}))
	require.True(t, sess.registerAttachment(second))
	require.True(t, sess.selectAttachmentTab(first, domain.TabStableID(firstTab.stableID)))
	require.True(t, sess.selectAttachmentTab(second, domain.TabStableID(secondTab.stableID)))

	firstBefore := first.viewSnapshot()
	secondBefore := second.viewSnapshot()
	require.True(t, sess.updateAttachmentView(first, func(view *attachmentView) {
		view.windowTop = 7
		view.windowRows = 12
		view.bookmark = 11
		view.liveBottom = false
	}))
	gotFirst := first.viewSnapshot()
	gotSecond := second.viewSnapshot()
	require.Equal(t, domain.TabStableID(firstTab.stableID), gotFirst.tabID)
	require.Equal(t, 7, gotFirst.windowTop)
	require.Equal(t, vt.RowID(11), gotFirst.bookmark)
	require.False(t, gotFirst.liveBottom)
	require.Equal(t, secondBefore, gotSecond, "first attachment view mutation leaked to peer")
	require.NotEqual(t, firstBefore.revision, gotFirst.revision)

	rc := d.attachCoordinator(sess, nil, first, true)
	d.attachCoordinator(sess, nil, second, true)
	firstToken := sess.attachmentToken(first, first.transport())
	firstToken.lease = rc.attachmentLease(first)
	first.publishAttachmentCapability(firstToken)
	secondToken := sess.attachmentToken(second, second.transport())
	secondToken.lease = rc.attachmentLease(second)
	second.publishAttachmentCapability(secondToken)
	firstEpoch := first.output.currentEpoch()
	secondEpoch := second.output.currentEpoch()

	d.handleAttachmentClientFrame(secondToken, ports.Frame{
		Type:    ports.MsgOutputResetRequest,
		Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{}),
	})
	require.Greater(t, second.output.currentEpoch(), secondEpoch)
	require.Equal(t, firstEpoch, first.output.currentEpoch(), "peer output reset crossed attachment boundary")

	require.True(t, d.resizeAttachmentForLease(secondToken, domain.Size{Cols: 120, Rows: 40}))
	require.Equal(t, domain.Size{Cols: 120, Rows: 40}, second.size)
	require.Equal(t, domain.Size{Cols: 80, Rows: 24}, first.size, "peer resize crossed attachment boundary")
	sess.mu.Lock()
	tb := sess.tabs[0]
	sess.mu.Unlock()
	tb.mu.Lock()
	sharedSize := tb.size
	tb.mu.Unlock()
	require.Equal(t, domain.Size{Cols: 120, Rows: 38}, sharedSize, "latest attachment resize must update shared session content")

	d.clientGone(sess, first, first.transport(), true)
	attachments := sess.snapshotAttachments()
	for _, candidate := range attachments {
		require.NotSame(t, first, candidate, "detached attachment remained registered")
	}
	var peerRegistered bool
	for _, candidate := range attachments {
		if candidate == second {
			peerRegistered = true
			break
		}
	}
	require.True(t, peerRegistered, "detaching one attachment removed its peer")
	require.Same(t, sess, second.currentSession())
}

func TestAcceptanceDirectAttachmentSupportsVerticalSplit(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	d.ptys = newFactory(t, newQuietPTY())
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.attachmentToken(ac, ac.transport())
	token.lease = rc.attachmentLease(ac)
	ac.publishAttachmentCapability(token)

	d.enterPalette(sess, ac)
	d.handlePaletteInput(ac, []byte("SPD\r"))

	sess.mu.Lock()
	tab := sess.tabs[0]
	sess.mu.Unlock()
	tab.mu.Lock()
	defer tab.mu.Unlock()
	require.NotNil(t, tab.tree)
	require.NotNil(t, tab.tree.Root)
	require.Equal(t, layout.Vertical, tab.tree.Root.Dir)
	require.Len(t, tab.panes, 2)
}
