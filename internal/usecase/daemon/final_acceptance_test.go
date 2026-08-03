package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, domain.Size{Cols: 80, Rows: 23}, sess.tabs[0].size, "attachment resize mutated shared session content")
	sess.mu.Unlock()

	d.clientGone(sess, first, first.transport(), true)
	require.NotContains(t, sess.snapshotAttachments(), first)
	require.Contains(t, sess.snapshotAttachments(), second, "detaching one attachment removed its peer")
	require.Same(t, sess, second.currentSession())
}
