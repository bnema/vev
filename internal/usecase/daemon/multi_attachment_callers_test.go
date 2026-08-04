package daemon

import (
	"encoding/json"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/require"
)

func TestDaemonActionTargetsUseEachInitiatingAttachmentView(t *testing.T) {
	sess := registryTestSession()
	first, second := registryTestAttachment(1), registryTestAttachment(2)
	require.True(t, sess.registerAttachment(first))
	require.True(t, sess.registerAttachment(second))
	require.True(t, sess.selectAttachmentTab(second, domain.TabStableID(sess.tabs[1].stableID)))

	firstTarget := resolveDaemonActionTargetForAttachment(sess, first)
	secondTarget := resolveDaemonActionTargetForAttachment(sess, second)
	require.Same(t, sess.tabs[0], firstTarget.tab)
	require.Same(t, sess.tabs[1], secondTarget.tab)
	require.Same(t, sess.tabs[0].focusedPane(), firstTarget.pane)
	require.Same(t, sess.tabs[1].focusedPane(), secondTarget.pane)
}

func TestAttachmentsKeepDifferentPaneFocusForActionsRenderingAndMetadata(t *testing.T) {
	sess, firstPane, secondPane := multiPaneAttachmentSession(t)
	first, second := registryTestAttachment(1), registryTestAttachment(2)
	require.True(t, sess.registerAttachment(first))
	require.True(t, sess.registerAttachment(second))
	require.True(t, sess.updateAttachmentView(second, func(view *attachmentView) {
		view.paneID = domain.PaneStableID(secondPane.stableID)
	}))

	firstTarget := resolveDaemonActionTargetForAttachment(sess, first)
	secondTarget := resolveDaemonActionTargetForAttachment(sess, second)
	require.Same(t, firstPane, firstTarget.pane)
	require.Same(t, secondPane, secondTarget.pane)

	firstState, ok := captureLocalRenderState(sess, first, renderCaptureRequest{})
	require.True(t, ok)
	secondState, ok := captureLocalRenderState(sess, second, renderCaptureRequest{})
	require.True(t, ok)
	require.Equal(t, firstPane.id, firstState.layout.focus)
	require.Equal(t, secondPane.id, secondState.layout.focus)

	d := newTestDaemon(t, nil, stubClock{})
	var rows []struct {
		Pane    string `json:"pane"`
		Focused bool   `json:"focused"`
	}
	output, err := (controlExec{d: d, sess: sess, tab: sess.tabs[0], target: daemonActionTarget{attachment: second}}).ListPanes(true)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(output), &rows))
	for _, row := range rows {
		require.Equal(t, row.Pane == string(secondPane.id), row.Focused)
	}
}

func TestAttachmentFocusActionUpdatesOnlyInitiatingPaneView(t *testing.T) {
	sess, firstPane, secondPane := multiPaneAttachmentSession(t)
	first, second := registryTestAttachment(1), registryTestAttachment(2)
	require.True(t, sess.registerAttachment(first))
	require.True(t, sess.registerAttachment(second))

	d := newTestDaemon(t, nil, stubClock{})
	require.NoError(t, d.focusDir(sess, second, layout.Right, nil))
	require.Equal(t, domain.PaneStableID(firstPane.stableID), first.viewSnapshot().paneID)
	require.Equal(t, domain.PaneStableID(secondPane.stableID), second.viewSnapshot().paneID)
}

func TestPaneRemovalRepairsEachAttachmentViewIndependently(t *testing.T) {
	sess, firstPane, secondPane := multiPaneAttachmentSession(t)
	first, second := registryTestAttachment(1), registryTestAttachment(2)
	require.True(t, sess.registerAttachment(first))
	require.True(t, sess.registerAttachment(second))
	require.True(t, sess.updateAttachmentView(second, func(view *attachmentView) {
		view.paneID = domain.PaneStableID(secondPane.stableID)
	}))

	require.NoError(t, sess.runMutation(func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		tb := sess.tabs[0]
		tb.mu.Lock()
		closeErr := tb.tree.Close(firstPane.id)
		if closeErr == nil {
			delete(tb.panes, firstPane.id)
			tb.bumpLayoutGenerationLocked()
		}
		tb.mu.Unlock()
		if closeErr != nil {
			return closeErr
		}
		sess.repairAttachmentViewsLocked()
		return nil
	}))

	require.Equal(t, domain.PaneStableID(secondPane.stableID), first.viewSnapshot().paneID)
	require.Equal(t, domain.PaneStableID(secondPane.stableID), second.viewSnapshot().paneID)
}

func multiPaneAttachmentSession(t *testing.T) (*session, *pane, *pane) {
	t.Helper()
	sess := registryTestSession()
	tb := sess.tabs[0]
	firstPane := tb.panes[layout.PaneID("pane-1")]
	secondPane := newPaneWithStableID("pane-2", "stable-pane-b", nil, domain.Size{Cols: 80, Rows: 23})
	tb.mu.Lock()
	tb.panes[secondPane.id] = secondPane
	tb.tree = &layout.Tree{
		Root:  &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(firstPane.id), layout.NewLeaf(secondPane.id)}},
		Focus: firstPane.id,
	}
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()
	return sess, firstPane, secondPane
}
