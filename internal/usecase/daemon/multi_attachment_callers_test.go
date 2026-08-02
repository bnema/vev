package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
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
