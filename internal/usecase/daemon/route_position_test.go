package daemon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

func TestRouteAppliesPreferredTabPerAttachment(t *testing.T) {
	for _, test := range []struct {
		name      string
		preferred domain.TabStableID
		want      domain.TabStableID
	}{
		{name: "existing preferred tab", preferred: "tab-2", want: "tab-2"},
		{name: "missing preferred tab falls back", preferred: "missing", want: "tab-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			sess := addControlSession(d, "work", "tab-1", "pane-1")
			sess.incarnation = domain.SessionLifecycleID{1}
			second := newTabWithStableID("tab-2", "pane-2", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
			sess.mu.Lock()
			sess.tabs = append(sess.tabs, second)
			sess.mu.Unlock()
			tr := &closeTrackingTransport{}
			target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
			hello := protocol.Hello{
				Version: protocol.Version, Intent: protocol.IntentAttach, Name: sess.name,
				Size: defaultSize, ExactTarget: &target, PreferredTabID: test.preferred,
				EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
			}

			_, ac, err := d.routeWithContext(context.Background(), hello, tr)
			require.NoError(t, err)
			sess.repairAttachmentView(ac)
			require.Equal(t, test.want, ac.viewSnapshot().tabID)
			d.clientGone(sess, ac, tr, false)
		})
	}
}

func TestResumeAppliesPreferredTabToParkedAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	oldTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	second := newTabWithStableID("tab-2", "pane-2", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, second)
	sess.mu.Unlock()
	sess.repairAttachmentView(ac)
	token := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)

	hello := helloResumeCapable(protocol.IntentResume, "work", token)
	hello.PreferredTabID = "tab-2"
	newTransport := &closeTrackingTransport{}
	resumedSession, resumed, err := d.route(hello, newTransport)
	require.NoError(t, err)
	require.Same(t, ac, resumed)
	require.Equal(t, domain.TabStableID("tab-2"), resumed.viewSnapshot().tabID)
	d.clientGone(resumedSession, resumed, newTransport, false)
}

func TestPaintPublishesChangedAttachmentRoutePosition(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.incarnation = domain.SessionLifecycleID{1}
	second := newTabWithStableID("tab-2", "pane-2", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, second)
	sess.mu.Unlock()
	tr := &closeTrackingTransport{}
	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	hello := protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: sess.name,
		Size: defaultSize, ExactTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	}
	_, ac, err := d.routeWithContext(context.Background(), hello, tr)
	require.NoError(t, err)
	rc := sess.renderCoordinator()
	require.True(t, rc.markAttachmentReady(rc.attachmentLease(ac)))
	sess.repairAttachmentView(ac)

	require.True(t, d.firstPaintForTransition(sess.captureAttachmentCapability(ac, tr)))
	sess.selectAttachmentTab(ac, "tab-2")
	d.paint(sess, ac, false, nil)

	var positions []protocol.RoutePosition
	for _, frame := range tr.Sends() {
		if frame.Type != ports.MsgRoutePosition {
			continue
		}
		position, err := ports.UnmarshalRoutePosition(frame.Payload)
		require.NoError(t, err)
		positions = append(positions, position)
	}
	require.Equal(t, []protocol.RoutePosition{
		{Target: target, ActiveTabID: "tab-1"},
		{Target: target, ActiveTabID: "tab-2"},
	}, positions)
	d.clientGone(sess, ac, tr, false)
}
