package daemon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestRouteRemoteTargetSelectsExactLiveTab(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	tr, _ := newCapturingTransport(t)
	target := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: sess.incarnation, SessionName: "work", LiveTabID: "tab-1"}
	hello := ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: &target, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned}
	_, ac, err := d.routeWithContext(context.Background(), hello, tr)
	require.NoError(t, err)
	require.NotNil(t, ac)
	require.Same(t, sess, ac.currentAttachmentSession())
	require.Equal(t, domain.TabStableID("tab-1"), ac.viewSnapshot().tabID)
	d.clientGone(sess, ac, tr, false)
}

func TestFinishRouteAttachRollsBackCreatedSession(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	sess, err := createSessionForTest(d, "work", false, "/tmp/work", defaultSize, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	target := domain.RemoteSessionTarget{LifecycleID: sess.incarnation, SessionName: "work", LiveTabID: "missing-tab"}
	hello := ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: defaultSize,
		RemoteTarget: &target, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}
	d.mu.Lock()
	_, err = d.finishRouteAttach(sess, &closeTrackingTransport{}, defaultSize, terminalEnv{}, hello, true, true)
	var protocol *protoErr
	require.ErrorAs(t, err, &protocol)
	require.Equal(t, ports.ErrNoSuchTarget, protocol.code)
	d.mu.Lock()
	_, retained := d.sessions[sess.id]
	d.mu.Unlock()
	require.False(t, retained, "failed route attach must remove its newly created session")
}

func TestRouteRemoteTargetRejectsSameNameReplacement(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-new", "pane-new")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	tr, _ := newCapturingTransport(t)
	var old domain.SessionLifecycleID
	old[0] = 8
	target := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: old, SessionName: "work", LiveTabID: "tab-new"}
	hello := ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: &target, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned}
	_, _, err := d.routeWithContext(context.Background(), hello, tr)
	var protocol *protoErr
	require.ErrorAs(t, err, &protocol)
	require.Equal(t, ports.ErrNoSuchTarget, protocol.code)
}
