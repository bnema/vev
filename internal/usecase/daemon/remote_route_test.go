package daemon

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestMixedAttachmentCapabilitiesKeepGraphicsAttachmentLocal(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	kittyTransport, _ := newCapturingTransport(t)
	_, kitty, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: defaultSize,
		ClientID: [16]byte{1}, KittyDirectGraphics: true,
	}, kittyTransport)
	require.NoError(t, err)
	require.NotNil(t, kitty.graphicsOutput)

	fixture, err := os.ReadFile("testdata/kitten-icat-stream-chunk.bin")
	require.NoError(t, err)
	pane := sess.tabs[0].focusedPane()
	pane.mu.Lock()
	pane.screen.Write(fixture)
	pane.mu.Unlock()

	textTransport, _ := newCapturingTransport(t)
	_, text, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: defaultSize,
		ClientID: [16]byte{2},
	}, textTransport)
	require.NoError(t, err)
	require.Nil(t, text.graphicsOutput)
	text.overlays.noticeMu.Lock()
	var warning string
	for _, toast := range text.overlays.noticeToasts {
		warning = toast.n.Message
	}
	text.overlays.noticeMu.Unlock()
	require.Contains(t, warning, "Kitty graphics are unavailable")
	d.clientGone(sess, kitty, kittyTransport, false)
	d.clientGone(sess, text, textTransport, false)
}

func TestDirectRemoteAttachSuppressesUndeclaredGraphicsBackend(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	tr, _ := newCapturingTransport(t)
	_, ac, err := d.routeWithContext(context.Background(), ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: defaultSize,
		Env: []string{"TERM=xterm-kitty", "KITTY_WINDOW_ID=1"}, Remote: true,
	}, tr)
	require.NoError(t, err)
	require.NotNil(t, ac)
	require.Nil(t, ac.graphicsOutput, "environment heuristics do not declare direct graphics")
	d.clientGone(sess, ac, tr, false)
}

func TestResumeRemoteRoutePreservesDeclaredGraphicsCapability(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	local := ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: defaultSize,
		ClientID: [16]byte{1, 2, 3, 4}, Env: []string{"TERM=xterm-kitty", "KITTY_WINDOW_ID=1"}, KittyDirectGraphics: true,
	}
	oldTransport, _ := newCapturingTransport(t)
	_, ac, err := d.route(local, oldTransport)
	require.NoError(t, err)
	require.NotNil(t, ac.graphicsOutput)
	token := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)

	remote := helloResumeCapable(ports.IntentResume, "work", token)
	remote.Remote = true
	remote.KittyDirectGraphics = true
	replacement, _ := newCapturingTransport(t)
	_, resumed, ok, err := d.resumeParked(remote, replacement, defaultSize)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, ac, resumed)
	require.NotNil(t, resumed.graphicsOutput, "remote resume must retain the declared direct graphics backend")
	require.True(t, resumed.terminalCapabilities.SupportsKittyGraphics())
	d.clientGone(sess, resumed, replacement, false)
}

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
	// The caller must hold d.mu on entry; finishRouteAttach releases it on
	// both success and error paths before returning.
	d.mu.Lock()
	_, err = d.finishRouteAttach(sess, &closeTrackingTransport{}, defaultSize, hello, true, true)
	var protocol *protoErr
	require.ErrorAs(t, err, &protocol)
	require.Equal(t, ports.ErrNoSuchTarget, protocol.code)
	d.mu.Lock()
	_, retained := d.sessions[sess.id]
	d.mu.Unlock()
	require.False(t, retained, "failed route attach must remove its newly created session")
}

func TestFinishRouteAttachPreservesConcurrentAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	sess, err := createSessionForTest(d, "work", false, "/tmp/work", defaultSize, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	winner, winnerTransport := attachWhenRouteCleanupSnapshots(t, d, sess)

	missing := domain.RemoteSessionTarget{LifecycleID: sess.incarnation, SessionName: "work", LiveTabID: "missing-tab"}
	d.mu.Lock()
	_, err = d.finishRouteAttach(sess, &closeTrackingTransport{}, defaultSize, ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: defaultSize,
		RemoteTarget: &missing, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}, true, true)

	var protocol *protoErr
	require.ErrorAs(t, err, &protocol)
	require.Equal(t, ports.ErrNoSuchTarget, protocol.code)
	require.Same(t, sess, winner().currentAttachmentSession())
	d.mu.Lock()
	require.Same(t, sess, d.sessions[sess.id])
	d.mu.Unlock()
	d.clientGone(sess, winner(), winnerTransport, false)
}

func TestFailedHandshakeCleanupPreservesConcurrentAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	sess, err := createSessionForTest(d, "work", false, "/tmp/work", defaultSize, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	initialTransport, _ := newCapturingTransport(t)
	target := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: sess.incarnation, SessionName: "work", LiveTabID: domain.TabStableID(sess.tabs[0].stableID)}
	_, initial, err := d.routeWithContext(context.Background(), ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: defaultSize,
		RemoteTarget: &target, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}, initialTransport)
	require.NoError(t, err)
	initial.routeCreatedSession = true
	initial.routeSessionPurge = true
	winner, winnerTransport := attachWhenRouteCleanupSnapshots(t, d, sess)

	d.failHandshakeAttachment(sess, initial, initialTransport, false)

	require.Same(t, sess, winner().currentAttachmentSession())
	d.mu.Lock()
	require.Same(t, sess, d.sessions[sess.id])
	d.mu.Unlock()
	d.clientGone(sess, winner(), winnerTransport, false)
}

func attachWhenRouteCleanupSnapshots(t *testing.T, d *Daemon, sess *session) (func() *attachedClient, ports.Transport) {
	t.Helper()
	winnerTransport, _ := newCapturingTransport(t)
	var winner *attachedClient
	d.afterAttachmentEffectParticipantsSnapshotted = func(string, []*attachedClient) {
		d.afterAttachmentEffectParticipantsSnapshotted = nil
		target := domain.RemoteSessionTarget{
			Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: sess.incarnation,
			SessionName: sess.name, LiveTabID: domain.TabStableID(sess.tabs[0].stableID),
		}
		var err error
		_, winner, err = d.routeWithContext(context.Background(), ports.Hello{
			Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: sess.name, Size: defaultSize,
			RemoteTarget: &target, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
		}, winnerTransport)
		require.NoError(t, err)
	}
	return func() *attachedClient {
		t.Helper()
		require.NotNil(t, winner)
		return winner
	}, winnerTransport
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
