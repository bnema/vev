package daemon

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRouteExactSessionTargetSelectsLifecycle(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = newTestLifecycle(t)
	tr, _ := newCapturingTransport(t)
	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	hello := protocol.Hello{
		Version:     protocol.Version,
		Intent:      protocol.IntentAttach,
		Name:        target.SessionName,
		Size:        domain.Size{Cols: 80, Rows: 24},
		ExactTarget: &target,
	}

	routed, ac, err := d.routeWithContext(context.Background(), hello, tr)
	require.NoError(t, err)
	require.Same(t, sess, routed)
	require.Same(t, sess, ac.currentAttachmentSession())
	d.clientGone(sess, ac, tr, false)
}

func TestStoppedSessionAttachTargetAcceptsSameLifecycleNowLive(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = newTestLifecycle(t)
	tr, _ := newCapturingTransport(t)
	target := protocol.SessionAttachTarget{LifecycleID: sess.incarnation, SessionName: sess.name, TabID: domain.TabStableID(sess.tabs[0].stableID), TabIndex: protocol.NoTabIndex, Stopped: true}
	hello := protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Name: sess.name, Size: domain.Size{Cols: 80, Rows: 24}, SessionTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}

	routed, ac, err := d.routeWithContext(context.Background(), hello, tr)
	require.NoError(t, err)
	require.Same(t, sess, routed)
	d.mu.Lock()
	require.Len(t, d.sessions, 1)
	d.mu.Unlock()
	d.clientGone(sess, ac, tr, false)
}

func TestSessionAttachTargetRejectsLifecycleReplacement(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.incarnation = newTestLifecycle(t)
	stale := sess.incarnation
	sess.incarnation[0]++
	tr, _ := newCapturingTransport(t)
	target := protocol.SessionAttachTarget{LifecycleID: stale, SessionName: sess.name, TabID: domain.TabStableID(sess.tabs[0].stableID), TabIndex: protocol.NoTabIndex}
	hello := protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Name: sess.name, Size: domain.Size{Cols: 80, Rows: 24}, SessionTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}

	_, _, err := d.routeWithContext(context.Background(), hello, tr)
	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, protocol.ErrNoSuchTarget, protocolErr.code)
}

func TestRouteExactSessionTargetRejectsNameMismatch(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = newTestLifecycle(t)
	tr, _ := newCapturingTransport(t)
	hello := protocol.Hello{
		Version:     protocol.Version,
		Intent:      protocol.IntentAttach,
		Name:        "other",
		Size:        domain.Size{Cols: 80, Rows: 24},
		ExactTarget: &protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name},
	}

	_, _, err := d.routeWithContext(context.Background(), hello, tr)
	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, protocol.ErrNoSuchSession, protocolErr.code)
}

func TestLockedExactSessionTargetRejectsReplacement(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = newTestLifecycle(t)
	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	replacement := sess.incarnation
	replacement[0]++
	sess.incarnation = replacement

	d.mu.Lock()
	err := d.validateExactSessionTargetLocked(target)
	d.mu.Unlock()

	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, protocol.ErrNoSuchSession, protocolErr.code)
}

func TestRouteExactSessionTargetRejectsLifecycleReplacement(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = newTestLifecycle(t)
	tr, _ := newCapturingTransport(t)
	wrong := sess.incarnation
	wrong[0]++
	hello := protocol.Hello{
		Version:     protocol.Version,
		Intent:      protocol.IntentAttach,
		Name:        sess.name,
		Size:        domain.Size{Cols: 80, Rows: 24},
		ExactTarget: &protocol.ExactSessionTarget{LifecycleID: wrong, SessionName: sess.name},
	}

	_, _, err := d.routeWithContext(context.Background(), hello, tr)
	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, protocol.ErrNoSuchSession, protocolErr.code)
}
