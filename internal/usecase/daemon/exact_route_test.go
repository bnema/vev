package daemon

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRouteExactSessionTargetSelectsLifecycle(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	tr, _ := newCapturingTransport(t)
	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	hello := ports.Hello{
		Version:     ports.ProtocolVersion,
		Intent:      ports.IntentAttach,
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

func TestRouteExactSessionTargetRejectsNameMismatch(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	tr, _ := newCapturingTransport(t)
	hello := ports.Hello{
		Version:     ports.ProtocolVersion,
		Intent:      ports.IntentAttach,
		Name:        "other",
		Size:        domain.Size{Cols: 80, Rows: 24},
		ExactTarget: &protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name},
	}

	_, _, err := d.routeWithContext(context.Background(), hello, tr)
	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, ports.ErrNoSuchSession, protocolErr.code)
}

func TestLockedExactSessionTargetRejectsReplacement(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	replacement := sess.incarnation
	replacement[0]++
	sess.incarnation = replacement

	d.mu.Lock()
	err := d.validateExactSessionTargetLocked(target)
	d.mu.Unlock()

	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, ports.ErrNoSuchSession, protocolErr.code)
}

func TestRouteExactSessionTargetRejectsLifecycleReplacement(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	tr, _ := newCapturingTransport(t)
	wrong := sess.incarnation
	wrong[0]++
	hello := ports.Hello{
		Version:     ports.ProtocolVersion,
		Intent:      ports.IntentAttach,
		Name:        sess.name,
		Size:        domain.Size{Cols: 80, Rows: 24},
		ExactTarget: &protocol.ExactSessionTarget{LifecycleID: wrong, SessionName: sess.name},
	}

	_, _, err := d.routeWithContext(context.Background(), hello, tr)
	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, ports.ErrNoSuchSession, protocolErr.code)
}
