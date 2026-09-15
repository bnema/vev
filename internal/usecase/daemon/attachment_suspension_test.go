package daemon

import (
	"io"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAttachmentSuspensionRevokesEffectsAndGeometry(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, ac.transport())
	ac.installTestAttachmentCapability(token)
	_, ok := sess.geometry.claimAttachment(sess, ac)
	require.True(t, ok)
	require.NoError(t, d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 1}))
	require.Nil(t, sess.geometry.latest.Load())
	require.Nil(t, rc.attachmentLease(ac))
	current := sess.captureAttachmentCapability(ac, ac.transport())
	_, admitted := ac.beginAttachmentEffect(current)
	require.False(t, admitted)
	require.Equal(t, paintRejected, d.paint(sess, ac, true, nil))
	require.True(t, ac.currentTransportIs(token.transport.transport))
}

func TestAttachmentActivationExactLifecycle(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained", true: "stale"}[stale], func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
			d.attachCoordinator(sess, nil, ac, true)
			token := sess.captureAttachmentCapability(ac, ac.transport())
			ac.installTestAttachmentCapability(token)
			require.NoError(t, d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 1}))
			target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
			if stale {
				target.LifecycleID[0] ^= 1
			}
			err := d.activateAttachment(ac, ac.transportSnapshot(), protocol.ActivateAttachment{RequestID: 2, Target: target, Size: domain.Size{Cols: 100, Rows: 30}})
			if stale {
				require.Error(t, err)
				require.Nil(t, sess.geometry.latest.Load())
				return
			}
			require.NoError(t, err)
			require.Equal(t, ac, sess.geometry.latest.Load().attachment)
			require.Equal(t, 100, ac.geometrySnapshot().Cols)
			current := sess.captureAttachmentCapability(ac, ac.transport())
			effect, admitted := ac.beginAttachmentEffect(current)
			require.True(t, admitted)
			effect.End()
		})
	}
}

func TestAttachmentSuspensionBarrierDrainsAdmittedEffects(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, ac.transport())
	ac.installTestAttachmentCapability(token)
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	frozen := make(chan struct{})
	d.afterAttachmentEffectGateFrozen = func(action string, _ *attachedClient) {
		if action == "suspend" {
			close(frozen)
		}
	}
	result := make(chan error, 1)
	go func() { result <- d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 7}) }()
	<-frozen
	select {
	case <-sends:
		t.Fatal("ACK before effect drained")
	default:
	}
	_, admitted = ac.beginAttachmentEffect(token)
	require.False(t, admitted)
	require.NoError(t, effect.sendControl(protocol.Pong{}))
	effect.End()
	require.NoError(t, <-result)
	require.IsType(t, protocol.Pong{}, decodeServerMessage(t, <-sends))
	ack := decodeServerMessage(t, <-sends).(protocol.AttachmentSuspended)
	require.Equal(t, uint64(7), ack.RequestID)
}

func TestAttachmentActivationPublicationPrecedesSuccess(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, ac.transport())
	ac.installTestAttachmentCapability(token)
	require.NoError(t, d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 1}))
	suspended := decodeServerMessage(t, <-sends).(protocol.AttachmentSuspended)
	require.NoError(t, d.activateAttachment(ac, ac.transportSnapshot(), protocol.ActivateAttachment{RequestID: 2, Target: suspended.Target, Size: ac.sizeSnapshot()}))
	var full *protocol.Output
	var position *protocol.RoutePosition
	for len(sends) != 0 {
		switch m := decodeServerMessage(t, <-sends).(type) {
		case protocol.Output:
			require.Nil(t, full)
			require.True(t, m.Full)
			require.Zero(t, m.Base)
			require.NotNil(t, m.Context)
			full = &m
		case protocol.RoutePosition:
			require.NotNil(t, full, "route position must follow full publication")
			require.Nil(t, position)
			position = &m
		case protocol.AttachmentActivated:
			require.NotNil(t, full, "activation success must follow full publication")
			require.NotNil(t, position, "activation success must follow route position")
			require.Equal(t, full.Epoch, m.Epoch)
			require.Equal(t, full.New, m.State)
			require.Equal(t, full.Context.Publication, m.ViewPublication)
			require.Equal(t, full.Context.Route, m.Identity)
			return
		}
	}
	t.Fatal("missing activation success")
}

func TestAttachmentSuspensionCloseDoesNotPark(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	ac.resumeCapable, ac.resumeToken = true, 123
	d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, ac.transport())
	ac.installTestAttachmentCapability(token)
	require.NoError(t, d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 1}))
	d.clientGone(sess, ac, token.transport.transport, false)
	require.Nil(t, ac.currentAttachmentSession())
	require.Empty(t, d.parked)
	require.False(t, ac.parked)
}

func TestAttachmentSuspensionPeerGeometryFallbackAndReclaim(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	peerTransport, _ := newCapturingTransport(t)
	peer, err := d.attachClient(sess, peerTransport, domain.Size{Cols: 60, Rows: 20}, attachClientOptions{})
	require.NoError(t, err)
	d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, ac.transport())
	ac.installTestAttachmentCapability(token)
	_, ok := sess.geometry.claimAttachment(sess, ac)
	require.True(t, ok)
	require.NoError(t, d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 1}))
	require.Equal(t, peer, sess.geometry.latest.Load().attachment)
	peerToken := sess.captureAttachmentCapability(peer, peer.transport())
	effect, admitted := peer.beginAttachmentEffect(peerToken)
	require.True(t, admitted)
	effect.End()
	require.NoError(t, d.activateAttachment(ac, ac.transportSnapshot(), protocol.ActivateAttachment{RequestID: 2, Target: protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}, Size: ac.sizeSnapshot()}))
	require.Equal(t, ac, sess.geometry.latest.Load().attachment)
}

func TestAttachmentSuspensionReaderKeepsControlAlive(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	tr := newMockServerConnection(t)
	incoming := make(chan protocol.ClientMessage, 4)
	incoming <- protocol.SuspendAttachment{RequestID: 1}
	incoming <- protocol.Input{Data: []byte("ignored"), InputSeq: 1}
	incoming <- protocol.Ping{}
	close(incoming)
	tr.EXPECT().Recv().RunAndReturn(func() (wire.Envelope, error) {
		m, ok := <-incoming
		if !ok {
			return wire.Envelope{}, io.EOF
		}
		return mustClientEnvelope(m), nil
	})
	var messages []protocol.ServerMessage
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error {
		messages = append(messages, decodeServerMessage(t, f))
		return nil
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	ac.replaceTransport(tr)
	ac.resumeCapable, ac.resumeToken = true, 123
	d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, tr)
	ac.installTestAttachmentCapability(token)
	d.runConnLoop(ac)
	require.IsType(t, protocol.AttachmentSuspended{}, messages[0])
	require.IsType(t, protocol.Pong{}, messages[1])
	require.Zero(t, ac.echoAck.Load())
	require.Nil(t, ac.currentAttachmentSession())
	require.Empty(t, d.parked)
}

func TestAttachmentActivationCloseDuringFullPublication(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	tr := newMockServerConnection(t)
	var activated bool
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error {
		switch decodeServerMessage(t, f).(type) {
		case protocol.Output:
			return io.ErrClosedPipe
		case protocol.AttachmentActivated:
			activated = true
		}
		return nil
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	ac.replaceTransport(tr)
	ac.resumeCapable, ac.resumeToken = true, 123
	d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, tr)
	ac.installTestAttachmentCapability(token)
	require.False(t, d.handleAttachmentClientMessage(token, protocol.SuspendAttachment{RequestID: 1}))
	token = sess.captureAttachmentCapability(ac, tr)
	require.True(t, d.handleAttachmentClientMessage(token, protocol.ActivateAttachment{RequestID: 2, Target: protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}, Size: ac.sizeSnapshot()}))
	d.attachmentCleanupWg.Wait()
	require.False(t, activated)
	require.Nil(t, ac.currentAttachmentSession())
	require.Empty(t, d.parked)
}
