package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func (ac *attachedClient) installTestAttachmentCapability(capability attachmentCapability) {
	ac.lifecycle.mu.Lock()
	defer ac.lifecycle.mu.Unlock()
	ac.lifecycle.initLocked()
	if ac.lifecycle.phase != attachmentEffectsStable || ac.lifecycle.inFlight != 0 {
		panic("daemon: installing an active test attachment capability")
	}
	ac.lifecycle.capability = capability
	ac.lifecycle.failedTransport = transportSnapshot{}
}

func (s *session) detachIfCurrent(ac *attachedClient) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !attachmentRegisteredLocked(s, ac) || ac.currentAttachmentSession() != s {
		return false
	}
	s.unregisterAttachmentLocked(ac)
	ac.setSession(nil)
	ac.invalidateRetiredAttachmentCapability()
	return true
}

func TestAttachmentCapabilityRejectsIndependentIdentityChanges(t *testing.T) {
	tests := []struct {
		name   string
		change func(*session, *attachedClient, *renderCoordinator)
	}{
		{
			name: "session membership removal",
			change: func(sess *session, ac *attachedClient, _ *renderCoordinator) {
				sess.mu.Lock()
				delete(sess.attachments, ac)
				sess.mu.Unlock()
			},
		},
		{
			name: "attachment session replacement",
			change: func(_ *session, ac *attachedClient, _ *renderCoordinator) {
				ac.setSession(&session{})
			},
		},
		{
			name: "connection generation increment",
			change: func(_ *session, ac *attachedClient, _ *renderCoordinator) {
				ac.lifecycle.generation.Add(1)
			},
		},
		{
			name: "transport replacement",
			change: func(_ *session, ac *attachedClient, _ *renderCoordinator) {
				ac.replaceTransport(&closeTrackingTransport{})
			},
		},
		{
			name: "transport incarnation increment",
			change: func(_ *session, ac *attachedClient, _ *renderCoordinator) {
				ac.replaceTransport(ac.transport())
			},
		},
		{
			name: "render lease rebind",
			change: func(_ *session, ac *attachedClient, rc *renderCoordinator) {
				rc.mu.Lock()
				rc.rebindAttachmentWithReadinessLocked(ac, true)
				rc.mu.Unlock()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
			transport := &closeTrackingTransport{}
			ac.replaceTransport(transport)
			rc := d.attachCoordinator(sess, nil, ac, true)
			capability := sess.captureAttachmentCapability(ac, transport)
			capability.lease = rc.attachmentLease(ac)
			ac.installTestAttachmentCapability(capability)

			require.True(t, capability.current())
			tt.change(sess, ac, rc)
			require.False(t, capability.current())
			effect, admitted := ac.beginAttachmentEffect(capability)
			require.False(t, admitted)
			require.Nil(t, effect)
		})
	}
}

func TestAttachmentCapabilityInvalidationRejectsNewEffects(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(sess, nil, ac, true)
	capability := sess.captureAttachmentCapability(ac, transport)
	capability.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(capability)

	frozen := freezeAttachmentEffectGates(ac)
	require.True(t, frozen.acquired)
	require.True(t, frozen.drained)
	ac.invalidateFrozenAttachmentCapability()
	frozen.unfreeze()

	require.False(t, capability.current())
	_, admitted := ac.beginAttachmentEffect(capability)
	require.False(t, admitted)
}
