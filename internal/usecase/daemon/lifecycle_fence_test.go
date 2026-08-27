package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestValidateAttachmentTransitionLocksSourceCoordinatorBeforeLeaseValidation(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	transport := &closeTrackingTransport{}
	source := &session{sessionCore: sessionCore{id: domain.SessionID("source")}}
	target := &session{sessionCore: sessionCore{id: domain.SessionID("target")}}
	ac := &attachedClient{tr: transport}
	ac.setSession(source)
	source.mu.Lock()
	registered := source.registerAttachmentLocked(ac)
	source.mu.Unlock()
	require.True(t, registered)
	sourceCoordinator := newRenderCoordinator(renderCoordinatorOptions{})
	source.installRenderCoordinator(sourceCoordinator)
	sourceCoordinator.attach(ac)
	token := source.captureAttachmentCapability(ac, transport)
	require.NotNil(t, token.lease)
	d.sessions[source.id] = source
	d.sessions[target.id] = target

	checked := false
	coordinatorUnlocked := false
	d.afterAttachmentTransitionCoordinatorsLocked = func() {
		coordinatorUnlocked = sourceCoordinator.mu.TryLock()
		if coordinatorUnlocked {
			sourceCoordinator.mu.Unlock()
		}
		checked = true
	}
	req := attachmentTransitionRequest{
		source: source, target: target, next: ac,
		expectedTransport: ac.transportSnapshot(), sourceCapability: &token,
		preflighted: true, attachmentEffectsFrozen: true,
	}
	d.mu.Lock()
	d.notices.routingMu.Lock()
	unlockSessions := lockAttachmentSessions(source, target)
	publication, err := d.validateAttachmentTransitionPrelocked(req)
	unlockSessions()
	d.notices.routingMu.Unlock()
	d.mu.Unlock()
	if publication != nil {
		publication.unlockCoordinators()
	}
	require.NoError(t, err)
	require.False(t, coordinatorUnlocked, "source coordinator was not held during lease validation")
	require.True(t, checked)
	require.NotNil(t, publication)
}

func TestAttachmentCapabilityRevalidatesTheCapturedTransportIncarnation(t *testing.T) {
	first := &closeTrackingTransport{}
	second := &closeTrackingTransport{}
	ac := &attachedClient{tr: first}
	sess := &session{sessionCore: sessionCore{id: domain.SessionID("work")}}
	ac.setSession(sess)
	sess.mu.Lock()
	registered := sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	require.True(t, registered)

	captured := make(chan struct{})
	release := make(chan struct{})
	ac.beforeAttachmentCapabilityValidation = func() {
		close(captured)
		<-release
	}
	result := make(chan attachmentCapability, 1)
	go func() { result <- sess.captureAttachmentCapability(ac, first) }()
	<-captured
	ac.replaceTransport(second)
	close(release)

	token := <-result
	require.Nil(t, token.ac, "a token must not bind a replacement transport")
	require.Equal(t, uint64(1), ac.transportSnapshot().incarnation)
	require.Same(t, second, ac.transport())
}
