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
	require.True(t, source.registerAttachmentLocked(ac))
	source.mu.Unlock()
	sourceCoordinator := newRenderCoordinator(renderCoordinatorOptions{})
	source.installRenderCoordinator(sourceCoordinator)
	sourceCoordinator.attach(ac)
	token := source.attachmentToken(ac, transport)
	require.NotNil(t, token.lease)
	d.sessions[source.id] = source
	d.sessions[target.id] = target

	checked := false
	d.afterAttachmentTransitionCoordinatorsLocked = func() {
		if sourceCoordinator.mu.TryLock() {
			sourceCoordinator.mu.Unlock()
			t.Fatalf("source coordinator was not held during lease validation")
		}
		checked = true
	}
	req := attachmentTransitionRequest{
		source: source, target: target, next: ac,
		expectedTransport: ac.transportSnapshot(), sourceToken: &token,
		preflighted: true, attachmentEffectsFrozen: true,
	}
	d.mu.Lock()
	d.notices.routingMu.Lock()
	unlockSessions := lockAttachmentSessions(source, target)
	publication, err := d.validateAttachmentTransitionPrelocked(req)
	unlockSessions()
	d.notices.routingMu.Unlock()
	d.mu.Unlock()
	require.NoError(t, err)
	require.True(t, checked)
	require.NotNil(t, publication)
	publication.unlockCoordinators()
}

func TestAttachmentTokenRevalidatesTheCapturedTransportIncarnation(t *testing.T) {
	first := &closeTrackingTransport{}
	second := &closeTrackingTransport{}
	ac := &attachedClient{tr: first}
	sess := &session{sessionCore: sessionCore{id: domain.SessionID("work")}}
	ac.setSession(sess)
	sess.mu.Lock()
	require.True(t, sess.registerAttachmentLocked(ac))
	sess.mu.Unlock()

	captured := make(chan struct{})
	release := make(chan struct{})
	ac.beforeAttachmentTokenValidation = func() {
		close(captured)
		<-release
	}
	result := make(chan attachmentConnectionToken, 1)
	go func() { result <- sess.attachmentToken(ac, first) }()
	<-captured
	ac.replaceTransport(second)
	close(release)

	token := <-result
	require.Nil(t, token.ac, "a token must not bind a replacement transport")
	require.Equal(t, uint64(1), ac.transportSnapshot().incarnation)
	require.Same(t, second, ac.transport())
}
