package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestAttachmentTransitionPrelockedMatchesWrapper(t *testing.T) {
	t.Parallel()

	type transitionObservation struct {
		result              attachmentTransitionResult
		activeRole          attachmentRole
		displacedRole       attachmentRole
		activeGeneration    uint64
		displacedGeneration uint64
		activeSession       *session
		displacedSession    *session
	}

	setup := func(t *testing.T) (*Daemon, *session, *attachedClient, *attachedClient) {
		t.Helper()
		d := newTestDaemon(t, nil, stubClock{})
		sess := &session{sessionCore: sessionCore{id: domain.SessionID("work")}}
		old := &attachedClient{tr: &closeTrackingTransport{}}
		old.setSession(sess)
		sess.client = old
		d.sessions[sess.id] = sess
		next := &attachedClient{tr: &closeTrackingTransport{}}
		return d, sess, old, next
	}
	observe := func(sess *session, old, next *attachedClient, result attachmentTransitionResult) transitionObservation {
		return transitionObservation{
			result:              result,
			activeRole:          sess.attachmentRole(next),
			displacedRole:       sess.attachmentRole(old),
			activeGeneration:    next.roleGeneration.Load(),
			displacedGeneration: old.roleGeneration.Load(),
			activeSession:       next.currentSession(),
			displacedSession:    old.currentSession(),
		}
	}

	wrapperDaemon, wrapperSession, wrapperOld, wrapperNext := setup(t)
	wrapperResult, err := wrapperDaemon.transitionAttachment(attachmentTransitionRequest{
		target:            wrapperSession,
		next:              wrapperNext,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: wrapperNext.transportSnapshot(),
		ready:             true,
	})
	require.NoError(t, err)
	wrapper := observe(wrapperSession, wrapperOld, wrapperNext, wrapperResult)

	prelockedDaemon, prelockedSession, prelockedOld, prelockedNext := setup(t)
	frozen := freezeRoleEffectGates(prelockedNext, prelockedOld)
	require.True(t, frozen.acquired)
	require.True(t, frozen.drained)
	defer frozen.unfreeze()

	req := attachmentTransitionRequest{
		target:                prelockedSession,
		next:                  prelockedNext,
		expectedRole:          attachmentDetached,
		targetRole:            attachmentActive,
		expectedTransport:     prelockedNext.transportSnapshot(),
		expectedTargetCurrent: prelockedOld,
		preflighted:           true,
		roleEffectsFrozen:     true,
		ready:                 true,
	}
	type prelockedOutcome struct {
		result attachmentTransitionResult
		err    error
	}
	outcome := make(chan prelockedOutcome, 1)
	go func() {
		prelockedDaemon.mu.Lock()
		prelockedDaemon.notices.routingMu.Lock()
		unlockSessions := lockAttachmentSessions(prelockedSession, prelockedSession)
		publication, validateErr := prelockedDaemon.validateAttachmentTransitionPrelocked(req)
		var result attachmentTransitionResult
		if validateErr == nil {
			result = prelockedDaemon.publishAttachmentTransitionPrelocked(publication)
			publication.unlockCoordinators()
		}
		unlockSessions()
		prelockedDaemon.notices.routingMu.Unlock()
		prelockedDaemon.mu.Unlock()
		outcome <- prelockedOutcome{result: result, err: validateErr}
	}()

	var prelockedResult attachmentTransitionResult
	select {
	case got := <-outcome:
		require.NoError(t, got.err)
		prelockedResult = got.result
	case <-time.After(time.Second):
		t.Fatal("prelocked attachment transition recursively acquired an already-held lock")
	}
	prelocked := observe(prelockedSession, prelockedOld, prelockedNext, prelockedResult)

	require.Equal(t, wrapper.activeRole, prelocked.activeRole)
	require.Equal(t, wrapper.displacedRole, prelocked.displacedRole)
	require.Equal(t, wrapper.activeGeneration, prelocked.activeGeneration)
	require.Equal(t, wrapper.displacedGeneration, prelocked.displacedGeneration)
	require.Same(t, prelockedSession, prelocked.activeSession)
	require.Same(t, prelockedSession, prelocked.displacedSession)
	require.Equal(t, wrapper.result.published.role, prelocked.result.published.role)
	require.Equal(t, wrapper.result.displaced.role, prelocked.result.displaced.role)
	require.NotNil(t, prelocked.result.published.lease)
}

func TestValidateAttachmentTransitionPrelockedLeavesMembershipUntouchedOnFailure(t *testing.T) {
	t.Parallel()

	d := newTestDaemon(t, nil, stubClock{})
	source := &session{sessionCore: sessionCore{id: domain.SessionID("source")}}
	target := &session{sessionCore: sessionCore{id: domain.SessionID("target")}, active: 0}
	next := &attachedClient{tr: &closeTrackingTransport{}}
	next.setSession(source)
	source.addSnatchedLocked(next)
	old := &attachedClient{tr: &closeTrackingTransport{}}
	old.setSession(target)
	target.client = old
	d.sessions[source.id] = source
	d.sessions[target.id] = target

	frozen := freezeRoleEffectGates(next, old)
	require.True(t, frozen.acquired)
	require.True(t, frozen.drained)
	defer frozen.unfreeze()

	req := attachmentTransitionRequest{
		source:                source,
		target:                target,
		next:                  next,
		expectedRole:          attachmentSnatched,
		targetRole:            attachmentActive,
		expectedTransport:     next.transportSnapshot(),
		expectedTargetCurrent: old,
		preflighted:           true,
		roleEffectsFrozen:     true,
		activateTargetTab:     true,
		targetTabIndex:        1,
	}

	d.mu.Lock()
	d.notices.routingMu.Lock()
	unlockSessions := lockAttachmentSessions(source, target)
	publication, err := d.validateAttachmentTransitionPrelocked(req)
	unlockSessions()
	d.notices.routingMu.Unlock()
	d.mu.Unlock()

	require.ErrorIs(t, err, errAttachmentTransition)
	require.Nil(t, publication)
	require.Equal(t, attachmentSnatched, source.attachmentRole(next))
	require.Equal(t, attachmentDetached, target.attachmentRole(next))
	require.Equal(t, attachmentActive, target.attachmentRole(old))
	require.Same(t, source, next.currentSession())
	require.Equal(t, uint64(0), next.roleGeneration.Load())
	require.Equal(t, uint64(0), old.roleGeneration.Load())
	require.Equal(t, 0, target.active)
}
