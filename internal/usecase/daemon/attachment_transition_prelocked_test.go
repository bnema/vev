package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestValidateAttachmentTransitionPrelockedLeavesMembershipUntouchedOnFailure(t *testing.T) {
	t.Parallel()

	d := newTestDaemon(t, nil, stubClock{})
	source := &session{sessionCore: sessionCore{id: domain.SessionID("source")}}
	target := &session{sessionCore: sessionCore{id: domain.SessionID("target")}}
	next := &attachedClient{tr: &closeTrackingTransport{}}
	next.setSession(source)
	source.registerAttachmentLocked(next)
	old := &attachedClient{tr: &closeTrackingTransport{}}
	old.setSession(target)
	target.registerAttachment(old)
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
		expectedRole:          attachmentActive,
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
	require.Equal(t, attachmentActive, source.attachmentRole(next))
	require.Equal(t, attachmentDetached, target.attachmentRole(next))
	require.Equal(t, attachmentActive, target.attachmentRole(old))
	require.Same(t, source, next.currentSession())
	require.Equal(t, uint64(0), next.roleGeneration.Load())
	require.Equal(t, uint64(0), old.roleGeneration.Load())
	require.Equal(t, 0, testAttachmentTabIndex(target))
}
