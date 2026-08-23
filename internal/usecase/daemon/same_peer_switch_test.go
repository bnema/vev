package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestSamePeerSwitchTransitionsExactTargetAndPreferredTab(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)

	lifecycle := domain.SessionLifecycleID{7}
	target := &session{
		sessionCore: sessionCore{id: "target", name: "target", incarnation: lifecycle, attachments: make(map[*attachedClient]struct{})},
		ctx:         source.ctx,
		cancel:      func() {},
		tabs: []*tab{
			newTab(nil, domain.Size{Cols: 80, Rows: 23}),
			newTab(nil, domain.Size{Cols: 80, Rows: 23}),
		},
	}
	for _, tab := range target.tabs {
		publishTiledPaneOwners(target, tab)
	}
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{Generation: 1})

	token := source.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()
	requestTarget := ports.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "target"}
	ac.offerSamePeerTarget(requestTarget)

	d.switchSamePeerForAttachment(token, ports.SamePeerSwitchRequest{
		RequestID:      1,
		Target:         requestTarget,
		PreferredTabID: domain.TabStableID(target.tabs[1].stableID),
	})

	require.Same(t, target, ac.currentAttachmentSession())
	require.Equal(t, domain.TabStableID(target.tabs[1].stableID), ac.viewSnapshot().tabID)
	identityFrame := awaitFrame(t, sends, ports.MsgCommittedRouteIdentity)
	identity, err := ports.UnmarshalCommittedRouteIdentity(identityFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "target"}, identity.Target)
}

func TestFinishSendErrorDetachClearsSamePeerOffer(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.offerSamePeerTarget(ports.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "target"})

	d.finishSendErrorDetach(sess, ac, ac.transport())

	ac.samePeerOfferMu.Lock()
	defer ac.samePeerOfferMu.Unlock()
	require.Nil(t, ac.samePeerOffer)
}

func TestSamePeerSwitchRejectsStaleTargetWithoutMutation(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{Generation: 1})

	token := source.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()

	d.switchSamePeerForAttachment(token, ports.SamePeerSwitchRequest{
		RequestID: 1,
		Target:    ports.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{9}, SessionName: "missing"},
	})

	require.Same(t, source, ac.currentAttachmentSession())
	failureFrame := awaitFrame(t, sends, ports.MsgSamePeerSwitchFailure)
	failure, err := ports.UnmarshalSamePeerSwitchFailure(failureFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.SamePeerSwitchFailure{RequestID: 1, Code: ports.SamePeerSwitchStaleTarget}, failure)
}
