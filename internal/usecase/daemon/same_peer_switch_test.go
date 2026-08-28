package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func TestPaletteSessionSelectionCompletesSamePeerSwitch(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)

	lifecycle := domain.SessionLifecycleID{7}
	target := &session{
		sessionCore: sessionCore{
			id: "target", name: "target", incarnation: lifecycle,
			attachments: make(map[*attachedClient]struct{}),
		},
		ctx: source.ctx, cancel: func() {},
		tabs: []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23})},
	}
	publishTiledPaneOwners(target, target.tabs[0])
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1})

	token := beginRecentRoutePaletteEffect(t, d, source, ac)
	d.handleInputForAttachment(token, []byte("\x1b "))
	awaitFrame(t, sends, wire.MsgOutput)
	d.handleInputForAttachment(token, []byte("target\r"))
	targetFrame := awaitFrame(t, sends, wire.MsgAttachTarget)
	attachTarget, err := wire.UnmarshalAttachTarget(targetFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, &protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "target"}, attachTarget.ExactTarget)

	d.switchSamePeerForAttachment(token, protocol.SamePeerSwitchRequest{
		RequestID: 1, Target: *attachTarget.ExactTarget,
	})

	require.Same(t, target, ac.currentAttachmentSession())
	identityFrame := awaitFrame(t, sends, wire.MsgCommittedRouteIdentity)
	identity, err := wire.UnmarshalCommittedRouteIdentity(identityFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, attachTarget.ExactTarget, &identity.Target)
}

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
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1})

	token := source.captureAttachmentCapability(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()
	requestTarget := protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "target"}
	ac.offerSamePeerTarget(requestTarget)

	d.switchSamePeerForAttachment(effect, protocol.SamePeerSwitchRequest{
		RequestID:      1,
		Target:         requestTarget,
		PreferredTabID: domain.TabStableID(target.tabs[1].stableID),
	})

	require.Same(t, target, ac.currentAttachmentSession())
	require.Equal(t, domain.TabStableID(target.tabs[1].stableID), ac.viewSnapshot().tabID)
	identityFrame := awaitFrame(t, sends, wire.MsgCommittedRouteIdentity)
	identity, err := wire.UnmarshalCommittedRouteIdentity(identityFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "target"}, identity.Target)
}

func TestFinishSendErrorDetachClearsSamePeerOffer(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.offerSamePeerTarget(protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "target"})

	d.finishSendErrorDetach(sess, ac, ac.transport())

	ac.samePeerOfferMu.Lock()
	defer ac.samePeerOfferMu.Unlock()
	require.Nil(t, ac.samePeerOffer)
}

func TestSamePeerSwitchRejectsStaleTargetWithoutMutation(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1})

	token := source.captureAttachmentCapability(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()

	d.switchSamePeerForAttachment(effect, protocol.SamePeerSwitchRequest{
		RequestID: 1,
		Target:    protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{9}, SessionName: "missing"},
	})

	require.Same(t, source, ac.currentAttachmentSession())
	failureFrame := awaitFrame(t, sends, wire.MsgSamePeerSwitchFailure)
	failure, err := wire.UnmarshalSamePeerSwitchFailure(failureFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.SamePeerSwitchFailure{RequestID: 1, Code: protocol.SamePeerSwitchStaleTarget}, failure)
}
