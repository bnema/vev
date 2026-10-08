package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
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
	awaitFrame(t, sends, "Output")
	d.handleInputForAttachment(token, []byte("target\r"))
	targetFrame := awaitFrame(t, sends, "AttachTarget")
	attachTarget := decodeServerMessage(t, targetFrame).(protocol.AttachTarget)
	require.Equal(t, &protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "target"}, attachTarget.ExactTarget)

	d.switchSamePeerForAttachment(token, protocol.SamePeerSwitchRequest{
		RequestID: 1, Target: *attachTarget.ExactTarget,
	})

	require.Same(t, target, ac.currentAttachmentSession())
	identityFrame := awaitFrame(t, sends, "CommittedRouteIdentity")
	identity := decodeServerMessage(t, identityFrame).(protocol.CommittedRouteIdentity)
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
	identityFrame := awaitFrame(t, sends, "CommittedRouteIdentity")
	identity := decodeServerMessage(t, identityFrame).(protocol.CommittedRouteIdentity)
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
	failureFrame := awaitFrame(t, sends, "SamePeerSwitchFailure")
	failure := decodeServerMessage(t, failureFrame).(protocol.SamePeerSwitchFailure)
	require.Equal(t, protocol.SamePeerSwitchFailure{RequestID: 1, Code: protocol.SamePeerSwitchStaleTarget}, failure)
}

// TestSamePeerSwitchClientInitiated pins that the client picker can move its
// attachment without a daemon offer, while a pending offer still admits only
// its own exact target.
func TestSamePeerSwitchClientInitiated(t *testing.T) {
	lifecycle := domain.SessionLifecycleID{7}
	requestTarget := protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "target"}
	otherOffer := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{8}, SessionName: "other"}
	tests := []struct {
		name     string
		offer    *protocol.ExactSessionTarget
		purging  bool
		switched bool
	}{
		{name: "no pending offer switches", switched: true},
		{name: "matching offer switches", offer: &requestTarget, switched: true},
		{name: "another pending offer refuses", offer: &otherOffer},
		{name: "a running purge refuses", purging: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, source, ac, sends, releases := newManualTabSession(t, 1)
			defer releaseAll(releases)
			target := &session{
				sessionCore: sessionCore{id: "target", name: "target", incarnation: lifecycle, attachments: make(map[*attachedClient]struct{})},
				ctx:         source.ctx,
				cancel:      func() {},
				tabs:        []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23})},
			}
			publishTiledPaneOwners(target, target.tabs[0])
			d.mu.Lock()
			d.sessions[target.id] = target
			d.mu.Unlock()
			ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1})
			if tt.offer != nil {
				ac.offerSamePeerTarget(*tt.offer)
			}
			d.mu.Lock()
			d.purgeAdmissionClosing = tt.purging
			d.mu.Unlock()

			token := source.captureAttachmentCapability(ac, ac.transport())
			effect, admitted := ac.beginAttachmentEffect(token)
			require.True(t, admitted)
			defer effect.End()
			d.switchSamePeerForAttachment(effect, protocol.SamePeerSwitchRequest{RequestID: 1, Target: requestTarget})

			d.mu.Lock()
			require.Zero(t, d.purgeAdmissionActive, "the switch releases its purge admission")
			d.purgeAdmissionClosing = false
			d.mu.Unlock()
			if tt.switched {
				require.Same(t, target, ac.currentAttachmentSession())
				identity := decodeServerMessage(t, awaitFrame(t, sends, "CommittedRouteIdentity")).(protocol.CommittedRouteIdentity)
				require.Equal(t, requestTarget, identity.Target)
				return
			}
			require.Same(t, source, ac.currentAttachmentSession())
			failure := decodeServerMessage(t, awaitFrame(t, sends, "SamePeerSwitchFailure")).(protocol.SamePeerSwitchFailure)
			require.Equal(t, protocol.SamePeerSwitchStaleTarget, failure.Code)
		})
	}
}

func TestSamePeerSwitchRefreshesTargetFromSwitchingAttachment(t *testing.T) {
	targetEnv := []string{"SHELL=/usr/bin/fish", "WAYLAND_DISPLAY=wayland-old", "SSH_AUTH_SOCK=/run/other-agent"}
	tests := []struct {
		name      string
		clientEnv []string
		want      []string
	}{
		{
			name:      "local attachment refreshes session-bound variables",
			clientEnv: []string{"SHELL=/bin/bash", "WAYLAND_DISPLAY=wayland-1"},
			want:      []string{"SHELL=/usr/bin/fish", "WAYLAND_DISPLAY=wayland-1"},
		},
		{
			name: "daemon-owned attachment leaves the target untouched",
			want: targetEnv,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, source, ac, _, releases := newManualTabSession(t, 1)
			defer releaseAll(releases)
			ac.setClientEnvironment(tt.clientEnv)

			lifecycle := domain.SessionLifecycleID{7}
			target := &session{
				sessionCore: sessionCore{id: "target", name: "target", incarnation: lifecycle, attachments: make(map[*attachedClient]struct{})},
				ctx:         source.ctx,
				cancel:      func() {},
				tabs:        []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23})},
				env:         append([]string(nil), targetEnv...),
			}
			publishTiledPaneOwners(target, target.tabs[0])
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

			d.switchSamePeerForAttachment(effect, protocol.SamePeerSwitchRequest{RequestID: 1, Target: requestTarget})

			require.Same(t, target, ac.currentAttachmentSession())
			target.mu.Lock()
			defer target.mu.Unlock()
			require.Equal(t, tt.want, target.env)
		})
	}
}
