package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/picker"
)

func remoteLifecycleForTest() domain.SessionLifecycleID {
	var id domain.SessionLifecycleID
	id[0] = 7
	return id
}

func TestLocalPickerOfferCarriesExactLifecycle(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	source, ac, sends := addRemoteRefreshPickerOwner(t, d, "source")
	target, _, _ := addRemoteRefreshPickerOwner(t, d, "target")
	target.incarnation = remoteLifecycleForTest()
	token := source.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()

	require.NoError(t, d.switchToTargetForAttachment(token, picker.Target{Session: target.id}, sessionHandoffGuard{allowSamePeer: true}, "picker-select"))
	frame := receiveRemotePicker(t, sends, "local attach target")
	got, err := ports.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.Empty(t, got.Endpoint)
	require.Equal(t, ports.IntentAttach, got.Intent)
	require.True(t, got.SamePeer)
	require.Equal(t, &ports.ExactSessionTarget{LifecycleID: target.incarnation, SessionName: target.name}, got.ExactTarget)
	require.Same(t, source, ac.currentAttachmentSession(), "the source remains attached until the client confirms the switch")
}

func TestStoppedLocalPickerHandoffWaitsForClientClose(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	source, ac, sends := addRemoteRefreshPickerOwner(t, d, "source")
	lifecycle := remoteLifecycleForTest()
	d.inactive["stopped"] = inactiveSession{
		name: "stopped", state: ports.SessionDown, incarnation: lifecycle,
	}
	graphics := newGraphicsOutputState()
	graphics.assets["asset"] = graphicsOutputAsset{id: graphics.namespaceBase}
	graphics.mayHaveEmitted = true
	ac.output.graphicsOutput = graphics

	token := source.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()

	require.NoError(t, d.switchToTargetForAttachment(token, picker.Target{
		Name: "stopped", Incarnation: lifecycle,
	}, sessionHandoffGuard{closePicker: true, allowSamePeer: true}, "picker-select"))

	cleanup := receiveRemotePicker(t, sends, "graphics cleanup")
	require.Equal(t, ports.MsgOutput, cleanup.Type)
	handoff := receiveRemotePicker(t, sends, "stopped local attach target")
	require.Equal(t, ports.MsgAttachTarget, handoff.Type)
	target, err := ports.UnmarshalAttachTarget(handoff.Payload)
	require.NoError(t, err)
	require.False(t, target.SamePeer)
	require.Equal(t, &ports.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "stopped"}, target.ExactTarget)
	require.Same(t, source, ac.currentAttachmentSession(), "the source remains attached until the client receives the handoff and closes")
	require.True(t, attachmentRegistered(source, ac))
}

func TestRemotePickerRichHandoffCarriesLifecycleTabAndPolicy(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	lifecycle := remoteLifecycleForTest()
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: lifecycle, Name: "work", State: "up", Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()
	sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
	token := sess.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
	require.NoError(t, d.sendRemoteAttachTargetForAttachment(token, target, sessionHandoffGuard{closePicker: true}, "picker-select"))
	frame := receiveRemotePicker(t, sends, "rich attach target")
	got, err := ports.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.NotNil(t, got.RemoteTarget)
	require.Equal(t, remoteTarget, *got.RemoteTarget)
	require.Equal(t, ports.EnvironmentPolicyDaemonOwned, got.EnvironmentPolicy)
}

func TestRemotePickerRichHandoffRejectsMismatchedRouteKey(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*picker.Target)
	}{
		{name: "missing key", mutate: func(target *picker.Target) { target.RemoteKey = nil }},
		{name: "endpoint mismatch", mutate: func(target *picker.Target) {
			key := *target.RemoteKey
			key.Host = "other"
			target.RemoteKey = &key
			target.Session = key.ID()
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := newRemotePickerDaemon(nil)
			lifecycle := remoteLifecycleForTest()
			d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
				Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{
					LifecycleID: lifecycle, Name: "work", State: ports.RemoteCatalogSessionUp,
					Tabs: []ports.RemoteCatalogTab{{ID: "tab-1"}},
				}},
			}})
			d.remoteCatalog.mu.Lock()
			d.remoteCatalog.status["arch"] = remoteHostFresh
			d.remoteCatalog.mu.Unlock()
			sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
			token := sess.attachmentToken(ac, ac.transport())
			effect, admitted := ac.beginAttachmentEffect(token)
			require.True(t, admitted)
			token.effect = effect
			defer effect.End()

			key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
			remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
			target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
			test.mutate(&target)

			require.ErrorIs(t, d.sendRemoteAttachTargetForAttachment(token, target, sessionHandoffGuard{}, "picker-select"), errAttachmentTransition)
			for {
				select {
				case frame := <-sends:
					require.NotEqual(t, ports.MsgAttachTarget, frame.Type)
				default:
					return
				}
			}
		})
	}
}

func TestRemotePickerRichHandoffRejectsReplacedLifecycle(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	lifecycle := remoteLifecycleForTest()
	cachedLifecycle := lifecycle
	cachedLifecycle[0]++
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: cachedLifecycle, Name: "work", State: "up", Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()
	sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
	token := sess.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
	require.ErrorIs(t, d.sendRemoteAttachTargetForAttachment(token, target, sessionHandoffGuard{}, "picker-select"), errAttachmentTransition)
	for {
		select {
		case frame := <-sends:
			require.NotEqual(t, ports.MsgAttachTarget, frame.Type, "rejected handoff must not send an attach target")
		default:
			return
		}
	}
}
