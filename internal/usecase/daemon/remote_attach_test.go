package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
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
	token := source.captureAttachmentCapability(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()

	require.NoError(t, d.switchToTargetForAttachment(effect, picker.Target{Session: target.id}, sessionHandoffGuard{allowSamePeer: true}, "picker-select"))
	frame := receiveRemotePicker(t, sends, "local attach target")
	got, err := wire.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.Empty(t, got.Endpoint)
	require.Equal(t, protocol.IntentAttach, got.Intent)
	require.True(t, got.SamePeer)
	require.Equal(t, &protocol.ExactSessionTarget{LifecycleID: target.incarnation, SessionName: target.name}, got.ExactTarget)
	require.Same(t, source, ac.currentAttachmentSession(), "the source remains attached until the client confirms the switch")
}

func TestStoppedLocalPickerHandoffWaitsForClientClose(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	source, ac, sends := addRemoteRefreshPickerOwner(t, d, "source")
	lifecycle := remoteLifecycleForTest()
	d.inactive["stopped"] = inactiveSession{
		name: "stopped", state: protocol.SessionDown, incarnation: lifecycle,
	}
	graphics := newGraphicsOutputState()
	graphics.assets["asset"] = graphicsOutputAsset{id: graphics.namespaceBase}
	graphics.mayHaveEmitted = true
	ac.output.graphicsOutput = graphics

	token := source.captureAttachmentCapability(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()

	require.NoError(t, d.switchToTargetForAttachment(effect, picker.Target{
		Name: "stopped", Incarnation: lifecycle,
	}, sessionHandoffGuard{closePicker: true, allowSamePeer: true}, "picker-select"))

	cleanup := receiveRemotePicker(t, sends, "graphics cleanup")
	require.Equal(t, wire.MsgOutput, cleanup.Type)
	handoff := receiveRemotePicker(t, sends, "stopped local attach target")
	require.Equal(t, wire.MsgAttachTarget, handoff.Type)
	target, err := wire.UnmarshalAttachTarget(handoff.Payload)
	require.NoError(t, err)
	require.False(t, target.SamePeer)
	require.Equal(t, &protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "stopped"}, target.ExactTarget)
	require.Same(t, source, ac.currentAttachmentSession(), "the source remains attached until the client receives the handoff and closes")
	require.True(t, attachmentRegistered(source, ac))
}

func TestRemotePickerRichHandoffCarriesLifecycleTabAndPolicy(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	lifecycle := remoteLifecycleForTest()
	d.remoteCatalog.replaceCache([]catalogue.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []catalogue.RemoteCatalogSession{{
			LifecycleID: lifecycle, Name: "work", State: "up", Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()
	sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
	token := sess.captureAttachmentCapability(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
	require.NoError(t, d.sendRemoteAttachTargetForAttachment(effect, target, sessionHandoffGuard{closePicker: true}, "picker-select"))
	frame := receiveRemotePicker(t, sends, "rich attach target")
	got, err := wire.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.NotNil(t, got.RemoteTarget)
	require.Equal(t, remoteTarget, *got.RemoteTarget)
	require.Equal(t, protocol.EnvironmentPolicyDaemonOwned, got.EnvironmentPolicy)
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
			d.remoteCatalog.replaceCache([]catalogue.RemoteCatalogCacheEntry{{
				Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []catalogue.RemoteCatalogSession{{
					LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp,
					Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1"}},
				}},
			}})
			d.remoteCatalog.mu.Lock()
			d.remoteCatalog.status["arch"] = remoteHostFresh
			d.remoteCatalog.mu.Unlock()
			sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
			token := sess.captureAttachmentCapability(ac, ac.transport())
			effect, admitted := ac.beginAttachmentEffect(token)
			require.True(t, admitted)
			defer effect.End()

			key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
			remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
			target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
			test.mutate(&target)

			require.ErrorIs(t, d.sendRemoteAttachTargetForAttachment(effect, target, sessionHandoffGuard{}, "picker-select"), errAttachmentTransition)
			for {
				select {
				case frame := <-sends:
					require.NotEqual(t, wire.MsgAttachTarget, frame.Type)
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
	d.remoteCatalog.replaceCache([]catalogue.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []catalogue.RemoteCatalogSession{{
			LifecycleID: cachedLifecycle, Name: "work", State: "up", Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()
	sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
	token := sess.captureAttachmentCapability(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
	require.ErrorIs(t, d.sendRemoteAttachTargetForAttachment(effect, target, sessionHandoffGuard{}, "picker-select"), errAttachmentTransition)
	for {
		select {
		case frame := <-sends:
			require.NotEqual(t, wire.MsgAttachTarget, frame.Type, "rejected handoff must not send an attach target")
		default:
			return
		}
	}
}
