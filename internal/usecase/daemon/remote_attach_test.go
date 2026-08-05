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

func TestRemotePickerRichHandoffCarriesLifecycleTabAndPolicy(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	lifecycle := remoteLifecycleForTest()
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: lifecycle, Name: "work", State: "running", Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
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
	require.NoError(t, d.sendRemoteAttachTargetForAttachment(token, target, key, sessionHandoffGuard{closePicker: true}, "picker-select"))
	frame := receiveRemotePicker(t, sends, "rich attach target")
	got, err := ports.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.NotNil(t, got.RemoteTarget)
	require.Equal(t, remoteTarget, *got.RemoteTarget)
	require.Equal(t, ports.EnvironmentPolicyDaemonOwned, got.EnvironmentPolicy)
}

func TestRemotePickerRichHandoffRejectsReplacedLifecycle(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	lifecycle := remoteLifecycleForTest()
	cachedLifecycle := lifecycle
	cachedLifecycle[0]++
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: cachedLifecycle, Name: "work", State: "running", Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "local")
	token := sess.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
	require.ErrorIs(t, d.sendRemoteAttachTargetForAttachment(token, target, key, sessionHandoffGuard{}, "picker-select"), errAttachmentTransition)
}
