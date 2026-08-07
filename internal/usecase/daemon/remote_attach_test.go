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

func TestRemotePickerSelectionRejectsReplacedLifecycle(t *testing.T) {
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
	sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = picker.New(nil, picker.SelectionConfig{})
	ac.overlays.pickerGeneration++
	ac.overlays.pickerMu.Unlock()
	token := sess.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}

	require.ErrorIs(t, d.switchToTargetForAttachment(token, target, sessionHandoffGuard{closePicker: true}, "picker-select"), errAttachmentTransition)
	require.Same(t, sess, ac.currentAttachmentSession())
	require.True(t, ac.overlays.pickerActive())
	select {
	case frame := <-sends:
		require.NotEqual(t, ports.MsgAttachTarget, frame.Type)
	default:
	}
}
