package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestAttachedClientOwnerBindingNarrowsLocalOnlyBehavior(t *testing.T) {
	local := &session{sessionCore: sessionCore{id: "local"}}
	remote := &remoteView{id: 7}
	ac := &attachedClient{}

	ac.setSession(local)
	require.Same(t, local, ac.currentAttachmentOwner())
	require.Same(t, local, ac.currentAttachmentSession())

	ac.setAttachmentOwner(remote)
	require.Same(t, remote, ac.currentAttachmentOwner())
	require.Nil(t, ac.currentAttachmentSession(), "remote owners must not reach local-only paths")
}

func TestAttachedClientOwnerBindingNormalizesTypedNil(t *testing.T) {
	ac := &attachedClient{}

	ac.setSession(nil)
	require.Nil(t, ac.currentAttachmentOwner())

	var remote *remoteView
	ac.setAttachmentOwner(remote)
	require.Nil(t, ac.currentAttachmentOwner())
}

func TestAttachmentTokenRejectsAnOwnerReboundToRemoteView(t *testing.T) {
	transport := &closeTrackingTransport{}
	local := &session{sessionCore: sessionCore{id: "local"}}
	ac := &attachedClient{tr: transport}
	ac.setSession(local)
	local.mu.Lock()
	require.True(t, local.registerAttachmentLocked(ac))
	local.mu.Unlock()

	token := local.attachmentToken(ac, transport)
	require.True(t, token.current())

	ac.setAttachmentOwner(&remoteView{id: 1})
	require.False(t, token.current(), "a local token must not survive an exact owner rebind")
}

func TestRemoteAttachmentTokenUsesExactRemoteOwnerMembership(t *testing.T) {
	transport := &closeTrackingTransport{}
	view := &remoteView{id: 1}
	ac := &attachedClient{tr: transport}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()

	token := attachmentOwnerToken(view, ac, transport)
	require.True(t, token.current())
	require.True(t, token.attachmentCurrent(), "remote tokens do not acquire a local render lease")

	view.mu.Lock()
	require.True(t, view.unregisterAttachmentLocked(ac))
	view.mu.Unlock()
	require.False(t, token.current(), "remote membership is part of exact token identity")
}

func TestTransitionToRemoteViewPublishesOneAuthoritativeOwner(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	view := &remoteView{key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	before := source.attachmentToken(ac, ac.transport())
	require.True(t, before.current())
	published, err := d.transitionToRemoteView(before, view)
	require.NoError(t, err)

	require.False(t, before.current())
	require.True(t, published.current())
	require.Same(t, view, ac.currentAttachmentOwner())
	require.Empty(t, source.snapshotAttachments())
	require.True(t, view.attachmentRegistered(ac))
	require.Same(t, source, ac.previousOwner.Get())
}

func TestShutdownRetiresRemoteViewsWithoutLocalSession(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	transport := &closeTrackingTransport{}
	ac := &attachedClient{tr: transport}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	token := attachmentOwnerToken(view, ac, transport)
	require.True(t, token.current())

	d.shutdownAll(ports.ReasonServerShutdown)

	d.mu.Lock()
	require.Empty(t, d.remoteViews)
	require.Empty(t, d.remoteViewsByKey)
	d.mu.Unlock()
	require.False(t, token.current())
}

func TestRemoteClientGoneParksExactOwnerBinding(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	transport := &closeTrackingTransport{}
	ac := &attachedClient{tr: transport, resumeCapable: true}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	token := attachmentOwnerToken(view, ac, transport)

	require.True(t, d.clientGoneRemote(view, token, false))
	require.Nil(t, ac.currentAttachmentOwner())
	require.False(t, view.attachmentRegistered(ac))
	d.mu.Lock()
	parked := d.parked[ac.resumeToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Same(t, view, parked.owner)
}

func TestRemoteParkingRetainsStableOwnerBinding(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{
		id:  1,
		key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"},
	}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	ac := &attachedClient{tr: &closeTrackingTransport{}, resumeCapable: true}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()

	token := d.markParkingInFlightOwner(view, ac)
	require.NotZero(t, token)
	view.mu.Lock()
	require.True(t, view.unregisterAttachmentLocked(ac))
	view.mu.Unlock()
	ac.setAttachmentOwner(nil)
	require.True(t, d.parkAttachmentOwner(view, ac))

	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Same(t, view, parked.owner)
}

func TestRemoteViewRegistryUsesDaemonLocalIDAndExactRemoteLifecycleKey(t *testing.T) {
	target := domain.RemoteSessionTarget{
		Endpoint:      "user@host",
		DisplayOrigin: "host",
		LifecycleID:   domain.SessionLifecycleID{1},
		SessionName:   "remote",
		LiveTabID:     "tab-1",
	}
	key, err := remoteViewKeyForTarget(target)
	require.NoError(t, err)

	d := &Daemon{
		remoteViews:      make(map[remoteViewID]*remoteView),
		remoteViewsByKey: make(map[remoteViewKey]remoteViewID),
	}
	first := &remoteView{key: key}
	second := &remoteView{key: key}

	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(first))
	require.NotZero(t, first.id)
	require.Same(t, first, d.remoteViewByKeyLocked(key))
	require.Error(t, d.registerRemoteViewLocked(second), "a lifecycle key cannot alias another local view")
	require.True(t, d.unregisterRemoteViewLocked(first))
	require.Nil(t, d.remoteViewByKeyLocked(key))
	d.mu.Unlock()
}
