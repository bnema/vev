package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func remoteBackFixture(t *testing.T) (*Daemon, *session, *attachedClient, *remoteView, attachmentConnectionToken) {
	t.Helper()
	d, local, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	view := &remoteView{key: remoteViewKey{
		endpoint:    "host",
		lifecycleID: domain.SessionLifecycleID{1},
		sessionName: "remote",
	}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	token, err := d.transitionToRemoteView(local.attachmentToken(ac, ac.transport()), view)
	require.NoError(t, err)
	return d, local, ac, view, token
}

func TestBackSessionForAttachmentReturnsFromRemoteViewToPreviousLocal(t *testing.T) {
	d, local, ac, view, token := remoteBackFixture(t)

	require.NoError(t, d.backSessionForAttachment(token))

	require.Same(t, local, ac.currentAttachmentSession())
	require.True(t, local.attachmentRegistered(ac))
	require.False(t, view.attachmentRegistered(ac))
	require.Same(t, view, ac.previousOwner.Get(), "reverse navigation records the remote owner for toggling back")

	current := local.attachmentToken(ac, ac.transport())
	require.True(t, current.current())
	require.NotNil(t, current.lease, "returning to a local owner restores its coordinator lease")
}

func TestBackSessionForAttachmentReturnsToPreviousRemoteView(t *testing.T) {
	d, local, ac, view, remoteToken := remoteBackFixture(t)

	require.NoError(t, d.backSessionForAttachment(remoteToken))
	localToken := local.attachmentToken(ac, ac.transport())
	require.NoError(t, d.backSessionForAttachment(localToken))

	require.Same(t, view, ac.currentAttachmentOwner())
	require.True(t, view.attachmentRegistered(ac))
	require.False(t, local.attachmentRegistered(ac))
	require.Same(t, local, ac.previousOwner.Get())
}

func TestRemoteRecentAppearsInLocalStatusMRUAfterReturn(t *testing.T) {
	d, local, ac, _, token := remoteBackFixture(t)

	require.NoError(t, d.backSessionForAttachment(token))

	state := d.barStateForAttachmentPaletteHintsFor(local, ac, "", nil, nil)
	require.Equal(t, []recentSession{{name: "remote@host", mruAt: 1}}, state.mru)
	require.Empty(t, d.recentSessions(local), "remote recency stays out of global navigation MRU")
}

func TestBackSessionForAttachmentFailsClosedForStaleOrNonLocalPredecessor(t *testing.T) {
	sentinel := &remoteView{id: 99}
	tests := []struct {
		name  string
		prep  func(*Daemon, *session, *attachedClient, *remoteView)
		check func(*testing.T, *session, *attachedClient, *remoteView)
	}{
		{
			name: "stale local predecessor",
			prep: func(d *Daemon, local *session, _ *attachedClient, _ *remoteView) {
				replacement := &session{sessionCore: sessionCore{id: local.id, name: local.name}}
				d.mu.Lock()
				d.sessions[local.id] = replacement
				d.mu.Unlock()
			},
			check: func(t *testing.T, _ *session, ac *attachedClient, view *remoteView) {
				require.Same(t, view, ac.currentAttachmentOwner())
				require.True(t, view.attachmentRegistered(ac))
				require.Nil(t, ac.previousOwner.Get(), "stale local history is discarded without changing ownership")
			},
		},
		{
			name: "non-local predecessor",
			prep: func(_ *Daemon, _ *session, ac *attachedClient, _ *remoteView) {
				ac.previousOwner.Set(sentinel)
			},
			check: func(t *testing.T, _ *session, ac *attachedClient, view *remoteView) {
				other := ac.previousOwner.Get()
				require.Same(t, view, ac.currentAttachmentOwner())
				require.True(t, view.attachmentRegistered(ac))
				require.Same(t, sentinel, other)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, local, ac, view, token := remoteBackFixture(t)
			tt.prep(d, local, ac, view)

			require.NoError(t, d.backSessionForAttachment(token))
			tt.check(t, local, ac, view)
		})
	}
}

func TestBackSessionForAttachmentRejectsReboundRemoteToken(t *testing.T) {
	d, local, ac, view, token := remoteBackFixture(t)
	ac.replaceTransport(&closeTrackingTransport{})

	require.NoError(t, d.backSessionForAttachment(token))
	require.Same(t, view, ac.currentAttachmentOwner())
	require.True(t, view.attachmentRegistered(ac))
	require.Same(t, local, ac.previousOwner.Get(), "a stale token must not consume valid history")
}
