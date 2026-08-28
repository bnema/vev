package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

func TestRenamePublishesCommittedRouteIdentityToAttachedClients(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t)
	sess.mu.Lock()
	sess.name = "0"
	sess.ephemeral = true
	sess.incarnation = domain.IncarnationID{1}
	sess.mu.Unlock()
	token := sess.captureAttachmentCapability(ac, ac.transport())
	ac.installTestAttachmentCapability(token)
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1})

	require.NoError(t, d.renameSession(sess, "vps-infra"))

	frame := awaitFrame(t, sends, ports.MsgCommittedRouteIdentity)
	identity, err := ports.UnmarshalCommittedRouteIdentity(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.ExactSessionTarget{
		LifecycleID: domain.IncarnationID{1},
		SessionName: "vps-infra",
	}, identity.Target)
	require.False(t, identity.Ephemeral)
}

func TestRenameDefersCommittedRouteIdentityUntilFirstRouteSnapshot(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t)
	sess.mu.Lock()
	sess.name = "0"
	sess.ephemeral = true
	sess.incarnation = domain.IncarnationID{1}
	sess.mu.Unlock()
	token := sess.captureAttachmentCapability(ac, ac.transport())
	ac.installTestAttachmentCapability(token)

	require.NoError(t, d.renameSession(sess, "vps-infra"))
	require.Empty(t, sends, "the route ledger must be published before its identity can be updated")

	activeRef := protocol.RouteRef{Key: 1, Generation: 1}
	snapshot := protocol.RecentRouteSnapshot{
		Generation:  1,
		Active:      activeRef,
		ActiveEntry: protocol.RecentRouteEntry{Key: 1, Generation: 1, Target: testRouteTarget("0", 1), Name: "0", Kind: protocol.RouteKindLocal},
		Home:        activeRef,
	}
	payload, err := ports.MarshalRecentRouteSnapshot(snapshot)
	require.NoError(t, err)
	require.False(t, d.handleAttachmentClientFrame(token, ports.Frame{Type: ports.MsgRecentRouteSnapshot, Payload: payload}))

	frame := awaitFrame(t, sends, ports.MsgCommittedRouteIdentity)
	identity, err := ports.UnmarshalCommittedRouteIdentity(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.ExactSessionTarget{
		LifecycleID: domain.IncarnationID{1},
		SessionName: "vps-infra",
	}, identity.Target)
	require.False(t, identity.Ephemeral)
}

func TestEphemeralPromotionPreservesIncarnation(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	store, state := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(t, store)(d)

	sess, err := createSessionForTest(d, "0", true, "/tmp", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	sess.mu.Lock()
	original := sess.incarnation
	sess.mu.Unlock()
	require.NotZero(t, original)

	require.NoError(t, d.renameSession(sess, "local"))
	require.Equal(t, original, sess.incarnation)
	require.Equal(t, original, state.record(t, "local").IncarnationID)
}

func TestRenamePreservesIncarnationSnapshotSources(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{}
	WithSnapshotRepository(repository)(d)
	store, state := newMockStore(t)
	WithStore(t, store)(d)
	sess := newSnapshotTestSession(t, "old", false, "/work")
	d.sessions = map[domain.SessionID]*session{sess.id: sess}
	sess.mu.Lock()
	record := sess.persistRecordLocked(1)
	sess.mu.Unlock()
	require.NoError(t, testPersister(t, d).Save(record))

	require.NoError(t, d.renameSession(sess, "new"))
	require.Empty(t, repository.calls, "rename must not delete incarnation-keyed snapshots")
	require.False(t, state.has("old"))
	require.True(t, state.has("new"))
	require.Equal(t, sess.incarnation, state.record(t, "new").IncarnationID)
}
