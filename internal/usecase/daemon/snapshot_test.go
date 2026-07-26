package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/pkg/renderer"
)

// TestSnapshotWorkerPublishesContentAddressedCapture verifies that all new
// checkpoints use the repository publication contract.
func TestWithSnapshotRepositoryRejectsTypedNil(t *testing.T) {
	var repository *snapshotAcceptanceRepository
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})

	WithSnapshotRepository(repository)(d)

	require.Nil(t, d.snapshotRepository)
	require.False(t, d.snapsEnabled)
}

func TestDurableWriterFailureNamesIncludesBufferedCapture(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	capture := &snapshotCapture{name: "work", session: newSnapshotTestSession(t, "work", false, "/work")}
	d.snapshotWorkerMu.Lock()
	d.snapshotAdmitted[capture] = struct{}{}
	d.snapshotJobs <- capture
	d.snapshotWorkerMu.Unlock()

	require.Equal(t, []string{"work"}, d.durableWriterFailureNames())
}

func TestCheckpointPublicationWithoutCommittedRefFailsCapture(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotRepository(portsmocks.NewMockSnapshotRepository(t))(d)
	coordinator := portsmocks.NewMockCheckpointCoordinator(t)
	WithCheckpointCoordinator(coordinator)(d)
	startSnapshotEncodeWorker(t, d)
	coordinator.EXPECT().PublishCheckpoint(mock.Anything, "work", mock.Anything).Return(domain.CatalogueRecord{Name: "work"}, nil).Once()

	sess := newSnapshotTestSession(t, "work", false, "/work")
	require.True(t, d.captureSession(sess))
	awaitSnapshotIdle(t, sess)
	require.True(t, sess.snapDirty.Load(), "nil committed publication must remain retryable")
}

func TestSnapshotWorkerPublishesContentAddressedCapture(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository)(d)
	startSnapshotEncodeWorker(t, d)

	repository.EXPECT().Publish(mock.Anything, mock.MatchedBy(func(p ports.SnapshotPublication) bool {
		return p.Name == "work" && p.Generation == 1 && len(p.Manifest) > 0 && len(p.Objects) > 0
	})).Return(nil).Once()

	sess := newSnapshotTestSession(t, "work", false, "/work")
	require.True(t, d.captureSession(sess))
	awaitSnapshotClean(t, sess)
}

func newSnapshotTestSession(t *testing.T, name string, ephemeral bool, cwd string) *session {
	t.Helper()
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().Pid().Return(1234).Maybe()
	pty.EXPECT().Read(mock.Anything).Return(0, errors.New("unused")).Maybe()
	pty.EXPECT().Write(mock.Anything).Return(0, errors.New("unused")).Maybe()
	pty.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	pty.EXPECT().ForegroundPgid().Return(0, nil).Maybe()

	tb := newTab(pty, domain.Size{Cols: 8, Rows: 3})
	p := tb.panes["pane-1"]
	p.screen.Write([]byte("hello"))
	appendHistoryRow(t, p.history, []renderer.Cell{{Rune: 'h'}, {Rune: 'i'}})
	sess := &session{id: domain.SessionID("sess-" + name), name: name, ephemeral: ephemeral, ctx: context.Background(), cancel: func() {}, tabs: []*tab{tb}, active: 0, cwd: cwd, createdAt: 42}
	if !ephemeral {
		sess.incarnation = domain.IncarnationID{1}
	}
	sess.snapEligible.Store(!ephemeral && name != "")
	return sess
}

// noOpSnapshotRepository supplies the current durable repository contract to
// focused test sinks that only need to observe publication or deletion calls.
type noOpSnapshotRepository struct{}

var _ ports.SnapshotRepository = noOpSnapshotRepository{}

func (noOpSnapshotRepository) Publish(context.Context, ports.SnapshotPublication) error { return nil }
func (noOpSnapshotRepository) LoadCheckpoint(context.Context, domain.IncarnationID, string, ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	return ports.SnapshotGeneration{}, errors.New("unused")
}
func (noOpSnapshotRepository) RepairHEAD(context.Context, domain.IncarnationID, ports.CheckpointRef) error {
	return nil
}
func (noOpSnapshotRepository) DeleteIncarnation(context.Context, domain.IncarnationID) error {
	return nil
}
func (noOpSnapshotRepository) CollectGarbage(context.Context, map[domain.IncarnationID]domain.CheckpointRef) error {
	return nil
}

func awaitSnapshotIdle(t testing.TB, sess *session) {
	t.Helper()
	timer := time.NewTimer(testWaitTimeout)
	defer timer.Stop()
	for {
		sess.snapshotMu.Lock()
		pending := sess.snapshotPending
		changed := sess.snapshotChangeLocked()
		sess.snapshotMu.Unlock()
		if !pending {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatal("timed out waiting for snapshot to become idle")
		}
	}
}

func awaitSnapshotClean(t *testing.T, sess *session) {
	t.Helper()
	timer := time.NewTimer(testWaitTimeout)
	defer timer.Stop()
	for {
		sess.snapshotMu.Lock()
		clean := !sess.snapDirty.Load()
		changed := sess.snapshotChangeLocked()
		sess.snapshotMu.Unlock()
		if clean {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatal("timed out waiting for snapshot to become clean")
		}
	}
}

func startSnapshotEncodeWorker(t *testing.T, d *Daemon) {
	t.Helper()
	d.startSnapshotEncodeWorker()
	t.Cleanup(d.stopSnapshotEncodeWorker)
}
