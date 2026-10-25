package daemon

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
)

// TestWithSnapshotRepositoryRejectsTypedNil verifies typed nil repositories are disabled.
func TestWithSnapshotRepositoryRejectsTypedNil(t *testing.T) {
	var repository *portsmocks.MockSnapshotRepository
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))

	WithSnapshotRepository(repository)(d)

	require.Nil(t, d.snapshotRepository)
	require.False(t, d.snapsEnabled)
}

func TestDurableWriterFailureNamesIncludesBufferedCapture(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
	capture := &snapshotCapture{name: "work", session: newSnapshotTestSession(t, "work", false, "/work")}
	d.snapshots.mu.Lock()
	d.snapshots.admitted[capture] = struct{}{}
	d.snapshots.jobs <- capture
	d.snapshots.mu.Unlock()

	require.Equal(t, []string{"work"}, d.durableWriterFailureNames())
}

func TestCheckpointCatalogueFailureKeepsCaptureRetryable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
		catalogue := portsmocks.NewMockCatalogue(t)
		repository := portsmocks.NewMockSnapshotRepository(t)
		record := domain.CatalogueRecord{
			Name:          "work",
			IncarnationID: domain.IncarnationID{1},
		}
		catalogue.EXPECT().Record("work").Return(record, true, nil).Once()
		repository.EXPECT().Publish(mock.Anything, mock.Anything).Return(nil).Once()
		catalogue.EXPECT().Replace("work", mock.Anything).Return(errors.New("catalogue unavailable")).Once()
		WithSnapshotRepository(repository)(d)
		WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)
		startSnapshotEncodeWorker(t, d)

		sess := newSnapshotTestSession(t, "work", false, "/work")
		require.True(t, d.captureSession(sess))
		requireSnapshotIdle(t, sess)
		require.True(t, sess.snapDirty.Load(), "failed catalogue commit must remain retryable")
	})
}

// TestSnapshotWorkerPublishesContentAddressedCapture verifies that all new
// checkpoints use the repository publication contract.
func TestSnapshotWorkerPublishesContentAddressedCapture(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
		startSnapshotEncodeWorker(t, d)

		repository.EXPECT().Publish(mock.Anything, mock.MatchedBy(func(p ports.SnapshotPublication) bool {
			return p.Name == "work" && p.Generation == 1 && len(p.Manifest) > 0 && len(p.Objects) > 0
		})).Return(nil).Once()

		sess := newSnapshotTestSession(t, "work", false, "/work")
		require.True(t, d.captureSession(sess))
		requireSnapshotClean(t, sess)
	})
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
	sess := &session{sessionCore: sessionCore{id: domain.SessionID("sess-" + name), name: name, ephemeral: ephemeral, createdAt: 42}, ctx: context.Background(), cancel: func() {}, tabs: []*tab{tb}, cwd: cwd}
	if !ephemeral {
		sess.incarnation = domain.IncarnationID{1}
	}
	sess.snapEligible.Store(!ephemeral && name != "")
	return sess
}

func startSnapshotEncodeWorker(t *testing.T, d *Daemon) {
	t.Helper()
	d.startSnapshotEncodeWorker()
	t.Cleanup(d.stopSnapshotEncodeWorker)
}
