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
func TestSnapshotWorkerPublishesContentAddressedCapture(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
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
	sess.snapEligible.Store(!ephemeral && name != "")
	return sess
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
