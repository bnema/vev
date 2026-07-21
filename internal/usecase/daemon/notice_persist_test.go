package daemon

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

// TestKillSessionServerShutdownSaturatedAppendsNotice proves that when the
// daemon is shutting down and the snapshot worker is saturated — so there is
// no way to retain the session's terminal state and no attached client to
// toast, because the daemon is dying — the failure is handed to the
// NoticeStore instead of being silently lost.
func TestKillSessionServerShutdownSaturatedAppendsNotice(t *testing.T) {
	store := discardSnapshotStore{}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)

	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return snapcodec.Marshal(s)
	}

	active := newSnapshotTestSession(t, "active", false, "/work")
	queued := newSnapshotTestSession(t, "queued", false, "/work")
	markSnapshotDirty(active)
	markSnapshotDirty(queued)
	require.True(t, d.scheduleSnapshot(active))
	<-entered // the worker is now parked inside marshal for active
	require.True(t, d.scheduleSnapshot(queued), "one capture waits in the bounded queue")
	for i := range snapshotFinalQueueCapacity {
		s := newSnapshotTestSession(t, fmt.Sprintf("terminal-%d", i), false, "/work")
		markSnapshotDirty(s)
		require.True(t, d.scheduleFinalSnapshot(s), "fill the terminal-retention fallback to capacity")
	}

	target := newSnapshotTestSession(t, "target", false, "/work")
	targetPTY := target.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	targetPTY.EXPECT().Close().Return(nil).Maybe()
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()
	markSnapshotDirty(target)

	notices := portsmocks.NewMockNoticeStore(t)
	var appended domain.Notification
	notices.EXPECT().Append(mock.Anything).RunAndReturn(func(n domain.Notification) error {
		appended = n
		return nil
	}).Once()
	d.noticeStore = notices

	err := d.killSession(target, ports.ReasonServerShutdown, false)
	require.ErrorContains(t, err, "snapshot worker unavailable or saturated")

	d.mu.Lock()
	_, stillPresent := d.sessions[target.id]
	d.mu.Unlock()
	require.False(t, stillPresent, "the session must be torn down even though its state could not be saved")

	require.Equal(t, domain.NoticeSnapshotWrite, appended.Code)
	require.Equal(t, domain.NoticeError, appended.Severity)
	require.Equal(t, "session target shut down without saving terminal state", appended.Message)
	require.NotEmpty(t, appended.Details)
}

// TestKillSessionServerShutdownNotSaturatedSkipsAppend proves the notice store
// is left untouched when the final snapshot is retained normally.
func TestKillSessionServerShutdownNotSaturatedSkipsAppend(t *testing.T) {
	store := discardSnapshotStore{}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)

	notices := portsmocks.NewMockNoticeStore(t) // no .EXPECT(): Append must not be called
	d.noticeStore = notices

	target := newSnapshotTestSession(t, "target", false, "/work")
	targetPTY := target.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	targetPTY.EXPECT().Close().Return(nil).Maybe()
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()
	markSnapshotDirty(target)

	err := d.killSession(target, ports.ReasonServerShutdown, false)
	require.NoError(t, err)
}

// TestStartupDrainRecordsAndToastsPendingNotices proves notices persisted by a
// previous daemon are recorded to history and toasted to the first client that
// attaches after this daemon starts.
func TestStartupDrainRecordsAndToastsPendingNotices(t *testing.T) {
	drained := []domain.Notification{
		{Code: domain.NoticeSnapshotWrite, Severity: domain.NoticeError, Message: "session a shut down without saving terminal state"},
		{Code: domain.NoticeSnapshotWrite, Severity: domain.NoticeError, Message: "session b shut down without saving terminal state"},
	}
	notices := portsmocks.NewMockNoticeStore(t)
	notices.EXPECT().Drain().Return(drained, nil).Once()

	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	d.noticeStore = notices

	d.restoreSnapshots(t.Context())

	require.Len(t, d.notices.history(), 2, "each drained notice is recorded individually")

	tr, _ := newCapturingTransport(t)
	sess, ac, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  ports.IntentEphemeral,
		Size:    domain.Size{Cols: 80, Rows: 24},
	}, tr)
	require.NoError(t, err)
	d.firstPaint(sess, ac, ac.size)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticeSnapshotWrite, toasts[0].Code)
	require.Equal(t, 2, toasts[0].Count, "two same-code drained notices dedup into one toast counted twice")
	require.Empty(t, d.notices.drainPending(), "firstPaint must consume the queue")
}
