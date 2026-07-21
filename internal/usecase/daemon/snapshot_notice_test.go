package daemon

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

// TestSnapshotWriteFailureNotifiesGlobally proves a failed snapshot write surfaces
// as a GLOBAL notice: the failing session is never registered (it stands in for a
// session already torn down by the time its async write fails), yet a client
// attached to a DIFFERENT, surviving session still receives the toast. This is the
// whole point of routing through NotifyGlobal instead of a session-scoped notice.
func TestSnapshotWriteFailureNotifiesGlobally(t *testing.T) {
	// newNoticeFixture registers a survivor session "work"/"manual" owned by ac.
	d, _, survivorClient, _ := newNoticeFixture(t, newNoticeClock())
	store := portsmocks.NewMockSnapshotStore(t)
	WithSnapshotStore(store)(d)
	d.procCwd = func(int) (string, error) { return "/pane", nil }
	startSnapshotEncodeWorker(t, d)

	// The failing session is deliberately NOT added to d.sessions.
	failing := newSnapshotTestSession(t, "failing", false, "/fallback")

	store.EXPECT().Write("failing", mock.Anything).RunAndReturn(func(string, []byte) error {
		return errors.New("disk full")
	}).Once()

	require.True(t, d.captureSession(failing))

	// The write fails on the worker goroutine; awaitToastCount is the bounded,
	// poll-only completion signal. No require runs on the worker goroutine.
	toasts := awaitToastCount(t, survivorClient, 1)
	require.Equal(t, domain.NoticeSnapshotWrite, toasts[0].Code)
	require.Equal(t, "couldn't save session failing; recent state may be lost on restart", toasts[0].Message)
	require.Equal(t, domain.NoticeError, toasts[0].Severity)
	require.Equal(t, domain.SessionID(""), toasts[0].SessionID,
		"snapshot write failures are global, never scoped to the (possibly gone) failing session")
	require.Contains(t, toasts[0].Details, "disk full")
}

// TestCloseLastTabSaturatedSnapshotSurfacesNotice proves that when closing the
// last tab of a named session is refused because the snapshot worker cannot
// retain the final state, the session survives for a retry and the still
// attached client is told why, instead of the close silently no-oping.
//
// The snapshot worker is saturated deterministically with the same fixture as
// the kill-session ordering tests: one capture blocked in marshal, one in the
// bounded queue, and the terminal-retention fallback filled to capacity. No
// assertion runs on the worker goroutine; the marshal gate is released only on
// cleanup, after every assertion has run on the test goroutine.
func TestCloseLastTabSaturatedSnapshotSurfacesNotice(t *testing.T) {
	store := discardSnapshotStore{}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)

	entered := make(chan struct{})
	release := make(chan struct{})
	// Release the sole blocked marshal on the test goroutine before the worker
	// is stopped in cleanup, so teardown never waits on the gate.
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

	// A single-tab named session whose client is still attached.
	target := newSnapshotTestSession(t, "target", false, "/work")
	tr, _ := newCapturingTransport(t)
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	ac.setSession(target)
	target.client = ac
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()
	require.Len(t, target.tabs, 1)

	d.closeTab(target, target.tabs[0], false)

	// The refused close retains the session for a retry.
	d.mu.Lock()
	require.Same(t, target, d.sessions[target.id], "a refused close of the last tab must retain the session")
	d.mu.Unlock()

	// The refusal is recorded in history and shown on the still-attached client.
	hist := d.notices.history()
	require.NotEmpty(t, hist, "the refused close must be recorded as a notice")
	require.Equal(t, domain.NoticeSnapshotSaturated, hist[0].Code)
	require.Equal(t, target.id, hist[0].SessionID)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticeSnapshotSaturated, toasts[0].Code)
	require.Equal(t, "couldn't close tab: session state not yet saved; try again", toasts[0].Message)
}
