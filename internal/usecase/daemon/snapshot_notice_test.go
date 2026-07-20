package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
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
