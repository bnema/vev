package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotFailureNoticeDeduplicatesStableCauseAndClearsOnSuccess(t *testing.T) {
	d, _, client, _ := newNoticeFixture(t, newNoticeClock())
	capture := &snapshotCapture{session: newSnapshotTestSession(t, "failed", false, "/work"), name: "failed"}

	d.reportSnapshotFailure(capture, "publish", errors.New("first transient failure"))
	d.reportSnapshotFailure(capture, "publish", errors.New("same class, changed details"))

	history := d.notices.history()
	require.Len(t, history, 1)
	require.EqualValues(t, 2, history[0].Count)
	require.Len(t, awaitToastCount(t, client, 1), 1)

	d.finishSnapshotCapture(capture, true)
	d.reportSnapshotFailure(capture, "publish", errors.New("failure after success"))
	require.Len(t, awaitToastCount(t, client, 1), 1)
	require.Len(t, d.notices.history(), 1)
	require.EqualValues(t, 3, d.notices.history()[0].Count)
}
