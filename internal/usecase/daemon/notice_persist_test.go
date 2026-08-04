package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

// TestKillSessionServerShutdownSaturatedAppendsNotice proves that when the
// daemon is shutting down and the snapshot worker is saturated — so there is
// no way to retain the session's terminal state and no attached client to
// toast, because the daemon is dying — the failure is handed to the
// NoticeStore instead of being silently lost.
// TestStartupClaimRecordsAndToastsPendingNotices proves notices persisted by
// a previous daemon are recorded to history and toasted to the first client
// that attaches after this daemon starts before their claim is acknowledged.
func TestStartupClaimRecordsAndToastsPendingNotices(t *testing.T) {
	claimed := []domain.Notification{
		{Code: domain.NoticeSnapshotWrite, Severity: domain.NoticeError, Message: "session a shut down without saving terminal state"},
		{Code: domain.NoticeSnapshotWrite, Severity: domain.NoticeError, Message: "session b shut down without saving terminal state"},
	}
	notices := portsmocks.NewMockNoticeStore(t)
	notices.EXPECT().Claim().Return(claimed, nil).Once()
	notices.EXPECT().Ack().Return(nil).Once()

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
	d.firstPaint(sess, ac)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticeSnapshotWrite, toasts[0].Code)
	require.Equal(t, 2, toasts[0].Count, "two same-code drained notices dedup into one toast counted twice")
	require.Empty(t, d.notices.drainPending(), "firstPaint must consume the queue")
}
