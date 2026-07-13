package daemon

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

// TestFinishAttachDoesNotWaitForBlockedOldRenderWorker verifies the replacement
// boundary rather than relying on a scheduler delay: an old coordinator worker
// is already blocked in Send when finishAttach publishes its replacement.
func TestFinishAttachDoesNotWaitForBlockedOldRenderWorker(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	clock := newCoordinatorMockClock(t, 4)
	d := newTestDaemon(t, newFactory(t, pty), clock.clock)

	oldTransport := portsmocks.NewMockTransport(t)
	oldSendEntered := make(chan struct{})
	oldClosed := make(chan struct{})
	var closeOnce sync.Once
	oldTransport.EXPECT().Send(mock.Anything).RunAndReturn(func(ports.Frame) error {
		close(oldSendEntered)
		<-oldClosed
		return io.EOF
	}).Once()
	oldTransport.EXPECT().Close().RunAndReturn(func() error {
		closeOnce.Do(func() { close(oldClosed) })
		return nil
	}).Once()

	hello := ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  ports.IntentNew,
		Name:    "stalled-attachment",
		Size:    domain.Size{Cols: 80, Rows: 24},
		TermEnv: "xterm-256color",
	}
	sess, old, err := d.route(hello, oldTransport)
	require.NoError(t, err)
	rc := sess.renderCoordinator()
	lease := rc.attachmentLease(old)
	require.True(t, rc.markAttachmentReady(lease))

	rc.invalidateForAttachment(old, renderInvalidation{class: invalidateUrgent, reset: true, producer: "test"})
	awaitCoordinatorScheduledTimer(t, clock).ch <- time.Time{}
	<-oldSendEntered

	replacement := portsmocks.NewMockTransport(t)
	replacement.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	replacement.EXPECT().Close().Return(nil).Maybe()
	attached := make(chan *attachedClient, 1)
	go func() {
		d.mu.Lock()
		attached <- d.finishAttach(sess, replacement, hello.Size, terminalEnv{}, hello)
	}()

	select {
	case ac := <-attached:
		require.Same(t, replacement, ac.transport())
	case <-time.After(time.Second):
		t.Fatal("replacement attach waited for the blocked old render worker")
	}

	// The exact old link is revoked and closed; it cannot be mistaken for the
	// replacement even after the retired worker finally leaves Send.
	d.attachmentCleanupWg.Wait()
	require.Nil(t, old.transport())
	replacement.AssertNotCalled(t, "Close")
}
