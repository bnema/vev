package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

// TestFinishAttachWaitsForAdmittedOldRender verifies role-effect
// linearization: an old render already admitted and blocked in Send completes
// before replacement publication can change its capability.
func TestFinishAttachWaitsForAdmittedOldRender(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	clock := newCoordinatorMockClock(t, 4)
	d := newTestDaemon(t, newFactory(t, pty), clock.clock)

	oldTransport := portsmocks.NewMockTransport(t)
	oldSendEntered := make(chan struct{})
	releaseOldSend := make(chan struct{})
	oldTransport.EXPECT().Send(mock.Anything).RunAndReturn(func(ports.Frame) error {
		close(oldSendEntered)
		<-releaseOldSend
		return nil
	}).Once()
	oldTransport.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	oldTransport.EXPECT().Close().Return(nil).Maybe()

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
		ac, err := d.finishAttach(sess, replacement, hello.Size, terminalEnv{}, hello)
		require.NoError(t, err)
		attached <- ac
	}()

	select {
	case <-attached:
		t.Fatal("replacement published before the admitted old render completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseOldSend)
	select {
	case ac := <-attached:
		require.Same(t, replacement, ac.transport())
	case <-time.After(time.Second):
		t.Fatal("replacement did not publish after the admitted render completed")
	}

	d.attachmentCleanupWg.Wait()
	require.Same(t, oldTransport, old.transport(), "the displaced attachment remains snatched")
	replacement.AssertNotCalled(t, "Close")

	// Retire the session before mock cleanup so asynchronous terminal Detached
	// notification cannot race the clock's C expectation after the test returns.
	releasePTY()
	awaitTestCompletion(t, d.done, "session did not retire after its final PTY closed")
	d.waitNotifies()
}
