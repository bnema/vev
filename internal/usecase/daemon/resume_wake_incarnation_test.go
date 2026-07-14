package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// A wake can have passed coordinator scheduling before a link loss parks its
// attachment. Resume deliberately reuses that attachment object, so pointer
// identity alone must not let the old wake paint the new transport before its
// Welcome frame.
func TestParkedResumeRejectsDispatchedWakeFromPriorAttachment(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})

	oldTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	token := ac.resumeToken
	rc := sess.renderCoordinator()
	require.NotNil(t, rc)
	require.True(t, rc.markAttachmentReady(rc.attachmentLease(ac)))

	// Pause after dispatch but before the coordinator's normal wake callback
	// reaches paint/sendMu. This is the precise stale-wake window.
	originalWake := rc.opts.wake
	wakeDispatched := make(chan struct{})
	releaseWake := make(chan struct{})
	rc.opts.wake = func(w renderWake) {
		close(wakeDispatched)
		<-releaseWake
		originalWake(w)
	}
	wakeDone := make(chan struct{})
	go func() {
		d.invalidateRenderNow(sess, ac, true, "resume-wake-test")
		close(wakeDone)
	}()
	<-wakeDispatched

	d.clientGone(sess, ac, oldTransport, false)
	newTransport := &closeTrackingTransport{}
	resumedSess, resumedAC, ok, err := d.resumeParked(helloResumeCapable(ports.IntentResume, sess.name, token), newTransport, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)

	close(releaseWake)
	<-wakeDone
	require.Empty(t, newTransport.Sends(), "a stale pre-resume wake must not emit Output before Welcome")
	ac.sendMu.Lock()
	staleWakeComposed := ac.pipelineCache.valid
	ac.sendMu.Unlock()
	require.False(t, staleWakeComposed, "a stale wake must not mutate the resumed attachment shadow")

	// The new attachment incarnation accepts the required paint only after its
	// replacement transport has completed Welcome.
	rc.opts.wake = originalWake
	require.True(t, rc.markAttachmentReady(rc.attachmentLease(ac)))
	d.firstPaint(sess, ac, ac.size)
	require.NotEmpty(t, newTransport.Sends())
}
