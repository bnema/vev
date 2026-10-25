package daemon

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func TestBoundedHandshakeCompletionCancellationDoesNotDoubleReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := &completionCancellationContext{}
		tr := newQuietConnection(t)
		done := make(chan error, 1)
		go func() {
			done <- boundedHandshakeOperation(ctx, tr, func() error {
				ctx.canceled = true
				return nil
			})
		}()

		err := requireReceived(t, done, "handshake completion/cancellation race deadlocked")
		require.ErrorIs(t, err, context.Canceled)
	})
}

type completionCancellationContext struct{ canceled bool }

func (c *completionCancellationContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *completionCancellationContext) Done() <-chan struct{}       { return nil }
func (c *completionCancellationContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	return nil
}
func (*completionCancellationContext) Value(any) any { return nil }

func TestBoundedHandshakeCancellationDoesNotWaitForUninterruptibleOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tr := newQuietConnection(t)
		started := make(chan struct{})
		release := make(chan struct{})
		operationDone := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			result <- boundedHandshakeOperation(ctx, tr, func() error {
				close(started)
				<-release
				close(operationDone)
				return nil
			})
		}()

		<-started
		cancel()
		err := requireReceived(t, result, "handshake cancellation waited for the transport operation")
		require.ErrorIs(t, err, context.Canceled)
		close(release)
		requireReceived(t, operationDone, "handshake operation did not finish")
	})
}

func TestHandshakeFirstPaintCancellationDoesNotRacePaintResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		paintStarted := make(chan struct{})
		releasePaint := make(chan struct{})
		paintDone := make(chan struct{})
		d.afterAttachmentEffectAdmitted = func(attachmentCapability) {
			close(paintStarted)
			<-ctx.Done()
			<-releasePaint
			close(paintDone)
		}

		tr, trH := newHandshakeBlockingConnection(t, false)
		handshakeDone := make(chan struct{})
		go func() {
			d.handleHelloFrameWithContext(ctx, make(chan struct{}), func() {}, func() {}, tr,
				mustHello(protocol.IntentNew, "paint-cancel", domain.Size{Cols: 80, Rows: 24}))
			close(handshakeDone)
		}()

		requireReceived(t, paintStarted, "first paint did not start")
		cancel()
		requireReceived(t, trH.Done(), "handshake cancellation did not close the transport")
		select {
		case <-paintDone:
			t.Fatal("first paint completed before cancellation returned")
		default:
		}
		close(releasePaint)
		requireReceived(t, paintDone, "delayed first paint did not finish")
		requireReceived(t, handshakeDone, "handshake cancellation did not finish")
	})
}

func TestHandshakeFailureAfterWelcomeDoesNotReportExplicitDetach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, release := newBlockingPTY(t)
		defer release()
		d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
		tr, trLog := newTrackedConnection(t)
		sess, ac, err := d.route(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Name: "welcome-failed", Size: domain.Size{Cols: 80, Rows: 24}, TermEnv: "xterm-256color"}, tr)
		require.NoError(t, err)

		d.failHandshakeAttachment(sess, ac, tr, true)

		var detached *protocol.Detached
		for _, frame := range trLog.Sends() {
			message, decodeErr := sessionwire.DecodeServerEnvelope(frame.Payload)
			require.NoError(t, decodeErr)
			if value, ok := message.(protocol.Detached); ok {
				detached = &value
				break
			}
		}
		require.NotNil(t, detached, "the client needs a lifecycle response after Welcome")
		require.Equal(t, protocol.ReasonServerShutdown, detached.Reason, "a failed destination handshake must return to the picker")
	})
}

func TestHandshakeTimeoutClosesBlockedReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock, clockTimers := newSignalClock(t, 1)
		d := newTestDaemon(t, nil, clock)
		tr, trH := newHandshakeBlockingConnection(t, false)
		done := make(chan struct{})
		go func() {
			d.handleConn(tr)
			close(done)
		}()

		timer := <-clockTimers
		require.Equal(t, protocol.HandshakeTimeout, timer.duration)
		timer.ch <- time.Time{}
		requireReceived(t, done, "handshake receive did not stop after timeout")
		requireClosedHandshakeTransport(t, trH)
		d.mu.Lock()
		require.Empty(t, d.sessions)
		require.Empty(t, d.resume.parked)
		d.mu.Unlock()
	})
}

func TestFailedResumeHandshakeRestoresParkedCredential(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, release := newBlockingPTY(t)
		defer release()
		d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
		oldTransport, _ := newHandshakeBlockingConnection(t, false)
		sess, ac, err := d.route(helloResumeCapable(protocol.IntentNew, "resume-timeout", 0), oldTransport)
		require.NoError(t, err)
		token := ac.resumeToken
		d.clientGone(sess, ac, oldTransport, false)
		require.Empty(t, sess.snapshotAttachments())

		clock, clockTimers := newSignalClock(t, 1)
		d.clock = clock
		resumeTransport, resumeTransportH := newHandshakeBlockingConnection(t, true)
		done := make(chan struct{})
		go func() {
			d.handleHelloFrame(resumeTransport, mustClientEnvelope(helloResumeCapable(protocol.IntentResume, sess.name, token)))
			close(done)
		}()

		timer := <-clockTimers
		require.Equal(t, protocol.HandshakeTimeout, timer.duration)
		<-resumeTransportH.welcome
		timer.ch <- time.Time{}
		requireReceived(t, done, "failed resume handshake did not finish")
		requireClosedHandshakeTransport(t, resumeTransportH)
		d.mu.Lock()
		parked := d.resume.parked[token]
		d.mu.Unlock()
		require.NotNil(t, parked, "failed pre-claim resume must restore the parked credential")
		require.Same(t, ac, parked.ac)
		require.Equal(t, token, ac.resumeToken)
		require.True(t, ac.parked)
		require.Empty(t, sess.snapshotAttachments())
	})
}

func TestHandshakeTimeoutClosesBlockedWelcomeSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, release := newBlockingPTY(t)
		defer release()
		clock, clockTimers := newSignalClock(t, 4)
		d := newTestDaemon(t, newFactory(t, pty), clock)
		tr, trH := newHandshakeBlockingConnection(t, true)
		done := make(chan struct{})
		go func() {
			d.handleHelloFrame(tr, mustHello(protocol.IntentNew, "timeout-send", domain.Size{Cols: 80, Rows: 24}))
			close(done)
		}()

		timer := <-clockTimers
		require.Equal(t, protocol.HandshakeTimeout, timer.duration)
		<-trH.welcome
		timer.ch <- time.Time{}
		requireReceived(t, done, "handshake send did not stop after timeout")
		requireClosedHandshakeTransport(t, trH)
		d.mu.Lock()
		require.Empty(t, d.sessions, "a timed-out newly-created handshake must not leave an empty session")
		require.Empty(t, d.resume.parked)
		d.mu.Unlock()
	})
}

func requireClosedHandshakeTransport(t *testing.T, tr *handshakeLog) {
	t.Helper()
	select {
	case <-tr.Done():
	default:
		t.Fatal("handshake timeout did not close transport")
	}
}

func TestHandshakeTimeoutCancelsRouteRestoreWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock, clockTimers := newSignalClock(t, 1)
		d := newTestDaemon(t, nil, clock)
		restoreDone := make(chan struct{})
		d.mu.Lock()
		d.inactive["restoring"] = inactiveSession{name: "restoring", restoreDone: restoreDone}
		d.mu.Unlock()

		tr, trH := newHandshakeBlockingConnection(t, false)
		done := make(chan struct{})
		go func() {
			d.handleHelloFrame(tr, mustHello(protocol.IntentAttach, "restoring", domain.Size{Cols: 80, Rows: 24}))
			close(done)
		}()

		timer := <-clockTimers
		require.Equal(t, protocol.HandshakeTimeout, timer.duration)
		timer.ch <- time.Time{}
		requireReceived(t, done, "route restore wait did not stop after handshake timeout")
		requireClosedHandshakeTransport(t, trH)
		d.mu.Lock()
		require.Empty(t, d.sessions)
		require.Empty(t, d.resume.parked)
		d.mu.Unlock()
	})
}

func TestHandshakeTimeoutRemovesRestoredEmptySession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, release := newBlockingPTY(t)
		defer release()
		d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
		d.mu.Lock()
		d.inactive["restored"] = inactiveSession{name: "restored", cwd: "/tmp", state: protocol.SessionDown}
		d.mu.Unlock()

		clock, clockTimers := newSignalClock(t, 4)
		d.clock = clock
		tr, trH := newHandshakeBlockingConnection(t, true)
		done := make(chan struct{})
		go func() {
			d.handleHelloFrame(tr, mustHello(protocol.IntentAttach, "restored", domain.Size{Cols: 80, Rows: 24}))
			close(done)
		}()

		timer := <-clockTimers
		require.Equal(t, protocol.HandshakeTimeout, timer.duration)
		<-trH.welcome
		timer.ch <- time.Time{}
		requireReceived(t, done, "timed-out restored attachment cleanup did not finish")
		requireClosedHandshakeTransport(t, trH)
		d.mu.Lock()
		require.Empty(t, d.sessions)
		_, retained := d.inactive["restored"]
		d.mu.Unlock()
		require.True(t, retained, "failed attach must retain the stopped session authority")
	})
}

func TestHandshakeTimeoutPreservesUnrelatedAttachment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, release := newBlockingPTY(t)
		defer release()
		d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
		oldTransport, oldTransportLog := newTrackedConnection(t)
		sess, old, err := d.route(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Name: "shared", Size: domain.Size{Cols: 80, Rows: 24}}, oldTransport)
		require.NoError(t, err)

		clock, clockTimers := newSignalClock(t, 1)
		d.clock = clock
		tr, trH := newHandshakeBlockingConnection(t, true)
		done := make(chan struct{})
		go func() {
			d.handleHelloFrame(tr, mustHello(protocol.IntentAttach, "shared", domain.Size{Cols: 80, Rows: 24}))
			close(done)
		}()

		timer := <-clockTimers
		require.Equal(t, protocol.HandshakeTimeout, timer.duration)
		<-trH.welcome
		timer.ch <- time.Time{}
		requireReceived(t, done, "timed-out attachment cleanup did not finish")
		requireClosedHandshakeTransport(t, trH)
		require.False(t, oldTransportLog.Closed(), "cleanup closed an unrelated attachment transport")
		require.Same(t, sess, old.currentAttachmentSession())
		require.Equal(t, []*attachedClient{old}, sess.snapshotAttachments())
		d.mu.Lock()
		require.Len(t, d.sessions, 1)
		require.Empty(t, d.resume.parked)
		d.mu.Unlock()
		_ = d.killSession(sess, protocol.ReasonServerShutdown, false)
	})
}

func TestSuccessfulRouteOutlivesStoppedHandshakeContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, release := newBlockingPTY(t)
		defer release()
		d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
		ctx, cancel := context.WithCancel(context.Background())
		sess, ac, err := d.routeWithContext(ctx, protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Name: "lifetime", Size: domain.Size{Cols: 80, Rows: 24}}, newQuietConnection(t))
		require.NoError(t, err)
		cancel()
		select {
		case <-sess.ctx.Done():
			t.Fatal("session lifetime was parented to the handshake context")
		default:
		}
		require.NotNil(t, ac)
		_ = d.killSession(sess, protocol.ReasonServerShutdown, false)
	})
}
