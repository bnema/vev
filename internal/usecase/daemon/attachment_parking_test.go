package daemon

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
)

var errWelcomeSendFailed = errors.New("welcome send failed")

func TestAttachmentLossParksOnlyThatAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))

	firstTransport := newQuietConnection(t)
	sess, first, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport, secondTransportLog := newTrackedConnection(t)
	_, second, err := d.route(helloResumeCapable(protocol.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)

	d.clientGone(sess, first, firstTransport, false)

	d.mu.Lock()
	parked := d.resume.parked[first.resumeToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	sess.mu.Lock()
	_, firstAttached := sess.attachments[first]
	_, secondAttached := sess.attachments[second]
	sess.mu.Unlock()
	require.False(t, firstAttached)
	require.True(t, secondAttached)
	require.True(t, slices.Contains(sess.snapshotAttachments(), second))
	require.False(t, secondTransportLog.Closed())
}

func TestTwoAttachmentsParkIndependently(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	firstTransport := newQuietConnection(t)
	sess, first, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport := newQuietConnection(t)
	_, second, err := d.route(helloResumeCapable(protocol.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)
	firstToken, secondToken := first.resumeToken, second.resumeToken

	d.clientGone(sess, first, firstTransport, false)
	d.clientGone(sess, second, secondTransport, false)
	d.mu.Lock()
	firstParked, secondParked := d.resume.parked[firstToken], d.resume.parked[secondToken]
	d.mu.Unlock()
	require.NotNil(t, firstParked)
	require.NotNil(t, secondParked)
	require.NotSame(t, firstParked, secondParked)
	sess.mu.Lock()
	require.Empty(t, sess.attachments)
	sess.mu.Unlock()
}

func TestParkExpiryRemovesOnlyOneAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	firstTransport := newQuietConnection(t)
	sess, first, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport := newQuietConnection(t)
	_, second, err := d.route(helloResumeCapable(protocol.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)
	d.clientGone(sess, first, firstTransport, false)
	d.clientGone(sess, second, secondTransport, false)
	firstToken, secondToken := first.resumeToken, second.resumeToken
	d.mu.Lock()
	firstParked := d.resume.parked[firstToken]
	d.mu.Unlock()
	d.expireParked(firstToken, firstParked)
	d.mu.Lock()
	_, firstRetained := d.resume.parked[firstToken]
	_, secondRetained := d.resume.parked[secondToken]
	d.mu.Unlock()
	require.False(t, firstRetained)
	require.True(t, secondRetained)
}

func TestResumeRotatesCredentialAndRejectsStaleTransport(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	oldTransport := newQuietConnection(t)
	sess, ac, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	oldToken := ac.resumeToken
	oldGeneration := ac.lifecycle.generationValue()
	d.clientGone(sess, ac, oldTransport, false)

	newTransport := newQuietConnection(t)
	resumed, same, ok, err := d.resumeParked(helloResumeCapable(protocol.IntentResume, "work", oldToken), newTransport, defaultSize)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumed)
	require.Same(t, ac, same)
	require.NotEqual(t, oldToken, ac.resumeToken)
	require.Greater(t, ac.lifecycle.generationValue(), oldGeneration)
	require.True(t, d.commitResumeClaim(ac))
	d.mu.Lock()
	_, oldRetained := d.resume.parked[oldToken]
	d.mu.Unlock()
	require.False(t, oldRetained)

	before := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)
	require.Equal(t, before, ac.resumeToken)
	d.mu.Lock()
	_, staleParked := d.resume.parked[oldToken]
	d.mu.Unlock()
	require.False(t, staleParked)
}

func TestSuccessfulResumeRejectsOldCredentialAfterWelcome(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	oldTransport := newQuietConnection(t)
	sess, ac, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	oldToken := ac.resumeToken
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1})
	d.clientGone(sess, ac, oldTransport, false)

	resumedTransport, resumedTransportLog := newTrackedConnection(t)
	d.handleHello(resumedTransport, helloResumeCapable(protocol.IntentResume, "work", oldToken))
	newToken := ac.resumeToken
	require.NotEqual(t, oldToken, newToken)
	d.mu.Lock()
	_, oldRetained := d.resume.parked[oldToken]
	newParked := d.resume.parked[newToken]
	d.mu.Unlock()
	require.False(t, oldRetained)
	require.NotNil(t, newParked)
	require.NotEmpty(t, resumedTransportLog.Sends())
	require.Equal(t, "Welcome", envelopeMessageName(t, resumedTransportLog.Sends()[0].Payload), "Welcome must remain the first server frame after Hello")
}

func TestFailedResumeHandshakeKeepsParkedCredential(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	oldTransport := newQuietConnection(t)
	sess, ac, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)

	failed, _ := newTrackedConnection(t, func(c *mockServerConnection, _ *connLog) {
		c.EXPECT().Send(mock.Anything).Return(errWelcomeSendFailed).Maybe()
	})
	d.handleHello(failed, helloResumeCapable(protocol.IntentResume, "work", token))
	require.Nil(t, ac.transport())
	d.mu.Lock()
	parked := d.resume.parked[token]
	claimed := parked != nil && parked.claimed
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.False(t, claimed)
	require.Equal(t, token, ac.resumeToken)

	_, resumed, ok, err := d.resumeParked(helloResumeCapable(protocol.IntentResume, "work", token), newQuietConnection(t), defaultSize)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, ac, resumed)
}

func TestConcurrentResumeHasExactlyOneWinner(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	oldTransport := newQuietConnection(t)
	sess, ac, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)

	type result struct {
		ok  bool
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			_, _, ok, resumeErr := d.resumeParked(helloResumeCapable(protocol.IntentResume, "work", token), newQuietConnection(t), defaultSize)
			results <- result{ok: ok, err: resumeErr}
		}()
	}
	var winners int
	for range 2 {
		got := <-results
		if got.ok {
			winners++
		}
	}
	require.Equal(t, 1, winners)
	require.NotEqual(t, token, ac.resumeToken)
}

func TestKillSessionClosesEveryAttachmentAndParkedCredential(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	firstTransport, firstTransportLog := newTrackedConnection(t)
	sess, _, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport, secondTransportLog := newTrackedConnection(t)
	_, _, err = d.route(helloResumeCapable(protocol.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)
	parkedTransport, parkedTransportLog := newTrackedConnection(t)
	_, parkedAttachment, err := d.route(helloResumeCapable(protocol.IntentAttach, "work", 0), parkedTransport)
	require.NoError(t, err)
	d.clientGone(sess, parkedAttachment, parkedTransport, false)
	parkedToken := parkedAttachment.resumeToken

	require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, false))
	d.waitNotifies()
	require.True(t, firstTransportLog.Closed())
	require.True(t, secondTransportLog.Closed())
	require.True(t, parkedTransportLog.Closed())
	d.mu.Lock()
	_, retained := d.resume.parked[parkedToken]
	d.mu.Unlock()
	require.False(t, retained)
}

func TestResumeCredentialIsInvalidAfterDaemonRestart(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	transport := newQuietConnection(t)
	_, ac, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), transport)
	require.NoError(t, err)
	token := ac.resumeToken
	restarted := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	_, _, err = restarted.route(helloResumeCapable(protocol.IntentResume, "work", token), newQuietConnection(t))
	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, protocol.ErrNoSuchSession, protocolErr.code)
}

func TestHandleHelloAllowsMultipleAttachments(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))
	firstTransport := newQuietConnection(t)
	sess, first, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport := newQuietConnection(t)
	_, second, err := d.route(helloResumeCapable(protocol.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)
	require.Equal(t, true, sess.attachmentRegistered(first))
	require.Equal(t, true, sess.attachmentRegistered(second))
	require.NotSame(t, first, second)
}
