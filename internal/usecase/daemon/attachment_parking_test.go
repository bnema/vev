package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

type resumeWelcomeFailureTransport struct {
	closeTrackingTransport
}

func (*resumeWelcomeFailureTransport) Send(ports.Frame) error { return errWelcomeSendFailed }

var errWelcomeSendFailed = errors.New("welcome send failed")

func TestAttachmentLossParksOnlyThatAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	firstTransport := &closeTrackingTransport{}
	sess, first, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport := &closeTrackingTransport{}
	_, second, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)

	d.clientGone(sess, first, firstTransport, false)

	d.mu.Lock()
	parked := d.parked[first.resumeToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	sess.mu.Lock()
	_, firstAttached := sess.attachments[first]
	_, secondAttached := sess.attachments[second]
	sess.mu.Unlock()
	require.False(t, firstAttached)
	require.True(t, secondAttached)
	require.Contains(t, sess.snapshotAttachments(), second)
	require.False(t, secondTransport.Closed())
}

func TestTwoAttachmentsParkIndependently(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	firstTransport := &closeTrackingTransport{}
	sess, first, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport := &closeTrackingTransport{}
	_, second, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)
	firstToken, secondToken := first.resumeToken, second.resumeToken

	d.clientGone(sess, first, firstTransport, false)
	d.clientGone(sess, second, secondTransport, false)
	d.mu.Lock()
	firstParked, secondParked := d.parked[firstToken], d.parked[secondToken]
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
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	firstTransport := &closeTrackingTransport{}
	sess, first, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport := &closeTrackingTransport{}
	_, second, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)
	d.clientGone(sess, first, firstTransport, false)
	d.clientGone(sess, second, secondTransport, false)
	firstToken, secondToken := first.resumeToken, second.resumeToken
	d.mu.Lock()
	firstParked := d.parked[firstToken]
	d.mu.Unlock()
	d.expireParked(firstToken, firstParked)
	d.mu.Lock()
	_, firstRetained := d.parked[firstToken]
	_, secondRetained := d.parked[secondToken]
	d.mu.Unlock()
	require.False(t, firstRetained)
	require.True(t, secondRetained)
}

func TestResumeRotatesCredentialAndRejectsStaleTransport(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	oldTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	oldToken := ac.resumeToken
	oldGeneration := ac.connectionGeneration.Load()
	d.clientGone(sess, ac, oldTransport, false)

	newTransport := &closeTrackingTransport{}
	resumed, same, ok, err := d.resumeParked(helloResumeCapable(ports.IntentResume, "work", oldToken), newTransport, defaultSize)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumed)
	require.Same(t, ac, same)
	require.NotEqual(t, oldToken, ac.resumeToken)
	require.Greater(t, ac.connectionGeneration.Load(), oldGeneration)
	require.True(t, d.commitResumeClaim(ac))
	d.mu.Lock()
	_, oldRetained := d.parked[oldToken]
	d.mu.Unlock()
	require.False(t, oldRetained)

	before := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)
	require.Equal(t, before, ac.resumeToken)
	d.mu.Lock()
	_, staleParked := d.parked[oldToken]
	d.mu.Unlock()
	require.False(t, staleParked)
}

func TestSuccessfulResumeRejectsOldCredentialAfterWelcome(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	oldTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	oldToken := ac.resumeToken
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{Generation: 1})
	d.clientGone(sess, ac, oldTransport, false)

	resumedTransport := &closeTrackingTransport{}
	d.handleHello(resumedTransport, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(helloResumeCapable(ports.IntentResume, "work", oldToken))})
	newToken := ac.resumeToken
	require.NotEqual(t, oldToken, newToken)
	d.mu.Lock()
	_, oldRetained := d.parked[oldToken]
	newParked := d.parked[newToken]
	d.mu.Unlock()
	require.False(t, oldRetained)
	require.NotNil(t, newParked)
	require.NotEmpty(t, resumedTransport.Sends())
	require.Equal(t, ports.MsgWelcome, resumedTransport.Sends()[0].Type, "Welcome must remain the first server frame after Hello")
}

func TestFailedResumeHandshakeKeepsParkedCredential(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	oldTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)

	failed := &resumeWelcomeFailureTransport{}
	d.handleHello(failed, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(helloResumeCapable(ports.IntentResume, "work", token))})
	require.Nil(t, ac.transport())
	d.mu.Lock()
	parked := d.parked[token]
	claimed := parked != nil && parked.claimed
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.False(t, claimed)
	require.Equal(t, token, ac.resumeToken)

	_, resumed, ok, err := d.resumeParked(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{}, defaultSize)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, ac, resumed)
}

func TestConcurrentResumeHasExactlyOneWinner(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	oldTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
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
			_, _, ok, resumeErr := d.resumeParked(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{}, defaultSize)
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
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	firstTransport := &closeTrackingTransport{}
	sess, _, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport := &closeTrackingTransport{}
	_, _, err = d.route(helloResumeCapable(ports.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)
	parkedTransport := &closeTrackingTransport{}
	_, parkedAttachment, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), parkedTransport)
	require.NoError(t, err)
	d.clientGone(sess, parkedAttachment, parkedTransport, false)
	parkedToken := parkedAttachment.resumeToken

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	d.waitNotifies()
	require.True(t, firstTransport.Closed())
	require.True(t, secondTransport.Closed())
	require.True(t, parkedTransport.Closed())
	d.mu.Lock()
	_, retained := d.parked[parkedToken]
	d.mu.Unlock()
	require.False(t, retained)
}

func TestResumeCredentialIsInvalidAfterDaemonRestart(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	transport := &closeTrackingTransport{}
	_, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), transport)
	require.NoError(t, err)
	token := ac.resumeToken
	restarted := newTestDaemon(t, newFactory(t, pty), stubClock{})
	_, _, err = restarted.route(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{})
	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, ports.ErrNoSuchSession, protocolErr.code)
}

func TestHandleHelloAllowsMultipleAttachments(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	firstTransport := &closeTrackingTransport{}
	sess, first, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), firstTransport)
	require.NoError(t, err)
	secondTransport := &closeTrackingTransport{}
	_, second, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), secondTransport)
	require.NoError(t, err)
	require.Equal(t, true, sess.attachmentRegistered(first))
	require.Equal(t, true, sess.attachmentRegistered(second))
	require.NotSame(t, first, second)
}
