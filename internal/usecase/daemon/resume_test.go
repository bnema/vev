package daemon

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/vt"
)

type closeTrackingTransport struct {
	mu     sync.Mutex
	closed bool
	sends  []ports.Frame
}

func (t *closeTrackingTransport) Send(f ports.Frame) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sends = append(t.sends, f)
	return nil
}
func (t *closeTrackingTransport) Recv() (ports.Frame, error) {
	return ports.Frame{}, errors.New("closed")
}
func (t *closeTrackingTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}
func (t *closeTrackingTransport) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *closeTrackingTransport) Sends() []ports.Frame {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]ports.Frame(nil), t.sends...)
}

type closeCountingBlockedTransport struct {
	closeOnce sync.Once
	closed    chan struct{}
	count     int
	mu        sync.Mutex
}

func newCloseCountingBlockedTransport() *closeCountingBlockedTransport {
	return &closeCountingBlockedTransport{closed: make(chan struct{})}
}

func (t *closeCountingBlockedTransport) Send(ports.Frame) error {
	<-t.closed
	return errors.New("closed")
}

func (t *closeCountingBlockedTransport) Recv() (ports.Frame, error) {
	return ports.Frame{}, errors.New("closed")
}

func (t *closeCountingBlockedTransport) Close() error {
	t.mu.Lock()
	t.count++
	t.mu.Unlock()
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (t *closeCountingBlockedTransport) CloseCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.count
}

func helloResumeCapable(intent uint8, name string, token uint64) ports.Hello {
	return ports.Hello{
		Version:     ports.ProtocolVersion,
		Intent:      intent,
		ClientID:    [16]byte{1, 2, 3, 4},
		ResumeToken: token,
		Name:        name,
		Size:        domain.Size{Cols: 80, Rows: 24},
		TermEnv:     "xterm-256color",
	}
}

func TestNamedLinkLossParks(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)

	d.clientGone(sess, ac, ac.transport(), false)
	require.Equal(t, 1, sessionCount(d), "named session survives parked link loss")
	require.Nil(t, sess.client)
	d.mu.Lock()
	_, parked := d.parked[token]
	d.mu.Unlock()
	require.True(t, parked, "named resume-capable link loss is parked")
}

func TestBoundedSendTimeoutCannotTargetResumedTransport(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	oldTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "bounded-send-resume", 0), oldTransport)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)

	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	ac.sendMu.Lock()
	expected := ac.transportSnapshot()
	orphanDone := make(chan struct{})
	type boundedResult struct {
		transport ports.Transport
		err       error
	}
	result := make(chan boundedResult, 1)
	go func() {
		transport, sendErr := d.boundedSendWithTimeout(time.Second, expected.transport, func() error {
			defer close(orphanDone)
			return ac.sendExpectedTransport(expected, frameDetached(ports.ReasonDetach))
		})
		result <- boundedResult{transport: transport, err: sendErr}
	}()

	// The worker is parked behind sendMu, so it has not observed a transport.
	// Expire its deadline before resuming this same attachment with a new link.
	timer := <-clock.timers
	timer.ch <- time.Time{}
	got := <-result
	require.ErrorIs(t, got.err, errSendTimedOut)
	require.Same(t, oldTransport, got.transport)
	require.NoError(t, ac.closeCapturedTransport(got.transport))

	newTransport := &closeTrackingTransport{}
	d.mu.Lock()
	resumedSess, resumedAC, ok, err := d.resumeParkedLocked(
		helloResumeCapable(ports.IntentResume, sess.name, token),
		newTransport,
		domain.Size{Cols: 80, Rows: 24},
	)
	d.mu.Unlock()
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)

	ac.sendMu.Unlock()
	<-orphanDone
	require.Empty(t, newTransport.Sends(), "orphaned send must not write to the resumed transport")
	require.False(t, newTransport.Closed(), "orphaned send must not close the resumed transport")
}

func TestDetachedNotificationTimeoutClosesCapturedTransportOnce(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)
	tr := newCloseCountingBlockedTransport()
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}

	d.notifyDetachedSnapshotAsync(detachedAttachmentSnapshot{
		ac:        ac,
		transport: ac.transportSnapshot(),
	}, ports.ReasonDetach)
	timer := <-clock.timers
	timer.ch <- time.Time{}
	d.waitNotifies()

	require.Equal(t, 1, tr.CloseCount(), "timeout revocation must close the captured transport at most once")
	require.Nil(t, ac.transport(), "timeout cleanup must revoke the captured transport")
}

func TestHandleHelloResumeDefersFreshOutputUntilWelcome(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	clock := newCoordinatorMockClock(t, 4)
	d := newTestDaemon(t, newFactorySeq(t, pty), clock.clock)
	oldTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "resume-welcome-gate", 0), oldTransport)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)

	tr := newWelcomeBlockingTransport(t)
	done := make(chan struct{})
	resumeHello := helloResumeCapable(ports.IntentResume, sess.name, token)
	go func() {
		d.handleHello(tr.tr, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(resumeHello)})
		close(done)
	}()

	<-tr.welcomeEntered
	welcome := <-tr.sends
	require.Equal(t, ports.MsgWelcome, welcome.Type)
	sess.mu.Lock()
	resumed := sess.client
	sess.mu.Unlock()
	require.Same(t, ac, resumed)

	d.invalidateRender(sess, resumed, true, "resume-welcome-gate-test")
	timer := awaitLatestCoordinatorTimer(t, clock)
	rc := sess.renderCoordinator()
	rc.mu.Lock()
	workerDone := rc.normalLane.token.done
	rc.mu.Unlock()
	require.NotNil(t, workerDone)
	timer.ch <- time.Time{}
	awaitTestCompletion(t, workerDone, "coordinator deadline worker did not complete")
	requireNoCoordinatorOutputFrame(t, tr.sends)

	tr.release()
	output := awaitFrame(t, tr.sends, ports.MsgOutput)
	first, err := ports.UnmarshalOutput(output.Payload)
	require.NoError(t, err)
	require.Zero(t, first.BaseStateNum)
	tr.finish()
	<-done
	requireNoCoordinatorOutputFrame(t, tr.sends)
}

func TestEphemeralLinkLossParksAndResumes(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})

	tr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentEphemeral, "", 0), tr)
	require.NoError(t, err)
	require.True(t, sess.ephemeral)
	token := ac.resumeToken
	require.NotZero(t, token, "ephemeral sessions receive resume tokens")

	d.clientGone(sess, ac, ac.transport(), false)
	require.Equal(t, 1, sessionCount(d), "ephemeral link loss keeps session alive")
	require.Nil(t, sess.client)
	d.mu.Lock()
	_, parked := d.parked[token]
	d.mu.Unlock()
	require.True(t, parked, "ephemeral resume-capable link loss is parked")

	newTr := &closeTrackingTransport{}
	resumedSess, resumedAC, err := d.route(helloResumeCapable(ports.IntentResume, sess.name, token), newTr)
	require.NoError(t, err)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)
	require.NotEqual(t, token, resumedAC.resumeToken, "resume rotates token")
	require.Same(t, resumedAC, sess.client)
}

// TestResumeParkedTokenReplacedDuringWaitFailsClosed covers the lifecycle race
// where a parked credential is consumed or replaced while IntentResume waits on
// the attachment send lock. The nonzero token must fail closed instead of
// falling through to ordinary attach/create routing.
func TestResumeParkedTokenReplacedDuringWaitFailsClosed(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)
	d.clientGone(sess, ac, oldTr, false)
	require.Nil(t, sess.client)
	d.mu.Lock()
	parked := d.parked[token]
	sessionsBefore := len(d.sessions)
	d.mu.Unlock()
	require.NotNil(t, parked)

	reachedLookup := make(chan struct{})
	releaseLookup := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseLookup) }) })
	d.beforeResumeParkedSendMu = func() {
		close(reachedLookup)
		<-releaseLookup
	}

	resumeTr := &closeTrackingTransport{}
	type routeResult struct {
		sess *session
		ac   *attachedClient
		err  error
	}
	result := make(chan routeResult, 1)
	go func() {
		routedSess, routedAC, routeErr := d.route(helloResumeCapable(ports.IntentResume, "work", token), resumeTr)
		result <- routeResult{sess: routedSess, ac: routedAC, err: routeErr}
	}()
	awaitTestCompletion(t, reachedLookup, "resumeParked did not pause after the parked lookup")

	d.mu.Lock()
	require.Same(t, parked, d.parked[token], "fixture: token must still be parked at the seam")
	d.removeParkedLocked(token, parked)
	require.Nil(t, d.parked[token])
	require.Same(t, sess, d.sessions[sess.id], "fixture: named session must remain registered")
	d.mu.Unlock()

	releaseOnce.Do(func() { close(releaseLookup) })
	got := awaitTestValue(t, result, "IntentResume did not finish after parked token replacement")

	require.Error(t, got.err)
	require.Nil(t, got.sess)
	require.Nil(t, got.ac)
	var pe *protoErr
	require.ErrorAs(t, got.err, &pe)
	require.Equal(t, ports.ErrNoSuchSession, pe.code)
	require.Contains(t, pe.Error(), "resume token is no longer valid")
	require.Nil(t, sess.client, "lifecycle-race resume must not take over the named session")
	require.Empty(t, resumeTr.Sends(), "failed resume must not complete a Welcome handshake")
	d.mu.Lock()
	_, stillParked := d.parked[token]
	sessionsAfter := len(d.sessions)
	d.mu.Unlock()
	require.False(t, stillParked, "consumed token must stay invalid")
	require.Equal(t, sessionsBefore, sessionsAfter, "lifecycle-race resume must not create a session")
}

// TestResumeLiveAttachmentParkedResumeRaceFailsClosed covers the live-recovery
// path that parks a still-active credential and then loses a competing
// resumeParked race before sendMu revalidation. The lifecycle sentinel must
// surface as fail-closed ErrNoSuchSession, never as a raw ErrInternal leak to
// handleHello.
func TestResumeLiveAttachmentParkedResumeRaceFailsClosed(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)
	require.Same(t, ac, sess.client)
	d.mu.Lock()
	_, parkedAtStart := d.parked[token]
	sessionsBefore := len(d.sessions)
	d.mu.Unlock()
	require.False(t, parkedAtStart, "fixture: live attachment must not be parked yet")

	reachedLookup := make(chan struct{})
	releaseLookup := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseLookup) }) })
	d.beforeResumeParkedSendMu = func() {
		close(reachedLookup)
		<-releaseLookup
	}

	resumeTr := &closeTrackingTransport{}
	type routeResult struct {
		sess *session
		ac   *attachedClient
		err  error
	}
	result := make(chan routeResult, 1)
	go func() {
		routedSess, routedAC, routeErr := d.route(helloResumeCapable(ports.IntentResume, "work", token), resumeTr)
		result <- routeResult{sess: routedSess, ac: routedAC, err: routeErr}
	}()
	awaitTestCompletion(t, reachedLookup, "live resume did not pause inside resumeParked after parking")

	d.mu.Lock()
	parked := d.parked[token]
	require.NotNil(t, parked, "fixture: live recovery must have parked before the sendMu seam")
	require.Same(t, ac, parked.ac)
	d.removeParkedLocked(token, parked)
	require.Nil(t, d.parked[token])
	require.Same(t, sess, d.sessions[sess.id], "fixture: named session must remain registered")
	d.mu.Unlock()

	releaseOnce.Do(func() { close(releaseLookup) })
	got := awaitTestValue(t, result, "live IntentResume did not finish after competing resume consumed the token")

	require.Error(t, got.err)
	require.Nil(t, got.sess)
	require.Nil(t, got.ac)
	require.False(t, errors.Is(got.err, errResumeTokenLifecycleRace), "raw lifecycle sentinel must not reach route callers")
	var pe *protoErr
	require.ErrorAs(t, got.err, &pe)
	require.Equal(t, ports.ErrNoSuchSession, pe.code)
	require.Contains(t, pe.Error(), "resume token is no longer valid")
	require.Nil(t, sess.client, "losing live resume must not take over the named session")
	require.Empty(t, resumeTr.Sends(), "failed live resume must not complete a Welcome handshake")
	d.mu.Lock()
	_, stillParked := d.parked[token]
	sessionsAfter := len(d.sessions)
	d.mu.Unlock()
	require.False(t, stillParked, "consumed token must stay invalid")
	require.Equal(t, sessionsBefore, sessionsAfter, "losing live resume must not create a session")
}

// TestResumeActiveTokenBeforeParkRecoversSameAttachment covers the laptop-sleep
// reconnect race: IntentResume arrives with the live attachment's token before
// transport teardown has parked it. Same client must recover; unknown and
// mismatched credentials stay fail-closed.
func TestResumeActiveTokenBeforeParkRecoversSameAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)
	require.True(t, ac.resumeCapable)
	require.Same(t, ac, sess.client)
	d.mu.Lock()
	_, parked := d.parked[token]
	d.mu.Unlock()
	require.False(t, parked, "fixture: live attachment must not be parked yet")

	_, _, err = d.route(helloResumeCapable(ports.IntentResume, "work", 0xdecafbad), &closeTrackingTransport{})
	require.Error(t, err)
	var unknownErr *protoErr
	require.ErrorAs(t, err, &unknownErr)
	require.Equal(t, ports.ErrNoSuchSession, unknownErr.code)
	require.Contains(t, unknownErr.Error(), "resume token is no longer valid")
	require.Same(t, ac, sess.client)
	require.Equal(t, token, ac.resumeToken)
	require.Same(t, oldTr, ac.transport())

	wrongClient := helloResumeCapable(ports.IntentResume, "work", token)
	wrongClient.ClientID = [16]byte{9, 9, 9, 9}
	_, _, err = d.route(wrongClient, &closeTrackingTransport{})
	require.Error(t, err)
	var mismatchErr *protoErr
	require.ErrorAs(t, err, &mismatchErr)
	require.Equal(t, ports.ErrNoSuchSession, mismatchErr.code)
	require.Contains(t, mismatchErr.Error(), "resume token is no longer valid")
	require.Same(t, ac, sess.client)
	require.Equal(t, token, ac.resumeToken)
	require.Same(t, oldTr, ac.transport())

	newTr := &closeTrackingTransport{}
	resumedSess, resumedAC, err := d.route(helloResumeCapable(ports.IntentResume, "work", token), newTr)
	require.NoError(t, err, "same-token same-client IntentResume must recover before parking")
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)
	require.Same(t, ac, sess.client)
	require.Same(t, newTr, ac.transport())
	require.NotEqual(t, token, ac.resumeToken, "successful resume rotates the credential")
	d.mu.Lock()
	_, oldTokenParked := d.parked[token]
	d.mu.Unlock()
	require.False(t, oldTokenParked, "consumed live credential must not remain resumeable")

	d.clientGone(sess, ac, oldTr, false)
	require.Same(t, ac, sess.client, "retired pre-park transport must not detach the rebound attachment")
	require.Same(t, newTr, ac.transport())
	require.False(t, newTr.Closed())
}

// TestResumeDuringTeardownBeforeParkRecoversSameAttachment covers the opposite
// interleaving from live-before-park recovery: old-link teardown wins
// detachIfCurrentTransport, then pauses before parkAttachment publishes the
// resume token. The parking-in-flight marker must already be visible in that
// gap (published before detach). Same-client IntentResume must wait it out and
// recover; unknown tokens stay fail-closed.
func TestResumeDuringTeardownBeforeParkRecoversSameAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)
	require.Same(t, ac, sess.client)

	reachedGap := make(chan struct{})
	releaseGap := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseGap) }) })
	d.afterClientGoneDetach = func() {
		close(reachedGap)
		<-releaseGap
	}

	goneDone := make(chan struct{})
	go func() {
		d.clientGone(sess, ac, oldTr, false)
		close(goneDone)
	}()
	awaitTestCompletion(t, reachedGap, "teardown did not pause after detach before park")

	require.Nil(t, sess.client, "fixture: detach must have cleared the live owner")
	d.mu.Lock()
	parkingInGap := d.parking[token]
	_, parkedInGap := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parkingInGap, "fixture: parking marker must precede detach publication")
	require.Same(t, ac, parkingInGap.ac)
	require.Same(t, sess, parkingInGap.sess)
	require.False(t, parkedInGap, "fixture: park must not have published yet")

	_, _, err = d.route(helloResumeCapable(ports.IntentResume, "work", 0xdecafbad), &closeTrackingTransport{})
	require.Error(t, err)
	var unknownErr *protoErr
	require.ErrorAs(t, err, &unknownErr)
	require.Equal(t, ports.ErrNoSuchSession, unknownErr.code)
	require.Contains(t, unknownErr.Error(), "resume token is no longer valid")

	wrongClient := helloResumeCapable(ports.IntentResume, "work", token)
	wrongClient.ClientID = [16]byte{9, 9, 9, 9}
	_, _, err = d.route(wrongClient, &closeTrackingTransport{})
	require.Error(t, err)
	var mismatchErr *protoErr
	require.ErrorAs(t, err, &mismatchErr)
	require.Equal(t, ports.ErrNoSuchSession, mismatchErr.code)
	require.Contains(t, mismatchErr.Error(), "resume token is no longer valid")

	newTr := &closeTrackingTransport{}
	type routeResult struct {
		sess *session
		ac   *attachedClient
		err  error
	}
	waiterArmed := make(chan struct{})
	d.afterParkingWaitArmed = func() { close(waiterArmed) }
	result := make(chan routeResult, 1)
	go func() {
		routedSess, routedAC, routeErr := d.route(helloResumeCapable(ports.IntentResume, "work", token), newTr)
		result <- routeResult{sess: routedSess, ac: routedAC, err: routeErr}
	}()
	awaitTestCompletion(t, waiterArmed, "resume waiter did not arm on parking marker before park")

	releaseOnce.Do(func() { close(releaseGap) })
	awaitTestCompletion(t, goneDone, "teardown did not finish after park release")
	got := awaitTestValue(t, result, "IntentResume did not finish after teardown-before-park gap")

	require.NoError(t, got.err, "same-client token must recover across detach-before-park")
	require.Same(t, sess, got.sess)
	require.Same(t, ac, got.ac)
	require.Same(t, ac, sess.client)
	require.Same(t, newTr, ac.transport())
	require.NotEqual(t, token, ac.resumeToken, "successful resume rotates the credential")
	require.True(t, oldTr.Closed(), "teardown/resume must retire the old transport")
	require.False(t, newTr.Closed(), "rebound transport must survive")
	d.mu.Lock()
	_, oldTokenParked := d.parked[token]
	_, stillParking := d.parking[token]
	d.mu.Unlock()
	require.False(t, oldTokenParked, "consumed credential must not remain resumeable")
	require.False(t, stillParking, "parking marker must be consumed after park/resume")

	d.clientGone(sess, ac, oldTr, false)
	require.Same(t, ac, sess.client, "stale old-link cleanup must not detach the rebound attachment")
	require.Same(t, newTr, ac.transport())
	require.False(t, newTr.Closed())
}

// TestConcurrentLiveResumesWaitParkingMarkerBeforePark covers two same-token
// IntentResume handshakes where the winner of detachIfCurrentTransport pauses
// before parkAttachment. The parking marker must already be published so the
// loser waits instead of treating the credential as unknown across that gap.
func TestConcurrentLiveResumesWaitParkingMarkerBeforePark(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)
	require.Same(t, ac, sess.client)

	reachedGap := make(chan struct{})
	releaseGap := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseGap) }) })
	d.afterResumeLiveDetach = func() {
		close(reachedGap)
		<-releaseGap
	}

	type routeResult struct {
		sess *session
		ac   *attachedClient
		err  error
	}
	firstTr := &closeTrackingTransport{}
	firstResult := make(chan routeResult, 1)
	go func() {
		routedSess, routedAC, routeErr := d.route(helloResumeCapable(ports.IntentResume, "work", token), firstTr)
		firstResult <- routeResult{sess: routedSess, ac: routedAC, err: routeErr}
	}()
	awaitTestCompletion(t, reachedGap, "first live resume did not pause after detach before park")

	require.Nil(t, sess.client, "fixture: winning resume must have cleared the live owner")
	d.mu.Lock()
	parkingInGap := d.parking[token]
	_, parkedInGap := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parkingInGap, "fixture: parking marker must precede detach for concurrent resumes")
	require.Same(t, ac, parkingInGap.ac)
	require.False(t, parkedInGap, "fixture: park must not have published yet")

	_, _, err = d.route(helloResumeCapable(ports.IntentResume, "work", 0xdecafbad), &closeTrackingTransport{})
	require.Error(t, err)
	var unknownErr *protoErr
	require.ErrorAs(t, err, &unknownErr)
	require.Equal(t, ports.ErrNoSuchSession, unknownErr.code)

	secondTr := &closeTrackingTransport{}
	secondWaiting := make(chan struct{})
	d.afterParkingWaitArmed = func() {
		close(secondWaiting)
	}
	secondResult := make(chan routeResult, 1)
	go func() {
		routedSess, routedAC, routeErr := d.route(helloResumeCapable(ports.IntentResume, "work", token), secondTr)
		secondResult <- routeResult{sess: routedSess, ac: routedAC, err: routeErr}
	}()
	awaitTestCompletion(t, secondWaiting, "second live resume did not arm parking wait")

	releaseOnce.Do(func() { close(releaseGap) })
	first := awaitTestValue(t, firstResult, "first concurrent IntentResume did not finish")
	second := awaitTestValue(t, secondResult, "second concurrent IntentResume did not finish")

	var winner, loser routeResult
	var winnerTr, loserTr *closeTrackingTransport
	switch {
	case first.err == nil && second.err != nil:
		winner, loser = first, second
		winnerTr, loserTr = firstTr, secondTr
	case second.err == nil && first.err != nil:
		winner, loser = second, first
		winnerTr, loserTr = secondTr, firstTr
	default:
		t.Fatalf("expected exactly one concurrent resume to succeed; first=%v second=%v", first.err, second.err)
	}

	require.Same(t, sess, winner.sess)
	require.Same(t, ac, winner.ac)
	require.Same(t, ac, sess.client)
	require.Same(t, winnerTr, ac.transport())
	require.NotEqual(t, token, ac.resumeToken, "successful resume rotates the credential")
	require.True(t, oldTr.Closed(), "winning resume must retire the old transport")
	require.False(t, winnerTr.Closed(), "winning rebound transport must survive")

	var loserErr *protoErr
	require.ErrorAs(t, loser.err, &loserErr)
	require.Equal(t, ports.ErrNoSuchSession, loserErr.code)
	require.Contains(t, loserErr.Error(), "resume token is no longer valid")
	require.True(t, loserTr.Closed() || len(loserTr.Sends()) == 0, "losing resume must not complete Welcome")

	d.mu.Lock()
	_, oldTokenParked := d.parked[token]
	_, stillParking := d.parking[token]
	d.mu.Unlock()
	require.False(t, oldTokenParked, "consumed credential must not remain resumeable")
	require.False(t, stillParking, "parking marker must be consumed after park/resume")

	d.clientGone(sess, ac, oldTr, false)
	require.Same(t, ac, sess.client, "stale old-link cleanup must not detach the rebound attachment")
	require.Same(t, winnerTr, ac.transport())
	require.False(t, winnerTr.Closed())
}

func setupResumableSnatchedAttachment(t *testing.T, d *Daemon) (*session, *attachedClient, *attachedClient, *closeTrackingTransport, uint64) {
	t.Helper()
	oldTr := &closeTrackingTransport{}
	sess, waiting, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := waiting.resumeToken
	require.NotZero(t, token)

	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), &closeTrackingTransport{})
	require.NoError(t, err)
	d.attachmentCleanupWg.Wait()
	require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting))
	require.Same(t, active, sess.client)
	return sess, waiting, active, oldTr, token
}

// TestSnatchedResumeDuringTeardownBeforeParkRecoversSameAttachment covers the
// snatched unroute→park gap: parkSnatchedAttachment publishes the parking
// marker before ownership removal, then pauses before parkAttachmentAs.
// Same-client IntentResume must wait it out and recover as snatched without
// replacing the active owner; unknown/mismatched tokens stay fail-closed.
func TestSnatchedResumeDuringTeardownBeforeParkRecoversSameAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	sess, waiting, active, oldTr, token := setupResumableSnatchedAttachment(t, d)
	roleToken := sess.attachmentToken(waiting, oldTr)

	reachedGap := make(chan struct{})
	releaseGap := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseGap) }) })
	d.afterSnatchedUnrouteBeforePark = func() {
		close(reachedGap)
		<-releaseGap
	}

	parkDone := make(chan bool, 1)
	go func() {
		parkDone <- d.parkSnatchedAttachment(roleToken)
	}()
	awaitTestCompletion(t, reachedGap, "snatched park did not pause after unroute before park")

	require.Equal(t, attachmentDetached, sess.attachmentRole(waiting), "fixture: unroute must have cleared snatched ownership")
	d.mu.Lock()
	parkingInGap := d.parking[token]
	_, parkedInGap := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parkingInGap, "fixture: parking marker must precede snatched ownership removal")
	require.Same(t, waiting, parkingInGap.ac)
	require.Same(t, sess, parkingInGap.sess)
	require.False(t, parkedInGap, "fixture: park must not have published yet")
	require.Same(t, active, sess.client, "fixture: active owner must remain undisturbed")

	_, _, err := d.route(helloResumeCapable(ports.IntentResume, "work", 0xdecafbad), &closeTrackingTransport{})
	require.Error(t, err)
	var unknownErr *protoErr
	require.ErrorAs(t, err, &unknownErr)
	require.Equal(t, ports.ErrNoSuchSession, unknownErr.code)
	require.Contains(t, unknownErr.Error(), "resume token is no longer valid")

	wrongClient := helloResumeCapable(ports.IntentResume, "work", token)
	wrongClient.ClientID = [16]byte{9, 9, 9, 9}
	_, _, err = d.route(wrongClient, &closeTrackingTransport{})
	require.Error(t, err)
	var mismatchErr *protoErr
	require.ErrorAs(t, err, &mismatchErr)
	require.Equal(t, ports.ErrNoSuchSession, mismatchErr.code)
	require.Contains(t, mismatchErr.Error(), "resume token is no longer valid")

	newTr := &closeTrackingTransport{}
	type routeResult struct {
		sess *session
		ac   *attachedClient
		err  error
	}
	waiterArmed := make(chan struct{})
	d.afterParkingWaitArmed = func() { close(waiterArmed) }
	result := make(chan routeResult, 1)
	go func() {
		routedSess, routedAC, routeErr := d.route(helloResumeCapable(ports.IntentResume, "work", token), newTr)
		result <- routeResult{sess: routedSess, ac: routedAC, err: routeErr}
	}()
	awaitTestCompletion(t, waiterArmed, "resume waiter did not arm on snatched parking marker before park")

	releaseOnce.Do(func() { close(releaseGap) })
	require.True(t, awaitTestValue(t, parkDone, "snatched park did not finish after release"), "parkSnatchedAttachment must succeed")
	got := awaitTestValue(t, result, "IntentResume did not finish after snatched teardown-before-park gap")

	require.NoError(t, got.err, "same-client snatched token must recover across unroute-before-park")
	require.Same(t, sess, got.sess)
	require.Same(t, waiting, got.ac)
	require.Same(t, active, sess.client, "snatched resume must not replace the active owner")
	require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting))
	require.Same(t, newTr, waiting.transport())
	require.NotEqual(t, token, waiting.resumeToken, "successful resume rotates the credential")
	require.True(t, oldTr.Closed(), "teardown/resume must retire the old transport")
	require.False(t, newTr.Closed(), "rebound transport must survive")
	d.mu.Lock()
	_, oldTokenParked := d.parked[token]
	_, stillParking := d.parking[token]
	d.mu.Unlock()
	require.False(t, oldTokenParked, "consumed credential must not remain resumeable")
	require.False(t, stillParking, "parking marker must be consumed after park/resume")
}

// TestConcurrentSnatchedResumesWaitParkingMarkerBeforePark covers two same-token
// IntentResume handshakes while parkSnatchedAttachment pauses after unroute and
// before park. The parking marker must already be published so losers wait
// instead of treating the credential as unknown across that gap.
func TestConcurrentSnatchedResumesWaitParkingMarkerBeforePark(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	sess, waiting, active, oldTr, token := setupResumableSnatchedAttachment(t, d)
	roleToken := sess.attachmentToken(waiting, oldTr)

	reachedGap := make(chan struct{})
	releaseGap := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseGap) }) })
	d.afterSnatchedUnrouteBeforePark = func() {
		close(reachedGap)
		<-releaseGap
	}

	parkDone := make(chan bool, 1)
	go func() {
		parkDone <- d.parkSnatchedAttachment(roleToken)
	}()
	awaitTestCompletion(t, reachedGap, "snatched park did not pause after unroute before park")

	require.Equal(t, attachmentDetached, sess.attachmentRole(waiting), "fixture: unroute must have cleared snatched ownership")
	d.mu.Lock()
	parkingInGap := d.parking[token]
	_, parkedInGap := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parkingInGap, "fixture: parking marker must precede unroute for concurrent resumes")
	require.Same(t, waiting, parkingInGap.ac)
	require.False(t, parkedInGap, "fixture: park must not have published yet")

	_, _, err := d.route(helloResumeCapable(ports.IntentResume, "work", 0xdecafbad), &closeTrackingTransport{})
	require.Error(t, err)
	var unknownErr *protoErr
	require.ErrorAs(t, err, &unknownErr)
	require.Equal(t, ports.ErrNoSuchSession, unknownErr.code)

	type routeResult struct {
		sess *session
		ac   *attachedClient
		err  error
	}
	firstTr := &closeTrackingTransport{}
	secondTr := &closeTrackingTransport{}
	bothWaitersArmed := make(chan struct{})
	var armedCount atomic.Int32
	d.afterParkingWaitArmed = func() {
		if armedCount.Add(1) == 2 {
			close(bothWaitersArmed)
		}
	}
	firstResult := make(chan routeResult, 1)
	secondResult := make(chan routeResult, 1)
	go func() {
		routedSess, routedAC, routeErr := d.route(helloResumeCapable(ports.IntentResume, "work", token), firstTr)
		firstResult <- routeResult{sess: routedSess, ac: routedAC, err: routeErr}
	}()
	go func() {
		routedSess, routedAC, routeErr := d.route(helloResumeCapable(ports.IntentResume, "work", token), secondTr)
		secondResult <- routeResult{sess: routedSess, ac: routedAC, err: routeErr}
	}()
	awaitTestCompletion(t, bothWaitersArmed, "both snatched resume waiters did not arm on parking marker before park")

	releaseOnce.Do(func() { close(releaseGap) })
	require.True(t, awaitTestValue(t, parkDone, "snatched park did not finish after release"))
	first := awaitTestValue(t, firstResult, "first concurrent snatched IntentResume did not finish")
	second := awaitTestValue(t, secondResult, "second concurrent snatched IntentResume did not finish")

	var winner, loser routeResult
	var winnerTr, loserTr *closeTrackingTransport
	switch {
	case first.err == nil && second.err != nil:
		winner, loser = first, second
		winnerTr, loserTr = firstTr, secondTr
	case second.err == nil && first.err != nil:
		winner, loser = second, first
		winnerTr, loserTr = secondTr, firstTr
	default:
		t.Fatalf("expected exactly one concurrent snatched resume to succeed; first=%v second=%v", first.err, second.err)
	}

	require.Same(t, sess, winner.sess)
	require.Same(t, waiting, winner.ac)
	require.Same(t, active, sess.client, "snatched resume must not replace the active owner")
	require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting))
	require.Same(t, winnerTr, waiting.transport())
	require.NotEqual(t, token, waiting.resumeToken, "successful resume rotates the credential")
	require.True(t, oldTr.Closed(), "winning resume must retire the old transport")
	require.False(t, winnerTr.Closed(), "winning rebound transport must survive")

	var loserErr *protoErr
	require.ErrorAs(t, loser.err, &loserErr)
	require.Equal(t, ports.ErrNoSuchSession, loserErr.code)
	require.Contains(t, loserErr.Error(), "resume token is no longer valid")
	require.True(t, loserTr.Closed() || len(loserTr.Sends()) == 0, "losing resume must not complete Welcome")

	d.mu.Lock()
	_, oldTokenParked := d.parked[token]
	_, stillParking := d.parking[token]
	d.mu.Unlock()
	require.False(t, oldTokenParked, "consumed credential must not remain resumeable")
	require.False(t, stillParking, "parking marker must be consumed after park/resume")
}

// TestStaleSnatchedParkClearsOnlyAbandonedParkingMarker covers a stale
// incarnation that publishes a parking marker then loses exact unroute. Only
// that attachment's abandoned marker is cleared; another attachment's marker
// and the fresh snatched incarnation remain intact.
func TestStaleSnatchedParkClearsOnlyAbandonedParkingMarker(t *testing.T) {
	workPTY, releaseWork := newBlockingPTY(t)
	defer releaseWork()
	otherPTY, releaseOther := newBlockingPTY(t)
	defer releaseOther()
	// Separate PTYs so work teardown cannot cascade-close the other session.
	d := newTestDaemon(t, newFactorySeq(t, workPTY, otherPTY), stubClock{})

	sess, waiting, active, oldTr, token := setupResumableSnatchedAttachment(t, d)
	stale := sess.attachmentToken(waiting, oldTr)

	otherTr := &closeTrackingTransport{}
	otherSess, otherAC, err := d.route(helloResumeCapable(ports.IntentNew, "other", 0), otherTr)
	require.NoError(t, err)
	otherToken := d.markParkingInFlight(otherSess, otherAC)
	require.NotZero(t, otherToken)
	require.NotEqual(t, token, otherToken)

	fresh := &closeTrackingTransport{}
	waiting.replaceTransport(fresh)

	require.False(t, d.parkSnatchedAttachment(stale))
	d.mu.Lock()
	_, parked := d.parked[token]
	_, stillParking := d.parking[token]
	otherMarker := d.parking[otherToken]
	d.mu.Unlock()
	require.False(t, parked)
	require.False(t, stillParking, "stale unroute must clear only the abandoned same-attachment marker")
	require.NotNil(t, otherMarker, "stale unroute must not clear another attachment's marker")
	require.Same(t, otherAC, otherMarker.ac)
	require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting))
	require.Same(t, fresh, waiting.transport())
	require.False(t, fresh.Closed())
	require.Same(t, active, sess.client)
	require.Equal(t, token, waiting.resumeToken)
}

// TestTerminalSnatchedRemovalClearsOrphanedParkingMarker covers the race where a
// park path publishes a same-attachment parking-in-flight marker, then terminal
// snatched removal wins before park completes. Only that attachment's marker is
// retired and waiters unblock; other markers stay intact and the stale token
// fails closed. A subsequent legitimate snatched park remains resumable.
func TestTerminalSnatchedRemovalClearsOrphanedParkingMarker(t *testing.T) {
	workPTY, releaseWork := newBlockingPTY(t)
	defer releaseWork()
	otherPTY, releaseOther := newBlockingPTY(t)
	defer releaseOther()
	legitPTY, releaseLegit := newBlockingPTY(t)
	defer releaseLegit()
	// Separate PTYs so session teardown cannot cascade across marker fixtures.
	d := newTestDaemon(t, newFactorySeq(t, workPTY, otherPTY, legitPTY), stubClock{})

	sess, waiting, active, oldTr, token := setupResumableSnatchedAttachment(t, d)
	roleToken := sess.attachmentToken(waiting, oldTr)

	otherTr := &closeTrackingTransport{}
	otherSess, otherAC, err := d.route(helloResumeCapable(ports.IntentNew, "other", 0), otherTr)
	require.NoError(t, err)
	otherToken := d.markParkingInFlight(otherSess, otherAC)
	require.NotZero(t, otherToken)
	require.NotEqual(t, token, otherToken)

	// Simulate the losing park path publishing before terminal unroute wins.
	require.Equal(t, token, d.markParkingInFlight(sess, waiting))
	d.mu.Lock()
	orphaned := d.parking[token]
	otherMarker := d.parking[otherToken]
	d.mu.Unlock()
	require.NotNil(t, orphaned, "fixture: park path must publish the parking marker")
	require.Same(t, waiting, orphaned.ac)
	require.NotNil(t, otherMarker, "fixture: other attachment marker must remain published")
	require.Same(t, otherAC, otherMarker.ac)

	waiterArmed := make(chan struct{})
	d.afterParkingWaitArmed = func() { close(waiterArmed) }
	waiterDone := make(chan bool, 1)
	go func() {
		waiterDone <- d.waitParkingInFlight(helloResumeCapable(ports.IntentResume, "work", token))
	}()
	awaitTestCompletion(t, waiterArmed, "parking waiter did not arm on the orphaned snatched marker")

	require.True(t, d.removeSnatchedAttachment(roleToken), "terminal snatched removal must win exact unroute")
	require.Equal(t, attachmentDetached, sess.attachmentRole(waiting))
	require.Same(t, active, sess.client, "terminal snatched removal must not disturb the active owner")

	waited := awaitTestValue(t, waiterDone, "parking waiter stayed blocked after terminal snatched removal")
	require.True(t, waited, "waiter must observe the same-attachment marker before it is cleared")

	d.mu.Lock()
	_, stillParking := d.parking[token]
	otherAfter := d.parking[otherToken]
	_, parked := d.parked[token]
	d.mu.Unlock()
	require.False(t, stillParking, "terminal winner must clear the same attachment's orphaned marker")
	require.False(t, parked, "terminal removal must not publish parked ownership")
	require.NotNil(t, otherAfter, "terminal winner must not clear another attachment's marker")
	require.Same(t, otherAC, otherAfter.ac)

	_, _, err = d.route(helloResumeCapable(ports.IntentResume, "work", 0xdecafbad), &closeTrackingTransport{})
	require.Error(t, err)
	var unknownErr *protoErr
	require.ErrorAs(t, err, &unknownErr)
	require.Equal(t, ports.ErrNoSuchSession, unknownErr.code)
	require.Contains(t, unknownErr.Error(), "resume token is no longer valid")

	_, _, err = d.route(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{})
	require.Error(t, err)
	var retiredErr *protoErr
	require.ErrorAs(t, err, &retiredErr)
	require.Equal(t, ports.ErrNoSuchSession, retiredErr.code)
	require.Contains(t, retiredErr.Error(), "resume token is no longer valid")

	// A later legitimate snatched park on a fresh attachment must still resume.
	legitTr := &closeTrackingTransport{}
	legitSess, legitWaiting, err := d.route(helloResumeCapable(ports.IntentNew, "legit", 0), legitTr)
	require.NoError(t, err)
	legitToken := legitWaiting.resumeToken
	require.NotZero(t, legitToken)
	_, _, err = d.route(helloResumeCapable(ports.IntentAttach, "legit", 0), &closeTrackingTransport{})
	require.NoError(t, err)
	d.attachmentCleanupWg.Wait()
	require.Equal(t, attachmentSnatched, legitSess.attachmentRole(legitWaiting))
	require.True(t, d.parkSnatchedAttachment(legitSess.attachmentToken(legitWaiting, legitTr)))

	resumeTr := &closeTrackingTransport{}
	routedSess, routedAC, err := d.route(helloResumeCapable(ports.IntentResume, "legit", legitToken), resumeTr)
	require.NoError(t, err)
	require.Same(t, legitSess, routedSess)
	require.Same(t, legitWaiting, routedAC)
	require.Equal(t, attachmentSnatched, legitSess.attachmentRole(legitWaiting))
	require.Same(t, resumeTr, legitWaiting.transport())
}

// TestExplicitDetachClearsOrphanedSameAttachmentParkingMarker covers the race
// where non-explicit teardown publishes a parking-in-flight marker, then
// explicit detach wins the seat. The winner must clear only that attachment's
// orphaned marker and unblock waiters; other attachments' markers and a later
// parked lifecycle must stay intact. Unknown tokens stay fail-closed.
func TestExplicitDetachClearsOrphanedSameAttachmentParkingMarker(t *testing.T) {
	workPTY, releaseWork := newBlockingPTY(t)
	defer releaseWork()
	otherPTY, releaseOther := newBlockingPTY(t)
	defer releaseOther()
	// Separate PTYs keep the other session alive while work detaches.
	d := newTestDaemon(t, newFactorySeq(t, workPTY, otherPTY), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)

	otherTr := &closeTrackingTransport{}
	otherSess, otherAC, err := d.route(helloResumeCapable(ports.IntentNew, "other", 0), otherTr)
	require.NoError(t, err)
	otherToken := d.markParkingInFlight(otherSess, otherAC)
	require.NotZero(t, otherToken)
	require.NotEqual(t, token, otherToken)

	// Pre-mark as a same-attachment non-explicit teardown would before detach.
	require.Equal(t, token, d.markParkingInFlight(sess, ac))
	d.mu.Lock()
	orphaned := d.parking[token]
	otherMarker := d.parking[otherToken]
	d.mu.Unlock()
	require.NotNil(t, orphaned, "fixture: non-explicit teardown must publish the parking marker")
	require.Same(t, ac, orphaned.ac)
	require.NotNil(t, otherMarker, "fixture: other attachment marker must remain published")
	require.Same(t, otherAC, otherMarker.ac)

	waiterArmed := make(chan struct{})
	d.afterParkingWaitArmed = func() { close(waiterArmed) }
	waiterDone := make(chan bool, 1)
	go func() {
		waiterDone <- d.waitParkingInFlight(helloResumeCapable(ports.IntentResume, "work", token))
	}()
	awaitTestCompletion(t, waiterArmed, "parking waiter did not arm on the orphaned marker")

	d.clientGone(sess, ac, oldTr, true)
	require.Nil(t, sess.client, "explicit detach must clear the live owner")

	waited := awaitTestValue(t, waiterDone, "parking waiter stayed blocked after explicit detach")
	require.True(t, waited, "waiter must observe the same-attachment marker before it is cleared")

	d.mu.Lock()
	_, stillParking := d.parking[token]
	otherAfter := d.parking[otherToken]
	d.mu.Unlock()
	require.False(t, stillParking, "explicit winner must clear the same attachment's orphaned marker")
	require.NotNil(t, otherAfter, "explicit winner must not clear another attachment's marker")
	require.Same(t, otherAC, otherAfter.ac)

	_, _, err = d.route(helloResumeCapable(ports.IntentResume, "work", 0xdecafbad), &closeTrackingTransport{})
	require.Error(t, err)
	var unknownErr *protoErr
	require.ErrorAs(t, err, &unknownErr)
	require.Equal(t, ports.ErrNoSuchSession, unknownErr.code)
	require.Contains(t, unknownErr.Error(), "resume token is no longer valid")

	_, _, err = d.route(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{})
	require.Error(t, err)
	var retiredErr *protoErr
	require.ErrorAs(t, err, &retiredErr)
	require.Equal(t, ports.ErrNoSuchSession, retiredErr.code)
	require.Contains(t, retiredErr.Error(), "resume token is no longer valid")

	// A later parked lifecycle for another attachment must survive a clear
	// scoped to the retired attachment identity.
	require.True(t, d.parkAttachment(otherSess, otherAC))
	d.mu.Lock()
	parkedOther := d.parked[otherToken]
	_, otherStillParking := d.parking[otherToken]
	d.mu.Unlock()
	require.NotNil(t, parkedOther, "subsequent park must publish the other attachment")
	require.False(t, otherStillParking, "park consumes the other attachment's in-flight marker")

	d.clearParkingInFlight(otherToken, ac)
	d.mu.Lock()
	parkedAfterMismatchedClear := d.parked[otherToken]
	d.mu.Unlock()
	require.Same(t, parkedOther, parkedAfterMismatchedClear, "mismatched clear must not drop a subsequent parked lifecycle")
}

// TestLiveResumeRejectsLateMarkerAfterTerminalCleanupWins covers the race where
// live resume has validated the same-client credential, then explicit detach or
// session removal wins before markParkingInFlight. Late marker publication must
// be rejected so IntentResume returns promptly ErrNoSuchSession with no parking
// marker left to strand waiters. Direct parkAttachment after detach still works.
func TestLiveResumeRejectsLateMarkerAfterTerminalCleanupWins(t *testing.T) {
	cases := []struct {
		name string
		win  func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, oldTr *closeTrackingTransport, token uint64)
	}{
		{
			name: "explicit_detach",
			win: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, oldTr *closeTrackingTransport, token uint64) {
				t.Helper()
				d.clientGone(sess, ac, oldTr, true)
				require.Nil(t, sess.client, "explicit detach must clear the live owner before late mark")
			},
		},
		{
			name: "session_removal",
			win: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, oldTr *closeTrackingTransport, token uint64) {
				t.Helper()
				require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, true))
				d.mu.Lock()
				registered := d.sessions[sess.id] == sess
				d.mu.Unlock()
				require.False(t, registered, "killSession must unregister before late mark")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pty, release := newBlockingPTY(t)
			defer release()
			d := newTestDaemon(t, newFactory(t, pty), stubClock{})

			oldTr := &closeTrackingTransport{}
			sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
			require.NoError(t, err)
			token := ac.resumeToken
			require.NotZero(t, token)
			require.Same(t, ac, sess.client)

			validated := make(chan struct{})
			releaseMark := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(releaseMark) }) })
			d.beforeMarkParkingInFlight = func() {
				close(validated)
				<-releaseMark
			}

			type routeResult struct {
				err error
			}
			result := make(chan routeResult, 1)
			go func() {
				_, _, routeErr := d.route(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{})
				result <- routeResult{err: routeErr}
			}()
			awaitTestCompletion(t, validated, "live resume did not pause after credential validation before mark")

			tc.win(t, d, sess, ac, oldTr, token)

			d.mu.Lock()
			_, parkingBeforeRelease := d.parking[token]
			d.mu.Unlock()
			require.False(t, parkingBeforeRelease, "terminal cleanup must not leave a parking marker before late mark resumes")

			releaseOnce.Do(func() { close(releaseMark) })
			got := awaitTestValue(t, result, "live IntentResume stayed blocked after terminal cleanup won before mark")
			require.Error(t, got.err)
			var pe *protoErr
			require.ErrorAs(t, got.err, &pe)
			require.Equal(t, ports.ErrNoSuchSession, pe.code)
			require.Contains(t, pe.Error(), "resume token is no longer valid")

			d.mu.Lock()
			_, stillParking := d.parking[token]
			_, stillParked := d.parked[token]
			d.mu.Unlock()
			require.False(t, stillParking, "late markParkingInFlight must not recreate a parking marker after terminal cleanup")
			require.False(t, stillParked, "terminal cleanup must not leave a parked credential")
		})
	}

	t.Run("direct_park_after_detach_without_marker", func(t *testing.T) {
		pty, release := newBlockingPTY(t)
		defer release()
		d := newTestDaemon(t, newFactory(t, pty), stubClock{})

		oldTr := &closeTrackingTransport{}
		sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
		require.NoError(t, err)
		token := ac.resumeToken
		require.NotZero(t, token)

		require.True(t, d.detachIfCurrentTransport(sess, ac, ac.transportSnapshot()))
		require.Nil(t, sess.client)
		require.Zero(t, d.markParkingInFlight(sess, ac), "detached active attachment must not publish a late marker")

		d.mu.Lock()
		_, parkingBeforePark := d.parking[token]
		d.mu.Unlock()
		require.False(t, parkingBeforePark)

		require.True(t, d.parkAttachment(sess, ac), "direct parkAttachment after detach must still park without recreating an in-flight marker")
		d.mu.Lock()
		parked := d.parked[token]
		_, stillParking := d.parking[token]
		d.mu.Unlock()
		require.NotNil(t, parked)
		require.Same(t, ac, parked.ac)
		require.False(t, stillParking, "park must not leave an in-flight marker")
	})
}

// TestMarkParkingInFlightRequiresExactLiveOwnership covers lock-safe rejection
// when the attachment is no longer the exact active or snatched owner, while
// still advertising for both live roles.
func TestMarkParkingInFlightRequiresExactLiveOwnership(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, waiting, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := waiting.resumeToken
	require.NotZero(t, token)

	require.Equal(t, token, d.markParkingInFlight(sess, waiting), "active owner must publish")
	d.clearParkingInFlight(token, waiting)

	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), &closeTrackingTransport{})
	require.NoError(t, err)
	d.attachmentCleanupWg.Wait()
	require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting))
	require.Same(t, active, sess.client)

	require.Equal(t, token, d.markParkingInFlight(sess, waiting), "snatched owner must publish")
	d.clearParkingInFlight(token, waiting)

	roleToken := sess.attachmentToken(waiting, oldTr)
	require.True(t, d.unrouteSnatchedAttachment(roleToken, false))
	require.Equal(t, attachmentDetached, sess.attachmentRole(waiting))
	require.Zero(t, d.markParkingInFlight(sess, waiting), "detached snatched attachment must not publish")

	d.mu.Lock()
	_, stillParking := d.parking[token]
	d.mu.Unlock()
	require.False(t, stillParking)

	require.Equal(t, active.resumeToken, d.markParkingInFlight(sess, active), "active owner must still publish after snatched unroute")
	d.clearParkingInFlight(active.resumeToken, active)
}

// TestStaleClientGoneAfterTransportCheckDoesNotDetachReboundAttachment covers
// the TOCTOU where clientGone passes the old-transport precheck, then a live
// resume rebinds the same attachment before detach runs.
func TestStaleClientGoneAfterTransportCheckDoesNotDetachReboundAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)

	passedPrecheck := make(chan struct{})
	releaseDetach := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseDetach) }) })
	d.beforeClientGoneDetach = func() {
		close(passedPrecheck)
		<-releaseDetach
	}

	goneDone := make(chan struct{})
	go func() {
		d.clientGone(sess, ac, oldTr, false)
		close(goneDone)
	}()
	awaitTestCompletion(t, passedPrecheck, "stale clientGone did not reach the post-precheck seam")

	newTr := &closeTrackingTransport{}
	resumedSess, resumedAC, err := d.route(helloResumeCapable(ports.IntentResume, "work", token), newTr)
	require.NoError(t, err)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)
	require.Same(t, newTr, ac.transport())

	releaseOnce.Do(func() { close(releaseDetach) })
	awaitTestCompletion(t, goneDone, "stale clientGone did not finish after resume rebound")

	require.Same(t, ac, sess.client, "stale cleanup must not detach the rebound attachment")
	require.Same(t, newTr, ac.transport())
	require.False(t, newTr.Closed(), "rebound transport must survive stale old-link cleanup")
	require.True(t, oldTr.Closed(), "live resume retires the captured old transport")
}

// TestResumeLiveAttachmentParkFailureRetiresOldTransport covers shutdown racing
// park after live detach: the old link must still be revoked/closed exactly once
// and the session must not keep a half-detached owner.
func TestResumeLiveAttachmentParkFailureRetiresOldTransport(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)

	d.afterDetachRoleEffectsFrozen = func() {
		d.mu.Lock()
		d.closing = true
		d.mu.Unlock()
	}

	_, _, err = d.route(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{})
	require.Error(t, err)
	var pe *protoErr
	require.ErrorAs(t, err, &pe)
	require.Equal(t, ports.ErrServerShutdown, pe.code)
	require.Nil(t, sess.client, "failed live park must leave the session without an owner")
	require.Nil(t, ac.transport(), "failed live park must revoke the captured old transport")
	require.True(t, oldTr.Closed(), "failed live park must close the captured old transport")
	d.mu.Lock()
	_, parked := d.parked[token]
	d.mu.Unlock()
	require.False(t, parked, "failed live park must not publish a resume credential")
}

func TestResumeRebindsRotatesAndDoesNotOpenPTY(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})

	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)
	oldToken := ac.resumeToken
	d.clientGone(sess, ac, ac.transport(), false)

	tr2, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	resumedSess, resumedAC, err := d.route(helloResumeCapable(ports.IntentResume, "work", oldToken), tr2)
	require.NoError(t, err)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)
	require.NotEqual(t, oldToken, resumedAC.resumeToken, "resume rotates token")
	require.Same(t, resumedAC, sess.client)
}

func TestOutputAckLagAloneDoesNotForceFullStateRepaint(t *testing.T) {
	ac := &attachedClient{output: newOutputStateStream()}
	ac.output.next = 3
	ac.ackOutputState(3)
	ac.ackOutputState(2)
	ac.ackOutputState(4)
	require.Equal(t, uint64(3), ac.output.acked, "stale or future ACKs must not move output state incorrectly")

	ac.sendMu.Lock()
	ac.output.next = 5
	reset := false
	require.False(t, reset, "reliable output ack lag alone must not force dependency-free full repaint")

	f := ac.output.frame([]byte("incremental while reliable backlog drains"), reset, 0)
	ac.sendMu.Unlock()
	out, err := ports.UnmarshalOutput(f.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(5), out.BaseStateNum, "output should remain incremental unless an explicit reset is requested")
	require.Equal(t, uint64(6), out.NewStateNum)

	ac.sendMu.Lock()
	reset = true
	require.True(t, reset, "explicit reset should still force full repaint")
	full := ac.output.frame([]byte("explicit full repaint"), reset, 0)
	ac.sendMu.Unlock()
	fullOut, err := ports.UnmarshalOutput(full.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(0), fullOut.BaseStateNum)
	require.Equal(t, uint64(7), fullOut.NewStateNum)
}

func TestResumeClientIDMismatchDoesNotConsumeParkedToken(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})

	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, ac.transport(), false)

	wrongClient := helloResumeCapable(ports.IntentResume, "work", token)
	wrongClient.ClientID = [16]byte{9, 9, 9, 9}
	_, _, ok, err := d.resumeParked(wrongClient, &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
	d.mu.Lock()
	_, stillParked := d.parked[token]
	d.mu.Unlock()
	require.Error(t, err)
	require.False(t, ok)
	require.True(t, stillParked, "mismatched client must not consume parked token")
	require.Equal(t, token, ac.resumeToken)
	require.True(t, ac.parked)

	tr2, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	resumedSess, resumedAC, err := d.route(helloResumeCapable(ports.IntentResume, "work", token), tr2)
	require.NoError(t, err)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)
}

func TestResumeCloseCapturedOldTransportDoesNotCloseReboundTransport(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.True(t, sess.detachIfCurrent(ac))
	require.True(t, d.parkAttachment(sess, ac))
	generation := ac.roleGeneration.Load()

	newTr := &closeTrackingTransport{}
	resumedSess, resumedAC, ok, err := d.resumeParked(helloResumeCapable(ports.IntentResume, "work", token), newTr, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)
	require.Greater(t, ac.roleGeneration.Load(), generation, "resume must publish active ownership through the attachment transition")

	_ = ac.closeCapturedTransport(oldTr)
	require.True(t, oldTr.Closed(), "old transport is closed")
	require.False(t, newTr.Closed(), "newly rebound transport is not closed by old cleanup")
	require.Same(t, newTr, ac.transport())
}

func TestResumeRebasesFullOutputWindowBeforeFirstPaint(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	ac.output.next = maxUnackedOutputStates
	token := ac.resumeToken
	require.True(t, sess.detachIfCurrent(ac))
	require.True(t, d.parkAttachment(sess, ac))

	newTr := &closeTrackingTransport{}
	resumedSess, resumedAC, err := d.route(helloResumeCapable(ports.IntentResume, "work", token), newTr)
	require.NoError(t, err)
	require.Same(t, ac, resumedAC)
	require.True(t, resumedSess.renderCoordinator().markAttachmentReady(resumedSess.renderCoordinator().attachmentLease(resumedAC)))
	d.firstPaint(resumedSess, resumedAC, resumedAC.size)

	sends := newTr.Sends()
	require.Len(t, sends, 1)
	first, err := ports.UnmarshalOutput(sends[0].Payload)
	require.NoError(t, err)
	require.Zero(t, first.BaseStateNum)
	require.Equal(t, uint64(maxUnackedOutputStates+1), first.NewStateNum)
	resumedAC.ackOutputState(first.NewStateNum)

	resumedSess.tabs[0].focusedPane().screen.Write([]byte("A"))
	d.paint(resumedSess, resumedAC, false, nil)
	sends = newTr.Sends()
	require.Len(t, sends, 2)
	second, err := ports.UnmarshalOutput(sends[1].Payload)
	require.NoError(t, err)
	require.Equal(t, first.NewStateNum, second.BaseStateNum)
}

func TestParkingReleasesPaneCapturesBeforeHeadlessCloseAndResume(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
	oldTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)

	tb := sess.tabs[0]
	survivor := tb.panes["pane-1"]
	closed := newPane("pane-2", nil, domain.Size{Cols: 40, Rows: 23})
	tb.mu.Lock()
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-1"}
	tb.panes[closed.id] = closed
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()
	d.applyTabLayout(sess, tb)
	survivor.screen.Write([]byte("survivor"))
	closed.screen.Write([]byte("closed"))
	d.paint(sess, ac, true, nil)

	ac.sendMu.Lock()
	require.Contains(t, ac.captureFrames, closed, "fixture must render and capture the pane before parking")
	ac.sendMu.Unlock()
	token := ac.resumeToken
	require.True(t, sess.detachIfCurrent(ac))
	require.True(t, d.parkAttachment(sess, ac))

	// Headless close cannot find the parked attachment through sess.client.
	// Its capture must already have been released before the attachment parked.
	require.NoError(t, d.closePane(sess, tb, closed.id, nil, false))
	ac.sendMu.Lock()
	require.NotContains(t, ac.captureFrames, closed, "parked attachment must not retain a pane closed while headless")
	ac.sendMu.Unlock()

	newTransport := &closeTrackingTransport{}
	resumedSess, resumedAC, err := d.route(helloResumeCapable(ports.IntentResume, sess.name, token), newTransport)
	require.NoError(t, err)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)
	require.True(t, resumedSess.renderCoordinator().markAttachmentReady(resumedSess.renderCoordinator().attachmentLease(resumedAC)))
	d.firstPaint(resumedSess, resumedAC, resumedAC.size)

	sends := newTransport.Sends()
	require.Len(t, sends, 1)
	output, err := ports.UnmarshalOutput(sends[0].Payload)
	require.NoError(t, err)
	require.Zero(t, output.BaseStateNum, "resume must start with a complete frame")
	terminal := vt.NewScreen(resumedAC.size.Cols, resumedAC.size.Rows)
	terminal.Write(output.Data)
	contents := strings.Join(frameRows(terminal.Frame), "\n")
	require.Contains(t, contents, "survivor", "resume first paint must contain current headless content")
	require.NotContains(t, contents, "closed", "resume first paint must not contain closed pane content")
}

func TestExplicitDetachDoesNotPark(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)
	d.clientGone(sess, ac, ac.transport(), true)
	d.mu.Lock()
	parked := len(d.parked)
	d.mu.Unlock()
	require.Zero(t, parked)
}

func TestResumeParkUsesConfiguredGraceAndExpiresOnlyAfterGrace(t *testing.T) {
	clk := &signalClock{timers: make(chan *signalTimer, 8)}
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), clk)
	WithResumeParkGrace(20 * time.Minute)(d)
	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)
	token := ac.resumeToken

	d.clientGone(sess, ac, ac.transport(), false)
	timer := <-clk.timers
	require.Equal(t, 20*time.Minute, timer.duration)
	d.mu.Lock()
	_, parkedBeforeGrace := d.parked[token]
	d.mu.Unlock()
	require.True(t, parkedBeforeGrace, "parked attachment remains before configured grace timer fires")

	timer.ch <- clk.Now()
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, ok := d.parked[token]
		return !ok
	}, 2*time.Second, 10*time.Millisecond)
}

func TestParkExpiryAndShutdownCleanup(t *testing.T) {
	clk := &signalClock{timers: make(chan *signalTimer, 8)}
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), clk)
	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, ac.transport(), false)
	timer := <-clk.timers
	timer.ch <- clk.Now()
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, ok := d.parked[token]
		return !ok
	}, 2*time.Second, 10*time.Millisecond)

	pty2, release2 := newBlockingPTY(t)
	defer release2()
	d2 := newTestDaemon(t, newFactory(t, pty2), stubClock{})
	tr2, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess2, ac2, err := d2.route(helloResumeCapable(ports.IntentNew, "other", 0), tr2)
	require.NoError(t, err)
	d2.clientGone(sess2, ac2, ac2.transport(), false)
	d2.shutdownAll(ports.ReasonServerShutdown)
	d2.mu.Lock()
	parked := len(d2.parked)
	d2.mu.Unlock()
	require.Zero(t, parked)
}

func TestLiveParkAndResumeRetainsPreviousSession(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)

	previous := &session{sessionCore: sessionCore{id: "previous"}}
	ac.previousSession.Set(previous)
	d.clientGone(sess, ac, ac.transport(), false)

	token := ac.resumeToken
	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Same(t, previous, ac.previousSession.Get(), "a live parked attachment keeps its toggle")

	tr2, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	resumedSess, resumedAC, err := d.route(helloResumeCapable(ports.IntentResume, "work", token), tr2)
	require.NoError(t, err)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)
	require.Same(t, previous, resumedAC.previousSession.Get(), "resume keeps the live attachment toggle")
}

func TestDiscardingParkedAttachmentClearsPreviousSession(t *testing.T) {
	for _, tc := range []struct {
		name    string
		discard func(*Daemon, uint64, *parkedAttachment) []parkedAttachmentRetirement
	}{
		{
			name: "expiry",
			discard: func(d *Daemon, token uint64, parked *parkedAttachment) []parkedAttachmentRetirement {
				d.removeParkedLocked(token, parked)
				return nil
			},
		},
		{
			name: "session purge",
			discard: func(d *Daemon, _ uint64, parked *parkedAttachment) []parkedAttachmentRetirement {
				return d.purgeParkedForSessionLocked(parked.sess)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pty, release := newBlockingPTY(t)
			defer release()
			d := newTestDaemon(t, newFactory(t, pty), stubClock{})
			tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
			sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
			require.NoError(t, err)

			ac.previousSession.Set(&session{sessionCore: sessionCore{id: "previous"}})
			d.clientGone(sess, ac, ac.transport(), false)

			d.mu.Lock()
			parked := d.parked[ac.resumeToken]
			require.NotNil(t, parked)
			retirements := tc.discard(d, ac.resumeToken, parked)
			d.mu.Unlock()
			finishParkedAttachmentRetirements(retirements)

			require.Nil(t, ac.previousSession.Get())
		})
	}
}

func TestEphemeralParkExpiryKeepsSession(t *testing.T) {
	clk := &signalClock{timers: make(chan *signalTimer, 8)}
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), clk)

	tr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentEphemeral, "", 0), tr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)

	d.clientGone(sess, ac, ac.transport(), false)
	timer := <-clk.timers
	timer.ch <- clk.Now()

	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, ok := d.parked[token]
		return !ok
	}, 2*time.Second, 10*time.Millisecond)

	require.Equal(t, 1, sessionCount(d), "token expiry does not kill ephemeral session")
	sess.mu.Lock()
	require.Nil(t, sess.client)
	sess.mu.Unlock()
}

func TestKilledSessionPurgesParkedResumeToken(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, ac.transport(), false)

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, true))
	d.mu.Lock()
	_, parked := d.parked[token]
	d.mu.Unlock()
	_, _, ok, err := d.resumeParked(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
	require.False(t, parked, "killSession purges parked token")
	require.NoError(t, err)
	require.False(t, ok, "killed session cannot be resumed")
}

// TestKilledSessionUnblocksParkingWaiterFailsClosed covers session removal while
// a same-token IntentResume is waiting on an in-flight parking marker. killSession
// must purge only that session's markers so the waiter unblocks and fails closed,
// without touching another session's parking marker.
func TestKilledSessionUnblocksParkingWaiterFailsClosed(t *testing.T) {
	workPTY, releaseWork := newBlockingPTY(t)
	defer releaseWork()
	otherPTY, releaseOther := newBlockingPTY(t)
	defer releaseOther()
	// Separate PTYs are required: a shared mock PTY makes killSession(work)
	// close other's pane and cascade-remove the other session under -race,
	// falsely failing the other-marker / fail-closed assertions.
	d := newTestDaemon(t, newFactorySeq(t, workPTY, otherPTY), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.NotZero(t, token)

	otherTr := &closeTrackingTransport{}
	otherSess, otherAC, err := d.route(helloResumeCapable(ports.IntentNew, "other", 0), otherTr)
	require.NoError(t, err)
	otherToken := d.markParkingInFlight(otherSess, otherAC)
	require.NotZero(t, otherToken)
	require.NotEqual(t, token, otherToken)

	require.Equal(t, token, d.markParkingInFlight(sess, ac))
	d.mu.Lock()
	parkingMarker := d.parking[token]
	otherMarker := d.parking[otherToken]
	d.mu.Unlock()
	require.NotNil(t, parkingMarker, "fixture: parking marker must be published before kill")
	require.Same(t, ac, parkingMarker.ac)
	require.NotNil(t, otherMarker, "fixture: other session marker must remain published")
	require.Same(t, otherAC, otherMarker.ac)

	waiterArmed := make(chan struct{})
	d.afterParkingWaitArmed = func() { close(waiterArmed) }
	waiterDone := make(chan bool, 1)
	go func() {
		waiterDone <- d.waitParkingInFlight(helloResumeCapable(ports.IntentResume, "work", token))
	}()
	awaitTestCompletion(t, waiterArmed, "parking waiter did not arm before session kill")

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, true))

	waited := awaitTestValue(t, waiterDone, "parking waiter stayed blocked after session kill")
	require.True(t, waited, "waiter must observe the session marker before it is purged")

	d.mu.Lock()
	_, stillParking := d.parking[token]
	otherAfter := d.parking[otherToken]
	otherRegistered := d.sessions[otherSess.id] == otherSess
	closing := d.closing
	d.mu.Unlock()
	require.False(t, stillParking, "killSession must purge the killed session's parking marker")
	require.NotNil(t, otherAfter, "killSession must not purge another session's parking marker")
	require.Same(t, otherAC, otherAfter.ac)
	require.True(t, otherRegistered, "other session must survive work kill when PTYs are isolated")
	require.False(t, closing, "daemon must not shut down while the other session remains")

	_, _, err = d.route(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{})
	require.Error(t, err)
	var killedErr *protoErr
	require.ErrorAs(t, err, &killedErr)
	require.Equal(t, ports.ErrNoSuchSession, killedErr.code)
	require.Contains(t, killedErr.Error(), "resume token is no longer valid")
}

func TestParkedPredecessorResumesSnatchedWithoutStealingActiveAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	sess, oldAC, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)
	token := oldAC.resumeToken
	d.clientGone(sess, oldAC, oldAC.transport(), false)

	activeTr := &closeTrackingTransport{}
	_, activeAC, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), activeTr)
	require.NoError(t, err)
	require.NotSame(t, oldAC, activeAC)

	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked, "normal attach preserves the parked predecessor")
	require.Equal(t, attachmentSnatched, parked.role)
	_, resumedAC, ok, err := d.resumeParked(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, oldAC, resumedAC)
	require.Equal(t, attachmentSnatched, sess.attachmentRole(resumedAC))
	require.Same(t, activeAC, sess.client)
}

func TestStaleClientGoneDoesNotDetachOrCloseFreshTransport(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, p), stubClock{})

	oldTr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTr)
	require.NoError(t, err)
	freshTr := &closeTrackingTransport{}
	ac.replaceTransport(freshTr)

	d.clientGone(sess, ac, oldTr, false)

	require.Same(t, ac, sess.client, "stale connection must not detach current client")
	require.False(t, oldTr.Closed(), "stale transport is owned by its own loop/handler")
	require.False(t, freshTr.Closed(), "fresh resumed transport must not be closed by stale loop")
}

func TestRawTerminalSideEffectsAreOutputStateNeutral(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tr := &closeTrackingTransport{}
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()

	require.NoError(t, d.boundedSendOutputErr(ac, []byte("copy")))
	require.NoError(t, d.boundedSendOutputErr(ac, []byte("more")))

	sends := tr.Sends()
	require.Len(t, sends, 2)
	first, err := ports.UnmarshalOutput(sends[0].Payload)
	require.NoError(t, err)
	second, err := ports.UnmarshalOutput(sends[1].Payload)
	require.NoError(t, err)
	require.Zero(t, first.BaseStateNum)
	require.Zero(t, first.NewStateNum)
	require.Zero(t, second.BaseStateNum)
	require.Zero(t, second.NewStateNum)
}

func TestSequencedInputDoesNotPrematurelyEchoAck(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), tr)
	require.NoError(t, err)

	d.handleSequencedInput(sess, ac, 42, []byte("x"))

	require.Zero(t, ac.echoAck.Load())
}

func TestResumeParkedReplacesFuturePTYEnvironment(t *testing.T) {
	initialPTY, releaseInitial := newBlockingPTY(t)
	futurePTY, releaseFuture := newBlockingPTY(t)
	defer releaseInitial()
	defer releaseFuture()
	var commands []string
	var envs [][]string
	factory := portsmocks.NewMockPTYFactory(t)
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, command string, _ []string, env []string, _ string, size domain.Size) (ports.PTY, error) {
			if size != (domain.Size{Cols: 80, Rows: 22}) {
				return newQuietPTY(), nil
			}
			commands = append(commands, command)
			envs = append(envs, append([]string(nil), env...))
			if len(commands) == 1 {
				return initialPTY, nil
			}
			return futurePTY, nil
		},
	).Maybe()
	d := newTestDaemon(t, factory, stubClock{})
	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	hello := helloResumeCapable(ports.IntentNew, "work", 0)
	hello.Env = []string{"SECRET=before", "SHELL=/bin/sh", "TERM=old", "VEV=old"}
	sess, ac, err := d.route(hello, tr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.True(t, sess.detachIfCurrent(ac))
	require.True(t, d.parkAttachment(sess, ac))

	resumeHello := helloResumeCapable(ports.IntentResume, "work", token)
	resumeHello.Env = []string{"SECRET=after", "PAIR=a=b", "SHELL=/usr/bin/fish", "TERM=old", "COLORTERM=old", "TERM_PROGRAM=old", "VEV=old"}
	resumeHello.TrueColor = true
	_, _, ok, err := d.resumeParked(resumeHello, &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, d.createTab(sess, domain.Size{Cols: 80, Rows: 24}))

	sess.mu.Lock()
	future := sess.tabs[1].focusedPane()
	futureTabID, futurePaneID := sess.tabs[1].stableID, future.stableID
	require.True(t, sess.terminal.TrueColor)
	require.Equal(t, resumeHello.Env, sess.env)
	sess.mu.Unlock()
	require.Equal(t, []string{"/bin/sh", "/usr/bin/fish"}, commands)
	require.Equal(t, []string{"SECRET=after", "PAIR=a=b", "SHELL=/usr/bin/fish", "TERM=xterm-direct", "COLORTERM=truecolor", "TERM_PROGRAM=vev", "VEV=session=work,tab=" + futureTabID + ",pane=" + futurePaneID}, envs[1])
}

func TestResumeRenegotiatesOutputWindowOnReusedStream(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
	firstHello := helloResumeCapable(ports.IntentNew, "work", 0)
	firstHello.MaxOutputInFlight = 8
	sess, ac, err := d.route(firstHello, &closeTrackingTransport{})
	require.NoError(t, err)
	require.Equal(t, uint64(8), ac.output.maxOutstanding)
	stream := ac.output
	token := ac.resumeToken
	require.True(t, sess.detachIfCurrent(ac))
	require.True(t, d.parkAttachment(sess, ac))

	resumeOne := helloResumeCapable(ports.IntentResume, "work", token)
	resumeOne.MaxOutputInFlight = 1
	_, resumed, err := d.route(resumeOne, &closeTrackingTransport{})
	require.NoError(t, err)
	require.Same(t, stream, resumed.output)
	require.Equal(t, uint64(1), resumed.output.maxOutstanding)

	token = resumed.resumeToken
	require.True(t, sess.detachIfCurrent(resumed))
	require.True(t, d.parkAttachment(sess, resumed))
	resumeEight := helloResumeCapable(ports.IntentResume, "work", token)
	resumeEight.MaxOutputInFlight = 8
	_, resumed, err = d.route(resumeEight, &closeTrackingTransport{})
	require.NoError(t, err)
	require.Same(t, stream, resumed.output)
	require.Equal(t, uint64(8), resumed.output.maxOutstanding)
}
