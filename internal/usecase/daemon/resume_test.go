package daemon

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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
	d.mu.Lock()
	_, _, ok, err := d.resumeParkedLocked(wrongClient, &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
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

	newTr := &closeTrackingTransport{}
	d.mu.Lock()
	resumedSess, resumedAC, ok, err := d.resumeParkedLocked(helloResumeCapable(ports.IntentResume, "work", token), newTr, domain.Size{Cols: 80, Rows: 24})
	d.mu.Unlock()
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)

	_ = ac.closeCapturedTransport(oldTr)
	require.True(t, oldTr.Closed(), "old transport is closed")
	require.False(t, newTr.Closed(), "newly rebound transport is not closed by old cleanup")
	require.Same(t, newTr, ac.transport())
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
	_, _, ok, err := d.resumeParkedLocked(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
	d.mu.Unlock()
	require.False(t, parked, "killSession purges parked token")
	require.NoError(t, err)
	require.False(t, ok, "killed session cannot be resumed")
}

func TestStaleParkedTokenCannotStealActiveAttachment(t *testing.T) {
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
	_, parked := d.parked[token]
	_, resumedAC, ok, err := d.resumeParkedLocked(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
	d.mu.Unlock()
	require.False(t, parked, "normal attach invalidates stale parked token")
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, resumedAC)
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

func TestOutputStateNumberingIsSharedAndMonotone(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tr := &closeTrackingTransport{}
	ac := &attachedClient{tr: tr}
	ac.initOverlays()

	require.NoError(t, d.boundedSendOutputErr(ac, []byte("copy")))
	require.NoError(t, d.boundedSendOutputErr(ac, []byte("more")))

	sends := tr.Sends()
	require.Len(t, sends, 2)
	first, err := ports.UnmarshalOutput(sends[0].Payload)
	require.NoError(t, err)
	second, err := ports.UnmarshalOutput(sends[1].Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.NewStateNum)
	require.Equal(t, uint64(2), second.NewStateNum)
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

func TestResumeParkedUpdatesTerminalEnv(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	tr, _, _ := newConn(t, mustHello(ports.IntentAttach, "unused", domain.Size{}))
	hello := helloResumeCapable(ports.IntentNew, "work", 0)
	hello.TrueColor = false
	sess, ac, err := d.route(hello, tr)
	require.NoError(t, err)
	token := ac.resumeToken
	require.True(t, sess.detachIfCurrent(ac))
	require.True(t, d.parkAttachment(sess, ac))

	resumeHello := helloResumeCapable(ports.IntentResume, "work", token)
	resumeHello.TrueColor = true
	d.mu.Lock()
	_, _, ok, err := d.resumeParkedLocked(resumeHello, &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
	d.mu.Unlock()
	require.NoError(t, err)
	require.True(t, ok)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.True(t, sess.terminal.TrueColor)
}
