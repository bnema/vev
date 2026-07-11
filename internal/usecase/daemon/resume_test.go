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

	newTr := &closeTrackingTransport{}
	resumedSess, resumedAC, ok, err := d.resumeParked(helloResumeCapable(ports.IntentResume, "work", token), newTr, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)

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
	d.firstPaint(resumedSess, resumedAC, resumedAC.size)

	sends := newTr.Sends()
	require.Len(t, sends, 1)
	first, err := ports.UnmarshalOutput(sends[0].Payload)
	require.NoError(t, err)
	require.Zero(t, first.BaseStateNum)
	require.Equal(t, uint64(maxUnackedOutputStates+1), first.NewStateNum)
	resumedAC.ackOutputState(first.NewStateNum)

	resumedSess.tabs[0].focusedPane().screen.Write([]byte("A"))
	d.paint(resumedSess, resumedAC, false)
	sends = newTr.Sends()
	require.Len(t, sends, 2)
	second, err := ports.UnmarshalOutput(sends[1].Payload)
	require.NoError(t, err)
	require.Equal(t, first.NewStateNum, second.BaseStateNum)
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

	previous := &session{id: "previous"}
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
		discard func(*Daemon, uint64, *parkedAttachment)
	}{
		{
			name: "expiry",
			discard: func(d *Daemon, token uint64, parked *parkedAttachment) {
				d.removeParkedLocked(token, parked)
			},
		},
		{
			name: "session purge",
			discard: func(d *Daemon, _ uint64, parked *parkedAttachment) {
				d.purgeParkedForSessionLocked(parked.sess)
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

			ac.previousSession.Set(&session{id: "previous"})
			d.clientGone(sess, ac, ac.transport(), false)

			d.mu.Lock()
			parked := d.parked[ac.resumeToken]
			require.NotNil(t, parked)
			tc.discard(d, ac.resumeToken, parked)
			d.mu.Unlock()

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
	d.mu.Unlock()
	_, resumedAC, ok, err := d.resumeParked(helloResumeCapable(ports.IntentResume, "work", token), &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
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
	_, _, ok, err := d.resumeParked(resumeHello, &closeTrackingTransport{}, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	require.True(t, ok)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.True(t, sess.terminal.TrueColor)
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
