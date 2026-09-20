package app

// KillResult caller contract (explicit result slice). runKill and
// requestDaemonStop send one Kill with a unique nonzero RequestID and consume
// exactly one matching KillResult; a lost, uncorrelated, or wrong-typed reply is
// outcome-unknown and is never replayed, while a caller canceled before the
// send is a definite not-sent outcome. These tests drive the two helpers those
// callers share with a controllable in-memory client connection, so every case
// is bounded and uses no sleeps.

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// fakeKillConnection is a scripted ports.ClientConnection. Sends are recorded
// and counted; ReceiveServer serves queued replies in order, then either a
// reply derived from the last sent Kill (replyForKill), a terminal error, or a
// block until Close.
type fakeKillConnection struct {
	mu           sync.Mutex
	sends        []protocol.ClientMessage
	recv         []protocol.ServerMessage
	replyForKill func(protocol.Kill) protocol.ServerMessage
	sendErr      error
	recvErr      error
	// block: once no reply is available, ReceiveServer parks until Close.
	block  bool
	closed chan struct{}
	once   sync.Once
	// recvEntered reports that a blocked ReceiveServer was actually entered.
	recvEntered chan struct{}
	enterOnce   sync.Once
}

func newFakeKillConnection() *fakeKillConnection {
	return &fakeKillConnection{closed: make(chan struct{}), recvEntered: make(chan struct{})}
}

func (c *fakeKillConnection) SendClient(message protocol.ClientMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sendErr != nil {
		return c.sendErr
	}
	c.sends = append(c.sends, message)
	return nil
}

func (c *fakeKillConnection) ReceiveServer() (protocol.ServerMessage, error) {
	c.mu.Lock()
	if len(c.recv) > 0 {
		next := c.recv[0]
		c.recv = c.recv[1:]
		c.mu.Unlock()
		return next, nil
	}
	// replyForKill derives a matching result from the exact request that was
	// sent, so a test never has to predict the fresh random RequestID.
	if c.replyForKill != nil && len(c.sends) > 0 {
		if kill, ok := c.sends[len(c.sends)-1].(protocol.Kill); ok {
			reply := c.replyForKill(kill)
			c.mu.Unlock()
			return reply, nil
		}
	}
	block, recvErr := c.block, c.recvErr
	c.mu.Unlock()
	if !block {
		return nil, recvErr
	}
	c.enterOnce.Do(func() { close(c.recvEntered) })
	<-c.closed
	return nil, io.ErrClosedPipe
}

func (c *fakeKillConnection) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}

func (*fakeKillConnection) LinkState() ports.LinkState         { return ports.LinkState(0) }
func (*fakeKillConnection) LinkEvents() <-chan ports.LinkEvent { return nil }

func (c *fakeKillConnection) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *fakeKillConnection) sentCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sends)
}

func (c *fakeKillConnection) sentKill(t *testing.T) protocol.Kill {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Len(t, c.sends, 1, "exactly one Kill must be sent (no replay)")
	kill, ok := c.sends[0].(protocol.Kill)
	require.True(t, ok, "sent message %T is not a Kill", c.sends[0])
	return kill
}

// sendAndAwait drives the shared send/receive pair once, as runKill and
// requestDaemonStop do.
func sendAndAwait(ctx context.Context, conn *fakeKillConnection, request protocol.Kill) error {
	requestID, err := sendKillRequest(ctx, conn, request)
	if err != nil {
		return err
	}
	return receiveKillResult(ctx, conn, requestID)
}

// matchingResult mirrors one Kill's RequestID into a successful result.
func matchingResult(outcome protocol.KillOutcome, code uint16, text string) func(protocol.Kill) protocol.ServerMessage {
	return func(kill protocol.Kill) protocol.ServerMessage {
		return protocol.KillResult{RequestID: kill.RequestID, Outcome: outcome, Code: code, Text: text}
	}
}

func TestKillResultMatchingSuccessConsumesOneCorrelatedResult(t *testing.T) {
	conn := newFakeKillConnection()
	conn.replyForKill = matchingResult(protocol.KillSucceeded, 0, "")

	require.NoError(t, sendAndAwait(context.Background(), conn, protocol.Kill{Name: "work", Scope: protocol.KillSession}))

	kill := conn.sentKill(t)
	require.NotZero(t, kill.RequestID, "a Kill must carry a nonzero RequestID")
	require.Equal(t, "work", kill.Name)
	require.Equal(t, protocol.KillSession, kill.Scope)
	require.Equal(t, 1, conn.sentCount())
}

func TestKillResultFailedDefiniteIsNotUnknown(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "no such session", text: "no such session: work"},
		{name: "participants changed", text: "session changed during the operation; try again"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newFakeKillConnection()
			conn.replyForKill = matchingResult(protocol.KillFailed, protocol.ErrInternal, tt.text)

			err := sendAndAwait(context.Background(), conn, protocol.Kill{Name: "work", Scope: protocol.KillSession})
			require.Error(t, err)
			require.ErrorContains(t, err, tt.text)
			require.NotErrorIs(t, err, errKillOutcomeUnknown, "a definite failure must not be reported as unknown")
			require.Equal(t, 1, conn.sentCount(), "a definite failure must not be replayed")
		})
	}
}

func TestKillResultUnknownOutcomeStaysUnknown(t *testing.T) {
	conn := newFakeKillConnection()
	conn.replyForKill = matchingResult(protocol.KillOutcomeUnknown, protocol.ErrServerShutdown, "daemon is shutting down")

	err := sendAndAwait(context.Background(), conn, protocol.Kill{Scope: protocol.KillAll})
	require.ErrorIs(t, err, errKillOutcomeUnknown)
	require.Equal(t, 1, conn.sentCount(), "an unknown outcome must never be replayed")
}

func TestKillResultWrongTypeOrIDIsOutcomeUnknown(t *testing.T) {
	tests := []struct {
		name  string
		reply protocol.ServerMessage
	}{
		{name: "wrong type", reply: protocol.Pong{}},
		{name: "wrong type error", reply: protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "boom"}},
		{name: "wrong id", reply: protocol.KillResult{RequestID: 99, Outcome: protocol.KillSucceeded}},
		{name: "zero id", reply: protocol.KillResult{RequestID: 0, Outcome: protocol.KillSucceeded}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newFakeKillConnection()
			conn.recv = []protocol.ServerMessage{tt.reply}

			err := sendAndAwait(context.Background(), conn, protocol.Kill{Scope: protocol.KillSession})
			require.ErrorIs(t, err, errKillOutcomeUnknown)
			require.Equal(t, 1, conn.sentCount(), "an uncorrelated reply must not trigger a replay")
		})
	}
}

func TestKillResultClosedBeforeReplyIsOutcomeUnknown(t *testing.T) {
	conn := newFakeKillConnection()
	conn.recvErr = io.EOF

	err := sendAndAwait(context.Background(), conn, protocol.Kill{Scope: protocol.KillSession})
	require.ErrorIs(t, err, errKillOutcomeUnknown)
	require.Equal(t, 1, conn.sentCount(), "a close before the reply must never be read as success")
}

// TestKillResultCancelAfterSendIsOutcomeUnknownWithoutReplay proves a caller
// canceled while awaiting the result closes the exact connection, reports an
// unknown outcome (never EOF-as-success), and never resends the request.
func TestKillResultCancelAfterSendIsOutcomeUnknownWithoutReplay(t *testing.T) {
	conn := newFakeKillConnection()
	conn.block = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- sendAndAwait(ctx, conn, protocol.Kill{Name: "work", Scope: protocol.KillSession}) }()

	// Wait until the receive is actually parked, then cancel. No sleep: the
	// fake signals entry deterministically.
	<-conn.recvEntered
	cancel()

	require.ErrorIs(t, <-done, errKillOutcomeUnknown)
	require.Equal(t, 1, conn.sentCount(), "a canceled kill must never be replayed")

	select {
	case <-conn.closed:
	default:
		t.Fatal("cancellation after send must close the exact connection")
	}
}

// TestKillResultTimeoutAfterSendIsOutcomeUnknownWithoutReplay proves a caller
// whose result deadline expires while awaiting the reply closes the exact
// connection, reports an unknown outcome, and never resends the request.
func TestKillResultTimeoutAfterSendIsOutcomeUnknownWithoutReplay(t *testing.T) {
	conn := newFakeKillConnection()
	conn.block = true

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- sendAndAwait(ctx, conn, protocol.Kill{Name: "work", Scope: protocol.KillSession}) }()

	<-conn.recvEntered
	require.ErrorIs(t, <-done, errKillOutcomeUnknown)
	require.Equal(t, 1, conn.sentCount(), "a timed-out kill must never be replayed")

	select {
	case <-conn.closed:
	default:
		t.Fatal("a result timeout must close the exact connection")
	}
}

// TestKillResultCancelBeforeSendIsDefiniteNotSent proves a caller canceled
// before the request is sent reports a definite not-sent outcome and puts no
// bytes on the wire.
func TestKillResultCancelBeforeSendIsDefiniteNotSent(t *testing.T) {
	conn := newFakeKillConnection()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sendAndAwait(ctx, conn, protocol.Kill{Name: "work", Scope: protocol.KillSession})
	require.ErrorIs(t, err, errKillNotSent)
	require.NotErrorIs(t, err, errKillOutcomeUnknown, "a not-sent request has a definite outcome")
	require.Zero(t, conn.sentCount(), "a canceled-before-send kill must not be sent")
}

func TestKillRequestSendFailureIsReported(t *testing.T) {
	conn := newFakeKillConnection()
	conn.sendErr = errors.New("write failed")

	err := sendAndAwait(context.Background(), conn, protocol.Kill{Name: "work", Scope: protocol.KillSession})
	require.ErrorContains(t, err, "write failed")
	require.Zero(t, conn.sentCount())
}

// TestKillRequestIDsAreUniqueAndNonzero proves each request allocates a fresh,
// nonzero correlation ID so a reply for one request can never match another.
func TestKillRequestIDsAreUniqueAndNonzero(t *testing.T) {
	seen := make(map[uint64]struct{})
	for range 64 {
		conn := newFakeKillConnection()
		conn.replyForKill = matchingResult(protocol.KillSucceeded, 0, "")
		require.NoError(t, sendAndAwait(context.Background(), conn, protocol.Kill{Scope: protocol.KillAll}))
		id := conn.sentKill(t).RequestID
		require.NotZero(t, id)
		_, duplicate := seen[id]
		require.False(t, duplicate, "duplicate RequestID %d", id)
		seen[id] = struct{}{}
	}
}

// countingLifecycleProbe records every lifecycle-acquire attempt and returns
// the configured owner or error, so a test can prove both whether the transfer
// wait was entered and how it resolved.
type countingLifecycleProbe struct {
	attempts atomic.Int64
	owner    lifecycleOwnership
	err      error
}

func (p *countingLifecycleProbe) TryAcquire(string) (lifecycleOwnership, error) {
	p.attempts.Add(1)
	return p.owner, p.err
}

// startServingOneKill binds ipc.SocketDir() under a temp runtime root, points
// the lifecycle probe at a counting fake, and serves exactly one control
// connection in the background. reply==nil closes the connection without
// answering, which models a lost reply; otherwise the reply is correlated to the
// exact request read from the wire. The returned channel closes once the
// scripted server has finished.
func startServingOneKill(t *testing.T, probe *countingLifecycleProbe, reply func(protocol.Kill) protocol.ServerMessage) <-chan struct{} {
	t.Helper()
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	// Bound the transfer wait so a regression that wrongly waits fails fast
	// instead of burning the production retry budget.
	originalBackoff := defaultBackoff
	t.Cleanup(func() { defaultBackoff = originalBackoff })
	defaultBackoff = backoffConfig{initial: time.Millisecond, max: time.Millisecond, total: 5 * time.Millisecond}

	originalProbe := daemonLifecycleProbe
	t.Cleanup(func() { daemonLifecycleProbe = originalProbe })
	daemonLifecycleProbe = probe

	listener, err := ipc.Listen(ipc.SocketDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = raw.Close() }()
		connection := sessionwire.NewServerConnection(raw)
		message, err := connection.ReceiveClient()
		if err != nil {
			return
		}
		kill, ok := message.(protocol.Kill)
		if !ok || reply == nil {
			return
		}
		_ = connection.SendServer(reply(kill))
	}()
	return done
}

func awaitServingOneKill(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the scripted control server did not finish")
	}
}

// TestDaemonStopLostReplyConfirmedByLifecycleTransfer proves a daemon-stop whose
// explicit result is lost is still reported as success once the caller directly
// acquires lifecycle ownership: the authoritative transfer confirms the accepted
// shutdown completed.
func TestDaemonStopLostReplyConfirmedByLifecycleTransfer(t *testing.T) {
	probe := &countingLifecycleProbe{owner: fakeLifecycleOwnership{release: func() error { return nil }}}
	done := startServingOneKill(t, probe, nil)

	require.NoError(t, requestDaemonStop(context.Background()))
	awaitServingOneKill(t, done)
	require.EqualValues(t, 1, probe.attempts.Load(), "a lost reply must be resolved by exactly one ownership probe")
}

// TestDaemonStopLostReplyAndTransferFailurePreservesUnknownOutcome proves a
// daemon-stop whose result is lost AND whose lifecycle confirmation fails keeps
// errors.Is(err, errKillOutcomeUnknown) and carries both diagnostics, so the
// caller never reads a lost reply as a definite success or a definite failure.
func TestDaemonStopLostReplyAndTransferFailurePreservesUnknownOutcome(t *testing.T) {
	probe := &countingLifecycleProbe{err: errors.New("lifecycle still busy")}
	done := startServingOneKill(t, probe, nil)

	err := requestDaemonStop(context.Background())
	require.ErrorIs(t, err, errKillOutcomeUnknown)
	require.ErrorContains(t, err, "awaiting explicit result", "the lost-reply diagnostic must survive")
	require.ErrorContains(t, err, "lifecycle confirmation also failed")
	awaitServingOneKill(t, done)
}

// TestDaemonStopDefiniteFailureReturnsWithoutLifecycleWait proves a definite
// KillFailed daemon-stop reply is returned immediately and never enters the
// lifecycle-transfer wait, so a rejected stop cannot be retried as if the
// outcome were still unknown.
func TestDaemonStopDefiniteFailureReturnsWithoutLifecycleWait(t *testing.T) {
	probe := &countingLifecycleProbe{err: errors.New("lifecycle must not be probed")}
	done := startServingOneKill(t, probe, func(kill protocol.Kill) protocol.ServerMessage {
		return protocol.KillResult{RequestID: kill.RequestID, Outcome: protocol.KillFailed, Code: protocol.ErrInternal, Text: "daemon refused stop"}
	})

	err := requestDaemonStop(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "daemon refused stop")
	require.NotErrorIs(t, err, errKillOutcomeUnknown, "a definite failure must not be reported as unknown")
	require.Zero(t, probe.attempts.Load(), "a definite failure must return without waiting for ownership transfer")
	awaitServingOneKill(t, done)
}

// TestDaemonStopUnknownResultKeepsUnknownWhenOwnershipReleaseFails pins the
// ownership-release branch: even when the probe grants ownership, a release
// failure keeps the lost-reply diagnostic instead of discarding it.
func TestDaemonStopUnknownResultKeepsUnknownWhenOwnershipReleaseFails(t *testing.T) {
	probe := &countingLifecycleProbe{owner: fakeLifecycleOwnership{release: func() error { return errors.New("unlock failed") }}}
	done := startServingOneKill(t, probe, nil)

	err := requestDaemonStop(context.Background())
	require.ErrorIs(t, err, errKillOutcomeUnknown)
	require.ErrorContains(t, err, "confirmed ownership release failed")
	require.ErrorContains(t, err, "unlock failed")
	awaitServingOneKill(t, done)
}
