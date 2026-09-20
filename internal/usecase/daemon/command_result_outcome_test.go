package daemon

// Explicit CommandResult daemon contract for the one-shot control path: a
// dispatched command has exactly one closed terminal outcome; a request whose
// deadline expires while its command is still running answers with exactly one
// correlated CommandOutcomeUnknown and never a second result for the late
// completion; and every definite refusal or success preserves the wire request
// correlation and a bounded error. All cases are deterministic and bounded
// (fake clock / PTY gates / awaitFrame timeouts), never sleeping.

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// gatedControlFactory blocks its first pane Open until release, so a test holds
// a dispatched command inside its PTY spawn while the request deadline expires.
// Later Opens (a background floating prewarm) return immediately.
type gatedControlFactory struct {
	entered chan struct{}
	release chan struct{}
	// gateOnce signals first-Open entry exactly once; releaseOnce closes the
	// release channel exactly once. They are separate because Open blocks inside
	// its own Once body waiting for the release that releaseOnce performs.
	gateOnce    sync.Once
	releaseOnce sync.Once

	mu          sync.Mutex
	ptys        []*gatedControlPTY
	first       *gatedControlPTY
	firstOpened bool
	shut        bool
}

func newGatedControlFactory() *gatedControlFactory {
	f := &gatedControlFactory{entered: make(chan struct{}), release: make(chan struct{})}
	// Pre-allocate the gated pane so a test can await its reader even while the
	// first Open is still parked inside the gate.
	f.first = &gatedControlPTY{readStarted: make(chan struct{}), stop: make(chan struct{})}
	return f
}

func (f *gatedControlFactory) Open(_ context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
	f.mu.Lock()
	first := f.first
	gate := !f.firstOpened
	if gate {
		f.firstOpened = true
	}
	f.mu.Unlock()
	if gate {
		f.gateOnce.Do(func() { close(f.entered) })
		<-f.release
		return first, nil
	}
	p := &gatedControlPTY{readStarted: make(chan struct{}), stop: make(chan struct{})}
	f.mu.Lock()
	f.ptys = append(f.ptys, p)
	shut := f.shut
	f.mu.Unlock()
	if shut {
		_ = p.Close()
	}
	return p, nil
}

func (f *gatedControlFactory) releaseOpen() {
	f.releaseOnce.Do(func() { close(f.release) })
}

func (f *gatedControlFactory) firstReadStarted() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.first.readStarted
}

func (f *gatedControlFactory) close() {
	f.releaseOpen()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shut = true
	_ = f.first.Close()
	for _, p := range f.ptys {
		_ = p.Close()
	}
}

// gatedControlPTY signals when its reader starts and parks until closed, so a
// test observes a committed tab's launched reader without sleeping.
type gatedControlPTY struct {
	readStarted chan struct{}
	stop        chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
}

func (p *gatedControlPTY) Read([]byte) (int, error) {
	p.startOnce.Do(func() { close(p.readStarted) })
	<-p.stop
	return 0, io.EOF
}

func (*gatedControlPTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*gatedControlPTY) Resize(domain.Geometry) error { return nil }
func (*gatedControlPTY) Pid() int                     { return 0 }
func (*gatedControlPTY) ForegroundPgid() (int, error) { return 0, nil }

func (p *gatedControlPTY) Close() error {
	p.closeOnce.Do(func() { close(p.stop) })
	return nil
}

func controlSessionTabCount(t *testing.T, d *Daemon, name string) int {
	t.Helper()
	d.mu.Lock()
	sess := d.findByNameLocked(name)
	d.mu.Unlock()
	require.NotNil(t, sess, "session %q must exist", name)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.tabs)
}

// requireNoFurtherCommandResults proves the handler produced no second result.
func requireNoFurtherCommandResults(t *testing.T, sends chan wire.Envelope) {
	t.Helper()
	for {
		select {
		case frame := <-sends:
			if envelopeMessageName(t, frame.Payload) != "CommandResult" {
				continue
			}
			message, err := sessionwire.DecodeServerEnvelope(frame.Payload)
			require.NoError(t, err)
			t.Fatalf("command answered a second result: %+v", message)
		default:
			return
		}
	}
}

// commandResultCaptureTransport records every server envelope and whether the
// one-shot control connection was retired, so a test can assert both without a
// mock expectation race.
type commandResultCaptureTransport struct {
	mu     sync.Mutex
	sends  []wire.Envelope
	closed bool
}

func (c *commandResultCaptureTransport) Send(envelope wire.Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sends = append(c.sends, envelope)
	return nil
}

func (*commandResultCaptureTransport) Recv() (wire.Envelope, error) {
	return wire.Envelope{}, io.EOF
}

func (c *commandResultCaptureTransport) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *commandResultCaptureTransport) commandResults(t *testing.T) []protocol.CommandResult {
	t.Helper()
	c.mu.Lock()
	frames := append([]wire.Envelope(nil), c.sends...)
	c.mu.Unlock()
	results := make([]protocol.CommandResult, 0, len(frames))
	for _, frame := range frames {
		message, err := sessionwire.DecodeServerEnvelope(frame.Payload)
		require.NoError(t, err)
		if result, ok := message.(protocol.CommandResult); ok {
			results = append(results, result)
		}
	}
	return results
}

func (c *commandResultCaptureTransport) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// TestHandleCommandTimeoutSendsOneCorrelatedUnknownAndDropsLateCompletion is
// the core timeout contract: the command is dispatched and still running when
// the deadline expires, so the client receives exactly one CommandOutcomeUnknown
// correlated to its RequestID. The command then commits late; that late
// completion produces no second result and cannot change the observed outcome.
func TestHandleCommandTimeoutSendsOneCorrelatedUnknownAndDropsLateCompletion(t *testing.T) {
	factory := newGatedControlFactory()
	clock := &signalClock{timers: make(chan *signalTimer, 16)}
	d := newTestDaemon(t, factory, clock)
	addControlSession(d, "work", "t_work", "p_work")
	t.Cleanup(func() {
		factory.close()
		d.sessWg.Wait()
	})

	frame := commandFrame(t, protocol.CommandRequest{RequestID: 17, Slug: "new-tab", TargetSession: "work"})
	tr, sends, releaseConn := newConn(t, frame)
	defer releaseConn()
	done := make(chan error, 1)
	go func() { done <- d.handleCommandFrame(tr, frame) }()

	// The command is admitted and parked inside its pane spawn.
	awaitTestCompletion(t, factory.entered, "command was not dispatched")
	timer := <-clock.timers
	require.Equal(t, CommandRequestTimeout, timer.duration)
	timer.ch <- time.Time{}

	result := awaitCommandResult(t, sends)
	require.Equal(t, uint64(17), result.RequestID, "the unknown outcome must correlate to its request")
	require.Equal(t, protocol.CommandOutcomeUnknown, result.Outcome)
	require.True(t, result.Valid(), "%+v must obey the outcome contract", result)
	require.Zero(t, result.Code, "an unknown outcome carries no definite code")
	require.Empty(t, result.Output, "an unknown outcome carries no output")
	require.Equal(t, ErrCommandRequestTimeout.Error(), result.Text)
	require.NoError(t, <-done, "the one-shot handler must answer and return")

	// Release the parked command: it now commits after the client already saw
	// the unknown outcome.
	factory.releaseOpen()
	awaitTestCompletion(t, factory.firstReadStarted(), "late completion never committed its tab")
	require.Equal(t, 2, controlSessionTabCount(t, d, "work"), "the timed-out command still committed late")

	requireNoFurtherCommandResults(t, sends)
}

// TestHandleCommandTimeoutRetiresConnectionAfterOneResult proves the timeout
// path attempts exactly one correlated unknown result and then retires the
// one-shot control connection, so a late completion has no channel to answer on.
func TestHandleCommandTimeoutRetiresConnectionAfterOneResult(t *testing.T) {
	factory := newGatedControlFactory()
	clock := &signalClock{timers: make(chan *signalTimer, 16)}
	d := newTestDaemon(t, factory, clock)
	addControlSession(d, "work", "t_work", "p_work")
	t.Cleanup(func() {
		factory.close()
		d.sessWg.Wait()
	})

	capture := &commandResultCaptureTransport{}
	frame := commandFrame(t, protocol.CommandRequest{RequestID: 23, Slug: "new-tab", TargetSession: "work"})
	done := make(chan error, 1)
	go func() { done <- d.handleCommandFrame(&rawServerConnection{raw: capture}, frame) }()

	awaitTestCompletion(t, factory.entered, "command was not dispatched")
	timer := <-clock.timers
	require.Equal(t, CommandRequestTimeout, timer.duration)
	timer.ch <- time.Time{}
	require.NoError(t, <-done)

	results := capture.commandResults(t)
	require.Len(t, results, 1, "a timed-out command answers exactly one result")
	require.Equal(t, uint64(23), results[0].RequestID)
	require.Equal(t, protocol.CommandOutcomeUnknown, results[0].Outcome)
	require.True(t, capture.isClosed(), "a one-shot control connection is retired after its result")
}

// TestHandleCommandDefiniteOutcomesPreserveCorrelationAndOutcome proves each
// definite refusal and each success answers once with the closed outcome, the
// wire request correlation, and (for failures) a bounded error and no output.
func TestHandleCommandDefiniteOutcomesPreserveCorrelationAndOutcome(t *testing.T) {
	tests := []struct {
		name        string
		request     protocol.CommandRequest
		wantOutcome protocol.CommandOutcome
		wantCode    uint16
		wantOutput  bool
	}{
		{
			name:        "success carries a bounded output",
			request:     protocol.CommandRequest{Slug: "list-sessions"},
			wantOutcome: protocol.CommandSucceeded,
		},
		{
			name:        "unknown slug is a definite failure",
			request:     protocol.CommandRequest{Slug: "no-such"},
			wantOutcome: protocol.CommandFailed,
			wantCode:    protocol.ErrUnknownCommand,
		},
		{
			name:        "non-scriptable command is a definite failure",
			request:     protocol.CommandRequest{Slug: "session-picker"},
			wantOutcome: protocol.CommandFailed,
			wantCode:    protocol.ErrNotScriptable,
		},
		{
			name:        "version mismatch is a definite failure",
			request:     protocol.CommandRequest{Version: protocol.Version + 1, Slug: "list-sessions"},
			wantOutcome: protocol.CommandFailed,
			wantCode:    protocol.ErrVersionMismatch,
		},
		{
			name:        "attached relay refusal is a definite failure",
			request:     protocol.CommandRequest{Attached: true, Slug: "list-sessions"},
			wantOutcome: protocol.CommandFailed,
			wantCode:    protocol.ErrNotScriptable,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			request := tt.request
			request.RequestID = uint64(31 + i)

			result := sendCommand(t, d, request)

			require.Equal(t, request.RequestID, result.RequestID, "the outcome must correlate to its request")
			require.Equal(t, tt.wantOutcome, result.Outcome)
			require.True(t, result.Valid(), "%+v must obey the outcome contract", result)
			if tt.wantOutcome == protocol.CommandFailed {
				require.Equal(t, tt.wantCode, result.Code)
				require.NotEmpty(t, result.Text, "a definite failure must carry bounded error text")
				require.LessOrEqual(t, len(result.Text), wire.ControlEnvelopeLimit)
				require.Empty(t, result.Output, "a failure must not carry success output")
			} else {
				require.Zero(t, result.Code)
				require.Empty(t, result.Text)
				require.NotEmpty(t, result.Output)
			}
		})
	}
}

// TestAttachedCommandRefusalSendsDefiniteFailedOutcome proves an attached
// command refusal answers with a decodable, definite failure correlated to the
// frame's RequestID rather than an unspecified outcome the wire boundary would
// refuse.
func TestAttachedCommandRefusalSendsDefiniteFailedOutcome(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)

	require.False(t, d.handleAttachmentClientFrame(token, mustClientEnvelope(protocol.CommandRequest{
		Version: protocol.Version, RequestID: 41, Slug: "no-such", Attached: true,
	})))

	var result protocol.CommandResult
	found := false
	for _, frame := range transport.Sends() {
		message, err := sessionwire.DecodeServerEnvelope(frame.Payload)
		require.NoError(t, err)
		if decoded, ok := message.(protocol.CommandResult); ok {
			result, found = decoded, true
		}
	}
	require.True(t, found, "an attached refusal must answer with a CommandResult")
	require.Equal(t, uint64(41), result.RequestID)
	require.Equal(t, protocol.CommandFailed, result.Outcome)
	require.Equal(t, protocol.ErrUnknownCommand, result.Code)
	require.True(t, result.Valid(), "%+v must obey the outcome contract", result)
}
