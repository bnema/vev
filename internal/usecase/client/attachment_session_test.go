package client

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Real attachment worker tests (Plan 001 P5.3b). They are deterministic and
// drive the typed session protocol directly: no wall-clock sleeps, no real
// terminal, and no broker process.

var errSessionTestClosed = errors.New("session test stream is closed")

// sessionTestStream is one scripted broker logical connection. It records every
// typed client message, delivers scripted server messages, and unblocks
// ReceiveServer on Close exactly as the ports.BrokerLogicalConnection contract
// requires.
type sessionTestStream struct {
	mu        sync.Mutex
	sent      []protocol.ClientMessage
	incoming  chan protocol.ServerMessage
	closed    chan struct{}
	closeOnce sync.Once
	err       error
}

func newSessionTestStream() *sessionTestStream {
	return &sessionTestStream{
		incoming: make(chan protocol.ServerMessage, 8),
		closed:   make(chan struct{}),
	}
}

func (s *sessionTestStream) SendClient(message protocol.ClientMessage) error {
	select {
	case <-s.closed:
		return errSessionTestClosed
	default:
	}
	s.mu.Lock()
	s.sent = append(s.sent, message)
	s.mu.Unlock()
	return nil
}

func (s *sessionTestStream) ReceiveServer() (protocol.ServerMessage, error) {
	select {
	case message := <-s.incoming:
		return message, nil
	case <-s.closed:
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		if err != nil {
			return nil, errors.Join(io.EOF, err)
		}
		return nil, io.EOF
	}
}

func (s *sessionTestStream) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}

func (s *sessionTestStream) LinkState() ports.LinkState { return ports.LinkStateConnected }

func (s *sessionTestStream) LinkEvents() <-chan ports.LinkEvent { return nil }

func (s *sessionTestStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *sessionTestStream) Done() <-chan struct{} { return s.closed }

func (s *sessionTestStream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *sessionTestStream) deliver(message protocol.ServerMessage) {
	select {
	case s.incoming <- message:
	case <-s.closed:
	}
}

// fail publishes the stable stream cause and closes, exactly as a transported
// logical stream does on loss.
func (s *sessionTestStream) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
	_ = s.Close()
}

func (s *sessionTestStream) closedNow() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *sessionTestStream) messages() []protocol.ClientMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.ClientMessage(nil), s.sent...)
}

// sessionTestStreamWithDeadline additionally implements
// ports.HandshakeDeadlineProvider, so the supervisor-level deadline tests can
// prove an accepted absolute deadline is adopted verbatim.
type sessionTestStreamWithDeadline struct {
	*sessionTestStream
	deadline time.Time
}

func (s *sessionTestStreamWithDeadline) HandshakeDeadline() time.Time { return s.deadline }

func sessionTestOutput(publication uint64, data string) protocol.Output {
	return protocol.Output{
		Epoch: 1, New: 1, Full: true,
		Size: domain.Size{Cols: 80, Rows: 24},
		Context: &protocol.ViewContext{
			Publication: publication,
			Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{
				LifecycleID: domain.SessionLifecycleID{1},
				SessionName: "alpha",
			}},
			TabID:         "t_abc123",
			FocusedPaneID: "p_def456",
		},
		Data: []byte(data),
	}
}

func sessionTestRequest(local bool) ports.BrokerOpenStreamRequest {
	request := ports.BrokerOpenStreamRequest{
		Epoch:     3,
		Purpose:   ports.BrokerStreamAttachment,
		Admission: ports.BrokerAdmissionExact,
		Local:     local,
		Target: protocol.ExactSessionTarget{
			LifecycleID: domain.SessionLifecycleID{1},
			SessionName: "alpha",
		},
		Policy: ports.BrokerPolicy{
			ProtocolVersion:      protocol.Version,
			CatalogSchemaVersion: 1,
			EnvironmentPolicy:    protocol.EnvironmentPolicyClientOwned,
			Transport:            "transport",
			Trust:                "trust",
			Launch:               "launch",
			Isolation:            "isolation",
		},
	}
	if !local {
		request.Endpoint = "user@example"
		request.Registration = domain.RemoteRegistration{
			Endpoint:    "user@example",
			Incarnation: [16]byte{1},
			Generation:  1,
		}
	}
	return request
}

func sessionTestWorkerConfig(request ports.BrokerOpenStreamRequest) sessionAttachmentConfig {
	return sessionAttachmentConfig{
		Request:  request,
		ClientID: [16]byte{0xAB},
		Geometry: domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}},
	}
}

// sessionTestAttachments records every successful MarkAttached without
// draining: a wait observes the transition but never consumes it, so the test
// can assert attachment after a script already synchronized on it.
type sessionTestAttachments struct {
	count  atomic.Int64
	tokens chan AttachmentToken
}

func newSessionTestAttachments() *sessionTestAttachments {
	return &sessionTestAttachments{tokens: make(chan AttachmentToken, 8)}
}

func (a *sessionTestAttachments) record(token AttachmentToken) {
	a.count.Add(1)
	select {
	case a.tokens <- token:
	default:
	}
}

func (a *sessionTestAttachments) attached() bool { return a.count.Load() > 0 }

func (a *sessionTestAttachments) token(t *testing.T) AttachmentToken {
	t.Helper()
	select {
	case token := <-a.tokens:
		return token
	case <-time.After(5 * time.Second):
		t.Fatal("the worker never attached")
		return AttachmentToken{}
	}
}

// sessionTestRun boots one worker through the real foreground host.
func sessionTestRun(t *testing.T, stream ports.BrokerLogicalConnection, terminal *workerTestTerminal, cfg sessionAttachmentConfig, attached *sessionTestAttachments) <-chan AttachmentEvent {
	t.Helper()
	host := newWorkerTestHost(terminal, nil, attached.record)
	worker := newSessionAttachmentWorker(cfg)
	events := make(chan AttachmentEvent, 1)
	go func() {
		event, _ := host.Run(context.Background(), AttachmentToken{Generation: 4, Attempt: 1}, worker, stream)
		events <- event
	}()
	return events
}

// awaitHello blocks until the worker sent its Hello.
func awaitHello(t *testing.T, stream *sessionTestStream) protocol.Hello {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		for _, message := range stream.messages() {
			if hello, ok := message.(protocol.Hello); ok {
				return hello
			}
		}
		select {
		case <-deadline:
			t.Fatal("the worker never sent Hello")
			return protocol.Hello{}
		case <-time.After(time.Millisecond):
		}
	}
}

// TestSessionAttachmentWorkerStaysConnectingUntilPublication proves Welcome is
// never attachment: only a validated, committed initial publication marks
// attached, and the frame is written and flushed through the UI transaction.
func TestSessionAttachmentWorkerStaysConnectingUntilPublication(t *testing.T) {
	stream := newSessionTestStream()
	terminal := newWorkerTestTerminal()
	attached := newSessionTestAttachments()
	events := sessionTestRun(t, stream, terminal, sessionTestWorkerConfig(sessionTestRequest(true)), attached)

	hello := awaitHello(t, stream)
	require.Equal(t, protocol.Version, hello.Version)
	require.Equal(t, protocol.IntentAttach, hello.Intent)
	require.NotNil(t, hello.ExactTarget)
	require.False(t, hello.Remote)

	stream.deliver(protocol.Welcome{SessionName: "alpha"})
	// Welcome alone must not attach, write, or publish.
	require.Never(t, attached.attached, 20*time.Millisecond, 2*time.Millisecond, "Welcome alone must never attach")
	require.Zero(t, terminal.flushCount(), "Welcome alone writes nothing")
	require.Empty(t, terminal.publications(), "Welcome alone publishes nothing")

	stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
	require.Equal(t, AttachmentToken{Generation: 4, Attempt: 1}, attached.token(t))
	require.Equal(t, "\x1b[Hready", terminal.written())
	require.Equal(t, 1, terminal.flushCount())
	require.Len(t, terminal.publications(), 1)
	require.Equal(t, ports.UIStatusTransitioning, terminal.publications()[0].Status)

	require.NoError(t, stream.Close())
	select {
	case event := <-events:
		require.Equal(t, AttachmentEventEnded, event.Kind)
		require.Equal(t, AttachmentToken{Generation: 4, Attempt: 1}, event.Token)
	case <-time.After(5 * time.Second):
		t.Fatal("the worker never settled after the stream closed")
	}
}

// TestSessionAttachmentWorkerFailureStages is the EOF/refusal coverage table:
// before Welcome, after Welcome but before publication, and after attachment,
// plus a protocol refusal and a publication failure.
func TestSessionAttachmentWorkerFailureStages(t *testing.T) {
	lossErr := ports.BrokerStreamLost{
		Connection: ports.BrokerConnectionID{1},
		Stream:     1,
		Epoch:      3,
		Cause:      domain.RemoteFailureTransport,
		Err:        errors.New("transport lost"),
	}
	publishErr := errors.New("ui publication failed")

	tests := []struct {
		name             string
		script           func(stream *sessionTestStream, terminal *workerTestTerminal, attached *sessionTestAttachments)
		wantKind         AttachmentEventKind
		wantAttached     bool
		wantErr          error
		wantProtocolCode uint16
	}{
		{
			name:     "eof_before_welcome",
			script:   func(stream *sessionTestStream, _ *workerTestTerminal, _ *sessionTestAttachments) { _ = stream.Close() },
			wantKind: AttachmentEventEnded,
		},
		{
			name: "refusal_before_welcome",
			script: func(stream *sessionTestStream, _ *workerTestTerminal, _ *sessionTestAttachments) {
				stream.deliver(protocol.ErrorMsg{Code: protocol.ErrNoSuchSession, Text: "no such session"})
			},
			wantKind:         AttachmentEventFailed,
			wantProtocolCode: protocol.ErrNoSuchSession,
		},
		{
			name: "unexpected_before_welcome",
			script: func(stream *sessionTestStream, _ *workerTestTerminal, _ *sessionTestAttachments) {
				stream.deliver(protocol.Sessions{})
			},
			wantKind: AttachmentEventFailed,
		},
		{
			name: "eof_after_welcome_before_publication",
			script: func(stream *sessionTestStream, _ *workerTestTerminal, _ *sessionTestAttachments) {
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				_ = stream.Close()
			},
			wantKind: AttachmentEventEnded,
		},
		{
			name: "invalid_initial_publication",
			script: func(stream *sessionTestStream, _ *workerTestTerminal, _ *sessionTestAttachments) {
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				// A full frame with no context never commits.
				stream.deliver(protocol.Output{Epoch: 1, New: 1, Full: true})
			},
			wantKind: AttachmentEventFailed,
			wantErr:  errAttachmentPublication,
		},
		{
			name: "publication_failure_before_attachment",
			script: func(stream *sessionTestStream, terminal *workerTestTerminal, _ *sessionTestAttachments) {
				terminal.publishErr = publishErr
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
			},
			wantKind: AttachmentEventFailed,
			wantErr:  publishErr,
		},
		{
			name: "unavailable_observation_commits_attachment",
			script: func(stream *sessionTestStream, terminal *workerTestTerminal, attached *sessionTestAttachments) {
				terminal.publishErr = ports.ErrUIUnavailable
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
				attached.token(t)
				_ = stream.Close()
			},
			wantKind:     AttachmentEventEnded,
			wantAttached: true,
		},
		{
			name: "eof_after_attachment",
			script: func(stream *sessionTestStream, _ *workerTestTerminal, attached *sessionTestAttachments) {
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
				attached.token(t)
				_ = stream.Close()
			},
			wantKind:     AttachmentEventEnded,
			wantAttached: true,
		},
		{
			name: "loss_after_attachment",
			script: func(stream *sessionTestStream, _ *workerTestTerminal, attached *sessionTestAttachments) {
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
				attached.token(t)
				stream.fail(lossErr)
			},
			wantKind:     AttachmentEventLost,
			wantAttached: true,
			wantErr:      lossErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newSessionTestStream()
			terminal := newWorkerTestTerminal()
			attached := newSessionTestAttachments()
			events := sessionTestRun(t, stream, terminal, sessionTestWorkerConfig(sessionTestRequest(true)), attached)
			awaitHello(t, stream)

			tt.script(stream, terminal, attached)

			var event AttachmentEvent
			select {
			case event = <-events:
			case <-time.After(5 * time.Second):
				t.Fatal("the worker never settled")
			}
			require.Equal(t, AttachmentToken{Generation: 4, Attempt: 1}, event.Token)
			require.Equal(t, tt.wantKind, event.Kind, "event error: %v", event.Err)
			if tt.wantErr != nil {
				require.ErrorIs(t, event.Err, tt.wantErr)
			}
			if tt.wantProtocolCode != 0 {
				var protocolErr *ProtocolError
				require.ErrorAs(t, event.Err, &protocolErr)
				require.Equal(t, tt.wantProtocolCode, protocolErr.Code)
			}
			require.Equal(t, tt.wantAttached, attached.attached())
			if !tt.wantAttached {
				// A frame that never committed was never written or flushed; the
				// UI transaction still observed its own boundary.
				require.Empty(t, terminal.written(), "an uncommitted frame must not be written")
				require.Zero(t, terminal.flushCount(), "an uncommitted frame must not be flushed")
			}
		})
	}
}

func TestSessionAttachmentWorkerHelloEnvironment(t *testing.T) {
	tests := []struct {
		name string
		env  AttachmentEnvironment
	}{
		{name: "unset"},
		{name: "supplied", env: AttachmentEnvironment{TermEnv: "xterm-256color", Cwd: "/workspace", TrueColor: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newSessionTestStream()
			terminal := newWorkerTestTerminal()
			attached := newSessionTestAttachments()
			supervisor := &Supervisor{cfg: SupervisorConfig{Terminal: terminal, AttachmentEnvironment: tt.env}, clientID: [16]byte{1}}
			worker, err := supervisor.newAttachmentWorker(sessionTestRequest(true), nil)
			require.NoError(t, err)
			events := make(chan AttachmentEvent, 1)
			host := newWorkerTestHost(terminal, nil, attached.record)
			go func() {
				event, _ := host.Run(context.Background(), AttachmentToken{Generation: 1, Attempt: 1}, worker, stream)
				events <- event
			}()

			hello := awaitHello(t, stream)
			require.Equal(t, tt.env.TermEnv, hello.TermEnv)
			require.Equal(t, tt.env.Cwd, hello.Cwd)
			require.Equal(t, tt.env.TrueColor, hello.TrueColor)
			require.NoError(t, stream.Close())
			<-events
		})
	}
}

func TestSessionAttachmentWorkerHandlesProtocolErrorAfterWelcome(t *testing.T) {
	tests := []struct {
		name     string
		attached bool
	}{
		{name: "awaiting initial publication"},
		{name: "attached pump", attached: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newSessionTestStream()
			terminal := newWorkerTestTerminal()
			attachments := newSessionTestAttachments()
			events := sessionTestRun(t, stream, terminal, sessionTestWorkerConfig(sessionTestRequest(true)), attachments)
			awaitHello(t, stream)
			stream.deliver(protocol.Welcome{SessionName: "alpha"})
			if tt.attached {
				stream.deliver(sessionTestOutput(1, "initial"))
				require.Eventually(t, attachments.attached, time.Second, time.Millisecond)
			}
			stream.deliver(protocol.ErrorMsg{Code: 42, Text: "refused"})
			event := <-events
			var protocolErr *ProtocolError
			require.ErrorAs(t, event.Err, &protocolErr)
			require.Equal(t, uint16(42), protocolErr.Code)
			require.Equal(t, "refused", protocolErr.Text)
		})
	}
}

func TestSessionAttachmentWorkerValidatesIdentityByAdmission(t *testing.T) {
	namedRequest := sessionTestRequest(true)
	namedRequest.Admission, namedRequest.Name, namedRequest.Target = ports.BrokerAdmissionCreateNamed, "alpha", protocol.ExactSessionTarget{}
	ephemeralRequest := sessionTestRequest(true)
	ephemeralRequest.Admission, ephemeralRequest.Target = ports.BrokerAdmissionCreateEphemeral, protocol.ExactSessionTarget{}
	tests := []struct {
		name    string
		request ports.BrokerOpenStreamRequest
		welcome protocol.Welcome
		output  protocol.Output
		wantErr bool
	}{
		{name: "exact welcome mismatch", request: sessionTestRequest(true), welcome: protocol.Welcome{CommittedIdentity: &protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "alpha"}}}, wantErr: true},
		{name: "exact publication mismatch", request: sessionTestRequest(true), welcome: protocol.Welcome{SessionName: "alpha"}, output: func() protocol.Output {
			o := sessionTestOutput(1, "wrong")
			o.Context.Route.Target.SessionName = "beta"
			return o
		}(), wantErr: true},
		{name: "exact match", request: sessionTestRequest(true), welcome: protocol.Welcome{CommittedIdentity: &protocol.CommittedRouteIdentity{Target: sessionTestRequest(true).Target}}, output: sessionTestOutput(1, "ok")},
		{name: "create named welcome mismatch", request: namedRequest, welcome: protocol.Welcome{SessionName: "beta"}, wantErr: true},
		{name: "create named publication mismatch", request: namedRequest, welcome: protocol.Welcome{SessionName: "alpha"}, output: func() protocol.Output {
			o := sessionTestOutput(1, "wrong")
			o.Context.Route.Target.SessionName = "beta"
			return o
		}(), wantErr: true},
		{name: "create named match", request: namedRequest, welcome: protocol.Welcome{SessionName: "alpha"}, output: sessionTestOutput(1, "ok")},
		{name: "create ephemeral accepts committed identity", request: ephemeralRequest, welcome: protocol.Welcome{CommittedIdentity: &protocol.CommittedRouteIdentity{Target: sessionTestOutput(1, "").Context.Route.Target}}, output: sessionTestOutput(1, "ok")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newSessionTestStream()
			terminal := newWorkerTestTerminal()
			attached := newSessionTestAttachments()
			events := sessionTestRun(t, stream, terminal, sessionTestWorkerConfig(tt.request), attached)
			awaitHello(t, stream)
			stream.deliver(tt.welcome)
			if tt.output.Context != nil {
				stream.deliver(tt.output)
			}
			if tt.wantErr {
				event := <-events
				var identityErr *AttachmentIdentityError
				require.ErrorAs(t, event.Err, &identityErr)
				require.False(t, attached.attached())
				require.Empty(t, terminal.written())
				return
			}
			require.Eventually(t, attached.attached, time.Second, time.Millisecond)
			require.Equal(t, "ok", terminal.written())
			require.NoError(t, stream.Close())
			<-events
		})
	}
}

// TestSessionAttachmentWorkerRemoteHello proves the same worker derives a
// remote Hello from a remote request: the typed-preamble rules stay per
// logical stream, and no endpoint is rewritten.
func TestSessionAttachmentWorkerRemoteHello(t *testing.T) {
	stream := newSessionTestStream()
	terminal := newWorkerTestTerminal()
	attached := newSessionTestAttachments()
	request := sessionTestRequest(false)
	events := sessionTestRun(t, stream, terminal, sessionTestWorkerConfig(request), attached)

	hello := awaitHello(t, stream)
	require.True(t, hello.Remote)
	require.Equal(t, protocol.EnvironmentPolicyClientOwned, hello.EnvironmentPolicy)
	require.Equal(t, protocol.IntentAttach, hello.Intent)
	require.NotNil(t, hello.ExactTarget)
	require.Equal(t, "alpha", hello.ExactTarget.SessionName)

	require.NoError(t, stream.Close())
	select {
	case event := <-events:
		require.Equal(t, AttachmentEventEnded, event.Kind)
	case <-time.After(5 * time.Second):
		t.Fatal("the worker never settled")
	}
}

// TestSessionAttachmentWorkerNeverReadsTerminalInput proves the worker has no
// path to terminal input: it consumes no bytes and sends no Input frame while
// the foreground owns no input source.
func TestSessionAttachmentWorkerNeverReadsTerminalInput(t *testing.T) {
	stream := newSessionTestStream()
	terminal := newWorkerTestTerminal()
	attached := newSessionTestAttachments()
	events := sessionTestRun(t, stream, terminal, sessionTestWorkerConfig(sessionTestRequest(true)), attached)

	awaitHello(t, stream)
	stream.deliver(protocol.Welcome{SessionName: "alpha"})
	stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
	_ = attached.token(t)

	for _, message := range stream.messages() {
		_, isInput := message.(protocol.Input)
		require.False(t, isInput, "the worker must never send terminal input in P5.3b")
	}
	require.Equal(t, "\x1b[Hready", terminal.written(), "only session output reaches the terminal")

	require.NoError(t, stream.Close())
	select {
	case <-events:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker never settled")
	}
}
