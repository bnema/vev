package client

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

// Real attachment worker tests (Plan 001 P5.3b). They are deterministic and
// drive the typed session protocol directly: no wall-clock sleeps, no real
// terminal, and no broker process.

var errSessionTestClosed = errors.New("session test stream is closed")

// streamLog records scripted traffic without implementing any application port.
type streamLog struct {
	conn      *portsmocks.MockBrokerLogicalConnection
	mu        sync.Mutex
	sent      []protocol.ClientMessage
	incoming  chan protocol.ServerMessage
	closed    chan struct{}
	closeOnce sync.Once
	err       error
	// closeHook, when set, runs once during Close so a test can observe what
	// the supervisor revoked before it closed the stream.
	closeHook func()
}

func newSessionStream(t *testing.T, opts ...func(*portsmocks.MockBrokerLogicalConnection, *streamLog)) *streamLog {
	s := &streamLog{incoming: make(chan protocol.ServerMessage, 8), closed: make(chan struct{})}
	return configureSessionStream(t, s, opts...)
}
func configureSessionStream(t *testing.T, s *streamLog, opts ...func(*portsmocks.MockBrokerLogicalConnection, *streamLog)) *streamLog {
	s.conn = portsmocks.NewMockBrokerLogicalConnection(t)
	for _, opt := range opts {
		opt(s.conn, s)
	}
	s.conn.EXPECT().SendClient(mock.Anything).RunAndReturn(s.recordSend).Maybe()
	s.conn.EXPECT().ReceiveServer().RunAndReturn(func() (protocol.ServerMessage, error) {
		select {
		case message := <-s.incoming:
			return message, nil
		case <-s.closed:
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.err != nil {
				return nil, errors.Join(io.EOF, s.err)
			}
			return nil, io.EOF
		}
	}).Maybe()
	s.conn.EXPECT().Capabilities().Return(protocol.ConnectionCapabilities{}).Maybe()
	s.conn.EXPECT().LinkState().Return(ports.LinkStateConnected).Maybe()
	s.conn.EXPECT().LinkEvents().Return(nil).Maybe()
	s.conn.EXPECT().Close().RunAndReturn(func() error {
		s.closeOnce.Do(func() {
			close(s.closed)
			if s.closeHook != nil {
				s.closeHook()
			}
		})
		return nil
	}).Maybe()
	s.conn.EXPECT().Done().Return((<-chan struct{})(s.closed)).Maybe()
	s.conn.EXPECT().Err().RunAndReturn(func() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }).Maybe()
	return s
}

func (s *streamLog) recordSend(message protocol.ClientMessage) error {
	select {
	case <-s.closed:
		return errSessionTestClosed
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, message)
	return nil
}

type blockingSendSessionStream struct {
	*streamLog
	entered  chan struct{}
	finished chan struct{}
}

func (s *blockingSendSessionStream) send(protocol.ClientMessage) error {
	close(s.entered)
	<-s.closed
	close(s.finished)
	return errSessionTestClosed
}

func TestSessionAttachmentSendCancellationClosesStreamAndJoinsSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stream := newBlockingSendSessionStream(t)
		fg := &attachmentForeground{done: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		returned := make(chan error, 1)
		go func() {
			returned <- (&sessionAttachmentWorker{}).send(ctx, fg, stream.conn, protocol.Detach{})
		}()

		<-stream.entered
		cancel()
		require.ErrorIs(t, <-returned, context.Canceled)
		select {
		case <-stream.conn.Done():
		default:
			t.Fatal("cancellation did not close stream")
		}
		select {
		case <-stream.finished:
		default:
			t.Fatal("send goroutine was not joined")
		}
	})
}

func TestSessionAttachmentSettleReportsClosedProcess(t *testing.T) {
	tests := []struct {
		name       string
		cause      error
		wantClosed bool
	}{
		{name: "closed process sends closed detach", cause: ErrClientClosed, wantClosed: true},
		{name: "plain cancellation sends nothing", cause: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stream := newSessionStream(t)
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(tt.cause)
				w := &sessionAttachmentWorker{cfg: sessionAttachmentConfig{Clock: systemClock{}}}

				event := w.settle(ctx, &attachmentForeground{done: make(chan struct{})}, stream.conn, AttachmentToken{}, nil)

				require.Equal(t, AttachmentEventFailed, event.Kind)
				stream.mu.Lock()
				defer stream.mu.Unlock()
				if tt.wantClosed {
					require.Equal(t, []protocol.ClientMessage{protocol.Detach{Closed: true}}, stream.sent)
				} else {
					require.Empty(t, stream.sent)
				}
			})
		})
	}
}

func (s *streamLog) deliver(message protocol.ServerMessage) {
	select {
	case s.incoming <- message:
	case <-s.closed:
	}
}

// fail publishes the stable stream cause and closes, exactly as a transported
// logical stream does on loss.
func (s *streamLog) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
	_ = s.conn.Close()
}

func (s *streamLog) closedNow() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *streamLog) messages() []protocol.ClientMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.ClientMessage(nil), s.sent...)
}

// deadlineSessionMocks composes generated ports for the optional deadline contract.
type deadlineSessionMocks struct {
	*portsmocks.MockBrokerLogicalConnection
	*portsmocks.MockHandshakeDeadlineProvider
}

func sessionDeadline(t *testing.T, log *streamLog, deadline time.Time) ports.BrokerLogicalConnection {
	provider := portsmocks.NewMockHandshakeDeadlineProvider(t)
	provider.EXPECT().HandshakeDeadline().Return(deadline).Maybe()
	return deadlineSessionMocks{log.conn, provider}
}

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
		Request:            request,
		ClientID:           [16]byte{0xAB},
		Geometry:           domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}},
		SessionEnvironment: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Env: append([]string(nil), request.Env...)},
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
	synctest.Wait()
	select {
	case token := <-a.tokens:
		return token
	default:
		t.Fatal("the worker never attached")
		return AttachmentToken{}
	}
}

// sessionTestRun boots one worker through the real foreground host.
func sessionTestRun(t *testing.T, stream ports.BrokerLogicalConnection, terminal *workerTestTerminal, cfg sessionAttachmentConfig, attached *sessionTestAttachments) <-chan AttachmentEvent {
	t.Helper()
	host := newWorkerTestHost(t, terminal, nil, attached.record)
	worker, err := newSessionAttachmentWorker(cfg)
	require.NoError(t, err)
	events := make(chan AttachmentEvent, 1)
	go func() {
		event, _ := host.Run(context.Background(), AttachmentToken{Generation: 4, Attempt: 1}, worker, stream)
		events <- event
	}()
	return events
}

// awaitHello returns the Hello the worker sent once the bubble is quiescent.
func awaitHello(t *testing.T, stream *streamLog) protocol.Hello {
	t.Helper()
	synctest.Wait()
	for _, message := range stream.messages() {
		if hello, ok := message.(protocol.Hello); ok {
			return hello
		}
	}
	t.Fatal("the worker never sent Hello")
	return protocol.Hello{}
}

// TestSessionAttachmentWorkerStaysConnectingUntilPublication proves Welcome is
// never attachment: only a validated, committed initial publication marks
// attached, and the frame is written and flushed through the UI transaction.
func TestSessionAttachmentWorkerStaysConnectingUntilPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stream := newSessionStream(t)
		terminal := newWorkerTestTerminal(t)
		attached := newSessionTestAttachments()
		events := sessionTestRun(t, stream.conn, terminal, sessionTestWorkerConfig(sessionTestRequest(true)), attached)

		hello := awaitHello(t, stream)
		require.Equal(t, protocol.Version, hello.Version)
		require.Equal(t, protocol.IntentAttach, hello.Intent)
		require.NotNil(t, hello.ExactTarget)
		require.False(t, hello.Remote)
		// An exact attach carries its target's session name: the strict Hello
		// contract requires Name to equal the exact target's name, so an empty Name
		// would be rejected before the daemon ever routes it.
		require.Equal(t, hello.ExactTarget.SessionName, hello.Name)
		require.NoError(t, protocol.ValidateHello(hello))

		stream.deliver(protocol.Welcome{SessionName: "alpha"})
		// Welcome alone must not attach, write, or publish.
		synctest.Wait()
		require.False(t, attached.attached(), "Welcome alone must never attach")
		require.Zero(t, terminal.flushCount(), "Welcome alone writes nothing")
		require.Empty(t, terminal.publications(), "Welcome alone publishes nothing")

		stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
		synctest.Wait()
		require.Equal(t, AttachmentToken{Generation: 4, Attempt: 1}, attached.token(t))
		require.Equal(t, "\x1b[Hready", terminal.written())
		require.Equal(t, 1, terminal.flushCount())
		require.Len(t, terminal.publications(), 2)
		// The frame commits the pre-attach Connecting presentation with a public
		// generation of zero, then the committed Attached presentation carries the
		// real action generation without writing a second frame.
		require.Equal(t, ports.UIStatusConnecting, terminal.publications()[0].Status)
		require.Zero(t, terminal.publications()[0].Generation)
		require.Equal(t, ports.UIStatusAttached, terminal.publications()[1].Status)
		require.Equal(t, uint64(4), terminal.publications()[1].Generation)
		require.Equal(t, 1, terminal.flushCount(), "the attached presentation writes no second frame")
		synctest.Wait()
		require.True(t, func() bool {
			for _, message := range stream.messages() {
				if ack, ok := message.(protocol.Ack); ok {
					return ack == (protocol.Ack{Epoch: 1, State: 1})
				}
			}
			return false
		}(), "committed initial output must release the daemon output window")

		require.NoError(t, stream.conn.Close())
		select {
		case event := <-events:
			require.Equal(t, AttachmentEventEnded, event.Kind)
			require.Equal(t, AttachmentToken{Generation: 4, Attempt: 1}, event.Token)
		case <-time.After(5 * time.Second):
			t.Fatal("the worker never settled after the stream closed")
		}
	})
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
		script           func(stream *streamLog, terminal *workerTestTerminal, attached *sessionTestAttachments)
		wantKind         AttachmentEventKind
		wantAttached     bool
		wantErr          error
		wantProtocolCode uint16
	}{
		{
			name:     "eof_before_welcome",
			script:   func(stream *streamLog, _ *workerTestTerminal, _ *sessionTestAttachments) { _ = stream.conn.Close() },
			wantKind: AttachmentEventEnded,
		},
		{
			name: "refusal_before_welcome",
			script: func(stream *streamLog, _ *workerTestTerminal, _ *sessionTestAttachments) {
				stream.deliver(protocol.ErrorMsg{Code: protocol.ErrNoSuchSession, Text: "no such session"})
			},
			wantKind:         AttachmentEventFailed,
			wantProtocolCode: protocol.ErrNoSuchSession,
		},
		{
			name: "unexpected_before_welcome",
			script: func(stream *streamLog, _ *workerTestTerminal, _ *sessionTestAttachments) {
				stream.deliver(protocol.Sessions{})
			},
			wantKind: AttachmentEventFailed,
		},
		{
			name: "eof_after_welcome_before_publication",
			script: func(stream *streamLog, _ *workerTestTerminal, _ *sessionTestAttachments) {
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				_ = stream.conn.Close()
			},
			wantKind: AttachmentEventEnded,
		},
		{
			name: "invalid_initial_publication",
			script: func(stream *streamLog, _ *workerTestTerminal, _ *sessionTestAttachments) {
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				// A full frame with no context never commits.
				stream.deliver(protocol.Output{Epoch: 1, New: 1, Full: true})
			},
			wantKind: AttachmentEventFailed,
			wantErr:  errAttachmentPublication,
		},
		{
			name: "publication_failure_before_attachment",
			script: func(stream *streamLog, terminal *workerTestTerminal, _ *sessionTestAttachments) {
				terminal.publishErr = publishErr
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
			},
			wantKind: AttachmentEventFailed,
			wantErr:  publishErr,
		},
		{
			name: "unavailable_observation_commits_attachment",
			script: func(stream *streamLog, terminal *workerTestTerminal, attached *sessionTestAttachments) {
				terminal.publishErr = ports.ErrUIUnavailable
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
				attached.token(t)
				_ = stream.conn.Close()
			},
			wantKind:     AttachmentEventEnded,
			wantAttached: true,
		},
		{
			name: "eof_after_attachment",
			script: func(stream *streamLog, _ *workerTestTerminal, attached *sessionTestAttachments) {
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
				attached.token(t)
				_ = stream.conn.Close()
			},
			wantKind:     AttachmentEventEnded,
			wantAttached: true,
		},
		{
			name: "loss_after_attachment",
			script: func(stream *streamLog, _ *workerTestTerminal, attached *sessionTestAttachments) {
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
			synctest.Test(t, func(t *testing.T) {
				stream := newSessionStream(t)
				terminal := newWorkerTestTerminal(t)
				attached := newSessionTestAttachments()
				events := sessionTestRun(t, stream.conn, terminal, sessionTestWorkerConfig(sessionTestRequest(true)), attached)
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
		})
	}
}

func TestSessionAttachmentWorkerHelloEnvironment(t *testing.T) {
	tests := []struct {
		name string
		env  AttachmentEnvironment
		want terminalcap.ColorCapabilities
	}{
		{name: "unset defaults to truecolor", want: terminalcap.ColorCapabilities{Mode: terminalcap.TrueColor}},
		{name: "supplied truecolor", env: AttachmentEnvironment{TermEnv: "xterm-256color", Cwd: "/workspace", Color: terminalcap.ColorCapabilities{Mode: terminalcap.TrueColor, Source: terminalcap.SourceDeclared}}, want: terminalcap.ColorCapabilities{Mode: terminalcap.TrueColor, Source: terminalcap.SourceDeclared}},
		{name: "supplied ansi256", env: AttachmentEnvironment{TermEnv: "xterm-256color", Cwd: "/workspace", Color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI256}}, want: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI256}},
		{name: "supplied forced ansi16", env: AttachmentEnvironment{TermEnv: "xterm-256color", Cwd: "/workspace", Color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceForced}}, want: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceForced}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stream := newSessionStream(t)
				terminal := newWorkerTestTerminal(t)
				attached := newSessionTestAttachments()
				supervisor := &Supervisor{cfg: SupervisorConfig{Terminal: terminal.port, AttachmentEnvironment: tt.env}, clientID: [16]byte{1}}
				request := sessionTestRequest(true)
				worker, err := supervisor.newAttachmentWorker(request, attachmentTab{}, nil, SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Cwd: tt.env.Cwd, Env: append([]string(nil), request.Env...)})
				require.NoError(t, err)
				events := make(chan AttachmentEvent, 1)
				host := newWorkerTestHost(t, terminal, nil, attached.record)
				go func() {
					event, _ := host.Run(context.Background(), AttachmentToken{Generation: 1, Attempt: 1}, worker, stream.conn)
					events <- event
				}()

				hello := awaitHello(t, stream)
				require.Equal(t, tt.env.TermEnv, hello.TermEnv)
				require.Equal(t, tt.env.Cwd, hello.Cwd)
				require.Equal(t, tt.want, hello.Color)
				require.NoError(t, stream.conn.Close())
				<-events
			})
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
			synctest.Test(t, func(t *testing.T) {
				stream := newSessionStream(t)
				terminal := newWorkerTestTerminal(t)
				attachments := newSessionTestAttachments()
				events := sessionTestRun(t, stream.conn, terminal, sessionTestWorkerConfig(sessionTestRequest(true)), attachments)
				awaitHello(t, stream)
				stream.deliver(protocol.Welcome{SessionName: "alpha"})
				if tt.attached {
					stream.deliver(sessionTestOutput(1, "initial"))
					synctest.Wait()
					require.True(t, attachments.attached())
				}
				stream.deliver(protocol.ErrorMsg{Code: 42, Text: "refused"})
				event := <-events
				var protocolErr *ProtocolError
				require.ErrorAs(t, event.Err, &protocolErr)
				require.Equal(t, uint16(42), protocolErr.Code)
				require.Equal(t, "refused", protocolErr.Text)
			})
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
			synctest.Test(t, func(t *testing.T) {
				stream := newSessionStream(t)
				terminal := newWorkerTestTerminal(t)
				attached := newSessionTestAttachments()
				events := sessionTestRun(t, stream.conn, terminal, sessionTestWorkerConfig(tt.request), attached)
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
				synctest.Wait()
				require.True(t, attached.attached())
				require.Equal(t, "ok", terminal.written())
				require.NoError(t, stream.conn.Close())
				<-events
			})
		})
	}
}

// TestSessionAttachmentWorkerRemoteHello proves the same worker derives a
// remote Hello from a remote request: the typed-preamble rules stay per
// logical stream, and no endpoint is rewritten.
func TestSessionAttachmentWorkerRemoteHello(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stream := newSessionStream(t)
		terminal := newWorkerTestTerminal(t)
		attached := newSessionTestAttachments()
		request := sessionTestRequest(false)
		events := sessionTestRun(t, stream.conn, terminal, sessionTestWorkerConfig(request), attached)

		hello := awaitHello(t, stream)
		require.True(t, hello.Remote)
		require.Equal(t, protocol.EnvironmentPolicyClientOwned, hello.EnvironmentPolicy)
		require.Equal(t, protocol.IntentAttach, hello.Intent)
		require.NotNil(t, hello.ExactTarget)
		require.Equal(t, "alpha", hello.ExactTarget.SessionName)

		require.NoError(t, stream.conn.Close())
		select {
		case event := <-events:
			require.Equal(t, AttachmentEventEnded, event.Kind)
		case <-time.After(5 * time.Second):
			t.Fatal("the worker never settled")
		}
	})
}

// TestSessionAttachmentWorkerNeverReadsTerminalInput proves the worker has no
// path to terminal input: it consumes no bytes and sends no Input frame while
// the foreground owns no input source.
func TestSessionAttachmentWorkerNeverReadsTerminalInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stream := newSessionStream(t)
		terminal := newWorkerTestTerminal(t)
		attached := newSessionTestAttachments()
		events := sessionTestRun(t, stream.conn, terminal, sessionTestWorkerConfig(sessionTestRequest(true)), attached)

		awaitHello(t, stream)
		stream.deliver(protocol.Welcome{SessionName: "alpha"})
		stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
		_ = attached.token(t)

		for _, message := range stream.messages() {
			_, isInput := message.(protocol.Input)
			require.False(t, isInput, "the worker must never send terminal input in P5.3b")
		}
		require.Equal(t, "\x1b[Hready", terminal.written(), "only session output reaches the terminal")

		require.NoError(t, stream.conn.Close())
		select {
		case <-events:
		case <-time.After(5 * time.Second):
			t.Fatal("the worker never settled")
		}
	})
}

func newBlockingSendSessionStream(t *testing.T) *blockingSendSessionStream {
	base := newSessionStream(t)
	s := &blockingSendSessionStream{streamLog: base, entered: make(chan struct{}), finished: make(chan struct{})}
	configured := configureSessionStream(t, base, func(m *portsmocks.MockBrokerLogicalConnection, _ *streamLog) {
		m.EXPECT().SendClient(mock.Anything).RunAndReturn(s.send).Maybe()
	})
	base.conn = configured.conn
	return s
}

// streamDone reports whether a logical connection has been closed.
func streamDone(stream ports.BrokerLogicalConnection) bool {
	select {
	case <-stream.Done():
		return true
	default:
		return false
	}
}
