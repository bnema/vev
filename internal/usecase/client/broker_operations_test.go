package client

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Client control operations (Plan 001 P7, worker 3). Every case is
// deterministic: a manually fired clock drives the one bound, scripted streams
// stand in for the broker carriage, and no case sleeps.

const brokerOpsTestEpoch = ports.BrokerEpoch(0x5a)

// brokerOpsTestStream is one scripted broker logical stream. It records every
// typed client message, delivers scripted server messages, and unblocks
// ReceiveServer on Close exactly as the ports.BrokerLogicalConnection contract
// requires.
type brokerOpsTestStream struct {
	mu       sync.Mutex
	sent     []protocol.ClientMessage
	sendErr  error
	recvErr  error
	incoming chan protocol.ServerMessage

	closeOnce sync.Once
	closed    chan struct{}
	closes    int
}

func newBrokerOpsTestStream() *brokerOpsTestStream {
	return &brokerOpsTestStream{
		incoming: make(chan protocol.ServerMessage, 8),
		closed:   make(chan struct{}),
	}
}

func (s *brokerOpsTestStream) failSend(err error) {
	s.mu.Lock()
	s.sendErr = err
	s.mu.Unlock()
}

func (s *brokerOpsTestStream) failReceive(err error) {
	s.mu.Lock()
	s.recvErr = err
	s.mu.Unlock()
}

func (s *brokerOpsTestStream) SendClient(message protocol.ClientMessage) error {
	s.mu.Lock()
	if s.sendErr != nil {
		err := s.sendErr
		s.mu.Unlock()
		return err
	}
	s.sent = append(s.sent, message)
	s.mu.Unlock()
	return nil
}

func (s *brokerOpsTestStream) ReceiveServer() (protocol.ServerMessage, error) {
	select {
	case message := <-s.incoming:
		return message, nil
	default:
	}
	s.mu.Lock()
	err := s.recvErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case message := <-s.incoming:
		return message, nil
	case <-s.closed:
		return nil, io.EOF
	}
}

func (s *brokerOpsTestStream) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}

func (s *brokerOpsTestStream) LinkState() ports.LinkState { return ports.LinkStateConnected }

func (s *brokerOpsTestStream) LinkEvents() <-chan ports.LinkEvent { return nil }

func (s *brokerOpsTestStream) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closes++
		s.mu.Unlock()
		close(s.closed)
	})
	return nil
}

func (s *brokerOpsTestStream) Done() <-chan struct{} { return s.closed }

func (s *brokerOpsTestStream) Err() error { return nil }

func (s *brokerOpsTestStream) deliver(message protocol.ServerMessage) {
	select {
	case s.incoming <- message:
	case <-s.closed:
	}
}

func (s *brokerOpsTestStream) messages() []protocol.ClientMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.ClientMessage(nil), s.sent...)
}

func (s *brokerOpsTestStream) sentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

func (s *brokerOpsTestStream) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

func (s *brokerOpsTestStream) awaitDispatch(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool { return s.sentCount() > 0 }, time.Second, time.Millisecond,
		"the control request was never dispatched")
}

// brokerOpsTestService is a scripted BrokerService. It records every open, the
// reconcile hints it received, and whether the borrowed service was closed.
type brokerOpsTestService struct {
	id       ports.BrokerConnectionID
	snapshot ports.BrokerSnapshot

	mu         sync.Mutex
	opens      []ports.BrokerOpenStreamRequest
	reconciles []string
	closes     int
	nextStream ports.BrokerStreamID
	open       func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error)
}

func newBrokerOpsTestService(id ports.BrokerConnectionID, snapshot ports.BrokerSnapshot) *brokerOpsTestService {
	return &brokerOpsTestService{id: id, snapshot: snapshot}
}

func (s *brokerOpsTestService) ConnectionID() ports.BrokerConnectionID { return s.id }

func (s *brokerOpsTestService) NextStreamID() (ports.BrokerStreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextStream++
	return s.nextStream, nil
}

func (s *brokerOpsTestService) Done() <-chan struct{} { return make(chan struct{}) }
func (s *brokerOpsTestService) Err() error            { return nil }

func (s *brokerOpsTestService) Snapshot() ports.BrokerSnapshot { return s.snapshot.Clone() }

func (s *brokerOpsTestService) Subscribe() (ports.BrokerSubscription, error) {
	return nil, errors.New("broker ops test service does not support subscriptions")
}

func (s *brokerOpsTestService) SubscribePreview(ports.BrokerPreviewRequest) (ports.BrokerPreviewSubscription, error) {
	return nil, errors.New("broker ops test service does not support preview")
}

func (s *brokerOpsTestService) OpenStream(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	s.mu.Lock()
	s.opens = append(s.opens, request)
	open := s.open
	s.mu.Unlock()
	if open == nil {
		return nil, errors.New("broker ops test service does not support streams")
	}
	return open(ctx, request)
}

func (s *brokerOpsTestService) openedRequests() []ports.BrokerOpenStreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.BrokerOpenStreamRequest(nil), s.opens...)
}

func (s *brokerOpsTestService) reconcileHints() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.reconciles...)
}

func (s *brokerOpsTestService) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

func (s *brokerOpsTestService) CloseStream(ports.BrokerConnectionID, ports.BrokerStreamID) error {
	return nil
}

func (s *brokerOpsTestService) AddHost(context.Context, string, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return domain.RemoteRegistration{}, nil
}

func (s *brokerOpsTestService) RemoveHost(context.Context, domain.RemoteRegistration) (bool, error) {
	return false, nil
}

func (s *brokerOpsTestService) UpdateHostPolicy(context.Context, domain.RemoteRegistration, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return domain.RemoteRegistration{}, nil
}

func (s *brokerOpsTestService) RequestReconcile(endpoint string) {
	s.mu.Lock()
	s.reconciles = append(s.reconciles, endpoint)
	s.mu.Unlock()
}

func (s *brokerOpsTestService) Close() error {
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	return nil
}

var _ ports.BrokerService = (*brokerOpsTestService)(nil)

func brokerOpsTestLocalSnapshot() ports.BrokerSnapshot {
	now := time.Unix(1000, 0)
	return ports.BrokerSnapshot{
		Epoch:    brokerOpsTestEpoch,
		Revision: 1,
		Daemons:  []ports.BrokerDaemonObservation{pickerTestLocalObservation(now)},
	}
}

func brokerOpsTestRemoteSnapshot() ports.BrokerSnapshot {
	now := time.Unix(1000, 0)
	return ports.BrokerSnapshot{
		Epoch:    brokerOpsTestEpoch,
		Revision: 1,
		Daemons: []ports.BrokerDaemonObservation{
			pickerTestRemoteObservation("user@example.test", 1, 2, now),
		},
	}
}

func brokerOpsTestLocalRoute() BrokerOperationRoute {
	return BrokerOperationRoute{Epoch: brokerOpsTestEpoch, Destination: ports.BrokerEndpointFence{Local: true}}
}

func brokerOpsTestRemoteRoute() BrokerOperationRoute {
	return BrokerOperationRoute{
		Epoch:       brokerOpsTestEpoch,
		Destination: ports.BrokerEndpointFence{Registration: pickerTestRegistration("user@example.test", 1)},
	}
}

// brokerOpsTestOperation wires one executor over a scripted service whose every
// open returns the supplied stream.
func brokerOpsTestOperation(t *testing.T, snapshot ports.BrokerSnapshot, stream *brokerOpsTestStream) (*BrokerOperations, *brokerOpsTestService, *supervisorTestClock) {
	t.Helper()
	service := newBrokerOpsTestService(ports.BrokerConnectionID{7}, snapshot)
	service.open = func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	}
	clock := newSupervisorTestClock()
	ops, err := NewBrokerOperations(service, clock)
	require.NoError(t, err)
	return ops, service, clock
}

// TestBrokerOperationsConstructorRefusesMissingDependencies proves a nil or
// typed-nil service or clock is refused rather than deferred to a panic.
func TestBrokerOperationsConstructorRefusesMissingDependencies(t *testing.T) {
	clock := newSupervisorTestClock()
	service := newBrokerOpsTestService(ports.BrokerConnectionID{1}, brokerOpsTestLocalSnapshot())

	var typedNilService *brokerOpsTestService
	var typedNilClock *supervisorTestClock

	tests := []struct {
		name    string
		service ports.BrokerService
		clock   ports.Clock
		wantErr bool
	}{
		{name: "valid", service: service, clock: clock},
		{name: "nil service", service: nil, clock: clock, wantErr: true},
		{name: "typed nil service", service: typedNilService, clock: clock, wantErr: true},
		{name: "nil clock", service: service, clock: nil, wantErr: true},
		{name: "typed nil clock", service: service, clock: typedNilClock, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ops, err := NewBrokerOperations(tt.service, tt.clock)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, ops)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ops)
		})
	}
}

// TestBrokerOperationRouteValidation pins the closed route shape: a nonzero
// epoch with a valid local or remote fence.
func TestBrokerOperationRouteValidation(t *testing.T) {
	remote := pickerTestRegistration("user@example.test", 1)
	tests := []struct {
		name    string
		route   BrokerOperationRoute
		wantErr bool
	}{
		{name: "local", route: BrokerOperationRoute{Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}}},
		{name: "remote", route: BrokerOperationRoute{Epoch: 3, Destination: ports.BrokerEndpointFence{Registration: remote}}},
		{name: "zero epoch", route: BrokerOperationRoute{Destination: ports.BrokerEndpointFence{Local: true}}, wantErr: true},
		{name: "zero destination", route: BrokerOperationRoute{Epoch: 3}, wantErr: true},
		{name: "local carrying a registration", route: BrokerOperationRoute{Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true, Registration: remote}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.route.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestBrokerOperationsPreDispatchRefusalsAreNotSent proves every refusal that
// happens before the request reaches the wire wraps ErrBrokerOperationNotSent
// and opens no stream.
func TestBrokerOperationsPreDispatchRefusalsAreNotSent(t *testing.T) {
	valid := protocol.CommandRequest{Version: protocol.Version, Slug: "next-tab"}

	tests := []struct {
		name string
		call func(*testing.T, *BrokerOperations)
	}{
		{
			name: "route with zero epoch",
			call: func(t *testing.T, ops *BrokerOperations) {
				_, err := ops.List(context.Background(), BrokerOperationRoute{Destination: ports.BrokerEndpointFence{Local: true}})
				require.ErrorIs(t, err, ErrBrokerOperationNotSent)
			},
		},
		{
			name: "destination missing from the snapshot",
			call: func(t *testing.T, ops *BrokerOperations) {
				route := BrokerOperationRoute{Epoch: brokerOpsTestEpoch, Destination: ports.BrokerEndpointFence{Registration: pickerTestRegistration("user@elsewhere.test", 3)}}
				_, err := ops.List(context.Background(), route)
				require.ErrorIs(t, err, ErrBrokerOperationNotSent)
			},
		},
		{
			name: "route epoch does not match the current snapshot",
			call: func(t *testing.T, ops *BrokerOperations) {
				_, err := ops.List(context.Background(), BrokerOperationRoute{Epoch: brokerOpsTestEpoch + 1, Destination: ports.BrokerEndpointFence{Local: true}})
				require.ErrorIs(t, err, ErrBrokerOperationNotSent)
			},
		},
		{
			name: "already cancelled caller",
			call: func(t *testing.T, ops *BrokerOperations) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := ops.Command(ctx, brokerOpsTestLocalRoute(), valid)
				require.ErrorIs(t, err, ErrBrokerOperationNotSent)
				require.ErrorIs(t, err, context.Canceled)
			},
		},
		{
			name: "command version mismatch",
			call: func(t *testing.T, ops *BrokerOperations) {
				_, err := ops.Command(context.Background(), brokerOpsTestLocalRoute(), protocol.CommandRequest{Version: protocol.Version + 1, Slug: "next-tab"})
				require.ErrorIs(t, err, ErrBrokerOperationNotSent)
			},
		},
		{
			name: "command carries a request ID",
			call: func(t *testing.T, ops *BrokerOperations) {
				request := valid
				request.RequestID = 9
				_, err := ops.Command(context.Background(), brokerOpsTestLocalRoute(), request)
				require.ErrorIs(t, err, ErrBrokerOperationNotSent)
			},
		},
		{
			name: "command is attached",
			call: func(t *testing.T, ops *BrokerOperations) {
				request := valid
				request.Attached = true
				_, err := ops.Command(context.Background(), brokerOpsTestLocalRoute(), request)
				require.ErrorIs(t, err, ErrBrokerOperationNotSent)
			},
		},
		{
			name: "command is self without an explicit target",
			call: func(t *testing.T, ops *BrokerOperations) {
				request := valid
				request.Self = true
				_, err := ops.Command(context.Background(), brokerOpsTestLocalRoute(), request)
				require.ErrorIs(t, err, ErrBrokerOperationNotSent)
			},
		},
		{
			name: "kill carries an invalid session name",
			call: func(t *testing.T, ops *BrokerOperations) {
				_, err := ops.Kill(context.Background(), brokerOpsTestLocalRoute(), "bad name")
				require.ErrorIs(t, err, ErrBrokerOperationNotSent)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newBrokerOpsTestStream()
			ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)
			tt.call(t, ops)
			require.Empty(t, service.openedRequests(), "a not-sent refusal must open no stream")
			require.Equal(t, 0, stream.sentCount(), "a not-sent refusal must send nothing")
			require.Empty(t, service.reconcileHints(), "a not-sent refusal must not request reconcile")
		})
	}
}

// TestBrokerOperationOpenFailureIsNotSent proves a refused stream open is a
// definite not-sent outcome rather than an indeterminate one.
func TestBrokerOperationOpenFailureIsNotSent(t *testing.T) {
	service := newBrokerOpsTestService(ports.BrokerConnectionID{7}, brokerOpsTestLocalSnapshot())
	service.open = func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible}
	}
	ops, err := NewBrokerOperations(service, newSupervisorTestClock())
	require.NoError(t, err)

	_, err = ops.Command(context.Background(), brokerOpsTestLocalRoute(), protocol.CommandRequest{Version: protocol.Version, Slug: "next-tab"})
	require.ErrorIs(t, err, ErrBrokerOperationNotSent)
	require.Empty(t, service.reconcileHints())
}

// TestBrokerOperationsListUsesOneControlStream proves List opens exactly one
// control stream, sends exactly one List, and returns the daemon's own
// sessions rather than a fabricated or cached value.
func TestBrokerOperationsListUsesOneControlStream(t *testing.T) {
	tests := []struct {
		name      string
		list      func(*BrokerOperations, context.Context, BrokerOperationRoute) ([]protocol.SessionInfo, error)
		startMode ports.BrokerDaemonStartMode
	}{
		{name: "list never starts the daemon", list: (*BrokerOperations).List, startMode: ports.BrokerDaemonExistingOnly},
		{name: "list starting may start the daemon", list: (*BrokerOperations).ListStarting, startMode: ports.BrokerDaemonStartIfNeeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newBrokerOpsTestStream()
			stream.deliver(protocol.Sessions{Sessions: []protocol.SessionInfo{{Name: "alpha", State: protocol.SessionUp}}})
			ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)

			sessions, err := tt.list(ops, context.Background(), brokerOpsTestLocalRoute())
			require.NoError(t, err)
			require.Equal(t, []protocol.SessionInfo{{Name: "alpha", State: protocol.SessionUp}}, sessions)

			requests := service.openedRequests()
			require.Len(t, requests, 1)
			request := requests[0]
			require.Equal(t, ports.BrokerStreamControl, request.Purpose)
			require.Equal(t, ports.BrokerStreamAdmission(0), request.Admission)
			require.Empty(t, request.Name)
			require.Equal(t, protocol.ExactSessionTarget{}, request.Target)
			require.Empty(t, request.Env)
			require.Equal(t, tt.startMode, request.StartMode)
			require.True(t, request.Local)
			require.Equal(t, service.ConnectionID(), request.Connection)
			require.NotZero(t, request.Stream)
			require.Equal(t, brokerOpsTestEpoch, request.Epoch)

			sent := stream.messages()
			require.Len(t, sent, 1)
			require.Equal(t, protocol.List{}, sent[0])
			require.Equal(t, 1, stream.closeCount(), "the owned stream is closed exactly once")
			require.Equal(t, 0, service.closeCount(), "the borrowed service is never closed")
		})
	}
}

// TestBrokerOperationsListLostReplyIsError proves a lost or malformed listing
// is an ordinary error and never an empty fabricated result.
func TestBrokerOperationsListLostReplyIsError(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*brokerOpsTestStream)
	}{
		{
			name: "wrong reply type",
			prepare: func(s *brokerOpsTestStream) {
				s.deliver(protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded})
			},
		},
		{
			name:    "EOF before a reply",
			prepare: func(s *brokerOpsTestStream) { s.failReceive(io.EOF) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newBrokerOpsTestStream()
			tt.prepare(stream)
			ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)
			sessions, err := ops.List(context.Background(), brokerOpsTestLocalRoute())
			require.Error(t, err)
			require.Nil(t, sessions)
			require.Len(t, service.openedRequests(), 1, "the listing is never retried")
			require.Empty(t, service.reconcileHints(), "a listing has no mutation to reconcile")
		})
	}
}

// TestBrokerOperationsCommandCorrelation proves a terminal, correlated,
// well-formed command reply is returned verbatim while every wrong reply
// becomes a synthetic, correlated OutcomeUnknown with a nil error.
func TestBrokerOperationsCommandCorrelation(t *testing.T) {
	tests := []struct {
		name       string
		reply      protocol.ServerMessage
		wantResult protocol.CommandResult
		wantSynth  bool
	}{
		{
			name:       "succeeded verbatim",
			reply:      protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded, Output: "ok\n"},
			wantResult: protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded, Output: "ok\n"},
		},
		{
			name:       "failed verbatim",
			reply:      protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandFailed, Code: 5, Text: "boom"},
			wantResult: protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandFailed, Code: 5, Text: "boom"},
		},
		{
			name:      "wrong request ID is synthetic",
			reply:     protocol.CommandResult{RequestID: 42, Outcome: protocol.CommandSucceeded},
			wantSynth: true,
		},
		{
			name:      "malformed success is synthetic",
			reply:     protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded, Code: 5},
			wantSynth: true,
		},
		{
			name:      "wrong reply type is synthetic",
			reply:     protocol.Sessions{},
			wantSynth: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newBrokerOpsTestStream()
			stream.deliver(tt.reply)
			ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)
			result, err := ops.Command(context.Background(), brokerOpsTestLocalRoute(), protocol.CommandRequest{Version: protocol.Version, Slug: "next-tab"})
			require.NoError(t, err)
			if tt.wantSynth {
				require.Equal(t, protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandOutcomeUnknown, Text: "command outcome unknown"}, result)
				require.Len(t, service.reconcileHints(), 1, "an indeterminate command requests exactly one reconcile")
				require.Len(t, service.openedRequests(), 1, "an indeterminate command is never replayed")
				return
			}
			require.Equal(t, tt.wantResult, result)
			require.Empty(t, service.reconcileHints())
		})
	}
}

// TestBrokerOperationsCommandDaemonUnknownIsVerbatim proves a daemon-reported
// Unknown is returned as a terminal result (with one reconcile hint) and never
// hidden behind a plain context error.
func TestBrokerOperationsCommandDaemonUnknownIsVerbatim(t *testing.T) {
	stream := newBrokerOpsTestStream()
	stream.deliver(protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandOutcomeUnknown, Text: "daemon could not finish"})
	ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)

	result, err := ops.Command(context.Background(), brokerOpsTestLocalRoute(), protocol.CommandRequest{Version: protocol.Version, Slug: "next-tab"})
	require.NoError(t, err)
	require.Equal(t, protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandOutcomeUnknown, Text: "daemon could not finish"}, result)
	require.Equal(t, []string{""}, service.reconcileHints())
}

// TestBrokerOperationsKillCorrelation proves the Kill-family correlation table:
// terminal replies are verbatim, wrong or malformed replies become a synthetic
// OutcomeUnknown, and partial kill-all failures are preserved.
func TestBrokerOperationsKillCorrelation(t *testing.T) {
	tests := []struct {
		name       string
		call       func(context.Context, *BrokerOperations, BrokerOperationRoute) (protocol.KillResult, error)
		reply      protocol.ServerMessage
		wantResult protocol.KillResult
		wantSynth  bool
	}{
		{
			name: "kill succeeded",
			call: func(ctx context.Context, ops *BrokerOperations, route BrokerOperationRoute) (protocol.KillResult, error) {
				return ops.Kill(ctx, route, "work")
			},
			reply:      protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded},
			wantResult: protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded},
		},
		{
			name: "kill failed with a definite code",
			call: func(ctx context.Context, ops *BrokerOperations, route BrokerOperationRoute) (protocol.KillResult, error) {
				return ops.Kill(ctx, route, "ghost")
			},
			reply:      protocol.KillResult{RequestID: 1, Outcome: protocol.KillFailed, Code: 4, Text: "no such session: ghost"},
			wantResult: protocol.KillResult{RequestID: 1, Outcome: protocol.KillFailed, Code: 4, Text: "no such session: ghost"},
		},
		{
			name: "kill all partial failures verbatim",
			call: func(ctx context.Context, ops *BrokerOperations, route BrokerOperationRoute) (protocol.KillResult, error) {
				return ops.KillAll(ctx, route)
			},
			reply:      protocol.KillResult{RequestID: 1, Outcome: protocol.KillFailed, Code: 8, Text: "partial", Failures: []protocol.KillFailure{{Class: "stopped", Name: "work", Text: "boom"}}},
			wantResult: protocol.KillResult{RequestID: 1, Outcome: protocol.KillFailed, Code: 8, Text: "partial", Failures: []protocol.KillFailure{{Class: "stopped", Name: "work", Text: "boom"}}},
		},
		{
			name: "wrong request ID is synthetic",
			call: func(ctx context.Context, ops *BrokerOperations, route BrokerOperationRoute) (protocol.KillResult, error) {
				return ops.Kill(ctx, route, "work")
			},
			reply:     protocol.KillResult{RequestID: 5, Outcome: protocol.KillSucceeded},
			wantSynth: true,
		},
		{
			name: "malformed failure is synthetic",
			call: func(ctx context.Context, ops *BrokerOperations, route BrokerOperationRoute) (protocol.KillResult, error) {
				return ops.Kill(ctx, route, "work")
			},
			reply:     protocol.KillResult{RequestID: 1, Outcome: protocol.KillFailed},
			wantSynth: true,
		},
		{
			name: "wrong reply type is synthetic",
			call: func(ctx context.Context, ops *BrokerOperations, route BrokerOperationRoute) (protocol.KillResult, error) {
				return ops.KillAll(ctx, route)
			},
			reply:     protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded},
			wantSynth: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newBrokerOpsTestStream()
			stream.deliver(tt.reply)
			ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)
			result, err := tt.call(context.Background(), ops, brokerOpsTestLocalRoute())
			require.NoError(t, err)
			if tt.wantSynth {
				require.Equal(t, protocol.KillResult{RequestID: 1, Outcome: protocol.KillOutcomeUnknown, Text: "kill outcome unknown"}, result)
				require.Len(t, service.reconcileHints(), 1)
				require.Len(t, service.openedRequests(), 1, "an indeterminate kill is never replayed")
				return
			}
			require.Equal(t, tt.wantResult, result)
			require.Empty(t, service.reconcileHints())
		})
	}
}

// TestBrokerOperationsKillShapes proves Kill, KillAll, and StopDaemon carry the
// exact v58 scope, name, and start-mode each semantic requires.
func TestBrokerOperationsKillShapes(t *testing.T) {
	tests := []struct {
		name          string
		call          func(context.Context, *BrokerOperations, BrokerOperationRoute) (protocol.KillResult, error)
		wantScope     protocol.KillScope
		wantName      string
		wantStartMode ports.BrokerDaemonStartMode
	}{
		{
			name: "kill session",
			call: func(ctx context.Context, ops *BrokerOperations, route BrokerOperationRoute) (protocol.KillResult, error) {
				return ops.Kill(ctx, route, "work")
			},
			wantScope:     protocol.KillSession,
			wantName:      "work",
			wantStartMode: ports.BrokerDaemonStartIfNeeded,
		},
		{
			name: "kill all",
			call: func(ctx context.Context, ops *BrokerOperations, route BrokerOperationRoute) (protocol.KillResult, error) {
				return ops.KillAll(ctx, route)
			},
			wantScope:     protocol.KillAll,
			wantStartMode: ports.BrokerDaemonStartIfNeeded,
		},
		{
			name: "stop daemon is existing only",
			call: func(ctx context.Context, ops *BrokerOperations, route BrokerOperationRoute) (protocol.KillResult, error) {
				return ops.StopDaemon(ctx, route)
			},
			wantScope:     protocol.KillDaemon,
			wantStartMode: ports.BrokerDaemonExistingOnly,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newBrokerOpsTestStream()
			stream.deliver(protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded})
			ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)

			result, err := tt.call(context.Background(), ops, brokerOpsTestLocalRoute())
			require.NoError(t, err)
			require.Equal(t, protocol.KillSucceeded, result.Outcome)

			requests := service.openedRequests()
			require.Len(t, requests, 1)
			require.Equal(t, ports.BrokerStreamControl, requests[0].Purpose)
			require.Equal(t, tt.wantStartMode, requests[0].StartMode)

			sent := stream.messages()
			require.Len(t, sent, 1)
			kill, ok := sent[0].(protocol.Kill)
			require.True(t, ok)
			require.Equal(t, tt.wantScope, kill.Scope)
			require.Equal(t, tt.wantName, kill.Name)
			require.Equal(t, uint64(1), kill.RequestID)
		})
	}
}

// TestBrokerOperationsStopDaemonLostAcknowledgementIsUnknown proves the
// ExistingOnly stop reports OutcomeUnknown rather than a success when its
// acknowledgement never arrives: a close or EOF is never a success.
func TestBrokerOperationsStopDaemonLostAcknowledgementIsUnknown(t *testing.T) {
	stream := newBrokerOpsTestStream()
	stream.failReceive(io.EOF)
	ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)

	result, err := ops.StopDaemon(context.Background(), brokerOpsTestLocalRoute())
	require.NoError(t, err)
	require.Equal(t, protocol.KillOutcomeUnknown, result.Outcome)
	require.Equal(t, uint64(1), result.RequestID)
	require.Equal(t, ports.BrokerDaemonExistingOnly, service.openedRequests()[0].StartMode)
	require.Len(t, service.reconcileHints(), 1)
}

// TestBrokerOperationsSendErrorIsOutcomeUnknown proves a send error may be a
// partial delivery, so it becomes a synthetic OutcomeUnknown for a mutation.
func TestBrokerOperationsSendErrorIsOutcomeUnknown(t *testing.T) {
	stream := newBrokerOpsTestStream()
	stream.failSend(errors.New("write: broken pipe"))
	ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)

	result, err := ops.Kill(context.Background(), brokerOpsTestLocalRoute(), "work")
	require.NoError(t, err)
	require.Equal(t, protocol.KillOutcomeUnknown, result.Outcome)
	require.Equal(t, uint64(1), result.RequestID)
	require.Len(t, service.reconcileHints(), 1)
	require.Len(t, service.openedRequests(), 1, "a dispatched kill is never replayed")
}

// TestBrokerOperationsDispatchBoundsAreOutcomeUnknown proves a lapsed bound and
// a cancellation after dispatch both become a synthetic OutcomeUnknown rather
// than a not-sent error or a success.
func TestBrokerOperationsDispatchBoundsAreOutcomeUnknown(t *testing.T) {
	tests := []struct {
		name    string
		trigger func(t *testing.T, stream *brokerOpsTestStream, clock *supervisorTestClock, cancel context.CancelFunc)
	}{
		{
			name: "deadline after dispatch",
			trigger: func(t *testing.T, stream *brokerOpsTestStream, clock *supervisorTestClock, _ context.CancelFunc) {
				timer := clock.awaitTimer(t)
				stream.awaitDispatch(t)
				timer.fire()
			},
		},
		{
			name: "cancellation after dispatch",
			trigger: func(t *testing.T, stream *brokerOpsTestStream, _ *supervisorTestClock, cancel context.CancelFunc) {
				stream.awaitDispatch(t)
				cancel()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newBrokerOpsTestStream()
			ops, service, clock := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			type outcome struct {
				result protocol.KillResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				result, err := ops.Kill(ctx, brokerOpsTestLocalRoute(), "work")
				done <- outcome{result: result, err: err}
			}()
			tt.trigger(t, stream, clock, cancel)

			select {
			case got := <-done:
				require.NoError(t, got.err)
				require.Equal(t, protocol.KillOutcomeUnknown, got.result.Outcome)
				require.Equal(t, uint64(1), got.result.RequestID)
			case <-time.After(5 * time.Second):
				t.Fatal("control operation did not settle")
			}
			require.Len(t, service.reconcileHints(), 1)
			require.Len(t, service.openedRequests(), 1, "the operation is never replayed")
			require.GreaterOrEqual(t, stream.closeCount(), 1, "the owned stream is closed to unblock the read")
		})
	}
}

// TestBrokerOperationsLateReplyCannotChangeVerdict proves a reply arriving after
// the bound fired is dropped and never replaces the indeterminate verdict, and
// that the timer was disarmed before return.
func TestBrokerOperationsLateReplyCannotChangeVerdict(t *testing.T) {
	stream := newBrokerOpsTestStream()
	ops, _, clock := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)

	type outcome struct {
		result protocol.KillResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := ops.Kill(context.Background(), brokerOpsTestLocalRoute(), "work")
		done <- outcome{result: result, err: err}
	}()
	timer := clock.awaitTimer(t)
	stream.awaitDispatch(t)
	timer.fire()

	var got outcome
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("control operation did not settle")
	}
	require.NoError(t, got.err)
	require.Equal(t, protocol.KillOutcomeUnknown, got.result.Outcome)
	require.True(t, timer.stopped(), "the bound timer is disarmed before return")

	// A late success can never replace the settled verdict.
	stream.deliver(protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded})
	require.Equal(t, protocol.KillOutcomeUnknown, got.result.Outcome)
}

// TestBrokerOperationsRemoteRouteUsesExactRegistration proves a remote
// destination resolves policy from the exact snapshot observation and requests
// reconcile against its endpoint.
func TestBrokerOperationsRemoteRouteUsesExactRegistration(t *testing.T) {
	stream := newBrokerOpsTestStream()
	stream.deliver(protocol.KillResult{RequestID: 1, Outcome: protocol.KillOutcomeUnknown, Text: "daemon shutting down"})
	ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestRemoteSnapshot(), stream)

	result, err := ops.StopDaemon(context.Background(), brokerOpsTestRemoteRoute())
	require.NoError(t, err)
	require.Equal(t, protocol.KillOutcomeUnknown, result.Outcome)

	requests := service.openedRequests()
	require.Len(t, requests, 1)
	require.False(t, requests[0].Local)
	require.Equal(t, "user@example.test", requests[0].Endpoint)
	require.Equal(t, pickerTestRegistration("user@example.test", 1), requests[0].Registration)
	require.Equal(t, pickerTestPolicy(), requests[0].Policy)

	// A daemon-reported Unknown requests a reconcile of the exact endpoint.
	require.Equal(t, []string{"user@example.test"}, service.reconcileHints())
}

// TestBrokerOperationsReplacedRegistrationIsNotSent proves a remote destination
// whose registration was replaced is refused before dispatch rather than
// resolved by endpoint alone.
func TestBrokerOperationsReplacedRegistrationIsNotSent(t *testing.T) {
	stream := newBrokerOpsTestStream()
	ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestRemoteSnapshot(), stream)

	replaced := BrokerOperationRoute{
		Epoch:       brokerOpsTestEpoch,
		Destination: ports.BrokerEndpointFence{Registration: pickerTestRegistration("user@example.test", 9)},
	}
	_, err := ops.List(context.Background(), replaced)
	require.ErrorIs(t, err, ErrBrokerOperationNotSent)
	require.Empty(t, service.openedRequests())
}

// TestBrokerOperationsCommandCopiesArgsAtAdmission proves a dispatched command
// cannot be mutated by a caller after admission and carries the normalized
// shape.
func TestBrokerOperationsCommandCopiesArgsAtAdmission(t *testing.T) {
	stream := newBrokerOpsTestStream()
	stream.deliver(protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded})
	ops, _, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)

	args := []string{"original"}
	_, err := ops.Command(context.Background(), brokerOpsTestLocalRoute(), protocol.CommandRequest{Version: protocol.Version, Slug: "next-tab", Args: args})
	require.NoError(t, err)
	args[0] = "mutated"

	sent := stream.messages()
	require.Len(t, sent, 1)
	command, ok := sent[0].(protocol.CommandRequest)
	require.True(t, ok)
	require.Equal(t, []string{"original"}, command.Args)
	require.False(t, command.Attached)
	require.False(t, command.Self)
	require.Equal(t, uint64(1), command.RequestID)
}

// TestBrokerOperationsConcurrentStreamsDoNotCollide proves concurrent
// operations each allocate a distinct stream identity, open exactly one stream,
// and receive their own correlated reply.
func TestBrokerOperationsConcurrentStreamsDoNotCollide(t *testing.T) {
	const operations = 16

	service := newBrokerOpsTestService(ports.BrokerConnectionID{7}, brokerOpsTestLocalSnapshot())
	openGate := make(chan struct{})
	service.open = func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		<-openGate
		stream := newBrokerOpsTestStream()
		stream.deliver(protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded})
		return stream, nil
	}
	ops, err := NewBrokerOperations(service, newSupervisorTestClock())
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, operations)
	for range operations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := ops.Kill(context.Background(), brokerOpsTestLocalRoute(), "work")
			if err != nil {
				errs <- err
				return
			}
			if result.Outcome != protocol.KillSucceeded || result.RequestID != 1 {
				errs <- errors.New("uncorrelated concurrent kill result")
			}
		}()
	}
	close(openGate)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	requests := service.openedRequests()
	require.Len(t, requests, operations)
	seen := make(map[ports.BrokerStreamID]struct{}, operations)
	for _, request := range requests {
		require.NotZero(t, request.Stream)
		_, dup := seen[request.Stream]
		require.False(t, dup, "two concurrent operations named the same stream")
		seen[request.Stream] = struct{}{}
	}
}

// TestBrokerOperationsClosingOneStreamPreservesSibling proves cancelling one
// operation closes only its own stream and never disturbs a concurrent
// sibling's reply.
func TestBrokerOperationsClosingOneStreamPreservesSibling(t *testing.T) {
	cancelled := newBrokerOpsTestStream()
	sibling := newBrokerOpsTestStream()
	sibling.deliver(protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded})

	service := newBrokerOpsTestService(ports.BrokerConnectionID{7}, brokerOpsTestLocalSnapshot())
	service.open = func(_ context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		if request.Stream == 1 {
			return cancelled, nil
		}
		return sibling, nil
	}
	ops, err := NewBrokerOperations(service, newSupervisorTestClock())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelledDone := make(chan protocol.KillResult, 1)
	go func() {
		result, _ := ops.Kill(ctx, brokerOpsTestLocalRoute(), "work")
		cancelledDone <- result
	}()
	cancelled.awaitDispatch(t)

	siblingResult, err := ops.Kill(context.Background(), brokerOpsTestLocalRoute(), "other")
	require.NoError(t, err)
	require.Equal(t, protocol.KillSucceeded, siblingResult.Outcome)

	cancel()
	select {
	case result := <-cancelledDone:
		require.Equal(t, protocol.KillOutcomeUnknown, result.Outcome)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled operation did not settle")
	}
	require.GreaterOrEqual(t, cancelled.closeCount(), 1)
	require.Equal(t, 1, sibling.closeCount(), "a sibling stream is closed only by its own call")
}

// TestBrokerOperationsBorrowedServiceStaysOpen proves the executor never closes
// the service it borrows, across success and refusal.
func TestBrokerOperationsBorrowedServiceStaysOpen(t *testing.T) {
	stream := newBrokerOpsTestStream()
	stream.deliver(protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded})
	ops, service, _ := brokerOpsTestOperation(t, brokerOpsTestLocalSnapshot(), stream)

	_, err := ops.Kill(context.Background(), brokerOpsTestLocalRoute(), "work")
	require.NoError(t, err)
	_, err = ops.Kill(context.Background(), brokerOpsTestLocalRoute(), "bad name")
	require.Error(t, err)

	require.Equal(t, 0, service.closeCount(), "the executor never closes the borrowed service")
}

// TestBrokerOperationsConcurrentSafeAccess exercises mixed operations
// concurrently so the race detector proves the executor holds no shared mutable
// state.
func TestBrokerOperationsConcurrentSafeAccess(t *testing.T) {
	service := newBrokerOpsTestService(ports.BrokerConnectionID{7}, brokerOpsTestLocalSnapshot())
	service.open = func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		stream := newBrokerOpsTestStream()
		stream.deliver(protocol.Sessions{})
		stream.deliver(protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded})
		stream.deliver(protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded})
		return stream, nil
	}
	ops, err := NewBrokerOperations(service, newSupervisorTestClock())
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, _ = ops.List(context.Background(), brokerOpsTestLocalRoute())
		}()
		go func() {
			defer wg.Done()
			_, _ = ops.Kill(context.Background(), brokerOpsTestLocalRoute(), "work")
		}()
		go func() {
			defer wg.Done()
			_, _ = ops.Command(context.Background(), brokerOpsTestLocalRoute(), protocol.CommandRequest{Version: protocol.Version, Slug: "next-tab"})
		}()
	}
	wg.Wait()
}

// TestBrokerOperationsAcquisitionBoundIsNotSent proves the single 10s bound
// also covers stream acquisition: an open that never completes because the
// bounded context lapses is refused as a definite not-sent outcome and
// dispatches nothing.
func TestBrokerOperationsAcquisitionBoundIsNotSent(t *testing.T) {
	require.Equal(t, 10*time.Second, brokerOperationTimeout)

	service := newBrokerOpsTestService(ports.BrokerConnectionID{7}, brokerOpsTestLocalSnapshot())
	// A real adapter honors the bounded context; this one blocks until the bound
	// lapses, exactly as a socket that accepts but never admits would.
	service.open = func(ctx context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	clock := newSupervisorTestClock()
	ops, err := NewBrokerOperations(service, clock)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := ops.Kill(context.Background(), brokerOpsTestLocalRoute(), "work")
		done <- err
	}()
	timer := clock.awaitTimer(t)
	require.Eventually(t, func() bool { return len(service.openedRequests()) == 1 }, time.Second, time.Millisecond)
	timer.fire()

	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrBrokerOperationNotSent)
	case <-time.After(5 * time.Second):
		t.Fatal("control operation did not settle")
	}
	require.Empty(t, service.reconcileHints(), "a not-sent outcome never requests reconcile")
}

// TestBrokerOperationAuthorityExactSelection pins the helper directly,
// including that a local destination never resolves a remote observation and a
// remote destination never resolves the local one.
func TestBrokerOperationAuthorityExactSelection(t *testing.T) {
	local := brokerOpsTestLocalSnapshot()
	remote := brokerOpsTestRemoteSnapshot()

	if _, ok := brokerOperationAuthority(local, ports.BrokerEndpointFence{Local: true}); !ok {
		t.Fatal("a local destination must resolve the local observation")
	}
	if _, ok := brokerOperationAuthority(remote, ports.BrokerEndpointFence{Local: true}); ok {
		t.Fatal("a local destination must not resolve against a remote-only snapshot")
	}
	if _, ok := brokerOperationAuthority(local, ports.BrokerEndpointFence{Registration: pickerTestRegistration("user@example.test", 1)}); ok {
		t.Fatal("a remote destination must not resolve against a local-only snapshot")
	}
	if _, ok := brokerOperationAuthority(remote, ports.BrokerEndpointFence{Registration: pickerTestRegistration("user@example.test", 1)}); !ok {
		t.Fatal("a remote destination must resolve its exact registration")
	}
}
