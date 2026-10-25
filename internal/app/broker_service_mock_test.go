package app

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

// brokerServiceState is the recorded and shared state behind one
// newBrokerServiceMock. It implements no interface: the mock's RunAndReturn
// closures capture it, and tests read it through its accessors.
type brokerServiceState struct {
	mu           sync.Mutex
	connectionID ports.BrokerConnectionID
	// subErr, when set, is what Subscribe returns instead of a subscription.
	subErr error
	// doneOnClose closes Done when the service is closed; otherwise only
	// finish does.
	doneOnClose bool
	// openStream builds the logical stream one admitted OpenStream returns.
	// Nil refuses every open as a closed admission.
	openStream func(ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error)

	done      chan struct{}
	doneOnce  sync.Once
	closed    bool
	closes    int
	subCloses int
	next      ports.BrokerStreamID
	opened    []ports.BrokerOpenStreamRequest
	mutations []string
	streams   []*brokerStreamState
}

// finish closes Done, so a caller observes the connection going terminal.
func (s *brokerServiceState) finish() { s.doneOnce.Do(func() { close(s.done) }) }

func (s *brokerServiceState) record(name string) {
	s.mu.Lock()
	s.mutations = append(s.mutations, name)
	s.mu.Unlock()
}

func (s *brokerServiceState) addStream(stream *brokerStreamState) {
	s.mu.Lock()
	s.streams = append(s.streams, stream)
	s.mu.Unlock()
}

func (s *brokerServiceState) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

// subscriptionCloses returns the total Close count across the subscriptions.
func (s *brokerServiceState) subscriptionCloses() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subCloses
}

// mutationNames returns the effectful methods the caller asked for, if any.
func (s *brokerServiceState) mutationNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.mutations...)
}

// openedRequests returns the attachment streams opened; the preflight's
// control listings are reported by openedControls.
func (s *brokerServiceState) openedRequests() []ports.BrokerOpenStreamRequest {
	return s.openedWithPurpose(ports.BrokerStreamAttachment)
}

func (s *brokerServiceState) openedControls() []ports.BrokerOpenStreamRequest {
	return s.openedWithPurpose(ports.BrokerStreamControl)
}

func (s *brokerServiceState) allOpened() []ports.BrokerOpenStreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.BrokerOpenStreamRequest(nil), s.opened...)
}

func (s *brokerServiceState) openedWithPurpose(purpose ports.BrokerStreamPurpose) []ports.BrokerOpenStreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ports.BrokerOpenStreamRequest
	for _, request := range s.opened {
		if request.Purpose == purpose {
			out = append(out, request)
		}
	}
	return out
}

// streamHello returns the Hello the supervisor sent on the single admitted
// attachment stream, proving the composition carried the CLI intent through to
// the typed session handshake.
func (s *brokerServiceState) streamHello(t *testing.T) protocol.Hello {
	t.Helper()
	s.mu.Lock()
	var streams []*brokerStreamState
	for _, stream := range s.streams {
		if stream.request.Purpose == ports.BrokerStreamAttachment {
			streams = append(streams, stream)
		}
	}
	s.mu.Unlock()
	require.Len(t, streams, 1)
	for _, message := range streams[0].sentMessages() {
		if hello, ok := message.(protocol.Hello); ok {
			return hello
		}
	}
	t.Fatal("the composed client never sent a session Hello")
	return protocol.Hello{}
}

// newBrokerServiceMock is the one scripted ports.BrokerService the app tests
// share. It serves one fixed snapshot, records every stream request, mutation
// and close, and lets a test end its connection through the state. Options
// register their overrides first, then the defaults are registered with
// Maybe, and testify matches the first registered expectation.
func newBrokerServiceMock(t *testing.T, snapshot ports.BrokerSnapshot, opts ...func(*portsmocks.MockBrokerService, *brokerServiceState)) (*portsmocks.MockBrokerService, *brokerServiceState) {
	t.Helper()
	service := portsmocks.NewMockBrokerService(t)
	state := &brokerServiceState{connectionID: ports.BrokerConnectionID{4}, done: make(chan struct{})}
	for _, opt := range opts {
		opt(service, state)
	}
	service.EXPECT().ConnectionID().RunAndReturn(func() ports.BrokerConnectionID { return state.connectionID }).Maybe()
	service.EXPECT().NextStreamID().RunAndReturn(func() (ports.BrokerStreamID, error) {
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.closed {
			return 0, ports.BrokerAdmissionClosed
		}
		state.next++
		return state.next, nil
	}).Maybe()
	service.EXPECT().Done().RunAndReturn(func() <-chan struct{} { return state.done }).Maybe()
	service.EXPECT().Err().Return(nil).Maybe()
	service.EXPECT().Snapshot().RunAndReturn(snapshot.Clone).Maybe()
	service.EXPECT().Subscribe().RunAndReturn(func() (ports.BrokerSubscription, error) {
		if state.subErr != nil {
			return nil, state.subErr
		}
		subscription := portsmocks.NewMockBrokerSubscription(t)
		subscription.EXPECT().Changed().Return((<-chan struct{})(make(chan struct{}, 1))).Maybe()
		subscription.EXPECT().Close().Run(func() {
			state.mu.Lock()
			state.subCloses++
			state.mu.Unlock()
		}).Maybe()
		return subscription, nil
	}).Maybe()
	service.EXPECT().SubscribePreview(mock.Anything).RunAndReturn(func(ports.BrokerPreviewRequest) (ports.BrokerPreviewSubscription, error) {
		state.record("SubscribePreview")
		return nil, errors.New("the scripted broker service does not support preview")
	}).Maybe()
	service.EXPECT().OpenStream(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		state.mu.Lock()
		state.mutations = append(state.mutations, "OpenStream")
		if state.closed {
			state.mu.Unlock()
			return nil, ports.BrokerAdmissionClosed
		}
		state.opened = append(state.opened, request)
		open := state.openStream
		state.mu.Unlock()
		if open == nil {
			return nil, ports.BrokerAdmissionClosed
		}
		return open(request)
	}).Maybe()
	service.EXPECT().CloseStream(mock.Anything, mock.Anything).RunAndReturn(func(ports.BrokerConnectionID, ports.BrokerStreamID) error {
		state.record("CloseStream")
		return nil
	}).Maybe()
	service.EXPECT().AddHost(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(context.Context, string, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
		state.record("AddHost")
		return domain.RemoteRegistration{}, nil
	}).Maybe()
	service.EXPECT().RemoveHost(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, domain.RemoteRegistration) (bool, error) {
		state.record("RemoveHost")
		return false, nil
	}).Maybe()
	service.EXPECT().UpdateHostPolicy(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(context.Context, domain.RemoteRegistration, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
		state.record("UpdateHostPolicy")
		return domain.RemoteRegistration{}, nil
	}).Maybe()
	service.EXPECT().RequestReconcile(mock.Anything).Run(func(string) { state.record("RequestReconcile") }).Maybe()
	service.EXPECT().Close().RunAndReturn(func() error {
		state.mu.Lock()
		state.closes++
		first := !state.closed
		state.closed = true
		state.mu.Unlock()
		if first && state.doneOnClose {
			state.finish()
		}
		return nil
	}).Maybe()
	return service, state
}

// brokerStreamState is the recorded state behind one newBrokerStreamMock.
type brokerStreamState struct {
	request ports.BrokerOpenStreamRequest
	// blockWhenEmpty makes ReceiveServer wait for Close once the queue is
	// drained, instead of reporting EOF at once.
	blockWhenEmpty bool

	mu     sync.Mutex
	sent   []protocol.ClientMessage
	queue  []protocol.ServerMessage
	closed bool
	done   chan struct{}
}

func (s *brokerStreamState) sentMessages() []protocol.ClientMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.ClientMessage(nil), s.sent...)
}

// sentCount reports how many typed messages the stream received.
func (s *brokerStreamState) sentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

// newBrokerStreamMock is one scripted broker logical stream. It records every
// typed client message and serves a fixed reply queue: an exhausted queue is an
// EOF (after Close when blockWhenEmpty), which is exactly the lost-reply
// condition a caller must classify as unknown rather than success.
func newBrokerStreamMock(t *testing.T, replies []protocol.ServerMessage, opts ...func(*portsmocks.MockBrokerLogicalConnection, *brokerStreamState)) (*portsmocks.MockBrokerLogicalConnection, *brokerStreamState) {
	t.Helper()
	stream := portsmocks.NewMockBrokerLogicalConnection(t)
	state := &brokerStreamState{queue: append([]protocol.ServerMessage(nil), replies...), done: make(chan struct{})}
	for _, opt := range opts {
		opt(stream, state)
	}
	stream.EXPECT().SendClient(mock.Anything).RunAndReturn(func(message protocol.ClientMessage) error {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.sent = append(state.sent, message)
		return nil
	}).Maybe()
	stream.EXPECT().ReceiveServer().RunAndReturn(func() (protocol.ServerMessage, error) {
		state.mu.Lock()
		if len(state.queue) != 0 {
			message := state.queue[0]
			state.queue = state.queue[1:]
			state.mu.Unlock()
			return message, nil
		}
		block := state.blockWhenEmpty
		state.mu.Unlock()
		if block {
			<-state.done
		}
		return nil, io.EOF
	}).Maybe()
	stream.EXPECT().Capabilities().Return(protocol.ConnectionCapabilities{}).Maybe()
	stream.EXPECT().LinkState().Return(ports.LinkStateConnected).Maybe()
	stream.EXPECT().LinkEvents().Return(nil).Maybe()
	stream.EXPECT().Done().RunAndReturn(func() <-chan struct{} { return state.done }).Maybe()
	stream.EXPECT().Err().Return(nil).Maybe()
	stream.EXPECT().Close().RunAndReturn(func() error {
		state.mu.Lock()
		defer state.mu.Unlock()
		if !state.closed {
			state.closed = true
			close(state.done)
		}
		return nil
	}).Maybe()
	return stream, state
}
