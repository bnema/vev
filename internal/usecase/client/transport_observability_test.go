package client_test

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
)

func TestRuntimeObserverDependencyUsesSerializedContract(t *testing.T) {
	deps := reflect.TypeFor[client.Dependencies]()
	field, ok := deps.FieldByName("RuntimeObserver")
	if !ok || field.Type != reflect.TypeFor[ports.SerializedRuntimeObserver]() {
		t.Fatalf("Dependencies.RuntimeObserver is %v, want %v; raw blocking sinks must not enter the hot path", field.Type, reflect.TypeFor[ports.SerializedRuntimeObserver]())
	}
}

func TestBlockingRuntimeObserverDoesNotDelayTerminalFlushOrACK(t *testing.T) {
	var out bytes.Buffer
	var restores atomic.Int32
	resizeCh := make(chan domain.Geometry)
	flushed := make(chan struct{})
	term := portsmocks.NewMockTerminal(t)
	term.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
	term.EXPECT().EnterRaw().Return(func() error { restores.Add(1); return nil }, nil).Once()
	in := newBlockingReader()
	defer in.unblock()
	term.EXPECT().In().Return(in).Maybe()
	term.EXPECT().Out().Return(&out).Maybe()
	term.EXPECT().Flush().Run(func() { closeOnce(flushed) }).Return(nil).Maybe()
	term.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	observer := &blockingClientRuntimeObserver{entered: make(chan struct{}), release: make(chan struct{})}
	reporter := ports.NewSerializedRuntimeObserver(observer, 1)
	defer reporter.Close()
	acked := make(chan struct{})
	transport := portsmocks.NewMockTransport(t)
	transport.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	transport.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	transport.EXPECT().Send(isType(wire.MsgAck)).Run(func(wire.Frame) { close(acked) }).Return(nil).Once()
	unblock := scriptRecv(transport,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(wire.MsgOutput, mustMarshalOutput(protocol.Output{Epoch: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("flush-before-observe"), New: 3}))},
	)
	defer unblock()
	transport.EXPECT().Close().Return(nil).Once()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- runTestClient(ctx, testDependencies(transportDialer{transport: transport}, term, realClock{}, nil, reporter), client.AttachRequest{Intent: protocol.IntentEphemeral})
	}()
	awaitClientRuntime(t, observer.entered, "blocking observer")
	awaitClientRuntime(t, flushed, "terminal flush")
	awaitClientRuntime(t, acked, "ACK")
	cancel()
	close(observer.release)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Attach did not complete after observer release")
	}
	require.Equal(t, "\x1b]10;?\x07\x1b]11;?\x07\x1b]4;0;?;1;?;2;?;3;?;4;?;5;?;6;?;7;?;8;?;9;?;10;?;11;?;12;?;13;?;14;?;15;?\x07\x1b[?2031$pflush-before-observe", out.String())
	require.Equal(t, int32(1), restores.Load())
}

func TestTerminalFlushBoundaryTransportObservability(t *testing.T) {
	var out bytes.Buffer
	var restores atomic.Int32
	resizeCh := make(chan domain.Geometry)
	term, in := newHappyTerminal(t, &out, &restores, resizeCh)
	defer in.unblock()

	observer := &clientRuntimeObserver{}
	reporter := ports.NewSerializedRuntimeObserver(observer, 64)
	defer reporter.Close()
	transport := portsmocks.NewMockTransport(t)
	transport.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	transport.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	unblock := scriptRecv(transport,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(wire.MsgOutput, mustMarshalOutput(protocol.Output{Epoch: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("unchanged-by-observer"), New: 3}))},
		recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	)
	defer unblock()
	transport.EXPECT().Send(isType(wire.MsgAck)).Return(nil).Maybe()
	transport.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), testDependencies(transportDialer{transport: transport}, term, realClock{}, nil, reporter), client.AttachRequest{Intent: protocol.IntentEphemeral})
	require.NoError(t, err)
	reporter.Flush()
	require.Equal(t, "\x1b]10;?\x07\x1b]11;?\x07\x1b]4;0;?;1;?;2;?;3;?;4;?;5;?;6;?;7;?;8;?;9;?;10;?;11;?;12;?;13;?;14;?;15;?\x07\x1b[?2031$punchanged-by-observer", out.String(), "observer must not alter terminal bytes")
	require.Equal(t, int32(1), restores.Load())
	// Carriage spans belong only to concrete adapters. The client owns the
	// post-successful-flush terminal boundary, before its ACK is queued.
	observer.requireOrdered(t, ports.RuntimeTerminalFlushed)
}

type blockingClientRuntimeObserver struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *blockingClientRuntimeObserver) ObserveRuntime(ports.RuntimeMark) {
	o.once.Do(func() {
		close(o.entered)
		<-o.release
	})
}

func awaitClientRuntime(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

type clientRuntimeObserver struct {
	mu    sync.Mutex
	marks []ports.RuntimeMark
}

func (o *clientRuntimeObserver) ObserveRuntime(mark ports.RuntimeMark) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.marks = append(o.marks, mark)
}

func (o *clientRuntimeObserver) requireOrdered(t *testing.T, kinds ...ports.RuntimeMarkKind) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	next := 0
	for _, mark := range o.marks {
		if next < len(kinds) && mark.Kind == kinds[next] {
			next++
		}
	}
	if next != len(kinds) {
		t.Fatalf("runtime marks %#v do not contain ordered %#v", o.marks, kinds)
	}
}
