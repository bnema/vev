package client_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/client"
)

func TestTerminalFlushBoundaryTransportObservability(t *testing.T) {
	var out bytes.Buffer
	var restores atomic.Int32
	resizeCh := make(chan domain.Size)
	term, in := newHappyTerminal(t, &out, &restores, resizeCh)
	defer in.unblock()

	observer := &clientRuntimeObserver{}
	transport := portsmocks.NewMockTransport(t)
	transport.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	unblock := scriptRecv(transport,
		recvItem{f: frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(ports.MsgOutput, ports.MarshalOutput(ports.Output{Data: []byte("unchanged-by-observer"), NewStateNum: 3}))},
		recvItem{f: frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))},
	)
	defer unblock()
	transport.EXPECT().Send(isType(ports.MsgAck)).Return(nil).Maybe()
	transport.EXPECT().Close().Return(nil).Once()

	err := client.Attach(context.Background(), transport, term, realClock{}, ports.IntentEphemeral, "", client.WithRuntimeObserver(observer))
	require.NoError(t, err)
	require.Equal(t, "unchanged-by-observer", out.String(), "observer must not alter terminal bytes")
	require.Equal(t, int32(1), restores.Load())
	// Carriage spans belong only to concrete adapters. The client owns the
	// post-successful-flush terminal boundary, before its ACK is queued.
	observer.requireOrdered(t, ports.RuntimeTerminalFlushed)
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
