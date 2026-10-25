package sessionwire

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// pipeState is one end of the in-memory envelope pipe behind
// newProtoPipePair. It implements no interface: the wire mocks capture it.
type pipeState struct {
	mu     sync.Mutex
	in     chan wire.Envelope
	peer   *pipeState
	closed bool
}

func (p *pipeState) send(envelope wire.Envelope) error {
	p.mu.Lock()
	peer, closed := p.peer, p.closed
	p.mu.Unlock()
	if closed {
		return context.Canceled
	}
	select {
	case peer.in <- envelope:
		return nil
	default:
		return ErrUnsupportedSend
	}
}

func (p *pipeState) recv() (wire.Envelope, error) {
	envelope, ok := <-p.in
	if !ok {
		return wire.Envelope{}, context.DeadlineExceeded
	}
	return envelope, nil
}

func (p *pipeState) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func newProtoPipeMock(t *testing.T, state *pipeState) *wiremocks.MockTransport {
	t.Helper()
	transport := wiremocks.NewMockTransport(t)
	transport.EXPECT().Send(mock.Anything).RunAndReturn(state.send).Maybe()
	transport.EXPECT().Recv().RunAndReturn(state.recv).Maybe()
	transport.EXPECT().Close().RunAndReturn(state.close).Maybe()
	return transport
}

// newProtoPipePair returns two connected in-memory wire.Transport mocks for
// preamble tests: Send on one lands in the other's Recv.
func newProtoPipePair(t *testing.T) (*wiremocks.MockTransport, *wiremocks.MockTransport) {
	t.Helper()
	a := &pipeState{in: make(chan wire.Envelope, 16)}
	b := &pipeState{in: make(chan wire.Envelope, 16)}
	a.peer, b.peer = b, a
	// Unblock any preamble goroutine still reading when the test ends, so the
	// synctest bubble can drain.
	t.Cleanup(func() { close(a.in); close(b.in) })
	return newProtoPipeMock(t, a), newProtoPipeMock(t, b)
}

// TestProtoPreambleHandshake proves the preamble exchange over raw
// envelope transports: acceptance negotiates minima, mismatches refuse with
// a typed response, and the deadline bounds a silent peer.
func TestProtoPreambleHandshake(t *testing.T) {
	t.Run("accept negotiates minima", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client, server := newProtoPipePair(t)
			serverCeilings := protoCeilings{maxReceiveEnvelopeBytes: wire.AbsoluteEnvelopeLimit / 2, outputDataLimit: uint64(protocol.MaxOutputDataLen) / 2}
			serverDone := make(chan protoCeilings, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ceilings, err := runProtoServerPreamble(ctx, server, serverCeilings)
				require.NoError(t, err)
				serverDone <- ceilings
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			clientCeilings, err := runProtoClientPreamble(ctx, client, defaultProtoCeilings())
			require.NoError(t, err)
			require.Equal(t, serverCeilings, clientCeilings)
			require.Equal(t, serverCeilings, <-serverDone)
		})
	})

	t.Run("version mismatch refuses", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client, server := newProtoPipePair(t)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, _ = runProtoServerPreamble(ctx, server, defaultProtoCeilings())
			}()
			bad := preambleRequestToWire(defaultProtoCeilings())
			bad.Version = uint32(protocol.Version) + 1
			raw, err := (proto.Marshal(bad))
			require.NoError(t, err)
			require.NoError(t, client.Send(wire.Envelope{Payload: raw}))
			envelope, err := client.Recv()
			require.NoError(t, err)
			var response wire.PreambleResponse
			require.NoError(t, (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(envelope.Payload, &response)))
			require.False(t, response.GetAccepted())
			require.Equal(t, uint32(3), response.GetRejection().GetCode())
		})
	})

	t.Run("wrong magic refuses", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			bad := preambleRequestToWire(defaultProtoCeilings())
			bad.Magic = 0xDEADBEEF
			_, err := checkPreambleRequest(bad)
			require.ErrorIs(t, err, ErrPreambleRejected)
		})
	})

	t.Run("wrong role refuses", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			bad := preambleRequestToWire(defaultProtoCeilings())
			bad.Role.Role = 2
			_, err := checkPreambleRequest(bad)
			require.ErrorIs(t, err, ErrPreambleRejected)

			resp := preambleResponseToWire(true, defaultProtoCeilings(), 0)
			resp.Role.Role = 1
			_, err = checkPreambleResponse(resp)
			require.ErrorIs(t, err, ErrPreambleRejected)
		})
	})

	t.Run("oversized advertisement refuses", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			bad := preambleRequestToWire(defaultProtoCeilings())
			bad.MaxReceiveEnvelopeBytes = wire.AbsoluteEnvelopeLimit + 1
			_, err := checkPreambleRequest(bad)
			require.ErrorIs(t, err, ErrPreambleRejected)

			bad = preambleRequestToWire(defaultProtoCeilings())
			bad.OutputDataLimit = uint64(protocol.MaxOutputDataLen) + 1
			_, err = checkPreambleRequest(bad)
			require.ErrorIs(t, err, ErrPreambleRejected)
		})
	})

	t.Run("deadline bounds silent peer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client, _ := newProtoPipePair(t)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, err := runProtoClientPreamble(ctx, client, defaultProtoCeilings())
			require.ErrorIs(t, err, ErrPreambleTimeout)
		})
	})

	t.Run("oversize preamble rejected", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client, server := newProtoPipePair(t)
			go func() {
				_ = client.Send(wire.Envelope{Payload: make([]byte, wire.PreambleLimit+1)})
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := runProtoServerPreamble(ctx, server, defaultProtoCeilings())
			require.ErrorIs(t, err, wire.ErrScanLength)
		})
	})
}
