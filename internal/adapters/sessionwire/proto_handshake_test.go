package sessionwire

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// protoPipe is a scripted in-memory wire.Transport for preamble tests.
type protoPipe struct {
	mu     sync.Mutex
	in     chan wire.Envelope
	peer   *protoPipe
	closed bool
}

func newProtoPipePair() (*protoPipe, *protoPipe) {
	a := &protoPipe{in: make(chan wire.Envelope, 16)}
	b := &protoPipe{in: make(chan wire.Envelope, 16)}
	a.peer, b.peer = b, a
	return a, b
}

func (p *protoPipe) Send(envelope wire.Envelope) error {
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

func (p *protoPipe) Recv() (wire.Envelope, error) {
	envelope, ok := <-p.in
	if !ok {
		return wire.Envelope{}, context.DeadlineExceeded
	}
	return envelope, nil
}

func (p *protoPipe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

// TestProtoPreambleHandshake proves the preamble exchange over raw
// envelope transports: acceptance negotiates minima, mismatches refuse with
// a typed response, and the deadline bounds a silent peer.
func TestProtoPreambleHandshake(t *testing.T) {
	t.Run("accept negotiates minima", func(t *testing.T) {
		client, server := newProtoPipePair()
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

	t.Run("version mismatch refuses", func(t *testing.T) {
		client, server := newProtoPipePair()
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

	t.Run("wrong magic refuses", func(t *testing.T) {
		bad := preambleRequestToWire(defaultProtoCeilings())
		bad.Magic = 0xDEADBEEF
		_, err := checkPreambleRequest(bad)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})

	t.Run("wrong role refuses", func(t *testing.T) {
		bad := preambleRequestToWire(defaultProtoCeilings())
		bad.Role.Role = 2
		_, err := checkPreambleRequest(bad)
		require.ErrorIs(t, err, ErrPreambleRejected)

		resp := preambleResponseToWire(true, defaultProtoCeilings(), 0)
		resp.Role.Role = 1
		_, err = checkPreambleResponse(resp)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})

	t.Run("oversized advertisement refuses", func(t *testing.T) {
		bad := preambleRequestToWire(defaultProtoCeilings())
		bad.MaxReceiveEnvelopeBytes = wire.AbsoluteEnvelopeLimit + 1
		_, err := checkPreambleRequest(bad)
		require.ErrorIs(t, err, ErrPreambleRejected)

		bad = preambleRequestToWire(defaultProtoCeilings())
		bad.OutputDataLimit = uint64(protocol.MaxOutputDataLen) + 1
		_, err = checkPreambleRequest(bad)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})

	t.Run("deadline bounds silent peer", func(t *testing.T) {
		client, _ := newProtoPipePair()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := runProtoClientPreamble(ctx, client, defaultProtoCeilings())
		require.ErrorIs(t, err, ErrPreambleTimeout)
	})

	t.Run("oversize preamble rejected", func(t *testing.T) {
		client, server := newProtoPipePair()
		go func() {
			_ = client.Send(wire.Envelope{Payload: make([]byte, wire.PreambleLimit+1)})
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := runProtoServerPreamble(ctx, server, defaultProtoCeilings())
		require.ErrorIs(t, err, wire.ErrScanLength)
	})
}
