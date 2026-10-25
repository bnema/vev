package brokeripc

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"testing/synctest"

	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// rawRelayState is the plain state behind the relay core mock: it records the
// opaque envelopes the stream relays toward the daemon.
type rawRelayState struct {
	received chan []byte
	done     chan struct{}
	once     sync.Once
}

func newRawRelayCoreMock(t *testing.T, opts ...func(*portsmocks.MockBrokerEnvelopeStream, *rawRelayState)) (*portsmocks.MockBrokerEnvelopeStream, *rawRelayState) {
	t.Helper()
	state := &rawRelayState{received: make(chan []byte, 1), done: make(chan struct{})}
	m := portsmocks.NewMockBrokerEnvelopeStream(t)
	for _, opt := range opts {
		opt(m, state)
	}
	m.EXPECT().SendEnvelope(mock.Anything).RunAndReturn(func(payload []byte) error {
		state.received <- append([]byte(nil), payload...)
		return nil
	}).Maybe()
	m.EXPECT().RecvEnvelope().RunAndReturn(func() ([]byte, error) { <-state.done; return nil, io.EOF }).Maybe()
	m.EXPECT().Done().RunAndReturn(func() <-chan struct{} { return state.done }).Maybe()
	m.EXPECT().Err().Return(nil).Maybe()
	m.EXPECT().Close().RunAndReturn(func() error {
		state.once.Do(func() { close(state.done) })
		return nil
	}).Maybe()
	return m, state
}

func TestServerStreamRelaysOpaqueEnvelopes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		coreMock, core := newRawRelayCoreMock(t)
		session := &serverSession{ctx: context.Background(), cfg: Config{}.withDefaults()}
		stream, err := newServerStream(session, 1, coreMock)
		require.NoError(t, err)
		session.wg.Add(1)
		go stream.relayClient()
		payload := []byte{0xff, 0xfe, 0xfd, 0x80}
		frame := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
		copy(frame[4:], payload)
		require.NoError(t, stream.deliver(frame[:2]))
		require.NoError(t, stream.deliver(frame[2:]))
		synctest.Wait()
		select {
		case got := <-core.received:
			require.Equal(t, payload, got)
		default:
			t.Fatal("opaque envelope not relayed")
		}
		stream.close(nil)
		session.wg.Wait()
	})
}
