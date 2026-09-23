package brokeripc

import (
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type rawRelayCore struct {
	received chan []byte
	done     chan struct{}
}

func (c *rawRelayCore) SendEnvelope(payload []byte) error {
	c.received <- append([]byte(nil), payload...)
	return nil
}
func (c *rawRelayCore) RecvEnvelope() ([]byte, error) { <-c.done; return nil, io.EOF }
func (c *rawRelayCore) Done() <-chan struct{}         { return c.done }
func (c *rawRelayCore) Err() error                    { return nil }
func (c *rawRelayCore) Close() error {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	return nil
}

func TestServerStreamRelaysOpaqueEnvelopes(t *testing.T) {
	core := &rawRelayCore{received: make(chan []byte, 1), done: make(chan struct{})}
	session := &serverSession{ctx: context.Background(), cfg: Config{}.withDefaults()}
	stream, err := newServerStream(session, 1, core)
	require.NoError(t, err)
	session.wg.Add(1)
	go stream.relayClient()
	payload := []byte{0xff, 0xfe, 0xfd, 0x80}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	require.NoError(t, stream.deliver(frame[:2]))
	require.NoError(t, stream.deliver(frame[2:]))
	select {
	case got := <-core.received:
		require.Equal(t, payload, got)
	case <-time.After(5 * time.Second):
		t.Fatal("opaque envelope not relayed")
	}
	stream.close(nil)
	session.wg.Wait()
}
