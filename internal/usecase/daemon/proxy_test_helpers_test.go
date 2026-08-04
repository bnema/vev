package daemon

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type proxyRecv struct {
	frame ports.Frame
	err   error
}

type proxyTestTransport struct {
	recv   chan proxyRecv
	closed chan struct{}
	sent   chan ports.Frame
	once   sync.Once

	sendFails   atomic.Bool
	sendGate    <-chan struct{}
	sendEntered chan struct{}
	receiving   atomic.Int32
	concurrent  atomic.Bool
	sending     atomic.Int32
}

func newProxyTestTransport() *proxyTestTransport {
	return &proxyTestTransport{recv: make(chan proxyRecv, 16), closed: make(chan struct{}), sent: make(chan ports.Frame, 32)}
}

func (t *proxyTestTransport) Send(frame ports.Frame) error {
	if t.sending.Add(1) != 1 {
		t.concurrent.Store(true)
	}
	defer t.sending.Add(-1)
	if t.sendFails.Load() {
		return io.ErrClosedPipe
	}
	if t.sendGate != nil {
		if t.sendEntered != nil {
			select {
			case t.sendEntered <- struct{}{}:
			default:
			}
		}
		select {
		case <-t.sendGate:
		case <-t.closed:
			return io.ErrClosedPipe
		}
	}
	select {
	case t.sent <- frame:
		return nil
	case <-t.closed:
		return io.ErrClosedPipe
	}
}

func (t *proxyTestTransport) Recv() (ports.Frame, error) {
	t.receiving.Add(1)
	defer t.receiving.Add(-1)
	select {
	case item := <-t.recv:
		return item.frame, item.err
	case <-t.closed:
		return ports.Frame{}, io.EOF
	}
}

func (t *proxyTestTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

func proxyWelcome(name string, token uint64, capabilities uint32) ports.Frame {
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{
		SessionID: "remote-id", SessionName: name, ResumeToken: token, Capabilities: capabilities,
	})}
}

func proxyHandshakeSnapshot() ports.Frame {
	return ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("remote initial\n"),
	})}
}

func newProxyTestDaemon(t *testing.T, factory ports.RemoteDialerFactory, clk ports.Clock) *Daemon {
	t.Helper()
	if clk == nil {
		clk = stubClock{}
	}
	return New(nil, clk, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithRemoteDiscovery(nil, nil, nil, factory, ports.RemoteTransportUDP))
}

func requireProxyHello(t *testing.T, tr *proxyTestTransport) ports.Hello {
	t.Helper()
	select {
	case frame := <-tr.sent:
		require.Equal(t, ports.MsgHello, frame.Type)
		hello, err := ports.UnmarshalHello(frame.Payload)
		require.NoError(t, err)
		return hello
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for proxy Hello")
		return ports.Hello{}
	}
}

func stopProxy(t *testing.T, p *proxySession) {
	t.Helper()
	p.stop()
	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for proxy link to stop")
	}
}
