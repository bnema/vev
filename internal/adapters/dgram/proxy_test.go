package dgram

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

type fakeTransport struct {
	recv            chan recvResult
	sent            chan ports.Frame
	sendErr         chan error
	sendErrDefault  error
	sideEffectOnErr bool
	linkState       ports.LinkState
	linkEvents      chan ports.LinkEvent
	closed          chan struct{}
	closeOnce       sync.Once
}

type recvResult struct {
	frame ports.Frame
	err   error
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{recv: make(chan recvResult, 4), sent: make(chan ports.Frame, 4), sendErr: make(chan error, 4), linkEvents: make(chan ports.LinkEvent, 4), linkState: ports.LinkStateConnected, closed: make(chan struct{})}
}
func (f *fakeTransport) Send(fr ports.Frame) error {
	select {
	case err := <-f.sendErr:
		if f.sideEffectOnErr {
			f.sent <- fr
		}
		return err
	default:
		if f.sendErrDefault != nil {
			return f.sendErrDefault
		}
	}
	f.sent <- fr
	return nil
}
func (f *fakeTransport) Recv() (ports.Frame, error) {
	select {
	case r := <-f.recv:
		return r.frame, r.err
	case <-f.closed:
		return ports.Frame{}, io.EOF
	}
}
func (f *fakeTransport) Close() error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}
func (f *fakeTransport) LinkState() ports.LinkState         { return f.linkState }
func (f *fakeTransport) LinkEvents() <-chan ports.LinkEvent { return f.linkEvents }

func TestProxyRuntimeCopiesFramesAndDaemonEOFIsTerminal(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	errCh := make(chan error, 1)
	go func() { errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Hour}.Run(t.Context()) }()
	client.recv <- recvResult{frame: ports.Frame{Type: ports.MsgInput, Payload: []byte("x")}}
	select {
	case got := <-daemon.sent:
		if got.Type != ports.MsgInput {
			t.Fatalf("daemon got type %v", got.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for copied frame")
	}
	daemon.recv <- recvResult{err: io.EOF}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run err=%v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for terminal EOF")
	}
}

func TestProxyRuntimeRestartsRecoverableClientReceiveUntilIdleTTL(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	client.linkState = ports.LinkStateOffline
	client.linkEvents <- ports.LinkEvent{State: ports.LinkStateOffline}
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: 50 * time.Millisecond, RetryBackoff: time.Nanosecond}.Run(t.Context())
	}()
	client.recv <- recvResult{err: errors.New("temporary receive failure")}
	client.recv <- recvResult{frame: ports.Frame{Type: ports.MsgInput, Payload: []byte("after recovery")}}
	select {
	case got := <-daemon.sent:
		if got.Type != ports.MsgInput || string(got.Payload) != "after recovery" {
			t.Fatalf("daemon got %+v, want recovered input", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for recovered frame")
	}
	daemon.recv <- recvResult{err: io.EOF}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run err=%v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for daemon EOF")
	}
}

func TestProxyRuntimeRetriesRecoverableClientSendWithoutDroppingFrame(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	client.linkState = ports.LinkStateOffline
	client.linkEvents <- ports.LinkEvent{State: ports.LinkStateOffline}
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: 50 * time.Millisecond, RetryBackoff: time.Nanosecond}.Run(t.Context())
	}()
	client.sendErr <- ErrPendingFull
	daemon.recv <- recvResult{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("preserved during outage")}}
	select {
	case got := <-client.sent:
		if got.Type != ports.MsgOutput || string(got.Payload) != "preserved during outage" {
			t.Fatalf("client got %+v, want retried output", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for recovered daemon output")
	}
	daemon.recv <- recvResult{err: io.EOF}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run err=%v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for daemon EOF")
	}
}

func TestProxyRuntimeDoesNotRetrySideEffectingClientSendError(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	client.linkState = ports.LinkStateOffline
	client.sideEffectOnErr = true
	client.sendErr <- errors.New("write timed out after queuing")
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Hour, RetryBackoff: time.Nanosecond}.Run(t.Context())
	}()
	daemon.recv <- recvResult{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("maybe queued")}}
	select {
	case got := <-client.sent:
		if got.Type != ports.MsgOutput || string(got.Payload) != "maybe queued" {
			t.Fatalf("client got %+v, want side-effected output", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for side-effected output")
	}
	select {
	case got := <-client.sent:
		t.Fatalf("side-effecting send error was retried and duplicated output: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	daemon.recv <- recvResult{err: io.EOF}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run err=%v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for daemon EOF")
	}
}

func TestProxyRuntimeErrPendingFullRetryStartsIdleTTL(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	client.linkState = ports.LinkStateOffline
	client.sendErrDefault = ErrPendingFull
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: 20 * time.Millisecond, RetryBackoff: time.Millisecond}.Run(t.Context())
	}()
	daemon.recv <- recvResult{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("stalled")}}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrLinkDead) {
			t.Fatalf("Run err=%v, want ErrLinkDead", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for idle expiry from ErrPendingFull retry")
	}
}

func TestProxyRuntimeKeepsRecoverableLinkUntilIdleTTL(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	client.linkState = ports.LinkStateOffline
	client.linkEvents <- ports.LinkEvent{State: ports.LinkStateOffline}
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: 20 * time.Millisecond}.Run(t.Context())
	}()
	client.recv <- recvResult{err: errors.New("temporary receive failure")}
	select {
	case err := <-errCh:
		t.Fatalf("Run returned before idle TTL: %v", err)
	case <-time.After(5 * time.Millisecond):
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrLinkDead) {
			t.Fatalf("Run err=%v, want ErrLinkDead", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for idle expiry")
	}
}

func TestProxyRuntimeClosedLinkEventsDoNotSpin(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	close(client.linkEvents)
	errCh := make(chan error, 1)
	go func() { errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Hour}.Run(t.Context()) }()
	daemon.recv <- recvResult{err: io.EOF}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run err=%v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for daemon EOF")
	}
}
