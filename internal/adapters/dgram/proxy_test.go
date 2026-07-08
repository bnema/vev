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
	recv       chan recvResult
	sent       chan ports.Frame
	sendErr    chan error
	linkState  ports.LinkState
	linkEvents chan ports.LinkEvent
	closed     chan struct{}
	closeOnce  sync.Once
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
		return err
	default:
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

func TestProxyRuntimeRetriesRecoverableClientSendWithoutDroppingFrame(t *testing.T) {
	tests := []struct {
		name  string
		state ports.LinkState
		err   error
	}{
		{name: "connected pending full", state: ports.LinkStateConnected, err: ErrPendingFull},
		{name: "degraded transient error", state: ports.LinkStateDegraded, err: errors.New("degraded write drop")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeTransport()
			daemon := newFakeTransport()
			// No link event is emitted, so the idle timer is never armed; the
			// retry alone must recover. IdleTTL is large to make that explicit.
			client.linkState = tt.state
			errCh := make(chan error, 1)
			go func() {
				errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Hour, RetryBackoff: time.Nanosecond}.Run(t.Context())
			}()
			client.sendErr <- tt.err
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
		})
	}
}

// TestProxyRuntimeSurvivesPendingFullAndPreservesOrder is the congestion
// regression: a persistently pending-full client window must not tear the proxy
// down (the old idle TTL killed it after 30s) and every frame must arrive in
// order. IdleTTL is a nanosecond: if a retry armed the idle timer the proxy
// would die at once, so surviving proves retries no longer arm it.
func TestProxyRuntimeSurvivesPendingFullAndPreservesOrder(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	client.linkState = ports.LinkStateConnected
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Nanosecond, RetryBackoff: time.Nanosecond}.Run(t.Context())
	}()
	const frames = 4
	for i := range frames {
		client.sendErr <- ErrPendingFull
		daemon.recv <- recvResult{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}}}
		select {
		case got := <-client.sent:
			if got.Type != ports.MsgOutput || len(got.Payload) != 1 || got.Payload[0] != byte(i) {
				t.Fatalf("frame %d delivered out of order or dropped: %+v", i, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("frame %d dropped or proxy killed under pending-full", i)
		}
	}
	select {
	case err := <-errCh:
		t.Fatalf("proxy returned under sustained pending-full: %v", err)
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

func TestProxyRuntimeOfflineLinkEventExpiresViaIdleTTL(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	client.linkState = ports.LinkStateOffline
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: 20 * time.Millisecond}.Run(t.Context())
	}()
	client.linkEvents <- ports.LinkEvent{State: ports.LinkStateOffline}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrLinkDead) {
			t.Fatalf("Run err=%v, want ErrLinkDead", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for idle expiry after offline link event")
	}
}

func TestProxyRuntimeDeadLinkEventTearsDown(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Hour}.Run(t.Context())
	}()
	client.linkEvents <- ports.LinkEvent{State: ports.LinkStateDead}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrLinkDead) {
			t.Fatalf("Run err=%v, want ErrLinkDead", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dead link event did not tear down proxy")
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
