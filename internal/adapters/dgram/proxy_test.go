package dgram

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
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

func TestProxyRuntimeClampsDatagramHelloOutputWindowAndPreservesControl(t *testing.T) {
	for _, requested := range []uint8{0, 1, 8} {
		t.Run(fmt.Sprintf("requested_%d", requested), func(t *testing.T) {
			client := newFakeTransport()
			daemon := newFakeTransport()
			errCh := make(chan error, 1)
			go func() { errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Hour}.Run(t.Context()) }()
			hello := ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", MaxOutputInFlight: requested}
			client.recv <- recvResult{frame: ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)}}

			select {
			case frame := <-daemon.sent:
				got, err := ports.UnmarshalHello(frame.Payload)
				if err != nil {
					t.Fatal(err)
				}
				if got.MaxOutputInFlight != 1 {
					t.Fatalf("proxied output window=%d, want datagram-safe 1", got.MaxOutputInFlight)
				}
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for proxied Hello")
			}

			input := ports.Frame{Type: ports.MsgInput, Payload: []byte("still flowing")}
			ack := ports.Frame{Type: ports.MsgAck, Payload: ports.MarshalAck(ports.Ack{AckedStateNum: 7})}
			client.recv <- recvResult{frame: input}
			client.recv <- recvResult{frame: ack}
			for _, want := range []ports.Frame{input, ack} {
				select {
				case got := <-daemon.sent:
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("proxied control frame=%+v, want %+v", got, want)
					}
				case <-time.After(time.Second):
					t.Fatalf("timeout waiting for proxied %v", want.Type)
				}
			}

			daemon.recv <- recvResult{err: io.EOF}
			if err := <-errCh; err != nil {
				t.Fatalf("Run err=%v, want nil", err)
			}
		})
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

type proxyManualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers chan *proxyManualTimer
}

func newProxyManualClock(now time.Time) *proxyManualClock {
	return &proxyManualClock{now: now, timers: make(chan *proxyManualTimer, 4)}
}

func (c *proxyManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *proxyManualClock) NewTimer(d time.Duration) ports.Timer {
	tm := &proxyManualTimer{c: make(chan time.Time, 1), d: d}
	c.timers <- tm
	return tm
}

func (c *proxyManualClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	c.mu.Unlock()
	for {
		select {
		case tm := <-c.timers:
			if tm.duration() <= d {
				tm.fire(now)
			} else {
				c.timers <- tm
				return
			}
		default:
			return
		}
	}
}

type proxyManualTimer struct {
	mu sync.Mutex
	c  chan time.Time
	d  time.Duration
}

func (t *proxyManualTimer) C() <-chan time.Time { return t.c }
func (t *proxyManualTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.d = d
	return true
}
func (t *proxyManualTimer) Stop() bool { return true }
func (t *proxyManualTimer) duration() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.d
}
func (t *proxyManualTimer) fire(now time.Time) {
	select {
	case t.c <- now:
	default:
	}
}

func waitForProxyManualTimers(t *testing.T, clk *proxyManualClock, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(clk.timers) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("manual timers=%d, want at least %d", len(clk.timers), n)
}

func TestProxyRuntimeClientDeadWaitsForIdleTTL(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	clk := newProxyManualClock(time.Unix(0, 0))
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Minute, Clock: clk}.Run(t.Context())
	}()

	client.linkEvents <- ports.LinkEvent{State: ports.LinkStateDead}
	waitForProxyManualTimers(t, clk, 1)
	select {
	case err := <-errCh:
		t.Fatalf("Run returned before IdleTTL after client LinkStateDead: %v", err)
	default:
	}

	clk.advance(time.Minute)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrLinkDead) {
			t.Fatalf("Run err=%v, want ErrLinkDead", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for idle expiry after client LinkStateDead")
	}
}

func TestProxyRuntimeClientRecvDeadWaitsForIdleTTL(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	clk := newProxyManualClock(time.Unix(0, 0))
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Minute, Clock: clk}.Run(t.Context())
	}()

	client.recv <- recvResult{err: ErrLinkDead}
	waitForProxyManualTimers(t, clk, 1)
	select {
	case err := <-errCh:
		t.Fatalf("Run returned before IdleTTL after client Recv ErrLinkDead: %v", err)
	default:
	}

	clk.advance(time.Minute)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrLinkDead) {
			t.Fatalf("Run err=%v, want ErrLinkDead", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for idle expiry after client Recv ErrLinkDead")
	}
}

func TestProxyRuntimeClientSendDeadWaitsForIdleTTL(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	clk := newProxyManualClock(time.Unix(0, 0))
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Minute, Clock: clk}.Run(t.Context())
	}()

	client.sendErr <- ErrLinkDead
	daemon.recv <- recvResult{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("client is gone")}}
	waitForProxyManualTimers(t, clk, 1)
	select {
	case err := <-errCh:
		t.Fatalf("Run returned before IdleTTL after client Send ErrLinkDead: %v", err)
	default:
	}

	clk.advance(time.Minute)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrLinkDead) {
			t.Fatalf("Run err=%v, want ErrLinkDead", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for idle expiry after client Send ErrLinkDead")
	}
}

func TestProxyRuntimeDaemonRecvDeadTearsDown(t *testing.T) {
	client := newFakeTransport()
	daemon := newFakeTransport()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ProxyRuntime{Client: client, Daemon: daemon, IdleTTL: time.Hour}.Run(t.Context())
	}()
	daemon.recv <- recvResult{err: ErrLinkDead}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrLinkDead) {
			t.Fatalf("Run err=%v, want ErrLinkDead", err)
		}
	case <-time.After(time.Second):
		t.Fatal("daemon recv dead did not tear down proxy")
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

type retryingOfflineTransport struct {
	closed        chan struct{}
	firstSend     chan struct{}
	closeOnce     sync.Once
	firstSendOnce sync.Once
}

func newRetryingOfflineTransport() *retryingOfflineTransport {
	return &retryingOfflineTransport{closed: make(chan struct{}), firstSend: make(chan struct{})}
}

func (t *retryingOfflineTransport) Send(ports.Frame) error {
	t.firstSendOnce.Do(func() { close(t.firstSend) })
	return errors.New("offline")
}

func (t *retryingOfflineTransport) Recv() (ports.Frame, error) {
	<-t.closed
	return ports.Frame{}, io.EOF
}

func (t *retryingOfflineTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (t *retryingOfflineTransport) LinkState() ports.LinkState { return ports.LinkStateOffline }
func (t *retryingOfflineTransport) LinkEvents() <-chan ports.LinkEvent {
	return nil
}

func TestProxyCopierStopsRetryingWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	client := newRetryingOfflineTransport()
	daemon := newFakeTransport()
	done := make(chan struct{})
	copier := proxyCopier{ctx: ctx, errCh: make(chan<- proxyCopyErr), retryBackoff: time.Hour, clk: realClock{}}
	go func() {
		copier.copyFrames(proxyCopyDirection{src: daemon, dst: client, recvKind: proxyDaemonRecv, sendKind: proxyClientSend, retryRecoverable: true})
		close(done)
	}()

	daemon.recv <- recvResult{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("retry until cancel")}}
	select {
	case <-client.firstSend:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for proxy send attempt")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retrying copier did not stop after context cancellation")
	}
}

func TestClampDatagramHelloOutputWindowPreservesProtocolValidation(t *testing.T) {
	t.Run("malformed payload remains malformed", func(t *testing.T) {
		payload := append(ports.MarshalHello(ports.Hello{Version: ports.ProtocolVersion, MaxOutputInFlight: 8}), 0xff)
		frame := ports.Frame{Type: ports.MsgHello, Payload: payload}
		got := clampDatagramHelloOutputWindow(frame)
		if !reflect.DeepEqual(got, frame) {
			t.Fatalf("malformed Hello changed: got %#v, want %#v", got, frame)
		}
		if _, err := ports.UnmarshalHello(got.Payload); err == nil {
			t.Fatal("malformed Hello became decodable")
		}
	})

	t.Run("mismatched version remains mismatched", func(t *testing.T) {
		wantVersion := ports.ProtocolVersion + 1
		frame := ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(ports.Hello{
			Version: wantVersion, MaxOutputInFlight: 8,
		})}
		got, err := ports.UnmarshalHello(clampDatagramHelloOutputWindow(frame).Payload)
		if err != nil {
			t.Fatal(err)
		}
		if got.Version != wantVersion {
			t.Fatalf("version=%d, want unchanged mismatch %d", got.Version, wantVersion)
		}
		if got.MaxOutputInFlight != 1 {
			t.Fatalf("output window=%d, want 1", got.MaxOutputInFlight)
		}
	})
}
