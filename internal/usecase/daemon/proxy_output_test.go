package daemon

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var proxyHandshakeState atomic.Uint64

type proxyRecv struct {
	frame ports.Frame
	err   error
}

type proxyTestTransport struct {
	recv      chan proxyRecv
	closed    chan struct{}
	closeOnce sync.Once
	sent      chan ports.Frame

	sendGate     <-chan struct{}
	sendEntered  chan struct{}
	ungatedSends atomic.Int32
	sending      atomic.Int32
	concurrent   atomic.Bool
	receiving    atomic.Int32
	// sendFails rejects every write while leaving Recv able to deliver queued
	// frames, so a reply failure can be observed without also ending the receive
	// pump.
	sendFails atomic.Bool
}

func newProxyTestTransport() *proxyTestTransport {
	return newProxyTestTransportWithSendGate(nil, nil, 0)
}

func newProxyTestTransportWithSendGate(gate <-chan struct{}, entered chan struct{}, ungatedSends int32) *proxyTestTransport {
	transport := &proxyTestTransport{
		recv:        make(chan proxyRecv, 16),
		closed:      make(chan struct{}),
		sent:        make(chan ports.Frame, 32),
		sendGate:    gate,
		sendEntered: entered,
	}
	transport.ungatedSends.Store(ungatedSends)
	return transport
}

func (t *proxyTestTransport) Send(frame ports.Frame) error {
	if t.sending.Add(1) != 1 {
		t.concurrent.Store(true)
	}
	defer t.sending.Add(-1)
	if t.sendFails.Load() {
		return io.ErrClosedPipe
	}
	if t.sendGate != nil && t.ungatedSends.Add(-1) < 0 {
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

func applyTestScreenText(p *proxySession, row, col int, text string) bool {
	p.mu.Lock()
	generation := p.linkGeneration
	size := p.contentSize
	p.mu.Unlock()
	frame := renderer.NewFrame(size.Cols, size.Rows)
	for x, r := range []rune(text) {
		if col+x < frame.Width {
			frame.Set(col+x, row, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
		}
	}
	spans := make([]ports.ScreenSpan, size.Rows)
	for y := range size.Rows {
		spans[y] = ports.ScreenSpan{Y: uint16(y), Cells: append([]renderer.Cell(nil), frame.Row(y)...)}
	}
	_, _, changed := p.applyScreenUpdateForGeneration(generation, ports.ScreenUpdate{
		NewStateNum: 1, Kind: ports.ScreenUpdateSnapshot, Size: size,
		Cursor: ports.ScreenCursor{Row: uint16(row), Col: uint16(col), Visible: true}, Spans: spans,
	})
	return changed
}

func (t *proxyTestTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func proxyWelcome(name string, token uint64, capabilities uint32) ports.Frame {
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{
		SessionID:    "remote-id",
		SessionName:  name,
		ResumeToken:  token,
		Capabilities: capabilities,
	})}
}

func proxyHandshakeSnapshot() ports.Frame {
	const cols, rows = 80, 22
	spans := make([]ports.ScreenSpan, rows)
	for y := range rows {
		cells := make([]renderer.Cell, cols)
		for x := range cells {
			cells[x] = renderer.BlankCell()
		}
		spans[y] = ports.ScreenSpan{Y: uint16(y), Cells: cells}
	}
	payload, err := ports.MarshalScreenUpdate(ports.ScreenUpdate{
		NewStateNum: proxyHandshakeState.Add(1), Kind: ports.ScreenUpdateSnapshot, Size: domain.Size{Cols: cols, Rows: rows},
		Cursor: ports.ScreenCursor{Visible: true}, Spans: spans,
	})
	if err != nil {
		panic(err)
	}
	return ports.Frame{Type: ports.MsgScreenUpdate, Payload: payload}
}

func proxyMeta(name string) ports.Frame {
	payload, err := ports.MarshalSessionMeta(ports.SessionMeta{
		SessionName: name,
		Tabs:        []ports.SessionTabMeta{{Index: 0, Name: "shell"}},
	})
	if err != nil {
		panic(err)
	}
	return ports.Frame{Type: ports.MsgSessionMeta, Payload: payload}
}

// newProxyTestDaemon always installs a controllable clock so no proxy test can
// depend on wall-clock deadlines. Pass a signalClock to drive resume backoff.
func newProxyTestDaemon(t *testing.T, factory ports.RemoteDialerFactory, clk ports.Clock) *Daemon {
	t.Helper()
	if clk == nil {
		clk = stubClock{}
	}
	return New(nil, clk, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithRemoteDiscovery(nil, nil, nil, factory, ports.RemoteTransportUDP))
}

// proxySessionSnapshot clones the registry under d.mu so assertions never read
// d.sessions while a registration writes it. Assertions run on the clone, after
// the lock has been released.
func proxySessionSnapshot(d *Daemon) map[domain.SessionID]attachmentSession {
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps.Clone(d.sessions)
}

func proxyFactoryFor(t *testing.T, key domain.RemoteSessionKey, transports ...ports.Transport) ports.RemoteDialerFactory {
	t.Helper()
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	for _, transport := range transports {
		dialer := portsmocks.NewMockDialer(t)
		factory.EXPECT().DialerForRemote(key.Host, key.Name, ports.RemoteTransportUDP, mock.Anything).Return(dialer, nil).Once()
		dialer.EXPECT().Dial(mock.Anything).Return(transport, nil).Once()
	}
	return factory
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

func TestProxyHandshakePublishesOnlyAfterWelcomeAndMetadata(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	tr := newProxyTestTransport()
	d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr), stubClock{})

	result := make(chan struct {
		proxy *proxySession
		err   error
	}, 1)
	go func() {
		proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
		result <- struct {
			proxy *proxySession
			err   error
		}{proxy, err}
	}()

	hello := requireProxyHello(t, tr)
	require.Equal(t, ports.ProtocolVersion, hello.Version)
	require.Equal(t, ports.IntentAttach, hello.Intent)
	require.True(t, hello.Proxied)
	require.Equal(t, key.Name, hello.Name)
	require.Equal(t, contentSize(domain.Size{Cols: 80, Rows: 24}, false), hello.Size)
	require.Equal(t, uint8(maxUnackedOutputStates), hello.MaxOutputInFlight)
	require.Empty(t, proxySessionSnapshot(d), "Hello alone must not publish a proxy")

	tr.recv <- proxyRecv{frame: proxyWelcome(key.Name, 44, ports.CapabilityResume|ports.CapabilityProxied)}
	require.Empty(t, proxySessionSnapshot(d), "Welcome alone must not publish a proxy")
	tr.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	tr.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}

	created := awaitTestValue(t, result, "proxy handshake did not complete")
	require.NoError(t, created.err)
	require.Same(t, created.proxy, proxySessionSnapshot(d)[key.ID()])
	require.Equal(t, key, created.proxy.key)
	require.Equal(t, uint64(44), created.proxy.resumeToken)
	stopProxy(t, created.proxy)
}

func TestProxyHandshakeTimesOutBeforePublication(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	tr := newProxyTestTransport()
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr), clock)

	result := make(chan struct {
		proxy *proxySession
		err   error
	}, 1)
	go func() {
		proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
		result <- struct {
			proxy *proxySession
			err   error
		}{proxy: proxy, err: err}
	}()

	requireProxyHello(t, tr)
	timer := awaitTestValue(t, clock.timers, "proxy handshake did not arm its timeout")
	require.Equal(t, proxyHandshakeTimeout, timer.duration)
	tr.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	tr.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	timer.ch <- time.Time{}

	opened := awaitTestValue(t, result, "proxy handshake timeout did not finish")
	require.ErrorIs(t, opened.err, errProxyHandshakeTimeout)
	require.Nil(t, opened.proxy)
	require.Empty(t, proxySessionSnapshot(d), "timed-out handshake must not publish a proxy")
	select {
	case <-tr.closed:
	default:
		t.Fatal("timed-out handshake transport was not closed")
	}
	require.Zero(t, tr.receiving.Load(), "timed-out handshake receive pump outlived transport cleanup")
}

func TestProxyHandshakeTimeoutClosesBlockedSends(t *testing.T) {
	tests := []struct {
		name           string
		ungatedSends   int32
		queueHandshake bool
	}{
		{name: "hello", queueHandshake: false},
		{name: "initial ACK", ungatedSends: 1, queueHandshake: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
			entered := make(chan struct{}, 1)
			tr := newProxyTestTransportWithSendGate(make(chan struct{}), entered, tt.ungatedSends)
			if tt.queueHandshake {
				tr.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
				tr.recv <- proxyRecv{frame: proxyMeta(key.Name)}
				tr.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
			}
			clock := &signalClock{timers: make(chan *signalTimer, 1)}
			d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr), clock)
			result := make(chan error, 1)
			go func() {
				_, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
				result <- err
			}()

			if tt.queueHandshake {
				requireProxyHello(t, tr)
			}
			awaitTestCompletion(t, entered, "proxy handshake send did not block")
			timer := awaitTestValue(t, clock.timers, "proxy handshake did not arm its timeout")
			require.Equal(t, proxyHandshakeTimeout, timer.duration)
			timer.ch <- time.Time{}

			require.ErrorIs(t, awaitTestValue(t, result, "blocked handshake send did not finish"), errProxyHandshakeTimeout)
			select {
			case <-tr.closed:
			default:
				t.Fatal("timed-out handshake transport was not closed")
			}
			require.Zero(t, tr.receiving.Load(), "timed-out handshake receive pump outlived transport cleanup")
		})
	}
}

func TestProxyHandshakeContextUsesSystemClockWhenClockMissing(t *testing.T) {
	d := &Daemon{}
	ctx, timedOut, stop := d.proxyHandshakeContext(context.Background())
	defer stop()
	require.NoError(t, ctx.Err())
	select {
	case <-timedOut:
		t.Fatal("missing clock caused an immediate handshake timeout")
	default:
	}
}

func TestProxyHandshakeRejectsInvalidWelcomeWithoutPublication(t *testing.T) {
	tests := []struct {
		name  string
		first ports.Frame
	}{
		{name: "missing proxied capability", first: proxyWelcome("work", 1, ports.CapabilityResume)},
		{name: "malformed welcome", first: ports.Frame{Type: ports.MsgWelcome, Payload: []byte{0xff}}},
		{name: "wrong first type", first: proxyMeta("work")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
			tr := newProxyTestTransport()
			tr.recv <- proxyRecv{frame: tt.first}
			d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr), stubClock{})

			proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
			require.Error(t, err)
			require.Nil(t, proxy)
			require.Empty(t, proxySessionSnapshot(d))
			select {
			case <-tr.closed:
			default:
				t.Fatal("failed handshake transport was not closed")
			}
		})
	}
}

func TestProxyRegistryDedupesByExactStructuredKey(t *testing.T) {
	firstKey := domain.RemoteSessionKey{Host: "build.example", Name: "work"}
	secondKey := domain.RemoteSessionKey{Host: "build", Name: "example-work"}
	firstTransport := newProxyTestTransport()
	firstTransport.recv <- proxyRecv{frame: proxyWelcome(firstKey.Name, 1, ports.CapabilityProxied)}
	firstTransport.recv <- proxyRecv{frame: proxyMeta(firstKey.Name)}
	firstTransport.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
	secondTransport := newProxyTestTransport()
	secondTransport.recv <- proxyRecv{frame: proxyWelcome(secondKey.Name, 2, ports.CapabilityProxied)}
	secondTransport.recv <- proxyRecv{frame: proxyMeta(secondKey.Name)}
	secondTransport.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	for key, transport := range map[domain.RemoteSessionKey]ports.Transport{firstKey: firstTransport, secondKey: secondTransport} {
		dialer := portsmocks.NewMockDialer(t)
		factory.EXPECT().DialerForRemote(key.Host, key.Name, ports.RemoteTransportUDP, mock.Anything).Return(dialer, nil).Once()
		dialer.EXPECT().Dial(mock.Anything).Return(transport, nil).Once()
	}
	d := newProxyTestDaemon(t, factory, stubClock{})

	first, err := d.openProxySession(context.Background(), firstKey, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	deduped, err := d.openProxySession(context.Background(), firstKey, domain.Size{Cols: 100, Rows: 30})
	require.NoError(t, err)
	require.Same(t, first, deduped)
	second, err := d.openProxySession(context.Background(), secondKey, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	require.NotSame(t, first, second)
	require.Len(t, proxySessionSnapshot(d), 2)

	stopProxy(t, first)
	stopProxy(t, second)
}

func TestProxyCancelDuringDialOrHandshakeNeverPublishes(t *testing.T) {
	t.Run("dial", func(t *testing.T) {
		key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
		factory := portsmocks.NewMockRemoteDialerFactory(t)
		dialer := portsmocks.NewMockDialer(t)
		factory.EXPECT().DialerForRemote(key.Host, key.Name, ports.RemoteTransportUDP, mock.Anything).Return(dialer, nil).Once()
		dialEntered := make(chan struct{})
		dialer.EXPECT().Dial(mock.Anything).RunAndReturn(func(ctx context.Context) (ports.Transport, error) {
			close(dialEntered)
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()
		d := newProxyTestDaemon(t, factory, stubClock{})
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan struct {
			proxy *proxySession
			err   error
		}, 1)
		go func() {
			proxy, err := d.openProxySession(ctx, key, domain.Size{Cols: 80, Rows: 24})
			result <- struct {
				proxy *proxySession
				err   error
			}{proxy: proxy, err: err}
		}()
		awaitTestCompletion(t, dialEntered, "proxy dial did not start")
		cancel()

		opened := awaitTestValue(t, result, "canceled proxy dial did not finish")
		proxy, err := opened.proxy, opened.err
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, proxy)
		require.Empty(t, proxySessionSnapshot(d))
	})

	t.Run("handshake", func(t *testing.T) {
		key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
		tr := newProxyTestTransport()
		d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr), stubClock{})
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := d.openProxySession(ctx, key, domain.Size{Cols: 80, Rows: 24})
			result <- err
		}()
		requireProxyHello(t, tr)
		cancel()
		require.ErrorIs(t, awaitTestValue(t, result, "canceled proxy handshake did not finish"), context.Canceled)
		require.Empty(t, proxySessionSnapshot(d))
		select {
		case <-tr.closed:
		default:
			t.Fatal("cancellation did not close the handshaking transport")
		}
	})
}

func TestProxyLinkResumesAndReplacementIsTerminal(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	first := newProxyTestTransport()
	first.recv <- proxyRecv{frame: proxyWelcome(key.Name, 55, ports.CapabilityResume|ports.CapabilityProxied)}
	first.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	first.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
	second := newProxyTestTransport()
	factory := proxyFactoryFor(t, key, first, second)
	// The resume backoff is a ports.Clock deadline, so it is fired rather than
	// waited out. The buffer also absorbs the warm timer armed by replacement.
	clock := &signalClock{timers: make(chan *signalTimer, 8)}
	d := newProxyTestDaemon(t, factory, clock)

	proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	firstHello := requireProxyHello(t, first)
	require.Equal(t, ports.IntentAttach, firstHello.Intent)
	handshakeTimer := awaitTestValue(t, clock.timers, "proxy handshake timeout was not registered")
	require.Equal(t, proxyHandshakeTimeout, handshakeTimer.duration)

	first.recv <- proxyRecv{err: io.EOF}
	backoff := awaitTestValue(t, clock.timers, "proxy resume backoff timer was not armed")
	require.Equal(t, proxyResumeInitialBackoff, backoff.duration)
	backoff.ch <- time.Time{}
	resumeHello := requireProxyHello(t, second)
	require.Equal(t, ports.IntentResume, resumeHello.Intent)
	require.Equal(t, uint64(55), resumeHello.ResumeToken)
	require.Equal(t, firstHello.ClientID, resumeHello.ClientID)
	second.recv <- proxyRecv{frame: proxyWelcome(key.Name, 66, ports.CapabilityResume|ports.CapabilityProxied)}
	second.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	second.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
	second.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: ports.ReasonReplaced})}}

	select {
	case <-proxy.done:
	case <-time.After(time.Second):
		t.Fatal("replacement did not terminate proxy link loop")
	}
	proxy.mu.Lock()
	require.True(t, proxy.expired)
	require.Equal(t, uint64(66), proxy.resumeToken)
	proxy.mu.Unlock()
	require.Same(t, proxy, proxySessionSnapshot(d)[key.ID()], "replacement leaves the expired proxy available for local presentation")
}

func TestRunProxyTransportStopsReceivePumpAfterInvalidFrame(t *testing.T) {
	d, proxy, transport, generation := newProxyOutputSession(t)
	t.Cleanup(func() { _ = transport.Close() })
	transport.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgPong, Payload: ports.MarshalPong(ports.Pong{})}}
	transport.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte{1}}}
	transport.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgPong, Payload: ports.MarshalPong(ports.Pong{})}}

	result := d.runProxyTransport(context.Background(), proxy, generation, transport)
	require.Equal(t, proxyLinkStop, result)
	select {
	case <-transport.closed:
	default:
		t.Fatal("runProxyTransport returned without closing its receive pump transport")
	}
	require.Zero(t, transport.receiving.Load(), "receive pump outlived transport consumer")
}

func TestProxySenderSerializesAllTransportWrites(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	tr := newProxyTestTransportWithSendGate(gate, entered, 2)
	tr.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	tr.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	tr.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
	d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr), stubClock{})
	proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	_ = requireProxyHello(t, tr)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			errs <- proxy.sendCurrent(ports.Frame{Type: ports.MsgPing, Payload: ports.MarshalPing(ports.Ping{})})
		})
	}
	// Once the first sender enters Send, the others must remain behind sendMu.
	awaitTestCompletion(t, entered, "proxy sender did not enter transport")
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.False(t, tr.concurrent.Load(), "Transport.Send calls overlapped")
	stopProxy(t, proxy)
}

func TestProxyHandshakeRejectsMissingInitialScreenSnapshotWithoutPublication(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	tr := newProxyTestTransport()
	tr.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	tr.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	tr.recv <- proxyRecv{err: io.EOF}
	d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr), stubClock{})

	proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
	require.Error(t, err)
	require.Nil(t, proxy)
	require.Empty(t, proxySessionSnapshot(d))
}

func TestProxyHandshakeRejectsInvalidInitialMetadata(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	tr := newProxyTestTransport()
	tr.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	tr.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgSessionMeta, Payload: []byte{0xff}}}
	d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr), stubClock{})

	proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
	require.Error(t, err)
	require.Nil(t, proxy)
	require.Empty(t, proxySessionSnapshot(d))
}

func TestProxyOpenValidatesDependenciesAndKey(t *testing.T) {
	d := newProxyTestDaemon(t, nil, stubClock{})
	_, err := d.openProxySession(context.Background(), domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 80, Rows: 24})
	require.Error(t, err)

	factory := portsmocks.NewMockRemoteDialerFactory(t)
	d = newProxyTestDaemon(t, factory, stubClock{})
	_, err = d.openProxySession(context.Background(), domain.RemoteSessionKey{Host: "arch", Name: "bad:name"}, domain.Size{Cols: 80, Rows: 24})
	require.Error(t, err)
	require.Empty(t, proxySessionSnapshot(d))
}

func newProxyOutputSession(t *testing.T) (*Daemon, *proxySession, *proxyTestTransport, uint64) {
	t.Helper()
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	transport := newProxyTestTransport()
	generation, _ := proxy.installTransport(transport, false)
	return newProxyTestDaemon(t, nil, stubClock{}), proxy, transport, generation
}

func TestProxyScreenResumeAndFreshLinksRequireMovingSnapshots(t *testing.T) {
	d, proxy, first, generation := newProxyOutputSession(t)
	snapshot, err := ports.UnmarshalScreenUpdate(proxyHandshakeSnapshot().Payload)
	require.NoError(t, err)
	require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, snapshot))
	_ = awaitTestValue(t, first.sent, "initial screen was not acknowledged")
	proxy.mu.Lock()
	applied := proxy.appliedState
	proxy.mu.Unlock()

	second := newProxyTestTransport()
	generation, _ = proxy.installTransport(second, true)
	proxy.mu.Lock()
	require.Equal(t, applied, proxy.appliedState, "resume must preserve the applied state floor")
	require.False(t, proxy.screenReady, "resume must wait for its replacement snapshot")
	proxy.mu.Unlock()
	gap := snapshot
	gap.Kind = ports.ScreenUpdateDelta
	gap.BaseStateNum, gap.NewStateNum = applied, applied+1
	require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, gap))
	require.Equal(t, ports.MsgOutputResetRequest, awaitTestValue(t, second.sent, "resume delta did not request a reset").Type)
	require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, gap))
	select {
	case frame := <-second.sent:
		t.Fatalf("duplicate resume gap sent %v", frame.Type)
	default:
	}
	moving := snapshot
	moving.NewStateNum = applied + 1
	require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, moving))
	require.Equal(t, ports.MsgAck, awaitTestValue(t, second.sent, "resume snapshot was not acknowledged").Type)

	third := newProxyTestTransport()
	generation, _ = proxy.installTransport(third, false)
	proxy.mu.Lock()
	require.Zero(t, proxy.appliedState, "fresh link must clear the state floor")
	require.False(t, proxy.screenReady)
	proxy.mu.Unlock()
	require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, gap))
	require.Equal(t, ports.MsgOutputResetRequest, awaitTestValue(t, third.sent, "fresh delta did not request a reset").Type)
	fresh := snapshot
	fresh.NewStateNum = 1
	require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, fresh))
	require.Equal(t, ports.MsgAck, awaitTestValue(t, third.sent, "fresh state-one snapshot was not acknowledged").Type)
	proxy.mu.Lock()
	require.Equal(t, uint64(1), proxy.appliedState)
	proxy.mu.Unlock()
}

func TestProxyScreenStaleSnapshotsAndApplyFailuresRequestOneResetWithoutMutation(t *testing.T) {
	newProxy := func(t *testing.T) (*Daemon, *proxySession, uint64) {
		t.Helper()
		d, proxy, _, generation := newProxyOutputSession(t)
		return d, proxy, generation
	}

	t.Run("stale snapshot while not ready", func(t *testing.T) {
		d, proxy, generation := newProxy(t)
		snapshot := screenSnapshotForProxy(t, proxy, 7)
		require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, snapshot))
		proxy.mu.Lock()
		before := proxy.screen.frame.Clone()
		proxy.screenReady = false
		proxy.resetRequested = false
		proxy.mu.Unlock()

		stale := snapshot
		stale.NewStateNum = 7
		ack, reset, changed := proxy.applyScreenUpdateForGeneration(generation, stale)
		require.Zero(t, ack)
		require.True(t, reset, "a stale snapshot must request a reset while not ready")
		require.False(t, changed)
		proxy.mu.Lock()
		require.Equal(t, uint64(7), proxy.appliedState)
		require.False(t, proxy.screenReady)
		require.True(t, proxy.resetRequested)
		require.Equal(t, before, proxy.screen.frame)
		proxy.mu.Unlock()

		ack, reset, changed = proxy.applyScreenUpdateForGeneration(generation, stale)
		require.Zero(t, ack)
		require.False(t, reset, "an already-requested reset must be deduplicated")
		require.False(t, changed)
	})

	t.Run("snapshot apply failure", func(t *testing.T) {
		d, proxy, generation := newProxy(t)
		initial := screenSnapshotForProxy(t, proxy, 7)
		require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, initial))
		proxy.mu.Lock()
		before := proxy.screen.frame.Clone()
		proxy.mu.Unlock()
		invalid := initial
		invalid.NewStateNum = 8
		invalid.Spans = invalid.Spans[:len(invalid.Spans)-1]
		ack, reset, changed := proxy.applyScreenUpdateForGeneration(generation, invalid)
		require.Zero(t, ack)
		require.True(t, reset)
		require.False(t, changed)
		proxy.mu.Lock()
		require.Equal(t, uint64(7), proxy.appliedState)
		require.False(t, proxy.screenReady)
		require.True(t, proxy.resetRequested)
		require.Equal(t, before, proxy.screen.frame)
		proxy.mu.Unlock()
		_, reset, _ = proxy.applyScreenUpdateForGeneration(generation, invalid)
		require.False(t, reset, "snapshot Apply failure reset was not deduplicated")
	})

	t.Run("delta apply failure", func(t *testing.T) {
		d, proxy, generation := newProxy(t)
		initial := screenSnapshotForProxy(t, proxy, 7)
		require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, initial))
		proxy.mu.Lock()
		before := proxy.screen.frame.Clone()
		size := proxy.contentSize
		proxy.mu.Unlock()
		invalid := ports.ScreenUpdate{
			Kind: ports.ScreenUpdateDelta, BaseStateNum: 7, NewStateNum: 8, Size: size,
			Spans: []ports.ScreenSpan{{Y: uint16(size.Rows), Cells: []renderer.Cell{{Rune: 'x'}}}},
		}
		ack, reset, changed := proxy.applyScreenUpdateForGeneration(generation, invalid)
		require.Zero(t, ack)
		require.True(t, reset)
		require.False(t, changed)
		proxy.mu.Lock()
		require.Equal(t, uint64(7), proxy.appliedState)
		require.False(t, proxy.screenReady)
		require.True(t, proxy.resetRequested)
		require.Equal(t, before, proxy.screen.frame)
		proxy.mu.Unlock()
		_, reset, _ = proxy.applyScreenUpdateForGeneration(generation, invalid)
		require.False(t, reset, "delta Apply failure reset was not deduplicated")
	})
}

func screenSnapshotForProxy(t *testing.T, proxy *proxySession, state uint64) ports.ScreenUpdate {
	t.Helper()
	proxy.mu.Lock()
	size := proxy.contentSize
	proxy.mu.Unlock()
	snapshot := blankSnapshot(size)
	snapshot.NewStateNum = state
	return snapshot
}

func TestProxyScreenSnapshotReportsChanged(t *testing.T) {
	_, proxy, _, generation := newProxyOutputSession(t)
	snapshot := screenSnapshotForProxy(t, proxy, 1)
	ack, reset, changed := proxy.applyScreenUpdateForGeneration(generation, snapshot)
	require.Equal(t, uint64(1), ack)
	require.False(t, reset)
	require.True(t, changed, "every accepted snapshot invalidates the composed frame")
}

func TestProxyScreenUpdate(t *testing.T) {
	t.Run("initial snapshot", func(t *testing.T) {
		d, proxy, transport, generation := newProxyOutputSession(t)
		snapshot := screenSnapshotForProxy(t, proxy, 1)
		require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, snapshot))
		require.Equal(t, ports.MsgAck, awaitTestValue(t, transport.sent, "initial screen was not acknowledged").Type)
		proxy.mu.Lock()
		require.Equal(t, uint64(1), proxy.appliedState)
		require.True(t, proxy.screenReady)
		require.False(t, proxy.resetRequested)
		proxy.mu.Unlock()
	})

	t.Run("matching delta", func(t *testing.T) {
		d, proxy, transport, generation := newProxyOutputSession(t)
		snapshot := screenSnapshotForProxy(t, proxy, 1)
		require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, snapshot))
		_ = awaitTestValue(t, transport.sent, "initial screen was not acknowledged")

		delta := ports.ScreenUpdate{
			Kind: ports.ScreenUpdateDelta, BaseStateNum: 1, NewStateNum: 2,
			Size: snapshot.Size, Spans: []ports.ScreenSpan{{Y: 0, Cells: cells("delta")}},
		}
		require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, delta))
		require.Equal(t, ports.MsgAck, awaitTestValue(t, transport.sent, "screen delta was not acknowledged").Type)
		proxy.mu.Lock()
		require.Equal(t, uint64(2), proxy.appliedState)
		require.Equal(t, 'd', proxy.screen.frame.At(0, 0).Rune)
		proxy.mu.Unlock()
	})

	for _, tc := range []struct {
		name string
		base uint64
		new  uint64
	}{
		{name: "duplicate", base: 0, new: 1},
		{name: "gap", base: 3, new: 4},
	} {
		t.Run(tc.name+" requests exactly one reset", func(t *testing.T) {
			d, proxy, transport, generation := newProxyOutputSession(t)
			snapshot := screenSnapshotForProxy(t, proxy, 1)
			require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, snapshot))
			_ = awaitTestValue(t, transport.sent, "initial screen was not acknowledged")

			update := ports.ScreenUpdate{
				Kind: ports.ScreenUpdateDelta, BaseStateNum: tc.base, NewStateNum: tc.new,
				Size: snapshot.Size,
			}
			require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, update))
			require.Equal(t, ports.MsgOutputResetRequest, awaitTestValue(t, transport.sent, "screen mismatch did not request reset").Type)
			require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, update))
			select {
			case frame := <-transport.sent:
				t.Fatalf("duplicate %s sent %v", tc.name, frame.Type)
			default:
			}
			proxy.mu.Lock()
			require.Equal(t, uint64(1), proxy.appliedState)
			require.False(t, proxy.screenReady)
			require.True(t, proxy.resetRequested)
			proxy.mu.Unlock()
		})
	}

	t.Run("awaited base-zero snapshot advances the floor", func(t *testing.T) {
		d, proxy, transport, generation := newProxyOutputSession(t)
		initial := screenSnapshotForProxy(t, proxy, 5)
		require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, initial))
		_ = awaitTestValue(t, transport.sent, "initial screen was not acknowledged")

		gap := ports.ScreenUpdate{Kind: ports.ScreenUpdateDelta, BaseStateNum: 9, NewStateNum: 10, Size: initial.Size}
		require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, gap))
		require.Equal(t, ports.MsgOutputResetRequest, awaitTestValue(t, transport.sent, "screen gap did not request reset").Type)

		moving := initial
		moving.NewStateNum = 6
		require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, moving))
		require.Equal(t, ports.MsgAck, awaitTestValue(t, transport.sent, "awaited snapshot was not acknowledged").Type)
		proxy.mu.Lock()
		require.Equal(t, uint64(6), proxy.appliedState)
		require.True(t, proxy.screenReady)
		require.False(t, proxy.resetRequested)
		proxy.mu.Unlock()
	})

	t.Run("stale generation is ignored", func(t *testing.T) {
		_, proxy, transport, oldGeneration := newProxyOutputSession(t)
		replacement := newProxyTestTransport()
		newGeneration, _ := proxy.installTransport(replacement, false)
		require.NotEqual(t, oldGeneration, newGeneration)
		ack, reset, changed := proxy.applyScreenUpdateForGeneration(oldGeneration, screenSnapshotForProxy(t, proxy, 1))
		require.Zero(t, ack)
		require.False(t, reset)
		require.False(t, changed)
		proxy.mu.Lock()
		require.Zero(t, proxy.appliedState)
		proxy.mu.Unlock()
		select {
		case frame := <-transport.sent:
			t.Fatalf("stale screen update sent %v", frame.Type)
		default:
		}
	})

	t.Run("ACK and reset send failures are resumable", func(t *testing.T) {
		t.Run("ACK", func(t *testing.T) {
			d, proxy, transport, generation := newProxyOutputSession(t)
			transport.sendFails.Store(true)
			snapshot := screenSnapshotForProxy(t, proxy, 1)
			payload, err := ports.MarshalScreenUpdate(snapshot)
			require.NoError(t, err)
			transport.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgScreenUpdate, Payload: payload}}
			require.Equal(t, proxyLinkResume, d.runProxyTransport(context.Background(), proxy, generation, transport))
		})

		t.Run("reset", func(t *testing.T) {
			d, proxy, transport, generation := newProxyOutputSession(t)
			transport.sendFails.Store(true)
			proxy.mu.Lock()
			size := proxy.contentSize
			proxy.mu.Unlock()
			gap := ports.ScreenUpdate{Kind: ports.ScreenUpdateDelta, BaseStateNum: 1, NewStateNum: 2, Size: size}
			payload, err := ports.MarshalScreenUpdate(gap)
			require.NoError(t, err)
			transport.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgScreenUpdate, Payload: payload}}
			require.Equal(t, proxyLinkResume, d.runProxyTransport(context.Background(), proxy, generation, transport))
		})
	})

	t.Run("blocked send releases proxy.mu", func(t *testing.T) {
		d, proxy, transport, generation := newProxyOutputSession(t)
		gate := make(chan struct{})
		entered := make(chan struct{}, 1)
		transport.sendGate = gate
		transport.sendEntered = entered
		snapshot := screenSnapshotForProxy(t, proxy, 1)
		result := make(chan error, 1)
		go func() {
			result <- d.handleProxyScreenUpdate(proxy, generation, snapshot)
		}()
		awaitTestCompletion(t, entered, "screen ACK sender did not enter transport")

		unlocked := make(chan ports.Transport, 1)
		go func() {
			proxy.mu.Lock()
			currentTransport := proxy.transport
			proxy.mu.Unlock()
			unlocked <- currentTransport
		}()
		var currentTransport ports.Transport
		select {
		case currentTransport = <-unlocked:
		case <-time.After(time.Second):
			t.Fatal("proxy.mu was held while screen send was blocked")
		}
		require.Same(t, transport, currentTransport)
		require.NoError(t, transport.Close())
		require.Error(t, awaitTestValue(t, result, "blocked screen send did not return"))
	})
}

// TestProxyLinkProtocolErrorStillStops fences malformed MsgOutput rejection
// separately from resumable structured screen reply failures.
func TestProxyLinkProtocolErrorStillStops(t *testing.T) {
	d, proxy, transport, generation := newProxyOutputSession(t)
	t.Cleanup(func() { _ = transport.Close() })
	transport.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte{1}}}

	require.Equal(t, proxyLinkStop, d.runProxyTransport(context.Background(), proxy, generation, transport))
}

func proxySideEffectFrame(data []byte) ports.Frame {
	return ports.Frame{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{
		BaseStateNum: 0,
		NewStateNum:  0,
		Data:         data,
	})}
}

func TestProxySideEffectForwardsOSC52ToCurrentThinClient(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	remote := newProxyTestTransport()
	thin := newProxyTestTransport()
	proxy, ac := newAttachedProxyFixture(t, d, thin, remote)
	ac.echoAck.Store(17)

	const osc52 = "\x1b]52;c;YQ==\a"
	result, err := d.handleLinkFrame(proxy, 1, proxySideEffectFrame([]byte(osc52)))
	require.Equal(t, proxyLinkResume, result)
	require.NoError(t, err)

	forwarded := awaitTestValue(t, thin.sent, "proxy OSC52 side effect was not forwarded")
	require.Equal(t, ports.MsgOutput, forwarded.Type)
	out, err := ports.UnmarshalOutput(forwarded.Payload)
	require.NoError(t, err)
	require.Zero(t, out.BaseStateNum)
	require.Zero(t, out.NewStateNum)
	require.Equal(t, uint64(17), out.EchoAck)
	require.Equal(t, []byte(osc52), out.Data)
	select {
	case frame := <-thin.sent:
		t.Fatalf("proxy OSC52 side effect forwarded more than once: %v", frame.Type)
	default:
	}
	proxy.mu.Lock()
	require.Zero(t, proxy.appliedState)
	require.False(t, proxy.resetRequested)
	require.True(t, proxy.screenReady, "side effect must not alter proxy screen readiness")
	proxy.mu.Unlock()
	select {
	case frame := <-remote.sent:
		t.Fatalf("side effect unexpectedly replied on proxy link: %v", frame.Type)
	default:
	}
}

func TestProxySideEffectRejectsStateBearingAndMalformedMsgOutput(t *testing.T) {
	tests := []struct {
		name  string
		frame ports.Frame
	}{
		{name: "state bearing", frame: ports.Frame{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("screen")})}},
		{name: "malformed", frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte{1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			remote := newProxyTestTransport()
			thin := newProxyTestTransport()
			proxy, _ := newAttachedProxyFixture(t, d, thin, remote)

			result, err := d.handleLinkFrame(proxy, 1, tt.frame)
			require.Equal(t, proxyLinkStop, result)
			require.Error(t, err)
			select {
			case frame := <-thin.sent:
				t.Fatalf("invalid proxy output forwarded: %v", frame.Type)
			default:
			}
		})
	}
}

func TestProxySideEffectDropsStaleGenerationAndRole(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	remote := newProxyTestTransport()
	thin := newProxyTestTransport()
	proxy, ac := newAttachedProxyFixture(t, d, thin, remote)
	frame := proxySideEffectFrame([]byte("effect"))

	result, err := d.handleLinkFrame(proxy, 0, frame)
	require.Equal(t, proxyLinkResume, result)
	require.NoError(t, err)
	select {
	case forwarded := <-thin.sent:
		t.Fatalf("stale proxy generation forwarded side effect: %v", forwarded.Type)
	default:
	}

	token := attachmentToken(proxy, ac, ac.transport())
	ticket, admitted := ac.beginRoleEffect(token)
	require.True(t, admitted)
	ticket.End()
	ac.roleGeneration.Add(1)
	result, err = d.handleLinkFrame(proxy, 1, frame)
	require.Equal(t, proxyLinkResume, result)
	require.NoError(t, err)
	select {
	case forwarded := <-thin.sent:
		t.Fatalf("stale attachment role forwarded side effect: %v", forwarded.Type)
	default:
	}
}

func TestProxySideEffectSendFailureResumesLink(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	remote := newProxyTestTransport()
	thin := newProxyTestTransport()
	thin.sendFails.Store(true)
	proxy, _ := newAttachedProxyFixture(t, d, thin, remote)
	err := d.handleProxySideEffect(proxy, 1, ports.Output{Data: []byte("effect")})
	require.ErrorContains(t, err, "link reply send failed")
	remote.recv <- proxyRecv{frame: proxySideEffectFrame([]byte("effect"))}

	require.Equal(t, proxyLinkResume, d.runProxyTransport(context.Background(), proxy, 1, remote))
}
