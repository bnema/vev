package daemon

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

func newProxyTestDaemon(t *testing.T, factory ports.RemoteDialerFactory) *Daemon {
	t.Helper()
	return New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithRemoteDiscovery(nil, nil, nil, factory, ports.RemoteTransportUDP))
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
	d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr))

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
	require.Equal(t, domain.Size{Cols: 80, Rows: 22}, hello.Size)
	require.Equal(t, uint8(maxUnackedOutputStates), hello.MaxOutputInFlight)
	require.Empty(t, d.sessions, "Hello alone must not publish a proxy")

	tr.recv <- proxyRecv{frame: proxyWelcome(key.Name, 44, ports.CapabilityResume|ports.CapabilityProxied)}
	require.Empty(t, d.sessions, "Welcome alone must not publish a proxy")
	tr.recv <- proxyRecv{frame: proxyMeta(key.Name)}

	created := awaitTestValue(t, result, "proxy handshake did not complete")
	require.NoError(t, created.err)
	require.Same(t, created.proxy, d.sessions[key.ID()])
	require.Equal(t, key, created.proxy.key)
	require.Equal(t, uint64(44), created.proxy.resumeToken)
	stopProxy(t, created.proxy)
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
			d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr))

			proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
			require.Error(t, err)
			require.Nil(t, proxy)
			require.Empty(t, d.sessions)
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
	secondTransport := newProxyTestTransport()
	secondTransport.recv <- proxyRecv{frame: proxyWelcome(secondKey.Name, 2, ports.CapabilityProxied)}
	secondTransport.recv <- proxyRecv{frame: proxyMeta(secondKey.Name)}
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	for key, transport := range map[domain.RemoteSessionKey]ports.Transport{firstKey: firstTransport, secondKey: secondTransport} {
		dialer := portsmocks.NewMockDialer(t)
		factory.EXPECT().DialerForRemote(key.Host, key.Name, ports.RemoteTransportUDP, mock.Anything).Return(dialer, nil).Once()
		dialer.EXPECT().Dial(mock.Anything).Return(transport, nil).Once()
	}
	d := newProxyTestDaemon(t, factory)

	first, err := d.openProxySession(context.Background(), firstKey, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	deduped, err := d.openProxySession(context.Background(), firstKey, domain.Size{Cols: 100, Rows: 30})
	require.NoError(t, err)
	require.Same(t, first, deduped)
	second, err := d.openProxySession(context.Background(), secondKey, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	require.NotSame(t, first, second)
	require.Len(t, d.sessions, 2)

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
		d := newProxyTestDaemon(t, factory)
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
		require.Empty(t, d.sessions)
	})

	t.Run("handshake", func(t *testing.T) {
		key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
		tr := newProxyTestTransport()
		d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr))
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := d.openProxySession(ctx, key, domain.Size{Cols: 80, Rows: 24})
			result <- err
		}()
		requireProxyHello(t, tr)
		cancel()
		require.ErrorIs(t, awaitTestValue(t, result, "canceled proxy handshake did not finish"), context.Canceled)
		require.Empty(t, d.sessions)
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
	second := newProxyTestTransport()
	factory := proxyFactoryFor(t, key, first, second)
	d := newProxyTestDaemon(t, factory)

	proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	firstHello := requireProxyHello(t, first)
	require.Equal(t, ports.IntentAttach, firstHello.Intent)

	first.recv <- proxyRecv{err: io.EOF}
	resumeHello := requireProxyHello(t, second)
	require.Equal(t, ports.IntentResume, resumeHello.Intent)
	require.Equal(t, uint64(55), resumeHello.ResumeToken)
	require.Equal(t, firstHello.ClientID, resumeHello.ClientID)
	second.recv <- proxyRecv{frame: proxyWelcome(key.Name, 66, ports.CapabilityResume|ports.CapabilityProxied)}
	second.recv <- proxyRecv{frame: proxyMeta(key.Name)}
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
	require.Same(t, proxy, d.sessions[key.ID()], "replacement leaves the expired proxy available for local presentation")
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
	tr := newProxyTestTransportWithSendGate(gate, entered, 1)
	tr.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	tr.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr))
	proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	_ = requireProxyHello(t, tr)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- proxy.sendCurrent(ports.Frame{Type: ports.MsgPing, Payload: ports.MarshalPing(ports.Ping{})})
		}()
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

func TestProxyHandshakeRejectsInvalidInitialMetadata(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	tr := newProxyTestTransport()
	tr.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	tr.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgSessionMeta, Payload: []byte{0xff}}}
	d := newProxyTestDaemon(t, proxyFactoryFor(t, key, tr))

	proxy, err := d.openProxySession(context.Background(), key, domain.Size{Cols: 80, Rows: 24})
	require.Error(t, err)
	require.Nil(t, proxy)
	require.Empty(t, d.sessions)
}

func TestProxyOpenValidatesDependenciesAndKey(t *testing.T) {
	d := newProxyTestDaemon(t, nil)
	_, err := d.openProxySession(context.Background(), domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 80, Rows: 24})
	require.Error(t, err)

	factory := portsmocks.NewMockRemoteDialerFactory(t)
	d = newProxyTestDaemon(t, factory)
	_, err = d.openProxySession(context.Background(), domain.RemoteSessionKey{Host: "arch", Name: "bad:name"}, domain.Size{Cols: 80, Rows: 24})
	require.Error(t, err)
	require.Empty(t, d.sessions)
}

func newProxyOutputSession(t *testing.T) (*Daemon, *proxySession, *proxyTestTransport, uint64) {
	t.Helper()
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	transport := newProxyTestTransport()
	generation, _ := proxy.installTransport(transport)
	return newProxyTestDaemon(t, nil), proxy, transport, generation
}

func proxyScreenText(p *proxySession) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return string([]rune{p.screen.Frame.At(0, 0).Rune, p.screen.Frame.At(1, 0).Rune})
}

func proxyOutputState(p *proxySession) (uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.appliedState, p.awaitingReset
}

func TestProxyOutputStateMachine(t *testing.T) {
	tests := []struct {
		name         string
		initial      *ports.Output
		output       ports.Output
		wantAck      uint64
		wantReset    bool
		wantChanged  bool
		wantState    uint64
		wantAwaiting bool
		wantScreen   string
	}{
		{
			name:        "base zero initial reset",
			output:      ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("A")},
			wantAck:     1,
			wantChanged: true,
			wantState:   1,
			wantScreen:  "A ",
		},
		{
			name:        "matching increment",
			initial:     &ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("A")},
			output:      ports.Output{BaseStateNum: 1, NewStateNum: 2, Data: []byte("B")},
			wantAck:     2,
			wantChanged: true,
			wantState:   2,
			wantScreen:  "AB",
		},
		{
			name:         "duplicate old state requests reset",
			initial:      &ports.Output{BaseStateNum: 0, NewStateNum: 2, Data: []byte("A")},
			output:       ports.Output{BaseStateNum: 1, NewStateNum: 2, Data: []byte("B")},
			wantReset:    true,
			wantState:    2,
			wantAwaiting: true,
			wantScreen:   "A ",
		},
		{
			name:         "gap requests reset",
			initial:      &ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("A")},
			output:       ports.Output{BaseStateNum: 3, NewStateNum: 4, Data: []byte("B")},
			wantReset:    true,
			wantState:    1,
			wantAwaiting: true,
			wantScreen:   "A ",
		},
		{
			name:        "stateless side effect does not advance state",
			output:      ports.Output{NewStateNum: 0, Data: []byte("!")},
			wantChanged: true,
			wantScreen:  "! ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, proxy, _, generation := newProxyOutputSession(t)
			if tt.initial != nil {
				_, _, _ = proxy.applyOutputForGeneration(generation, *tt.initial)
			}

			ack, reset, changed := proxy.applyOutput(tt.output)
			require.Equal(t, tt.wantAck, ack)
			require.Equal(t, tt.wantReset, reset)
			require.Equal(t, tt.wantChanged, changed)
			state, awaiting := proxyOutputState(proxy)
			require.Equal(t, tt.wantState, state)
			require.Equal(t, tt.wantAwaiting, awaiting)
			require.Equal(t, tt.wantScreen, proxyScreenText(proxy))
		})
	}
}

func TestProxyOutputMismatchSendsOneResetUntilBaseZero(t *testing.T) {
	d, proxy, transport, generation := newProxyOutputSession(t)
	require.NoError(t, d.handleProxyOutput(proxy, generation, ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("A")}))
	require.Equal(t, ports.MsgAck, awaitTestValue(t, transport.sent, "initial output was not acknowledged").Type)

	mismatch := ports.Output{BaseStateNum: 3, NewStateNum: 4, Data: []byte("discard")}
	require.NoError(t, d.handleProxyOutput(proxy, generation, mismatch))
	reset := awaitTestValue(t, transport.sent, "output mismatch did not request reset")
	require.Equal(t, ports.MsgOutputResetRequest, reset.Type)
	require.NoError(t, d.handleProxyOutput(proxy, generation, mismatch))
	select {
	case frame := <-transport.sent:
		t.Fatalf("second mismatch sent %v, want no frame", frame.Type)
	default:
	}

	require.NoError(t, d.handleProxyOutput(proxy, generation, ports.Output{BaseStateNum: 0, NewStateNum: 5, Data: []byte("R")}))
	ack := awaitTestValue(t, transport.sent, "reset output was not acknowledged")
	require.Equal(t, ports.MsgAck, ack.Type)
	state, awaiting := proxyOutputState(proxy)
	require.Equal(t, uint64(5), state)
	require.False(t, awaiting)
	require.Equal(t, "R ", proxyScreenText(proxy))
}

func TestProxyOutputMalformedOrStaleCannotAdvanceState(t *testing.T) {
	t.Run("malformed", func(t *testing.T) {
		d, proxy, _, generation := newProxyOutputSession(t)
		result, err := d.handleLinkFrame(proxy, generation, ports.Frame{Type: ports.MsgOutput, Payload: []byte{1}})
		require.Equal(t, proxyLinkStop, result)
		require.Error(t, err)
		state, awaiting := proxyOutputState(proxy)
		require.Zero(t, state)
		require.False(t, awaiting)
	})

	t.Run("stale generation", func(t *testing.T) {
		d, proxy, transport, oldGeneration := newProxyOutputSession(t)
		newGeneration, _ := proxy.installTransport(newProxyTestTransport())
		require.NotEqual(t, oldGeneration, newGeneration)
		require.NoError(t, d.handleProxyOutput(proxy, oldGeneration, ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("A")}))
		state, awaiting := proxyOutputState(proxy)
		require.Zero(t, state)
		require.False(t, awaiting)
		require.Equal(t, "  ", proxyScreenText(proxy))
		select {
		case frame := <-transport.sent:
			t.Fatalf("stale output sent %v", frame.Type)
		default:
		}
	})
}

func TestProxyOutputAckFailureStillDoesNotApplyStaleState(t *testing.T) {
	d, proxy, transport, generation := newProxyOutputSession(t)
	transport.sendGate = make(chan struct{})
	require.NoError(t, transport.Close())
	err := d.handleProxyOutput(proxy, generation, ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("A")})
	require.Error(t, err)
	state, _ := proxyOutputState(proxy)
	require.Equal(t, uint64(1), state, "ACK failure follows a successfully applied state")
}

func TestProxyOutputReconnectRequiresBaseZero(t *testing.T) {
	d, proxy, first, generation := newProxyOutputSession(t)
	require.NoError(t, d.handleProxyOutput(proxy, generation, ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("A")}))
	_ = awaitTestValue(t, first.sent, "initial output was not acknowledged") // ACK

	second := newProxyTestTransport()
	generation, _ = proxy.installTransport(second)
	require.NoError(t, d.handleProxyOutput(proxy, generation, ports.Output{BaseStateNum: 1, NewStateNum: 2, Data: []byte("B")}))
	frame := awaitTestValue(t, second.sent, "reconnected output did not request reset")
	require.Equal(t, ports.MsgOutputResetRequest, frame.Type)
	state, awaiting := proxyOutputState(proxy)
	require.Zero(t, state)
	require.True(t, awaiting)
	require.Equal(t, "A ", proxyScreenText(proxy))

	require.NoError(t, d.handleProxyOutput(proxy, generation, ports.Output{BaseStateNum: 0, NewStateNum: 7, Data: []byte("R")}))
	frame = awaitTestValue(t, second.sent, "reset reconnected output was not acknowledged")
	require.Equal(t, ports.MsgAck, frame.Type)
	state, awaiting = proxyOutputState(proxy)
	require.Equal(t, uint64(7), state)
	require.False(t, awaiting)
}

func TestProxyOutputResizeRequiresBaseZero(t *testing.T) {
	d, proxy, transport, generation := newProxyOutputSession(t)
	require.NoError(t, d.handleProxyOutput(proxy, generation, ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("A")}))
	_ = awaitTestValue(t, transport.sent, "initial output was not acknowledged") // ACK

	proxy.resetOutputState()
	require.NoError(t, d.handleProxyOutput(proxy, generation, ports.Output{BaseStateNum: 1, NewStateNum: 2, Data: []byte("B")}))
	frame := awaitTestValue(t, transport.sent, "resize mismatch did not request reset")
	require.Equal(t, ports.MsgOutputResetRequest, frame.Type)
	state, awaiting := proxyOutputState(proxy)
	require.Zero(t, state)
	require.True(t, awaiting)
	require.Equal(t, "A ", proxyScreenText(proxy))
}

func TestProxyOutputConcurrentCancelDoesNotHoldProxyLockDuringSend(t *testing.T) {
	d, proxy, transport, generation := newProxyOutputSession(t)
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	transport.sendGate = gate
	transport.sendEntered = entered

	result := make(chan error, 1)
	go func() {
		result <- d.handleProxyOutput(proxy, generation, ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("A")})
	}()
	awaitTestCompletion(t, entered, "output sender did not enter transport")

	locked := make(chan struct{})
	go func() {
		_, _ = proxyOutputState(proxy)
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("proxy.mu was held while transport Send was blocked")
	}
	require.NoError(t, transport.Close())
	require.Error(t, awaitTestValue(t, result, "blocked output send did not return"))
}
