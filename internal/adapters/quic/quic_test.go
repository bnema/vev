package quic

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
	quicgo "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

func TestQUICEnvelopeRoundTrip(t *testing.T) {
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	require.Len(t, fingerprint, 32)

	listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 4)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	accepted := make(chan wire.Transport, 1)
	go func() {
		transport, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- transport
	}()

	dialer := DialConfig(listener.Addr(), "vev-bootstrap", fingerprint, Config{}, 10*time.Second)
	client, err := dialer.Dial(context.Background())
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	select {
	case server := <-accepted:
		defer func() { _ = server.Close() }()
		payload := []byte("quic envelope stays exact")
		require.NoError(t, client.Send(wire.Envelope{Payload: payload}))
		got, err := server.Recv()
		require.NoError(t, err)
		require.Equal(t, payload, got.Payload)
		require.NoError(t, server.Send(wire.Envelope{Payload: payload}))
		echo, err := client.Recv()
		require.NoError(t, err)
		require.Equal(t, payload, echo.Payload)
	case <-time.After(10 * time.Second):
		t.Fatal("listener did not accept")
	}
}

func TestQUICWrongFingerprintFails(t *testing.T) {
	cert, _, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 4)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		_, _ = listener.Accept()
	}()

	wrong := make([]byte, 32)
	dialer := DialConfig(listener.Addr(), "vev-bootstrap", wrong, Config{}, 5*time.Second)
	_, err = dialer.Dial(context.Background())
	require.Error(t, err)
}

func TestQUICConfigUsesTunnelSafeInitialPacketSize(t *testing.T) {
	for _, config := range []*quicgo.Config{clientQUICConfig(Config{}), serverQUICConfig(Config{})} {
		require.Equal(t, uint16(1200), config.InitialPacketSize)
	}
}

func TestQUICConfigValidation(t *testing.T) {
	cert, _, err := GenerateEphemeralCert()
	require.NoError(t, err)
	_, err = ListenConfig("", cert, Config{KeepAlivePeriod: -time.Second}, 4)
	require.Error(t, err)
	_, err = ListenConfig("", cert, Config{MaxIncomingStreams: -1}, 4)
	require.Error(t, err)
	_, err = ListenConfig("", cert, Config{}, -1)
	require.Error(t, err)
	// Empty addr selects loopback, never the wildcard.
	listener, err := ListenConfig("", cert, Config{}, 4)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	require.Contains(t, listener.Addr(), "127.0.0.1")
}

// TestQUICOrderlyCloseDeliversFinalEnvelope is the regression guard for an
// orderly close discarding the envelope written immediately before it: the
// send side must be FINed, not RESET_STREAMed, so the peer still receives the
// final envelope before observing EOF.
func TestQUICOrderlyCloseDeliversFinalEnvelope(t *testing.T) {
	server, client := dialTestPairWithConfig(t, Config{})
	payload := []byte("final envelope survives an orderly close")
	require.NoError(t, server.Send(wire.Envelope{Payload: payload}))
	require.NoError(t, server.Close())

	got, err := client.Recv()
	require.NoError(t, err, "an orderly close must not discard the final envelope")
	require.Equal(t, payload, got.Payload)
	_, err = client.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestQUICResolveAddrsExpandsLiteralsAndHonorsContext(t *testing.T) {
	addrs, err := resolveQUICAddrs(context.Background(), "127.0.0.1:4433")
	require.NoError(t, err)
	require.Len(t, addrs, 1)
	require.Equal(t, 4433, addrs[0].Port)
	require.True(t, addrs[0].IP.Equal(net.ParseIP("127.0.0.1")))

	addrs, err = resolveQUICAddrs(context.Background(), "[::1]:4433")
	require.NoError(t, err)
	require.Len(t, addrs, 1)
	require.True(t, addrs[0].IP.Equal(net.ParseIP("::1")))

	_, err = resolveQUICAddrs(context.Background(), "127.0.0.1:notaport")
	require.Error(t, err)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = resolveQUICAddrs(canceled, "definitely-not-a-real-host.invalid:4433")
	require.Error(t, err, "resolution must be bounded by the dial context")
}

// TestQUICDialFallsBackAcrossResolvedAddresses proves every resolved address
// is attempted under the one shared dial deadline: a black-holed first
// candidate must not prevent the reachable second from being dialed.
func TestQUICDialFallsBackAcrossResolvedAddresses(t *testing.T) {
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 4)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	accepted := make(chan wire.Transport, 1)
	go func() {
		if transport, err := listener.Accept(); err == nil {
			accepted <- transport
		}
	}()
	_, portText, err := net.SplitHostPort(listener.Addr())
	require.NoError(t, err)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)

	dialer := DialConfig("placeholder:1", "vev-bootstrap", fingerprint, Config{}, 4*time.Second)
	dialer.resolve = func(context.Context, string) ([]*net.UDPAddr, error) {
		return []*net.UDPAddr{
			{IP: net.ParseIP("127.0.0.1"), Port: 1}, // nothing listens here
			{IP: net.ParseIP("127.0.0.1"), Port: port},
		}, nil
	}
	client, err := dialer.Dial(context.Background())
	require.NoError(t, err, "the dialer must fail over to a later resolved address")
	defer func() { _ = client.Close() }()

	select {
	case server := <-accepted:
		defer func() { _ = server.Close() }()
		payload := []byte("second address")
		require.NoError(t, client.Send(wire.Envelope{Payload: payload}))
		got, err := server.Recv()
		require.NoError(t, err)
		require.Equal(t, payload, got.Payload)
	case <-time.After(10 * time.Second):
		t.Fatal("fallback address was not dialed")
	}
}

type markRecorder struct {
	mu    sync.Mutex
	marks []ports.RuntimeMark
}

func (r *markRecorder) ObserveRuntime(mark ports.RuntimeMark) {
	r.mu.Lock()
	r.marks = append(r.marks, mark)
	r.mu.Unlock()
}

func (r *markRecorder) Flush() {}
func (r *markRecorder) Close() {}

// TestQUICDialObserverEmitsAdapterMarks proves the observer threaded through
// DialConfig reaches the dialed transport and observes its raw carriage.
func TestQUICDialObserverEmitsAdapterMarks(t *testing.T) {
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 4)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	accepted := make(chan wire.Transport, 1)
	go func() {
		if transport, err := listener.Accept(); err == nil {
			accepted <- transport
		}
	}()

	recorder := &markRecorder{}
	client, err := DialConfig(listener.Addr(), "vev-bootstrap", fingerprint, Config{}, 10*time.Second, WithRuntimeObserver(recorder)).Dial(context.Background())
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	select {
	case server := <-accepted:
		defer func() { _ = server.Close() }()
		require.NoError(t, client.Send(wire.Envelope{Payload: []byte("observed")}))
		_, err = server.Recv()
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("listener did not accept")
	}

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	require.NotEmpty(t, recorder.marks, "observer must receive marks from the QUIC transport")
	for _, mark := range recorder.marks {
		require.Equal(t, "quic", mark.Component)
	}
}

func TestQUICTransportCloseReportsEOF(t *testing.T) {
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 4)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	accepted := make(chan wire.Transport, 1)
	go func() {
		transport, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- transport
	}()
	dialer := DialConfig(listener.Addr(), "vev-bootstrap", fingerprint, Config{}, 10*time.Second)
	client, err := dialer.Dial(context.Background())
	require.NoError(t, err)

	select {
	case server := <-accepted:
		// A graceful peer close reads as EOF, like IPC/SSH.
		require.NoError(t, server.Close())
		_, err = client.Recv()
		require.ErrorIs(t, err, io.EOF)
		_ = client.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("listener did not accept")
		_ = client.Close()
	}
}

func TestQUICProbeSkippedOnce(t *testing.T) {
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 4)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	accepted := make(chan wire.Transport, 1)
	go func() {
		transport, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- transport
	}()
	dialer := DialConfig(listener.Addr(), "vev-bootstrap", fingerprint, Config{}, 10*time.Second)
	client, err := dialer.Dial(context.Background())
	require.NoError(t, err)

	select {
	case server := <-accepted:
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()
		// The dial probe is skipped silently; the next Recv blocks
		// for real traffic, so close unblocks it with an error.
		received := make(chan error, 1)
		go func() {
			_, err := server.Recv()
			received <- err
		}()
		require.NoError(t, client.Close())
		select {
		case err := <-received:
			require.Error(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("server Recv was not unblocked")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("listener did not accept")
		_ = client.Close()
	}
}

func TestQUICCloseUnblocksRecv(t *testing.T) {
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 4)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	accepted := make(chan wire.Transport, 1)
	go func() {
		transport, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- transport
	}()
	dialer := DialConfig(listener.Addr(), "vev-bootstrap", fingerprint, Config{}, 10*time.Second)
	client, err := dialer.Dial(context.Background())
	require.NoError(t, err)

	select {
	case server := <-accepted:
		received := make(chan error, 1)
		go func() {
			_, err := server.Recv()
			received <- err
		}()
		require.NoError(t, client.Close())
		select {
		case err := <-received:
			require.Error(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("server Recv was not unblocked by client close")
		}
		_ = server.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("listener did not accept")
		_ = client.Close()
	}
}

// dialTestPairWithConfig returns the server and client ends of one live
// transport whose listener uses the supplied Config, both enforcing the
// one-stream contract.
func dialTestPairWithConfig(t *testing.T, config Config) (*Transport, *Transport) {
	t.Helper()
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, config, 4)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan *Transport, 1)
	go func() {
		transport, err := listener.Accept()
		if err == nil {
			accepted <- transport.(*Transport)
		}
	}()
	client, err := DialConfig(listener.Addr(), "vev-bootstrap", fingerprint, Config{}, 10*time.Second).Dial(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	select {
	case server := <-accepted:
		t.Cleanup(func() { _ = server.Close() })
		return server, client.(*Transport)
	case <-time.After(10 * time.Second):
		t.Fatal("listener did not accept")
		return nil, nil
	}
}

// TestQUICUniStreamsTrulyDisabled proves the zero Config keeps quic-go's
// 100-stream library default from leaking in: without a negative limit the
// peer could open unidirectional streams.
func TestQUICUniStreamsTrulyDisabled(t *testing.T) {
	_, client := dialTestPairWithConfig(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := client.conn.OpenUniStreamSync(ctx)
	require.Error(t, err, "unidirectional streams must be truly disabled, not quic-go's 100-stream default")
}

// TestQUICServerCannotOpenStreams proves role-forbidden streams are truly
// disabled on the dialer side: the server never opens streams toward the
// client.
func TestQUICServerCannotOpenStreams(t *testing.T) {
	server, _ := dialTestPairWithConfig(t, Config{})
	openers := map[string]func(context.Context) error{
		"bidi": func(ctx context.Context) error {
			_, err := server.conn.OpenStreamSync(ctx)
			return err
		},
		"uni": func(ctx context.Context) error {
			_, err := server.conn.OpenUniStreamSync(ctx)
			return err
		},
	}
	for name, open := range openers {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			require.Error(t, open(ctx), "the server role must not open streams toward the dialer")
		})
	}
}

// TestQUICDelayedExtraStreamEnforced proves the one-stream contract holds
// for the whole connection lifetime, not just a handshake window: a second
// stream opened well after setup fails the live transport instead of being
// silently tolerated. The listener's explicit limit of two makes the second
// stream admissible at the QUIC layer so the test exercises the lifetime
// guard, not flow control.
func TestQUICDelayedExtraStreamEnforced(t *testing.T) {
	server, client := dialTestPairWithConfig(t, Config{MaxIncomingStreams: 2})
	// Land the violation long after the handshake has settled.
	time.Sleep(200 * time.Millisecond)
	openCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	extra, err := client.conn.OpenStreamSync(openCtx)
	require.NoError(t, err, "second stream is admissible under the explicit limit")
	defer func() { _ = extra.Close() }()
	// QUIC has no stream-open frame: the peer only learns about the stream
	// when a frame referencing it arrives.
	_, err = extra.Write([]byte("violation"))
	require.NoError(t, err)

	recvErr := make(chan error, 1)
	go func() {
		_, err := server.Recv()
		recvErr <- err
	}()
	select {
	case err := <-recvErr:
		require.Error(t, err, "delayed extra stream must fail the live transport")
	case <-time.After(5 * time.Second):
		t.Fatal("delayed extra stream was not enforced beyond the startup window")
	}
}

// TestQUICDialHasNoStartupGraceLatency proves connection setup does not pay a
// fixed per-connection stream-contract grace window. Each such window cost a
// flat 100ms; ten sequential loopback dials must now finish in a fraction of
// that accumulated cost, even on a loaded race-enabled machine. The bound is
// deliberately loose (well under the ~1s the removed window alone would add)
// so it measures the regression without being a tight timing assertion.
func TestQUICDialHasNoStartupGraceLatency(t *testing.T) {
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 16)
	require.NoError(t, err)
	accepted := make(chan wire.Transport, 16)
	acceptorDone := make(chan struct{})
	go func() {
		defer close(acceptorDone)
		for {
			transport, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- transport
		}
	}()

	const dials = 10
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	for range dials {
		client, err := DialConfig(listener.Addr(), "vev-bootstrap", fingerprint, Config{}, 5*time.Second).Dial(ctx)
		require.NoError(t, err)
		require.NoError(t, client.Close())
	}
	elapsed := time.Since(start)
	require.Less(t, elapsed, 700*time.Millisecond,
		"sequential dials must not pay a per-connection startup grace window")

	require.NoError(t, listener.Close())
	<-acceptorDone
	close(accepted)
	for transport := range accepted {
		_ = transport.Close()
	}
}

// readLinkEvent reads one link event or fails the test.
func readLinkEvent(t *testing.T, events <-chan ports.LinkEvent) (ports.LinkEvent, bool) {
	t.Helper()
	select {
	case event, ok := <-events:
		return event, ok
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a link event")
		return ports.LinkEvent{}, false
	}
}

// TestQUICLinkEventsReportTerminalOffline proves the LinkEvents contract is
// honest: the adapter publishes the terminal Offline transition and closes
// the event stream, locally on Close and remotely when the peer ends the
// connection, without leaking a QUIC library type in LinkEvent.Err.
func TestQUICLinkEventsReportTerminalOffline(t *testing.T) {
	server, client := dialTestPairWithConfig(t, Config{})
	require.Equal(t, ports.LinkStateConnected, server.LinkState())
	require.Equal(t, ports.LinkStateConnected, client.LinkState())

	require.NoError(t, server.Close())
	require.Equal(t, ports.LinkStateOffline, server.LinkState())
	event, ok := readLinkEvent(t, server.LinkEvents())
	require.True(t, ok)
	require.Equal(t, ports.LinkStateOffline, event.State)
	require.NoError(t, event.Err, "a local Close is not a failure")
	_, ok = readLinkEvent(t, server.LinkEvents())
	require.False(t, ok, "local event stream must close after the terminal event")

	require.Eventually(t, func() bool {
		return client.LinkState() == ports.LinkStateOffline
	}, 5*time.Second, 10*time.Millisecond, "peer must observe the terminal Offline state")
	event, ok = readLinkEvent(t, client.LinkEvents())
	require.True(t, ok)
	require.Equal(t, ports.LinkStateOffline, event.State)
	require.ErrorIs(t, event.Err, io.EOF, "peer failure must map to a boundary-safe error")
	_, ok = readLinkEvent(t, client.LinkEvents())
	require.False(t, ok, "peer event stream must close after the terminal event")
}

func (r *markRecorder) snapshot() []ports.RuntimeMark {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ports.RuntimeMark(nil), r.marks...)
}

// assertBalancedQUICMarks proves every started adapter operation also closed:
// the transport must never leak a start mark when Close (or a failure) ends an
// operation.
func assertBalancedQUICMarks(t *testing.T, marks []ports.RuntimeMark) {
	t.Helper()
	starts := make(map[uint64]ports.RuntimeMarkKind)
	for _, mark := range marks {
		switch mark.Kind {
		case ports.RuntimeAdapterSendStart, ports.RuntimeAdapterReceiveStart:
			_, duplicate := starts[mark.Sequence]
			require.False(t, duplicate, "duplicate start mark for one operation")
			starts[mark.Sequence] = mark.Kind
		case ports.RuntimeAdapterSendEnd, ports.RuntimeAdapterReceiveEnd:
			start, ok := starts[mark.Sequence]
			require.True(t, ok, "end mark without a start mark")
			want := ports.RuntimeAdapterSendEnd
			if start == ports.RuntimeAdapterReceiveStart {
				want = ports.RuntimeAdapterReceiveEnd
			}
			require.Equal(t, want, mark.Kind)
			delete(starts, mark.Sequence)
		}
	}
	require.Empty(t, starts, "start marks without an end mark")
}

// dialObservedTestPair dials a listener configured with observer, so the
// accepted (accept-side) transport carries runtime marks.
func dialObservedTestPair(t *testing.T, observer ports.SerializedRuntimeObserver) (wire.Transport, wire.Transport) {
	t.Helper()
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 4, WithListenerRuntimeObserver(observer))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan wire.Transport, 1)
	go func() {
		if transport, err := listener.Accept(); err == nil {
			accepted <- transport
		}
	}()

	client, err := DialConfig(listener.Addr(), "vev-bootstrap", fingerprint, Config{}, 10*time.Second).Dial(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	select {
	case server := <-accepted:
		t.Cleanup(func() { _ = server.Close() })
		return client, server
	case <-time.After(10 * time.Second):
		t.Fatal("listener did not accept")
		return nil, nil
	}
}

// TestQUICAcceptObserverEmitsAdapterMarks proves the observer wired through
// the listener (not just DialConfig) reaches the accepted transport.
func TestQUICAcceptObserverEmitsAdapterMarks(t *testing.T) {
	recorder := &markRecorder{}
	client, server := dialObservedTestPair(t, recorder)

	require.NoError(t, client.Send(wire.Envelope{Payload: []byte("observed")}))
	got, err := server.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("observed"), got.Payload)
	require.NoError(t, server.Send(wire.Envelope{Payload: []byte("ack")}))
	_, err = client.Recv()
	require.NoError(t, err)

	marks := recorder.snapshot()
	require.NotEmpty(t, marks, "accept side must observe carriage marks")
	for _, mark := range marks {
		require.Equal(t, "quic", mark.Component)
	}
	assertBalancedQUICMarks(t, marks)
}

// TestQUICObserverMarksBalanceAcrossClose proves Close gates new observed
// operations and waits for an in-flight blocked Recv to close its mark pair.
func TestQUICObserverMarksBalanceAcrossClose(t *testing.T) {
	recorder := &markRecorder{}
	client, server := dialObservedTestPair(t, recorder)

	require.NoError(t, client.Send(wire.Envelope{Payload: []byte("before close")}))
	_, err := server.Recv()
	require.NoError(t, err)

	blocked := make(chan struct{})
	go func() {
		_, _ = server.Recv()
		close(blocked)
	}()
	require.NoError(t, server.Close())
	<-blocked

	before := len(recorder.snapshot())
	// A Send after Close fails closed without opening a new observed operation.
	_ = server.Send(wire.Envelope{Payload: []byte("late")})
	marks := recorder.snapshot()
	require.Equal(t, before, len(marks), "operations after Close must not be observed")
	assertBalancedQUICMarks(t, marks)
}
