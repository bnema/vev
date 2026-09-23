// Package quic carries complete serialized Protobuf envelopes over one
// bidirectional QUIC stream per connection. The stream is framed with the
// shared streamframe protocol (4-byte big-endian length, no type byte);
// typed wrapping stays in sessionwire. QUIC library types never leave
// this package: composition sees only wire.Transport, wire.Dialer, and
// wire.Listener.
package quic

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/bnema/vev/internal/adapters/streamframe"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// alpn binds the QUIC handshake to the protocol epoch: only matched peers
// complete the TLS handshake. The value is fixed per epoch; bump the epoch
// for an intentional clean break.
const alpn = "vev/1"

// maxSkippedProbes bounds consecutive zero-length dial probes skipped by
// Recv. One probe is sent per Dial; anything beyond a small burst is a
// stalling peer, not a handshake.
const maxSkippedProbes = 4

// Close codes for explicit close mapping.
const (
	codeClosed quicgo.ApplicationErrorCode = 0
	codeReset  quicgo.ApplicationErrorCode = 1
)

// Stream reset codes mirror the close taxonomy on the single stream.
const streamCodeClosed quicgo.StreamErrorCode = 0

var (
	errNoStream     = errors.New("quic: peer opened no stream")
	errNotConnected = errors.New("quic: connection is closed")
	errFingerprint  = errors.New("quic: certificate fingerprint mismatch")
)

// Terminal framing errors reuse the streamframe taxonomy so callers
// match one error set across IPC, SSH, and QUIC carriages.
var (
	errZeroLength = streamframe.ErrZeroLength
	errTooLarge   = streamframe.ErrTooLarge
)

// Config tunes one QUIC endpoint. Zero values select the normative
// defaults: TLS 1.3, one peer-initiated bidirectional stream on the
// listener and none on the dialer, no unidirectional streams, keepalive /
// idle limits, bounded admission, no 0-RTT.
type Config struct {
	// KeepAlivePeriod defaults to 15s; MaxIdleTimeout defaults to 60s;
	// HandshakeIdleTimeout defaults to 5s.
	KeepAlivePeriod      time.Duration
	MaxIdleTimeout       time.Duration
	HandshakeIdleTimeout time.Duration
	// MaxIncomingStreams defaults to 1 on the listener (the dialer's
	// single stream) and to 0 on the dialer (the server never opens
	// streams): a zero public value always means "the role default".
	MaxIncomingStreams int64
	// MaxIncomingUniStreams defaults to 0 (no unidirectional streams on
	// either role). quic-go treats 0 as "library default" and only a
	// negative value as "none", so the zero value is translated to an
	// explicit negative before reaching the library.
	MaxIncomingUniStreams int64
}

// noIncomingStreams is quic-go's sentinel for "this direction is truly
// disabled": a zero quic-go limit would instead select its 100-stream
// library default.
const noIncomingStreams int64 = -1

// baseQUICConfig applies the transport tuning shared by both roles.
func baseQUICConfig(config Config) *quicgo.Config {
	keepAlive := config.KeepAlivePeriod
	if keepAlive == 0 {
		keepAlive = 15 * time.Second
	}
	maxIdle := config.MaxIdleTimeout
	if maxIdle == 0 {
		maxIdle = 60 * time.Second
	}
	handshakeIdle := config.HandshakeIdleTimeout
	if handshakeIdle == 0 {
		handshakeIdle = 5 * time.Second
	}
	return &quicgo.Config{
		// 1200-byte QUIC packets fit IPv6's minimum 1280-byte link MTU after
		// UDP/IP headers. This matters for VPN interfaces such as NetBird's
		// wt0, where quic-go's larger default initial packet fails with
		// EMSGSIZE before path MTU discovery can run.
		InitialPacketSize:     1200,
		KeepAlivePeriod:       keepAlive,
		MaxIdleTimeout:        maxIdle,
		HandshakeIdleTimeout:  handshakeIdle,
		MaxIncomingStreams:    config.MaxIncomingStreams,
		MaxIncomingUniStreams: config.MaxIncomingUniStreams,
		// 0-RTT stays disabled: only DialAddr/ListenAddr (never *Early)
		// are used, and resumption is observable via Used0RTT.
	}
}

// serverQUICConfig is the listener role: exactly one peer-initiated
// bidirectional stream (the dialer's) and no unidirectional streams.
func serverQUICConfig(config Config) *quicgo.Config {
	conf := baseQUICConfig(config)
	if config.MaxIncomingStreams == 0 {
		conf.MaxIncomingStreams = 1
	}
	if config.MaxIncomingUniStreams == 0 {
		conf.MaxIncomingUniStreams = noIncomingStreams
	}
	return conf
}

// clientQUICConfig is the dialer role: the server never opens streams, so
// both incoming directions are truly disabled by default. The dialer's own
// outgoing stream is unaffected by the incoming limits.
func clientQUICConfig(config Config) *quicgo.Config {
	conf := baseQUICConfig(config)
	if config.MaxIncomingStreams == 0 {
		conf.MaxIncomingStreams = noIncomingStreams
	}
	if config.MaxIncomingUniStreams == 0 {
		conf.MaxIncomingUniStreams = noIncomingStreams
	}
	return conf
}

// serverTLSConfig presents cert and requires TLS 1.3 with the epoch ALPN.
// Client certificates are not used: authentication is the SHA-256 pin
// checked by the dialer plus the bootstrap token consumed during authentication.
func serverTLSConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{alpn},
	}
}

// clientTLSConfig skips hostname verification: the dialer pins the exact
// SHA-256 of the server certificate instead. TLS 1.3 and the epoch ALPN
// are still enforced by the handshake.
func clientTLSConfig(serverName string, fingerprint []byte) *tls.Config {
	return &tls.Config{
		ServerName:         serverName,
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{alpn},
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errFingerprint
			}
			sum := sha256.Sum256(rawCerts[0])
			if string(sum[:]) != string(fingerprint) {
				return errFingerprint
			}
			return nil
		},
	}
}

// Transport is one QUIC connection's single bidirectional stream,
// framed as wire.Envelopes. Close is safe concurrently with Send/Recv
// and unblocks both.
type Transport struct {
	conn   *quicgo.Conn
	stream *quicgo.Stream
	framer *streamframe.Framer
	// packetConn is the dialer-owned local socket behind conn; the listener
	// owns its own socket and passes nil.
	packetConn net.PacketConn

	observer ports.SerializedRuntimeObserver

	// Observation bookkeeping mirrors the IPC transport: an accepted
	// operation always closes its mark pair, and Close stops accepting new
	// marks and then waits for in-flight ones to finish.
	operationMu     sync.Mutex
	operationCount  int
	observedClosing bool
	operationsDone  chan struct{}

	linkMu    sync.Mutex
	linkState ports.LinkState
	events    chan ports.LinkEvent

	closeOnce    sync.Once
	terminalOnce sync.Once
	// teardownDone closes when the deferred hard close after an orderly
	// Close has run.
	teardownDone chan struct{}
	closeErr     error
}

var (
	_ wire.Transport          = (*Transport)(nil)
	_ wire.AsyncTransport     = (*Transport)(nil)
	_ wire.BoundedTransport   = (*Transport)(nil)
	_ ports.LinkStateReporter = (*Transport)(nil)
)

// newTransport owns one connected stream. The caller must have enforced
// the single-stream contract already. observer may be nil; when set it
// receives process-local adapter marks for send and receive.
func newTransport(conn *quicgo.Conn, stream *quicgo.Stream, packetConn net.PacketConn, observer ports.SerializedRuntimeObserver) *Transport {
	t := &Transport{
		conn:         conn,
		stream:       stream,
		packetConn:   packetConn,
		observer:     observer,
		linkState:    ports.LinkStateConnected,
		events:       make(chan ports.LinkEvent, 4),
		teardownDone: make(chan struct{}),
	}
	t.framer = streamframe.NewFramer(stream, stream, t.closeStream)
	// The single-stream contract is enforced for the whole connection
	// lifetime by quic-go's role limits (no unidirectional streams, no
	// server-initiated streams) plus the guard watcher. There is no
	// startup grace window: it would tax every connection while the guard
	// already fails a violation whenever it lands.
	guardStreamContract(conn)
	// Link-state honesty: a single QUIC stream cannot derive a reliable
	// degraded/probing transition, so the adapter reports only the
	// terminal Offline transition and closes the event stream when the
	// connection dies on its own.
	go func() {
		<-conn.Context().Done()
		if t.packetConn != nil {
			_ = t.packetConn.Close()
		}
		t.signalTerminal(linkEventError(context.Cause(conn.Context())))
	}()
	return t
}

// guardStreamContract closes the connection, for the connection lifetime,
// the moment the peer opens anything beyond the single client-initiated
// bidirectional stream: a second bidirectional stream or any
// unidirectional stream. quic-go's role limits should prevent both
// entirely; the guard is defense in depth against an explicit limit.
//
// The acceptors are deliberately never canceled: quic-go's AcceptStream may
// hand a queued stream to an already-canceled waiter, so an abandoned
// acceptor would silently swallow a delayed violation.
func guardStreamContract(conn *quicgo.Conn) {
	go func() {
		bidi := make(chan error, 1)
		go func() {
			_, err := conn.AcceptStream(context.Background())
			bidi <- err
		}()
		uni := make(chan error, 1)
		go func() {
			_, err := conn.AcceptUniStream(context.Background())
			uni <- err
		}()
		select {
		case err := <-bidi:
			if err == nil {
				_ = conn.CloseWithError(codeReset, "extra bidirectional stream")
			}
		case err := <-uni:
			if err == nil {
				_ = conn.CloseWithError(codeReset, "forbidden unidirectional stream")
			}
		}
	}()
}

// Send queues the envelope for the sole writer and waits for its wire attempt.
func (t *Transport) Send(envelope wire.Envelope) error {
	end := t.observe(ports.RuntimeAdapterSendStart, uint64(len(envelope.Payload)))
	err := t.framer.Send(envelope.Payload)
	end(err == nil)
	if err != nil {
		return mapFramerError(err)
	}
	return nil
}

// SendAsync accepts the envelope for ordered background transmission.
func (t *Transport) SendAsync(envelope wire.Envelope) error {
	end := t.observe(ports.RuntimeAdapterSendStart, uint64(len(envelope.Payload)))
	err := t.framer.SendAsync(envelope.Payload)
	end(err == nil)
	if err != nil {
		return mapFramerError(err)
	}
	return nil
}

// observe opens one process-local adapter mark pair, or returns a no-op end
// when observation is disabled or Close has already begun. An accepted
// operation always emits its end mark, so Close can safely wait for it.
func (t *Transport) observe(start ports.RuntimeMarkKind, bytes uint64) func(bool) {
	if t.observer == nil {
		return func(bool) {}
	}
	if !t.beginObservedOperation() {
		return func(bool) {}
	}
	correlation := ports.NewRuntimeCorrelation()
	t.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("quic", correlation, start, bytes, true))
	end := ports.RuntimeAdapterSendEnd
	if start == ports.RuntimeAdapterReceiveStart {
		end = ports.RuntimeAdapterReceiveEnd
	}
	return func(valid bool) {
		defer t.finishObservedOperation()
		t.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("quic", correlation, end, bytes, valid))
	}
}

func (t *Transport) beginObservedOperation() bool {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	if t.observedClosing {
		return false
	}
	if t.operationCount == 0 {
		t.operationsDone = make(chan struct{})
	}
	t.operationCount++
	return true
}

func (t *Transport) finishObservedOperation() {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	t.operationCount--
	if t.operationCount == 0 {
		close(t.operationsDone)
	}
}

// beginShutdown stops accepting new observed operations and returns a channel
// closed when the in-flight ones finish, or nil when none were running.
func (t *Transport) beginShutdown() <-chan struct{} {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	t.observedClosing = true
	if t.operationCount == 0 {
		return nil
	}
	return t.operationsDone
}

// Recv reads one complete envelope, blocking until arrival or close.
// The dial probe (a framed zero-length envelope) is skipped at most
// maxSkippedProbes times: it proves stream liveness without reaching
// application decoders, but an unbounded skip would let a peer stall
// Recv forever without progress.
func (t *Transport) Recv() (wire.Envelope, error) {
	return t.recvSkippingProbes(t.framer.Recv)
}

// RecvBounded reads exactly one framed envelope without allocating a
// payload larger than limit: the four-byte length prefix is validated
// before any payload buffer exists. It backs the bootstrap auth record so
// an unauthenticated peer cannot force a large pre-auth allocation by
// advertising a huge frame and stalling. Zero-length dial probes are
// skipped exactly like Recv, and the shared streamframe framer enforces the
// same framing as every other carriage. It reads through the transport's one
// framer, so it must only be used before regular Send/Recv traffic begins.
func (t *Transport) RecvBounded(limit uint64) (wire.Envelope, error) {
	return t.recvSkippingProbes(func() ([]byte, error) { return t.framer.RecvBounded(limit) })
}

// recvSkippingProbes observes one receive operation and runs read for exactly
// one envelope, skipping the framed zero-length dial probe at most
// maxSkippedProbes times. Both Recv and RecvBounded share it, so the bounded
// path adds only the per-call ceiling and never a second framing copy.
func (t *Transport) recvSkippingProbes(read func() ([]byte, error)) (wire.Envelope, error) {
	end := t.observe(ports.RuntimeAdapterReceiveStart, 0)
	for skipped := 0; ; skipped++ {
		payload, err := read()
		if errors.Is(err, streamframe.ErrZeroLength) {
			if skipped >= maxSkippedProbes {
				end(false)
				return wire.Envelope{}, errZeroLength
			}
			continue
		}
		if err != nil {
			end(false)
			return wire.Envelope{}, mapFramerError(err)
		}
		end(true)
		return wire.Envelope{Payload: payload}, nil
	}
}

// zeroBytes best-effort erases a mutable secret buffer.
func zeroBytes(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

// LinkState reports transport connectivity only, never semantic
// handshake acceptance.
func (t *Transport) LinkState() ports.LinkState {
	t.linkMu.Lock()
	defer t.linkMu.Unlock()
	return t.linkState
}

// LinkEvents returns the best-effort link event stream. The adapter derives
// one terminal Offline event on connection failure or Close and then closes
// the channel; no degraded/probing transitions are invented.
func (t *Transport) LinkEvents() <-chan ports.LinkEvent { return t.events }

// gracefulCloseWindow bounds how long an orderly close keeps the connection
// alive after its FIN so an already-written final envelope can be transmitted
// before CONNECTION_CLOSE. Close itself never waits on it.
const gracefulCloseWindow = time.Second

// Close performs an orderly teardown: it stops the writer, FINs the send side
// instead of RESET_STREAMing it, and defers the hard connection close so an
// envelope already handed to the stream (the final envelope of an orderly
// close) is transmitted rather than discarded. It publishes the terminal
// Offline link event and closes the event stream.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		// Stop accepting observed operation marks and wait for in-flight
		// ones to close their pairs before returning.
		operationsDone := t.beginShutdown()
		// Publish the graceful terminal event before closing the
		// connection so a local Close reports a nil cause instead of
		// racing the peer-failure watcher.
		t.signalTerminal(nil)
		// Start the bounded hard close first: if the writer is blocked on a
		// peer that stopped reading, it is what unblocks the FIN below.
		go t.deferCloseConnection()
		// framer.Close stops new sends, unblocks a blocked Recv through the
		// injected closer, and waits for the writer to finish. It must run
		// before the FIN so an in-flight write is not cut short.
		t.closeErr = t.framer.Close()
		_ = t.stream.Close()
		if operationsDone != nil {
			<-operationsDone
		}
	})
	return t.closeErr
}

// WaitGracefulTeardown blocks until the deferred hard connection close that
// follows an orderly Close has run: the graceful window elapsed or the peer
// closed first. A process-owning caller (the broker mux QUIC proxy) waits after Close so
// the final synchronous envelope is not discarded by process exit; ctx bounds
// the wait. It returns ctx.Err() when the wait is cut short, and blocks until
// ctx expires when Close was never called.
func (t *Transport) WaitGracefulTeardown(ctx context.Context) error {
	select {
	case <-t.teardownDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// deferCloseConnection keeps the connection alive long enough to transmit a
// FIN and any stream data queued behind it, then hard-closes unless the peer
// already did. Closing the connection first would send CONNECTION_CLOSE ahead
// of the buffered stream data and discard it.
func (t *Transport) deferCloseConnection() {
	defer close(t.teardownDone)
	timer := time.NewTimer(gracefulCloseWindow)
	defer timer.Stop()
	select {
	case <-t.conn.Context().Done():
	case <-timer.C:
		_ = t.conn.CloseWithError(codeClosed, "closed")
	}
}

// signalTerminal publishes the terminal Offline event and closes the event
// stream exactly once, whether the transport ended via Close or via the
// peer. Publishing never blocks transport progress: when the consumer is
// slow the event is dropped and the closed channel plus LinkState still
// report Offline.
func (t *Transport) signalTerminal(err error) {
	t.terminalOnce.Do(func() {
		t.linkMu.Lock()
		t.linkState = ports.LinkStateOffline
		t.linkMu.Unlock()
		select {
		case t.events <- ports.LinkEvent{State: ports.LinkStateOffline, At: time.Now(), Err: err}:
		default:
		}
		close(t.events)
	})
}

// linkEventError keeps LinkEvent.Err boundary-safe: QUIC library types never
// leave this package.
func linkEventError(err error) error {
	if err == nil {
		return nil
	}
	var appErr *quicgo.ApplicationError
	var streamErr *quicgo.StreamError
	var idleErr *quicgo.IdleTimeoutError
	var transportErr *quicgo.TransportError
	if errors.As(err, &appErr) || errors.As(err, &streamErr) ||
		errors.As(err, &idleErr) || errors.As(err, &transportErr) {
		return io.EOF
	}
	return err
}

// closeStream is the framer's injected closer: it unblocks a blocked Recv
// without touching the send side, which Close() FINs only after the writer
// has drained.
func (t *Transport) closeStream() error {
	t.stream.CancelRead(streamCodeClosed)
	return nil
}

func mapFramerError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, streamframe.ErrZeroLength):
		return errZeroLength
	case errors.Is(err, streamframe.ErrTooLarge):
		return errTooLarge
	case errors.Is(err, streamframe.ErrClosed):
		return errNotConnected
	default:
		var appErr *quicgo.ApplicationError
		if errors.As(err, &appErr) {
			// QUIC library types never leave this package: a remote
			// or local application close reads as a terminal EOF so
			// callers classify graceful shutdown like IPC/SSH.
			return io.EOF
		}
		var streamErr *quicgo.StreamError
		if errors.As(err, &streamErr) {
			return io.EOF
		}
		return err
	}
}
