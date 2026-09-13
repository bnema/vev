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
	"encoding/binary"
	"errors"
	"io"
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
// checked by the dialer plus the bootstrap token consumed by P6.2.
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

	linkMu    sync.Mutex
	linkState ports.LinkState
	events    chan ports.LinkEvent

	closeOnce    sync.Once
	terminalOnce sync.Once
	closeErr     error
}

var (
	_ wire.Transport          = (*Transport)(nil)
	_ wire.AsyncTransport     = (*Transport)(nil)
	_ ports.LinkStateReporter = (*Transport)(nil)
)

// newTransport owns one connected stream. The caller must have enforced
// the single-stream contract already.
func newTransport(conn *quicgo.Conn, stream *quicgo.Stream) *Transport {
	t := &Transport{
		conn:      conn,
		stream:    stream,
		linkState: ports.LinkStateConnected,
		events:    make(chan ports.LinkEvent, 4),
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
	if err := t.framer.Send(envelope.Payload); err != nil {
		return mapFramerError(err)
	}
	return nil
}

// SendAsync accepts the envelope for ordered background transmission.
func (t *Transport) SendAsync(envelope wire.Envelope) error {
	if err := t.framer.SendAsync(envelope.Payload); err != nil {
		return mapFramerError(err)
	}
	return nil
}

// Recv reads one complete envelope, blocking until arrival or close.
// The dial probe (a framed zero-length envelope) is skipped at most
// maxSkippedProbes times: it proves stream liveness without reaching
// application decoders, but an unbounded skip would let a peer stall
// Recv forever without progress.
func (t *Transport) Recv() (wire.Envelope, error) {
	for skipped := 0; ; skipped++ {
		payload, err := t.framer.Recv()
		if errors.Is(err, streamframe.ErrZeroLength) {
			if skipped >= maxSkippedProbes {
				return wire.Envelope{}, errZeroLength
			}
			continue
		}
		if err != nil {
			return wire.Envelope{}, mapFramerError(err)
		}
		return wire.Envelope{Payload: payload}, nil
	}
}

// recvBounded reads exactly one framed envelope without allocating a
// payload larger than limit: the four-byte length prefix is validated
// before any payload buffer exists. It backs the bootstrap auth record so
// an unauthenticated peer cannot force a large pre-auth allocation by
// advertising a huge frame and stalling. Zero-length dial probes are
// skipped exactly like Recv. It reads the stream directly (bypassing the
// framer) and must only be used before regular Recv/Send traffic begins.
func (t *Transport) recvBounded(limit uint32) (wire.Envelope, error) {
	for skipped := 0; ; skipped++ {
		var header [4]byte
		if _, err := io.ReadFull(t.stream, header[:]); err != nil {
			return wire.Envelope{}, mapFramerError(err)
		}
		length := binary.BigEndian.Uint32(header[:])
		if length == 0 {
			if skipped >= maxSkippedProbes {
				return wire.Envelope{}, errZeroLength
			}
			continue
		}
		if length > limit {
			return wire.Envelope{}, errTooLarge
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(t.stream, payload); err != nil {
			zeroBytes(payload)
			return wire.Envelope{}, mapFramerError(err)
		}
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

// Close cancels the stream with an explicit code, closes the connection,
// and unblocks in-flight Send/Recv. It publishes the terminal Offline link
// event and closes the event stream.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		t.stream.CancelRead(streamCodeClosed)
		t.stream.CancelWrite(streamCodeClosed)
		// Publish the graceful terminal event before closing the
		// connection so a local Close reports a nil cause instead of
		// racing the peer-failure watcher.
		t.signalTerminal(nil)
		_ = t.conn.CloseWithError(codeClosed, "closed")
		t.closeErr = t.framer.Close()
	})
	return t.closeErr
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

func (t *Transport) closeStream() error {
	t.stream.CancelRead(streamCodeClosed)
	t.stream.CancelWrite(streamCodeClosed)
	return t.conn.CloseWithError(codeClosed, "closed")
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
