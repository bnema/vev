package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/bnema/vev/internal/protocol/wire"
)

// Dialer establishes one QUIC connection and opens its single bidirectional
// stream. Authentication is the exact SHA-256 certificate pin; TLS 1.3 and
// the epoch ALPN are enforced by the handshake. 0-RTT is never used.
//
// The dialer admits no peer-initiated stream at all: the server role never
// opens streams, so both incoming directions are truly disabled with the
// quic-go negative-limit semantics. Any violation that still arrives is
// also enforced for the connection lifetime by the stream-contract guard.
type Dialer struct {
	addr        string
	serverName  string
	fingerprint []byte
	tls         *tls.Config
	config      *quicgo.Config
	timeout     time.Duration
}

var _ wire.Dialer = (*Dialer)(nil)

// DialConfig carries the dial parameters. Fingerprint is the exact
// SHA-256 of the server certificate (32 bytes). Timeout bounds the
// whole dial including stream open; zero selects 15s, negative is
// rejected at Dial time.
func DialConfig(addr, serverName string, fingerprint []byte, config Config, timeout time.Duration) Dialer {
	if timeout == 0 {
		timeout = 15 * time.Second
	}
	return Dialer{
		addr:        addr,
		serverName:  serverName,
		fingerprint: append([]byte(nil), fingerprint...),
		config:      clientQUICConfig(config),
		timeout:     timeout,
	}
}

// Dial connects, rejects 0-RTT resumption, and opens exactly one
// bidirectional stream. The stream liveness probe makes the stream visible
// to the peer's AcceptStream; the dialer does not pay a startup grace window
// after it. Extra or unidirectional streams fail closed: quic-go's role
// limits refuse them before the handshake and the transport's lifetime guard
// kills the connection on any violation that still lands.
func (d Dialer) Dial(ctx context.Context) (wire.Transport, error) {
	if len(d.fingerprint) != 32 {
		return nil, errFingerprint
	}
	if d.addr == "" {
		return nil, errors.New("quic: empty dial address")
	}
	if d.timeout < 0 {
		return nil, errors.New("quic: negative dial timeout")
	}
	tlsConf := clientTLSConfig(d.serverName, d.fingerprint)
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	conn, err := quicgo.DialAddr(ctx, d.addr, tlsConf, d.config)
	if err != nil {
		return nil, err
	}
	if conn.ConnectionState().Used0RTT {
		_ = conn.CloseWithError(codeReset, "0rtt refused")
		return nil, errors.New("quic: 0-RTT resumption refused")
	}
	openCtx, openCancel := context.WithTimeout(ctx, d.timeout)
	defer openCancel()
	stream, err := conn.OpenStreamSync(openCtx)
	if err != nil {
		_ = conn.CloseWithError(codeReset, "no stream")
		return nil, err
	}
	// The server observes the connection once this stream carries data:
	// quic-go only surfaces the accepted connection after the peer's
	// AcceptStream fires, which requires stream activity. The probe is a
	// framed zero-length envelope the scanner rejects deterministically
	// (ErrZeroLength); the server's first Recv surfaces that error and
	// the stream stays usable for application envelopes afterwards.
	if _, err := stream.Write([]byte{0, 0, 0, 0}); err != nil {
		_ = conn.CloseWithError(codeReset, "probe failed")
		return nil, err
	}
	return newTransport(conn, stream), nil
}

// Listener accepts QUIC connections with bounded admission: at most
// maxPending unauthenticated connections wait; overflow fails fast.
type Listener struct {
	listener   *quicgo.Listener
	config     Config
	cert       tls.Certificate
	addr       string
	pendingMu  sync.Mutex
	pending    int
	maxPending int

	closeOnce sync.Once
	closeErr  error
}

var _ wire.Listener = (*Listener)(nil)

// ListenConfig bounds admission and validates its inputs. Empty addr
// selects loopback (never the wildcard); negative durations and stream
// limits are rejected. MaxPendingUnauthenticated defaults to 4 per the
// normative table; the auth deadline is the handshake idle timeout
// (default 5s, P6.2 tightens to 3s at the bootstrap layer). A zero stream
// limit means "no streams", translated to quic-go's negative semantics
// inside the role-specific config.
func ListenConfig(addr string, cert tls.Certificate, config Config, maxPending int) (*Listener, error) {
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	if config.KeepAlivePeriod < 0 || config.MaxIdleTimeout < 0 || config.HandshakeIdleTimeout < 0 {
		return nil, errors.New("quic: negative timeout")
	}
	if config.MaxIncomingStreams < 0 || config.MaxIncomingUniStreams < 0 {
		return nil, errors.New("quic: negative stream limit")
	}
	if maxPending < 0 {
		return nil, errors.New("quic: negative admission bound")
	}
	if maxPending == 0 {
		maxPending = 4
	}
	listener, err := quicgo.ListenAddr(addr, serverTLSConfig(cert), serverQUICConfig(config))
	if err != nil {
		return nil, err
	}
	return &Listener{
		listener:   listener,
		config:     config,
		cert:       cert,
		addr:       listener.Addr().String(),
		maxPending: maxPending,
	}, nil
}

// admitted is one connection whose admission slot is still held. deadline
// bounds stream open and authentication together; release MUST be called
// exactly once when the caller finishes authenticating (success or
// failure) so the unauthenticated admission bound is never released while
// a peer still holds an unauthenticated connection.
type admitted struct {
	transport *Transport
	release   func()
	deadline  time.Time
}

// Accept admits one connection, enforces exactly one peer-initiated
// bidirectional stream, and rejects extra or unidirectional streams. The
// admission slot is released once the stream contract is established:
// this generic listener performs no authentication of its own. Callers
// that authenticate after accepting (the bootstrap server) use admitOne
// and hold the slot across authentication.
func (l *Listener) Accept() (wire.Transport, error) {
	admittedConn, err := l.admitOne(context.Background(), acceptStreamBudget)
	if err != nil {
		return nil, err
	}
	admittedConn.release()
	return admittedConn.transport, nil
}

// acceptStreamBudget bounds generic Accept's wait for the peer's stream.
const acceptStreamBudget = 5 * time.Second

// admitOne admits one connection and holds its admission slot until
// release is called. The per-connection budget starts when the connection
// is admitted and covers stream open and authentication together, so a
// peer cannot occupy an unauthenticated slot past that deadline. When the
// stream contract is violated the slot is released and the next connection
// is admitted instead.
func (l *Listener) admitOne(ctx context.Context, budget time.Duration) (admitted, error) {
	if budget <= 0 {
		budget = acceptStreamBudget
	}
	for {
		if err := ctx.Err(); err != nil {
			return admitted{}, err
		}
		conn, err := l.listener.Accept(ctx)
		if err != nil {
			return admitted{}, err
		}
		if !l.admit() {
			_ = conn.CloseWithError(codeReset, "admission full")
			continue
		}
		deadline := time.Now().Add(budget)
		streamCtx, cancel := context.WithDeadline(ctx, deadline)
		transport, err := acceptSingleStream(streamCtx, conn)
		cancel()
		if err != nil {
			l.release()
			_ = conn.CloseWithError(codeReset, "stream contract")
			continue
		}
		var once sync.Once
		return admitted{
			transport: transport,
			deadline:  deadline,
			release:   func() { once.Do(l.release) },
		}, nil
	}
}

func (l *Listener) admit() bool {
	l.pendingMu.Lock()
	defer l.pendingMu.Unlock()
	if l.pending >= l.maxPending {
		return false
	}
	l.pending++
	return true
}

func (l *Listener) release() {
	l.pendingMu.Lock()
	defer l.pendingMu.Unlock()
	if l.pending > 0 {
		l.pending--
	}
}

// acceptSingleStream takes the peer's single bidirectional stream and
// fails when the peer opens none or the caller's deadline expires. The
// transport's lifetime guard enforces the stream contract on every later
// stream.
func acceptSingleStream(ctx context.Context, conn *quicgo.Conn) (*Transport, error) {
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, errNoStream
		}
		return nil, err
	}
	if stream == nil {
		return nil, errNoStream
	}
	return newTransport(conn, stream), nil
}

// Close stops the listener; established transports are unaffected.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.closeErr = l.listener.Close()
	})
	return l.closeErr
}

// Addr returns the bound address.
func (l *Listener) Addr() string { return l.addr }
