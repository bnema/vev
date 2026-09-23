package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/bnema/vev/internal/ports"
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
	config      *quicgo.Config
	timeout     time.Duration
	observer    ports.SerializedRuntimeObserver
	// resolve is the address-resolution seam; nil selects the context-aware
	// default resolver. Multi-address fallback is asserted through it without
	// depending on real DNS answers or black-holed routes.
	resolve func(ctx context.Context, addr string) ([]*net.UDPAddr, error)
}

var _ wire.Dialer = (*Dialer)(nil)

// Option tunes a dial.
type Option func(*dialOptions)

type dialOptions struct {
	observer ports.SerializedRuntimeObserver
}

// DialConfig carries the dial parameters. Fingerprint is the exact
// SHA-256 of the server certificate (32 bytes). Timeout bounds the whole
// dial: address resolution, every candidate connection, stream open, and
// the liveness probe share one deadline; zero selects 15s, negative is
// rejected at Dial time.
func DialConfig(addr, serverName string, fingerprint []byte, config Config, timeout time.Duration, opts ...Option) Dialer {
	if timeout == 0 {
		timeout = 15 * time.Second
	}
	var options dialOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return Dialer{
		addr:        addr,
		serverName:  serverName,
		fingerprint: append([]byte(nil), fingerprint...),
		config:      clientQUICConfig(config),
		timeout:     timeout,
		observer:    options.observer,
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
	// One shared deadline bounds resolution, every candidate address, stream
	// open, and the probe, so the documented timeout is truly
	// whole-operation rather than per-step.
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	tlsConf := clientTLSConfig(d.serverName, d.fingerprint)

	addrs, err := d.resolveAddrs(ctx)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for i, addr := range addrs {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}
		// Split the shared deadline across the remaining candidates so a
		// black-holed address cannot starve the rest of the resolved set.
		budget := time.Duration(0)
		if remaining := len(addrs) - i; remaining > 1 {
			if deadline, ok := ctx.Deadline(); ok {
				if slice := time.Until(deadline) / time.Duration(remaining); slice > 0 {
					budget = slice
				}
			}
		}
		conn, packetConn, err := d.dialCandidate(ctx, addr, tlsConf, budget)
		if err != nil {
			lastErr = err
			continue
		}
		transport, err := d.finishDial(ctx, conn, packetConn)
		if err != nil {
			lastErr = err
			_ = conn.CloseWithError(codeReset, "dial failed")
			_ = packetConn.Close()
			continue
		}
		return transport, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ctx.Err()
}

// dialCandidate opens one QUIC connection to a single resolved address. A
// positive budget carves a slice from the shared deadline so a black-holed
// candidate cannot consume the whole window. The returned socket is owned by
// the caller on success and closed here on failure.
func (d Dialer) dialCandidate(ctx context.Context, addr *net.UDPAddr, tlsConf *tls.Config, budget time.Duration) (*quicgo.Conn, net.PacketConn, error) {
	if budget > 0 {
		attemptCtx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		ctx = attemptCtx
	}
	packetConn, err := listenUDPFor(addr)
	if err != nil {
		return nil, nil, err
	}
	conn, err := quicgo.Dial(ctx, packetConn, addr, tlsConf, d.config)
	if err != nil {
		_ = packetConn.Close()
		return nil, nil, err
	}
	return conn, packetConn, nil
}

// finishDial rejects 0-RTT resumption, opens the single stream under the
// shared deadline, and sends the liveness probe. packetConn is the
// caller-created local socket backing conn; the returned transport owns it.
// On error the caller owns closing conn and packetConn.
func (d Dialer) finishDial(ctx context.Context, conn *quicgo.Conn, packetConn net.PacketConn) (wire.Transport, error) {
	if conn.ConnectionState().Used0RTT {
		return nil, errors.New("quic: 0-RTT resumption refused")
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	// The server observes the connection once this stream carries data:
	// quic-go only surfaces the accepted connection after the peer's
	// AcceptStream fires, which requires stream activity. The probe is a
	// framed zero-length envelope: the receiver's scanner reports
	// ErrZeroLength for it, and both Recv paths skip that error without
	// reaching application decoders, so the stream stays usable for
	// application envelopes afterwards.
	if _, err := stream.Write([]byte{0, 0, 0, 0}); err != nil {
		return nil, err
	}
	return newTransport(conn, stream, packetConn, d.observer), nil
}

// listenUDPFor binds a local socket matching the candidate's address family.
// quic-go's Dial takes ownership semantics from the caller (unlike DialAddr),
// so the returned socket must be closed with the connection.
func listenUDPFor(remote *net.UDPAddr) (*net.UDPConn, error) {
	local := &net.UDPAddr{IP: net.IPv6unspecified, Port: 0}
	if remote.IP.To4() != nil {
		local = &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	}
	return net.ListenUDP("udp", local)
}

// resolveAddrs turns the configured dial address into one or more concrete
// UDP addresses within ctx.
func (d Dialer) resolveAddrs(ctx context.Context) ([]*net.UDPAddr, error) {
	if d.resolve != nil {
		return d.resolve(ctx, d.addr)
	}
	return resolveQUICAddrs(ctx, d.addr)
}

// resolveQUICAddrs resolves addr with the context-aware default resolver.
// A literal IP yields exactly one address; a hostname yields every resolved
// address (deduplicated) so the dialer can fail over between them. The
// lookup is bounded by ctx, unlike quic-go's own unbounded DialAddr
// resolution.
func resolveQUICAddrs(ctx context.Context, addr string) ([]*net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("quic: invalid dial address %q: %w", addr, err)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum <= 0 || portNum > 65535 {
		return nil, fmt.Errorf("quic: invalid dial port %q", port)
	}
	if ip := net.ParseIP(host); ip != nil {
		return []*net.UDPAddr{{IP: ip, Port: portNum}}, nil
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	addrs := make([]*net.UDPAddr, 0, len(ips))
	seen := make(map[netip.Addr]struct{}, len(ips))
	for _, ip := range ips {
		ip = ip.Unmap()
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		addrs = append(addrs, &net.UDPAddr{IP: net.IP(ip.AsSlice()), Port: portNum})
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("quic: no addresses for host %q", host)
	}
	return addrs, nil
}

// Listener accepts QUIC connections with bounded admission: at most
// maxPending unauthenticated connections wait; overflow fails fast.
type Listener struct {
	listener   *quicgo.Listener
	config     Config
	cert       tls.Certificate
	addr       string
	observer   ports.SerializedRuntimeObserver
	pendingMu  sync.Mutex
	pending    int
	maxPending int

	closeOnce sync.Once
	closeErr  error
}

var _ wire.Listener = (*Listener)(nil)

// ListenerOption tunes a listener.
type ListenerOption func(*Listener)

// WithListenerRuntimeObserver enables process-local adapter marks on every
// accepted transport, mirroring the dial-side WithRuntimeObserver. A nil
// observer leaves observation disabled.
func WithListenerRuntimeObserver(observer ports.SerializedRuntimeObserver) ListenerOption {
	return func(l *Listener) { l.observer = observer }
}

// ListenConfig bounds admission and validates its inputs. Empty addr
// selects loopback (never the wildcard); negative durations and stream
// limits are rejected. MaxPendingUnauthenticated defaults to 4 per the
// normative table; the generic accept budget defaults to 5s. The bootstrap
// server passes its own 3s auth deadline as that budget and sets the
// matching handshake idle timeout, so both bounds agree there. A zero stream
// limit means "no streams", translated to quic-go's negative semantics
// inside the role-specific config.
func ListenConfig(addr string, cert tls.Certificate, config Config, maxPending int, opts ...ListenerOption) (*Listener, error) {
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
	l := &Listener{
		listener:   listener,
		config:     config,
		cert:       cert,
		addr:       listener.Addr().String(),
		maxPending: maxPending,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(l)
		}
	}
	return l, nil
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
		transport, err := acceptSingleStream(streamCtx, conn, l.observer)
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
func acceptSingleStream(ctx context.Context, conn *quicgo.Conn, observer ports.SerializedRuntimeObserver) (*Transport, error) {
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
	return newTransport(conn, stream, nil, observer), nil
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
