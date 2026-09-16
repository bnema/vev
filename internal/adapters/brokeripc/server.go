package brokeripc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Broker IPC listener and per-connection session (P3.3).
//
// The listener owns one per-user AF_UNIX endpoint through the P3.2 private IPC
// carriage: owner-only directory and 0600 socket, same-user kernel peer
// credentials on accept, race-safe stale-socket recovery, foreign-path refusal,
// and idempotent Close that unlinks only the socket inode it created. It caps
// concurrent accepted clients with one slot acquired before every accept, so
// the bound is enforced before a socket is admitted.
//
// Accept completes the broker preamble and admits the client to the broker core
// before it returns, so the returned ports.BrokerService knows the
// BrokerConnectionID assigned to exactly that connection. The session then
// runs the brokerwire conversation for that connection in its own goroutines:
// one reader, one snapshot publisher, one relay pair per logical stream, and
// one goroutine per admitted mutating operation. Every one of those is
// per-connection, so a slow or stalled client blocks only its own work.

// Listen binds the private per-user broker endpoint at path and returns a
// ports.BrokerListener that admits same-user clients only.
//
// path must be an absolute, cleaned path inside an owner-only directory; the
// listener creates or validates that parent as 0700 and tightens the bound
// socket to 0600. A path that already exists and is not a socket is refused
// without being removed, a stale socket left by a dead owner is recovered
// race-safely, and a live owner is reported as ipc.ErrDaemonRunning instead of
// being evicted.
//
// authority is the ports.BrokerAuthority admission seam: the listener calls
// AdmitClient once per accepted connection under the handshake budget and binds
// the returned service to exactly that connection. That service's ConnectionID
// is the identity the session stamps on every frame; the P3.4 broker use case
// supplies the implementation, and this adapter consumes it without
// constructing a broker itself. See ports.BrokerAuthority for the
// admission-context contract.
func Listen(path string, epoch ports.BrokerEpoch, authority ports.BrokerAuthority, cfg Config, opts ...ipc.Option) (ports.BrokerListener, error) {
	return listen(path, epoch, authority, cfg, ipc.SameUserPeerVerifier(), opts...)
}

// listen is Listen with an injected peer verifier, so a test can observe a
// deterministic same-user refusal without root. A nil verifier falls back to
// the platform default.
func listen(path string, epoch ports.BrokerEpoch, authority ports.BrokerAuthority, cfg Config, verify ipc.PeerVerifier, opts ...ipc.Option) (ports.BrokerListener, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if epoch == 0 || authority == nil {
		return nil, ErrConfig
	}
	mux, err := ipc.ListenMuxWithPeerVerifier(path, verify, opts...)
	if err != nil {
		return nil, err
	}
	l := &listener{
		epoch:     epoch,
		authority: authority,
		cfg:       cfg,
		mux:       mux,
		slots:     make(chan struct{}, cfg.MaxClients),
		admitted:  make(chan *serverSession, cfg.MaxClients),
		done:      make(chan struct{}),
		fatal:     make(chan struct{}),
		sessions:  make(map[*serverSession]struct{}),
		pending:   make(map[wire.BoundedTransport]struct{}),
	}
	l.ctx, l.cancel = context.WithCancel(context.Background())
	l.wg.Add(1)
	go l.acceptLoop()
	return l, nil
}

// listener implements ports.BrokerListener over one bound private carriage.
//
// The listener owns its accept loop: each accepted carriage completes its
// preamble and broker admission in its own goroutine, bounded by one client slot
// acquired before the accept. A client that stalls during the preamble therefore
// occupies a slot and a goroutine, never the loop, so no stalled handshake can
// delay another client's admission. Admitted sessions are published through a
// bounded FIFO to Accept.
//
// The listener owns a listener-scoped context, canceled by Close, that bounds
// every in-flight preamble and admission. Each accepted carriage is also
// tracked until a running session owns it, so Close can close a carriage whose
// framing I/O would otherwise stay blocked. Together those make Close drop an
// already-accepted client immediately instead of waiting out HandshakeTimeout.
type listener struct {
	epoch     ports.BrokerEpoch
	authority ports.BrokerAuthority
	cfg       Config
	mux       ipc.MuxListener
	slots     chan struct{}
	admitted  chan *serverSession

	ctx    context.Context
	cancel context.CancelFunc

	wg    sync.WaitGroup
	fatal chan struct{}

	mu         sync.Mutex
	closed     bool
	sessions   map[*serverSession]struct{}
	pending    map[wire.BoundedTransport]struct{}
	lastRefuse error
	fatalCause error
	fatalOnce  sync.Once
	closeOnce  sync.Once
	closeErr   error
	done       chan struct{}
}

var _ ports.BrokerListener = (*listener)(nil)

// acceptLoop accepts one verified same-user carriage at a time, bounded by the
// client slots, and hands each accepted carriage to its own admission goroutine.
// It stops when the listener closes or the carriage reports a terminal accept
// failure.
func (l *listener) acceptLoop() {
	defer l.wg.Done()
	for {
		select {
		case l.slots <- struct{}{}:
		case <-l.done:
			return
		}
		// The slot is released exactly once, whether admission fails here or the
		// session reaches its terminal outcome later.
		release := sync.OnceFunc(func() { <-l.slots })
		if l.isClosed() {
			release()
			return
		}
		transport, err := l.mux.Accept()
		if err != nil {
			release()
			if l.isClosed() {
				return
			}
			if errors.Is(err, ipc.ErrMuxPeerRejected) {
				// A refused peer is isolated to that connection: it is
				// recorded for diagnostics and admission continues.
				l.confirmRefusal(err)
				continue
			}
			// Any other carriage failure stops admission deterministically;
			// Accept reports it instead of spinning on a broken listener.
			l.failAccept(err)
			return
		}
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			l.admit(transport, release)
		}()
	}
}

// failAccept records the one terminal accept failure and wakes every blocked
// Accept. The terminal cause is stored separately from the diagnostic last
// refusal, so an in-flight admission refusal can never overwrite the cause that
// ended acceptance.
func (l *listener) failAccept(err error) {
	l.mu.Lock()
	if l.fatalCause == nil {
		l.fatalCause = err
	}
	l.mu.Unlock()
	l.fatalOnce.Do(func() {
		close(l.fatal)
	})
}

// fatalErr returns the terminal accept failure, if any. It is never the
// diagnostic last refusal.
func (l *listener) fatalErr() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fatalCause
}

// confirmRefusal records the most recent refused or failed admission. It is
// diagnostic only: a refusal is isolated to the peer that caused it.
func (l *listener) confirmRefusal(err error) {
	if err == nil {
		return
	}
	l.mu.Lock()
	l.lastRefuse = err
	l.mu.Unlock()
}

// refusal reports the most recent refused or failed admission.
func (l *listener) refusal() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastRefuse
}

// admit completes the preamble and broker admission for one accepted carriage
// and publishes the running session. Slot ownership transfers to the session on
// success.
func (l *listener) admit(transport wire.BoundedTransport, release func()) {
	l.trackPending(transport)
	session, err := l.startSession(transport, release)
	l.untrackPending(transport)
	if err != nil {
		release()
		l.confirmRefusal(err)
		return
	}
	select {
	case l.admitted <- session:
	case <-l.done:
		_ = session.Close()
	}
}

// trackPending records one accepted carriage that no running session owns yet,
// so Close can close it directly and unblock its framing I/O.
func (l *listener) trackPending(transport wire.BoundedTransport) {
	l.mu.Lock()
	l.pending[transport] = struct{}{}
	l.mu.Unlock()
}

// untrackPending drops one accepted carriage whose admission has finished.
func (l *listener) untrackPending(transport wire.BoundedTransport) {
	l.mu.Lock()
	delete(l.pending, transport)
	l.mu.Unlock()
}

// closePending closes every accepted carriage not yet owned by a running
// session, so a client stalled in framing or admission is disconnected at once
// rather than waiting out HandshakeTimeout.
func (l *listener) closePending() {
	l.mu.Lock()
	pending := make([]wire.BoundedTransport, 0, len(l.pending))
	for transport := range l.pending {
		pending = append(pending, transport)
	}
	l.mu.Unlock()
	for _, transport := range pending {
		_ = transport.Close()
	}
}

// inFlight reports how many accepted carriages are still completing preamble or
// admission.
func (l *listener) inFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pending)
}

// classifyAdmissionFailure maps one preamble or authority failure. Once the
// listener is closing, an accepted client that never reached admission was
// dropped by the close, so the failure is reported as ErrListenerClosed. A
// genuine handshake timeout with the listener still open keeps its
// context.DeadlineExceeded classification.
func (l *listener) classifyAdmissionFailure(err error) error {
	if l.isClosed() {
		return errors.Join(ErrListenerClosed, err)
	}
	return err
}

// Accept returns the next fully admitted client session, in accept order, and
// blocks until one is available, the listener closes, or the carriage reports a
// terminal accept failure. The returned ports.BrokerService is bound to exactly
// the connection admitted at accept; Close on the listener ends it.
//
// A peer the carriage refuses (a non-same-user credential, an unreadable
// credential, or an unsupported build) or a client that fails the preamble or
// admission is isolated: that peer is dropped and the listener keeps serving.
// The most recent refusal is available to diagnostics through refusal.
func (l *listener) Accept() (ports.BrokerService, error) {
	if l == nil {
		return nil, ErrListenerClosed
	}
	select {
	case session := <-l.admitted:
		return session, nil
	case <-l.fatal:
		select {
		case session := <-l.admitted:
			return session, nil
		default:
		}
		return nil, l.fatalErr()
	default:
	}
	select {
	case session := <-l.admitted:
		return session, nil
	case <-l.fatal:
		select {
		case session := <-l.admitted:
			return session, nil
		default:
		}
		return nil, l.fatalErr()
	case <-l.done:
		select {
		case session := <-l.admitted:
			return session, nil
		default:
		}
		return nil, ErrListenerClosed
	}
}

// startSession completes the preamble and admission for one accepted carriage
// and starts the session. The admission context is bounded to the handshake
// budget and is canceled as soon as admission returns or when the listener
// closes, so the admitted service must not retain it (see
// ports.BrokerAuthority) and Close can drop a client still in preamble or
// admission. Slot ownership transfers to the session on success.
func (l *listener) startSession(transport wire.BoundedTransport, release func()) (*serverSession, error) {
	// The handshake budget is bounded by the listener-scoped context, so
	// Listener.Close cancels a preamble or admission that is still waiting
	// instead of leaving it to run out HandshakeTimeout.
	ctx, cancel := context.WithTimeout(l.ctx, l.cfg.HandshakeTimeout)
	defer cancel()
	ceilings, err := runServerPreamble(ctx, transport, brokerwire.DefaultCeilings())
	if err != nil {
		_ = transport.Close()
		return nil, l.classifyAdmissionFailure(err)
	}
	core, err := l.authority.AdmitClient(ctx)
	if err != nil {
		_ = transport.Close()
		return nil, l.classifyAdmissionFailure(err)
	}
	if core == nil {
		_ = transport.Close()
		return nil, fmt.Errorf("%w: authority admitted a nil service", ErrConfig)
	}
	session, err := newServerSession(l.epoch, transport, ceilings, core, l.cfg, release, registrationDeadline(ctx))
	if err != nil {
		_ = core.Close()
		_ = transport.Close()
		return nil, err
	}
	// The session deregisters itself once it reaches its terminal state, so a
	// long-lived listener never accumulates retired connections.
	session.onShutdown = func() { l.forgetSession(session) }
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		// The run loop was never started, so Close (which waits on the run
		// loop's done channel) would wait forever. Tear the unstarted session
		// down directly instead.
		session.shutdown()
		return nil, ErrListenerClosed
	}
	l.sessions[session] = struct{}{}
	l.mu.Unlock()
	go session.run()
	return session, nil
}

// Close stops accepting, unblocks a blocked Accept, cancels every in-flight
// preamble and admission, closes every accepted carriage not yet owned by a
// running session, closes every live session (ending each connection and
// joining its workers), and unlinks only the socket inode this listener
// created. It is idempotent and concurrent-safe.
func (l *listener) Close() error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.closed = true
		l.mu.Unlock()
		// Mark the listener closed before canceling, so an admission that wakes
		// on the canceled context classifies its failure as ErrListenerClosed.
		close(l.done)
		l.cancel()
		l.closeErr = l.mux.Close()
		l.closePending()
		// The accept loop exits on the closed carriage; every admission
		// goroutine then finishes or drops its session because the listener is
		// closed.
		l.wg.Wait()
		drained := make([]*serverSession, 0, cap(l.admitted))
		for {
			select {
			case session := <-l.admitted:
				drained = append(drained, session)
				continue
			default:
			}
			break
		}
		l.mu.Lock()
		for session := range l.sessions {
			drained = append(drained, session)
		}
		l.sessions = make(map[*serverSession]struct{})
		l.mu.Unlock()
		seen := make(map[*serverSession]struct{}, len(drained))
		for _, session := range drained {
			if _, duplicate := seen[session]; duplicate {
				continue
			}
			seen[session] = struct{}{}
			if err := session.Close(); err != nil {
				l.closeErr = errors.Join(l.closeErr, err)
			}
		}
	})
	return l.closeErr
}

// Addr returns the bound endpoint path.
func (l *listener) Addr() string {
	if l == nil || l.mux == nil {
		return ""
	}
	return l.mux.Addr()
}

// forgetSession drops one retired session from the live set.
func (l *listener) forgetSession(session *serverSession) {
	l.mu.Lock()
	delete(l.sessions, session)
	l.mu.Unlock()
}

// liveSessions reports how many admitted sessions are still live.
func (l *listener) liveSessions() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.sessions)
}

func (l *listener) isClosed() bool {
	if l == nil {
		return true
	}
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// serverSession is one admitted broker connection: the brokerwire connection
// state machine, the outbound frame path, one snapshot publisher, and the
// bridged logical streams for exactly one client.
type serverSession struct {
	epoch     ports.BrokerEpoch
	scope     brokerwire.Scope
	conn      *brokerwire.Connection
	core      ports.BrokerService
	transport wire.BoundedTransport
	ceilings  brokerwire.Ceilings
	cfg       Config
	release   func()

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	subMu sync.Mutex
	pub   *publisher

	streamsMu sync.Mutex
	streams   map[ports.BrokerStreamID]*serverStream

	// onShutdown runs once, before done closes, so the owner can drop this
	// session from its live set. It is set before run starts.
	onShutdown func()

	// registerDeadline is the absolute instant by which the client's Register
	// must arrive: the accept-time handshake deadline that bounded the preamble
	// and admission. Until Register is accepted the session is guarded by it, so
	// a peer that stays silent after admission is settled instead of pinning the
	// admitted core lease and the listener slot. Once Register is accepted the
	// guard is stopped and the session runs on its own connection-lived context.
	registerDeadline time.Time
	// registered is set atomically the instant Register handling begins, before
	// any connection state is touched. The pre-Register guard reads it when the
	// registration deadline elapses, so a Register that arrives at the deadline
	// boundary is never settled as a peer timeout even if the guard wins the
	// race to the deadline.
	registered atomic.Bool
	// stopRegistration stops and joins the pre-Register guard. It is set by run
	// and called once from the Register dispatch; a no-op for a zero deadline.
	stopRegistration func()

	errMu    sync.Mutex
	closeErr error
	done     chan struct{}
}

var _ ports.BrokerService = (*serverSession)(nil)

// registrationDeadline returns the absolute instant by which an accepted client
// must send its Register: the accept-time handshake deadline that already
// bounded the preamble and admission. The session extends that same budget over
// the pre-Register wait, so a peer that completes the preamble and admission and
// then stays silent cannot hold a core client lease or a listener slot past the
// configured handshake budget. A missing deadline yields the zero time, which
// disables the bound.
func registrationDeadline(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}
	return deadline
}

// newServerSession binds one admitted connection to its exact scope.
//
// registerDeadline is the absolute instant by which the client's Register must
// arrive (the accept-time handshake deadline); the zero time disables the bound
// and exists only for direct construction outside the listener.
func newServerSession(epoch ports.BrokerEpoch, transport wire.BoundedTransport, ceilings brokerwire.Ceilings, core ports.BrokerService, cfg Config, release func(), registerDeadline time.Time) (*serverSession, error) {
	if epoch == 0 || transport == nil || core == nil {
		return nil, ErrConfig
	}
	connection := core.ConnectionID()
	if err := connection.Validate(); err != nil {
		return nil, errors.Join(ErrConfig, err)
	}
	if err := ceilings.Validate(); err != nil {
		return nil, errors.Join(ErrConfig, err)
	}
	conn, err := brokerwire.NewConnection(brokerwire.Scope{Epoch: epoch, Connection: connection})
	if err != nil {
		return nil, err
	}
	// The broker preamble was validated before this session was constructed,
	// so the connection moves past New immediately and waits for exactly one
	// Register.
	if err := conn.SignalPreamble(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	if release == nil {
		release = func() {}
	}
	return &serverSession{
		epoch:            epoch,
		scope:            conn.Scope(),
		conn:             conn,
		core:             core,
		transport:        transport,
		ceilings:         ceilings,
		cfg:              cfg,
		release:          release,
		ctx:              ctx,
		cancel:           cancel,
		streams:          make(map[ports.BrokerStreamID]*serverStream),
		registerDeadline: registerDeadline,
		done:             make(chan struct{}),
	}, nil
}

// run is the connection's single reader. It decodes one whole broker envelope at
// a time under the negotiated envelope and chunk ceilings; a malformed,
// oversize, or wrong-direction frame, a protocol-ordering violation, or a
// carriage error settles exactly this connection.
func (s *serverSession) run() {
	defer s.shutdown()
	// The pre-Register phase shares the accept-time handshake deadline that
	// bounded the preamble and admission. Arm it before the read loop and stop
	// (and join) it as soon as Register is accepted, so a silent peer cannot pin
	// the admitted core lease or the listener slot, and a healthy session is
	// never disturbed by it.
	s.stopRegistration = s.armRegistrationDeadline()
	defer s.stopRegistration()
	for {
		envelope, err := s.transport.RecvBounded(s.ceilings.MaxReceiveEnvelopeBytes)
		if err != nil {
			s.setCloseErr(transportFailure(err))
			return
		}
		message, err := brokerwire.DecodeClient(envelope.Payload, s.ceilings.MaxReceiveEnvelopeBytes, s.ceilings.StreamChunkLimit)
		if err != nil {
			s.setCloseErr(errors.Join(ErrMalformedFrame, err))
			return
		}
		if err := s.dispatch(message); err != nil {
			s.setCloseErr(err)
			return
		}
	}
}

// armRegistrationDeadline enforces the accept-time registration budget on the
// pre-Register phase. The peer must send its Register before the absolute
// deadline that already bounded the preamble and admission; if it does not, the
// guard settles the session, which closes the admitted core service and releases
// the listener slot. The bound is derived from the session's own connection-lived
// context, so shutdown cancels it too, and it is stopped as soon as Register is
// accepted.
//
// The guard consults the registered marker before settling: a Register whose
// handling already began is admitted even when the deadline elapses concurrently
// with it, so the boundary between an on-time Register and a silent peer is the
// marker, never a timer race.
//
// The returned stop function is idempotent and joins the guard goroutine, so an
// accepted (or settled) session never leaves a timer goroutine behind. A zero
// deadline disables the bound; only direct session construction uses that.
func (s *serverSession) armRegistrationDeadline() func() {
	if s.registerDeadline.IsZero() {
		return func() {}
	}
	ctx, cancel := context.WithDeadline(s.ctx, s.registerDeadline)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			// Only the registration budget itself settles the session; a session
			// cancel (shutdown) leaves the terminal cause to the reader. A
			// Register already being handled makes the budget moot: the marker is
			// set before any connection state is touched, so this check is the
			// deadline boundary.
			if errors.Is(ctx.Err(), context.DeadlineExceeded) && !s.registered.Load() {
				s.abort(errors.Join(ErrRegistrationTimeout, context.DeadlineExceeded))
			}
		case <-stop:
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			close(stop)
			<-done
		})
	}
}

// dispatch applies one client frame. A returned error is a protocol or carriage
// violation that settles the connection; a refusal the peer can observe is sent
// on the wire and returns nil.
func (s *serverSession) dispatch(message brokerwire.ClientMessage) error {
	switch m := message.(type) {
	case brokerwire.Register:
		// Mark registration as begun before touching connection state, so the
		// pre-Register guard sharing the accept-time deadline can distinguish a
		// Register arriving at the boundary from a silent peer and never settles
		// this healthy connection.
		s.registered.Store(true)
		if err := s.conn.Register(); err != nil {
			return errors.Join(ErrProtocol, err)
		}
		// The client met the accept-time registration budget: stop (and join)
		// the pre-Register guard before answering, so this healthy connection is
		// no longer bounded by the setup deadline.
		s.stopRegistration()
		return s.send(brokerwire.Registered{Epoch: s.epoch, Connection: s.scope.Connection})
	case brokerwire.Subscribe:
		if !s.scopeMatches(m.Epoch, m.Connection) {
			return s.refuseScope()
		}
		if err := s.conn.Subscribe(m.Generation); err != nil {
			return errors.Join(ErrProtocol, err)
		}
		return s.retargetPublisher(m.Generation)
	case brokerwire.Resync:
		if !s.scopeMatches(m.Epoch, m.Connection) {
			return s.refuseScope()
		}
		if err := s.conn.Resync(m.Generation); err != nil {
			return errors.Join(ErrProtocol, err)
		}
		s.wakePublisher()
		return nil
	case brokerwire.Unsubscribe:
		if !s.scopeMatches(m.Epoch, m.Connection) {
			return s.refuseScope()
		}
		if err := s.conn.Unsubscribe(m.Generation); err != nil {
			return errors.Join(ErrProtocol, err)
		}
		s.stopPublisher()
		return nil
	case brokerwire.AddHost:
		if !s.scopeMatches(m.Epoch, m.Connection) {
			return s.refuseScope()
		}
		return s.startMutation(m.Operation, func(ctx context.Context) (bool, error) {
			return false, s.core.AddHost(ctx, m.Endpoint)
		})
	case brokerwire.RemoveHost:
		if !s.scopeMatches(m.Epoch, m.Connection) {
			return s.refuseScope()
		}
		return s.startMutation(m.Operation, func(ctx context.Context) (bool, error) {
			removed, err := s.core.RemoveHost(ctx, m.Endpoint)
			return removed, err
		})
	case brokerwire.Reconcile:
		if !s.scopeMatches(m.Epoch, m.Connection) {
			return s.refuseScope()
		}
		s.core.RequestReconcile(m.Registration.Endpoint)
		return nil
	case brokerwire.OpenStream:
		if !s.scopeMatches(m.Epoch, m.Connection) {
			return s.refuseStream(m.Stream, scopeRefusal())
		}
		s.startStream(m)
		return nil
	case brokerwire.ClientStreamData:
		if !s.scopeMatches(m.Epoch, m.Connection) {
			return s.refuseScope()
		}
		return s.deliverStreamData(m)
	case brokerwire.CloseStream:
		if !s.scopeMatches(m.Epoch, m.Connection) {
			return s.refuseScope()
		}
		s.closeStreamByPeer(m.Stream)
		return nil
	default:
		return errors.Join(ErrProtocol, fmt.Errorf("brokeripc: unexpected client message %T", message))
	}
}

// scopeMatches reports whether one frame carries the epoch and connection
// identity assigned at accept. Every frame must: a mismatched frame is never
// applied to this connection's state.
func (s *serverSession) scopeMatches(epoch ports.BrokerEpoch, connection ports.BrokerConnectionID) bool {
	return epoch == s.epoch && connection == s.scope.Connection
}

// refuseScope drops one frame whose scope is not this connection's. It is
// deliberately not an error: the frame is fenced rather than obeyed, and the
// connection keeps serving its own scope. A stale stream open is answered with
// a typed stale refusal instead (see refuseStream).
func (s *serverSession) refuseScope() error { return nil }

// scopeRefusal is the typed refusal for a frame that does not carry this
// connection's accepted scope: a stale admission the peer can classify.
func scopeRefusal() error {
	return errors.Join(ErrScopeMismatch, ports.BrokerAdmissionStale)
}

// refuseStream tells the peer one logical stream was refused, with the closed
// admission or broker taxonomy carried on the detail.
func (s *serverSession) refuseStream(stream ports.BrokerStreamID, err error) error {
	detail := errorDetail(err)
	return s.send(brokerwire.StreamClosed{
		Epoch: s.epoch, Connection: s.scope.Connection, Stream: stream,
		Error: detail, HasError: detail != (brokerwire.ErrorDetail{}),
	})
}

// send encodes one server frame under the negotiated ceilings and writes it.
func (s *serverSession) send(message brokerwire.ServerMessage) error {
	payload, err := brokerwire.EncodeServer(message, s.ceilings.MaxReceiveEnvelopeBytes, s.ceilings.StreamChunkLimit)
	if err != nil {
		return err
	}
	if err := s.transport.Send(wire.Envelope{Payload: payload}); err != nil {
		return transportFailure(err)
	}
	return nil
}

// setCloseErr records the first terminal cause. Later causes never overwrite a
// cause that already settled the connection, so a diagnostic from a worker that
// observed the same terminal state cannot mask the original.
func (s *serverSession) setCloseErr(err error) {
	if err == nil {
		return
	}
	s.errMu.Lock()
	if s.closeErr == nil {
		s.closeErr = err
	}
	s.errMu.Unlock()
}

// terminalErr reports the cause that settled the connection, or nil for an
// orderly end.
func (s *serverSession) terminalErr() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.closeErr
}

// abort records a terminal cause from a worker goroutine and closes the
// carriage so the reader observes it promptly.
func (s *serverSession) abort(err error) {
	s.setCloseErr(err)
	_ = s.transport.Close()
}

// shutdown tears the session down exactly once, in a fixed order: cancel every
// owned context, stop the publisher, close the outer carriage (which unblocks a
// blocked read and any frame send), settle every bridged stream, close the
// brokerwire connection state, join every worker, release the stream
// resources, close the admitted core service, and release the listener's
// client slot.
//
// The outer carriage closes before the bridged streams are settled because
// settling a stream joins that stream's own carriage writer, and a writer
// parked on a non-reading peer is only interrupted when the outer carriage
// closes: settling streams first would wait on the peer instead of settling the
// connection, so the session slot and done would never be released.
func (s *serverSession) shutdown() {
	s.cancel()
	s.stopPublisher()
	_ = s.transport.Close()
	s.closeStreams()
	_ = s.conn.BeginClose()
	s.wg.Wait()
	_ = s.conn.FinishClose()
	if s.core != nil {
		if err := s.core.Close(); err != nil {
			s.setCloseErr(err)
		}
	}
	if s.release != nil {
		s.release()
	}
	if s.onShutdown != nil {
		s.onShutdown()
	}
	close(s.done)
}

// Close ends the connection and waits for every worker to stop. It is
// idempotent and concurrent-safe. It reports a non-nil error only for a session
// that ended on a protocol violation or an owned-resource failure: a peer
// disconnect and a local close are orderly ends, so an owner draining accepted
// sessions does not see a peer's departure as its own failure.
func (s *serverSession) Close() error {
	if s == nil {
		return nil
	}
	_ = s.transport.Close()
	<-s.done
	err := s.terminalErr()
	if orderlyDisconnect(err) {
		return nil
	}
	return err
}

// orderlyDisconnect reports whether one terminal cause is a peer disconnect or a
// local close rather than a failure. A registration timeout is a peer that
// completed admission and then stayed silent: the session settles, but the
// outcome is the peer's departure, so a listener draining its sessions never
// reports it as its own failure.
//
// A peer that closes abruptly can surface a reset (or a local-close sentinel
// from a concurrently interrupted read) instead of a clean EOF; both are still
// the peer's or this side's ordinary departure, never a sandbox failure.
func orderlyDisconnect(err error) bool {
	return err == nil ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, fs.ErrClosed) ||
		errors.Is(err, ErrConnectionClosed) ||
		errors.Is(err, ErrSessionClosed) ||
		errors.Is(err, ErrRegistrationTimeout)
}

// ConnectionID returns the identity assigned to this connection at accept.
func (s *serverSession) ConnectionID() ports.BrokerConnectionID { return s.scope.Connection }

// Snapshot returns the admitted core service's current publication.
func (s *serverSession) Snapshot() ports.BrokerSnapshot { return s.core.Snapshot() }

// Subscribe returns the admitted core service's subscription. The wire
// protocol's Subscribe is driven by the connection's reader, which publishes
// the broker core's newest snapshot for the client's generation.
func (s *serverSession) Subscribe() (ports.BrokerSubscription, error) { return s.core.Subscribe() }

// OpenStream delegates one scope-checked stream request to the admitted core
// service. The connection's own reader drives the same core through
// openStreamMessage, which additionally bridges the stream onto the wire; this
// method exists so a composed caller can use the session as a scoped
// ports.BrokerService.
func (s *serverSession) OpenStream(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	if err := s.checkRequestScope(request.Epoch, request.Connection); err != nil {
		return nil, err
	}
	scoped := request
	scoped.Epoch = s.epoch
	scoped.Connection = s.scope.Connection
	return s.core.OpenStream(ctx, scoped)
}

// CloseStream retires one scope-checked stream. An absent connection identity is
// filled from this session exactly like OpenStream, so a zero identity never
// reaches the core as a stale request, and a foreign one is still refused.
func (s *serverSession) CloseStream(connection ports.BrokerConnectionID, stream ports.BrokerStreamID) error {
	if err := s.checkRequestScope(s.epoch, connection); err != nil {
		return err
	}
	return s.core.CloseStream(s.scope.Connection, stream)
}

// AddHost delegates one mutating operation to the admitted core service.
func (s *serverSession) AddHost(ctx context.Context, target string) error {
	return s.core.AddHost(ctx, target)
}

// RemoveHost delegates one mutating operation to the admitted core service.
func (s *serverSession) RemoveHost(ctx context.Context, target string) (bool, error) {
	return s.core.RemoveHost(ctx, target)
}

// RequestReconcile delegates one reconcile hint to the admitted core service.
func (s *serverSession) RequestReconcile(endpoint string) { s.core.RequestReconcile(endpoint) }

// checkRequestScope refuses a caller-supplied identity that is neither absent
// nor this connection's own, so a caller can never steer another connection's
// work through this session.
func (s *serverSession) checkRequestScope(epoch ports.BrokerEpoch, connection ports.BrokerConnectionID) error {
	if epoch != 0 && epoch != s.epoch {
		return errors.Join(ErrScopeMismatch, ports.BrokerError{Code: ports.BrokerErrorStaleEpoch})
	}
	if !connection.IsZero() && connection != s.scope.Connection {
		return errors.Join(ErrScopeMismatch, ports.BrokerAdmissionStale)
	}
	return nil
}
