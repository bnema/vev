package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// PoolLimits bound live physical keys, clients, reservations (including I/O in
// flight), and logical streams. No unbounded queue or alias cache is retained.
type PoolLimits struct {
	Physical, Clients, Streams, StreamsPerClient int
	Idle                                         time.Duration
}

// poolKey is the exact authenticated (identity, policy) pair the broker pools
// one physical transport by. The resolved endpoint's opaque address is
// deliberately excluded: it selects the transport, not the pooling identity.
//
// A local stream request carries no endpoint and no registration, so its key is
// the broker-owned local identity and policy the resolver returns. Every local
// stream therefore shares one pooled physical connection to the local daemon,
// exactly as every stream to one remote identity does; the local daemon is not
// a configured host, so no registration participates in its key.
type poolKey struct {
	identity ports.BrokerDaemonIdentity
	policy   ports.BrokerPolicy
}
type pendingKey struct {
	fence  ports.BrokerEndpointFence
	policy ports.BrokerPolicy
	// startMode is part of the pending key on purpose: an acquisition that may
	// start the target daemon must never be coalesced with an ExistingOnly
	// acquisition that must not, even when they name the same unbound endpoint
	// and policy. Once the physical connection is authenticated and published,
	// the canonical key is identity plus exact policy alone: a compatible
	// transport is shareable regardless of how it was acquired.
	startMode ports.BrokerDaemonStartMode
}
type poolClient struct {
	// window is this client's bounded anti-replay admission window. Stream IDs
	// are allocated by the calling service and may arrive out of order when two
	// opens race, so a plain monotone high-water mark would refuse a valid
	// concurrent open; the window admits every unconsumed ID inside it and
	// refuses duplicates and IDs evicted past its bound.
	window  ports.BrokerStreamWindow
	streams map[ports.BrokerStreamID]*reservation
}
type reservation struct {
	cancel context.CancelFunc
	entry  *poolEntry
}
type poolEntry struct {
	key         poolKey
	pending     pendingKey
	ctx         context.Context
	cancel      context.CancelFunc
	ready, wake chan struct{}
	physical    ports.BrokerPhysicalConnection
	redirect    *poolEntry
	err         error
	refs        int
	retiring    bool
}

// Pool is transport independent and is not production-composed in P2.2.
// mu protects bookkeeping only; no port I/O or Close runs under it.
type Pool struct {
	mu        sync.Mutex
	epoch     ports.BrokerEpoch
	routes    ports.BrokerRouteAuthority
	binder    ports.BrokerIdentityBinder
	connector ports.BrokerEndpointConnector
	clock     ports.Clock
	limits    PoolLimits
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	next      uint64
	clients   map[ports.BrokerConnectionID]*poolClient
	entries   map[poolKey]*poolEntry
	pending   map[pendingKey]*poolEntry
	streams   int
	wg        sync.WaitGroup
}

func NewPool(epoch ports.BrokerEpoch, routes ports.BrokerRouteAuthority, binder ports.BrokerIdentityBinder, connector ports.BrokerEndpointConnector, clock ports.Clock, limits PoolLimits) (*Pool, error) {
	if epoch == 0 || routes == nil || binder == nil || connector == nil || clock == nil || limits.Physical <= 0 || limits.Clients <= 0 || limits.Streams <= 0 || limits.StreamsPerClient <= 0 || limits.Idle <= 0 {
		return nil, ports.BrokerAdmissionInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{epoch: epoch, routes: routes, binder: binder, connector: connector, clock: clock, limits: limits, ctx: ctx, cancel: cancel, clients: make(map[ports.BrokerConnectionID]*poolClient), entries: make(map[poolKey]*poolEntry), pending: make(map[pendingKey]*poolEntry)}, nil
}

// RegisterClient issues epoch-scoped nonreusable identities. The bounded active
// map needs no retired-ID tombstones. Stream IDs must increase within a client.
func (p *Pool) RegisterClient() (ports.BrokerConnectionID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ports.BrokerConnectionID{}, ports.BrokerAdmissionClosed
	}
	if len(p.clients) >= p.limits.Clients || p.next == ^uint64(0) {
		return ports.BrokerConnectionID{}, ports.BrokerAdmissionLimit
	}
	p.next++
	var id ports.BrokerConnectionID
	binary.BigEndian.PutUint64(id[:8], uint64(p.epoch))
	binary.BigEndian.PutUint64(id[8:], p.next)
	p.clients[id] = &poolClient{streams: make(map[ports.BrokerStreamID]*reservation)}
	return id, nil
}

func (p *Pool) CloseClient(id ports.BrokerConnectionID) {
	p.mu.Lock()
	c := p.clients[id]
	delete(p.clients, id)
	var cancels []context.CancelFunc
	if c != nil {
		for _, r := range c.streams {
			cancels = append(cancels, r.cancel)
		}
	}
	p.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (p *Pool) CloseStream(id ports.BrokerConnectionID, stream ports.BrokerStreamID) error {
	p.mu.Lock()
	c := p.clients[id]
	var r *reservation
	if c != nil {
		r = c.streams[stream]
	}
	p.mu.Unlock()
	if r == nil {
		return ports.BrokerAdmissionStale
	}
	r.cancel()
	return nil
}

func (p *Pool) OpenStream(ctx context.Context, req ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	if err := req.Validate(); err != nil {
		return nil, errors.Join(ports.BrokerAdmissionInvalid, err)
	}
	if req.Epoch != p.epoch {
		return nil, ports.BrokerError{Code: ports.BrokerErrorStaleEpoch}
	}
	ctx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	c := p.clients[req.Connection]
	if p.closed {
		p.mu.Unlock()
		cancel()
		return nil, ports.BrokerAdmissionClosed
	}
	if c == nil {
		p.mu.Unlock()
		cancel()
		return nil, ports.BrokerAdmissionStale
	}
	if err := c.window.Admit(req.Stream); err != nil {
		p.mu.Unlock()
		cancel()
		return nil, err
	}
	if p.streams >= p.limits.Streams || len(c.streams) >= p.limits.StreamsPerClient {
		p.mu.Unlock()
		cancel()
		return nil, ports.BrokerAdmissionLimit
	}
	r := &reservation{cancel: cancel}
	c.streams[req.Stream] = r
	p.streams++
	p.wg.Add(1)
	p.mu.Unlock()
	stop := context.AfterFunc(p.ctx, cancel)
	var entry *poolEntry
	release := func() {
		stop()
		cancel()
		p.mu.Lock()
		delete(c.streams, req.Stream)
		p.streams--
		var abandon context.CancelFunc
		reservedEntry := r.entry
		if reservedEntry != nil {
			reservedEntry.refs--
			if reservedEntry.refs == 0 {
				select {
				case <-reservedEntry.ready:
				default:
					reservedEntry.retiring = true
					abandon = reservedEntry.cancel
				}
			}
			select {
			case reservedEntry.wake <- struct{}{}:
			default:
			}
		}
		p.mu.Unlock()
		if abandon != nil {
			abandon()
		}
		p.wg.Done()
	}
	success := false
	defer func() {
		if !success {
			release()
		}
	}()
	req.Env = append([]string(nil), req.Env...)
	resolved, err := p.routes.ResolveDialTarget(ctx, req)
	if err != nil {
		return nil, normalizePoolError(err)
	}
	if err := resolved.Validate(); err != nil {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	if !resolved.Policy.Compatible(req.Policy) {
		return nil, ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy}
	}
	if err = ctx.Err(); err != nil {
		return nil, normalizePoolError(err)
	}
	key := poolKey{resolved.ExpectedIdentity.Identity, resolved.Policy}
	pending := pendingKey{fence: resolved.Fence, policy: resolved.Policy, startMode: resolved.StartMode}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ports.BrokerAdmissionClosed
	}
	if resolved.ExpectedIdentity.Bound {
		entry = p.entries[key]
	} else {
		entry = p.pending[pending]
	}
	if entry != nil && entry.retiring {
		p.mu.Unlock()
		return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "selected physical entry is retiring", Cause: errors.New("broker: selected physical entry is retiring")}
	}
	if entry == nil {
		if len(p.entries)+len(p.pending) >= p.limits.Physical {
			p.mu.Unlock()
			return nil, ports.BrokerAdmissionLimit
		}
		ectx, ecancel := context.WithCancel(p.ctx)
		entry = &poolEntry{key: key, pending: pending, ctx: ectx, cancel: ecancel, ready: make(chan struct{}), wake: make(chan struct{}, 1)}
		if resolved.ExpectedIdentity.Bound {
			p.entries[key] = entry
		} else {
			p.pending[pending] = entry
		}
		p.wg.Add(1)
		go p.runEntry(entry, resolved)
	}
	entry.refs++
	r.entry = entry
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, normalizePoolError(ctx.Err())
	case <-entry.ready:
	}
	winner, err := p.redirectReservation(entry, r)
	if err != nil {
		return nil, err
	}
	if winner != nil {
		entry = winner
		select {
		case <-ctx.Done():
			return nil, normalizePoolError(ctx.Err())
		case <-entry.ready:
		}
	}
	if entry.err != nil {
		return nil, entry.err
	}
	openCtx, openCancel := context.WithCancel(ctx)
	stopEntry := context.AfterFunc(entry.ctx, openCancel)
	raw, err := entry.physical.OpenStream(openCtx, req)
	stopEntry()
	openCancel()
	if err != nil {
		// A failed open may still carry a typed-nil connection, which compares
		// unequal to nil while holding no receiver: closing it would dereference
		// a nil pointer.
		if !nilDependency(raw) {
			_ = raw.Close()
		}
		return nil, p.streamTerminal(ctx, entry, req, normalizePoolError(err))
	}
	if nilDependency(raw) {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: ports.BrokerAdmissionInvalid}
	}
	if terminal := p.streamTerminal(ctx, entry, req, nil); terminal != nil {
		_ = raw.Close()
		return nil, terminal
	}
	stream := &pooledStream{BrokerLogicalConnection: raw, done: make(chan struct{}), release: release}
	success = true
	go func() {
		var terminal error
		select {
		case <-ctx.Done():
			terminal = ports.BrokerError{Code: ports.BrokerErrorCancelled}
		case <-entry.ctx.Done():
			terminal = ports.BrokerError{Code: ports.BrokerErrorAttachmentLost}
		case <-entry.physical.Done():
		case <-raw.Done():
			terminal = raw.Err()
		case <-stream.done:
			return
		}
		stream.finish(p.streamTerminal(ctx, entry, req, terminal))
	}()
	return stream, nil
}

func (p *Pool) redirectReservation(entry *poolEntry, reservation *reservation) (*poolEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	winner := entry.redirect
	if winner == nil {
		return nil, nil
	}
	if current := p.entries[winner.key]; current != winner || winner.retiring {
		return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "physical identity redirect unavailable"}
	}
	entry.refs--
	winner.refs++
	reservation.entry = winner
	return winner, nil
}

func (p *Pool) runEntry(e *poolEntry, endpoint ports.BrokerDialTarget) {
	defer p.wg.Done()
	physical, err := p.connector.Connect(e.ctx, endpoint)
	if err == nil && (nilDependency(physical) || (endpoint.ExpectedIdentity.Bound && physical.Identity() != endpoint.ExpectedIdentity.Identity) || physical.Identity().Validate() != nil || !physical.Policy().Compatible(endpoint.Policy) || physical.Incarnation().Validate() != nil) {
		err = ports.BrokerError{Code: ports.BrokerErrorIncompatible}
	}
	if err == nil {
		identity, bindErr := p.binder.BindAuthenticatedIdentity(e.ctx, ports.BrokerIdentityBindingRequest{Fence: endpoint.Fence, Policy: endpoint.Policy, Identity: physical.Identity()})
		if bindErr != nil {
			err = bindErr
		} else if identity != physical.Identity() {
			err = ports.BrokerError{Code: ports.BrokerErrorIncompatible}
		}
	}
	p.mu.Lock()
	if !endpoint.ExpectedIdentity.Bound {
		delete(p.pending, e.pending)
	}
	if err == nil {
		e.key = poolKey{identity: physical.Identity(), policy: endpoint.Policy}
		if winner := p.entries[e.key]; winner != nil && winner != e && !winner.retiring {
			e.redirect = winner
		} else {
			p.entries[e.key] = e
		}
	}
	e.physical = physical
	e.err = normalizePoolError(err)
	close(e.ready)
	redirect := e.redirect
	p.mu.Unlock()
	if redirect != nil {
		_ = physical.Close()
		e.cancel()
		return
	}
	if err == nil {
		timer := p.clock.NewTimer(p.limits.Idle)
		defer stopPoolTimer(timer)
		for {
			p.mu.Lock()
			idle := e.refs == 0
			p.mu.Unlock()
			stopPoolTimer(timer)
			if idle {
				timer.Reset(p.limits.Idle)
			}
			select {
			case <-e.ctx.Done():
				goto retire
			case <-physical.Done():
				goto retire
			case <-e.wake:
				continue
			case <-timer.C():
				p.mu.Lock()
				if e.refs == 0 {
					e.retiring = true
					p.mu.Unlock()
					goto retire
				}
				p.mu.Unlock()
			}
		}
	}
retire:
	p.mu.Lock()
	e.retiring = true
	p.mu.Unlock()
	e.cancel()
	if physical != nil {
		_ = physical.Close()
	}
	// Keep the key occupied until Close completes: never overlap physicals.
	p.mu.Lock()
	if p.entries[e.key] == e {
		delete(p.entries, e.key)
	}
	if p.pending[e.pending] == e {
		delete(p.pending, e.pending)
	}
	p.mu.Unlock()
}

// SharedPhysical returns the published physical transport the pool holds for
// one authenticated identity and exact policy, active or warm, without
// reserving it. Observation borrows it to avoid a second bootstrap; the
// borrower must not close it and must tolerate its retirement at any time.
// Borrowing never refreshes warm order or the idle deadline.
func (p *Pool) SharedPhysical(identity ports.BrokerDaemonIdentity, policy ports.BrokerPolicy) (ports.BrokerPhysicalConnection, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.entries[poolKey{identity: identity, policy: policy}]
	if p.closed || e == nil || e.retiring || e.redirect != nil {
		return nil, false
	}
	select {
	case <-e.ready:
	default:
		return nil, false
	}
	if e.err != nil || nilDependency(e.physical) {
		return nil, false
	}
	select {
	case <-e.physical.Done():
		return nil, false
	default:
	}
	return e.physical, true
}

// Close cancels pending operations, closes transports, and joins all owned
// workers. Bounded completion depends on the explicit cancellation/Close port
// contract; callers must not supply adapters that ignore it.
func (p *Pool) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cancel()
	p.wg.Wait()
	return nil
}

type pooledStream struct {
	ports.BrokerLogicalConnection
	once     sync.Once
	done     chan struct{}
	err      error
	closeErr error
	release  func()
}

func (s *pooledStream) Done() <-chan struct{} { return s.done }
func (s *pooledStream) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}
func (s *pooledStream) Close() error { s.finish(nil); return s.closeErr }
func (s *pooledStream) finish(err error) {
	s.once.Do(func() { s.err = err; s.closeErr = s.BrokerLogicalConnection.Close(); s.release(); close(s.done) })
}

// One goroutine owns this timer. Drain a buffered expiry before every reset,
// including clocks implementing pre-Go-1.23 timer semantics.
func stopPoolTimer(timer ports.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
}

func normalizePoolError(err error) error {
	if err == nil {
		return nil
	}
	code := ports.BrokerErrorUnavailable
	var typed ports.BrokerError
	var typedPtr *ports.BrokerError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		code = ports.BrokerErrorTimeout
	case errors.Is(err, context.Canceled):
		code = ports.BrokerErrorCancelled
	case errors.As(err, &typed) && typed.Validate() == nil:
		code = typed.Code
	case errors.As(err, &typedPtr) && typedPtr != nil && typedPtr.Validate() == nil:
		code = typedPtr.Code
	}
	return ports.BrokerError{Code: code, Cause: err}
}

// Precedence at terminal observation: pool shutdown, request/client cancellation,
// physical Done, entry retirement, logical result. A completed stream is immutable.
func (p *Pool) streamTerminal(ctx context.Context, e *poolEntry, req ports.BrokerOpenStreamRequest, fallback error) error {
	if p.ctx.Err() != nil {
		return ports.BrokerError{Code: ports.BrokerErrorCancelled, Cause: p.ctx.Err()}
	}
	if ctx.Err() != nil {
		return normalizePoolError(ctx.Err())
	}
	select {
	case <-e.physical.Done():
		kind := e.physical.FailureKind()
		if kind < domain.RemoteFailureTransport || kind > domain.RemoteFailureInvalidResponse {
			kind = domain.RemoteFailureTransport
		}
		return ports.BrokerStreamLost{Epoch: req.Epoch, Connection: req.Connection, Stream: req.Stream, Cause: kind, Err: e.physical.Err()}
	default:
	}
	if e.ctx.Err() != nil {
		return ports.BrokerError{Code: ports.BrokerErrorAttachmentLost}
	}
	return normalizePoolError(fallback)
}
