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
type poolClient struct {
	high    ports.BrokerStreamID
	streams map[ports.BrokerStreamID]*reservation
}
type reservation struct{ cancel context.CancelFunc }
type poolEntry struct {
	key         poolKey
	ctx         context.Context
	cancel      context.CancelFunc
	ready, wake chan struct{}
	physical    ports.BrokerPhysicalConnection
	err         error
	refs        int
	retiring    bool
}

// Pool is transport independent and is not production-composed in P2.2.
// mu protects bookkeeping only; no port I/O or Close runs under it.
type Pool struct {
	mu        sync.Mutex
	epoch     ports.BrokerEpoch
	resolver  ports.BrokerEndpointResolver
	connector ports.BrokerEndpointConnector
	clock     ports.Clock
	limits    PoolLimits
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	next      uint64
	clients   map[ports.BrokerConnectionID]*poolClient
	entries   map[poolKey]*poolEntry
	streams   int
	wg        sync.WaitGroup
}

func NewPool(epoch ports.BrokerEpoch, resolver ports.BrokerEndpointResolver, connector ports.BrokerEndpointConnector, clock ports.Clock, limits PoolLimits) (*Pool, error) {
	if epoch == 0 || resolver == nil || connector == nil || clock == nil || limits.Physical <= 0 || limits.Clients <= 0 || limits.Streams <= 0 || limits.StreamsPerClient <= 0 || limits.Idle <= 0 {
		return nil, ports.BrokerAdmissionInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{epoch: epoch, resolver: resolver, connector: connector, clock: clock, limits: limits, ctx: ctx, cancel: cancel, clients: make(map[ports.BrokerConnectionID]*poolClient), entries: make(map[poolKey]*poolEntry)}, nil
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
	if c == nil || req.Stream <= c.high {
		p.mu.Unlock()
		cancel()
		return nil, ports.BrokerAdmissionStale
	}
	c.high = req.Stream
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
		if entry != nil {
			entry.refs--
			if entry.refs == 0 {
				select {
				case <-entry.ready:
				default:
					entry.retiring = true
					abandon = entry.cancel
				}
			}
			select {
			case entry.wake <- struct{}{}:
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
	resolved, err := p.resolver.Resolve(ctx, req)
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
	key := poolKey{resolved.Identity, resolved.Policy}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ports.BrokerAdmissionClosed
	}
	entry = p.entries[key]
	if entry != nil && entry.retiring {
		entry = nil
		p.mu.Unlock()
		return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable}
	}
	if entry == nil {
		if len(p.entries) >= p.limits.Physical {
			p.mu.Unlock()
			return nil, ports.BrokerAdmissionLimit
		}
		ectx, ecancel := context.WithCancel(p.ctx)
		entry = &poolEntry{key: key, ctx: ectx, cancel: ecancel, ready: make(chan struct{}), wake: make(chan struct{}, 1)}
		p.entries[key] = entry
		p.wg.Add(1)
		go p.runEntry(entry, resolved)
	}
	entry.refs++
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, normalizePoolError(ctx.Err())
	case <-entry.ready:
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

func (p *Pool) runEntry(e *poolEntry, endpoint ports.BrokerResolvedEndpoint) {
	defer p.wg.Done()
	physical, err := p.connector.Connect(e.ctx, endpoint)
	if err == nil && (physical == nil || physical.Identity() != endpoint.Identity || !physical.Policy().Compatible(endpoint.Policy) || physical.Incarnation().Validate() != nil) {
		err = ports.BrokerError{Code: ports.BrokerErrorIncompatible}
	}
	e.physical = physical
	e.err = normalizePoolError(err)
	close(e.ready)
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
	delete(p.entries, e.key)
	p.mu.Unlock()
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
