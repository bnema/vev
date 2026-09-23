package broker

import (
	"context"
	"errors"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Offline broker composition.
//
// Authority admits one accepted client connection to the broker core and
// returns the connection-scoped ports.BrokerCoreService for exactly that
// connection. It is the structural implementation of ports.BrokerAuthority,
// the admission seam the broker IPC listener (internal/adapters/brokeripc)
// consumes and which this use case must not import: admission obtains one
// supervisor client lease and one pool connection
// identity, and rolls both back on any failure so a refused admission never
// pins the broker open or leaks a client slot. The admission context bounds
// admission only and is never retained; the admitted service owns a
// connection-lived context derived from the supervisor root.
//
// Registry construction determines observation and membership policy. The
// authority preserves those guards and exposes admitted mutations through the
// connection-scoped service.

// Authority admits one accepted client connection to the broker core. It owns
// no lifetime of its own: the supervisor owns the broker lifetime, the registry
// owns snapshots, and the pool owns transports. An admitted Service is bound to
// exactly one pool client identity and one broker epoch.
//
// It is the structural implementation of ports.BrokerAuthority, the admission
// seam the broker IPC listener consumes; this use case never imports that
// adapter.
type Authority struct {
	epoch      ports.BrokerEpoch
	registry   *Registry
	pool       *Pool
	supervisor *Supervisor
	clock      ports.Clock
	codec      ports.SessionCodec
}

var _ ports.BrokerAuthority = (*Authority)(nil)

// NewAuthority composes one connection-scoped broker authority over an existing
// registry, pool, and supervisor. All three must be live and share epoch.
//
// The caller owns the dependencies' lifecycles. In particular it must start the
// registry's single Registry.Run to own and drain the durable writer, and must
// cancel that run context and join Run before the store is closed. NewAuthority
// neither starts nor settles Run: serving connections without a live Run leaks
// the writer and never flushes the newest staged publication, and a registry
// that was never run leaves its durable writer undrained.
func NewAuthority(epoch ports.BrokerEpoch, registry *Registry, pool *Pool, supervisor *Supervisor, codec ports.SessionCodec, clocks ...ports.Clock) (*Authority, error) {
	if epoch == 0 || nilDependency(registry) || nilDependency(pool) || nilDependency(supervisor) {
		return nil, errors.New("broker: invalid authority dependencies")
	}
	if registry.epoch != epoch || pool.epoch != epoch {
		return nil, errors.New("broker: authority epoch mismatch")
	}
	clk := registry.clock
	if len(clocks) > 0 {
		clk = clocks[0]
	}
	if nilDependency(clk) || nilDependency(codec) {
		return nil, errors.New("broker: invalid authority clock")
	}
	return &Authority{epoch: epoch, registry: registry, pool: pool, supervisor: supervisor, clock: clk, codec: codec}, nil
}

// AdmitClient admits exactly one accepted client connection. The supervisor
// client lease and the pool connection identity are acquired together; if
// either fails, the other is released before returning, so admission either
// commits both or neither. ctx bounds admission only and is never retained: the
// returned service derives its own connection-lived context from the supervisor
// root, so a caller's setup deadline cannot disturb an admitted connection.
func (a *Authority) AdmitClient(ctx context.Context) (ports.BrokerCoreService, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lease, err := a.supervisor.AdmitClient()
	if err != nil {
		return nil, err
	}
	id, err := a.pool.RegisterClient()
	if err != nil {
		lease.Release()
		return nil, err
	}
	previewID, err := a.pool.RegisterClient()
	if err != nil {
		a.pool.CloseClient(id)
		lease.Release()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		a.pool.CloseClient(previewID)
		a.pool.CloseClient(id)
		lease.Release()
		return nil, err
	}
	return newService(serviceConfig{
		epoch: a.epoch, id: id, previewID: previewID, registry: a.registry,
		pool: a.pool, supervisor: a.supervisor, lease: lease, clock: a.clock, codec: a.codec,
	}), nil
}

// Service is one admitted client connection: the core's service for exactly one
// pool connection identity and broker epoch. It implements ports.BrokerCoreService.
type Service struct {
	epoch      ports.BrokerEpoch
	id         ports.BrokerConnectionID
	previewID  ports.BrokerConnectionID
	registry   *Registry
	pool       *Pool
	supervisor *Supervisor
	lease      *Lease
	clock      ports.Clock
	codec      ports.SessionCodec

	// ctx owns everything this connection started: in-flight setup opens and
	// live streams. It is derived from the supervisor root, never from the
	// admission context, so broker shutdown and Close both abort owned work.
	ctx    context.Context
	cancel context.CancelFunc

	mu                sync.Mutex
	closed            bool
	subs              map[*serviceSubscription]struct{}
	preview           *servicePreviewSubscription
	previewGeneration ports.BrokerPreviewGeneration
	// nextStream is this connection's strictly increasing, never-zero logical
	// stream ID allocator. It is the only allocator on this connection: the
	// supervisor and the broker operations each read one identity here and carry
	// it on OpenStream rather than relying on an implicit allocation there. An
	// allocated identity is consumed even when the open it names is refused.
	nextStream    ports.BrokerStreamID
	previewStream ports.BrokerStreamID
	// wg counts in-flight stream opens and membership mutations so Close drains
	// them, and their operation leases, before releasing connection resources.
	wg sync.WaitGroup

	closeOnce sync.Once

	// done closes exactly once when this connection is terminal, either because
	// the broker root shut down or because Close settled it locally. Err is
	// stable afterwards (nil for an orderly local Close). stopRoot detaches the
	// root watcher once Close has committed so a closed connection never leaves
	// a root callback behind.
	done     chan struct{}
	stopRoot func() bool
	termOnce sync.Once
	termErr  error
}

var _ ports.BrokerCoreService = (*Service)(nil)

type serviceConfig struct {
	epoch         ports.BrokerEpoch
	id, previewID ports.BrokerConnectionID
	registry      *Registry
	pool          *Pool
	supervisor    *Supervisor
	lease         *Lease
	clock         ports.Clock
	codec         ports.SessionCodec
}

func (c serviceConfig) validate() error {
	if c.epoch == 0 || c.id == (ports.BrokerConnectionID{}) || c.previewID == (ports.BrokerConnectionID{}) || c.id == c.previewID ||
		nilDependency(c.registry) || nilDependency(c.pool) || nilDependency(c.supervisor) || nilDependency(c.lease) || nilDependency(c.clock) || nilDependency(c.codec) {
		return errors.New("broker: invalid service dependencies")
	}
	return nil
}

func newService(c serviceConfig) *Service {
	if err := c.validate(); err != nil {
		panic(err) // Only the admitted authority constructs a service after acquiring both IDs and lease.
	}
	ctx, cancel := context.WithCancel(c.supervisor.RootContext())
	s := &Service{
		epoch: c.epoch, id: c.id, previewID: c.previewID, registry: c.registry, pool: c.pool, supervisor: c.supervisor, lease: c.lease, clock: c.clock, codec: c.codec,
		ctx: ctx, cancel: cancel, subs: make(map[*serviceSubscription]struct{}),
		done: make(chan struct{}),
	}
	// Broker shutdown ends every admitted connection. The watcher records the
	// loss as the terminal cause unless a local Close has already settled the
	// connection, so a connection closed by its owner reports nil and one lost
	// to a broker shutdown reports the typed loss.
	s.stopRoot = context.AfterFunc(c.supervisor.RootContext(), func() {
		s.terminalize(ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker shutdown", Cause: context.Canceled})
	})
	return s
}

// ConnectionID returns the pool identity assigned at admission. Clients carry
// it on stream operations so a stale request can be fenced.
func (s *Service) ConnectionID() ports.BrokerConnectionID { return s.id }

// NextStreamID allocates this connection's next logical stream identity. It is
// thread-safe, strictly monotone, and never zero, performs no I/O, and refuses
// a closed connection or an exhausted counter rather than wrapping. It is the
// only allocation seam on this connection: supervisor and BrokerOperations both
// allocate here and carry the resulting identity on the request.
func (s *Service) NextStreamID() (ports.BrokerStreamID, error) {
	if s == nil {
		return 0, ports.BrokerAdmissionClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ports.BrokerAdmissionClosed
	}
	if s.nextStream == ^ports.BrokerStreamID(0) {
		return 0, ports.BrokerAdmissionLimit
	}
	s.nextStream++
	return s.nextStream, nil
}

// Done closes exactly once when this connection is terminal: the broker root
// shut down or Close settled it locally. Err is stable afterwards.
func (s *Service) Done() <-chan struct{} { return s.done }

// Err returns this connection's terminal cause: nil for an orderly local Close,
// otherwise the broker-side loss that settled it. The cause is recorded exactly
// once and never overwritten, so it is stable once Done is closed.
func (s *Service) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.termErr
}

// terminalize records the terminal cause and closes Done exactly once. The
// first cause wins, so a race between a broker-root loss and a local Close
// yields one stable, observable outcome.
func (s *Service) terminalize(err error) {
	s.termOnce.Do(func() {
		s.mu.Lock()
		s.termErr = err
		s.mu.Unlock()
		close(s.done)
	})
}

// Snapshot delegates to the registry's current immutable publication.
func (s *Service) Snapshot() ports.BrokerSnapshot { return s.registry.Snapshot() }

// Subscribe delegates to the registry and tracks the returned subscription so
// Close releases every subscription this connection still owns. A closed
// connection refuses new subscriptions. Opening a subscription also records
// one unit of registry demand (Registry.SetDemand(true)): while a client
// watches the snapshot, the registry observes on the demand cadence. Without
// a subscriber it performs no scheduled observations. Close balances demand.
func (s *Service) Subscribe() (ports.BrokerSubscription, error) {
	return s.subscribe(true)
}

// SubscribePassive is Subscribe for a read-only snapshot reader (vev ls, vev
// host list): it receives every publication but records no demand, so reading
// state never triggers an observation.
func (s *Service) SubscribePassive() (ports.BrokerSubscription, error) {
	return s.subscribe(false)
}

func (s *Service) subscribe(demand bool) (ports.BrokerSubscription, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ports.BrokerAdmissionClosed
	}
	sub := &serviceSubscription{service: s, inner: s.registry.Subscribe(), demand: demand}
	s.subs[sub] = struct{}{}
	if demand {
		s.registry.SetDemand(true)
	}
	s.mu.Unlock()
	return sub, nil
}

// OpenStream opens one logical stream through the pool on behalf of the user.
// The request is fenced to this connection's exact epoch and identity before
// the pool sees it, and one operation lease pins the broker only while setup is
// in flight: a stream that opens successfully outlives its setup lease, and
// Close or broker shutdown still aborts anything that has not finished.
func (s *Service) OpenEnvelopeStream(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerEnvelopeStream, error) {
	if err := s.scope(&request); err != nil {
		return nil, err
	}
	// Service admission owns validation of caller input before the pool sees it.
	if err := request.Validate(); err != nil {
		return nil, errors.Join(ports.BrokerAdmissionInvalid, err)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ports.BrokerAdmissionClosed
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	operation, err := s.supervisor.AdmitOperation()
	if err != nil {
		return nil, err
	}
	defer operation.Release()

	openCtx, release := s.openContext(ctx)
	stream, err := s.pool.OpenStream(openCtx, request)
	if err != nil {
		release()
		return nil, err
	}
	// A local control or attachment stream is a natural signal that the local
	// daemon's own catalogue may be about to change (a session request is
	// being issued, or an attachment claim is being taken): mark a local
	// re-probe pending as soon as the stream opens, not only once it ends, so
	// a session created by this very attach appears in `ls --all`/other
	// pickers promptly, even while the client stays attached. An observation
	// stream never triggers this: it is the probe traffic itself, and
	// re-arming from it would starve the schedule instead of catching up to
	// it.
	if request.Local && (request.Purpose == ports.BrokerStreamControl || request.Purpose == ports.BrokerStreamAttachment) {
		s.registry.RequestProbe("")
	}
	// Detach the connection-scoped context link once the pool reports the
	// stream terminal, so a long-lived connection never accumulates one link
	// per opened stream. Nothing here ends the stream: setup completion and the
	// operation lease's release both happen while it keeps running.
	go func() {
		<-stream.Done()
		release()
		// The stream ending is the same signal again (a session was created,
		// killed, or its attachment state flipped over the stream's lifetime):
		// request one more local re-probe so a change made just before detach
		// is not left stale for up to the retry/fresh interval.
		if request.Local && (request.Purpose == ports.BrokerStreamControl || request.Purpose == ports.BrokerStreamAttachment) {
			s.registry.RequestProbe("")
		}
	}()
	return stream, nil
}

// CloseStream retires one stream of this connection. A foreign connection
// identity is refused as stale and never retires another connection's stream.
func (s *Service) CloseStream(connection ports.BrokerConnectionID, stream ports.BrokerStreamID) error {
	if !connection.IsZero() && connection != s.id {
		return ports.BrokerAdmissionStale
	}
	return s.pool.CloseStream(s.id, stream)
}

// AddHost delegates one admitted membership mutation to the registry.
func (s *Service) AddHost(ctx context.Context, endpoint string, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return serviceMutation(s, ctx, func(ctx context.Context) (domain.RemoteRegistration, error) {
		return s.registry.AddHost(ctx, endpoint, policy)
	})
}

// RemoveHost delegates one admitted exact-registration removal to the registry.
func (s *Service) RemoveHost(ctx context.Context, expected domain.RemoteRegistration) (bool, error) {
	return serviceMutation(s, ctx, func(ctx context.Context) (bool, error) {
		return s.registry.RemoveHost(ctx, expected)
	})
}

// UpdateHostPolicy delegates one admitted exact-registration policy update.
func (s *Service) UpdateHostPolicy(ctx context.Context, expected domain.RemoteRegistration, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return serviceMutation(s, ctx, func(ctx context.Context) (domain.RemoteRegistration, error) {
		return s.registry.UpdateHostPolicy(ctx, expected, policy)
	})
}

func serviceMutation[T any](s *Service, ctx context.Context, mutate func(context.Context) (T, error)) (T, error) {
	var zero T
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return zero, ports.BrokerAdmissionClosed
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	operation, err := s.supervisor.AdmitOperation()
	if err != nil {
		return zero, err
	}
	defer operation.Release()

	operationCtx, release := s.openContext(ctx)
	defer release()
	value, err := mutate(operationCtx)
	return value, membershipError(err)
}

func membershipError(err error) error {
	if err == nil {
		return nil
	}
	code := ports.BrokerErrorCode(0)
	text := ""
	var brokerError ports.BrokerError
	switch {
	case errors.Is(err, ports.ErrBrokerHostConflict):
		code, text = ports.BrokerErrorHostConflict, "host registration conflict"
	case errors.Is(err, ports.ErrBrokerMembershipImmutable):
		code, text = ports.BrokerErrorMembershipImmutable, "broker membership is immutable"
	case errors.As(err, &brokerError) && brokerError.Code == ports.BrokerErrorConflictingPolicy:
		code, text = ports.BrokerErrorConflictingPolicy, "host policy conflicts with existing policy"
	default:
		var unknown ports.BrokerStoreOutcomeUnknownError
		var unknownPointer *ports.BrokerStoreOutcomeUnknownError
		if errors.As(err, &unknown) || errors.As(err, &unknownPointer) {
			code, text = ports.BrokerErrorOutcomeUnknown, "mutation outcome is unknown"
		}
	}
	if code == 0 {
		return err
	}
	return ports.BrokerError{Code: code, Text: text, Cause: err}
}

// RequestReconcile schedules one coalesced observation for a configured
// endpoint. The registry validates membership and drops unknown endpoints, so
// this hint cannot expand authority or start an attachment.
func (s *Service) RequestReconcile(endpoint string) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return
	}
	s.registry.RequestProbe(endpoint)
}

// Close cancels and drains every owned resource exactly once: in-flight setup
// opens and live streams stop, subscriptions close, the pool client is retired,
// and the supervisor client lease is released. It is idempotent and
// concurrent-safe; every caller observes the same terminal state.
func (s *Service) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		subs := make([]*serviceSubscription, 0, len(s.subs))
		for sub := range s.subs {
			subs = append(subs, sub)
		}
		preview := s.preview
		s.mu.Unlock()

		s.cancel()
		if preview != nil {
			preview.Close()
		}
		s.wg.Wait()
		for _, sub := range subs {
			sub.Close()
		}
		s.pool.CloseClient(s.previewID)
		s.pool.CloseClient(s.id)
		s.lease.Release()
		// The connection is locally and orderly closed: detach the broker-root
		// watcher so it can neither fire later nor overwrite the clean outcome,
		// then publish the terminal state.
		if s.stopRoot != nil {
			s.stopRoot()
		}
		s.terminalize(nil)
	})
	return nil
}

// scope fences one request to this connection's exact epoch and identity,
// filling an absent identity with this connection's own. A foreign epoch or
// connection is refused before it can steer another connection's work.
func (s *Service) scope(request *ports.BrokerOpenStreamRequest) error {
	if request.Epoch != 0 && request.Epoch != s.epoch {
		return ports.BrokerError{Code: ports.BrokerErrorStaleEpoch}
	}
	if !request.Connection.IsZero() && request.Connection != s.id {
		return ports.BrokerAdmissionStale
	}
	request.Epoch = s.epoch
	request.Connection = s.id
	return nil
}

// openContext derives the context that owns one stream: the caller's context
// bounds the request, and the connection context bounds the service, so the
// stream and its setup end when either does. The returned release detaches the
// connection link once the stream has settled.
func (s *Service) openContext(ctx context.Context) (context.Context, func()) {
	openCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	return openCtx, func() { stop(); cancel() }
}

// serviceSubscription is one connection-owned subscription. Closing it releases
// the registry subscription and forgets it on the owning service.
type serviceSubscription struct {
	service *Service
	inner   ports.BrokerSubscription
	demand  bool
	once    sync.Once
}

func (s *serviceSubscription) Changed() <-chan struct{} { return s.inner.Changed() }

func (s *serviceSubscription) Close() {
	s.once.Do(func() {
		s.inner.Close()
		s.service.mu.Lock()
		delete(s.service.subs, s)
		if s.demand {
			s.service.registry.SetDemand(false)
		}
		s.service.mu.Unlock()
	})
}
