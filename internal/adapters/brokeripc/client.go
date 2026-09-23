package brokeripc

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/streamframe"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Broker IPC client.
//
// Dial connects to one per-user broker endpoint through the private IPC
// carriage (same-user kernel peer credentials on dial), completes the broker
// preamble, sends exactly one Register, adopts the scope the listener assigned
// in the Registered answer, and then runs one reader for the connection. The
// returned value implements ports.BrokerService, so a client process reaches
// local and remote daemons through the same façade it would use in-process.
//
// Identity is the connection's, not the caller's: OpenStream fills in an absent
// epoch, connection, and stream identity from this connection and refuses a
// caller-supplied identity that belongs elsewhere as stale. Stream identities
// are strictly increasing and never reused. Mutating operations carry a fresh
// operation identity, are bounded while pending, and report outcome-unknown
// rather than a silent retry when a reply is lost after the request was sent.

// Dial connects to the broker endpoint at path and returns a ports.BrokerService
// bound to exactly one accepted connection.
func Dial(ctx context.Context, path string, cfg Config) (ports.BrokerService, error) {
	return dial(ctx, path, cfg, ipc.SameUserPeerVerifier())
}

// Connector implements ports.BrokerConnector over the private broker IPC
// carriage. It holds only the endpoint and its bounds, so every Connect is an
// independent attempt and the caller owns Close on the returned service. The
// client process reaches the broker use case exclusively through this seam.
type Connector struct {
	path string
	cfg  Config
}

var _ ports.BrokerConnector = (*Connector)(nil)

// NewConnector returns a connector for one broker endpoint. An empty path is
// refused by Connect, exactly as Dial refuses it, so a misconfigured connector
// fails closed instead of dialing an unspecified socket.
func NewConnector(path string, cfg Config) *Connector {
	return &Connector{path: path, cfg: cfg}
}

// Connect dials the configured endpoint and returns the connection-scoped
// service. Cancellation is honored by the underlying Dial, which bounds the
// whole setup and closes the carriage when its context expires.
func (c *Connector) Connect(ctx context.Context) (ports.BrokerService, error) {
	if c == nil {
		return nil, ErrConfig
	}
	return Dial(ctx, c.path, c.cfg)
}

// dial is Dial with an injected peer verifier, so a test can observe a
// deterministic same-user refusal without root.
func dial(ctx context.Context, path string, cfg Config, verify ipc.PeerVerifier) (ports.BrokerService, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, ErrConfig
	}
	// One deadline bounds the whole setup: the Unix dial, the preamble
	// exchange, the Register send, and the wait for Registered. Each setup step
	// closes the carriage when that deadline expires, because a context alone
	// cannot interrupt blocking framing I/O; that is what stops a peer that
	// stalls after a successful preamble from parking Dial forever.
	setup, cancelSetup := context.WithTimeout(ctx, cfg.HandshakeTimeout)
	defer cancelSetup()
	transport, err := ipc.DialMuxContextWithPeerVerifier(setup, path, verify)
	if err != nil {
		return nil, err
	}
	ceilings, err := runClientPreamble(setup, transport, brokerwire.DefaultCeilings())
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	scope, err := register(setup, transport, ceilings)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	// Setup is complete and the carriage is healthy, so the established
	// connection detaches from the setup deadline: it gets its own
	// connection-lived context below, and cancelSetup (deferred) only ends an
	// already-finished setup.
	conn, err := brokerwire.NewConnection(scope)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	if err := conn.SignalPreamble(); err != nil {
		_ = transport.Close()
		return nil, err
	}
	if err := conn.Register(); err != nil {
		_ = transport.Close()
		return nil, err
	}
	ctx, cancelSession := context.WithCancel(context.Background())
	c := &client{
		transport: transport,
		ceilings:  ceilings,
		scope:     scope,
		conn:      conn,
		cfg:       cfg,
		ctx:       ctx,
		cancel:    cancelSession,
		pending:   make(map[ports.BrokerOperationID]chan operationResult),
		kinds:     make(map[ports.BrokerOperationID]brokerwire.RegisterMutationKind),
		streams:   make(map[ports.BrokerStreamID]*clientStream),
		done:      make(chan struct{}),
	}
	go c.run()
	return c, nil
}

// register completes the broker registration exchange: one Register follows the
// preamble, and the first server frame must be the Registered answer carrying
// the scope assigned at accept. Any other first frame is a protocol violation.
//
// The wait is bounded by ctx: a peer that stalls after a successful preamble is
// interrupted by closing the carriage, and the exchange worker is joined before
// returning, so no setup goroutine is abandoned on the carriage. A Registered
// observed after ctx expired is not a success.
func register(ctx context.Context, transport wire.BoundedTransport, ceilings brokerwire.Ceilings) (brokerwire.Scope, error) {
	type result struct {
		scope brokerwire.Scope
		err   error
	}
	done := make(chan result, 1)
	go func() {
		scope, err := exchangeRegister(transport, ceilings)
		done <- result{scope: scope, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = transport.Close()
		<-done
		return brokerwire.Scope{}, ctx.Err()
	case outcome := <-done:
		if outcome.err == nil {
			if err := ctx.Err(); err != nil {
				return brokerwire.Scope{}, err
			}
		}
		return outcome.scope, outcome.err
	}
}

// exchangeRegister runs one Register/Registered round trip on the carriage. It
// is synchronous: the caller bounds it by closing the carriage on ctx expiry.
func exchangeRegister(transport wire.BoundedTransport, ceilings brokerwire.Ceilings) (brokerwire.Scope, error) {
	payload, err := brokerwire.EncodeClient(brokerwire.Register{}, ceilings.MaxReceiveEnvelopeBytes, ceilings.StreamChunkLimit)
	if err != nil {
		return brokerwire.Scope{}, err
	}
	if err := transport.Send(wire.Envelope{Payload: payload}); err != nil {
		return brokerwire.Scope{}, transportFailure(err)
	}
	envelope, err := transport.RecvBounded(ceilings.MaxReceiveEnvelopeBytes)
	if err != nil {
		return brokerwire.Scope{}, transportFailure(err)
	}
	message, err := brokerwire.DecodeServer(envelope.Payload, ceilings.MaxReceiveEnvelopeBytes, ceilings.StreamChunkLimit)
	if err != nil {
		return brokerwire.Scope{}, errors.Join(ErrMalformedFrame, err)
	}
	registered, ok := message.(brokerwire.Registered)
	if !ok {
		return brokerwire.Scope{}, errors.Join(ErrProtocol, errors.New("brokeripc: broker did not answer Register with Registered"))
	}
	scope := brokerwire.Scope{Epoch: registered.Epoch, Connection: registered.Connection}
	if err := scope.Validate(); err != nil {
		return brokerwire.Scope{}, errors.Join(ErrProtocol, err)
	}
	return scope, nil
}

// operationResult is one reply to a mutating operation.
type operationResult struct {
	outcome      ports.BrokerMutationOutcome
	removed      bool
	registration domain.RemoteRegistration
	detail       brokerwire.ErrorDetail
}

// client implements ports.BrokerService over one broker IPC connection.
type client struct {
	transport wire.BoundedTransport
	ceilings  brokerwire.Ceilings
	scope     brokerwire.Scope
	conn      *brokerwire.Connection
	cfg       Config

	ctx    context.Context
	cancel context.CancelFunc

	mu                sync.Mutex
	closed            bool
	err               error
	sub               *clientSubscription
	generation        brokerwire.SubscriptionGeneration
	preview           *clientPreviewSubscription
	previewGeneration ports.BrokerPreviewGeneration
	nextStream        ports.BrokerStreamID
	snapshot          ports.BrokerSnapshot
	pending           map[ports.BrokerOperationID]chan operationResult
	kinds             map[ports.BrokerOperationID]brokerwire.RegisterMutationKind
	// The connection's own stream tracker is the anti-replay authority for
	// this connection: it admits every identity at most once inside the bounded
	// window, whether the open is afterwards accepted or refused.
	streams   map[ports.BrokerStreamID]*clientStream
	closeOnce sync.Once
	done      chan struct{}
}

var _ ports.BrokerService = (*client)(nil)

// run is the connection's single reader.
func (c *client) run() {
	for {
		envelope, err := c.transport.RecvBounded(c.ceilings.MaxReceiveEnvelopeBytes)
		if err != nil {
			c.closeWith(transportFailure(err))
			return
		}
		message, err := brokerwire.DecodeServer(envelope.Payload, c.ceilings.MaxReceiveEnvelopeBytes, c.ceilings.StreamChunkLimit)
		if err != nil {
			c.closeWith(errors.Join(ErrMalformedFrame, err))
			return
		}
		if err := c.dispatch(message); err != nil {
			c.closeWith(err)
			return
		}
	}
}

// dispatch applies one server frame. A returned error is a protocol violation
// that settles the connection.
func (c *client) dispatch(message brokerwire.ServerMessage) error {
	switch m := message.(type) {
	case brokerwire.SnapshotPart:
		if !c.scopeMatches(m.Epoch, m.Connection) {
			return errors.Join(ErrScopeMismatch, ErrProtocol)
		}
		committed, snapshot, err := c.conn.AddSnapshotPart(m)
		if err != nil {
			if errors.Is(err, brokerwire.ErrStaleGeneration) || errors.Is(err, brokerwire.ErrFutureGeneration) || errors.Is(err, brokerwire.ErrSnapshotStale) {
				return nil
			}
			return errors.Join(ErrMalformedFrame, err)
		}
		if committed {
			c.publish(snapshot)
		}
		return nil
	case brokerwire.PreviewPublication:
		if !c.scopeMatches(m.Epoch, m.Connection) {
			return errors.Join(ErrScopeMismatch, ErrProtocol)
		}
		c.mu.Lock()
		sub := c.preview
		active := sub != nil && c.previewGeneration == m.Generation
		c.mu.Unlock()
		if !active {
			return nil
		}
		publication := ports.BrokerPreviewPublication{Epoch: m.Epoch, Connection: m.Connection, Generation: m.Generation, Target: sub.request.Preview.Target, Width: sub.request.Preview.Width, Height: sub.request.Preview.Height, Preview: m.Preview}
		if m.HasError {
			publication.Err = failureFromDetail(m.Error)
		}
		if err := publication.Validate(); err != nil {
			return errors.Join(ErrMalformedFrame, err)
		}
		sub.publish(publication)
		return nil
	case brokerwire.OperationResult:
		if !c.scopeMatches(m.Epoch, m.Connection) {
			return errors.Join(ErrScopeMismatch, ErrProtocol)
		}
		c.mu.Lock()
		_, ok := c.pending[m.Operation]
		kind := c.kinds[m.Operation]
		c.mu.Unlock()
		// An abandoned or replayed completion has no authority over current
		// state and is dropped. A completion for a live operation must satisfy
		// that request kind's payload invariant.
		if !ok {
			return nil
		}
		if err := m.ValidateForMutation(kind); err != nil {
			c.completeOperation(m.Operation, operationResult{outcome: ports.BrokerOutcomeUnknown, detail: errorDetail(err)})
			return errors.Join(ErrMalformedFrame, err)
		}
		c.completeOperation(m.Operation, operationResult{outcome: m.Outcome, removed: m.Removed, registration: m.Registration, detail: m.Error})
		return nil
	case brokerwire.StreamOpened:
		if !c.scopeMatches(m.Epoch, m.Connection) {
			return errors.Join(ErrScopeMismatch, ErrProtocol)
		}
		if err := c.conn.StreamOpened(m.Stream); err != nil && !errors.Is(err, brokerwire.ErrStreamState) {
			return errors.Join(ErrProtocol, err)
		}
		if st := c.lookupStream(m.Stream); st != nil {
			select {
			case st.opened <- nil:
			default:
			}
		}
		return nil
	case brokerwire.ServerStreamData:
		if !c.scopeMatches(m.Epoch, m.Connection) {
			return errors.Join(ErrScopeMismatch, ErrProtocol)
		}
		disposition, err := c.conn.StreamData(m.Stream)
		if err != nil {
			return errors.Join(ErrProtocol, err)
		}
		if disposition == brokerwire.StreamDiscarded {
			return nil
		}
		st := c.lookupStream(m.Stream)
		if st == nil {
			// The stream was retired locally between the tracker check and the
			// lookup; its data is discarded, exactly like a retired stream.
			return nil
		}
		if err := st.deliver(m.Data); err != nil {
			if errors.Is(err, ErrStreamBackpressure) {
				st.fail(ErrStreamBackpressure)
			}
		}
		return nil
	case brokerwire.StreamClosed:
		if !c.scopeMatches(m.Epoch, m.Connection) {
			return errors.Join(ErrScopeMismatch, ErrProtocol)
		}
		_, _ = c.conn.CloseStream(m.Stream)
		if st := c.lookupStream(m.Stream); st != nil {
			var cause error
			if m.HasError {
				cause = failureFromDetail(m.Error)
			}
			st.terminate(cause)
		}
		return nil
	case brokerwire.Progress:
		if !c.scopeMatches(m.Epoch, m.Connection) {
			return errors.Join(ErrScopeMismatch, ErrProtocol)
		}
		_, _ = c.conn.StreamProgress(m.Stream)
		return nil
	case brokerwire.BrokerErrorMessage:
		if !c.scopeMatches(m.Epoch, m.Connection) {
			return errors.Join(ErrScopeMismatch, ErrProtocol)
		}
		cause := failureFromDetail(m.Error)
		if cause == nil {
			cause = ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "empty broker error frame"}
		}
		return cause
	case brokerwire.Shutdown:
		text := m.Text
		if text == "" {
			text = "broker shutdown"
		}
		return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: text}
	default:
		return errors.Join(ErrProtocol, errors.New("brokeripc: unexpected server message"))
	}
}

// scopeMatches reports whether one frame carries this connection's accepted
// scope.
func (c *client) scopeMatches(epoch ports.BrokerEpoch, connection ports.BrokerConnectionID) bool {
	return epoch == c.scope.Epoch && connection == c.scope.Connection
}

// publish installs one committed snapshot and wakes the active subscription.
func (c *client) publish(snapshot ports.BrokerSnapshot) {
	c.mu.Lock()
	c.snapshot = snapshot
	sub := c.sub
	c.mu.Unlock()
	if sub != nil {
		sub.notify()
	}
}

// releaseOperation removes all client-side state for an operation.
func (c *client) releaseOperation(operation ports.BrokerOperationID) {
	c.mu.Lock()
	delete(c.pending, operation)
	delete(c.kinds, operation)
	c.mu.Unlock()
}

// completeOperation hands one result to the operation's waiter, if any.
func (c *client) completeOperation(operation ports.BrokerOperationID, result operationResult) {
	c.mu.Lock()
	reply, ok := c.pending[operation]
	delete(c.pending, operation)
	delete(c.kinds, operation)
	c.mu.Unlock()
	if !ok {
		return
	}
	select {
	case reply <- result:
	default:
	}
}

// ConnectionID returns the identity assigned to this connection at accept.
func (c *client) ConnectionID() ports.BrokerConnectionID { return c.scope.Connection }

// NextStreamID allocates this connection's next logical stream identity. It is
// strictly monotone, never zero, and consumes no wire frame: it is the local
// allocation of the calling service, which then carries the exact
// epoch/connection/stream triplet on OpenStream. It performs no I/O and refuses
// a closed connection or an exhausted counter rather than wrapping.
func (c *client) NextStreamID() (ports.BrokerStreamID, error) {
	if c == nil {
		return 0, ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, c.terminalErrorLocked()
	}
	if c.nextStream == ^ports.BrokerStreamID(0) {
		return 0, ports.BrokerAdmissionLimit
	}
	c.nextStream++
	return c.nextStream, nil
}

// Done closes exactly once when this connection is terminal: the carriage
// failed, the broker retired it, or this side closed it. Err is stable
// afterwards.
func (c *client) Done() <-chan struct{} { return c.done }

// Err returns this connection's terminal cause: nil for an orderly local Close,
// otherwise the failure that settled it. The cause is recorded exactly once and
// never overwritten, so it is stable once Done is closed.
func (c *client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Snapshot returns the newest fully committed publication.
func (c *client) Snapshot() ports.BrokerSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshot.Clone()
}

// Subscribe asks the broker for publications of the current scope. The call is
// a bounded, non-blocking hint: it enqueues one Subscribe frame carrying a fresh
// generation and returns immediately. Each notification means a newer committed
// snapshot is available through Snapshot.
//
// Adopting the generation and enqueueing its frame are one step on this
// connection: the local generation series is consumed as soon as the frame is
// admitted, so a send that then fails has already superseded the previous
// subscription. That previous subscription is therefore retired rather than
// restored, and the connection is left with no current subscription: the caller
// sees the send failure and subscribes again with a fresh generation.
func (c *client) Subscribe() (ports.BrokerSubscription, error) {
	c.mu.Lock()
	if c.closed {
		err := c.terminalErrorLocked()
		c.mu.Unlock()
		return nil, err
	}
	generation := c.generation + 1
	if err := c.conn.Subscribe(generation); err != nil {
		c.mu.Unlock()
		return nil, admissionError(err)
	}
	c.generation = generation
	sub := newClientSubscription()
	previous := c.sub
	c.sub = sub
	c.mu.Unlock()
	if err := c.sendAsync(brokerwire.Subscribe{
		Epoch: c.scope.Epoch, Connection: c.scope.Connection, Generation: generation,
		Passive: c.cfg.PassiveSubscribe,
	}); err != nil {
		// The generation was already consumed by the connection's subscription
		// tracker, so the previous subscription's generation is no longer
		// current even though this frame never left. Restoring it would present
		// a superseded generation as the live subscription; retire both and
		// leave the connection with no current subscription instead.
		c.mu.Lock()
		if c.sub == sub {
			c.sub = nil
		}
		c.mu.Unlock()
		sub.Close()
		previous.Close()
		return nil, err
	}
	if previous != nil {
		previous.Close()
	}
	return sub, nil
}

// OpenStream opens one independently cancellable logical stream. An absent
// epoch or connection identity is filled from this connection and a
// caller-supplied identity that belongs elsewhere is refused as stale. The
// stream identity must be the exact one the calling service allocated with
// NextStreamID: it is admitted on this connection's bounded anti-replay window
// before any frame travels, so a duplicate and an identity evicted past the
// window are both refused, while a concurrent lower identity that was allocated
// first is still admitted.
func (c *client) SubscribePreview(request ports.BrokerPreviewRequest) (ports.BrokerPreviewSubscription, error) {
	if err := request.Validate(); err != nil {
		return nil, errors.Join(ports.BrokerAdmissionInvalid, err)
	}
	if request.Epoch != c.scope.Epoch || request.Connection != c.scope.Connection {
		return nil, ports.BrokerAdmissionStale
	}
	c.mu.Lock()
	if c.closed {
		err := c.terminalErrorLocked()
		c.mu.Unlock()
		return nil, err
	}
	if request.Generation <= c.previewGeneration {
		c.mu.Unlock()
		return nil, ports.BrokerAdmissionStale
	}
	previous := c.preview
	sub := newClientPreviewSubscription(c, request)
	c.preview, c.previewGeneration = sub, request.Generation
	c.mu.Unlock()
	if err := c.sendAsync(brokerwire.StartPreview{Epoch: request.Epoch, Connection: request.Connection, Generation: request.Generation, Route: request.Route, Preview: request.Preview}); err != nil {
		// The generation was already consumed as the connection's current
		// preview generation, so the previous subscription is superseded even
		// though this frame never left. Retire both and leave the connection
		// with no current preview, mirroring Subscribe's send-error path.
		c.mu.Lock()
		if c.preview == sub {
			c.preview = nil
		}
		c.mu.Unlock()
		sub.Close()
		previous.Close()
		return nil, err
	}
	if previous != nil {
		previous.Close()
	}
	return sub, nil
}

func (c *client) OpenStream(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	scoped, err := c.scopeRequest(request)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		err := c.terminalErrorLocked()
		c.mu.Unlock()
		return nil, err
	}
	// Admit the identity on the connection's own tracker before any frame
	// travels, so a racing StreamOpened can never precede local admission. The
	// tracker applies the bounded anti-replay window: a duplicate and an
	// identity evicted past the window are refused, while a concurrent lower
	// identity that was allocated first is still admitted.
	if err := c.conn.OpenStream(scoped.Stream); err != nil {
		c.mu.Unlock()
		return nil, admissionError(err)
	}
	stream, err := newClientStream(c, scoped.Stream)
	if err != nil {
		_, _ = c.conn.CloseStream(scoped.Stream)
		c.mu.Unlock()
		return nil, err
	}
	c.streams[scoped.Stream] = stream
	c.mu.Unlock()

	// OpenStream treats every send failure alike: a stream whose request
	// definitely never left is retired exactly like one that may have been
	// dispatched, because the local identity is already admitted either way.
	if _, err := c.sendContext(ctx, brokerwire.OpenStream{
		Epoch: scoped.Epoch, Connection: scoped.Connection, Stream: scoped.Stream,
		Purpose: scoped.Purpose, Admission: scoped.Admission, Name: scoped.Name,
		Local:    scoped.Local,
		Endpoint: scoped.Endpoint, Registration: scoped.Registration,
		Target: scoped.Target, Env: scoped.Env, Policy: scoped.Policy,
		StartMode: scoped.StartMode,
	}); err != nil {
		c.retireStream(scoped.Stream, err)
		return nil, err
	}
	select {
	case err := <-stream.opened:
		if err != nil {
			return nil, err
		}
		return stream, nil
	case <-ctx.Done():
		// The request was sent, so the broker may still establish the stream.
		// Retire it locally and tell the broker; the caller sees cancellation.
		c.retireStream(scoped.Stream, ctx.Err())
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.terminalError()
	}
}

// CloseStream retires one scope-checked logical stream.
func (c *client) CloseStream(connection ports.BrokerConnectionID, stream ports.BrokerStreamID) error {
	if !connection.IsZero() && connection != c.scope.Connection {
		return errors.Join(ErrScopeMismatch, ports.BrokerAdmissionStale)
	}
	c.mu.Lock()
	if c.closed {
		err := c.terminalErrorLocked()
		c.mu.Unlock()
		return err
	}
	_, err := c.conn.CloseStream(stream)
	c.mu.Unlock()
	if err != nil {
		return admissionError(err)
	}
	c.requestStreamClose(stream)
	if st := c.lookupStream(stream); st != nil {
		st.terminate(nil)
	}
	return nil
}

// AddHost sends one exact endpoint and policy mutation.
func (c *client) AddHost(ctx context.Context, endpoint string, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	result, err := c.mutate(ctx, brokerwire.MutationKindAddHost, func(operation ports.BrokerOperationID) brokerwire.ClientMessage {
		return brokerwire.AddHost{Epoch: c.scope.Epoch, Connection: c.scope.Connection, Operation: operation, Endpoint: endpoint, Policy: policy}
	})
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	return result.registration, mutationFailure(result)
}

// RemoveHost sends one exact-registration mutation.
func (c *client) RemoveHost(ctx context.Context, expected domain.RemoteRegistration) (bool, error) {
	result, err := c.mutate(ctx, brokerwire.MutationKindRemoveHost, func(operation ports.BrokerOperationID) brokerwire.ClientMessage {
		return brokerwire.RemoveHost{Epoch: c.scope.Epoch, Connection: c.scope.Connection, Operation: operation, Registration: expected}
	})
	if err != nil {
		return false, err
	}
	return result.removed, mutationFailure(result)
}

// UpdateHostPolicy sends one exact-registration replacement policy mutation.
func (c *client) UpdateHostPolicy(ctx context.Context, expected domain.RemoteRegistration, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	result, err := c.mutate(ctx, brokerwire.MutationKindUpdateHostPolicy, func(operation ports.BrokerOperationID) brokerwire.ClientMessage {
		return brokerwire.UpdateHostPolicy{Epoch: c.scope.Epoch, Connection: c.scope.Connection, Operation: operation, Registration: expected, Policy: policy}
	})
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	return result.registration, mutationFailure(result)
}

// RequestReconcile asks the broker to re-observe one endpoint. The port carries
// an endpoint while the wire carries an exact registration, so the hint is sent
// only for an endpoint this connection already holds a registration for; an
// unknown endpoint is a no-op. It never blocks and never fails the caller.
func (c *client) RequestReconcile(endpoint string) {
	c.mu.Lock()
	host, ok := c.snapshot.Find(endpoint)
	closed := c.closed
	c.mu.Unlock()
	if closed || !ok {
		return
	}
	_ = c.sendAsync(brokerwire.Reconcile{Epoch: c.scope.Epoch, Connection: c.scope.Connection, Registration: host.Registration})
}

// Close ends the connection, releasing its streams and subscription. It is
// idempotent and concurrent-safe.
func (c *client) Close() error {
	if c == nil {
		return nil
	}
	c.closeWith(nil)
	return nil
}

// scopeRequest fills an absent epoch or connection identity from this
// connection and refuses one that belongs elsewhere, then validates the
// resulting request. The stream identity must be present: after cutover the
// calling service allocates it with NextStreamID and carries the exact
// epoch/connection/stream triplet, so this adapter never invents an identity on
// an incomplete request.
func (c *client) scopeRequest(request ports.BrokerOpenStreamRequest) (ports.BrokerOpenStreamRequest, error) {
	scoped := request
	if scoped.Epoch == 0 {
		scoped.Epoch = c.scope.Epoch
	} else if scoped.Epoch != c.scope.Epoch {
		return ports.BrokerOpenStreamRequest{}, errors.Join(ErrScopeMismatch, ports.BrokerError{Code: ports.BrokerErrorStaleEpoch})
	}
	if scoped.Connection.IsZero() {
		scoped.Connection = c.scope.Connection
	} else if scoped.Connection != c.scope.Connection {
		return ports.BrokerOpenStreamRequest{}, errors.Join(ErrScopeMismatch, ports.BrokerAdmissionStale)
	}
	if scoped.Stream == 0 {
		return ports.BrokerOpenStreamRequest{}, errors.Join(ports.BrokerAdmissionInvalid, errors.New("brokeripc: open stream request has no stream identity"))
	}
	if err := scoped.Validate(); err != nil {
		return ports.BrokerOpenStreamRequest{}, errors.Join(ports.BrokerAdmissionInvalid, err)
	}
	return scoped, nil
}

// mutate sends one mutating operation and waits for its bounded completion. The
// operation identity is admitted on the connection's tracker (bounded by the
// same per-connection bound the wire enforces) and completed exactly once, so an
// abandoned or timed-out operation releases its pending slot and a late or
// replayed result is deduped rather than applied twice.
//
// A request that definitely never left the process fails as an ordinary definite
// error and records a failed outcome; once the wire attempt was launched, every
// failure is reported as outcome-unknown, so a caller refreshes authoritative
// state instead of replaying a non-idempotent mutation blindly.
func (c *client) mutate(ctx context.Context, kind brokerwire.RegisterMutationKind, build func(ports.BrokerOperationID) brokerwire.ClientMessage) (operationResult, error) {
	operation, err := newOperationID()
	if err != nil {
		return operationResult{}, err
	}
	reply := make(chan operationResult, 1)
	c.mu.Lock()
	if c.closed {
		err := c.terminalErrorLocked()
		c.mu.Unlock()
		return operationResult{}, err
	}
	if len(c.pending) >= c.cfg.MaxPendingOperations {
		c.mu.Unlock()
		return operationResult{}, ports.BrokerAdmissionLimit
	}
	if err := c.conn.AdmitOperation(operation); err != nil {
		c.mu.Unlock()
		return operationResult{}, admissionError(err)
	}
	c.pending[operation] = reply
	if c.kinds == nil {
		c.kinds = make(map[ports.BrokerOperationID]brokerwire.RegisterMutationKind)
	}
	c.kinds[operation] = kind
	c.mu.Unlock()
	// No result observed is exactly the outcome-unknown case; a request that
	// was never launched is recorded as failed instead.
	outcome := ports.BrokerOutcomeUnknown
	defer func() {
		c.releaseOperation(operation)
		_ = c.conn.CompleteOperation(operation, outcome)
	}()
	attempted, err := c.sendContext(ctx, build(operation))
	if err != nil {
		if !attempted {
			// The request definitely did not travel, so a definite failure is
			// the honest report and the operation is recorded as failed.
			outcome = ports.BrokerOutcomeFailed
			return operationResult{}, err
		}
		// The wire attempt was launched: the broker may have committed the
		// operation even though this side observed no result.
		return operationResult{outcome: ports.BrokerOutcomeUnknown, detail: errorDetail(err)}, nil
	}
	select {
	case result := <-reply:
		outcome = result.outcome
		return result, nil
	case <-ctx.Done():
		// The operation was already sent: the broker may have committed it.
		return operationResult{outcome: ports.BrokerOutcomeUnknown, detail: errorDetail(ctx.Err())}, nil
	case <-c.done:
		return operationResult{outcome: ports.BrokerOutcomeUnknown, detail: errorDetail(c.terminalError())}, nil
	}
}

// send encodes and writes one client frame synchronously.
func (c *client) send(message brokerwire.ClientMessage) error {
	payload, err := brokerwire.EncodeClient(message, c.ceilings.MaxReceiveEnvelopeBytes, c.ceilings.StreamChunkLimit)
	if err != nil {
		return err
	}
	if err := c.transport.Send(wire.Envelope{Payload: payload}); err != nil {
		return transportFailure(err)
	}
	return nil
}

// sendAsync admits one client frame for ordered background transmission. It
// reports backpressure as a retryable unavailability rather than blocking a
// caller that has no context to spend.
func (c *client) sendAsync(message brokerwire.ClientMessage) error {
	payload, err := brokerwire.EncodeClient(message, c.ceilings.MaxReceiveEnvelopeBytes, c.ceilings.StreamChunkLimit)
	if err != nil {
		return err
	}
	if async, ok := c.transport.(wire.AsyncTransport); ok {
		if err := async.SendAsync(wire.Envelope{Payload: payload}); err != nil {
			if errors.Is(err, streamframe.ErrBackpressure) {
				return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker IPC send queue is full"}
			}
			return transportFailure(err)
		}
		return nil
	}
	if err := c.transport.Send(wire.Envelope{Payload: payload}); err != nil {
		return transportFailure(err)
	}
	return nil
}

// sendContext writes one client frame, honoring cancellation while the carriage
// egress queue is full. It reports whether the wire attempt was launched, which
// is exactly the dispatch uncertainty a caller needs: an encoding failure, an
// already-cancelled call, or a connection already terminal before the attempt
// begins means the frame definitely did not travel, while every failure
// observed after the attempt is launched means it may have been dispatched and
// the caller must not replay blindly. The wire attempt is raced against the
// caller's context and the connection's terminal outcome, so a stalled send
// never pins the caller: the racing goroutine observes the closed carriage and
// returns.
func (c *client) sendContext(ctx context.Context, message brokerwire.ClientMessage) (attempted bool, err error) {
	payload, err := brokerwire.EncodeClient(message, c.ceilings.MaxReceiveEnvelopeBytes, c.ceilings.StreamChunkLimit)
	if err != nil {
		return false, err
	}
	// Synchronous preflight. Checking before the goroutine starts is what makes
	// the not-dispatched claim true: a context that is already done or a
	// connection that is already terminal can no longer race a launched Send.
	if err := ctx.Err(); err != nil {
		return false, err
	}
	select {
	case <-c.done:
		return false, c.terminalError()
	default:
	}
	done := make(chan error, 1)
	go func() { done <- c.transport.Send(wire.Envelope{Payload: payload}) }()
	select {
	case err := <-done:
		if err != nil {
			return true, transportFailure(err)
		}
		return true, nil
	case <-ctx.Done():
		return true, ctx.Err()
	case <-c.done:
		return true, c.terminalError()
	}
}

// requestStreamClose tells the broker one stream is retired. It is best-effort:
// the stream is already settled locally, so a full queue or a dead carriage
// needs no further action.
func (c *client) requestStreamClose(stream ports.BrokerStreamID) {
	_ = c.sendAsync(brokerwire.CloseStream{Epoch: c.scope.Epoch, Connection: c.scope.Connection, Stream: stream})
}

// retireStream settles one local stream and tells the broker to retire it.
func (c *client) retireStream(stream ports.BrokerStreamID, cause error) {
	st := c.lookupStream(stream)
	if st != nil {
		st.terminate(cause)
	}
	c.requestStreamClose(stream)
}

// lookupStream returns the local stream for one identity.
func (c *client) lookupStream(stream ports.BrokerStreamID) *clientStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[stream]
}

// unregisterStream drops one local stream record.
func (c *client) unregisterStream(stream ports.BrokerStreamID) {
	c.mu.Lock()
	delete(c.streams, stream)
	c.mu.Unlock()
}

// terminalErrorLocked returns the terminal cause with the connection lock held.
func (c *client) terminalErrorLocked() error {
	if c.err != nil {
		return c.err
	}
	return ErrConnectionClosed
}

// terminalError returns the terminal cause.
func (c *client) terminalError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.terminalErrorLocked()
}

// closeWith tears the connection down exactly once: cancel, close the carriage
// (unblocking a blocked read and any frame send), settle every local stream and
// the active subscription, and wake every waiter. A nil cause is an orderly
// local close.
func (c *client) closeWith(cause error) {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		c.cancel()
		_ = c.transport.Close()
		c.mu.Lock()
		c.closed = true
		if cause != nil && c.err == nil {
			c.err = cause
		}
		terminal := c.terminalErrorLocked()
		sub := c.sub
		c.sub = nil
		preview := c.preview
		c.preview = nil
		streams := make([]*clientStream, 0, len(c.streams))
		for _, st := range c.streams {
			streams = append(streams, st)
		}
		c.streams = make(map[ports.BrokerStreamID]*clientStream)
		c.mu.Unlock()
		for _, st := range streams {
			st.terminate(terminal)
		}
		if sub != nil {
			sub.Close()
		}
		if preview != nil {
			preview.Close()
		}
		close(c.done)
	})
}

// mutationFailure converts one operation outcome into its typed caller error.
func mutationFailure(result operationResult) error {
	switch result.outcome {
	case ports.BrokerOutcomeOK:
		return nil
	case ports.BrokerOutcomeFailed:
		if err := failureFromDetail(result.detail); err != nil {
			return err
		}
		return ports.BrokerError{Code: ports.BrokerErrorUnavailable}
	default:
		return ports.BrokerError{Code: ports.BrokerErrorOutcomeUnknown}
	}
}

// newOperationID samples one fresh non-zero operation identity.
func newOperationID() (ports.BrokerOperationID, error) {
	var id ports.BrokerOperationID
	if _, err := rand.Read(id[:]); err != nil {
		return ports.BrokerOperationID{}, err
	}
	if id.IsZero() {
		id[15] = 1
	}
	return id, nil
}

// clientSubscription is one local capacity-one change notification. Each
// subscriber owns its own channel and re-reads the newest snapshot on wake.
type clientSubscription struct {
	changed chan struct{}
	once    sync.Once
	mu      sync.Mutex
	closed  bool
}

func newClientSubscription() *clientSubscription {
	return &clientSubscription{changed: make(chan struct{}, 1)}
}

// Changed is signalled once per committed publication.
func (s *clientSubscription) Changed() <-chan struct{} { return s.changed }

// Close ends the subscription exactly once. The channel is closed under the same
// lock notify sends under, so a racing publication can never send on a closed
// channel.
func (s *clientSubscription) Close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.changed)
		s.mu.Unlock()
	})
}

// notify coalesces one publication wake.
func (s *clientSubscription) notify() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.changed <- struct{}{}:
	default:
	}
}
