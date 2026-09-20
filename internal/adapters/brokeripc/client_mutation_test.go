package brokeripc

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Send-uncertainty taxonomy for mutating operations (GO-003). These tests drive
// one client adapter over a channel-controlled carriage, so the exact instant
// after a wire attempt is launched is observable without sleeping.
//
// The contract under test: only a request that definitely never left the process
// fails as an ordinary, definite error; once the attempt was launched, a caller
// cancellation, a carriage failure, or a connection teardown reports
// outcome-unknown, so no caller replays a non-idempotent mutation blindly.

// errTestCarriage is the deterministic carriage failure the gated carriage
// reports for one released attempt.
var errTestCarriage = errors.New("brokeripc test: carriage write failed")

// gatedTransport is a channel-controlled client carriage. Every Send records its
// envelope, announces the entered attempt, and then blocks until the test
// releases it with an explicit result or abandons it. Close deliberately leaves
// an in-flight Send blocked, so an attempt held inside the carriage observes
// only the client's own terminal outcome until the test itself decides.
type gatedTransport struct {
	entered chan wire.Envelope
	release chan error
	abandon chan struct{}
	closed  chan struct{}

	closeOnce   sync.Once
	abandonOnce sync.Once

	mu    sync.Mutex
	sends int
}

func newGatedTransport() *gatedTransport {
	return &gatedTransport{
		entered: make(chan wire.Envelope, 8),
		release: make(chan error, 8),
		abandon: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

// Send enters one wire attempt and holds it until the test releases it.
func (t *gatedTransport) Send(envelope wire.Envelope) error {
	t.mu.Lock()
	t.sends++
	t.mu.Unlock()
	t.entered <- envelope
	select {
	case err := <-t.release:
		return err
	case <-t.abandon:
		return io.EOF
	}
}

func (t *gatedTransport) RecvBounded(uint64) (wire.Envelope, error) {
	<-t.closed
	return wire.Envelope{}, io.EOF
}

func (t *gatedTransport) Recv() (wire.Envelope, error) { return t.RecvBounded(0) }

// Close ends the carriage without disturbing an in-flight attempt.
func (t *gatedTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

// releaseSend hands one blocked attempt its deterministic result.
func (t *gatedTransport) releaseSend(err error) { t.release <- err }

// abandonAttempts releases every attempt still blocked in Send, so a test never
// leaks a send goroutine.
func (t *gatedTransport) abandonAttempts() {
	t.abandonOnce.Do(func() { close(t.abandon) })
}

func (t *gatedTransport) sendCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sends
}

// newGatedClient builds one ready client adapter over a gated carriage. It never
// starts the reader goroutine: the reply path is driven by the test, so every
// observation is deterministic.
func newGatedClient(t *testing.T, cfg Config) (*client, *gatedTransport) {
	t.Helper()
	transport := newGatedTransport()
	t.Cleanup(transport.abandonAttempts)
	scope := brokerwire.Scope{
		Epoch:      ports.BrokerEpoch(0x71),
		Connection: ports.BrokerConnectionID{0x71, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01},
	}
	conn, err := brokerwire.NewConnection(scope)
	require.NoError(t, err)
	require.NoError(t, conn.SignalPreamble())
	require.NoError(t, conn.Register())
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		transport: transport,
		ceilings:  brokerwire.DefaultCeilings(),
		scope:     scope,
		conn:      conn,
		cfg:       cfg.withDefaults(),
		ctx:       ctx,
		cancel:    cancel,
		pending:   make(map[ports.BrokerOperationID]chan operationResult),
		kinds:     make(map[ports.BrokerOperationID]brokerwire.RegisterMutationKind),
		streams:   make(map[ports.BrokerStreamID]*clientStream),
		done:      make(chan struct{}),
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, transport
}

// mutationOutcome is one membership mutation result: the removal bool, the
// authoritative registration (additions and policy updates), and the typed
// error a caller observes.
type mutationOutcome struct {
	removed      bool
	registration domain.RemoteRegistration
	err          error
}

// mutationCall adapts one public ports.BrokerService membership method to the
// shared table shape. Every entry drives the real public API, so the wire
// request the carriage captures is exactly the one production sends.
type mutationCall struct {
	name        string
	endpoint    string
	wantRemoved bool
	kind        brokerwire.RegisterMutationKind
	call        func(ctx context.Context, c *client) (bool, domain.RemoteRegistration, error)
	// success builds the payload of one legal wire result for this request
	// kind; the test fills in the connection scope and operation identity.
	success func(operation ports.BrokerOperationID) brokerwire.OperationResult
}

var mutationCalls = []mutationCall{
	{
		name:     "AddHost",
		endpoint: "new@host:22",
		kind:     brokerwire.MutationKindAddHost,
		call: func(ctx context.Context, c *client) (bool, domain.RemoteRegistration, error) {
			registration, err := c.AddHost(ctx, "new@host:22", testPolicy())
			return false, registration, err
		},
		success: func(operation ports.BrokerOperationID) brokerwire.OperationResult {
			return brokerwire.OperationResult{
				Operation: operation, Outcome: ports.BrokerOutcomeOK,
				Registration: mutationRegistration("new@host:22"),
			}
		},
	},
	{
		name:        "RemoveHost",
		endpoint:    "known",
		wantRemoved: true,
		kind:        brokerwire.MutationKindRemoveHost,
		call: func(ctx context.Context, c *client) (bool, domain.RemoteRegistration, error) {
			removed, err := c.RemoveHost(ctx, mutationRegistration("known"))
			return removed, domain.RemoteRegistration{}, err
		},
		success: func(operation ports.BrokerOperationID) brokerwire.OperationResult {
			return brokerwire.OperationResult{Operation: operation, Outcome: ports.BrokerOutcomeOK, Removed: true}
		},
	},
	{
		name:     "UpdateHostPolicy",
		endpoint: "known",
		kind:     brokerwire.MutationKindUpdateHostPolicy,
		call: func(ctx context.Context, c *client) (bool, domain.RemoteRegistration, error) {
			registration, err := c.UpdateHostPolicy(ctx, mutationRegistration("known"), testPolicy())
			return false, registration, err
		},
		success: func(operation ports.BrokerOperationID) brokerwire.OperationResult {
			expected := mutationRegistration("known")
			expected.Generation++
			return brokerwire.OperationResult{Operation: operation, Outcome: ports.BrokerOutcomeOK, Registration: expected}
		},
	},
}

// mutationRegistration builds one valid exact registration for a mutation
// fixture.
func mutationRegistration(endpoint string) domain.RemoteRegistration {
	registration, err := domain.NewRemoteRegistration(endpoint, [16]byte{0x11})
	if err != nil {
		panic(err)
	}
	return registration
}

// run drives one table entry through the public API.
func (call mutationCall) run(ctx context.Context, c *client) (bool, domain.RemoteRegistration, error) {
	return call.call(ctx, c)
}

// deliverSuccess plays one legal success result for a captured request.
func (call mutationCall) deliverSuccess(t *testing.T, c *client, operation ports.BrokerOperationID) {
	t.Helper()
	result := call.success(operation)
	result.Epoch = c.scope.Epoch
	result.Connection = c.scope.Connection
	require.NoError(t, c.dispatch(result))
}

// deliverResult plays one explicit broker reply for a captured request.
func deliverResult(t *testing.T, c *client, result brokerwire.OperationResult) {
	t.Helper()
	require.NoError(t, c.dispatch(result))
}

// awaitResult waits for one mutation call to return.
func awaitResult(t *testing.T, results <-chan mutationOutcome) mutationOutcome {
	t.Helper()
	select {
	case got := <-results:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("the mutating operation did not return")
		return mutationOutcome{}
	}
}

// awaitSend waits for one wire attempt to enter the carriage and decodes it.
func awaitSend(t *testing.T, g *gatedTransport) brokerwire.ClientMessage {
	t.Helper()
	select {
	case envelope := <-g.entered:
		message, err := brokerwire.DecodeClient(envelope.Payload, brokerwire.MaxBrokerEnvelopeBytes, brokerwire.MaxStreamChunkBytes)
		require.NoError(t, err, "the captured attempt must be a decodable client envelope")
		return message
	case <-time.After(5 * time.Second):
		t.Fatal("no wire attempt was entered")
		return nil
	}
}

// operationOf extracts the operation identity of one captured request.
func operationOf(t *testing.T, message brokerwire.ClientMessage) ports.BrokerOperationID {
	t.Helper()
	switch m := message.(type) {
	case brokerwire.AddHost:
		return m.Operation
	case brokerwire.RemoveHost:
		return m.Operation
	case brokerwire.UpdateHostPolicy:
		return m.Operation
	default:
		t.Fatalf("unexpected client message %T", message)
		return ports.BrokerOperationID{}
	}
}

// endpointOf extracts the target of one captured request. An AddHost names
// its endpoint directly; a RemoveHost or UpdateHostPolicy carries exact
// registration authority, whose endpoint is the target.
func endpointOf(t *testing.T, message brokerwire.ClientMessage) string {
	t.Helper()
	switch m := message.(type) {
	case brokerwire.AddHost:
		return m.Endpoint
	case brokerwire.RemoveHost:
		return m.Registration.Endpoint
	case brokerwire.UpdateHostPolicy:
		return m.Registration.Endpoint
	default:
		t.Fatalf("unexpected client message %T", message)
		return ""
	}
}

// registrationOf extracts the exact expected registration of one captured
// request. An AddHost carries no expected authority, only its policy.
func registrationOf(t *testing.T, message brokerwire.ClientMessage) (domain.RemoteRegistration, bool) {
	t.Helper()
	switch m := message.(type) {
	case brokerwire.RemoveHost:
		return m.Registration, true
	case brokerwire.UpdateHostPolicy:
		return m.Registration, true
	default:
		return domain.RemoteRegistration{}, false
	}
}

// policyOf extracts the policy of one captured request.
func policyOf(t *testing.T, message brokerwire.ClientMessage) ports.BrokerPolicy {
	t.Helper()
	switch m := message.(type) {
	case brokerwire.AddHost:
		return m.Policy
	case brokerwire.UpdateHostPolicy:
		return m.Policy
	default:
		t.Fatalf("client message %T carries no policy", message)
		return ports.BrokerPolicy{}
	}
}

// pendingOperations snapshots the client's in-flight waiters.
func pendingOperations(c *client) []ports.BrokerOperationID {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ports.BrokerOperationID, 0, len(c.pending))
	for operation := range c.pending {
		out = append(out, operation)
	}
	return out
}

func requireNoPendingMutationState(t *testing.T, c *client) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Empty(t, c.pending, "pending replies must be released")
	require.Empty(t, c.kinds, "pending mutation kinds must be released")
}

// TestPublicMembershipAPIDelegatesExactAuthority proves the public membership
// surface reaches the carriage with the caller's exact authority and returns the
// broker's authority verbatim: an addition returns the minted registration, a
// policy update returns the generation-advanced registration, and a removal
// returns the exact bool. Each call sends exactly one request carrying the
// caller's policy or exact registration, and never re-derives one locally.
func TestPublicMembershipAPIDelegatesExactAuthority(t *testing.T) {
	policy := testPolicy()
	cases := []struct {
		name  string
		index int
	}{
		{name: "AddHost", index: 0},
		{name: "RemoveHost", index: 1},
		{name: "UpdateHostPolicy", index: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			call := mutationCalls[tc.index]
			c, g := newGatedClient(t, Config{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			results := make(chan mutationOutcome, 1)
			go func() {
				removed, registration, err := call.run(ctx, c)
				results <- mutationOutcome{removed: removed, registration: registration, err: err}
			}()

			request := awaitSend(t, g)
			operation := operationOf(t, request)
			require.Equal(t, call.endpoint, endpointOf(t, request), "the captured request is the caller's own")
			if expected, ok := registrationOf(t, request); ok {
				require.Equal(t, mutationRegistration(call.endpoint), expected,
					"the exact expected registration must travel unmodified")
				require.NotZero(t, expected.Generation, "a removal or update never drops its fencing generation")
			}
			if call.kind != brokerwire.MutationKindRemoveHost {
				require.Equal(t, policy, policyOf(t, request), "the caller's exact policy must travel")
			}
			kind, ok := brokerwire.MutationKindOf(request)
			require.True(t, ok, "the captured request must be a membership mutation")
			require.Equal(t, call.kind, kind, "the client must emit the request kind the table names")
			g.releaseSend(nil)
			call.deliverSuccess(t, c, operation)

			got := awaitResult(t, results)
			require.NoError(t, got.err)
			require.Equal(t, call.wantRemoved, got.removed)
			want := call.success(operation).Registration
			require.Equal(t, want, got.registration, "the client returns the broker's authority verbatim")
			require.Equal(t, 1, g.sendCount(), "one delegation is exactly one wire request")
			require.Empty(t, pendingOperations(c))
		})
	}
}

// TestMutationSendUncertaintyTaxonomy pins the decision table for every mutating
// operation: a request that definitely never left is a definite error with zero
// sends; any failure observed after the attempt was launched is outcome-unknown
// with exactly one send and no replay.
func TestMutationSendUncertaintyTaxonomy(t *testing.T) {
	scenarios := []struct {
		name string
		// before, when set, runs before the call starts.
		before func(cancel context.CancelFunc)
		// drive runs while the call is in flight.
		drive       func(t *testing.T, c *client, g *gatedTransport, cancel context.CancelFunc)
		wantSends   int
		wantUnknown bool
	}{
		{
			name:      "pre-cancelled caller never dispatches and fails definitely",
			before:    func(cancel context.CancelFunc) { cancel() },
			wantSends: 0,
		},
		{
			name: "caller cancels while the attempt is blocked in Send",
			drive: func(t *testing.T, c *client, g *gatedTransport, cancel context.CancelFunc) {
				_ = awaitSend(t, g)
				cancel()
			},
			wantSends:   1,
			wantUnknown: true,
		},
		{
			name: "connection terminates while the attempt is blocked in Send",
			drive: func(t *testing.T, c *client, g *gatedTransport, cancel context.CancelFunc) {
				_ = awaitSend(t, g)
				require.NoError(t, c.Close())
			},
			wantSends:   1,
			wantUnknown: true,
		},
		{
			name: "reply lost after the request was sent",
			drive: func(t *testing.T, c *client, g *gatedTransport, cancel context.CancelFunc) {
				_ = awaitSend(t, g)
				g.releaseSend(nil)
				// The attempt completed, so the call now waits for its reply; only
				// the connection's terminal outcome ends that wait.
				require.NoError(t, c.Close())
			},
			wantSends:   1,
			wantUnknown: true,
		},
		{
			name: "Send fails after the request was captured",
			drive: func(t *testing.T, c *client, g *gatedTransport, cancel context.CancelFunc) {
				_ = awaitSend(t, g)
				g.releaseSend(errTestCarriage)
			},
			wantSends:   1,
			wantUnknown: true,
		},
		{
			name: "carriage EOF after the request was captured",
			drive: func(t *testing.T, c *client, g *gatedTransport, cancel context.CancelFunc) {
				_ = awaitSend(t, g)
				g.releaseSend(io.EOF)
			},
			wantSends:   1,
			wantUnknown: true,
		},
	}
	for _, call := range mutationCalls {
		for _, tt := range scenarios {
			t.Run(call.name+"/"+tt.name, func(t *testing.T) {
				c, g := newGatedClient(t, Config{MaxPendingOperations: 1})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tt.before != nil {
					tt.before(cancel)
				}
				results := make(chan mutationOutcome, 1)
				go func() {
					removed, registration, err := call.run(ctx, c)
					results <- mutationOutcome{removed: removed, registration: registration, err: err}
				}()
				if tt.drive != nil {
					tt.drive(t, c, g, cancel)
				}
				got := awaitResult(t, results)
				require.False(t, got.removed, "no reply means the target is never reported removed")
				require.Equal(t, domain.RemoteRegistration{}, got.registration,
					"no reply means no registration authority is invented")
				if tt.wantUnknown {
					var failure ports.BrokerError
					require.ErrorAs(t, got.err, &failure)
					require.Equal(t, ports.BrokerErrorOutcomeUnknown, failure.Code,
						"a launched attempt must never look like a definite failure")
				} else {
					require.ErrorIs(t, got.err, context.Canceled)
					var failure ports.BrokerError
					require.False(t, errors.As(got.err, &failure),
						"a request that never left must not be reported as a broker outcome")
				}
				require.Equal(t, tt.wantSends, g.sendCount(), "the request must travel at most once")
				requireNoPendingMutationState(t, c)
			})
		}
	}
}

// TestMutationDefiniteFailureRecordsAndReleases proves a mutation whose request
// definitely never left is recorded as failed rather than outcome-unknown, so
// its pending slot is released for the next operation and nothing is resent.
func TestMutationDefiniteFailureRecordsAndReleases(t *testing.T) {
	c, g := newGatedClient(t, Config{MaxPendingOperations: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Driving the public API with an already-cancelled caller is the definite
	// no-dispatch case: no request is encoded for the wire.
	added, err := c.AddHost(ctx, "pre@host:22", testPolicy())
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, domain.RemoteRegistration{}, added)
	require.Zero(t, g.sendCount(), "a pre-cancelled caller never enters the carriage")
	require.Empty(t, pendingOperations(c), "the definite failure releases its pending slot")

	// The bound of one proves the slot is genuinely free: a fresh mutation is
	// admitted and completes normally.
	fresh, cancelFresh := context.WithCancel(context.Background())
	defer cancelFresh()
	results := make(chan mutationOutcome, 1)
	go func() {
		removed, registration, callErr := mutationCalls[1].run(fresh, c)
		results <- mutationOutcome{removed: removed, registration: registration, err: callErr}
	}()
	request := awaitSend(t, g)
	operation := operationOf(t, request)
	g.releaseSend(nil)
	mutationCalls[1].deliverSuccess(t, c, operation)
	got := awaitResult(t, results)
	require.NoError(t, got.err)
	require.True(t, got.removed)
	require.Equal(t, 1, g.sendCount())
}

// TestMutationUnknownOutcomeReleasesWithoutResend proves one outcome-unknown
// mutation releases its pending slot exactly once, never resends the request,
// and ignores a late result for the same identity.
func TestMutationUnknownOutcomeReleasesWithoutResend(t *testing.T) {
	for _, call := range mutationCalls {
		t.Run(call.name, func(t *testing.T) {
			c, g := newGatedClient(t, Config{MaxPendingOperations: 1})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			results := make(chan mutationOutcome, 1)
			go func() {
				removed, registration, err := call.run(ctx, c)
				results <- mutationOutcome{removed: removed, registration: registration, err: err}
			}()
			request := awaitSend(t, g)
			operation := operationOf(t, request)
			require.Equal(t, call.endpoint, endpointOf(t, request), "the captured request is the caller's own")
			g.releaseSend(nil)
			cancel()
			got := awaitResult(t, results)
			var failure ports.BrokerError
			require.ErrorAs(t, got.err, &failure)
			require.Equal(t, ports.BrokerErrorOutcomeUnknown, failure.Code)
			require.False(t, got.removed)
			require.Equal(t, 1, g.sendCount(), "an uncertain attempt is never resent")

			// The identity was completed once, so its pending slot is released
			// rather than left in flight.
			require.ErrorIs(t, c.conn.AdmitOperation(operation), brokerwire.ErrOperationCompleted)
			require.Empty(t, pendingOperations(c))

			// A late legal result for the completed identity is dropped, and it
			// never provokes another wire attempt.
			late := call.success(operation)
			late.Epoch = c.scope.Epoch
			late.Connection = c.scope.Connection
			deliverResult(t, c, late)
			require.Empty(t, pendingOperations(c))
			require.Equal(t, 1, g.sendCount())

			// The connection stays usable: the next mutation is admitted under
			// the same bound of one and completes normally.
			fresh, cancelFresh := context.WithCancel(context.Background())
			defer cancelFresh()
			next := make(chan mutationOutcome, 1)
			go func() {
				removed, registration, err := call.run(fresh, c)
				next <- mutationOutcome{removed: removed, registration: registration, err: err}
			}()
			second := awaitSend(t, g)
			g.releaseSend(nil)
			call.deliverSuccess(t, c, operationOf(t, second))
			done := awaitResult(t, next)
			require.NoError(t, done.err)
			require.Equal(t, call.wantRemoved, done.removed)
			require.Equal(t, 2, g.sendCount())
		})
	}
}

// TestMutationDeliveredReplyIsDefinite proves an ordinary delivered reply stays
// the definite outcome for every operation, and a repeated result for the same
// identity changes nothing.
func TestMutationDeliveredReplyIsDefinite(t *testing.T) {
	for _, call := range mutationCalls {
		t.Run(call.name, func(t *testing.T) {
			c, g := newGatedClient(t, Config{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			results := make(chan mutationOutcome, 1)
			go func() {
				removed, registration, err := call.run(ctx, c)
				results <- mutationOutcome{removed: removed, registration: registration, err: err}
			}()
			request := awaitSend(t, g)
			operation := operationOf(t, request)
			require.Equal(t, call.endpoint, endpointOf(t, request))
			g.releaseSend(nil)
			call.deliverSuccess(t, c, operation)
			got := awaitResult(t, results)
			require.NoError(t, got.err)
			require.Equal(t, call.wantRemoved, got.removed)
			require.Equal(t, 1, g.sendCount())
			require.Empty(t, pendingOperations(c))

			// A replayed result for an already settled identity is dropped.
			replay := call.success(operation)
			replay.Epoch = c.scope.Epoch
			replay.Connection = c.scope.Connection
			deliverResult(t, c, replay)
			require.Empty(t, pendingOperations(c))
			require.Equal(t, 1, g.sendCount())
		})
	}
}

// TestMutationInvalidResultIsOutcomeUnknownAndAbortsProtocol proves every result
// that cannot carry authority for its request is refused rather than applied: a
// result of the wrong kind, a success missing its registration authority, a
// removal success that also carries a registration, and a result with a
// malformed error detail each complete the caller as outcome-unknown and settle
// the connection with a malformed-frame protocol violation.
func TestMutationInvalidResultIsOutcomeUnknownAndAbortsProtocol(t *testing.T) {
	cases := []struct {
		name   string
		call   mutationCall
		result func(c *client, operation ports.BrokerOperationID) brokerwire.OperationResult
	}{
		{
			name: "wrong kind for an addition",
			call: mutationCalls[0],
			result: func(c *client, operation ports.BrokerOperationID) brokerwire.OperationResult {
				return brokerwire.OperationResult{
					Epoch: c.scope.Epoch, Connection: c.scope.Connection, Operation: operation,
					Outcome: ports.BrokerOutcomeOK, Removed: true,
				}
			},
		},
		{
			name: "addition success missing its registration",
			call: mutationCalls[0],
			result: func(c *client, operation ports.BrokerOperationID) brokerwire.OperationResult {
				return brokerwire.OperationResult{
					Epoch: c.scope.Epoch, Connection: c.scope.Connection, Operation: operation,
					Outcome: ports.BrokerOutcomeOK,
				}
			},
		},
		{
			name: "policy update success missing its registration",
			call: mutationCalls[2],
			result: func(c *client, operation ports.BrokerOperationID) brokerwire.OperationResult {
				return brokerwire.OperationResult{
					Epoch: c.scope.Epoch, Connection: c.scope.Connection, Operation: operation,
					Outcome: ports.BrokerOutcomeOK,
				}
			},
		},
		{
			name: "removal success carrying a registration",
			call: mutationCalls[1],
			result: func(c *client, operation ports.BrokerOperationID) brokerwire.OperationResult {
				return brokerwire.OperationResult{
					Epoch: c.scope.Epoch, Connection: c.scope.Connection, Operation: operation,
					Outcome: ports.BrokerOutcomeOK, Removed: true, Registration: mutationRegistration("known"),
				}
			},
		},
		{
			name: "malformed error detail",
			call: mutationCalls[0],
			result: func(c *client, operation ports.BrokerOperationID) brokerwire.OperationResult {
				return brokerwire.OperationResult{
					Epoch: c.scope.Epoch, Connection: c.scope.Connection, Operation: operation,
					Outcome: ports.BrokerOutcomeFailed,
					Error:   brokerwire.ErrorDetail{Code: ports.BrokerErrorCode(99), Text: "bogus"}, HasError: true,
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, g := newGatedClient(t, Config{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			results := make(chan mutationOutcome, 1)
			go func() {
				removed, registration, err := tc.call.run(ctx, c)
				results <- mutationOutcome{removed: removed, registration: registration, err: err}
			}()
			request := awaitSend(t, g)
			operation := operationOf(t, request)
			g.releaseSend(nil)

			dispatchErr := c.dispatch(tc.result(c, operation))
			require.Error(t, dispatchErr, "an invalid result must settle the connection")
			require.ErrorIs(t, dispatchErr, ErrMalformedFrame, "an invalid result is a protocol violation")

			got := awaitResult(t, results)
			var failure ports.BrokerError
			require.ErrorAs(t, got.err, &failure)
			require.Equal(t, ports.BrokerErrorOutcomeUnknown, failure.Code,
				"a result that cannot be applied must never look like a definite outcome")
			require.False(t, got.removed)
			require.Equal(t, domain.RemoteRegistration{}, got.registration)
			require.Equal(t, 1, g.sendCount(), "an invalid result must never provoke a replay")
			require.Empty(t, pendingOperations(c))
		})
	}
}

// scriptedTransport is a client carriage whose single inbound frame can be
// injected: the reader goroutine consumes it and then blocks until the test
// closes the carriage. It lets a test drive the client's real run loop with one
// malformed frame and observe how a pending mutation is settled.
type scriptedTransport struct {
	ready chan wire.Envelope
	done  chan struct{}
	once  sync.Once

	mu    sync.Mutex
	sends []wire.Envelope
}

func newScriptedTransport() *scriptedTransport {
	return &scriptedTransport{ready: make(chan wire.Envelope, 1), done: make(chan struct{})}
}

func (t *scriptedTransport) deliver(envelope wire.Envelope) { t.ready <- envelope }

func (t *scriptedTransport) Send(envelope wire.Envelope) error {
	t.mu.Lock()
	t.sends = append(t.sends, envelope)
	t.mu.Unlock()
	return nil
}

func (t *scriptedTransport) RecvBounded(uint64) (wire.Envelope, error) {
	select {
	case envelope := <-t.ready:
		return envelope, nil
	case <-t.done:
		return wire.Envelope{}, io.EOF
	}
}

func (t *scriptedTransport) Recv() (wire.Envelope, error) { return t.RecvBounded(0) }

func (t *scriptedTransport) Close() error {
	t.once.Do(func() { close(t.done) })
	return nil
}

func (t *scriptedTransport) sendCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.sends)
}

// TestMalformedResultFrameSettlesPendingMutationAsUnknown proves a malformed
// server frame the reader refuses settles the connection and is reported to a
// pending mutation as outcome-unknown, never as a definite failure: the request
// was already sent, so the caller must refresh rather than replay it.
func TestMalformedResultFrameSettlesPendingMutationAsUnknown(t *testing.T) {
	transport := newScriptedTransport()
	scope := brokerwire.Scope{
		Epoch:      ports.BrokerEpoch(0x72),
		Connection: ports.BrokerConnectionID{0x72, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01},
	}
	conn, err := brokerwire.NewConnection(scope)
	require.NoError(t, err)
	require.NoError(t, conn.SignalPreamble())
	require.NoError(t, conn.Register())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &client{
		transport: transport,
		ceilings:  brokerwire.DefaultCeilings(),
		scope:     scope,
		conn:      conn,
		cfg:       Config{}.withDefaults(),
		ctx:       ctx,
		cancel:    cancel,
		pending:   make(map[ports.BrokerOperationID]chan operationResult),
		kinds:     make(map[ports.BrokerOperationID]brokerwire.RegisterMutationKind),
		streams:   make(map[ports.BrokerStreamID]*clientStream),
		done:      make(chan struct{}),
	}
	t.Cleanup(func() { _ = c.Close() })
	go c.run()

	results := make(chan mutationOutcome, 1)
	go func() {
		removed, err := c.RemoveHost(context.Background(), mutationRegistration("known"))
		results <- mutationOutcome{removed: removed, err: err}
	}()
	require.Eventually(t, func() bool { return transport.sendCount() == 1 }, 5*time.Second, time.Millisecond,
		"the mutation request must be launched before the malformed frame arrives")
	c.mu.Lock()
	pending := len(c.pending)
	c.mu.Unlock()
	require.Equal(t, 1, pending, "the mutation must still be pending when the malformed frame arrives")

	// One frame the strict scanner refuses: a payload that is not a valid server
	// envelope. The reader settles the connection on it.
	transport.deliver(wire.Envelope{Payload: []byte{0xFF, 0xFF, 0xFF}})

	got := awaitResult(t, results)
	var failure ports.BrokerError
	require.ErrorAs(t, got.err, &failure)
	require.Equal(t, ports.BrokerErrorOutcomeUnknown, failure.Code,
		"a malformed frame after dispatch must never look like a definite outcome")
	require.False(t, got.removed)
	require.Equal(t, domain.RemoteRegistration{}, got.registration)
	require.Equal(t, 1, transport.sendCount(), "the request must have travelled exactly once")
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the malformed frame must settle the connection")
	}
	require.ErrorIs(t, c.Err(), ErrMalformedFrame)
}

// TestSendContextPreflightClassification pins the private helper's taxonomy
// directly: an unencodable frame, an already-cancelled caller, and an
// already-terminal connection are definitely not dispatched; only a launched
// attempt is uncertain.
func TestSendContextPreflightClassification(t *testing.T) {
	t.Run("unencodable frame is definite", func(t *testing.T) {
		c, g := newGatedClient(t, Config{})
		attempted, err := c.sendContext(context.Background(), nil)
		require.False(t, attempted)
		require.ErrorIs(t, err, brokerwire.ErrInvalidMessage)
		require.Zero(t, g.sendCount())
	})

	t.Run("already-cancelled caller is definite", func(t *testing.T) {
		c, g := newGatedClient(t, Config{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		attempted, err := c.sendContext(ctx, brokerwire.AddHost{
			Epoch: c.scope.Epoch, Connection: c.scope.Connection,
			Operation: ports.BrokerOperationID{0x01}, Endpoint: "pre@host:22", Policy: testPolicy(),
		})
		require.False(t, attempted)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, g.sendCount())
	})

	t.Run("already-terminal connection is definite", func(t *testing.T) {
		c, g := newGatedClient(t, Config{})
		require.NoError(t, c.Close())
		attempted, err := c.sendContext(context.Background(), brokerwire.AddHost{
			Epoch: c.scope.Epoch, Connection: c.scope.Connection,
			Operation: ports.BrokerOperationID{0x01}, Endpoint: "pre@host:22", Policy: testPolicy(),
		})
		require.False(t, attempted)
		require.ErrorIs(t, err, ErrConnectionClosed)
		require.Zero(t, g.sendCount())
	})

	t.Run("launched attempt is uncertain", func(t *testing.T) {
		c, g := newGatedClient(t, Config{})
		type sendResult struct {
			attempted bool
			err       error
		}
		done := make(chan sendResult, 1)
		go func() {
			attempted, err := c.sendContext(context.Background(), brokerwire.RemoveHost{
				Epoch: c.scope.Epoch, Connection: c.scope.Connection,
				Operation: ports.BrokerOperationID{0x02}, Registration: mutationRegistration("known"),
			})
			done <- sendResult{attempted: attempted, err: err}
		}()
		_ = awaitSend(t, g)
		g.releaseSend(errTestCarriage)
		select {
		case got := <-done:
			require.True(t, got.attempted, "a launched attempt is dispatch-uncertain")
			require.ErrorIs(t, got.err, errTestCarriage)
		case <-time.After(5 * time.Second):
			t.Fatal("sendContext did not return")
		}
		require.Equal(t, 1, g.sendCount())
	})
}
