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
		streams:   make(map[ports.BrokerStreamID]*clientStream),
		done:      make(chan struct{}),
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, transport
}

// mutationOutcome is one AddHost or RemoveHost result.
type mutationOutcome struct {
	removed bool
	err     error
}

// mutationCall adapts one mutating operation to the shared table shape.
type mutationCall struct {
	name        string
	endpoint    string
	wantRemoved bool
	call        func(ctx context.Context, c *client, endpoint string) (bool, error)
}

var mutationCalls = []mutationCall{
	{
		name:     "AddHost",
		endpoint: "new@host:22",
		call: func(ctx context.Context, c *client, endpoint string) (bool, error) {
			return false, c.AddHost(ctx, endpoint)
		},
	},
	{
		name:        "RemoveHost",
		endpoint:    "known",
		wantRemoved: true,
		call: func(ctx context.Context, c *client, endpoint string) (bool, error) {
			return c.RemoveHost(ctx, endpoint)
		},
	},
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
	default:
		t.Fatalf("unexpected client message %T", message)
		return ports.BrokerOperationID{}
	}
}

// endpointOf extracts the target of one captured request.
func endpointOf(t *testing.T, message brokerwire.ClientMessage) string {
	t.Helper()
	switch m := message.(type) {
	case brokerwire.AddHost:
		return m.Endpoint
	case brokerwire.RemoveHost:
		return m.Endpoint
	default:
		t.Fatalf("unexpected client message %T", message)
		return ""
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

// deliverResult plays one broker reply for a captured request.
func deliverResult(t *testing.T, c *client, operation ports.BrokerOperationID, outcome ports.BrokerMutationOutcome, removed bool) {
	t.Helper()
	require.NoError(t, c.dispatch(brokerwire.OperationResult{
		Epoch: c.scope.Epoch, Connection: c.scope.Connection,
		Operation: operation, Outcome: outcome, Removed: removed,
	}))
}

// TestMutationSendUncertaintyTaxonomy pins the decision table for both mutating
// operations: a request that definitely never left is a definite error with zero
// sends; any failure observed after the attempt was launched is outcome-unknown
// with exactly one send.
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
			name: "Send fails after the request was captured",
			drive: func(t *testing.T, c *client, g *gatedTransport, cancel context.CancelFunc) {
				_ = awaitSend(t, g)
				g.releaseSend(errTestCarriage)
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
					removed, err := call.call(ctx, c, call.endpoint)
					results <- mutationOutcome{removed: removed, err: err}
				}()
				if tt.drive != nil {
					tt.drive(t, c, g, cancel)
				}
				got := awaitResult(t, results)
				require.False(t, got.removed, "no reply means the target is never reported removed")
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
				require.Empty(t, pendingOperations(c), "every mutation releases its pending slot")
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

	// Driving mutate directly makes the operation identity observable even
	// though the request is never encoded for the wire.
	var operation ports.BrokerOperationID
	result, err := c.mutate(ctx, func(id ports.BrokerOperationID) brokerwire.ClientMessage {
		operation = id
		return brokerwire.AddHost{
			Epoch: c.scope.Epoch, Connection: c.scope.Connection, Operation: id, Endpoint: "pre@host:22",
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, operationResult{}, result)
	require.NotZero(t, operation)
	require.Zero(t, g.sendCount(), "a pre-cancelled caller never enters the carriage")
	require.Empty(t, pendingOperations(c), "the definite failure releases its pending slot")

	// The identity is recorded with an outcome, not left in flight: admitting it
	// again is deduped, which is only true once the tracker was completed.
	require.ErrorIs(t, c.conn.AdmitOperation(operation), brokerwire.ErrOperationCompleted)

	// The bound of one proves the slot is genuinely free: a fresh mutation is
	// admitted and completes normally.
	fresh, cancelFresh := context.WithCancel(context.Background())
	defer cancelFresh()
	results := make(chan mutationOutcome, 1)
	go func() {
		removed, callErr := c.RemoveHost(fresh, "known")
		results <- mutationOutcome{removed: removed, err: callErr}
	}()
	request := awaitSend(t, g)
	g.releaseSend(nil)
	deliverResult(t, c, operationOf(t, request), ports.BrokerOutcomeOK, true)
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
				removed, err := call.call(ctx, c, call.endpoint)
				results <- mutationOutcome{removed: removed, err: err}
			}()
			request := awaitSend(t, g)
			operation := operationOf(t, request)
			require.Equal(t, call.endpoint, endpointOf(t, request), "the captured request is the caller's own")
			cancel()
			got := awaitResult(t, results)
			var failure ports.BrokerError
			require.ErrorAs(t, got.err, &failure)
			require.Equal(t, ports.BrokerErrorOutcomeUnknown, failure.Code)
			require.False(t, got.removed)
			require.Equal(t, 1, g.sendCount(), "an uncertain attempt is never resent")
			// The cancelled attempt is still held inside the carriage. Hand it its
			// result now, so the next mutation's attempt is the only one waiting
			// on the shared release channel.
			g.releaseSend(errTestCarriage)

			// The identity was completed once, so its pending slot is released
			// rather than left in flight.
			require.ErrorIs(t, c.conn.AdmitOperation(operation), brokerwire.ErrOperationCompleted)
			require.Empty(t, pendingOperations(c))

			// A late result for the completed identity is dropped, and it never
			// provokes another wire attempt.
			deliverResult(t, c, operation, ports.BrokerOutcomeOK, true)
			require.Empty(t, pendingOperations(c))
			require.Equal(t, 1, g.sendCount())

			// The connection stays usable: the next mutation is admitted under
			// the same bound of one and completes normally.
			fresh, cancelFresh := context.WithCancel(context.Background())
			defer cancelFresh()
			next := make(chan mutationOutcome, 1)
			go func() {
				removed, err := call.call(fresh, c, call.endpoint)
				next <- mutationOutcome{removed: removed, err: err}
			}()
			second := awaitSend(t, g)
			g.releaseSend(nil)
			deliverResult(t, c, operationOf(t, second), ports.BrokerOutcomeOK, call.wantRemoved)
			done := awaitResult(t, next)
			require.NoError(t, done.err)
			require.Equal(t, call.wantRemoved, done.removed)
			require.Equal(t, 2, g.sendCount())
		})
	}
}

// TestMutationDeliveredReplyIsDefinite proves an ordinary delivered reply stays
// the definite outcome for both operations, and a repeated result for the same
// identity changes nothing.
func TestMutationDeliveredReplyIsDefinite(t *testing.T) {
	for _, call := range mutationCalls {
		t.Run(call.name, func(t *testing.T) {
			c, g := newGatedClient(t, Config{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			results := make(chan mutationOutcome, 1)
			go func() {
				removed, err := call.call(ctx, c, call.endpoint)
				results <- mutationOutcome{removed: removed, err: err}
			}()
			request := awaitSend(t, g)
			operation := operationOf(t, request)
			require.Equal(t, call.endpoint, endpointOf(t, request))
			g.releaseSend(nil)
			deliverResult(t, c, operation, ports.BrokerOutcomeOK, call.wantRemoved)
			got := awaitResult(t, results)
			require.NoError(t, got.err)
			require.Equal(t, call.wantRemoved, got.removed)
			require.Equal(t, 1, g.sendCount())
			require.Empty(t, pendingOperations(c))

			// A replayed result for an already settled identity is dropped.
			deliverResult(t, c, operation, ports.BrokerOutcomeUnknown, true)
			require.Empty(t, pendingOperations(c))
			require.Equal(t, 1, g.sendCount())
		})
	}
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
			Operation: ports.BrokerOperationID{0x01}, Endpoint: "pre@host:22",
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
			Operation: ports.BrokerOperationID{0x01}, Endpoint: "pre@host:22",
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
				Operation: ports.BrokerOperationID{0x02}, Endpoint: "known",
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
