package brokeripc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// TestResubscribePublishesCurrentSnapshotPerGeneration races repeated
// resubscriptions: a superseded publisher must never consume the wake meant for
// its successor, so the generation that ends a burst publishes the current
// snapshot without any further core wake, and a resync republishes it. The
// whole burst runs with no yield between generations, so a shared wake channel
// would coalesce a later generation's demand into a superseded publisher.
func TestResubscribePublishesCurrentSnapshotPerGeneration(t *testing.T) {
	const (
		epoch       = ports.BrokerEpoch(0x51)
		generations = 32
		bursts      = 50
	)
	for burst := 0; burst < bursts; burst++ {
		transport := newRecordingTransport()
		core := newTestCore(epoch)
		session, err := newServerSession(epoch, transport, brokerwire.DefaultCeilings(), core, Config{}, func() {}, time.Time{})
		require.NoError(t, err)
		require.NoError(t, session.conn.Register())

		for index := 1; index <= generations; index++ {
			generation := brokerwire.SubscriptionGeneration(index)
			core.hub.publish(testSnapshot(epoch, ports.BrokerRevision(index)))
			require.NoError(t, session.conn.Subscribe(generation))
			require.NoError(t, session.retargetPublisher(generation))
		}

		// Each publisher owns its wake, so a later generation can never be left
		// waiting on a demand a superseded publisher consumed.
		session.subMu.Lock()
		active := session.pub
		session.subMu.Unlock()
		require.NotNil(t, active)
		require.Equal(t, brokerwire.SubscriptionGeneration(generations), active.generation)
		require.True(t,
			transport.waitForTransfer(brokerwire.SubscriptionGeneration(generations), ports.BrokerRevision(generations), 5*time.Second),
			"the final generation must publish the current snapshot for itself")

		// A resync republishes the current snapshot for the active generation.
		before := transport.transferCount(brokerwire.SubscriptionGeneration(generations), ports.BrokerRevision(generations))
		require.NoError(t, session.conn.Resync(brokerwire.SubscriptionGeneration(generations)))
		session.wakePublisher()
		require.True(t,
			transport.waitForTransferCount(brokerwire.SubscriptionGeneration(generations), ports.BrokerRevision(generations), before+1, 5*time.Second),
			"a resync must republish the current snapshot")

		session.shutdown()
	}
}

// TestSupersededPublisherOwnsItsWake proves each subscription generation owns
// its own coalescing wake, so a superseded publisher cannot consume the demand
// intended for its successor.
func TestSupersededPublisherOwnsItsWake(t *testing.T) {
	const epoch = ports.BrokerEpoch(0x52)
	transport := newRecordingTransport()
	core := newTestCore(epoch)
	session, err := newServerSession(epoch, transport, brokerwire.DefaultCeilings(), core, Config{}, func() {}, time.Time{})
	require.NoError(t, err)
	require.NoError(t, session.conn.Register())
	t.Cleanup(func() { session.shutdown() })

	core.hub.publish(testSnapshot(epoch, 1))
	require.NoError(t, session.conn.Subscribe(1))
	require.NoError(t, session.retargetPublisher(1))
	session.subMu.Lock()
	first := session.pub
	session.subMu.Unlock()

	core.hub.publish(testSnapshot(epoch, 2))
	require.NoError(t, session.conn.Subscribe(2))
	require.NoError(t, session.retargetPublisher(2))
	session.subMu.Lock()
	second := session.pub
	session.subMu.Unlock()

	require.NotNil(t, first)
	require.NotNil(t, second)
	require.NotSame(t, first, second)
	require.NotEqual(t, first.wake, second.wake, "each generation must own its wake channel")
}

// TestSessionCloseStreamFillsConnectionScope proves the session CloseStream
// delegates with this connection's scope exactly like OpenStream: an absent
// identity is filled from the session and a foreign one is refused.
func TestSessionCloseStreamFillsConnectionScope(t *testing.T) {
	const epoch = ports.BrokerEpoch(0x53)
	transport := newRecordingTransport()
	core := newTestCore(epoch)
	session, err := newServerSession(epoch, transport, brokerwire.DefaultCeilings(), core, Config{}, func() {}, time.Time{})
	require.NoError(t, err)
	t.Cleanup(func() { session.shutdown() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opened, err := session.OpenStream(ctx, ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl, Local: true, Stream: 1, Policy: testPolicy(),
	})
	require.NoError(t, err)
	require.NotNil(t, opened)

	core.mu.Lock()
	require.Len(t, core.opens, 1)
	require.Equal(t, core.id, core.opens[0].Connection, "OpenStream fills the session scope")
	require.Equal(t, ports.BrokerStreamID(1), core.opens[0].Stream)
	core.mu.Unlock()

	// A zero identity is filled from the session instead of reaching the core
	// as a stale request.
	require.NoError(t, session.CloseStream(ports.BrokerConnectionID{}, 1))
	core.mu.Lock()
	require.Equal(t, []ports.BrokerStreamID{1}, core.closedIDs)
	core.mu.Unlock()

	// A foreign identity is still refused.
	require.ErrorIs(t, session.CloseStream(ports.BrokerConnectionID{0x99}, 2), ports.BrokerAdmissionStale)
}

// TestRegistrationAssignsScopeAtAccept proves the client adapter adopts exactly
// the scope the listener assigned at accept.
func TestRegistrationAssignsScopeAtAccept(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, session := e.pair()
	require.Equal(t, session.ConnectionID(), client.ConnectionID())
	require.False(t, client.ConnectionID().IsZero())
}

// TestSubscribePublishesSnapshot proves subscription traffic: a committed
// publication reaches the client, wakes its subscription, and round-trips host,
// session, and tombstone state.
func TestSubscribePublishesSnapshot(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	sub, err := client.Subscribe()
	require.NoError(t, err)
	t.Cleanup(sub.Close)

	snapshot := e.publish(e.epoch, 7)
	require.Eventually(t, func() bool {
		return client.Snapshot().Revision == snapshot.Revision
	}, 5*time.Second, 10*time.Millisecond, "committed publication must reach the client")

	got := client.Snapshot()
	require.Equal(t, e.epoch, got.Epoch)
	require.Len(t, got.Hosts, 1)
	require.Equal(t, snapshot.Hosts[0].Endpoint, got.Hosts[0].Endpoint)
	require.Len(t, got.Hosts[0].Sessions, 2)
	require.Equal(t, snapshot.Hosts[0].Sessions[0].Name, got.Hosts[0].Sessions[0].Name)

	select {
	case <-sub.Changed():
	case <-time.After(5 * time.Second):
		t.Fatal("subscription was not woken by a committed publication")
	}
}

// TestSubscribeGenerationAdvancesAndReplaces proves a second Subscribe advances
// the generation series (refusing reuse) and replaces the previous
// subscription.
func TestSubscribeGenerationAdvancesAndReplaces(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	first, err := client.Subscribe()
	require.NoError(t, err)
	second, err := client.Subscribe()
	require.NoError(t, err)
	t.Cleanup(second.Close)

	select {
	case _, open := <-first.Changed():
		// The previous subscription is closed: a closed channel reads
		// immediately with open=false.
		require.False(t, open, "a superseded subscription must be closed")
	case <-time.After(time.Second):
		t.Fatal("the superseded subscription must be closed")
	}

	e.publish(e.epoch, 3)
	require.Eventually(t, func() bool { return client.Snapshot().Revision == 3 }, 5*time.Second, 10*time.Millisecond)
}

// subscribeResult is one Subscribe outcome.
type subscribeResult struct {
	sub ports.BrokerSubscription
	err error
}

// subscribeGenerationOf extracts the generation of one captured Subscribe
// frame.
func subscribeGenerationOf(t *testing.T, message brokerwire.ClientMessage) uint64 {
	t.Helper()
	subscribe, ok := message.(brokerwire.Subscribe)
	require.True(t, ok, "the captured attempt must be a Subscribe frame")
	return subscribe.Generation
}

// awaitSubscribe waits for one Subscribe call to return.
func awaitSubscribe(t *testing.T, results <-chan subscribeResult) subscribeResult {
	t.Helper()
	select {
	case got := <-results:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return")
		return subscribeResult{}
	}
}

// TestSubscribeSendFailureRetiresPreviousSubscription proves a Subscribe whose
// frame never leaves has already consumed the connection's generation series:
// the superseded subscription is retired instead of being restored, so no stale
// subscription is presented as the live one and none stays current.
func TestSubscribeSendFailureRetiresPreviousSubscription(t *testing.T) {
	c, carriage := newGatedClient(t, Config{})

	first := make(chan subscribeResult, 1)
	go func() {
		sub, err := c.Subscribe()
		first <- subscribeResult{sub, err}
	}()
	require.Equal(t, uint64(1), subscribeGenerationOf(t, awaitSend(t, carriage)))
	carriage.releaseSend(nil)
	previous := awaitSubscribe(t, first)
	require.NoError(t, previous.err)
	require.NotNil(t, previous.sub)

	// The second Subscribe consumes generation 2 on the connection's tracker
	// before its carriage write fails.
	second := make(chan subscribeResult, 1)
	go func() {
		sub, err := c.Subscribe()
		second <- subscribeResult{sub, err}
	}()
	require.Equal(t, uint64(2), subscribeGenerationOf(t, awaitSend(t, carriage)))
	carriage.releaseSend(errTestCarriage)

	failed := awaitSubscribe(t, second)
	require.ErrorIs(t, failed.err, errTestCarriage)
	require.Nil(t, failed.sub, "a failed Subscribe returns no subscription")

	select {
	case _, open := <-previous.sub.Changed():
		require.False(t, open, "the superseded subscription must be closed")
	case <-time.After(time.Second):
		t.Fatal("the previous subscription must be closed")
	}

	c.mu.Lock()
	current, generation := c.sub, c.generation
	c.mu.Unlock()
	require.Nil(t, current, "a failed Subscribe must not leave a current subscription")
	require.Equal(t, uint64(2), uint64(generation), "the consumed generation is not rolled back")
}

// TestOpenStreamCarriesTypedTraffic proves a logical stream carries the typed
// session protocol in both directions through the broker.
func TestOpenStreamCarriesTypedTraffic(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl,
		Local:   true,
		Policy:  testPolicy(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	conn := core.logicalConn(1)
	require.NotNil(t, conn, "the broker core must admit exactly one stream")

	require.NoError(t, stream.SendClient(protocol.Ping{}))
	select {
	case message := <-conn.fromClient:
		require.Equal(t, protocol.Ping{}, message, "the client message must reach the broker core")
	case <-time.After(5 * time.Second):
		t.Fatal("client message never reached the broker core")
	}

	conn.toClient <- protocol.Pong{}
	done := make(chan protocol.ServerMessage, 1)
	go func() {
		message, err := stream.ReceiveServer()
		if err != nil {
			done <- nil
			return
		}
		done <- message
	}()
	select {
	case message := <-done:
		require.Equal(t, protocol.Pong{}, message, "the daemon message must reach the client")
	case <-time.After(5 * time.Second):
		t.Fatal("daemon message never reached the client")
	}
}

// TestOpenStreamRefusedByCoreReachesClient proves a core refusal settles only
// that stream and reaches the caller as a typed failure.
func TestOpenStreamRefusedByCoreReachesClient(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()
	core.openErr = ports.BrokerError{Code: ports.BrokerErrorIncompatible}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl,
		Local:   true,
		Policy:  testPolicy(),
	})
	require.Error(t, err)
	require.Nil(t, stream)
	var failure ports.BrokerError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, ports.BrokerErrorIncompatible, failure.Code)

	// The connection survives a stream-local refusal: a later operation still
	// completes.
	require.NoError(t, client.AddHost(ctx, "after@refusal:22"))
}

// TestTerminalStreamFailureFailsOnlyThatStream proves one stream's terminal
// failure is reported to its owner and leaves a sibling stream usable.
func TestTerminalStreamFailureFailsOnlyThatStream(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()})
	require.NoError(t, err)
	second, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })

	core.logicalConn(1).fail(ports.BrokerError{Code: ports.BrokerErrorAttachmentLost})

	select {
	case <-first.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a failed stream must publish its terminal outcome")
	}
	require.Error(t, first.Err())
	select {
	case <-second.Done():
		t.Fatal("a failing sibling must not settle an unrelated stream")
	default:
	}

	// The sibling still carries traffic.
	require.NoError(t, second.SendClient(protocol.Ping{}))
	select {
	case message := <-core.logicalConn(2).fromClient:
		require.Equal(t, protocol.Ping{}, message)
	case <-time.After(5 * time.Second):
		t.Fatal("sibling stream stopped carrying traffic")
	}
	require.NoError(t, client.AddHost(ctx, "sibling@host:22"))
}

// TestCloseStreamRetiresBothSides proves a client close reaches the broker core
// and retires the stream identity.
func TestCloseStreamRetiresBothSides(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()})
	require.NoError(t, err)
	require.NoError(t, client.CloseStream(client.ConnectionID(), 1))

	require.Eventually(t, func() bool {
		core.mu.Lock()
		defer core.mu.Unlock()
		for _, id := range core.closedIDs {
			if id == 1 {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "the broker core must retire the closed stream")
	require.NoError(t, stream.Close(), "closing an already retired stream is idempotent")
}

// TestMutatingOperationsComplete proves AddHost and RemoveHost complete with
// typed results and reach the admitted core service.
func TestMutatingOperationsComplete(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, client.AddHost(ctx, "new@host:22"))
	removed, err := client.RemoveHost(ctx, "known")
	require.NoError(t, err)
	require.True(t, removed)
	removed, err = client.RemoveHost(ctx, "missing")
	require.NoError(t, err)
	require.False(t, removed)

	core.mu.Lock()
	defer core.mu.Unlock()
	require.Equal(t, []string{"new@host:22"}, core.added)
	require.Equal(t, []string{"known", "missing"}, core.removed)
}

// TestLostOperationReplyIsOutcomeUnknown proves a mutating operation whose reply
// never arrives is reported as outcome-unknown rather than retried or reported
// as a plain failure.
func TestLostOperationReplyIsOutcomeUnknown(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()
	core.blockAdd = make(chan struct{})
	t.Cleanup(func() { close(core.blockAdd) })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := client.AddHost(ctx, "slow@host:22")
	require.Error(t, err)
	var failure ports.BrokerError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, ports.BrokerErrorOutcomeUnknown, failure.Code)
}

// TestDisconnectCleanupReleasesEveryOwnedResource proves a client disconnect
// deterministically retires its streams, closes its admitted core service, and
// releases its listener slot.
func TestDisconnectCleanupReleasesEveryOwnedResource(t *testing.T) {
	e := startEndpoint(t, Config{MaxClients: 2, HandshakeTimeout: time.Second})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()})
	require.NoError(t, err)
	conn := core.logicalConn(1)
	require.NotNil(t, conn)

	require.NoError(t, client.Close())

	select {
	case <-core.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the admitted core service must be closed on disconnect")
	}
	select {
	case <-conn.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the bridged core connection must be closed on disconnect")
	}

	require.Eventually(t, func() bool {
		return e.listener.(*listener).liveSessions() == 0
	}, 5*time.Second, 10*time.Millisecond, "a retired session must leave the listener's live set")

	// The listener slot is reusable: a fresh client is admitted.
	fresh := e.dial()
	session := e.accept()
	require.Equal(t, fresh.ConnectionID(), session.ConnectionID())
}

// TestStalledSubscriberDoesNotBlockOtherClients proves publications are
// per-connection: a client that subscribes and never reads cannot delay another
// client's publication or operations.
func TestStalledSubscriberDoesNotBlockOtherClients(t *testing.T) {
	e := startEndpoint(t, Config{MaxClients: 4})
	stalled, _ := e.pair()
	_, err := stalled.Subscribe()
	require.NoError(t, err)

	active, _ := e.pair()
	sub, err := active.Subscribe()
	require.NoError(t, err)
	t.Cleanup(sub.Close)

	for revision := ports.BrokerRevision(1); revision <= 20; revision++ {
		e.publish(e.epoch, revision)
	}
	require.Eventually(t, func() bool { return active.Snapshot().Revision == 20 }, 5*time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, active.AddHost(ctx, "active@host:22"))
}

// TestClientStreamBackpressureSettlesOnlyThatStream proves a local consumer that
// stops draining a stream's bounded inbound queue settles exactly that stream:
// the peer is told, the stream reports backpressure, and the connection and its
// operations stay up.
func TestClientStreamBackpressureSettlesOnlyThatStream(t *testing.T) {
	e := startEndpoint(t, Config{MaxClients: 2})
	client := e.dialWith(Config{StreamInboundChunks: 1})
	_ = e.accept()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()})
	require.NoError(t, err)
	conn := core.logicalConn(1)
	require.NotNil(t, conn)

	// Two daemon messages arrive while the local consumer never reads: the
	// bounded capacity-one queue refuses the second one.
	conn.toClient <- protocol.Pong{}
	conn.toClient <- protocol.Pong{}

	select {
	case <-stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled stream consumer must settle the stream")
	}
	require.ErrorIs(t, stream.(*clientStream).Err(), ErrStreamBackpressure)

	require.Eventually(t, func() bool {
		core.mu.Lock()
		defer core.mu.Unlock()
		return len(core.closedIDs) > 0
	}, 5*time.Second, 10*time.Millisecond, "the broker must be told to retire the stalled stream")

	// The connection survives a stream-local stall.
	require.NoError(t, client.AddHost(ctx, "after@stall:22"))
}

// TestOrderlyStreamCloseReportsNoError proves a local close is an orderly end:
// Done closes and Err stays nil, so a caller can distinguish it from a failure.
func TestOrderlyStreamCloseReportsNoError(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()})
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	select {
	case <-stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a local close must publish its terminal outcome")
	}
	require.NoError(t, stream.Err())
	require.NoError(t, stream.Close(), "closing twice is idempotent")
}

// TestScopeIdentityFencing proves the client adapter refuses a caller-supplied
// identity that belongs to another connection and stream identities that do not
// advance.
func TestScopeIdentityFencing(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	foreign := ports.BrokerConnectionID{0x99}
	_, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl, Local: true, Connection: foreign, Policy: testPolicy(),
	})
	require.ErrorIs(t, err, ports.BrokerAdmissionStale)

	_, err = client.OpenStream(ctx, ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl, Local: true, Epoch: e.epoch + 1, Policy: testPolicy(),
	})
	require.ErrorIs(t, err, ErrScopeMismatch)
	var stale ports.BrokerError
	require.ErrorAs(t, err, &stale)
	require.Equal(t, ports.BrokerErrorStaleEpoch, stale.Code)

	first, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	_, err = client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Stream: 1, Policy: testPolicy()})
	require.ErrorIs(t, err, ports.BrokerAdmissionStale)
}

// TestRequestReconcileIsBoundedHint proves the reconcile hint is sent only for an
// endpoint the connection already holds a registration for, and that it is
// otherwise a no-op.
func TestRequestReconcileIsBoundedHint(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	client.RequestReconcile("unknown@host:22")
	time.Sleep(50 * time.Millisecond)
	core.mu.Lock()
	require.Empty(t, core.reconcile)
	core.mu.Unlock()

	sub, err := client.Subscribe()
	require.NoError(t, err)
	t.Cleanup(sub.Close)
	snapshot := e.publish(e.epoch, 4)
	require.Eventually(t, func() bool { return client.Snapshot().Revision == 4 }, 5*time.Second, 10*time.Millisecond)

	client.RequestReconcile(snapshot.Hosts[0].Endpoint)
	require.Eventually(t, func() bool {
		core.mu.Lock()
		defer core.mu.Unlock()
		return len(core.reconcile) == 1 && core.reconcile[0] == snapshot.Hosts[0].Endpoint
	}, 5*time.Second, 10*time.Millisecond)
}

// TestCoreErrorSurfacesAsTypedFailure proves a typed core failure reaches the
// client through the stream lifecycle.
func TestCoreErrorSurfacesAsTypedFailure(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	core.logicalConn(1).fail(ports.BrokerError{Code: ports.BrokerErrorAttachmentLost, Text: "daemon went away"})
	select {
	case <-stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("core loss must settle the stream")
	}
	var failure ports.BrokerError
	require.ErrorAs(t, stream.Err(), &failure)
	require.Equal(t, ports.BrokerErrorAttachmentLost, failure.Code)
	require.Equal(t, "daemon went away", failure.Text)
}

// TestSessionHandshakeFailureIsStreamLocal proves a malformed inner session
// stream settles only that stream.
func TestSessionHandshakeFailureIsStreamLocal(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy()})
	require.NoError(t, err)
	conn := core.logicalConn(1)
	require.NotNil(t, conn)

	// The client emits a malformed inner session frame: the broker's session
	// carriage refuses it and resets only this stream.
	raw := stream.(*clientStream)
	_, err = raw.pipe.Write([]byte{0, 0, 0, 4, 0xff, 0xff, 0xff, 0xff})
	require.NoError(t, err)
	select {
	case <-stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a malformed inner frame must settle its stream")
	}
	require.NoError(t, client.AddHost(ctx, "survivor@host:22"))

	// The broker core sees the stream retired.
	require.Eventually(t, func() bool {
		return core.logicalConn(1) == nil
	}, 5*time.Second, 10*time.Millisecond)
}

// TestDomainFixturesAreValid guards the test fixtures against drifting from the
// contracts the wire enforces.
func TestDomainFixturesAreValid(t *testing.T) {
	snapshot := testSnapshot(1, 1)
	require.NoError(t, snapshot.Validate())
	for _, host := range snapshot.Hosts {
		require.NoError(t, ports.ValidateDurableHostProjection(host))
	}
	require.NoError(t, testPolicy().Validate())
	catalogueSession := testSessionAt(0, 0)
	require.False(t, strings.HasPrefix(catalogueSession.Name, " "))
	require.NotEqual(t, domain.RemoteAvailabilityUnknown, testHostAt(0).Availability)
	require.True(t, errors.Is(errors.Join(ErrScopeMismatch, ports.BrokerAdmissionStale), ports.BrokerAdmissionStale))
}
