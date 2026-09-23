package brokeripc

import (
	"context"
	"errors"
	"io"
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
			require.NoError(t, session.retargetPublisher(generation, false))
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
	require.NoError(t, session.retargetPublisher(1, false))
	session.subMu.Lock()
	first := session.pub
	session.subMu.Unlock()

	core.hub.publish(testSnapshot(epoch, 2))
	require.NoError(t, session.conn.Subscribe(2))
	require.NoError(t, session.retargetPublisher(2, false))
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
	opened, err := session.OpenEnvelopeStream(ctx, openRequest(1))
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

// TestOpenStreamAdmissionRoundTrips proves the attachment-admission contract
// travels the IPC wire: a create-named request reaches the core with exactly
// its admission and validated name.
func TestOpenStreamAdmissionRoundTrips(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()
	require.NotNil(t, core)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, attachmentRequest(1, ports.BrokerAdmissionCreateNamed, "work"))
	require.NoError(t, err)
	require.NotNil(t, stream)

	core.mu.Lock()
	defer core.mu.Unlock()
	require.Len(t, core.opens, 1)
	require.Equal(t, ports.BrokerAdmissionCreateNamed, core.opens[0].Admission)
	require.Equal(t, "work", core.opens[0].Name)
	require.Equal(t, ports.BrokerStreamAttachment, core.opens[0].Purpose)
	require.True(t, core.opens[0].Local)
	require.Equal(t, ports.BrokerDaemonStartIfNeeded, core.opens[0].StartMode)
}

// TestObservationForcesExistingOnly proves the observation authorization
// travels the IPC wire: a control stream may carry start-if-needed, while an
// observation must carry existing-only and a spawn-capable observation is
// refused before any frame travels.
func TestObservationForcesExistingOnly(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()
	require.NotNil(t, core)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streamID := nextStreamID(t, client)
	observation := openRequest(streamID)
	observation.Purpose = ports.BrokerStreamObservation
	observation.StartMode = ports.BrokerDaemonExistingOnly
	stream, err := client.OpenStream(ctx, observation)
	require.NoError(t, err)
	require.NoError(t, stream.Close())

	core.mu.Lock()
	defer core.mu.Unlock()
	require.Len(t, core.opens, 1, "the existing-only observation reached the core")
	require.Equal(t, ports.BrokerStreamObservation, core.opens[0].Purpose)
	require.Equal(t, ports.BrokerDaemonExistingOnly, core.opens[0].StartMode)
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
	require.Len(t, got.Daemons, 1)
	require.Equal(t, snapshot.Daemons[0].Endpoint, got.Daemons[0].Endpoint)
	require.Len(t, got.Daemons[0].Sessions, 2)
	require.Equal(t, snapshot.Daemons[0].Sessions[0].Name, got.Daemons[0].Sessions[0].Name)

	select {
	case <-sub.Changed():
	case <-time.After(5 * time.Second):
		t.Fatal("subscription was not woken by a committed publication")
	}
}

// TestSubscribeCarriesPassiveReader proves a client configured as a passive
// reader reaches the core's passive subscription, while a default client
// subscribes as a watching client that records observation demand.
func TestSubscribeCarriesPassiveReader(t *testing.T) {
	tests := []struct {
		name    string
		passive bool
	}{
		{name: "watching client", passive: false},
		{name: "passive reader", passive: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := startEndpoint(t, Config{})
			client := e.dialWith(Config{PassiveSubscribe: tt.passive})
			e.accept()
			sub, err := client.Subscribe()
			require.NoError(t, err)
			t.Cleanup(sub.Close)
			core := e.authority.last()
			require.Eventually(t, func() bool { return len(core.subscribeKinds()) == 1 }, 5*time.Second, 10*time.Millisecond)
			require.Equal(t, []bool{tt.passive}, core.subscribeKinds())
		})
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
	stream, err := client.OpenStream(ctx, openRequest(1))
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

// TestReplyReachesClientBeforeOrderlyStreamClose proves a reply the broker core
// sends immediately before an orderly stream end (a StreamClosed carrying no
// error) always reaches the client's ReceiveServer before it observes EOF.
// This is the exact end-to-end sequence that dropped about 70% of `vev ls`
// calls: the daemon's handleList receives the request, does
// SendServer(Sessions) then Close, so the broker forwards protocol.Sessions and
// immediately sees the core end orderly. Before the fix, watchCore settled the
// stream (closing the client-side pipe and discarding its queue) racing the
// client's read of the already-forwarded reply; the client observed io.EOF
// instead of the reply and reported the broker operation lost or its outcome
// unknown. It is run repeatedly under -race because the race window is narrow,
// not guaranteed on a single pass.
//
// The request is sent and its receipt is confirmed before the daemon answers,
// exactly like every real caller (BrokerOperations always calls SendClient
// before ReceiveServer): a reply pushed before the daemon has even received
// the request races this fake's own SendClient/Done selection, which is a
// harness artifact unrelated to the fix under test.
func TestReplyReachesClientBeforeOrderlyStreamClose(t *testing.T) {
	e := startEndpoint(t, Config{})
	const iterations = 200
	for i := 0; i < iterations; i++ {
		client, _ := e.pair()
		core := e.authority.last()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := client.OpenStream(ctx, openRequest(1))
		require.NoError(t, err)

		conn := core.logicalConn(1)
		require.NotNil(t, conn, "iteration %d: the broker core must admit exactly one stream", i)

		require.NoError(t, stream.SendClient(protocol.List{}), "iteration %d", i)
		select {
		case <-conn.fromClient:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: request never reached the daemon", i)
		}

		// The daemon answers and, back to back, ends the stream orderly: exactly
		// the sequence SendServer(Sessions) then Close produces.
		conn.toClient <- protocol.Sessions{}
		require.NoError(t, conn.Close())

		message, err := stream.ReceiveServer()
		require.NoError(t, err, "iteration %d: the reply must reach the client before EOF", i)
		require.Equal(t, protocol.Sessions{}, message, "iteration %d", i)

		// The stream settles orderly right after: a further receive observes a
		// clean EOF, never an error, and never blocks.
		_, err = stream.ReceiveServer()
		require.ErrorIs(t, err, io.EOF, "iteration %d", i)

		_ = stream.Close()
		_ = client.Close()
		cancel()
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
	stream, err := client.OpenStream(ctx, openRequest(1))
	require.Error(t, err)
	require.Nil(t, stream)
	var failure ports.BrokerError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, ports.BrokerErrorIncompatible, failure.Code)

	// The connection survives a stream-local refusal: once the core admits
	// streams again, a later operation still completes.
	core.mu.Lock()
	core.openErr = nil
	core.mu.Unlock()
	requireConnectionDelegates(t, client)
}

// TestTerminalStreamFailureFailsOnlyThatStream proves one stream's terminal
// failure is reported to its owner and leaves a sibling stream usable.
func TestTerminalStreamFailureFailsOnlyThatStream(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := client.OpenStream(ctx, openRequest(nextStreamID(t, client)))
	require.NoError(t, err)
	second, err := client.OpenStream(ctx, openRequest(nextStreamID(t, client)))
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
	requireConnectionDelegates(t, client)
}

// TestCloseStreamRetiresBothSides proves a client close reaches the broker core
// and retires the stream identity.
func TestCloseStreamRetiresBothSides(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, openRequest(nextStreamID(t, client)))
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

// TestMembershipMutationsDelegateEndToEnd proves the whole membership contract
// travels the real AF_UNIX carriage in both directions: AddHost returns the
// registration the admitted core minted, UpdateHostPolicy returns the
// generation-advanced registration for the exact authority the caller named, and
// RemoveHost returns the core's exact bool. Each call delegates exactly once with
// the caller's own endpoint, policy, and exact registration, so the adapter never
// re-derives authority on the way.
func TestMembershipMutationsDelegateEndToEnd(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()
	core := e.authority.last()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	added, err := client.AddHost(ctx, "new@host:22", testPolicy())
	require.NoError(t, err)
	require.Equal(t, "new@host:22", added.Endpoint)
	require.NotZero(t, added.Generation, "an addition answer is fenced authority")
	require.NoError(t, added.Validate())

	updated, err := client.UpdateHostPolicy(ctx, added, testPolicy())
	require.NoError(t, err)
	require.Equal(t, added.Endpoint, updated.Endpoint)
	require.Equal(t, added.Incarnation, updated.Incarnation, "a policy update preserves identity")
	require.Equal(t, added.Generation+1, updated.Generation,
		"a policy update returns a registration advanced by exactly one generation")

	core.reportRemoved(true)
	removed, err := client.RemoveHost(ctx, updated)
	require.NoError(t, err)
	require.True(t, removed, "a present exact registration must report removal")

	core.reportRemoved(false)
	removed, err = client.RemoveHost(ctx, updated)
	require.NoError(t, err)
	require.False(t, removed, "an absent exact registration is an idempotent no-op, not an error")

	adds := core.addCalls()
	require.Len(t, adds, 1, "one addition is exactly one delegation")
	require.Equal(t, "new@host:22", adds[0].Endpoint)
	require.Equal(t, testPolicy(), adds[0].Policy, "the caller's exact policy must be delegated unmodified")
	require.Equal(t, added, adds[0].Result)

	updates := core.updateCalls()
	require.Len(t, updates, 1, "one policy update is exactly one delegation")
	require.Equal(t, added, updates[0].Expected, "the exact expected registration must be delegated unmodified")
	require.Equal(t, testPolicy(), updates[0].Policy, "the caller's exact replacement policy must be delegated")
	require.Equal(t, updated, updates[0].Result)

	removes := core.removeCalls()
	require.Len(t, removes, 2, "each removal is exactly one delegation")
	require.Equal(t, updated, removes[0].Expected, "the exact expected registration must be delegated")
	require.Equal(t, updated, removes[1].Expected)
}

// TestMembershipRefusalCodesEndToEnd proves the two membership refusal codes
// travel end to end: a stale generation removal is host conflict (11) and an
// immutable registry is membership-immutable (12). The refusal reaches the caller
// as a typed ports.BrokerError carrying the exact code and the matching sentinel
// as its cause, reports no authority, and never invents a registration or a
// removal.
func TestMembershipRefusalCodesEndToEnd(t *testing.T) {
	cases := []struct {
		name      string
		configure func(core *fakeCore)
		call      func(ctx context.Context, client ports.BrokerService) (domain.RemoteRegistration, bool, error)
		wantCode  ports.BrokerErrorCode
	}{
		{
			name:      "stale generation removal is host conflict 11",
			configure: func(core *fakeCore) { core.refuseRemoveHost(ports.ErrBrokerHostConflict) },
			call: func(ctx context.Context, client ports.BrokerService) (domain.RemoteRegistration, bool, error) {
				removed, err := client.RemoveHost(ctx, mutationRegistration("known"))
				return domain.RemoteRegistration{}, removed, err
			},
			wantCode: ports.BrokerErrorHostConflict,
		},
		{
			name:      "immutable registry is membership immutable 12",
			configure: func(core *fakeCore) { core.refuseAddHost(ports.ErrBrokerMembershipImmutable) },
			call: func(ctx context.Context, client ports.BrokerService) (domain.RemoteRegistration, bool, error) {
				registration, err := client.AddHost(ctx, "new@host:22", testPolicy())
				return registration, false, err
			},
			wantCode: ports.BrokerErrorMembershipImmutable,
		},
	}
	require.Equal(t, ports.BrokerErrorCode(11), ports.BrokerErrorHostConflict)
	require.Equal(t, ports.BrokerErrorCode(12), ports.BrokerErrorMembershipImmutable)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := startEndpoint(t, Config{})
			client, _ := e.pair()
			core := e.authority.last()
			tc.configure(core)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			registration, removed, err := tc.call(ctx, client)
			require.Equal(t, domain.RemoteRegistration{}, registration, "a refusal never invents authority")
			require.False(t, removed, "a refusal never reports the host gone")
			require.Error(t, err)
			var failure ports.BrokerError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, tc.wantCode, failure.Code, "the exact refusal code must reach the caller")
			require.NotEqual(t, ports.BrokerErrorOutcomeUnknown, failure.Code,
				"a definite refusal is never mistaken for a refresh-required outcome")

			// The connection survived the refusal: a later delegation still works.
			requireConnectionDelegates(t, client)
		})
	}
}

// TestMembershipWireRefusalFraming proves the refusal framing the server emits is
// exactly the contract the wire validates: a failed result carrying its typed
// error and no authority, under the exact code, with the operation identity the
// caller used. It drives the raw carriage so the frames are observed as a peer
// would receive them.
func TestMembershipWireRefusalFraming(t *testing.T) {
	cases := []struct {
		name      string
		configure func(core *fakeCore)
		send      func(scope brokerwire.Scope) brokerwire.ClientMessage
		kind      brokerwire.RegisterMutationKind
		wantCode  ports.BrokerErrorCode
	}{
		{
			name:      "remove conflict",
			configure: func(core *fakeCore) { core.refuseRemoveHost(ports.ErrBrokerHostConflict) },
			send: func(scope brokerwire.Scope) brokerwire.ClientMessage {
				return brokerwire.RemoveHost{
					Epoch: scope.Epoch, Connection: scope.Connection,
					Operation: ports.BrokerOperationID{0x31}, Registration: mutationRegistration("known"),
				}
			},
			kind:     brokerwire.MutationKindRemoveHost,
			wantCode: ports.BrokerErrorHostConflict,
		},
		{
			name:      "immutable addition",
			configure: func(core *fakeCore) { core.refuseAddHost(ports.ErrBrokerMembershipImmutable) },
			send: func(scope brokerwire.Scope) brokerwire.ClientMessage {
				return brokerwire.AddHost{
					Epoch: scope.Epoch, Connection: scope.Connection,
					Operation: ports.BrokerOperationID{0x32}, Endpoint: "new@host:22", Policy: testPolicy(),
				}
			},
			kind:     brokerwire.MutationKindAddHost,
			wantCode: ports.BrokerErrorMembershipImmutable,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := startEndpoint(t, Config{})
			raw := rawDial(t, e, brokerwire.DefaultCeilings())
			scope := raw.register(t)
			tc.configure(e.authority.last())

			request := tc.send(scope)
			raw.send(t, request)
			result, ok := raw.recv(t).(brokerwire.OperationResult)
			require.True(t, ok, "a membership mutation must be answered with an OperationResult")
			require.Equal(t, scope.Connection, result.Connection)
			require.Equal(t, operationOf(t, request), result.Operation)
			require.Equal(t, ports.BrokerOutcomeFailed, result.Outcome)
			require.True(t, result.HasError, "a failure must carry its typed detail")
			require.Equal(t, tc.wantCode, result.Error.Code, "the exact refusal code must travel")
			require.False(t, result.Removed, "a failure carries no removal authority")
			require.Equal(t, domain.RemoteRegistration{}, result.Registration,
				"a failure carries no registration authority")
			// The emitted result satisfies the same rule the peer enforces for
			// this request kind.
			require.NoError(t, result.ValidateForMutation(tc.kind))
		})
	}
}

// TestMembershipStoreOutcomeUnknownMapsToOutcomeUnknown proves an indeterminate
// durable write is never presented as a definite refusal: both the value and the
// pointer form of the store's outcome-unknown error become an outcome-unknown
// result carrying the outcome-unknown code and no authority, and the caller
// observes ports.BrokerErrorOutcomeUnknown so it refreshes rather than retries.
func TestMembershipStoreOutcomeUnknownMapsToOutcomeUnknown(t *testing.T) {
	cause := errors.New("torn write after the commit point")
	cases := map[string]error{
		"value":   ports.BrokerStoreOutcomeUnknownError{Err: cause},
		"pointer": &ports.BrokerStoreOutcomeUnknownError{Err: cause},
	}
	for name, stored := range cases {
		t.Run(name, func(t *testing.T) {
			e := startEndpoint(t, Config{})
			raw := rawDial(t, e, brokerwire.DefaultCeilings())
			scope := raw.register(t)
			core := e.authority.last()
			core.refuseAddHost(stored)

			request := brokerwire.AddHost{
				Epoch: scope.Epoch, Connection: scope.Connection,
				Operation: ports.BrokerOperationID{0x41}, Endpoint: "new@host:22", Policy: testPolicy(),
			}
			raw.send(t, request)
			result, ok := raw.recv(t).(brokerwire.OperationResult)
			require.True(t, ok)
			require.Equal(t, ports.BrokerOutcomeUnknown, result.Outcome,
				"an indeterminate write is outcome-unknown, never a definite failure")
			require.Equal(t, ports.BrokerErrorOutcomeUnknown, result.Error.Code)
			require.False(t, result.Removed)
			require.Equal(t, domain.RemoteRegistration{}, result.Registration)
			require.NoError(t, result.ValidateForMutation(brokerwire.MutationKindAddHost))

			// The same indeterminate write reaches the public caller as the
			// refresh-required code, so it is never mistaken for a retryable
			// conflict or a plain failure.
			client, _ := e.pair()
			fresh := e.authority.last()
			fresh.refuseAddHost(stored)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			added, err := client.AddHost(ctx, "new@host:22", testPolicy())
			require.Equal(t, domain.RemoteRegistration{}, added)
			var failure ports.BrokerError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, ports.BrokerErrorOutcomeUnknown, failure.Code)
		})
	}
}

// TestServerAbortsResultMissingRequiredAuthority proves the server never
// publishes an addition success that cannot fence the mutation it answers: an
// admitted core that returns no registration for a successful AddHost is a
// protocol violation, so the session settles without answering the caller
// instead of sending a result the peer would refuse.
func TestServerAbortsResultMissingRequiredAuthority(t *testing.T) {
	e := startEndpoint(t, Config{})
	raw := rawDial(t, e, brokerwire.DefaultCeilings())
	scope := raw.register(t)
	session := e.accept()
	core := e.authority.last()
	core.zeroAddRegistration()

	raw.send(t, brokerwire.AddHost{
		Epoch: scope.Epoch, Connection: scope.Connection,
		Operation: ports.BrokerOperationID{0x51}, Endpoint: "new@host:22", Policy: testPolicy(),
	})

	// No result travels; the connection settles as a protocol violation.
	err := raw.awaitReadError(t, 5*time.Second)
	require.Error(t, err, "a success without its required authority must settle the connection")
	awaitSessionDone(t, session.(*serverSession))
	require.ErrorIs(t, session.(*serverSession).terminalErr(), ErrProtocol)
	require.Error(t, session.Close(), "a protocol violation is not an orderly end")

	// The addition was delegated exactly once; the adapter refused to publish an
	// unfenceable answer rather than re-delegating or inventing one.
	require.Len(t, core.addCalls(), 1)
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
	_, err := client.OpenStream(ctx, openRequest(nextStreamID(t, client)))
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

	requireConnectionDelegates(t, active)
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
	stream, err := client.OpenStream(ctx, openRequest(nextStreamID(t, client)))
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
	requireConnectionDelegates(t, client)
}

// TestOrderlyStreamCloseReportsNoError proves a local close is an orderly end:
// Done closes and Err stays nil, so a caller can distinguish it from a failure.
func TestOrderlyStreamCloseReportsNoError(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, _ := e.pair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, openRequest(nextStreamID(t, client)))
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
		Purpose: ports.BrokerStreamControl, Local: true, Stream: 1, Connection: foreign, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	require.ErrorIs(t, err, ports.BrokerAdmissionStale)

	_, err = client.OpenStream(ctx, ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl, Local: true, Stream: 1, Epoch: e.epoch + 1, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	require.ErrorIs(t, err, ErrScopeMismatch)
	var stale ports.BrokerError
	require.ErrorAs(t, err, &stale)
	require.Equal(t, ports.BrokerErrorStaleEpoch, stale.Code)

	// A concurrent lower identity is admitted: an ID allocated before a higher
	// one can legitimately arrive after it.
	first, err := client.OpenStream(ctx, openRequest(2))
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	second, err := client.OpenStream(ctx, openRequest(1))
	require.NoError(t, err, "a concurrent lower identity is admitted, not refused as stale")
	t.Cleanup(func() { _ = second.Close() })
	// Replaying a consumed identity is refused, and so is an identity evicted
	// past the anti-replay window.
	_, err = client.OpenStream(ctx, openRequest(1))
	require.ErrorIs(t, err, ports.BrokerAdmissionStale, "a replayed identity is stale")
	_, err = client.OpenStream(ctx, openRequest(ports.BrokerStreamWindowSize+3))
	require.NoError(t, err)
	_, err = client.OpenStream(ctx, openRequest(1))
	require.ErrorIs(t, err, ports.BrokerAdmissionStale, "an identity evicted from the window is stale")
	_, err = client.OpenStream(ctx, openRequest(0))
	require.ErrorIs(t, err, ports.BrokerAdmissionInvalid, "a zero identity is never admitted")
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

	client.RequestReconcile(snapshot.Daemons[0].Endpoint)
	require.Eventually(t, func() bool {
		core.mu.Lock()
		defer core.mu.Unlock()
		return len(core.reconcile) == 1 && core.reconcile[0] == snapshot.Daemons[0].Endpoint
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
	stream, err := client.OpenStream(ctx, openRequest(nextStreamID(t, client)))
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
	stream, err := client.OpenStream(ctx, openRequest(nextStreamID(t, client)))
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
	requireConnectionDelegates(t, client)

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
	for _, host := range snapshot.Daemons {
		require.NoError(t, ports.ValidateDurableHostProjection(host))
	}
	require.NoError(t, testPolicy().Validate())
	catalogueSession := testSessionAt(0, 0)
	require.False(t, strings.HasPrefix(catalogueSession.Name, " "))
	require.NotEqual(t, domain.RemoteAvailabilityUnknown, testDaemonAt(0).Availability)
	require.True(t, errors.Is(errors.Join(ErrScopeMismatch, ports.BrokerAdmissionStale), ports.BrokerAdmissionStale))
}
