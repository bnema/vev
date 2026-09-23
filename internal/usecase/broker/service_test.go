package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// newTestAuthority composes one offline authority over an observation-disabled
// registry, a bounded pool, and a supervisor on a long grace.
func newTestAuthority(t testing.TB, epoch ports.BrokerEpoch, probe ports.BrokerHostProbe, connector poolConnector) (*Authority, *Registry, *Pool, *Supervisor, *manualClock, *testStore) {
	t.Helper()
	clock := newManualClock(time.Unix(0, 0))
	store := newTestStore()
	registry, err := NewRegistryWithConfig(epoch, store, probe, clock, nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)
	authority, pool, supervisor, _ := composeTestAuthority(t, epoch, registry, connector, clock)
	return authority, registry, pool, supervisor, clock, store
}

// composeTestAuthority wires one existing registry into a bounded pool and a
// supervisor on a long grace, and owns the single Registry.Run. Every dependency
// is drained at test end so no run goroutine leaks between tests: the registry
// run is canceled and joined (which flushes and stops its durable writer), the
// pool is closed, and the supervisor is closed. The supervisor's own manual
// clock is returned so a test can drive the idle lifecycle.
func composeTestAuthority(t testing.TB, epoch ports.BrokerEpoch, registry *Registry, connector poolConnector, clock *manualClock) (*Authority, *Pool, *Supervisor, *manualClock) {
	t.Helper()
	resolver := poolResolver(func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
		return ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}, Policy: r.Policy, Address: "fake", StartMode: r.StartMode, ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: "canonical", Bound: true}}, nil
	})
	pool, err := NewPool(epoch, resolver, registry, connector, clock, PoolLimits{Physical: 4, Clients: 8, Streams: 16, StreamsPerClient: 16, Warm: 4, Idle: time.Minute})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	supervisor, idleClock := newTestSupervisor(t, time.Hour)
	authority, err := NewAuthority(epoch, registry, pool, supervisor)
	require.NoError(t, err)
	// The caller owns Registry.Run (see NewAuthority): start the single run and
	// cancel it at cleanup, so the registry's durable writer is drained and its
	// goroutine never survives the test.
	registryCtx, stopRegistry := context.WithCancel(context.Background())
	registryDone := make(chan struct{})
	go func() { defer close(registryDone); registry.Run(registryCtx) }()
	t.Cleanup(func() { stopRegistry(); <-registryDone })
	return authority, pool, supervisor, idleClock
}

// newServiceMembershipFixture composes one admitted authority over an
// observation-disabled registry with the requested membership mode. The store is
// the caller's, so a test can observe the exact CAS count and inject failures.
func newServiceMembershipFixture(t testing.TB, store ports.BrokerHostStore, mode MembershipMode) (*Authority, *Registry, *Supervisor, *manualClock) {
	t.Helper()
	clock := newManualClock(time.Unix(0, 0))
	registry, err := NewRegistryWithConfig(1, store, nil, clock, nil, RegistryConfig{
		MembershipMode:       mode,
		ObservationDisabled:  true,
		IncarnationGenerator: (&incarnationSequencer{}).generate,
	})
	require.NoError(t, err)
	registry.jitter = identityJitter
	authority, _, supervisor, idleClock := composeTestAuthority(t, 1, registry, immediateConnector, clock)
	return authority, registry, supervisor, idleClock
}

// requireImmutableRefusal asserts one service-level membership mutation was
// refused with the typed immutable code 12 and the ports sentinel.
func requireImmutableRefusal(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var refusal ports.BrokerError
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, ports.BrokerErrorMembershipImmutable, refusal.Code)
	require.Equal(t, ports.BrokerErrorCode(12), refusal.Code, "immutable membership is the exact code 12")
	require.ErrorIs(t, err, ports.ErrBrokerMembershipImmutable)
}

// casThenGateStore commits its membership CAS and then parks the mutation until
// release, so a test can make a caller cancellation arrive strictly after the
// durable commit point but before the mutation returns.
type casThenGateStore struct {
	*testStore
	committed chan struct{}
	release   chan struct{}
}

func newCASThenGateStore() *casThenGateStore {
	return &casThenGateStore{testStore: newTestStore(), committed: make(chan struct{}, 1), release: make(chan struct{})}
}

func (s *casThenGateStore) ReplaceHosts(expected uint64, hosts []ports.BrokerHostRecord) error {
	if err := s.testStore.ReplaceHosts(expected, hosts); err != nil {
		return err
	}
	select {
	case s.committed <- struct{}{}:
	default:
	}
	<-s.release
	return nil
}

// releaseCAS lets a parked post-commit mutation return.
func (s *casThenGateStore) releaseCAS() {
	select {
	case <-s.release:
	default:
		close(s.release)
	}
}

// immediateConnector returns one healthy physical connection for any endpoint.
func immediateConnector(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
	return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
}

func poolClientCount(p *Pool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.clients)
}

func registrySubscriptionCount(r *Registry) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs)
}

func registryDemandCount(r *Registry) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.demand
}

func TestAuthorityAdmissionRollback(t *testing.T) {
	authority, _, pool, supervisor, _, _ := newTestAuthority(t, 1, nil, immediateConnector)

	first, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	var epochFromID ports.BrokerEpoch
	for i := 0; i < 8; i++ {
		epochFromID = epochFromID<<8 | ports.BrokerEpoch(first.ConnectionID()[i])
	}
	require.Equal(t, ports.BrokerEpoch(1), epochFromID, "the pool identity must carry the broker epoch")

	// Exhaust the pool's connection bound: the second admission must release the
	// supervisor client lease it already took.
	pool.limits.Clients = 2
	_, err = authority.AdmitClient(context.Background())
	require.ErrorIs(t, err, ports.BrokerAdmissionLimit)
	clients, _, _, _ := supervisorState(supervisor)
	require.Equal(t, 1, clients, "a refused admission must release its client lease")
	require.Equal(t, 2, poolClientCount(pool), "a refused admission must not leave a pool client")

	// A cancelled admission context takes no lease and registers no client.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = authority.AdmitClient(ctx)
	require.ErrorIs(t, err, context.Canceled)
	clients, _, _, _ = supervisorState(supervisor)
	require.Equal(t, 1, clients)

	require.NoError(t, first.Close())
	clients, _, _, _ = supervisorState(supervisor)
	require.Zero(t, clients)
	require.Zero(t, poolClientCount(pool))

	// A committed supervisor refuses every later admission without taking work.
	require.NoError(t, supervisor.Close())
	_, err = authority.AdmitClient(context.Background())
	requireBrokerClosed(t, err)
	clients, _, _, _ = supervisorState(supervisor)
	require.Zero(t, clients)
}

func TestAuthorityAdmissionContextNotRetained(t *testing.T) {
	authority, registry, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	ctx, cancel := context.WithCancel(context.Background())
	service, err := authority.AdmitClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	// The admission context is discarded: the connection keeps serving after the
	// caller's setup deadline expires.
	cancel()
	require.NoError(t, registry.setHosts(hostRecords(registration(t, "user@host:22", 1))))
	require.Len(t, service.Snapshot().Daemons, 1)

	stream, err := service.OpenStream(context.Background(), poolRequest(service.ConnectionID(), 1))
	require.NoError(t, err)
	require.NoError(t, stream.Close())
}

func TestAuthorityValidation(t *testing.T) {
	_, _, pool, supervisor, clock, _ := newTestAuthority(t, 1, nil, immediateConnector)

	// Live membership mutation is composed in this slice, so the authority no
	// longer refuses a registry that owns remote membership its connections can
	// now manage: a mutable remote registry is served.
	mutableRemote, err := NewRegistryWithConfig(1, newTestStore(), newTestProbe(1), clock, nil, RegistryConfig{MembershipMode: MembershipMutable})
	require.NoError(t, err)
	t.Cleanup(mutableRemote.settle)
	require.NoError(t, mutableRemote.setHosts(hostRecords(registration(t, "user@host:22", 1))))
	_, err = NewAuthority(1, mutableRemote, pool, supervisor)
	require.NoError(t, err, "a mutable remote registry must be served")

	// A local-only observer is served: its entry is configured authority rather
	// than membership, so it has nothing for the authority's connections to
	// manage either.
	localOnly, err := NewRegistryWithConfig(1, newTestStore(), nil, clock, nil, RegistryConfig{Local: &LocalObservation{
		DisplayOrigin: "local",
		Policy:        poolPolicy(),
		Probe:         newScriptedLocalProbe(1),
	}})
	require.NoError(t, err)
	t.Cleanup(localOnly.settle)
	_, err = NewAuthority(1, localOnly, pool, supervisor)
	require.NoError(t, err, "a local-only observer must be served")

	// Construction still rejects remote observation without a probe: a registry
	// that would observe remotes but owns no probe cannot be constructed, so no
	// authority ever receives one.
	_, err = NewRegistry(1, newTestStore(), nil, clock, nil)
	require.ErrorContains(t, err, "invalid registry dependencies", "remote observation without a probe must be refused")

	disabled, err := NewRegistryWithConfig(1, newTestStore(), nil, clock, nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)
	t.Cleanup(disabled.settle)
	_, err = NewAuthority(2, disabled, pool, supervisor)
	require.Error(t, err, "an epoch mismatch must be refused")

	_, err = NewAuthority(1, nil, pool, supervisor)
	require.Error(t, err, "a missing registry must be refused")
	_, err = NewAuthority(1, disabled, nil, supervisor)
	require.Error(t, err, "a missing pool must be refused")
	_, err = NewAuthority(1, disabled, pool, nil)
	require.Error(t, err, "a missing supervisor must be refused")

	_, err = NewAuthority(1, disabled, pool, supervisor)
	require.NoError(t, err)
}

func TestServiceOpenStreamScopeFencing(t *testing.T) {
	foreign := ports.BrokerConnectionID{0xff}
	tests := []struct {
		name      string
		build     func(id ports.BrokerConnectionID) ports.BrokerOpenStreamRequest
		wantStale bool
		wantEpoch bool
	}{
		{
			name: "a foreign connection is fenced as stale",
			build: func(ports.BrokerConnectionID) ports.BrokerOpenStreamRequest {
				return poolRequest(foreign, 2)
			},
			wantStale: true,
		},
		{
			name: "a foreign epoch is fenced as a stale epoch",
			build: func(id ports.BrokerConnectionID) ports.BrokerOpenStreamRequest {
				request := poolRequest(id, 3)
				request.Epoch = 2
				return request
			},
			wantEpoch: true,
		},
		{
			name: "an absent identity is filled from the connection",
			build: func(ports.BrokerConnectionID) ports.BrokerOpenStreamRequest {
				request := poolRequest(ports.BrokerConnectionID{}, 4)
				request.Epoch = 0
				return request
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authority, _, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
			service, err := authority.AdmitClient(context.Background())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, service.Close()) })

			stream, err := service.OpenStream(context.Background(), tc.build(service.ConnectionID()))
			switch {
			case tc.wantStale:
				require.ErrorIs(t, err, ports.BrokerAdmissionStale)
			case tc.wantEpoch:
				var typed ports.BrokerError
				require.ErrorAs(t, err, &typed)
				require.Equal(t, ports.BrokerErrorStaleEpoch, typed.Code)
			default:
				require.NoError(t, err)
				require.NoError(t, stream.Close())
			}
		})
	}
}

// TestServiceOpenStreamRequestsLocalReprobeOnOpenAndClose pins the "Extra"
// fix from Phase C2: OpenStream marks a local re-probe pending as soon as a
// local control/attachment stream opens, not only once it ends, so a session
// created or claimed over this very stream can appear in `ls --all`/the
// picker while the client stays attached, instead of waiting for detach. Both
// transitions are proven against the registry's actual scheduling with no
// clock advance at all, exactly like
// TestRegistryLocalPendingReprobeHonorsExplicitReconcileOnly proves the
// registry-side half of the mechanism.
func TestServiceOpenStreamRequestsLocalReprobeOnOpenAndClose(t *testing.T) {
	reachable := func() localProbeAnswer {
		return localProbeAnswer{snapshot: ports.BrokerDaemonObservation{
			Identity: "local-daemon", Incarnation: ports.BrokerDaemonIncarnation{9},
			ProtocolVersion: protocol.Version, Availability: domain.RemoteAvailabilityReachable,
		}}
	}

	clock := newManualClock(time.Unix(100, 0))
	store := newTestStore()
	local := newScriptedLocalProbe(3)
	registry, err := NewRegistryWithConfig(1, store, nil, clock, nil, RegistryConfig{
		Local: &LocalObservation{DisplayOrigin: "local", Policy: poolPolicy(), Probe: local},
	})
	require.NoError(t, err)
	registry.jitter = identityJitter
	authority, _, _, _ := composeTestAuthority(t, 1, registry, immediateConnector, clock)

	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	// Explicitly request the initial observation; admission alone does not
	// subscribe to snapshots and must not cause background polling.
	registry.RequestProbe("")
	call := receiveLocalCall(t, local)
	call.result <- reachable()
	waitLocal(t, registry, func(o ports.BrokerDaemonObservation) bool { return !o.Checking })

	// Opening a local control stream must itself mark a re-probe pending.
	stream, err := service.OpenStream(context.Background(), poolRequest(service.ConnectionID(), 1))
	require.NoError(t, err)
	opened := receiveLocalCall(t, local)
	opened.result <- reachable()
	waitLocal(t, registry, func(o ports.BrokerDaemonObservation) bool { return !o.Checking })

	// Closing the stream marks a second re-probe pending, exactly as it did
	// for stream completion before this fix.
	require.NoError(t, stream.Close())
	closed := receiveLocalCall(t, local)
	closed.result <- reachable()
}

func TestServiceNextStreamIDAllocation(t *testing.T) {
	authority, _, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	t.Run("monotone, nonzero, and never reused", func(t *testing.T) {
		seen := make(map[ports.BrokerStreamID]bool)
		previous := ports.BrokerStreamID(0)
		for range 64 {
			stream, err := service.NextStreamID()
			require.NoError(t, err)
			require.NotZero(t, stream, "an allocated identity is never zero")
			require.Greater(t, stream, previous, "allocation is strictly monotone")
			require.False(t, seen[stream], "an identity is never reused")
			seen[stream] = true
			previous = stream
		}
	})

	t.Run("concurrent allocation is monotone and collision-free", func(t *testing.T) {
		const count = 256
		ids := make([]ports.BrokerStreamID, count)
		errs := make([]error, count)
		var wg sync.WaitGroup
		for i := range count {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ids[i], errs[i] = service.NextStreamID()
			}()
		}
		wg.Wait()
		seen := make(map[ports.BrokerStreamID]bool, count)
		for i := range count {
			require.NoError(t, errs[i])
			require.NotZero(t, ids[i])
			require.False(t, seen[ids[i]], "two concurrent allocations never collide")
			seen[ids[i]] = true
		}
	})

	t.Run("exhaustion refuses rather than wrapping", func(t *testing.T) {
		exhausted, err := authority.AdmitClient(context.Background())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, exhausted.Close()) })
		exhausted.(*Service).nextStream = ^ports.BrokerStreamID(0)
		_, err = exhausted.NextStreamID()
		require.ErrorIs(t, err, ports.BrokerAdmissionLimit, "an exhausted counter never wraps to zero")
	})

	t.Run("a closed connection refuses", func(t *testing.T) {
		closed, err := authority.AdmitClient(context.Background())
		require.NoError(t, err)
		require.NoError(t, closed.Close())
		_, err = closed.NextStreamID()
		require.ErrorIs(t, err, ports.BrokerAdmissionClosed)
	})

	t.Run("an allocated identity is carried on the request", func(t *testing.T) {
		stream, err := service.NextStreamID()
		require.NoError(t, err)
		request := poolRequest(service.ConnectionID(), 0)
		request.Stream = stream
		opened, err := service.OpenStream(context.Background(), request)
		require.NoError(t, err)
		require.NoError(t, opened.Close())
		// Replaying the exact identity is refused as stale.
		_, err = service.OpenStream(context.Background(), request)
		require.ErrorIs(t, err, ports.BrokerAdmissionStale)
	})
}

func TestServiceOpenStreamAdmissionValidatesCallerInput(t *testing.T) {
	authority, _, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	for _, tc := range []struct {
		name   string
		mutate func(*ports.BrokerOpenStreamRequest)
	}{
		{"missing stream identity", func(r *ports.BrokerOpenStreamRequest) { r.Stream = 0 }},
		{"invalid purpose", func(r *ports.BrokerOpenStreamRequest) { r.Purpose = 0 }},
		{"invalid policy", func(r *ports.BrokerOpenStreamRequest) { r.Policy.Transport = "" }},
		{"observation cannot start daemon", func(r *ports.BrokerOpenStreamRequest) { r.Purpose = ports.BrokerStreamObservation }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := poolRequest(service.ConnectionID(), 1)
			tc.mutate(&request)
			_, err := service.OpenStream(context.Background(), request)
			require.ErrorIs(t, err, ports.BrokerAdmissionInvalid)
		})
	}
}

func TestServiceCloseStreamScopeFencing(t *testing.T) {
	authority, _, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	id := service.ConnectionID()

	stream, err := service.OpenStream(context.Background(), poolRequest(id, 1))
	require.NoError(t, err)

	// A wrong-scope close must not retire this connection's stream.
	require.ErrorIs(t, service.CloseStream(ports.BrokerConnectionID{0xff}, 1), ports.BrokerAdmissionStale)
	select {
	case <-stream.Done():
		t.Fatal("a wrong-scope close must not retire the stream")
	default:
	}
	require.NoError(t, service.CloseStream(id, 1))
	await(t, stream.Done())
}

func TestServiceSubscriptionCleanup(t *testing.T) {
	authority, registry, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	sub, err := service.Subscribe()
	require.NoError(t, err)
	require.Equal(t, 1, registrySubscriptionCount(registry))
	sub.Close()
	require.Zero(t, registrySubscriptionCount(registry), "an explicit close must release the subscription")

	sub, err = service.Subscribe()
	require.NoError(t, err)
	require.Equal(t, 1, registrySubscriptionCount(registry))

	require.NoError(t, service.Close())
	require.Zero(t, registrySubscriptionCount(registry), "Close must release tracked subscriptions")
	_, err = service.Subscribe()
	requireBrokerClosed(t, err)
}

// TestServiceSubscribeDrivesRegistryDemand pins the C2 wiring: opening a
// service-level subscription (an attached client keeps one open the whole
// time it watches the snapshot) increments the registry's demand count, and
// closing it decrements it back. This exercises the actual service.go call
// sites (Subscribe / serviceSubscription.Close), not Registry.SetDemand
// directly, and proves the count is balanced across concurrent subscriptions
// and idempotent Close.
func TestServiceSubscribeDrivesRegistryDemand(t *testing.T) {
	authority, registry, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	require.Zero(t, registryDemandCount(registry))

	sub, err := service.Subscribe()
	require.NoError(t, err)
	require.Equal(t, 1, registryDemandCount(registry))

	other, err := service.Subscribe()
	require.NoError(t, err)
	require.Equal(t, 2, registryDemandCount(registry), "a second live subscription must add a second unit of demand")

	sub.Close()
	require.Equal(t, 1, registryDemandCount(registry), "one closed subscription must leave the other's demand live")

	other.Close()
	require.Zero(t, registryDemandCount(registry))

	// Close is idempotent: a repeated close must never double-release demand.
	other.Close()
	require.Zero(t, registryDemandCount(registry))
}

// TestServiceSubscribeDemandByReader pins the Amendment 2 rule "reading state
// forces nothing": a passive subscription (vev ls, vev host list) receives
// publications but never records demand, while a watching client does.
func TestServiceSubscribeDemandByReader(t *testing.T) {
	tests := []struct {
		name       string
		passive    bool
		wantDemand int
	}{
		{name: "watching client records demand", passive: false, wantDemand: 1},
		{name: "passive reader records none", passive: true, wantDemand: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authority, registry, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
			admitted, err := authority.AdmitClient(context.Background())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, admitted.Close()) })
			service := admitted.(*Service)

			subscribe := service.Subscribe
			if tt.passive {
				subscribe = service.SubscribePassive
			}
			sub, err := subscribe()
			require.NoError(t, err)
			require.Equal(t, tt.wantDemand, registryDemandCount(registry))

			require.NoError(t, registry.setHosts(hostRecords(registration(t, "user@host:22", 1))))
			select {
			case <-sub.Changed():
			case <-time.After(time.Second):
				t.Fatal("every subscription must observe registry publications")
			}

			sub.Close()
			sub.Close()
			require.Zero(t, registryDemandCount(registry), "closing must balance exactly the demand it recorded")
		})
	}
}

func TestServiceSnapshotDelegates(t *testing.T) {
	authority, registry, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	sub, err := service.Subscribe()
	require.NoError(t, err)
	defer sub.Close()

	require.NoError(t, registry.setHosts(hostRecords(registration(t, "user@host:22", 1))))
	select {
	case <-sub.Changed():
	case <-time.After(time.Second):
		t.Fatal("the connection subscription must observe a registry publication")
	}
	require.Equal(t, registry.Snapshot(), service.Snapshot())
	require.Len(t, service.Snapshot().Daemons, 1)
}

func TestServiceOperationLeaseLifetime(t *testing.T) {
	entered := make(chan struct{})
	proceed := make(chan struct{})
	authority, _, _, supervisor, _, _ := newTestAuthority(t, 1, nil, func(ctx context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-proceed:
			return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
		}
	})
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	type opened struct {
		stream ports.BrokerLogicalConnection
		err    error
	}
	result := make(chan opened, 1)
	go func() {
		stream, err := service.OpenStream(context.Background(), poolRequest(service.ConnectionID(), 1))
		result <- opened{stream: stream, err: err}
	}()
	<-entered
	_, operations, _, _ := supervisorState(supervisor)
	require.Equal(t, 1, operations, "open setup must hold one operation lease")

	close(proceed)
	got := <-result
	require.NoError(t, got.err)
	require.NotNil(t, got.stream)
	_, operations, _, _ = supervisorState(supervisor)
	require.Zero(t, operations, "the operation lease must be released once setup completes")
	select {
	case <-got.stream.Done():
		t.Fatal("a stream that opened successfully must outlive its setup lease")
	default:
	}
	require.NoError(t, got.stream.Close())
	await(t, got.stream.Done())
}

func TestServiceCloseOpenRace(t *testing.T) {
	for round := 0; round < 25; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			entered := make(chan struct{})
			proceed := make(chan struct{})
			authority, _, pool, supervisor, _, _ := newTestAuthority(t, 1, nil, func(ctx context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
				close(entered)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-proceed:
					return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
				}
			})
			service, err := authority.AdmitClient(context.Background())
			require.NoError(t, err)

			result := make(chan error, 1)
			go func() {
				_, err := service.OpenStream(context.Background(), poolRequest(service.ConnectionID(), 1))
				result <- err
			}()
			<-entered

			// Close races the parked setup: whichever wins, both must settle
			// without deadlock and leave no lease or client behind.
			closed := make(chan error, 1)
			go func() { closed <- service.Close() }()
			close(proceed)
			require.NoError(t, <-closed)
			<-result

			clients, operations, _, _ := supervisorState(supervisor)
			require.Zero(t, clients)
			require.Zero(t, operations)
			require.Zero(t, poolClientCount(pool))
		})
	}
}

func TestServiceCloseIsIdempotentAndConcurrent(t *testing.T) {
	authority, registry, pool, supervisor, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	_, err = service.Subscribe()
	require.NoError(t, err)
	stream, err := service.OpenStream(context.Background(), poolRequest(service.ConnectionID(), 1))
	require.NoError(t, err)

	const closers = 8
	errs := make(chan error, closers)
	var wg sync.WaitGroup
	for i := 0; i < closers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- service.Close()
		}()
	}
	wg.Wait()
	for i := 0; i < closers; i++ {
		require.NoError(t, <-errs)
	}
	require.NoError(t, service.Close())

	require.Zero(t, registrySubscriptionCount(registry))
	require.Zero(t, poolClientCount(pool))
	clients, _, _, _ := supervisorState(supervisor)
	require.Zero(t, clients)
	await(t, stream.Done())

	_, err = service.OpenStream(context.Background(), poolRequest(service.ConnectionID(), 2))
	requireBrokerClosed(t, err)
}

func TestServiceDoneErrContract(t *testing.T) {
	t.Run("local close is orderly", func(t *testing.T) {
		authority, _, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
		service, err := authority.AdmitClient(context.Background())
		require.NoError(t, err)

		select {
		case <-service.Done():
			t.Fatal("Done closed before any terminal event")
		default:
		}
		require.NoError(t, service.Close())
		await(t, service.Done())
		require.NoError(t, service.Err(), "an orderly local Close reports no terminal cause")
		require.NoError(t, service.Err(), "Err is stable after Done")
	})

	t.Run("broker shutdown is a typed loss", func(t *testing.T) {
		authority, _, _, supervisor, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
		service, err := authority.AdmitClient(context.Background())
		require.NoError(t, err)

		require.NoError(t, supervisor.Close())
		await(t, service.Done())
		var typed ports.BrokerError
		require.ErrorAs(t, service.Err(), &typed)
		require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
	})
}

func TestServiceShutdownTerminatesOwnedWork(t *testing.T) {
	authority, _, _, supervisor, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)

	stream, err := service.OpenStream(context.Background(), poolRequest(service.ConnectionID(), 1))
	require.NoError(t, err)

	// Broker shutdown cancels the connection root: owned streams end even before
	// the session gets to call Close.
	require.NoError(t, supervisor.Close())
	await(t, stream.Done())

	require.NoError(t, service.Close())
	clients, _, _, _ := supervisorState(supervisor)
	require.Zero(t, clients)
}

// TestServiceImmutableMembershipRefusesEveryMutation proves the immutable
// connection refuses all three membership mutations with the typed immutable
// code 12 and the ports sentinel before any durable authority moves: no CAS is
// attempted and no publication changes.
func TestServiceImmutableMembershipRefusesEveryMutation(t *testing.T) {
	store := newMembershipStore()
	authority, registry, _, _ := newServiceMembershipFixture(t, store, MembershipImmutable)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	before := registry.Snapshot()
	_, err = service.AddHost(context.Background(), "new@host:22", poolPolicy())
	requireImmutableRefusal(t, err)

	removed, err := service.RemoveHost(context.Background(), registration(t, "new@host:22", 1))
	require.False(t, removed)
	requireImmutableRefusal(t, err)

	updated, err := service.UpdateHostPolicy(context.Background(), registration(t, "new@host:22", 1), policyWithTrust("changed-trust"))
	require.Equal(t, domain.RemoteRegistration{}, updated)
	requireImmutableRefusal(t, err)

	require.Equal(t, 0, store.casCalls(), "an immutable refusal never reaches the store CAS")
	require.Equal(t, before, registry.Snapshot(), "a refused mutation must not touch authority")

	service.RequestReconcile("new@host:22")
	require.Equal(t, before, registry.Snapshot(), "an unknown reconcile hint cannot change authority")
}

func TestServiceRequestReconcileDelegatesToRegistry(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	store := newTestStore()
	registration := registration(t, "user@host:22", 1)
	store.hosts = ports.BrokerHosts{Revision: 1, Hosts: []ports.BrokerHostRecord{{Registration: registration, Pinned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), registration.Endpoint)}}}
	probe := newTestProbe(2)
	registry, err := NewRegistry(1, store, probe, clock, nil)
	require.NoError(t, err)
	registry.jitter = identityJitter
	authority, _, _, _ := composeTestAuthority(t, 1, registry, immediateConnector, clock)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	registry.RequestProbe(registration.Endpoint)
	var initial probeCall
	require.Eventually(t, func() bool {
		select {
		case initial = <-probe.calls:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	initial.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Identity: "daemon", Incarnation: ports.BrokerDaemonIncarnation{1}, ProtocolVersion: 1, Availability: domain.RemoteAvailabilityReachable}}
	service.RequestReconcile(registration.Endpoint)
	var reconciled probeCall
	require.Eventually(t, func() bool {
		select {
		case reconciled = <-probe.calls:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Equal(t, registration, reconciled.registration)
}

// TestServiceMutableMembershipDelegatesOnce proves an admitted mutable
// connection delegates each mutation exactly once to durable authority: each
// call crosses the store CAS exactly once and returns the registry's own
// result.
func TestServiceMutableMembershipDelegatesOnce(t *testing.T) {
	store := newMembershipStore()
	authority, registry, _, _ := newServiceMembershipFixture(t, store, MembershipMutable)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	added, err := service.AddHost(context.Background(), "user@host:22", poolPolicy())
	require.NoError(t, err)
	require.False(t, added.IsZero())
	require.Equal(t, 1, store.casCalls(), "one addition crosses the store CAS exactly once")

	updated, err := service.UpdateHostPolicy(context.Background(), added, policyWithTrust("changed-trust"))
	require.NoError(t, err)
	require.Equal(t, added.Generation+1, updated.Generation)
	require.Equal(t, added.Incarnation, updated.Incarnation)
	require.Equal(t, 2, store.casCalls(), "one policy update crosses the store CAS exactly once")

	removed, err := service.RemoveHost(context.Background(), updated)
	require.NoError(t, err)
	require.True(t, removed)
	require.Equal(t, 3, store.casCalls(), "one removal crosses the store CAS exactly once")
	require.Empty(t, registry.Snapshot().Daemons)
}

// TestServiceMutationLeasePinsIdleLifecycle proves an in-flight membership
// mutation holds one operation lease: the idle timer stays disarmed while the
// mutation is parked past the grace, and the broker expires only after it
// returns and the lease is released.
func TestServiceMutationLeasePinsIdleLifecycle(t *testing.T) {
	store := newCASThenGateStore()
	authority, _, supervisor, idleClock := newServiceMembershipFixture(t, store, MembershipMutable)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	type outcome struct {
		registration domain.RemoteRegistration
		err          error
	}
	result := make(chan outcome, 1)
	go func() {
		registration, err := service.AddHost(context.Background(), "user@host:22", poolPolicy())
		result <- outcome{registration: registration, err: err}
	}()

	// The CAS has committed and the mutation is parked, so its operation lease is
	// held: the idle timer is disarmed even well past the grace.
	select {
	case <-store.committed:
	case <-time.After(5 * time.Second):
		t.Fatal("the mutation never reached the store CAS")
	}
	_, operations, armed, _ := supervisorState(supervisor)
	require.Equal(t, 1, operations, "the in-flight mutation holds one operation lease")
	require.False(t, armed, "a held operation lease disarms the idle timer")
	idleClock.Advance(time.Hour * 24)
	requireSupervisorOpen(t, supervisor)

	store.releaseCAS()
	got := <-result
	require.NoError(t, got.err)
	require.False(t, got.registration.IsZero())

	// The lease is released: the idle timer re-arms and the broker expires a
	// plain grace later. The connection still holds its client lease, so drop it
	// first.
	require.NoError(t, service.Close())
	_, operations, armed, _ = supervisorState(supervisor)
	require.Zero(t, operations)
	require.True(t, armed)
	idleClock.Advance(time.Hour)
	await(t, supervisor.Done())
}

// TestServiceCloseCancelsInFlightRegistryMutation proves Close cancels and
// drains an in-flight registry mutation: the connection context reaches the
// registry's mutation context, the caller observes the typed cancellation, and
// Close returns only after the mutation has joined.
func TestServiceCloseCancelsInFlightRegistryMutation(t *testing.T) {
	store := newMembershipStore()
	authority, registry, supervisor, _ := newServiceMembershipFixture(t, store, MembershipMutable)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)

	// Hold the registry lock so the mutation parks before it consults its
	// context, and Close can cancel the connection context while it waits.
	registry.mu.Lock()
	released := false
	t.Cleanup(func() {
		if !released {
			registry.mu.Unlock()
		}
	})

	type outcome struct {
		removed bool
		err     error
	}
	result := make(chan outcome, 1)
	go func() {
		removed, err := service.RemoveHost(context.Background(), registration(t, "user@host:22", 1))
		result <- outcome{removed: removed, err: err}
	}()

	// Wait until the mutation holds its operation lease, which is strictly after
	// it registered with the connection's drain group.
	require.Eventually(t, func() bool {
		_, operations, _, _ := supervisorState(supervisor)
		return operations == 1
	}, 5*time.Second, time.Millisecond, "the mutation never reached its operation lease")

	// Close cancels the connection context and waits for the in-flight mutation
	// to drain. It cannot return while the mutation is parked.
	closed := make(chan error, 1)
	go func() { closed <- service.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned before the in-flight mutation drained")
	case <-time.After(50 * time.Millisecond):
	}

	// Release the registry lock: the mutation now observes the cancelled
	// connection context instead of committing.
	registry.mu.Unlock()
	released = true

	got := <-result
	require.ErrorIs(t, got.err, context.Canceled, "the in-flight mutation observes the connection cancellation")
	require.False(t, got.removed)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close never returned after the mutation drained")
	}
	require.Zero(t, store.casCalls(), "a cancelled mutation never reaches the store CAS")
	require.Empty(t, registry.Snapshot().Daemons, "a cancelled mutation never mutates authority")
}

// TestServiceMutationCancellationAfterCommitStillReportsSuccess proves a caller
// cancellation that arrives strictly after the durable CAS still reports the
// committed success: the registry's mutation context is not consulted after
// commit, so the value crosses the CAS and returns rather than surfacing a
// cancellation for a mutation that did commit.
func TestServiceMutationCancellationAfterCommitStillReportsSuccess(t *testing.T) {
	store := newCASThenGateStore()
	authority, _, _, _ := newServiceMembershipFixture(t, store, MembershipMutable)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		registration domain.RemoteRegistration
		err          error
	}
	result := make(chan outcome, 1)
	go func() {
		registration, err := service.AddHost(ctx, "user@host:22", poolPolicy())
		result <- outcome{registration: registration, err: err}
	}()

	select {
	case <-store.committed:
	case <-time.After(5 * time.Second):
		t.Fatal("the mutation never reached the store CAS")
	}
	// The commit already happened; a cancellation now cannot un-commit it.
	cancel()
	store.releaseCAS()

	got := <-result
	require.NoError(t, got.err, "a cancellation after the CAS must not turn a committed mutation into a failure")
	require.False(t, got.registration.IsZero())
}

// TestServiceMembershipErrorMapping proves the service classifies a durable
// store outcome-unknown as the typed unknown code with its cause preserved, and
// surfaces the exact conflict code for a conflicting authority change.
func TestServiceMembershipErrorTextIsWireSafe(t *testing.T) {
	secretPath := "/home/alice/.config/vev/private-hosts.json"
	cause := errors.New(secretPath + "\x00\n" + strings.Repeat("é", ports.BrokerMaxErrorBytes))
	err := membershipError(ports.BrokerStoreOutcomeUnknownError{Err: cause})
	var failure ports.BrokerError
	require.ErrorAs(t, err, &failure)
	require.NoError(t, failure.Validate())
	require.LessOrEqual(t, len(failure.Text), ports.BrokerMaxErrorBytes)
	require.NotContains(t, failure.Text, "\x00")
	require.NotContains(t, failure.Text, "\n")
	require.NotContains(t, failure.Text, secretPath)
	require.Equal(t, "mutation outcome is unknown", failure.Text)
	require.ErrorIs(t, failure, cause)
	require.True(t, utf8.ValidString(failure.Text))
}

func TestServiceMembershipErrorMapping(t *testing.T) {
	t.Run("outcome unknown maps to outcome-unknown with its cause", func(t *testing.T) {
		cause := errors.New("power loss")
		store := newMembershipStore()
		store.failCAS(ports.BrokerStoreOutcomeUnknownError{Err: cause})
		authority, _, _, _ := newServiceMembershipFixture(t, store, MembershipMutable)
		service, err := authority.AdmitClient(context.Background())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, service.Close()) })

		_, err = service.AddHost(context.Background(), "user@host:22", poolPolicy())
		require.Error(t, err)
		var failure ports.BrokerError
		require.ErrorAs(t, err, &failure)
		require.Equal(t, ports.BrokerErrorOutcomeUnknown, failure.Code)
		require.ErrorIs(t, err, cause, "the underlying store cause stays inspectable")
		var unknown ports.BrokerStoreOutcomeUnknownError
		require.ErrorAs(t, err, &unknown)
	})

	t.Run("an exact conflict maps to code 11", func(t *testing.T) {
		store := newMembershipStore()
		store.failCAS(ports.ErrBrokerHostConflict)
		authority, _, _, _ := newServiceMembershipFixture(t, store, MembershipMutable)
		service, err := authority.AdmitClient(context.Background())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, service.Close()) })

		_, err = service.UpdateHostPolicy(context.Background(), registration(t, "user@host:22", 1), poolPolicy())
		require.Error(t, err)
		var failure ports.BrokerError
		require.ErrorAs(t, err, &failure)
		require.Equal(t, ports.BrokerErrorHostConflict, failure.Code)
		require.Equal(t, ports.BrokerErrorCode(11), failure.Code, "host conflict is the exact code 11")
		require.ErrorIs(t, err, ports.ErrBrokerHostConflict)
	})
}

func TestRegistryObservationDisabledRestoresAndPublishes(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	store := newTestStore()
	endpoint := "user@host:22"
	store.loaded = ports.BrokerSnapshot{
		Epoch:    9,
		Revision: 4,
		Daemons: []ports.BrokerDaemonObservation{{
			Endpoint:     endpoint,
			Registration: registration(t, endpoint, 1),
			Availability: domain.RemoteAvailabilityReachable,
		}},
	}
	r, err := NewRegistryWithConfig(1, store, nil, clock, nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)

	// The disabled registry restores durable state under its fresh epoch and
	// publishes it without observing anything.
	snapshot := r.Snapshot()
	require.Equal(t, ports.BrokerEpoch(1), snapshot.Epoch)
	require.EqualValues(t, 1, snapshot.Revision)
	host, ok := snapshot.Find(endpoint)
	require.True(t, ok)
	require.Equal(t, domain.RemoteAvailabilityReachable, host.Availability)
	require.Zero(t, clock.activeTimers())
}

func TestRegistryObservationDisabledIssuesNoProbesOrTimers(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	store := newTestStore()
	probe := newTestProbe(4)
	r, err := NewRegistryWithConfig(1, store, probe, clock, nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)
	require.NoError(t, r.setHosts(hostRecords(registration(t, "user@host:22", 1))))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()

	// A reconcile/probe demand must schedule nothing: no probe starts and no
	// timer is armed, so advancing the clock cannot produce observation work.
	r.RequestProbe("user@host:22")
	clock.Advance(time.Hour)
	require.Zero(t, clock.activeTimers())
	require.Empty(t, probe.calls)

	cancel()
	await(t, done)
	require.Zero(t, clock.activeTimers())
	require.Empty(t, probe.calls)
	require.Equal(t, runStopped, r.running.Load())
}

func TestRegistryObservationDisabledDrainsWriter(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	store := newBlockingStore()
	r, err := NewRegistryWithConfig(1, store, nil, clock, nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); r.Run(ctx) }()

	require.NoError(t, r.ReplaceHosts([]ports.BrokerHostRecord{{
		Registration: registration(t, "user@host:22", 1),
		Pinned:       true,
		Policy:       poolPolicy(),
		Route:        canonicalRoute(poolPolicy(), "user@host:22"),
	}}))
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("the durable writer never attempted a write")
	}

	// Run owns the writer: it must not settle while a durable write is in
	// flight, and it must still stop once that write drains.
	cancel()
	select {
	case <-runDone:
		t.Fatal("Run must not return before the in-flight durable write drains")
	case <-time.After(probeGrace):
	}
	store.releaseFirst()
	await(t, runDone)
	require.NotEmpty(t, store.writtenRevisions())
	require.Zero(t, clock.activeTimers())
}

func TestServiceConfigValidation(t *testing.T) {
	valid := serviceConfig{epoch: 1, id: ports.BrokerConnectionID{1}, previewID: ports.BrokerConnectionID{2}, registry: &Registry{}, pool: &Pool{}, supervisor: &Supervisor{}, lease: &Lease{}, clock: newManualClock(time.Unix(0, 0))}
	tests := []struct {
		name   string
		mutate func(*serviceConfig)
	}{
		{"zero epoch", func(c *serviceConfig) { c.epoch = 0 }},
		{"zero id", func(c *serviceConfig) { c.id = ports.BrokerConnectionID{} }},
		{"zero preview", func(c *serviceConfig) { c.previewID = ports.BrokerConnectionID{} }},
		{"same ids", func(c *serviceConfig) { c.previewID = c.id }},
		{"nil registry", func(c *serviceConfig) { c.registry = nil }},
		{"nil pool", func(c *serviceConfig) { c.pool = nil }},
		{"nil supervisor", func(c *serviceConfig) { c.supervisor = nil }},
		{"nil lease", func(c *serviceConfig) { c.lease = nil }},
		{"nil clock", func(c *serviceConfig) { c.clock = nil }},
	}
	require.NoError(t, valid.validate())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := valid
			tt.mutate(&config)
			require.Error(t, config.validate())
		})
	}
}
