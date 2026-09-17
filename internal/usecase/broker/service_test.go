package broker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// newTestAuthority composes one offline authority over an observation-disabled
// registry, a bounded pool, and a supervisor on a long grace. Every dependency
// is drained at test end so no run goroutine leaks between tests: the registry
// run is canceled and joined (which flushes and stops its durable writer), the
// pool is closed, and the supervisor is closed.
func newTestAuthority(t testing.TB, epoch ports.BrokerEpoch, probe ports.BrokerHostProbe, connector poolConnector) (*Authority, *Registry, *Pool, *Supervisor, *manualClock, *testStore) {
	t.Helper()
	clock := newManualClock(time.Unix(0, 0))
	store := newTestStore()
	registry, err := NewRegistryWithConfig(epoch, store, probe, clock, nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)
	resolver := poolResolver(func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerResolvedEndpoint, error) {
		return ports.BrokerResolvedEndpoint{Identity: "canonical", Policy: r.Policy, Address: "fake"}, nil
	})
	pool, err := NewPool(epoch, resolver, connector, clock, PoolLimits{Physical: 4, Clients: 8, Streams: 16, StreamsPerClient: 16, Idle: time.Minute})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	supervisor, _ := newTestSupervisor(t, time.Hour)
	authority, err := NewAuthority(epoch, registry, pool, supervisor)
	require.NoError(t, err)
	// The caller owns Registry.Run (see NewAuthority): start the single run and
	// cancel it at cleanup, so the registry's durable writer is drained and its
	// goroutine never survives the test.
	registryCtx, stopRegistry := context.WithCancel(context.Background())
	registryDone := make(chan struct{})
	go func() { defer close(registryDone); registry.Run(registryCtx) }()
	t.Cleanup(func() { stopRegistry(); <-registryDone })
	return authority, registry, pool, supervisor, clock, store
}

// immediateConnector returns one healthy physical connection for any endpoint.
func immediateConnector(_ context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
	pool.limits.Clients = 1
	_, err = authority.AdmitClient(context.Background())
	require.ErrorIs(t, err, ports.BrokerAdmissionLimit)
	clients, _, _, _ := supervisorState(supervisor)
	require.Equal(t, 1, clients, "a refused admission must release its client lease")
	require.Equal(t, 1, poolClientCount(pool), "a refused admission must not leave a pool client")

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
	require.NoError(t, registry.setHosts([]domain.RemoteRegistration{registration(t, "user@host:22", 1)}))
	require.Len(t, service.Snapshot().Hosts, 1)

	stream, err := service.OpenStream(context.Background(), poolRequest(service.ConnectionID(), 1))
	require.NoError(t, err)
	require.NoError(t, stream.Close())
}

func TestAuthorityValidation(t *testing.T) {
	_, _, pool, supervisor, clock, _ := newTestAuthority(t, 1, nil, immediateConnector)

	observing, err := NewRegistry(1, newTestStore(), newTestProbe(1), clock, nil)
	require.NoError(t, err)
	_, err = NewAuthority(1, observing, pool, supervisor)
	require.Error(t, err, "an observing registry must be refused by an offline authority")

	disabled, err := NewRegistryWithConfig(1, newTestStore(), nil, clock, nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)
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

func TestServiceSnapshotDelegates(t *testing.T) {
	authority, registry, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	sub, err := service.Subscribe()
	require.NoError(t, err)
	defer sub.Close()

	require.NoError(t, registry.setHosts([]domain.RemoteRegistration{registration(t, "user@host:22", 1)}))
	select {
	case <-sub.Changed():
	case <-time.After(time.Second):
		t.Fatal("the connection subscription must observe a registry publication")
	}
	require.Equal(t, registry.Snapshot(), service.Snapshot())
	require.Len(t, service.Snapshot().Hosts, 1)
}

func TestServiceOperationLeaseLifetime(t *testing.T) {
	entered := make(chan struct{})
	proceed := make(chan struct{})
	authority, _, _, supervisor, _, _ := newTestAuthority(t, 1, nil, func(ctx context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
			authority, _, pool, supervisor, _, _ := newTestAuthority(t, 1, nil, func(ctx context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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

func TestServiceOfflineMembershipRefusal(t *testing.T) {
	authority, registry, _, _, _, _ := newTestAuthority(t, 1, nil, immediateConnector)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	err = service.AddHost(context.Background(), "new@host:22")
	require.ErrorIs(t, err, ErrOfflineMembership)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)

	removed, err := service.RemoveHost(context.Background(), "new@host:22")
	require.False(t, removed)
	require.ErrorIs(t, err, ErrOfflineMembership)

	require.Empty(t, registry.Snapshot().Hosts, "a refused mutation must not touch authority")
	before := registry.Snapshot()
	service.RequestReconcile("new@host:22")
	require.Equal(t, before, registry.Snapshot(), "reconcile must be a no-op")
}

func TestRegistryObservationDisabledRestoresAndPublishes(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	store := newTestStore()
	endpoint := "user@host:22"
	store.loaded = ports.BrokerSnapshot{
		Epoch:    9,
		Revision: 4,
		Hosts: []ports.RemoteHostSnapshot{{
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
	require.NoError(t, r.setHosts([]domain.RemoteRegistration{registration(t, "user@host:22", 1)}))

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
