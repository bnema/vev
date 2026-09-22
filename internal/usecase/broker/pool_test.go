package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Stateful fakes make terminal races and blocked I/O explicit; no real carriage.
type poolResolver func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error)

func (f poolResolver) ResolveDialTarget(c context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
	return f(c, r)
}

type poolBinder func(context.Context, ports.BrokerIdentityBindingRequest) (ports.BrokerDaemonIdentity, error)

func (f poolBinder) BindAuthenticatedIdentity(c context.Context, r ports.BrokerIdentityBindingRequest) (ports.BrokerDaemonIdentity, error) {
	return f(c, r)
}

type poolConnector func(context.Context, ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error)

func (f poolConnector) Connect(c context.Context, r ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
	return f(c, r)
}

type fakeLogical struct {
	ports.ClientConnection
	done chan struct{}
	once sync.Once
}

func newFakeLogical() *fakeLogical                             { return &fakeLogical{done: make(chan struct{})} }
func (f *fakeLogical) Done() <-chan struct{}                   { return f.done }
func (f *fakeLogical) Err() error                              { return nil }
func (f *fakeLogical) Close() error                            { f.once.Do(func() { close(f.done) }); return nil }
func (f *fakeLogical) SendClient(protocol.ClientMessage) error { <-f.done; return errors.New("closed") }

type fakePhysical struct {
	endpoint ports.BrokerDialTarget
	// identity, when set, overrides the endpoint's expected identity so a test
	// can model a first-contact acquisition whose resolved endpoint binds none.
	identity ports.BrokerDaemonIdentity
	done     chan struct{}
	once     sync.Once
	open     func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error)
}

func (f *fakePhysical) Identity() ports.BrokerDaemonIdentity {
	if f.identity != "" {
		return f.identity
	}
	return f.endpoint.ExpectedIdentity.Identity
}
func (f *fakePhysical) Policy() ports.BrokerPolicy { return f.endpoint.Policy }
func (f *fakePhysical) Incarnation() ports.BrokerDaemonIncarnation {
	return ports.BrokerDaemonIncarnation{1}
}
func (f *fakePhysical) FailureKind() domain.RemoteFailureKind { return domain.RemoteFailureTransport }
func (f *fakePhysical) Done() <-chan struct{}                 { return f.done }
func (f *fakePhysical) Err() error                            { return errors.New("physical loss") }
func (f *fakePhysical) Close() error                          { f.once.Do(func() { close(f.done) }); return nil }
func (f *fakePhysical) OpenStream(c context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	if f.open != nil {
		return f.open(c, r)
	}
	return newFakeLogical(), nil
}
func poolPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{ProtocolVersion: 1, CatalogSchemaVersion: 1, Transport: "quic", Trust: "trusted", Launch: "explicit", Isolation: "user", EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}
}
func poolRequest(id ports.BrokerConnectionID, n uint64) ports.BrokerOpenStreamRequest {
	return ports.BrokerOpenStreamRequest{Epoch: 1, Connection: id, Stream: ports.BrokerStreamID(n), Local: true, Purpose: ports.BrokerStreamControl, Policy: poolPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded}
}
func setupPool(t testing.TB, connector poolConnector) (*Pool, *manualClock) {
	t.Helper()
	clock := newManualClock(time.Unix(0, 0))
	resolver := poolResolver(func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
		return ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}, Policy: r.Policy, Address: "fake", StartMode: r.StartMode, ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: "canonical", Bound: true}}, nil
	})
	binder := poolBinder(func(_ context.Context, request ports.BrokerIdentityBindingRequest) (ports.BrokerDaemonIdentity, error) {
		return request.Identity, nil
	})
	p, err := NewPool(1, resolver, binder, connector, clock, PoolLimits{Physical: 2, Clients: 128, Streams: 128, StreamsPerClient: 128, Idle: time.Minute})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	return p, clock
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
}

// TestPoolRefusedResolutionNeverDials proves a refused resolution is terminal:
// the pool returns the typed broker error and never asks the connector to open
// a physical transport, so an unprovisioned or conflicting request can never
// reach an address. The same ordering fences a refused local request.
func TestPoolRefusedResolutionNeverDials(t *testing.T) {
	var dials atomic.Int32
	resolver := poolResolver(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
		return ports.BrokerDialTarget{}, ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errors.New("refused")}
	})
	connector := poolConnector(func(context.Context, ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		dials.Add(1)
		return nil, nil
	})
	binder := poolBinder(func(_ context.Context, request ports.BrokerIdentityBindingRequest) (ports.BrokerDaemonIdentity, error) {
		return request.Identity, nil
	})
	pool, err := NewPool(1, resolver, binder, connector, newManualClock(time.Unix(0, 0)), PoolLimits{Physical: 1, Clients: 8, Streams: 8, StreamsPerClient: 8, Idle: time.Minute})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pool.Close()) })

	id, err := pool.RegisterClient()
	require.NoError(t, err)
	_, err = pool.OpenStream(context.Background(), poolRequest(id, 1))
	require.Error(t, err)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorConflictingPolicy, typed.Code)
	require.Zero(t, dials.Load(), "a refused request must never dial")
}

// TestPoolOpenFailureWithTypedNilConnection proves a failed open that still
// carries a typed-nil logical connection is not closed, and never panics the
// serving goroutine. A concrete-typed adapter returning a nil pointer directly
// produces an interface that compares unequal to nil while holding no receiver.
func TestPoolOpenFailureWithTypedNilConnection(t *testing.T) {
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		return &fakePhysical{
			endpoint: e,
			done:     make(chan struct{}),
			open: func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				var absent *fakeLogical
				return absent, errors.New("open failed")
			},
		}, nil
	})
	id, err := p.RegisterClient()
	require.NoError(t, err)

	stream, err := p.OpenStream(context.Background(), poolRequest(id, 1))
	require.Error(t, err)
	require.True(t, stream == nil, "a failed open must not yield a stream")
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
}

func TestPoolHundredStreamsLossAndIsolation(t *testing.T) {
	var calls atomic.Int32
	var physical *fakePhysical
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		calls.Add(1)
		physical = &fakePhysical{endpoint: e, done: make(chan struct{})}
		return physical, nil
	})
	streams := make([]ports.BrokerLogicalConnection, 100)
	var wg sync.WaitGroup
	for i := range streams {
		id, err := p.RegisterClient()
		require.NoError(t, err)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, e := p.OpenStream(context.Background(), poolRequest(id, 1))
			if e != nil {
				t.Error(e)
				return
			}
			streams[i] = s
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, 1, calls.Load())
	blocked := make(chan struct{})
	go func() { _ = streams[0].SendClient(nil); close(blocked) }()
	require.NoError(t, streams[0].Close())
	await(t, blocked)
	select {
	case <-streams[1].Done():
		t.Fatal("sibling closed")
	default:
	}
	require.NoError(t, physical.Close())
	for _, s := range streams[1:] {
		await(t, s.Done())
		var loss ports.BrokerError
		require.ErrorAs(t, s.Err(), &loss)
		require.Equal(t, ports.BrokerErrorAttachmentLost, loss.Code)
	}
}

func TestPoolAdmissionAndNonreuse(t *testing.T) {
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
	})
	p.limits.StreamsPerClient = 1
	id, err := p.RegisterClient()
	require.NoError(t, err)
	s, err := p.OpenStream(context.Background(), poolRequest(id, 1))
	require.NoError(t, err)
	_, err = p.OpenStream(context.Background(), poolRequest(id, 2))
	require.ErrorIs(t, err, ports.BrokerAdmissionLimit)
	require.NoError(t, s.Close())
	_, err = p.OpenStream(context.Background(), poolRequest(id, 2))
	require.ErrorIs(t, err, ports.BrokerAdmissionStale)
	s, err = p.OpenStream(context.Background(), poolRequest(id, 3))
	require.NoError(t, err)
	p.CloseClient(id)
	await(t, s.Done())
	_, err = p.OpenStream(context.Background(), poolRequest(id, 4))
	require.ErrorIs(t, err, ports.BrokerAdmissionStale)
}

// TestPoolStreamWindowAdmission pins the bounded anti-replay window at the pool
// boundary: a concurrent lower identity that was allocated first is admitted
// after a higher one, a duplicate is refused, and an identity evicted past the
// window is stale rather than merely duplicate.
func TestPoolStreamWindowAdmission(t *testing.T) {
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
	})
	p.limits.StreamsPerClient = ports.BrokerStreamWindowSize + 8
	id, err := p.RegisterClient()
	require.NoError(t, err)

	// 2 then 1: the lower identity was allocated first, so it is admitted.
	second, err := p.OpenStream(context.Background(), poolRequest(id, 2))
	require.NoError(t, err)
	defer second.Close()
	first, err := p.OpenStream(context.Background(), poolRequest(id, 1))
	require.NoError(t, err)
	defer first.Close()

	// A duplicate inside the window is stale.
	_, err = p.OpenStream(context.Background(), poolRequest(id, 1))
	require.ErrorIs(t, err, ports.BrokerAdmissionStale)
	// Zero is never a valid identity.
	_, err = p.OpenStream(context.Background(), poolRequest(id, 0))
	require.ErrorIs(t, err, ports.BrokerAdmissionInvalid)

	// Push the newest admitted identity past the window, then replay a low one.
	high, err := p.OpenStream(context.Background(), poolRequest(id, ports.BrokerStreamWindowSize+2))
	require.NoError(t, err)
	defer high.Close()
	_, err = p.OpenStream(context.Background(), poolRequest(id, 1))
	require.ErrorIs(t, err, ports.BrokerAdmissionStale, "an identity evicted from the window is stale")
}

// TestPoolPendingAcquisitionSeparatesStartMode proves an ExistingOnly
// acquisition is never coalesced with a spawn-capable acquisition to the same
// unbound endpoint: while the first acquisition is still in flight, the second
// starts its own instead of waiting on it, and each reaches the connector with
// its own authorization.
func TestPoolPendingAcquisitionSeparatesStartMode(t *testing.T) {
	var calls atomic.Int32
	modes := make(chan ports.BrokerDaemonStartMode, 4)
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		calls.Add(1)
		modes <- e.StartMode
		entered <- struct{}{}
		<-release
		return &fakePhysical{endpoint: e, identity: "canonical", done: make(chan struct{})}, nil
	})
	// The endpoint is first-contact: it binds no identity, so acquisition
	// coalescing is decided by the pending key, which is exactly where the start
	// mode has to participate.
	p.routes = poolResolver(func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
		return ports.BrokerDialTarget{
			Fence: ports.BrokerEndpointFence{Registration: r.Registration}, Policy: r.Policy,
			Address: "fake", StartMode: r.StartMode,
			ExpectedIdentity: ports.BrokerExpectedIdentity{},
		}, nil
	})
	id, _ := p.RegisterClient()

	existing := poolRequest(id, 1)
	existing.Local = false
	existing.Endpoint = "user@host"
	existing.Registration = domain.RemoteRegistration{Endpoint: "user@host", Incarnation: [16]byte{1}, Generation: 1}
	existing.StartMode = ports.BrokerDaemonExistingOnly
	startable := existing
	startable.Stream = 2
	startable.StartMode = ports.BrokerDaemonStartIfNeeded

	firstErr := make(chan error, 1)
	go func() { _, err := p.OpenStream(context.Background(), existing); firstErr <- err }()
	await(t, entered)
	secondErr := make(chan error, 1)
	go func() { _, err := p.OpenStream(context.Background(), startable); secondErr <- err }()
	// Both acquisitions must reach the connector while the first is still in
	// flight; a coalesced implementation would leave the second waiting here.
	await(t, entered)
	close(release)
	require.NoError(t, <-firstErr)
	require.NoError(t, <-secondErr)

	require.EqualValues(t, 2, calls.Load(), "a spawn-capable request must not reuse an ExistingOnly acquisition")
	got := map[ports.BrokerDaemonStartMode]bool{}
	for range 2 {
		select {
		case mode := <-modes:
			got[mode] = true
		case <-time.After(3 * time.Second):
			t.Fatal("the connector never observed both acquisitions")
		}
	}
	require.True(t, got[ports.BrokerDaemonExistingOnly], "the ExistingOnly authorization reaches the transport")
	require.True(t, got[ports.BrokerDaemonStartIfNeeded], "the start-if-needed authorization reaches the transport")
}

func TestPoolCoalescedCancellation(t *testing.T) {
	entered, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	p, _ := setupPool(t, func(ctx context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		calls.Add(1)
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-proceed:
			return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
		}
	})
	id1, _ := p.RegisterClient()
	id2, _ := p.RegisterClient()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := p.OpenStream(ctx, poolRequest(id1, 1)); result <- err }()
	await(t, entered)
	result2 := make(chan ports.BrokerLogicalConnection, 1)
	go func() {
		s, err := p.OpenStream(context.Background(), poolRequest(id2, 1))
		if err != nil {
			t.Error(err)
		}
		result2 <- s
	}()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, e := range p.entries {
			return e.refs == 2
		}
		return false
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	close(proceed)
	s := <-result2
	require.NotNil(t, s)
	require.EqualValues(t, 1, calls.Load())
	require.NoError(t, s.Close())
}

func TestPoolAbandonedAttemptAndShutdown(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	p, _ := setupPool(t, func(ctx context.Context, _ ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	})
	id, _ := p.RegisterClient()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := p.OpenStream(ctx, poolRequest(id, 1)); result <- err }()
	await(t, entered)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	await(t, exited)
	require.NoError(t, p.Close())
	_, err := p.RegisterClient()
	require.ErrorIs(t, err, ports.BrokerAdmissionClosed)
}

func TestPoolIdleEviction(t *testing.T) {
	physicals := make(chan *fakePhysical, 2)
	p, clock := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		f := &fakePhysical{endpoint: e, done: make(chan struct{})}
		physicals <- f
		return f, nil
	})
	id, _ := p.RegisterClient()
	s, err := p.OpenStream(context.Background(), poolRequest(id, 1))
	require.NoError(t, err)
	f := <-physicals
	require.NoError(t, s.Close())
	require.Eventually(t, func() bool { clock.mu.Lock(); defer clock.mu.Unlock(); return len(clock.timers) > 0 }, time.Second, time.Millisecond)
	clock.Advance(time.Minute)
	await(t, f.done)
	require.NoError(t, p.Close())
}

func TestPoolPolicyAndIdentityVerification(t *testing.T) {
	for _, mode := range []string{"identity", "policy", "resolver"} {
		t.Run(mode, func(t *testing.T) {
			p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
				if mode == "identity" {
					e.ExpectedIdentity.Identity = "impostor"
				}
				if mode == "policy" {
					e.Policy.Trust = "other"
				}
				return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
			})
			if mode == "resolver" {
				p.routes = poolResolver(func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
					policy := r.Policy
					policy.Trust = "other"
					return ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}, Policy: policy, Address: "fake", StartMode: r.StartMode, ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: "canonical", Bound: true}}, nil
				})
			}
			id, _ := p.RegisterClient()
			_, err := p.OpenStream(context.Background(), poolRequest(id, 1))
			var typed ports.BrokerError
			require.ErrorAs(t, err, &typed)
		})
	}
}

func BenchmarkPoolOpenClose(b *testing.B) {
	p, _ := setupPool(b, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
	})
	id, _ := p.RegisterClient()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := p.OpenStream(context.Background(), poolRequest(id, uint64(i+1)))
		if err != nil {
			b.Fatal(err)
		}
		_ = s.Close()
	}
}

func TestPoolAliasesAndPolicyPartition(t *testing.T) {
	var calls atomic.Int32
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		calls.Add(1)
		return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
	})
	id, _ := p.RegisterClient()
	for i, alias := range []string{"user@first", "user@second"} {
		r := poolRequest(id, uint64(i+1))
		r.Local = false
		r.Endpoint = alias
		r.Registration = domain.RemoteRegistration{Endpoint: alias, Incarnation: [16]byte{1}, Generation: 1}
		s, err := p.OpenStream(context.Background(), r)
		require.NoError(t, err)
		defer s.Close()
	}
	require.EqualValues(t, 1, calls.Load())
	r := poolRequest(id, 3)
	r.Policy.Isolation = "other"
	s, err := p.OpenStream(context.Background(), r)
	require.NoError(t, err)
	defer s.Close()
	require.EqualValues(t, 2, calls.Load())
	r.Stream = 4
	r.Policy.Trust = "third"
	_, err = p.OpenStream(context.Background(), r)
	require.ErrorIs(t, err, ports.BrokerAdmissionLimit)
}

func TestPoolPendingOpenCancellationDoesNotHoldLock(t *testing.T) {
	entered := make(chan struct{})
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		return &fakePhysical{endpoint: e, done: make(chan struct{}), open: func(ctx context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
			if r.Stream == 1 {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return newFakeLogical(), nil
		}}, nil
	})
	id, _ := p.RegisterClient()
	result := make(chan error, 1)
	go func() { _, err := p.OpenStream(context.Background(), poolRequest(id, 1)); result <- err }()
	await(t, entered)
	s, err := p.OpenStream(context.Background(), poolRequest(id, 2))
	require.NoError(t, err)
	require.NoError(t, p.CloseStream(id, 1))
	require.ErrorIs(t, <-result, context.Canceled)
	select {
	case <-s.Done():
		t.Fatal("sibling canceled")
	default:
	}
	require.NoError(t, p.Close())
	await(t, s.Done())
}
