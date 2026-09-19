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
type poolResolver func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerResolvedEndpoint, error)

func (f poolResolver) Resolve(c context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerResolvedEndpoint, error) {
	return f(c, r)
}

type poolConnector func(context.Context, ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error)

func (f poolConnector) Connect(c context.Context, r ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
	endpoint ports.BrokerResolvedEndpoint
	done     chan struct{}
	once     sync.Once
	open     func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error)
}

func (f *fakePhysical) Identity() ports.BrokerDaemonIdentity { return f.endpoint.Identity }
func (f *fakePhysical) Policy() ports.BrokerPolicy           { return f.endpoint.Policy }
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
	return ports.BrokerPolicy{ProtocolVersion: 1, CatalogSchemaVersion: 1, Transport: "fake", Trust: "trusted", Launch: "explicit", Isolation: "user", EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}
}
func poolRequest(id ports.BrokerConnectionID, n uint64) ports.BrokerOpenStreamRequest {
	return ports.BrokerOpenStreamRequest{Epoch: 1, Connection: id, Stream: ports.BrokerStreamID(n), Local: true, Purpose: ports.BrokerStreamControl, Policy: poolPolicy()}
}
func setupPool(t testing.TB, connector poolConnector) (*Pool, *manualClock) {
	t.Helper()
	clock := newManualClock(time.Unix(0, 0))
	resolver := poolResolver(func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerResolvedEndpoint, error) {
		return ports.BrokerResolvedEndpoint{Identity: "canonical", Policy: r.Policy, Address: "fake"}, nil
	})
	p, err := NewPool(1, resolver, connector, clock, PoolLimits{Physical: 2, Clients: 128, Streams: 128, StreamsPerClient: 128, Idle: time.Minute})
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
	resolver := poolResolver(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerResolvedEndpoint, error) {
		return ports.BrokerResolvedEndpoint{}, ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errors.New("refused")}
	})
	connector := poolConnector(func(context.Context, ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
		dials.Add(1)
		return nil, nil
	})
	pool, err := NewPool(1, resolver, connector, newManualClock(time.Unix(0, 0)), PoolLimits{Physical: 1, Clients: 8, Streams: 8, StreamsPerClient: 8, Idle: time.Minute})
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
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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

func TestPoolCoalescedCancellation(t *testing.T) {
	entered, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	p, _ := setupPool(t, func(ctx context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
	p, _ := setupPool(t, func(ctx context.Context, _ ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
	p, clock := setupPool(t, func(_ context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
			p, _ := setupPool(t, func(_ context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
				if mode == "identity" {
					e.Identity = "impostor"
				}
				if mode == "policy" {
					e.Policy.Trust = "other"
				}
				return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
			})
			if mode == "resolver" {
				p.resolver = poolResolver(func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerResolvedEndpoint, error) {
					policy := r.Policy
					policy.Trust = "other"
					return ports.BrokerResolvedEndpoint{Identity: "canonical", Policy: policy, Address: "fake"}, nil
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
	p, _ := setupPool(b, func(_ context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerResolvedEndpoint) (ports.BrokerPhysicalConnection, error) {
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
