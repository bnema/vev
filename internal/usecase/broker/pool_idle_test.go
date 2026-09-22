package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func waitPoolEmpty(t *testing.T, p *Pool) {
	t.Helper()
	require.Eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.entries) == 0 }, time.Second, time.Millisecond)
}

func TestPoolIdleEvictionAndReconnect(t *testing.T) {
	var calls atomic.Int32
	p, clock := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		calls.Add(1)
		return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
	})
	id, _ := p.RegisterClient()
	s, err := p.OpenStream(context.Background(), poolRequest(id, 1))
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.Eventually(t, func() bool { clock.mu.Lock(); defer clock.mu.Unlock(); return len(clock.timers) == 1 }, time.Second, time.Millisecond)
	clock.Advance(time.Minute)
	waitPoolEmpty(t, p)
	s, err = p.OpenStream(context.Background(), poolRequest(id, 2))
	require.NoError(t, err)
	require.EqualValues(t, 2, calls.Load())
	require.NoError(t, s.Close())
}

type retiringPhysical struct {
	*fakePhysical
	closing, allowClose chan struct{}
}

func (f *retiringPhysical) Close() error {
	close(f.closing)
	<-f.allowClose
	return f.fakePhysical.Close()
}

func TestPoolRetiringKeyUntilCloseCompletes(t *testing.T) {
	var calls atomic.Int32
	var first *retiringPhysical
	allow := make(chan struct{})
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		f := &fakePhysical{endpoint: e, done: make(chan struct{})}
		if calls.Add(1) == 1 {
			first = &retiringPhysical{fakePhysical: f, closing: make(chan struct{}), allowClose: allow}
			return first, nil
		}
		return f, nil
	})
	defer func() {
		select {
		case <-allow:
		default:
			close(allow)
		}
	}()
	id, _ := p.RegisterClient()
	s, err := p.OpenStream(context.Background(), poolRequest(id, 1))
	require.NoError(t, err)
	require.NoError(t, first.fakePhysical.Close())
	await(t, first.closing)
	await(t, s.Done())
	_, err = p.OpenStream(context.Background(), poolRequest(id, 2))
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
	require.EqualValues(t, 1, calls.Load())
	close(allow)
	waitPoolEmpty(t, p)
	s, err = p.OpenStream(context.Background(), poolRequest(id, 3))
	require.NoError(t, err)
	require.EqualValues(t, 2, calls.Load())
	require.NoError(t, s.Close())
}

func TestPoolFailedConnectRetry(t *testing.T) {
	var calls atomic.Int32
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		if calls.Add(1) == 1 {
			return nil, context.DeadlineExceeded
		}
		return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
	})
	id, _ := p.RegisterClient()
	_, err := p.OpenStream(context.Background(), poolRequest(id, 1))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	waitPoolEmpty(t, p)
	_, err = p.OpenStream(context.Background(), poolRequest(id, 1))
	require.ErrorIs(t, err, ports.BrokerAdmissionStale)
	s, err := p.OpenStream(context.Background(), poolRequest(id, 2))
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.EqualValues(t, 2, calls.Load())
}
