package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// warmHarness records every physical the pool dials per identity. The pool
// ports are mocks; fakePhysical stays a stateful fake because its Done channel
// models transport lifetime across pool workers.
type warmHarness struct {
	pool   *Pool
	clock  *manualClock
	client ports.BrokerConnectionID
	mu     sync.Mutex
	target ports.BrokerDaemonIdentity
	dials  map[ports.BrokerDaemonIdentity][]*fakePhysical
	stream uint64
}

func newWarmHarness(t *testing.T, warm int, idle time.Duration) *warmHarness {
	t.Helper()
	h := &warmHarness{clock: newManualClock(time.Unix(0, 0)), target: "alpha", dials: make(map[ports.BrokerDaemonIdentity][]*fakePhysical)}
	routes := portsmocks.NewMockBrokerRouteAuthority(t)
	routes.EXPECT().ResolveDialTarget(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		return ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}, Policy: r.Policy, Address: "fake", StartMode: r.StartMode, ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: h.target, Bound: true}}, nil
	}).Maybe()
	binder := portsmocks.NewMockBrokerIdentityBinder(t)
	binder.EXPECT().BindAuthenticatedIdentity(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, r ports.BrokerIdentityBindingRequest) (ports.BrokerDaemonIdentity, error) {
		return r.Identity, nil
	}).Maybe()
	connector := portsmocks.NewMockBrokerEndpointConnector(t)
	connector.EXPECT().Connect(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		f := &fakePhysical{endpoint: e, done: make(chan struct{})}
		h.mu.Lock()
		h.dials[e.ExpectedIdentity.Identity] = append(h.dials[e.ExpectedIdentity.Identity], f)
		h.mu.Unlock()
		return f, nil
	}).Maybe()
	pool, err := NewPool(1, routes, binder, connector, h.clock, PoolLimits{Physical: 4, Clients: 4, Streams: 16, StreamsPerClient: 16, Warm: warm, Idle: idle})
	require.NoError(t, err)
	h.pool = pool
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	h.client, err = pool.RegisterClient()
	require.NoError(t, err)
	return h
}

// visit opens and closes one logical stream to identity, as a client that
// attaches to a remote and then navigates away does.
func (h *warmHarness) visit(t *testing.T, identity ports.BrokerDaemonIdentity) {
	t.Helper()
	h.mu.Lock()
	h.target = identity
	h.stream++
	stream := h.stream
	h.mu.Unlock()
	s, err := h.pool.OpenStream(context.Background(), poolRequest(h.client, stream))
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func (h *warmHarness) physicals(identity ports.BrokerDaemonIdentity) []*fakePhysical {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*fakePhysical(nil), h.dials[identity]...)
}

func closed(f *fakePhysical) bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// reap advances the fake clock one grace at a time until f retires. Repeating
// the advance tolerates a worker that re-arms after a late wake; each step is
// a bounded fake-clock move, never a sleep.
func (h *warmHarness) reap(t *testing.T, f *fakePhysical, grace time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool { h.clock.Advance(grace); return closed(f) }, 3*time.Second, time.Millisecond)
}

func TestPoolWarmReuse(t *testing.T) {
	const grace = 5 * time.Minute
	tests := []struct {
		name string
		warm int
		idle time.Duration
		// run drives local→remote→local→remote style visits and returns the
		// dial count expected per identity afterwards.
		run  func(t *testing.T, h *warmHarness)
		want map[ports.BrokerDaemonIdentity]int
	}{
		{
			name: "reuse within grace",
			warm: 2, idle: grace,
			run: func(t *testing.T, h *warmHarness) {
				h.visit(t, "alpha")
				h.clock.Advance(grace / 2)
				h.visit(t, "alpha")
				require.False(t, closed(h.physicals("alpha")[0]))
			},
			want: map[ports.BrokerDaemonIdentity]int{"alpha": 1},
		},
		{
			name: "reap after grace",
			warm: 2, idle: grace,
			run: func(t *testing.T, h *warmHarness) {
				h.visit(t, "alpha")
				h.reap(t, h.physicals("alpha")[0], grace)
				require.Eventually(t, func() bool { _, ok := h.pool.SharedPhysical("alpha", poolPolicy()); return !ok }, 3*time.Second, time.Millisecond)
				require.Eventually(t, func() bool { h.pool.mu.Lock(); defer h.pool.mu.Unlock(); return len(h.pool.entries) == 0 }, 3*time.Second, time.Millisecond)
				h.visit(t, "alpha")
			},
			want: map[ports.BrokerDaemonIdentity]int{"alpha": 2},
		},
		{
			name: "no age bound keeps warm without timers",
			warm: 2, idle: 0,
			run: func(t *testing.T, h *warmHarness) {
				h.visit(t, "alpha")
				h.clock.Advance(24 * time.Hour)
				h.visit(t, "alpha")
				h.clock.mu.Lock()
				timers := len(h.clock.timers)
				h.clock.mu.Unlock()
				require.Zero(t, timers)
			},
			want: map[ports.BrokerDaemonIdentity]int{"alpha": 1},
		},
		{
			name: "bound evicts least recently idled",
			warm: 1, idle: 0,
			run: func(t *testing.T, h *warmHarness) {
				h.visit(t, "alpha")
				h.visit(t, "beta")
				await(t, h.physicals("alpha")[0].done)
				require.False(t, closed(h.physicals("beta")[0]))
				h.visit(t, "beta")
			},
			want: map[ports.BrokerDaemonIdentity]int{"alpha": 1, "beta": 1},
		},
		{
			name: "active streams do not count toward the bound",
			warm: 1, idle: 0,
			run: func(t *testing.T, h *warmHarness) {
				h.visit(t, "alpha")
				h.mu.Lock()
				h.target = "beta"
				h.stream++
				stream := h.stream
				h.mu.Unlock()
				held, err := h.pool.OpenStream(context.Background(), poolRequest(h.client, stream))
				require.NoError(t, err)
				h.visit(t, "alpha")
				require.False(t, closed(h.physicals("alpha")[0]))
				require.NoError(t, held.Close())
				await(t, h.physicals("alpha")[0].done)
			},
			want: map[ports.BrokerDaemonIdentity]int{"alpha": 1, "beta": 1},
		},
		{
			name: "zero warm retires on release",
			warm: 0, idle: 0,
			run: func(t *testing.T, h *warmHarness) {
				h.visit(t, "alpha")
				await(t, h.physicals("alpha")[0].done)
				require.Eventually(t, func() bool { h.pool.mu.Lock(); defer h.pool.mu.Unlock(); return len(h.pool.entries) == 0 }, 3*time.Second, time.Millisecond)
				h.visit(t, "alpha")
			},
			want: map[ports.BrokerDaemonIdentity]int{"alpha": 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newWarmHarness(t, tt.warm, tt.idle)
			tt.run(t, h)
			for identity, want := range tt.want {
				require.Len(t, h.physicals(identity), want, "dials for %s", identity)
			}
		})
	}
}

func TestPoolAdoptProbePhysical(t *testing.T) {
	for _, tt := range []struct {
		name string
		warm int
		idle time.Duration
		want bool
	}{
		{name: "warm probe is reused", warm: 2, want: true},
		{name: "warm probe has idle deadline", warm: 2, idle: time.Minute, want: true},
		{name: "warm disabled refuses probe", warm: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newWarmHarness(t, tt.warm, tt.idle)
			physical := &fakePhysical{endpoint: ports.BrokerDialTarget{Policy: poolPolicy(), ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: "alpha", Bound: true}}, done: make(chan struct{})}
			require.Equal(t, tt.want, h.pool.AdoptPhysical(physical, poolPolicy()))
			if !tt.want {
				require.NoError(t, physical.Close())
				return
			}
			shared, ok := h.pool.SharedPhysical("alpha", poolPolicy())
			require.True(t, ok)
			require.Same(t, physical, shared)
			h.visit(t, "alpha")
			require.Empty(t, h.physicals("alpha"), "attach must not redial after a first probe")
			require.False(t, h.pool.AdoptPhysical(&fakePhysical{endpoint: physical.endpoint, done: make(chan struct{})}, poolPolicy()), "duplicate probe cannot replace the attachment")
			if tt.idle > 0 {
				h.reap(t, physical, tt.idle)
			} else {
				require.NoError(t, h.pool.Close())
			}
			require.True(t, closed(physical))
		})
	}
}

func TestPoolCloseRetiresWarmTransports(t *testing.T) {
	h := newWarmHarness(t, 2, 0)
	h.visit(t, "alpha")
	h.visit(t, "beta")
	require.NoError(t, h.pool.Close())
	for _, identity := range []ports.BrokerDaemonIdentity{"alpha", "beta"} {
		require.True(t, closed(h.physicals(identity)[0]), identity)
	}
	_, ok := h.pool.SharedPhysical("alpha", poolPolicy())
	require.False(t, ok)
}

func TestPoolSharedPhysical(t *testing.T) {
	tests := []struct {
		name     string
		identity ports.BrokerDaemonIdentity
		policy   func() ports.BrokerPolicy
		want     bool
	}{
		{name: "warm exact key", identity: "alpha", policy: poolPolicy, want: true},
		{name: "unknown identity", identity: "beta", policy: poolPolicy},
		{name: "different policy", identity: "alpha", policy: func() ports.BrokerPolicy { p := poolPolicy(); p.Trust = "other"; return p }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newWarmHarness(t, 1, 0)
			h.visit(t, "alpha")
			physical, ok := h.pool.SharedPhysical(tt.identity, tt.policy())
			require.Equal(t, tt.want, ok)
			if tt.want {
				require.Same(t, h.physicals("alpha")[0], physical)
				// Borrowing neither dials nor refreshes warm order: beta still
				// evicts alpha.
				h.visit(t, "beta")
				await(t, h.physicals("alpha")[0].done)
				require.Len(t, h.physicals("alpha"), 1)
			}
		})
	}
}

func TestNewPoolWarmLimits(t *testing.T) {
	tests := []struct {
		name    string
		warm    int
		idle    time.Duration
		wantErr bool
	}{
		{name: "defaults off", warm: 0, idle: 0},
		{name: "bounded with grace", warm: 8, idle: time.Minute},
		{name: "negative warm", warm: -1, wantErr: true},
		{name: "negative idle", warm: 1, idle: -time.Second, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := NewPool(1, portsmocks.NewMockBrokerRouteAuthority(t), portsmocks.NewMockBrokerIdentityBinder(t), portsmocks.NewMockBrokerEndpointConnector(t), newManualClock(time.Unix(0, 0)), PoolLimits{Physical: 1, Clients: 1, Streams: 1, StreamsPerClient: 1, Warm: tt.warm, Idle: tt.idle})
			if tt.wantErr {
				require.ErrorIs(t, err, ports.BrokerAdmissionInvalid)
				return
			}
			require.NoError(t, err)
			require.NoError(t, pool.Close())
		})
	}
}
