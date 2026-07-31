package daemon

import (
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// warmHookClock runs a one-shot hook while armProxyWarm sits between its
// reservation and its publication, so a lifecycle race can be interleaved
// deterministically instead of being waited for.
type warmHookClock struct {
	timers chan *signalTimer

	mu   sync.Mutex
	hook func()
}

func (c *warmHookClock) Now() time.Time { return time.Time{} }

func (c *warmHookClock) NewTimer(d time.Duration) ports.Timer {
	timer := &signalTimer{ch: make(chan time.Time, 1), duration: d}
	c.mu.Lock()
	hook := c.hook
	c.hook = nil
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	if c.timers != nil {
		c.timers <- timer
	}
	return timer
}

func (c *warmHookClock) setHook(hook func()) {
	c.mu.Lock()
	c.hook = hook
	c.mu.Unlock()
}

func registerLifecycleProxy(t *testing.T, d *Daemon, key domain.RemoteSessionKey) *proxySession {
	t.Helper()
	p, err := newProxySession(key, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	d.mu.Lock()
	registered := d.registerSessionLocked(p)
	d.mu.Unlock()
	require.True(t, registered, "lifecycle fixture proxy was not published")
	return p
}

func newProxyLifecycleFixture(t *testing.T) (*Daemon, *proxySession, *warmHookClock) {
	t.Helper()
	clock := &warmHookClock{timers: make(chan *signalTimer, 4)}
	d := newProxyTestDaemon(t, nil, clock)
	return d, registerLifecycleProxy(t, d, domain.RemoteSessionKey{Host: "arch", Name: "work"}), clock
}

func proxyWarmToken(p *proxySession) (*proxyWarmTimer, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.warm, p.generation
}

func setProxyLifecycleClient(p *proxySession, ac *attachedClient) {
	p.sessionCore.mu.Lock()
	p.client = ac
	p.sessionCore.mu.Unlock()
}

func proxyRegistered(d *Daemon, p *proxySession) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions[p.id] == p
}

func proxyExpired(p *proxySession) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.expired
}

func armWarmTimer(t *testing.T, d *Daemon, p *proxySession, clock *warmHookClock) (*proxyWarmTimer, *signalTimer) {
	t.Helper()
	require.True(t, d.armProxyWarm(p), "warm timer was not published")
	timer := awaitTestValue(t, clock.timers, "warm timer was never created")
	token, _ := proxyWarmToken(p)
	require.NotNil(t, token, "published arm left no warm token")
	return token, timer
}

// TestArmProxyWarmKeepsExpiryPathWhenPublicationFails pins the two-phase arm:
// a re-arm that loses its revalidation must leave the incumbent timer both
// installed and current, otherwise a registered proxy is left with no path out
// of the live registry.
func TestArmProxyWarmKeepsExpiryPathWhenPublicationFails(t *testing.T) {
	tests := []struct {
		name string
		race func(d *Daemon, p *proxySession)
		undo func(d *Daemon, p *proxySession)
	}{
		{
			name: "client attaches during arm",
			race: func(_ *Daemon, p *proxySession) { setProxyLifecycleClient(p, &attachedClient{}) },
			undo: func(_ *Daemon, p *proxySession) { setProxyLifecycleClient(p, nil) },
		},
		{
			name: "replacement rekeys the lifecycle generation",
			race: func(_ *Daemon, p *proxySession) {
				p.mu.Lock()
				p.expired = true
				p.generation++
				if p.warm != nil {
					p.warm.generation = p.generation
				}
				p.mu.Unlock()
			},
		},
		{
			name: "daemon starts closing during arm",
			race: func(d *Daemon, _ *proxySession) {
				d.mu.Lock()
				d.closing = true
				d.mu.Unlock()
			},
			undo: func(d *Daemon, _ *proxySession) {
				d.mu.Lock()
				d.closing = false
				d.mu.Unlock()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, p, clock := newProxyLifecycleFixture(t)
			token, timer := armWarmTimer(t, d, p, clock)

			clock.setHook(func() { tt.race(d, p) })
			require.False(t, d.armProxyWarm(p), "raced re-arm must not publish a replacement")
			awaitTestValue(t, clock.timers, "raced re-arm never reached its clock")

			retained, generation := proxyWarmToken(p)
			require.NotNil(t, retained, "failed re-arm stripped the proxy of its only expiry path")
			require.Same(t, token, retained, "failed re-arm replaced the armed expiry timer")
			require.Equal(t, generation, retained.generation, "retained timer no longer owns the lifecycle generation")
			require.True(t, proxyRegistered(d, p), "a failed re-arm must not unregister the proxy")

			if tt.undo != nil {
				tt.undo(d, p)
			}
			timer.ch <- time.Time{}
			awaitTestCompletion(t, token.done, "retained warm timer never ran to completion")
			require.False(t, proxyRegistered(d, p), "retained warm timer did not expire the dormant proxy")
			require.True(t, proxyExpired(p), "expiry did not publish terminal lifecycle state")
			awaitTestCompletion(t, d.done, "last proxy expiry did not complete the daemon")
		})
	}
}

func TestMarkProxyReplacedWarmTimerOwnership(t *testing.T) {
	t.Run("rekeys an already armed timer", func(t *testing.T) {
		d, p, clock := newProxyLifecycleFixture(t)
		transport := newProxyTestTransport()
		t.Cleanup(func() { _ = transport.Close() })
		generation, _ := p.installTransport(transport)
		token, timer := armWarmTimer(t, d, p, clock)

		require.True(t, d.markProxyReplaced(p, generation, transport))
		require.Empty(t, clock.timers, "replacement armed a second timer instead of rekeying")

		rekeyed, lifecycle := proxyWarmToken(p)
		require.Same(t, token, rekeyed, "replacement replaced the armed timer")
		require.Equal(t, lifecycle, rekeyed.generation, "replacement did not rekey the armed timer")
		require.True(t, proxyExpired(p))
		p.mu.Lock()
		state := p.linkState
		p.mu.Unlock()
		require.Equal(t, ports.LinkStateDead, state)

		timer.ch <- time.Time{}
		awaitTestCompletion(t, token.done, "rekeyed warm timer never ran to completion")
		require.False(t, proxyRegistered(d, p), "rekeyed warm timer did not expire the proxy")
	})

	t.Run("arms an expiry for a headless proxy", func(t *testing.T) {
		d, p, clock := newProxyLifecycleFixture(t)
		transport := newProxyTestTransport()
		t.Cleanup(func() { _ = transport.Close() })
		generation, _ := p.installTransport(transport)

		require.True(t, d.markProxyReplaced(p, generation, transport))
		timer := awaitTestValue(t, clock.timers, "replacement did not arm a warm timer")
		token, lifecycle := proxyWarmToken(p)
		require.NotNil(t, token, "replaced headless proxy has no expiry path")
		require.Equal(t, lifecycle, token.generation)

		timer.ch <- time.Time{}
		awaitTestCompletion(t, token.done, "warm timer never ran to completion")
		require.False(t, proxyRegistered(d, p))
	})

	t.Run("stale link generation publishes nothing", func(t *testing.T) {
		d, p, _ := newProxyLifecycleFixture(t)
		transport := newProxyTestTransport()
		t.Cleanup(func() { _ = transport.Close() })
		generation, _ := p.installTransport(transport)

		require.False(t, d.markProxyReplaced(p, generation-1, transport))
		require.False(t, proxyExpired(p))
		warm, _ := proxyWarmToken(p)
		require.Nil(t, warm, "a rejected replacement must not arm an expiry")
	})
}

func TestExpireWarmProxyCompletesDaemonOnlyOnLastSession(t *testing.T) {
	t.Run("last session completes the daemon", func(t *testing.T) {
		d, p, clock := newProxyLifecycleFixture(t)
		token, timer := armWarmTimer(t, d, p, clock)

		timer.ch <- time.Time{}
		awaitTestCompletion(t, token.done, "warm timer never ran to completion")
		awaitTestCompletion(t, p.done, "expiry did not finish the proxy")
		awaitTestCompletion(t, d.done, "last session expiry did not complete the daemon")
		require.False(t, proxyRegistered(d, p))
		require.True(t, proxyExpired(p))
	})

	t.Run("surviving session keeps the daemon running", func(t *testing.T) {
		d, p, clock := newProxyLifecycleFixture(t)
		survivor := registerLifecycleProxy(t, d, domain.RemoteSessionKey{Host: "arch", Name: "other"})
		token, timer := armWarmTimer(t, d, p, clock)

		timer.ch <- time.Time{}
		awaitTestCompletion(t, token.done, "warm timer never ran to completion")
		require.False(t, proxyRegistered(d, p))
		require.True(t, proxyRegistered(d, survivor))
		select {
		case <-d.done:
			t.Fatal("daemon completed while a session was still registered")
		default:
		}
	})
}

// TestExpireWarmProxyIgnoresSupersededToken proves the expiry callback is exact:
// a token that lost its lifecycle generation must publish no terminal state and
// must not unregister the proxy.
func TestExpireWarmProxyIgnoresSupersededToken(t *testing.T) {
	d, p, clock := newProxyLifecycleFixture(t)
	stale, _ := armWarmTimer(t, d, p, clock)
	current, currentTimer := armWarmTimer(t, d, p, clock)
	require.NotSame(t, stale, current, "re-arm did not publish a replacement timer")
	awaitTestCompletion(t, stale.done, "superseded warm timer was not canceled")

	require.False(t, d.expireWarmProxy(p, stale), "a superseded token expired the proxy")
	require.True(t, proxyRegistered(d, p))
	require.False(t, proxyExpired(p))
	installed, generation := proxyWarmToken(p)
	require.Same(t, current, installed)
	require.Equal(t, generation, installed.generation)

	currentTimer.ch <- time.Time{}
	awaitTestCompletion(t, current.done, "current warm timer never ran to completion")
	require.False(t, proxyRegistered(d, p))
}
