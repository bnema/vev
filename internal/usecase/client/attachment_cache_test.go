package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// The peer uses typed messages so the tests exercise publication correlation,
// reader ownership and terminal isolation independently of carriage encoding.
type cacheTestPeer struct {
	ports.ClientConnection
	messages   chan protocol.ServerMessage
	sent       chan protocol.ClientMessage
	closed     chan struct{}
	once       sync.Once
	identity   protocol.CommittedRouteIdentity
	reject     bool
	activation func(protocol.ActivateAttachment)
	// beforeSend, beforeRecv, and onClose let lifecycle tests stall one
	// direction at a time. onClose runs after closed is signaled, modelling a
	// transport whose Close unblocks readers but returns slowly.
	beforeSend func(protocol.ClientMessage)
	beforeRecv func()
	afterClose func()
	onClose    func()
}

func testCacheConfig(idle time.Duration) domain.AttachmentCacheConfig {
	return domain.AttachmentCacheConfig{Enabled: true, Capacity: domain.DefaultAttachmentCacheCapacity, IdleTimeout: idle}
}

func newCacheTestPeer() *cacheTestPeer {
	return &cacheTestPeer{messages: make(chan protocol.ServerMessage, 16), sent: make(chan protocol.ClientMessage, 16), closed: make(chan struct{}), identity: testOutputView(1).Route}
}
func (p *cacheTestPeer) Close() error {
	p.once.Do(func() {
		close(p.closed)
		if p.onClose != nil {
			p.onClose()
		}
	})
	return nil
}
func (p *cacheTestPeer) ReceiveServer() (protocol.ServerMessage, error) {
	if p.beforeRecv != nil {
		p.beforeRecv()
	}
	select {
	case m := <-p.messages:
		return m, nil
	case <-p.closed:
		if p.afterClose != nil {
			p.afterClose()
		}
		return nil, io.EOF
	}
}
func (p *cacheTestPeer) SendClient(message protocol.ClientMessage) error {
	if p.beforeSend != nil {
		p.beforeSend(message)
	}
	p.sent <- message
	switch m := message.(type) {
	case protocol.SuspendAttachment:
		p.messages <- protocol.AttachmentSuspended{RequestID: m.RequestID, Target: p.identity.Target}
	case protocol.ActivateAttachment:
		if p.activation != nil {
			p.activation(m)
			return nil
		}
		if p.reject {
			p.messages <- protocol.ErrorMsg{}
			return nil
		}
		view := testOutputView(2)
		p.messages <- protocol.Output{Epoch: 2, New: 1, Full: true, Context: &view, Data: []byte("full")}
		p.messages <- protocol.RoutePosition{Target: view.Route.Target, ActiveTabID: "tab-2"}
		p.messages <- protocol.AttachmentActivated{RequestID: m.RequestID, Identity: view.Route, Epoch: 2, State: 1, ViewPublication: 2}
	}
	return nil
}
func cacheTestRequest(endpoint string) AttachRequest {
	return AttachRequest{Remote: true, OriginKey: endpoint, Intent: protocol.IntentAttach, SessionName: "work"}
}

// peerClosed reports whether Close has been called on a test peer.
func peerClosed(p *cacheTestPeer) bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}
func parkTestPeer(t *testing.T, c *attachmentCoordinator, endpoint string) (*cachedAttachment, *cacheTestPeer) {
	t.Helper()
	p := newCacheTestPeer()
	e := newCachedAttachment(p)
	e.startReader()
	e.epoch = 1
	require.True(t, c.suspend(context.Background(), e, cacheTestRequest(endpoint), attachResult{committedIdentity: &p.identity, resumeToken: 7}))
	return e, p
}
func TestAttachmentCacheMultipleEndpointsAndFullActivation(t *testing.T) {
	c := newAttachmentCoordinator(newInventoryTestClock(), testCacheConfig(time.Minute))
	defer c.close()
	a, pa := parkTestPeer(t, c, "a")
	b, pb := parkTestPeer(t, c, "b")
	for _, tt := range []struct {
		key   string
		entry *cachedAttachment
		peer  *cacheTestPeer
	}{{"a", a, pa}, {"b", b, pb}} {
		t.Run(tt.key, func(t *testing.T) {
			geometry := domain.Geometry{Size: domain.Size{Cols: 101, Rows: 37}}
			got, ack, full, position := c.activate(context.Background(), cacheTestRequest(tt.key), geometry, nil)
			require.Same(t, tt.entry, got)
			require.NotNil(t, ack)
			require.Equal(t, protocol.RoutePosition{Target: ack.Identity.Target, ActiveTabID: "tab-2"}, *position)
			require.Equal(t, []byte("full"), full.Data)
			require.IsType(t, protocol.SuspendAttachment{}, <-tt.peer.sent)
			activation := (<-tt.peer.sent).(protocol.ActivateAttachment)
			require.Equal(t, geometry.Size, activation.Size)
			got.close()
			<-got.readerDone
		})
	}
}
func TestAttachmentCacheEviction(t *testing.T) {
	for _, scenario := range []string{"expiry", "death", "stale lifecycle", "activation failure", "cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			clock := newInventoryTestClock()
			c := newAttachmentCoordinator(clock, testCacheConfig(time.Minute))
			defer c.close()
			e, p := parkTestPeer(t, c, "a")
			request := cacheTestRequest("a")
			ctx := context.Background()
			switch scenario {
			case "expiry":
				e.timer.(*inventoryTestTimer).ch <- clock.Now()
				<-e.dormantDone
			case "death":
				_ = p.Close()
				<-e.dormantDone
			case "stale lifecycle":
				target := p.identity.Target
				target.LifecycleID[0]++
				request.ExactTarget = &target
			case "activation failure":
				p.reject = true
			case "cancellation":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			got, _, _, _ := c.activate(ctx, request, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil)
			require.Nil(t, got)
			<-p.closed
			<-e.readerDone
			require.Empty(t, c.entries)
		})
	}
}
func TestAttachmentCacheDormantDrainAndExit(t *testing.T) {
	c := newAttachmentCoordinator(newInventoryTestClock(), testCacheConfig(time.Minute))
	e, p := parkTestPeer(t, c, "a")
	for i := 0; i < 100; i++ {
		p.messages <- protocol.Output{Data: []byte("must never reach terminal")}
	}
	c.close()
	<-p.closed
	<-e.readerDone
	<-e.dormantDone
	require.Empty(t, c.entries)
	require.Len(t, p.sent, 1, "dormant entry emits no input, resize or output ACK")
}

func TestAttachmentCacheRejectsUncommittedPublication(t *testing.T) {
	for _, scenario := range []string{"ack before full", "wrong request", "wrong epoch", "wrong publication", "wrong lifecycle", "old output chain", "ordinary output after activation", "missing position", "invalid position", "wrong position target", "position before full", "duplicate position"} {
		t.Run(scenario, func(t *testing.T) {
			c := newAttachmentCoordinator(newInventoryTestClock(), testCacheConfig(time.Minute))
			defer c.close()
			e, p := parkTestPeer(t, c, "a")
			p.activation = func(request protocol.ActivateAttachment) {
				view := testOutputView(2)
				full := protocol.Output{Epoch: 2, New: 1, Full: true, Context: &view}
				ack := protocol.AttachmentActivated{RequestID: request.RequestID, Identity: view.Route, Epoch: 2, State: 1, ViewPublication: 2}
				switch scenario {
				case "ack before full":
					p.messages <- ack
					return
				case "wrong request":
					ack.RequestID++
				case "wrong epoch":
					ack.Epoch++
				case "wrong publication":
					ack.ViewPublication++
				case "wrong lifecycle":
					ack.Identity.Target.LifecycleID[0]++
				case "ordinary output after activation":
					full.Full = false
				case "old output chain":
					full.Epoch = 1
				}
				position := protocol.RoutePosition{Target: view.Route.Target, ActiveTabID: "tab-2"}
				switch scenario {
				case "invalid position":
					position.ActiveTabID = ""
				case "wrong position target":
					position.Target.LifecycleID[0]++
				case "position before full":
					p.messages <- position
				}
				p.messages <- full
				if scenario != "missing position" {
					p.messages <- position
				}
				if scenario == "duplicate position" {
					p.messages <- position
				}
				p.messages <- ack
			}
			got, _, _, _ := c.activate(context.Background(), cacheTestRequest("a"), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil)
			require.Nil(t, got)
			<-e.readerDone
			require.False(t, c.has("a"))
		})
	}
}

func TestAttachmentCachePreferredTabEligibility(t *testing.T) {
	for _, tt := range []struct {
		name      string
		preferred domain.TabStableID
		warm      bool
	}{
		{"no preference", "", true},
		{"matching committed tab", "tab-2", true},
		{"different tab", "tab-1", false},
		{"disappeared tab", "missing", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newAttachmentCoordinator(newInventoryTestClock(), testCacheConfig(time.Minute))
			defer c.close()
			e, p := parkTestPeer(t, c, "a")
			e.request.PreferredTabID = "tab-2"
			request := cacheTestRequest("a")
			request.PreferredTabID = tt.preferred
			got, _, _, _ := c.activate(context.Background(), request, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil)
			if tt.warm {
				require.Same(t, e, got)
				got.release()
			} else {
				require.Nil(t, got)
				require.Len(t, p.sent, 1, "mismatched preference must fall back before activation")
			}
		})
	}
}

func TestAttachmentCacheActivationDrainsQueuedDormantMessages(t *testing.T) {
	for _, tt := range []struct {
		name   string
		queued recvResult
		warm   bool
	}{
		{"ordinary output", recvResult{message: protocol.Output{Data: []byte("discard")}}, true},
		{"receive error", recvResult{err: io.EOF}, false},
		{"detached", recvResult{message: protocol.Detached{}}, false},
		{"server error", recvResult{message: protocol.ErrorMsg{}}, false},
		{"premature activation", recvResult{message: protocol.AttachmentActivated{}}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newAttachmentCoordinator(newInventoryTestClock(), testCacheConfig(time.Minute))
			defer c.close()
			e, p := parkTestPeer(t, c, "a")
			// Freeze dormant consumption so the message is deterministically
			// queued at ownership transfer, rather than racing the worker.
			close(e.stopDormant)
			e.waitDormant()
			e.stopDormant = make(chan struct{})
			e.inbox <- tt.queued
			got, _, full, _ := c.activate(context.Background(), cacheTestRequest("a"), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil)
			if tt.warm {
				require.Same(t, e, got)
				require.Equal(t, []byte("full"), full.Data)
				got.release()
			} else {
				require.Nil(t, got)
				require.Len(t, p.sent, 1)
			}
		})
	}
}

// attachmentClock records every armed timer with its duration so lifecycle
// tests can fire exactly the timer they mean instead of keying on a duration
// inside the clock itself.
type attachmentClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*attachmentTimer
}

type attachmentTimer struct {
	ch chan time.Time
	d  time.Duration
}

func newAttachmentClock() *attachmentClock {
	return &attachmentClock{now: time.Unix(1000, 0)}
}

func (c *attachmentClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// armedFor reports whether a timer of duration d was ever armed.
func (c *attachmentClock) armedFor(d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, timer := range c.timers {
		if timer.d == d {
			return true
		}
	}
	return false
}

func (c *attachmentClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

func (c *attachmentClock) NewTimer(d time.Duration) ports.Timer {
	timer := &attachmentTimer{ch: make(chan time.Time, 1), d: d}
	c.mu.Lock()
	c.timers = append(c.timers, timer)
	c.mu.Unlock()
	return timer
}

func (t *attachmentTimer) C() <-chan time.Time    { return t.ch }
func (*attachmentTimer) Reset(time.Duration) bool { return false }
func (*attachmentTimer) Stop() bool               { return true }

// fire delivers the most recently armed timer for d once one is registered.
func (c *attachmentClock) fire(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		var match *attachmentTimer
		for _, timer := range c.timers {
			if timer.d == d {
				match = timer
			}
		}
		c.mu.Unlock()
		if match != nil {
			match.ch <- time.Now()
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no timer armed for %s", d)
}

func TestAttachmentCacheLRUCapacityAndRecency(t *testing.T) {
	t.Run("capacity one evicts the replaced destination", func(t *testing.T) {
		c := newAttachmentCoordinator(newAttachmentClock(), domain.AttachmentCacheConfig{Enabled: true, Capacity: 1})
		defer c.close()
		_, pa := parkTestPeer(t, c, "a")
		_, pb := parkTestPeer(t, c, "b")
		require.False(t, c.has("a"))
		require.True(t, c.has("b"))
		<-pa.closed
		require.False(t, peerClosed(pb), "the retained destination stays open")
	})
	t.Run("nine destinations retain the newest eight", func(t *testing.T) {
		c := newAttachmentCoordinator(newAttachmentClock(), testCacheConfig(0))
		defer c.close()
		peers := make(map[string]*cacheTestPeer)
		for i := 0; i < 9; i++ {
			key := fmt.Sprintf("e%d", i)
			_, p := parkTestPeer(t, c, key)
			peers[key] = p
		}
		require.False(t, c.has("e0"), "oldest dormant destination is evicted")
		<-peers["e0"].closed
		for i := 1; i < 9; i++ {
			require.True(t, c.has(fmt.Sprintf("e%d", i)))
		}
	})
	t.Run("least recently used dormant entry is evicted first", func(t *testing.T) {
		c := newAttachmentCoordinator(newAttachmentClock(), domain.AttachmentCacheConfig{Enabled: true, Capacity: 2})
		defer c.close()
		_, pa := parkTestPeer(t, c, "a")
		_, pb := parkTestPeer(t, c, "b")
		_, pc := parkTestPeer(t, c, "c")
		require.False(t, c.has("a"))
		require.True(t, c.has("b"))
		require.True(t, c.has("c"))
		<-pa.closed
		require.False(t, peerClosed(pb))
		require.False(t, peerClosed(pc))
	})
	t.Run("re-suspension makes the entry most recently used", func(t *testing.T) {
		c := newAttachmentCoordinator(newAttachmentClock(), domain.AttachmentCacheConfig{Enabled: true, Capacity: 2})
		defer c.close()
		e, pa := parkTestPeer(t, c, "a")
		_, pb := parkTestPeer(t, c, "b")
		got, _, _, _ := c.activate(context.Background(), cacheTestRequest("a"), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil)
		require.Same(t, e, got)
		require.True(t, c.suspend(context.Background(), e, cacheTestRequest("a"), attachResult{committedIdentity: &pa.identity, resumeToken: 7}))
		_, pc := parkTestPeer(t, c, "c")
		require.True(t, c.has("a"), "recently used entry survives")
		require.False(t, c.has("b"), "older entry is evicted")
		require.True(t, c.has("c"))
		<-pb.closed
		require.False(t, peerClosed(pa), "the re-suspended entry stays retained")
		require.False(t, peerClosed(pc))
	})
	t.Run("aliases stay distinct", func(t *testing.T) {
		c := newAttachmentCoordinator(newAttachmentClock(), domain.AttachmentCacheConfig{Enabled: true, Capacity: 1})
		defer c.close()
		_, _ = parkTestPeer(t, c, "alias-one")
		_, _ = parkTestPeer(t, c, "alias-two")
		require.True(t, c.has("alias-two"))
		require.False(t, c.has("alias-one"))
	})
}

func TestAttachmentCacheDisabledPolicyRetainsNothing(t *testing.T) {
	clock := newAttachmentClock()
	c := newAttachmentCoordinator(clock, domain.AttachmentCacheConfig{Enabled: false})
	p := newCacheTestPeer()
	e := newCachedAttachment(p)
	e.startReader()
	require.False(t, c.suspend(context.Background(), e, cacheTestRequest("a"), attachResult{committedIdentity: &p.identity, resumeToken: 1}))
	require.Empty(t, c.entries)
	require.Zero(t, clock.timerCount())
	require.False(t, c.enabled())
	e.release()
}

func TestAttachmentCacheWithoutIdleTimeoutArmsNoTimer(t *testing.T) {
	clock := newAttachmentClock()
	c := newAttachmentCoordinator(clock, testCacheConfig(0))
	defer c.close()
	e, _ := parkTestPeer(t, c, "a")
	require.Nil(t, e.timer, "no client age expiry allocates no idle timer")
	require.False(t, clock.armedFor(time.Minute), "only the shared handshake timer may exist")
	require.True(t, c.has("a"), "an entry without a timeout is retained")
}

func TestAttachmentCacheShutdownInitiatesEveryCloseBeforeJoining(t *testing.T) {
	const peers = 4
	c := newAttachmentCoordinator(newAttachmentClock(), testCacheConfig(0))
	var closed sync.WaitGroup
	closed.Add(peers)
	entries := make([]*cachedAttachment, 0, peers)
	for i := 0; i < peers; i++ {
		p := newCacheTestPeer()
		// The reader only completes after every peer received Close, so a
		// serialized close-then-join shutdown can never make progress.
		p.onClose = closed.Done
		p.afterClose = closed.Wait
		e := newCachedAttachment(p)
		e.startReader()
		require.True(t, c.suspend(context.Background(), e, cacheTestRequest(fmt.Sprintf("e%d", i)), attachResult{committedIdentity: &p.identity, resumeToken: 1}))
		entries = append(entries, e)
	}
	done := make(chan struct{})
	go func() {
		c.close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown serialized transport closes instead of initiating all of them first")
	}
	for _, e := range entries {
		_, err := e.receive(context.Background())
		require.Error(t, err)
	}
}

func TestAttachmentCacheShutdownIsBoundedWhenCloseStalls(t *testing.T) {
	clock := newAttachmentClock()
	c := newAttachmentCoordinator(clock, testCacheConfig(0))
	release := make(chan struct{})
	p := newCacheTestPeer()
	p.afterClose = func() { <-release }
	e := newCachedAttachment(p)
	e.startReader()
	require.True(t, c.suspend(context.Background(), e, cacheTestRequest("a"), attachResult{committedIdentity: &p.identity, resumeToken: 1}))
	done := make(chan struct{})
	go func() {
		c.close()
		close(done)
	}()
	<-p.closed
	clock.fire(t, attachmentRetirementJoinTimeout)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not honor its clock bound")
	}
	close(release)
}

func TestAttachmentCacheShutdownRacesLifecycle(t *testing.T) {
	for i := 0; i < 25; i++ {
		clock := newAttachmentClock()
		c := newAttachmentCoordinator(clock, testCacheConfig(time.Minute))
		e, p := parkTestPeer(t, c, "a")
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); <-start; c.close() }()
		go func() { defer wg.Done(); <-start; _ = p.Close() }()
		go func() {
			defer wg.Done()
			<-start
			c.activate(context.Background(), cacheTestRequest("a"), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil)
		}()
		close(start)
		wg.Wait()
		e.release()
		require.False(t, c.has("a"))
	}
}

func TestAttachmentCacheWarmActivationDeadlineFallsBack(t *testing.T) {
	ctx := context.Background()
	geometry := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}
	for _, tt := range []struct {
		name  string
		stall func(p *cacheTestPeer, release <-chan struct{})
	}{
		{"blocked receive", func(p *cacheTestPeer, release <-chan struct{}) {
			p.activation = func(protocol.ActivateAttachment) { <-release }
		}},
		{"blocked send", func(p *cacheTestPeer, release <-chan struct{}) {
			p.beforeSend = func(m protocol.ClientMessage) {
				if _, ok := m.(protocol.ActivateAttachment); ok {
					<-release
				}
			}
		}},
		{"slow close after revocation", func(p *cacheTestPeer, release <-chan struct{}) {
			p.activation = func(protocol.ActivateAttachment) { <-release }
			p.onClose = func() { <-release }
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clock := newAttachmentClock()
			c := newAttachmentCoordinator(clock, testCacheConfig(0))
			release := make(chan struct{})
			p := newCacheTestPeer()
			tt.stall(p, release)
			e := newCachedAttachment(p)
			e.startReader()
			require.True(t, c.suspend(ctx, e, cacheTestRequest("a"), attachResult{committedIdentity: &p.identity, resumeToken: 1}))
			activated := make(chan *cachedAttachment, 1)
			go func() {
				got, _, _, _ := c.activate(ctx, cacheTestRequest("a"), geometry, nil)
				activated <- got
			}()
			clock.fire(t, warmActivationTimeout)
			select {
			case got := <-activated:
				require.Nil(t, got, "the deadline revokes ownership instead of reusing a stalled transport")
			case <-time.After(2 * time.Second):
				t.Fatal("warm activation did not relinquish ownership at its deadline")
			}
			require.False(t, c.has("a"))
			// Release the deliberate stall so physical retirement can finish
			// before the deferred shutdown joins it.
			close(release)
			c.close()
		})
	}
}
