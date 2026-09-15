package client

import (
	"container/list"
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// warmActivationTimeout bounds the client-local reuse probe that precedes a
// cold dial. It is deliberately separate from protocol.HandshakeTimeout: when
// a retained transport stalls, the client logically revokes its ownership at
// this deadline and hands the following cold dial a fresh, full handshake
// budget. Physical retirement of the stalled transport continues in the
// background and is joined at runner shutdown.
const warmActivationTimeout = 2 * time.Second

// attachmentRetirementJoinTimeout bounds the shutdown join of detached
// transports. A transport that violates the ClientConnection.Close contract
// must not pin runner teardown.
const attachmentRetirementJoinTimeout = 5 * time.Second

// attachmentCoordinator owns dormant transports, not endpoint resolution or
// route history. Dormant workers may evict their own entry but own no terminal
// or route state. One coordinator exists per Runner.Run lifetime and ends in a
// terminal closed state.
type attachmentCoordinator struct {
	mu       sync.Mutex
	clock    ports.Clock
	cfg      domain.AttachmentCacheConfig
	entries  map[string]*cachedAttachment
	lru      *list.List // front = most recently suspended; values are *cachedAttachment
	retiring map[*cachedAttachment]struct{}
	closed   bool
}

// cachedAttachment is one dormant transport. Each entry owns one LRU element
// and at most one dormant worker and one retirement goroutine.
type cachedAttachment struct {
	transport     ports.ClientConnection
	inbox         chan recvResult
	done          chan struct{}
	readerDone    chan struct{}
	retireDone    chan struct{}
	readerStarted bool
	retireOnce    sync.Once
	once          sync.Once
	key           string
	element       *list.Element
	request       AttachRequest
	identity      protocol.CommittedRouteIdentity
	resumeToken   uint64
	epoch         uint64
	sequence      uint64
	expires       time.Time
	timer         ports.Timer
	stopDormant   chan struct{}
	dormantDone   chan struct{}
}

func newAttachmentCoordinator(clock ports.Clock, cfg domain.AttachmentCacheConfig) *attachmentCoordinator {
	if clock == nil {
		clock = systemClock{}
	}
	return &attachmentCoordinator{
		clock:    clock,
		cfg:      cfg,
		entries:  make(map[string]*cachedAttachment),
		lru:      list.New(),
		retiring: make(map[*cachedAttachment]struct{}),
	}
}

// enabled reports whether this coordinator retains dormant transports at all.
func (c *attachmentCoordinator) enabled() bool {
	return c.cfg.Enabled && c.cfg.Valid()
}

func newCachedAttachment(transport ports.ClientConnection) *cachedAttachment {
	return &cachedAttachment{
		transport:  transport,
		inbox:      make(chan recvResult, 1),
		done:       make(chan struct{}),
		readerDone: make(chan struct{}),
		retireDone: make(chan struct{}),
	}
}

func (e *cachedAttachment) startReader() {
	e.readerStarted = true
	go func() {
		defer close(e.readerDone)
		for {
			message, err := e.transport.ReceiveServer()
			var failure *protocol.DecodeFailure
			if errors.As(err, &failure) && (failure.Category == protocol.DecodeUnknownType || failure.Category == protocol.DecodeWrongDirection) {
				continue
			}
			select {
			case e.inbox <- recvResult{message: message, err: err}:
			case <-e.done:
				return
			}
			if err != nil {
				return
			}
		}
	}()
}

// close signals the port to unblock, exactly once. It never waits.
func (e *cachedAttachment) close() {
	e.once.Do(func() { close(e.done); _ = e.transport.Close() })
}

func (e *cachedAttachment) receive(ctx context.Context) (protocol.ServerMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.done:
		return nil, errors.New("vev: cached attachment closed")
	case r := <-e.inbox:
		return r.message, r.err
	}
}

// transition runs under the shared handshake budget, including blocked sends.
func (c *attachmentCoordinator) transition(ctx context.Context, e *cachedAttachment, operation func(context.Context) error) error {
	bounded, _, finish := newHandshakeContext(ctx, c.clock)
	stop := watchHandshakeTransport(bounded, e.transport)
	defer finish()
	defer stop()
	if err := bounded.Err(); err != nil {
		e.close()
		return err
	}
	err := operation(bounded)
	if err == nil {
		err = bounded.Err()
	}
	if err != nil {
		e.close()
	}
	return err
}

// warmActivation runs under the client-local warm budget while pumping the
// transition toast. At the deadline the caller revokes ownership and the
// stalled transport is retired asynchronously: neither a blocked send, a
// blocked receive, nor a slow Close delays the cold dial that follows.
func (c *attachmentCoordinator) warmActivation(ctx context.Context, e *cachedAttachment, transition *transitionUI, operation func(context.Context) error) error {
	bounded, _, finish := newBoundedContext(ctx, c.clock, warmActivationTimeout)
	defer finish()
	if err := bounded.Err(); err != nil {
		return err
	}
	completed := make(chan error, 1)
	go func() { completed <- operation(bounded) }()
	for {
		select {
		case err := <-completed:
			if ctxErr := bounded.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		case <-transition.tickC():
			if err := transition.advance(); err != nil {
				return err
			}
		case <-bounded.Done():
			return bounded.Err()
		}
	}
}

// suspend performs the daemon handshake and, on success, installs the entry as
// dormant. A false result leaves the entry owned by the caller.
func (c *attachmentCoordinator) suspend(ctx context.Context, e *cachedAttachment, request AttachRequest, result attachResult) bool {
	if !c.enabled() || !request.Remote || request.OriginKey == "" || result.committedIdentity == nil {
		return false
	}
	e.sequence++
	e.identity = *result.committedIdentity
	e.request = cloneAttachRequest(request)
	e.resumeToken = result.resumeToken
	err := c.transition(ctx, e, func(ctx context.Context) error {
		if err := e.transport.SendClient(protocol.SuspendAttachment{RequestID: e.sequence}); err != nil {
			return err
		}
		for {
			message, err := e.receive(ctx)
			if err != nil {
				return err
			}
			switch m := message.(type) {
			case protocol.Output:
				if m.Epoch > e.epoch {
					e.epoch = m.Epoch
				}
			case protocol.AttachmentSuspended:
				if m.Validate() != nil || m.RequestID != e.sequence || m.Target != e.identity.Target {
					return protocol.ErrInvalidAttachment
				}
				return nil
			case protocol.ErrorMsg, protocol.Detached:
				return protocol.ErrInvalidAttachment
			}
		}
	})
	if err != nil {
		return false
	}
	e.key = request.OriginKey
	e.expires = c.clock.Now().Add(c.cfg.IdleTimeout)
	if c.cfg.IdleTimeout > 0 {
		e.timer = c.clock.NewTimer(c.cfg.IdleTimeout)
	}
	e.stopDormant = make(chan struct{})
	e.dormantDone = make(chan struct{})
	victims, installed := c.install(e)
	if !installed {
		close(e.dormantDone)
		return false
	}
	go c.dormantLoop(e)
	for _, victim := range victims {
		c.retire(victim)
	}
	return true
}

// install links the entry into the exact-key map and the LRU list under the
// coordinator lock, then detaches any replaced entry and the least-recently
// used dormant entries beyond capacity. Victims are returned for retirement
// outside the lock. It reports false once the coordinator is terminal.
func (c *attachmentCoordinator) install(e *cachedAttachment) ([]*cachedAttachment, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, false
	}
	var victims []*cachedAttachment
	if old := c.entries[e.key]; old != nil {
		c.removeLocked(old)
		victims = append(victims, old)
	}
	e.element = c.lru.PushFront(e)
	c.entries[e.key] = e
	for len(c.entries) > c.cfg.Capacity {
		back := c.lru.Back()
		if back == nil {
			break
		}
		victim := back.Value.(*cachedAttachment)
		c.removeLocked(victim)
		victims = append(victims, victim)
	}
	return victims, true
}

// removeLocked unlinks one entry from both the map and the LRU list.
func (c *attachmentCoordinator) removeLocked(e *cachedAttachment) {
	if c.entries[e.key] == e {
		delete(c.entries, e.key)
	}
	if e.element != nil {
		c.lru.Remove(e.element)
		e.element = nil
	}
}

// detach unlinks an entry the dormant worker observed dying or expiring.
func (c *attachmentCoordinator) detach(e *cachedAttachment) {
	c.mu.Lock()
	c.removeLocked(e)
	c.mu.Unlock()
}

// dormantLoop owns the entry between suspension and activation. It drains
// output, observes transport death, and, only when an idle timeout is
// configured, arms one expiry timer. No timer or extra goroutine exists for
// the default no-expiry policy.
func (c *attachmentCoordinator) dormantLoop(e *cachedAttachment) {
	defer close(e.dormantDone)
	if e.timer != nil {
		defer e.timer.Stop()
	}
	var idle <-chan time.Time
	if e.timer != nil {
		idle = e.timer.C()
	}
	for {
		select {
		case <-e.done:
			c.detach(e)
			return
		case <-e.stopDormant:
			return
		case <-idle:
			e.close()
			c.detach(e)
			return
		case r := <-e.inbox:
			if r.err != nil {
				e.close()
				c.detach(e)
				return
			}
			switch r.message.(type) {
			case protocol.Detached, protocol.ErrorMsg:
				e.close()
				c.detach(e)
				return
			}
		}
	}
}

func (e *cachedAttachment) waitDormant() {
	if e.dormantDone != nil {
		<-e.dormantDone
	}
}

// activate attempts one warm reuse. It detaches the entry under the lock so
// no concurrent path can observe it as dormant, then validates eligibility
// before sending ActivateAttachment. Every failure retires the transport
// outside the lock and reports a miss; the caller proceeds with a cold dial.
func (c *attachmentCoordinator) activate(ctx context.Context, request AttachRequest, geometry domain.Geometry, transition *transitionUI) (*cachedAttachment, *protocol.AttachmentActivated, *protocol.Output, *protocol.RoutePosition) {
	c.mu.Lock()
	e := c.entries[request.OriginKey]
	if e != nil {
		c.removeLocked(e)
	}
	closed := c.closed
	c.mu.Unlock()
	if e == nil {
		return nil, nil, nil, nil
	}
	if closed {
		c.retire(e)
		return nil, nil, nil, nil
	}
	// A retained entry means a real reuse probe is starting: show its progress
	// exactly like a daemon-authored transition, without entering raw mode
	// early (the toast only paints once raw is already active).
	transition.start(warmActivationTarget(request))
	close(e.stopDormant)
	e.waitDormant()
	fail := func() (*cachedAttachment, *protocol.AttachmentActivated, *protocol.Output, *protocol.RoutePosition) {
		c.retire(e)
		return nil, nil, nil, nil
	}
	select {
	case <-e.done:
		return fail()
	default:
	}
	if (c.cfg.IdleTimeout > 0 && !c.clock.Now().Before(e.expires)) || !request.Remote || request.Intent == protocol.IntentNew || request.SessionName != e.identity.Target.SessionName || request.EnvironmentPolicy != e.request.EnvironmentPolicy || !slices.Equal(request.Environment, e.request.Environment) {
		return fail()
	}
	// Activation retains the attachment cursor; unlike cold Hello it cannot
	// apply a different preferred tab (including a disappeared tab's fallback).
	if request.PreferredTabID != "" && request.PreferredTabID != e.request.PreferredTabID {
		return fail()
	}
	if request.ExactTarget != nil && *request.ExactTarget != e.identity.Target {
		return fail()
	}
	if request.RemoteTarget != nil && (request.RemoteTarget.LifecycleID != e.identity.Target.LifecycleID || request.RemoteTarget.SessionName != e.identity.Target.SessionName) {
		return fail()
	}
	e.sequence++
	var full *protocol.Output
	var activated *protocol.AttachmentActivated
	var position *protocol.RoutePosition
	err := c.warmActivation(ctx, e, transition, func(ctx context.Context) error {
		// The dormant worker has relinquished inbox ownership. Drain messages
		// already observed before sending activation; ordinary output after
		// suspension is invalid, but must never become an activation response.
		// Once Activate is sent, publication validation remains strict.
		if err := e.drainDormant(); err != nil {
			return err
		}
		if err := e.transport.SendClient(protocol.ActivateAttachment{RequestID: e.sequence, Target: e.identity.Target, Size: geometry.Size, PixelWidth: geometry.PixelWidth, PixelHeight: geometry.PixelHeight}); err != nil {
			return err
		}
		for {
			message, err := e.receive(ctx)
			if err != nil {
				return err
			}
			switch m := message.(type) {
			case protocol.Output:
				if full != nil || !m.Full || m.Base != 0 || m.Epoch <= e.epoch || m.New == 0 || m.Context == nil || m.Context.Validate() != nil || m.Context.Route.Target != e.identity.Target {
					return protocol.ErrInvalidAttachment
				}
				full = &m
			case protocol.RoutePosition:
				if full == nil || position != nil || m.Validate() != nil || m.Target != e.identity.Target {
					return protocol.ErrInvalidAttachment
				}
				position = &m
			case protocol.AttachmentActivated:
				if position == nil || m.Validate() != nil || m.RequestID != e.sequence || m.Identity.Target != e.identity.Target || full == nil || m.Epoch != full.Epoch || m.State != full.New || m.ViewPublication != full.Context.Publication || m.Identity != full.Context.Route {
					return protocol.ErrInvalidAttachment
				}
				activated = &m
				return nil
			case protocol.ErrorMsg, protocol.Detached:
				return protocol.ErrInvalidAttachment
			}
		}
	})
	if err != nil {
		return fail()
	}
	e.epoch = full.Epoch
	return e, activated, full, position
}

func (e *cachedAttachment) drainDormant() error {
	for {
		select {
		case r := <-e.inbox:
			if r.err != nil {
				return r.err
			}
			switch r.message.(type) {
			case protocol.Output:
				// Discard only pre-activation ordinary output.
			default:
				return protocol.ErrInvalidAttachment
			}
		default:
			return nil
		}
	}
}

// retire detaches a transport from coordinator bookkeeping and physically
// closes it outside the lock. The join runs in one tracked goroutine per
// entry, so eviction, replacement, and navigation never serialize a slow
// physical close and shutdown can bound the wait.
func (c *attachmentCoordinator) retire(e *cachedAttachment) {
	if e == nil {
		return
	}
	e.retireOnce.Do(func() {
		c.mu.Lock()
		c.retiring[e] = struct{}{}
		c.mu.Unlock()
		go func() {
			e.close()
			e.waitDormant()
			if e.readerStarted {
				<-e.readerDone
			}
			close(e.retireDone)
			c.mu.Lock()
			delete(c.retiring, e)
			c.mu.Unlock()
		}()
	})
}

// close is terminal: it rejects later suspension or insertion, detaches every
// dormant entry and in-flight retirement, signals all physical closes before
// joining any of them, and then performs one clock-bounded join.
func (c *attachmentCoordinator) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	dormant := make([]*cachedAttachment, 0, len(c.entries))
	for _, e := range c.entries {
		// Clear the LRU link so a dormant worker that observes the close never
		// unlinks a stale element from the freshly reset list.
		e.element = nil
		dormant = append(dormant, e)
	}
	c.entries = make(map[string]*cachedAttachment)
	c.lru.Init()
	c.mu.Unlock()
	for _, e := range dormant {
		c.retire(e)
	}
	c.joinRetirements()
}

// joinRetirements waits for every tracked retirement within one bound. It
// allocates no timer when nothing is retiring, so a quiet shutdown produces no
// clock traffic. It never holds the coordinator lock while waiting.
func (c *attachmentCoordinator) joinRetirements() {
	c.mu.Lock()
	pending := c.snapshotRetiringLocked()
	c.mu.Unlock()
	if len(pending) == 0 {
		return
	}
	timer := c.clock.NewTimer(attachmentRetirementJoinTimeout)
	if timer == nil {
		timer = systemClock{}.NewTimer(attachmentRetirementJoinTimeout)
	}
	defer timer.Stop()
	for {
		for _, e := range pending {
			select {
			case <-e.retireDone:
			case <-timer.C():
				return
			}
		}
		c.mu.Lock()
		pending = c.snapshotRetiringLocked()
		c.mu.Unlock()
		if len(pending) == 0 {
			return
		}
	}
}

func (c *attachmentCoordinator) snapshotRetiringLocked() []*cachedAttachment {
	pending := make([]*cachedAttachment, 0, len(c.retiring))
	for e := range c.retiring {
		pending = append(pending, e)
	}
	return pending
}
