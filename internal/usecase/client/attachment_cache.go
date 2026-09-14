package client

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// DefaultAttachmentCacheTTL retains a suspended remote attachment for 15 minutes.
const DefaultAttachmentCacheTTL = 15 * time.Minute

// attachmentCoordinator owns transports, not endpoint resolution or route history.
// Dormant workers may evict their own entry but own no terminal or route state.
type attachmentCoordinator struct {
	mu      sync.Mutex
	clock   ports.Clock
	ttl     time.Duration
	entries map[string]*cachedAttachment
}

type cachedAttachment struct {
	transport     ports.ClientConnection
	inbox         chan recvResult
	done          chan struct{}
	readerDone    chan struct{}
	readerStarted bool
	once          sync.Once
	closeErr      error
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

func newAttachmentCoordinator(clock ports.Clock, ttl time.Duration) *attachmentCoordinator {
	if clock == nil {
		clock = systemClock{}
	}
	if ttl == 0 {
		ttl = DefaultAttachmentCacheTTL
	}
	return &attachmentCoordinator{clock: clock, ttl: ttl, entries: make(map[string]*cachedAttachment)}
}

func newCachedAttachment(transport ports.ClientConnection) *cachedAttachment {
	return &cachedAttachment{transport: transport, inbox: make(chan recvResult, 1), done: make(chan struct{}), readerDone: make(chan struct{})}
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

func (e *cachedAttachment) close() {
	e.once.Do(func() { close(e.done); e.closeErr = e.transport.Close() })
}

func (e *cachedAttachment) release() {
	e.close()
	e.waitDormant()
	if e.readerStarted {
		<-e.readerDone
	}
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

func (c *attachmentCoordinator) suspend(ctx context.Context, e *cachedAttachment, request AttachRequest, result attachResult) bool {
	if c.ttl < 0 || !request.Remote || request.OriginKey == "" || result.committedIdentity == nil {
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
		e.close()
		return false
	}
	key := request.OriginKey
	c.mu.Lock()
	old := c.entries[key]
	delete(c.entries, key)
	c.mu.Unlock()
	if old != nil {
		old.release()
	}
	e.expires = c.clock.Now().Add(c.ttl)
	e.timer = c.clock.NewTimer(c.ttl)
	e.stopDormant = make(chan struct{})
	e.dormantDone = make(chan struct{})
	c.mu.Lock()
	c.entries[key] = e
	c.mu.Unlock()
	go func() {
		defer close(e.dormantDone)
		defer func() {
			select {
			case <-e.done:
				<-e.readerDone
				c.mu.Lock()
				if c.entries[key] == e {
					delete(c.entries, key)
				}
				c.mu.Unlock()
			default:
			}
		}()
		defer e.timer.Stop()
		for {
			select {
			case <-e.done:
				return
			case <-e.stopDormant:
				return
			case <-e.timer.C():
				e.close()
				return
			case r := <-e.inbox:
				if r.err != nil {
					e.close()
					return
				}
				switch r.message.(type) {
				case protocol.Detached, protocol.ErrorMsg:
					e.close()
					return
				}
			}
		}
	}()
	return true
}

func (e *cachedAttachment) waitDormant() {
	if e.dormantDone != nil {
		<-e.dormantDone
	}
}

func (c *attachmentCoordinator) activate(ctx context.Context, request AttachRequest, geometry domain.Geometry) (*cachedAttachment, *protocol.AttachmentActivated, *protocol.Output, *protocol.RoutePosition) {
	c.mu.Lock()
	e := c.entries[request.OriginKey]
	delete(c.entries, request.OriginKey)
	c.mu.Unlock()
	if e == nil {
		return nil, nil, nil, nil
	}
	close(e.stopDormant)
	e.waitDormant()
	fail := func() (*cachedAttachment, *protocol.AttachmentActivated, *protocol.Output, *protocol.RoutePosition) {
		e.release()
		return nil, nil, nil, nil
	}
	select {
	case <-e.done:
		return fail()
	default:
	}
	if !c.clock.Now().Before(e.expires) || !request.Remote || request.Intent == protocol.IntentNew || request.SessionName != e.identity.Target.SessionName || request.EnvironmentPolicy != e.request.EnvironmentPolicy || !slices.Equal(request.Environment, e.request.Environment) {
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
	err := c.transition(ctx, e, func(ctx context.Context) error {
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

func (c *attachmentCoordinator) has(endpoint string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[endpoint] != nil
}

func (c *attachmentCoordinator) close() {
	c.mu.Lock()
	entries := c.entries
	c.entries = make(map[string]*cachedAttachment)
	c.mu.Unlock()
	for _, e := range entries {
		e.release()
	}
}
