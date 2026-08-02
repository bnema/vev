package daemon

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// proxySession is the local attachment identity for one structured remote
// session key. mu is a leaf lock: callers may take it below d.mu or sessionCore
// locks, but must not acquire another architecture lock or perform transport I/O
// while holding it. All Transport.Send calls are owned by sendMu.
type proxySession struct {
	sessionCore
	key domain.RemoteSessionKey

	mu             sync.Mutex
	generation     uint64
	linkGeneration uint64
	transport      ports.Transport
	resumeToken    uint64
	appliedState   uint64
	screenReady    bool
	resetRequested bool
	screen         *proxyScreenState
	meta           ports.SessionMeta
	attentionAt    []time.Time
	linkState      ports.LinkState
	expired        bool
	warm           *proxyWarmTimer
	cancel         context.CancelFunc
	done           chan struct{}
	doneOnce       sync.Once
	clientID       [16]byte
	contentSize    domain.Size

	sendMu    sync.Mutex
	inputNext uint64
	// commandMu permits at most one interactive palette request to be
	// outstanding without blocking input, ACK, resize, or liveness sends.
	commandMu      sync.Mutex
	commandNext    uint64
	commandPending map[uint64]proxyCommandPending
}

func (p *proxySession) core() *sessionCore {
	if p == nil {
		return nil
	}
	return &p.sessionCore
}

func (p *proxySession) isProxy() bool { return true }

func (p *proxySession) sessionMetaSnapshot() (ports.SessionMeta, bool) {
	if p == nil {
		return ports.SessionMeta{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.meta.Tabs) == 0 || int(p.meta.Active) >= len(p.meta.Tabs) {
		return ports.SessionMeta{}, false
	}
	return cloneSessionMeta(p.meta), true
}

// replaceSessionMetaLocked atomically replaces the relayed snapshot while
// preserving the first-observed timestamp of tabs that remain attentive.
// Callers must validate meta and hold p.mu.
func (p *proxySession) replaceSessionMetaLocked(meta ports.SessionMeta, now time.Time) {
	attentionAt := make([]time.Time, len(meta.Tabs))
	for i, tab := range meta.Tabs {
		if !tab.Attention {
			continue
		}
		if i < len(p.meta.Tabs) && p.meta.Tabs[i].Attention && i < len(p.attentionAt) {
			attentionAt[i] = p.attentionAt[i]
			continue
		}
		attentionAt[i] = now
	}
	p.meta = cloneSessionMeta(meta)
	p.attentionAt = attentionAt
}

// applyProxySessionMeta validates and atomically applies relayed metadata only
// for the exact current link generation. A remote session rename cannot mutate
// the immutable local identity, so it expires that exact proxy instead.
func (d *Daemon) applyProxySessionMeta(p *proxySession, generation uint64, meta ports.SessionMeta) (bool, error) {
	if d == nil || p == nil {
		return false, errors.New("proxy session: metadata target is unavailable")
	}
	if _, err := ports.MarshalSessionMeta(meta); err != nil {
		return false, fmt.Errorf("proxy session: invalid metadata: %w", err)
	}
	meta = cloneSessionMeta(meta)
	now := d.clock.Now()

	p.mu.Lock()
	if p.linkGeneration != generation {
		p.mu.Unlock()
		return false, nil
	}
	if meta.SessionName != p.key.Name {
		transport := p.transport
		p.mu.Unlock()
		if transport != nil {
			d.markProxyReplaced(p, generation, transport)
		}
		return false, fmt.Errorf("proxy session: metadata identity mismatch: got %q", meta.SessionName)
	}
	p.replaceSessionMetaLocked(meta, now)
	p.mu.Unlock()

	d.pokeAttentionTicker()
	p.sessionCore.mu.Lock()
	ac := p.client
	p.sessionCore.mu.Unlock()
	if ac != nil {
		d.invalidateRender(p, ac, false, "proxy_session.go")
	}
	return true, nil
}

func (p *proxySession) snapshotView(opts viewOptions) sessionView {
	if p == nil {
		return sessionView{}
	}
	p.mu.Lock()
	meta := cloneSessionMeta(p.meta)
	attentionAt := append([]time.Time(nil), p.attentionAt...)
	name := p.name
	expired := p.expired
	if expired {
		name += " [expired]"
	}
	p.mu.Unlock()

	view := sessionView{
		id:                p.id,
		name:              name,
		expired:           expired,
		createdAt:         p.createdAt,
		mruAt:             p.mruAt.Load(),
		active:            int(meta.Active),
		tabCount:          len(meta.Tabs),
		cannotAcceptMoves: true,
	}
	p.sessionCore.mu.Lock()
	view.attached = p.client != nil
	p.sessionCore.mu.Unlock()
	if opts.tabDetails {
		view.tabs = make([]tabView, len(meta.Tabs))
		for i, tab := range meta.Tabs {
			view.tabs[i] = tabView{name: tab.Name, attention: tab.Attention}
			if i < len(attentionAt) {
				view.tabs[i].attentionAt = attentionAt[i]
			}
			view.hasAttention = view.hasAttention || tab.Attention
		}
	} else {
		for _, tab := range meta.Tabs {
			view.hasAttention = view.hasAttention || tab.Attention
		}
	}
	return view
}

func (p *proxySession) statusSegments(_ bool) statusSnapshot {
	if p == nil {
		return statusSnapshot{}
	}
	name := p.lifecycleDisplayName()
	p.mu.Lock()
	meta := cloneSessionMeta(p.meta)
	p.mu.Unlock()
	snapshot := statusSnapshot{session: name, tabs: make([]statusTab, len(meta.Tabs))}
	for i, tab := range meta.Tabs {
		snapshot.tabs[i] = statusTab{
			name:      tab.Name,
			active:    uint16(i) == meta.Active,
			attention: tab.Attention,
		}
	}
	return snapshot
}

func (p *proxySession) activateTargetLocked(tabIndex int) bool {
	if tabIndex < 0 {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return tabIndex < len(p.meta.Tabs)
}

func newProxySession(key domain.RemoteSessionKey, size domain.Size) (*proxySession, error) {
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("proxy session key: %w", err)
	}
	content := contentSize(size, false)
	if size.Valid() && !validProxyScreenSize(content) {
		return nil, fmt.Errorf("proxy session size: invalid content size %dx%d", content.Cols, content.Rows)
	}
	var clientID [16]byte
	if _, err := io.ReadFull(rand.Reader, clientID[:]); err != nil {
		return nil, fmt.Errorf("generate proxy client id: %w", err)
	}
	p := &proxySession{
		sessionCore: sessionCore{
			id:   key.ID(),
			name: key.Display(),
			caps: sessionCapabilities{cannotAcceptMoves: true, cannotYieldMoves: true},
		},
		key:            key,
		generation:     1,
		screen:         newProxyScreenState(content),
		linkState:      ports.LinkStateConnected,
		done:           make(chan struct{}),
		clientID:       clientID,
		contentSize:    content,
		commandPending: make(map[uint64]proxyCommandPending),
	}
	return p, nil
}

type proxyConstruction struct {
	done chan struct{}
	err  error
}

var errProxySessionExpired = errors.New("proxy session: remote attachment has expired")

// openProxySession returns the exact already-published proxy for key, or elects
// one constructor for that structured key. Waiters never dial: they wait
// cancellably, then revalidate the exact registry winner under d.mu. The
// reservation itself is held only under d.mu; dial and handshake I/O run after
// releasing every daemon, core, and proxy architecture lock.
func (d *Daemon) openProxySession(ctx context.Context, key domain.RemoteSessionKey, size domain.Size) (*proxySession, error) {
	if d == nil {
		return nil, errors.New("proxy session: nil daemon")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("proxy session: %w", err)
	}
	if d.remoteDialerFactory == nil {
		return nil, errors.New("proxy session: remote dialer is not configured")
	}

	for {
		d.mu.Lock()
		existing, found, err := d.proxyForKeyLocked(key)
		if err != nil {
			d.mu.Unlock()
			return nil, err
		}
		if found {
			d.mu.Unlock()
			return existing, nil
		}
		if d.closing {
			d.mu.Unlock()
			return nil, errors.New("proxy session: daemon is shutting down")
		}
		if construction := d.proxyConstructions[key]; construction != nil {
			done := construction.done
			d.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			// Publication and construction completion are ordered under d.mu.
			// Revalidate rather than trusting a stale candidate pointer.
			d.mu.Lock()
			winner, registered, collisionErr := d.proxyForKeyLocked(key)
			constructionErr := construction.err
			d.mu.Unlock()
			if collisionErr != nil {
				return nil, collisionErr
			}
			if registered {
				return winner, nil
			}
			if constructionErr != nil &&
				!errors.Is(constructionErr, context.Canceled) &&
				!errors.Is(constructionErr, context.DeadlineExceeded) {
				return nil, constructionErr
			}
			// A canceled owner does not poison independent waiters. Re-elect
			// under d.mu if this caller still wants the proxy.
			continue
		}

		construction := &proxyConstruction{done: make(chan struct{})}
		if d.proxyConstructions == nil {
			d.proxyConstructions = make(map[domain.RemoteSessionKey]*proxyConstruction)
		}
		d.proxyConstructions[key] = construction
		d.mu.Unlock()

		proxy, constructionErr := d.constructProxySession(ctx, key, size)
		d.mu.Lock()
		construction.err = constructionErr
		if d.proxyConstructions[key] == construction {
			delete(d.proxyConstructions, key)
			close(construction.done)
		}
		d.mu.Unlock()
		return proxy, constructionErr
	}
}

// proxyForKeyLocked validates the opaque session-ID collision while d.mu is
// held. A true result is always the exact currently registered proxy.
func (d *Daemon) proxyForKeyLocked(key domain.RemoteSessionKey) (*proxySession, bool, error) {
	existing := d.sessions[key.ID()]
	if existing == nil {
		return nil, false, nil
	}
	proxy, ok := existing.(*proxySession)
	if !ok || proxy.key != key {
		return nil, false, errors.New("proxy session: remote identity collision")
	}
	proxy.mu.Lock()
	expired := proxy.expired
	proxy.mu.Unlock()
	if expired {
		return nil, false, errProxySessionExpired
	}
	return proxy, true, nil
}

// constructProxySession owns one reserved key. The caller has released d.mu
// before entering this function, and no architecture lock spans handshake I/O.
func (d *Daemon) constructProxySession(ctx context.Context, key domain.RemoteSessionKey, size domain.Size) (*proxySession, error) {
	proxy, err := newProxySession(key, size)
	if err != nil {
		return nil, err
	}
	root := context.Background()
	if d.serveCtx != nil {
		root = d.serveCtx
	}
	linkCtx, cancel := context.WithCancel(root)
	proxy.cancel = cancel
	published := false
	defer func() {
		if !published {
			proxy.disposeUnpublished()
		}
	}()

	if err := d.dialProxyHandshake(ctx, proxy, ports.IntentAttach); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	d.mu.Lock()
	if err := ctx.Err(); err != nil {
		d.mu.Unlock()
		return nil, err
	}
	if existing, found, err := d.proxyForKeyLocked(key); err != nil {
		d.mu.Unlock()
		return nil, err
	} else if found {
		d.mu.Unlock()
		return existing, nil
	}
	if d.closing || !d.registerSessionLocked(proxy) {
		d.mu.Unlock()
		return nil, errors.New("proxy session: daemon rejected publication")
	}
	published = true
	d.mu.Unlock()

	go d.runProxyLink(linkCtx, proxy)
	return proxy, nil
}

func (p *proxySession) finish() {
	if p != nil {
		p.doneOnce.Do(func() { close(p.done) })
	}
}

func (p *proxySession) disposeUnpublished() {
	if p == nil {
		return
	}
	p.stop()
	p.finish()
}

func (p *proxySession) stop() {
	if p == nil {
		return
	}
	if p.cancel != nil {
		p.cancel()
	}
	p.closeCurrentTransport()
}

func (p *proxySession) closeCurrentTransport() {
	if p == nil {
		return
	}
	p.mu.Lock()
	transport := p.transport
	p.mu.Unlock()
	if transport != nil {
		_ = transport.Close()
	}
}

var _ attachmentSession = (*proxySession)(nil)
