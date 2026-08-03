package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/picker"
)

const proxyKillTimeout = 10 * time.Second

var (
	errProxyKillTimeout      = errors.New("proxy session: remote kill timed out")
	errProxyKillGeneration   = errors.New("proxy session: remote kill lifecycle changed")
	errProxyKillConfirmation = errors.New("type the full remote session label exactly")
)

type proxyKillIOResult struct {
	frame ports.Frame
	err   error
}

// enterRemoteKillConfirmation turns a remote picker deletion into an exact,
// typed lifecycle capability. The proxy pointer is resolved when the prompt is
// opened, while its generation is deliberately captured only after the exact
// label is submitted.
func (d *Daemon) enterRemoteKillConfirmation(owner attachmentSession, ac *attachedClient, target picker.Target) bool {
	if d == nil || owner == nil || ac == nil || target.RemoteKey == nil || target.Session != target.RemoteKey.ID() {
		return false
	}
	key := *target.RemoteKey
	d.mu.Lock()
	proxy, ok := d.sessions[target.Session].(*proxySession)
	ok = ok && proxy.key == key
	d.mu.Unlock()
	if !ok {
		return false
	}
	title := " Type " + key.Display() + " to kill "
	proxy.mu.Lock()
	unreachable := proxy.expired || proxy.linkState == ports.LinkStateOffline || proxy.linkState == ports.LinkStateDead
	proxy.mu.Unlock()
	if unreachable {
		title = " Type " + key.Display() + " to kill (remote unreachable) "
	}
	d.enterTransitionPrompt(owner, ac, title, "", func(value string, token attachmentConnectionToken) error {
		if value != key.Display() {
			return errProxyKillConfirmation
		}
		if token.ac != nil {
			if !token.attachmentCurrent() {
				return errAttachmentTransition
			}
			token.effect.bindActionEnd(d, "remote-picker-delete")
			token.effect.End()
		}
		d.mu.Lock()
		if d.sessions[proxy.id] != proxy {
			d.mu.Unlock()
			return errProxyKillGeneration
		}
		proxy.mu.Lock()
		generation := proxy.generation
		proxy.mu.Unlock()
		d.mu.Unlock()
		ctx := d.serveCtx
		if ctx == nil {
			ctx = context.Background()
		}
		return d.killRemoteProxy(ctx, proxy, generation)
	})
	return true
}

// killRemoteProxy uses a fresh control transport and accepts only EOF as the
// remote daemon's authoritative kill acknowledgement. No live or cached state
// is changed before that acknowledgement and exact generation publication.
func (d *Daemon) killRemoteProxy(ctx context.Context, proxy *proxySession, generation uint64) error {
	if d == nil || proxy == nil || d.remoteDialerFactory == nil || d.clock == nil {
		return errors.New("proxy session: remote kill is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	d.mu.Lock()
	current := d.sessions[proxy.id] == proxy
	if current {
		proxy.mu.Lock()
		current = proxy.generation == generation
		proxy.mu.Unlock()
	}
	d.mu.Unlock()
	if !current {
		return errProxyKillGeneration
	}

	timer := d.clock.NewTimer(proxyKillTimeout)
	if timer == nil {
		return errProxyKillTimeout
	}
	defer timer.Stop()
	boundedCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	timedOut := make(chan struct{})
	stopTimeout := make(chan struct{})
	defer close(stopTimeout)
	go func() {
		select {
		case <-timer.C():
			close(timedOut)
			cancel()
		case <-stopTimeout:
		}
	}()

	dialer, err := d.remoteDialerFactory.DialerForRemote(proxy.key.Host, proxy.key.Name, d.remoteTransportMode, d.log)
	if err != nil {
		return fmt.Errorf("proxy session: select remote kill dialer: %w", err)
	}
	if dialer == nil {
		return errors.New("proxy session: remote kill dialer is nil")
	}
	transport, err := dialer.Dial(boundedCtx)
	if err != nil {
		return proxyKillContextError(ctx, timedOut, fmt.Errorf("proxy session: remote kill dial: %w", err))
	}
	if transport == nil {
		return errors.New("proxy session: remote kill transport is nil")
	}
	defer func() { _ = transport.Close() }()

	killFrame := ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{Name: proxy.key.Name})}
	sendDone := make(chan error, 1)
	go func() { sendDone <- transport.Send(killFrame) }()
	select {
	case err := <-sendDone:
		if err != nil {
			return fmt.Errorf("proxy session: send remote kill: %w", err)
		}
	case <-boundedCtx.Done():
		_ = transport.Close()
		// Transport promises Close unblocks Send, but this boundary cannot let a
		// broken adapter defeat the caller's bound. The buffered result lets the
		// caller return; a non-cooperative adapter may retain this one transport
		// goroutine until Send eventually exits. It owns no daemon locks or state.
		return proxyKillContextError(ctx, timedOut, boundedCtx.Err())
	}

	recvDone := make(chan proxyKillIOResult, 1)
	go func() {
		frame, err := transport.Recv()
		recvDone <- proxyKillIOResult{frame: frame, err: err}
	}()
	var received proxyKillIOResult
	select {
	case received = <-recvDone:
	case <-boundedCtx.Done():
		_ = transport.Close()
		// Apply the same bounded boundary defensively to Recv. A conforming
		// transport exits on Close; a broken one may retain this transport-owned
		// goroutine, but cannot delay or mutate the failed kill operation.
		return proxyKillContextError(ctx, timedOut, boundedCtx.Err())
	}
	if !errors.Is(received.err, io.EOF) {
		if received.err != nil {
			return fmt.Errorf("proxy session: receive remote kill result: %w", received.err)
		}
		if received.frame.Type == ports.MsgError {
			remoteErr, decodeErr := ports.UnmarshalErrorMsg(received.frame.Payload)
			if decodeErr != nil {
				return fmt.Errorf("proxy session: malformed remote kill error: %w", decodeErr)
			}
			return fmt.Errorf("proxy session: remote kill failed (%d): %s", remoteErr.Code, remoteErr.Text)
		}
		return fmt.Errorf("proxy session: unexpected remote kill reply type %d", received.frame.Type)
	}
	return d.publishRemoteProxyKill(proxy, generation)
}

func proxyKillContextError(parent context.Context, timedOut <-chan struct{}, fallback error) error {
	select {
	case <-timedOut:
		return errProxyKillTimeout
	default:
	}
	if err := parent.Err(); err != nil {
		return err
	}
	return fallback
}

// publishRemoteProxyKill atomically removes only the acknowledged proxy
// generation and matching discovery row. Teardown and persistence are external
// operations and therefore happen after every architecture lock is released.
func (d *Daemon) publishRemoteProxyKill(proxy *proxySession, generation uint64) error {
	d.mu.Lock()
	if d.sessions[proxy.id] != proxy {
		d.mu.Unlock()
		return errProxyKillGeneration
	}
	proxy.sessionCore.mu.Lock()
	proxy.mu.Lock()
	if proxy.generation != generation {
		proxy.mu.Unlock()
		proxy.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return errProxyKillGeneration
	}
	proxy.generation++
	proxy.expired = true
	warm := proxy.warm
	proxy.warm = nil
	cancel := proxy.cancel
	transport := proxy.transport
	coordinator := proxy.coordinator.Load()
	proxy.mu.Unlock()
	if !d.unregisterSessionLocked(proxy) {
		proxy.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return errProxyKillGeneration
	}
	proxy.sessionCore.mu.Unlock()

	cacheChanged := false
	d.remoteCatalog.mu.Lock()
	if entry, ok := d.remoteCatalog.cache[proxy.key.Host]; ok {
		sessions := make([]ports.RemoteCatalogSession, 0, len(entry.Sessions))
		for _, session := range entry.Sessions {
			if session.Name == proxy.key.Name {
				cacheChanged = true
				continue
			}
			sessions = append(sessions, session)
		}
		if cacheChanged {
			entry.Sessions = sessions
			d.remoteCatalog.cache[proxy.key.Host] = entry
		}
	}
	d.remoteCatalog.mu.Unlock()
	empty := len(d.sessions) == 0
	if empty {
		d.closing = true
	}
	d.mu.Unlock()

	warm.stop()
	if cancel != nil {
		cancel()
	}
	if transport != nil {
		_ = transport.Close()
	}
	if coordinator != nil {
		coordinator.beginSessionTeardown().finish()
		coordinator.waitForTimerWorkers()
	}
	proxy.finish()
	if cacheChanged {
		d.persistRemoteCatalogCache()
	}
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
	} else {
		d.refreshRemoteOpenPickers()
	}
	return nil
}
