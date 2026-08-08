package daemon

import (
	"context"
	"errors"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

var errHandshakeTimeout = errors.New("handshake timed out")

// newHandshakeContext owns one deadline for the complete inbound handshake.
// The caller must stop it before entering the long-lived connection loop.
func (d *Daemon) newHandshakeContext(parent context.Context) (context.Context, <-chan struct{}, func()) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	timedOut := make(chan struct{})
	clock := ports.Clock(systemClock{})
	if d != nil && d.clock != nil {
		clock = d.clock
	}
	timer := clock.NewTimer(ports.HandshakeTimeout)
	if timer == nil {
		timer = systemClock{}.NewTimer(ports.HandshakeTimeout)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-timer.C():
			close(timedOut)
			cancel()
		case <-stop:
		}
	}()
	var once sync.Once
	finish := func() {
		once.Do(func() {
			close(stop)
			<-done
			timer.Stop()
			cancel()
		})
	}
	return ctx, timedOut, finish
}

// watchHandshakeTransport closes a transport when the handshake context ends.
// Transport.Close is required to interrupt blocked Send and Recv operations.
func watchHandshakeTransport(ctx context.Context, transport ports.Transport) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			_ = transport.Close()
		case <-stop:
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
}

// boundedHandshakeOperation waits for one transport operation while the shared
// handshake context remains live. Closing the transport is the interruption
// mechanism because ports.Transport methods do not accept contexts.
func boundedHandshakeOperation(ctx context.Context, transport ports.Transport, operation func() error) error {
	if err := ctx.Err(); err != nil {
		_ = transport.Close()
		return err
	}
	completed := make(chan error, 1)
	go func() { completed <- operation() }()
	select {
	case err := <-completed:
		if ctxErr := ctx.Err(); ctxErr != nil {
			_ = transport.Close()
			return ctxErr
		}
		return err
	case <-ctx.Done():
		_ = transport.Close()
		// The result channel is buffered, so the operation can publish its
		// completion after Close without keeping this cancellation path stuck.
		return ctx.Err()
	}
}

func handshakeContextError(parent context.Context, timedOut <-chan struct{}, fallback error) error {
	select {
	case <-timedOut:
		return errors.Join(errHandshakeTimeout, context.DeadlineExceeded)
	default:
	}
	if parent == nil {
		return fallback
	}
	if err := parent.Err(); err != nil {
		return err
	}
	return fallback
}

// failHandshakeAttachment synchronously retires the exact connection admitted
// by route. A fresh route-owned session is purged only after its attachment is
// removed; existing sessions and their unrelated attachments are preserved.
func (d *Daemon) failHandshakeAttachment(sess *session, ac *attachedClient, tr ports.Transport, welcomed bool) {
	if sess == nil || ac == nil {
		if tr != nil {
			_ = tr.Close()
		}
		return
	}
	if d.abortResumeClaim(ac) {
		// abortResumeClaim closes the replacement transport and restores the
		// original parked credential. Closing the exact handshake link again is
		// harmless for transports and makes this boundary synchronous.
		if tr != nil {
			_ = tr.Close()
		}
		return
	}
	if welcomed {
		d.clientGone(sess, ac, tr, true)
	} else {
		d.clientGoneWithoutNotice(sess, ac, tr, true)
	}
	if tr != nil {
		_ = tr.Close()
	}
	if ac.routeCreatedSession {
		sess.mu.Lock()
		empty := len(sess.attachments) == 0
		sess.mu.Unlock()
		if empty {
			// killSession revalidates the attachment set under its publication
			// locks before removing the session.
			_ = d.killSession(sess, ports.ReasonSessionKilled, ac.routeSessionPurge)
		}
	}
}
