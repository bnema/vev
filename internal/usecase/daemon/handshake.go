package daemon

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

var errHandshakeTimeout = errors.New("handshake timed out")

// acceptedHandshakeDeadline returns the absolute handshake deadline an accepted
// connection already owns, or the zero time when the connection exposes no
// deadline seam and the daemon must start a fresh budget. The seam is asserted
// structurally so the daemon neither imports the mux/sessionwire adapters nor
// widens the ServerConnection contract.
func acceptedHandshakeDeadline(tr ports.ServerConnection) time.Time {
	provider, ok := tr.(ports.HandshakeDeadlineProvider)
	if !ok {
		return time.Time{}
	}
	return provider.HandshakeDeadline()
}

// newHandshakeContext owns one deadline for the complete inbound handshake.
// acceptedDeadline is the absolute deadline an accepted connection already
// owns; the zero time starts a fresh protocol.HandshakeTimeout budget, which
// preserves the behavior of ordinary connections lacking the optional seam.
// A non-zero acceptedDeadline is adopted verbatim: the timer covers only the
// time the connection has left, so queue delay is consumed instead of
// restarting the budget, and an already-elapsed deadline expires immediately.
// The caller must stop it before entering the long-lived connection loop.
func (d *Daemon) newHandshakeContext(parent context.Context, acceptedDeadline time.Time) (context.Context, <-chan struct{}, func()) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	timedOut := make(chan struct{})
	clock := ports.Clock(systemClock{})
	if d != nil && d.clock != nil {
		clock = d.clock
	}
	budget := protocol.HandshakeTimeout
	if !acceptedDeadline.IsZero() {
		budget = acceptedDeadline.Sub(clock.Now())
	}
	if budget <= 0 {
		// The accepted connection already spent its whole budget before the
		// daemon reached it. Expire now rather than start a second budget; the
		// closed timedOut channel keeps handshakeContextError reporting a
		// deadline rather than a bare cancellation.
		close(timedOut)
		cancel()
		return ctx, timedOut, cancel
	}
	timer := clock.NewTimer(budget)
	if timer == nil {
		timer = systemClock{}.NewTimer(budget)
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
func watchHandshakeTransport(ctx context.Context, transport ports.ServerConnection) func() {
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
// mechanism because ServerConnection methods do not accept contexts.
func boundedHandshakeOperation(ctx context.Context, transport ports.ServerConnection, operation func() error) error {
	_, err := boundedHandshakeOperationTracked(ctx, transport, operation)
	return err
}

// boundedHandshakeOperationTracked also returns a completion signal for the
// operation worker. Callers that own an effect gate must wait for this signal
// before releasing the gate after cancellation; the transport operation itself
// may still be unwinding after Close interrupts it.
func boundedHandshakeOperationTracked(ctx context.Context, transport ports.ServerConnection, operation func() error) (<-chan struct{}, error) {
	done := make(chan struct{})
	if err := ctx.Err(); err != nil {
		close(done)
		_ = transport.Close()
		return done, err
	}
	completed := make(chan error, 1)
	go func() {
		defer close(done)
		completed <- operation()
	}()
	select {
	case err := <-completed:
		if ctxErr := ctx.Err(); ctxErr != nil {
			_ = transport.Close()
			return done, ctxErr
		}
		return done, err
	case <-ctx.Done():
		_ = transport.Close()
		// The result channel is buffered, so the operation can publish its
		// completion after Close without keeping this cancellation path stuck.
		return done, ctx.Err()
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
func (d *Daemon) failHandshakeAttachment(sess *session, ac *attachedClient, tr ports.ServerConnection, welcomed bool) {
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
		_ = d.killSessionIfEmpty(sess, protocol.ReasonSessionKilled, ac.routeSessionPurge)
	}
}
