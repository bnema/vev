package client

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

var errHandshakeTimeout = errors.New("handshake timed out")

type systemClock struct{}
type systemTimer struct{ *time.Timer }

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTimer(delay time.Duration) ports.Timer {
	return systemTimer{Timer: time.NewTimer(delay)}
}
func (t systemTimer) C() <-chan time.Time        { return t.Timer.C }
func (t systemTimer) Reset(d time.Duration) bool { return t.Timer.Reset(d) }
func (t systemTimer) Stop() bool                 { return t.Timer.Stop() }

// newHandshakeContext owns one deadline for the complete outbound handshake.
// The caller must stop it before entering the long-lived connection loop.
func newHandshakeContext(parent context.Context, clock ports.Clock) (context.Context, <-chan struct{}, func()) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	timedOut := make(chan struct{})
	if clock == nil {
		clock = systemClock{}
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
// watchHandshakeTransport closes the transport when the context is canceled.
// It returns an idempotent cleanup function that stops and waits for the watcher.
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

// boundedHandshakeOperation runs a transport operation until it completes or the context ends.
// It returns the operation error, or the relevant context or transition error when the operation is interrupted.
func boundedHandshakeOperation(ctx context.Context, transport ports.Transport, operation func() error) error {
	return boundedHandshakeOperationWithTransition(ctx, transport, operation, nil)
}

// boundedHandshakeOperationWithTransition runs a handshake operation while monitoring
// context cancellation and UI transition updates. It closes the transport when the
// context ends or a transition update fails, and returns the operation, context, or
// transition error.
func boundedHandshakeOperationWithTransition(ctx context.Context, transport ports.Transport, operation func() error, transition *transitionUI) error {
	if err := ctx.Err(); err != nil {
		_ = transport.Close()
		return err
	}
	completed := make(chan error, 1)
	go func() { completed <- operation() }()
	for {
		select {
		case err := <-completed:
			if ctxErr := ctx.Err(); ctxErr != nil {
				_ = transport.Close()
				return ctxErr
			}
			return err
		case <-transition.tickC():
			if err := transition.advance(); err != nil {
				_ = transport.Close()
				return err
			}
		case <-ctx.Done():
			_ = transport.Close()
			// The result channel is buffered, so the operation can publish its
			// completion after Close without keeping this cancellation path stuck.
			return ctx.Err()
		}
	}
}

// boundedDial dials a transport within the context's lifetime.
func boundedDial(ctx context.Context, dialer ports.Dialer) (ports.Transport, error) {
	return boundedDialWithTransition(ctx, dialer, nil)
}

// boundedDialWithTransition dials a transport while advancing the optional transition UI.
// It abandons and closes transports when the context or transition ends before dialing completes.
// A successful transport remains usable after the dialing context is canceled.
//
// The returned error reports dialing, context, or transition failure.
func boundedDialWithTransition(ctx context.Context, dialer ports.Dialer, transition *transitionUI) (ports.Transport, error) {
	// The dial context only bounds startup. A successful carriage outlives the
	// handshake context; canceling that context after Welcome must not kill SSH.
	dialCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	type dialResult struct {
		transport ports.Transport
		err       error
	}
	result := make(chan dialResult, 1)
	go func() {
		transport, err := dialer.Dial(dialCtx)
		result <- dialResult{transport: transport, err: err}
	}()
	abandon := func() {
		cancel()
		go func() {
			late := <-result
			if late.transport != nil {
				_ = late.transport.Close()
			}
		}()
	}
	for {
		select {
		case result := <-result:
			if err := ctx.Err(); err != nil {
				cancel()
				if result.transport != nil {
					_ = result.transport.Close()
				}
				return nil, err
			}
			if result.err != nil {
				cancel()
			}
			return result.transport, result.err
		case <-transition.tickC():
			if err := transition.advance(); err != nil {
				abandon()
				return nil, err
			}
		case <-ctx.Done():
			abandon()
			return nil, ctx.Err()
		}
	}
}

func handshakeContextError(parent context.Context, timedOut <-chan struct{}, fallback error) error {
	select {
	case <-timedOut:
		return errors.Join(errHandshakeTimeout, context.DeadlineExceeded)
	default:
	}
	if err := parent.Err(); err != nil {
		return err
	}
	return fallback
}
