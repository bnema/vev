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
		if ctx.Err() != nil {
			_ = transport.Close()
			<-completed
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		_ = transport.Close()
		<-completed
		return ctx.Err()
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
