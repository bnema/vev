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
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		_ = transport.Close()
		<-completed
		return ctx.Err()
	}
}

func boundedDial(ctx context.Context, dialer ports.Dialer) (ports.Transport, error) {
	result := make(chan struct {
		transport ports.Transport
		err       error
	}, 1)
	go func() {
		transport, err := dialer.Dial(ctx)
		if ctx.Err() != nil && transport != nil {
			_ = transport.Close()
		}
		result <- struct {
			transport ports.Transport
			err       error
		}{transport: transport, err: err}
	}()
	select {
	case result := <-result:
		return result.transport, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
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
