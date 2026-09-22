package client

import (
	"context"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

type systemClock struct{}
type systemTimer struct{ timer *time.Timer }

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTimer(delay time.Duration) ports.Timer {
	return systemTimer{timer: time.NewTimer(delay)}
}
func (t systemTimer) C() <-chan time.Time        { return t.timer.C }
func (t systemTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }
func (t systemTimer) Stop() bool                 { return t.timer.Stop() }

// newBoundedContext owns one deadline of the requested length. The caller must
// invoke finish when the bounded operation ends.
func newBoundedContext(parent context.Context, clock ports.Clock, timeout time.Duration) (context.Context, <-chan struct{}, func()) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	timedOut := make(chan struct{})
	if clock == nil {
		clock = systemClock{}
	}
	timer := clock.NewTimer(timeout)
	if timer == nil {
		timer = systemClock{}.NewTimer(timeout)
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
