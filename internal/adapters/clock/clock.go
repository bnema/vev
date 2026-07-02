// Package clock provides the production ports.Clock: a thin wrapper over the
// standard library's time package. Usecases depend on ports.Clock so their
// time-dependent logic (the daemon's debounced render scheduler) can be driven
// deterministically in tests via mocks, while the app wires this real clock.
package clock

import (
	"time"

	"github.com/bnema/vev/internal/ports"
)

// realClock implements ports.Clock over the wall clock.
type realClock struct{}

// New returns the production ports.Clock.
func New() ports.Clock { return realClock{} }

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) ports.Timer {
	return &realTimer{t: time.NewTimer(d)}
}

// realTimer adapts *time.Timer to ports.Timer.
type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
