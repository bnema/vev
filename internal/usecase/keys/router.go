// Package keys routes terminal input bytes into vev Alt/ESC bindings or PTY
// passthrough bytes.
package keys

import (
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const (
	ESC      byte = 0x1b
	ESCDelay      = 40 * time.Millisecond
)

// Action is an intercepted vev key binding.
type Action int

const (
	ActionOpenPalette Action = iota
	ActionSwitchTab1
	ActionSwitchTab2
	ActionSwitchTab3
	ActionSwitchTab4
	ActionSwitchTab5
	ActionSwitchTab6
	ActionSwitchTab7
	ActionSwitchTab8
	ActionSwitchTab9
)

// Handler receives router outputs. Forward is called only for bytes that should
// reach the PTY; Action is called for intercepted bindings.
type Handler interface {
	Forward([]byte)
	Action(Action)
}

// Router implements vev's Alt/ESC input routing. It retains a trailing ESC for
// ESCDelay so split Alt-key sequences can still be intercepted without delaying
// known terminal control prefixes (ESC [ and ESC O), which pass through.
type Router struct {
	clock ports.Clock
	delay time.Duration
	h     Handler

	mu          sync.Mutex
	pending     bool
	timer       ports.Timer
	pendingDone chan struct{}
}

func NewRouter(clock ports.Clock, h Handler) *Router {
	return &Router{clock: clock, delay: ESCDelay, h: h}
}

// Route routes one transport read.
func (r *Router) Route(data []byte) {
	if len(data) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending {
		r.stopTimer()
		r.pending = false
		if consumed := r.routeAfterPendingESC(data); consumed > 0 {
			data = data[consumed:]
		}
	}
	r.route(data)
}

func (r *Router) route(data []byte) {
	buf := make([]byte, 0, len(data))
	flush := func() {
		if len(buf) == 0 {
			return
		}
		r.forward(buf)
		buf = buf[:0]
	}
	for i := 0; i < len(data); {
		if data[i] != ESC {
			buf = append(buf, data[i])
			i++
			continue
		}
		if i == len(data)-1 {
			flush()
			r.retainESC()
			return
		}
		next := data[i+1]
		if passThroughPrefix(next) {
			buf = append(buf, ESC, next)
			i += 2
			continue
		}
		if action, ok := binding(next); ok {
			flush()
			r.h.Action(action)
			i += 2
			continue
		}
		buf = append(buf, ESC)
		i++
	}
	flush()
}

func (r *Router) routeAfterPendingESC(data []byte) int {
	next := data[0]
	if passThroughPrefix(next) {
		r.forward([]byte{ESC, next})
		return 1
	}
	if action, ok := binding(next); ok {
		r.h.Action(action)
		return 1
	}
	r.forward([]byte{ESC})
	return 0
}

func (r *Router) retainESC() {
	r.pending = true
	r.timer = r.clock.NewTimer(r.delay)
	r.pendingDone = make(chan struct{})
	go func(timer ports.Timer, done <-chan struct{}) {
		select {
		case <-timer.C():
		case <-done:
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.pending && r.timer == timer {
			r.pending = false
			r.forward([]byte{ESC})
		}
	}(r.timer, r.pendingDone)
}

func (r *Router) stopTimer() {
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	if r.pendingDone != nil {
		close(r.pendingDone)
		r.pendingDone = nil
	}
}

func (r *Router) forward(data []byte) {
	cp := append([]byte(nil), data...)
	r.h.Forward(cp)
}

func passThroughPrefix(b byte) bool { return b == '[' || b == 'O' }

func binding(b byte) (Action, bool) {
	switch b {
	case ' ':
		return ActionOpenPalette, true
	case '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return ActionSwitchTab1 + Action(b-'1'), true
	default:
		return 0, false
	}
}
