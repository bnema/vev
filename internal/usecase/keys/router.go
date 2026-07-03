// Package keys routes terminal input bytes into vev Alt/ESC bindings or PTY
// passthrough bytes.
package keys

import (
	"sync"
	"time"
	"unicode/utf8"

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
	ActionJumpAttention
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
	pendingAlt  []byte
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
		pendingAlt := r.pendingAlt
		r.stopTimer()
		r.pending = false
		r.pendingAlt = nil
		combined := append(append([]byte(nil), pendingAlt...), data...)
		if consumed := r.routeAfterPendingESC(combined, len(pendingAlt)); consumed > len(pendingAlt) {
			data = data[consumed-len(pendingAlt):]
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
		remaining := data[i+1:]
		if action, size, ok := binding(remaining); ok {
			flush()
			r.h.Action(action)
			i += 1 + size
			continue
		}
		if partialUTF8Rune(remaining) {
			flush()
			r.retainESC(remaining)
			return
		}
		buf = append(buf, ESC)
		i++
	}
	flush()
}

func (r *Router) routeAfterPendingESC(data []byte, pendingAltLen int) int {
	next := data[0]
	if passThroughPrefix(next) {
		r.forward([]byte{ESC, next})
		return 1
	}
	if action, size, ok := binding(data); ok {
		r.h.Action(action)
		return size
	}
	if partialUTF8Rune(data) {
		r.retainESC(data)
		return len(data)
	}
	key, size := utf8.DecodeRune(data)
	if size > 1 {
		r.forward(append([]byte{ESC}, data[:size]...))
		return size
	}
	if key == utf8.RuneError && pendingAltLen > 0 {
		r.forward(append([]byte{ESC}, data[:pendingAltLen]...))
		return pendingAltLen
	}
	r.forward([]byte{ESC})
	return 0
}

func (r *Router) retainESC(altBytes ...[]byte) {
	r.pending = true
	r.pendingAlt = nil
	if len(altBytes) > 0 {
		r.pendingAlt = append([]byte(nil), altBytes[0]...)
	}
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
			data := append([]byte{ESC}, r.pendingAlt...)
			r.pendingAlt = nil
			r.forward(data)
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

func partialUTF8Rune(data []byte) bool {
	if len(data) == 0 || utf8.FullRune(data) {
		return false
	}
	key, size := utf8.DecodeRune(data)
	return key == utf8.RuneError && size == 1
}

func (r *Router) forward(data []byte) {
	cp := append([]byte(nil), data...)
	r.h.Forward(cp)
}

func passThroughPrefix(b byte) bool { return b == '[' || b == 'O' }

func binding(data []byte) (Action, int, bool) {
	if len(data) == 0 {
		return 0, 0, false
	}
	switch data[0] {
	case ' ':
		return ActionOpenPalette, 1, true
	case 'a':
		return ActionJumpAttention, 1, true
	}

	key, size := utf8.DecodeRune(data)
	if key == utf8.RuneError && size == 1 {
		return 0, 0, false
	}
	if idx, ok := topRowDigitIndex(key); ok {
		return ActionSwitchTab1 + Action(idx), size, true
	}
	return 0, 0, false
}

var azertyTopRowDigitVariants = []rune{'&', 'é', '"', '\'', '(', '-', 'è', '_', 'ç'}

// topRowDigitIndex maps the symbols emitted by physical top-row digit keys to
// zero-based digit positions. It is modifier-agnostic so Alt+digit and future
// Ctrl+digit bindings can share the same layout support.
func topRowDigitIndex(key rune) (int, bool) {
	if key >= '1' && key <= '9' {
		return int(key - '1'), true
	}
	for idx, variant := range azertyTopRowDigitVariants {
		if key == variant {
			return idx, true
		}
	}
	return 0, false
}
