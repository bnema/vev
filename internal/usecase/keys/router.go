// Package keys routes terminal input bytes into vev Alt/ESC bindings or PTY
// passthrough bytes.
package keys

import (
	"bytes"
	"slices"
	"sync"
	"sync/atomic"
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
	ActionFocusPaneLeft
	ActionFocusPaneRight
	ActionFocusPaneUp
	ActionFocusPaneDown
	ActionToggleFloatingPane
	ActionGrowPaneWidth
	ActionShrinkPaneWidth
	ActionGrowPaneHeight
	ActionShrinkPaneHeight
	ActionEqualizePanes
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
	clock    ports.Clock
	delay    time.Duration
	h        Handler
	bindings *atomic.Pointer[Bindings]

	mu             sync.Mutex
	pending        bool
	pendingAlt     []byte
	pendingHandler Handler
	timer          ports.Timer
	pendingDone    chan struct{}
}

// NewRouter constructs a Router. bindings may be nil, in which case
// currentBindings falls back to defaultBindings.
func NewRouter(clock ports.Clock, h Handler, bindings *atomic.Pointer[Bindings]) *Router {
	return &Router{clock: clock, delay: ESCDelay, h: h, bindings: bindings}
}

// Route routes one transport read with the router's default handler.
func (r *Router) Route(data []byte) { r.RouteWithHandler(data, r.h) }

// RouteWithHandler binds every synchronous or delayed result from data to the
// supplied handler. A retained ESC therefore cannot gain authority from a
// later transport frame after the original frame's ownership token is stale.
func (r *Router) RouteWithHandler(data []byte, h Handler) {
	if len(data) == 0 {
		return
	}
	if h == nil {
		h = r.h
	}
	r.mu.Lock()
	var pendingAlt []byte
	if r.pending {
		pendingAlt = append([]byte(nil), r.pendingAlt...)
	}
	pendingHandler := r.pendingHandler
	wasPending := r.pending
	if wasPending {
		r.stopTimer()
		r.pending = false
		r.pendingAlt = nil
		r.pendingHandler = nil
	}
	r.mu.Unlock()
	if wasPending {
		combined := append(append([]byte(nil), pendingAlt...), data...)
		if consumed := r.routeAfterPendingESC(combined, len(pendingAlt), pendingHandler); consumed > len(pendingAlt) {
			data = data[consumed-len(pendingAlt):]
		}
	}
	r.route(data, h)
}

func (r *Router) route(data []byte, h Handler) {
	buf := make([]byte, 0, len(data))
	flush := func() {
		if len(buf) == 0 {
			return
		}
		r.forward(buf, h)
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
			r.retainESC(h)
			return
		}
		remaining := data[i+1:]
		if bytes.HasPrefix(data[i:], ports.BracketedPasteOpenMarker) {
			// A bracketed paste whose closing marker is in this same frame is
			// forwarded verbatim: pasted content bytes (including embedded
			// ESC+letter Alt lookalikes or ESC [ 1 ; 3 A sequences) must never
			// fire an Action mid-paste. Without a closing marker in-frame we
			// fall through to today's per-byte routing (the '[' passes through
			// as a control prefix); Part A keeps pastes single-frame in
			// practice, so no cross-frame paste state is tracked here.
			if rel := bytes.Index(data[i:], ports.BracketedPasteCloseMarker); rel >= 0 {
				end := i + rel + len(ports.BracketedPasteCloseMarker)
				buf = append(buf, data[i:end]...)
				i = end
				continue
			}
		}
		if action, size, ok, partial := r.altArrowCSI(remaining); ok {
			flush()
			h.Action(action)
			i += 1 + size
			continue
		} else if partial {
			flush()
			r.retainESC(h, remaining)
			return
		}
		next := data[i+1]
		if passThroughPrefix(next) {
			buf = append(buf, ESC, next)
			i += 2
			continue
		}
		if action, size, ok := r.binding(remaining); ok {
			flush()
			h.Action(action)
			i += 1 + size
			continue
		}
		if partialUTF8Rune(remaining) {
			flush()
			r.retainESC(h, remaining)
			return
		}
		buf = append(buf, ESC)
		i++
	}
	flush()
}

func (r *Router) routeAfterPendingESC(data []byte, pendingAltLen int, h Handler) int {
	if action, size, ok, partial := r.altArrowCSI(data); ok {
		h.Action(action)
		return size
	} else if partial {
		r.retainESC(h, data)
		return len(data)
	}
	next := data[0]
	if action, size, ok := r.binding(data); ok {
		h.Action(action)
		return size
	}
	if partialUTF8Rune(data) {
		r.retainESC(h, data)
		return len(data)
	}
	_, size := utf8.DecodeRune(data)
	if size > 1 {
		r.forward(append([]byte{ESC}, data[:size]...), h)
		return size
	}
	if pendingAltLen > 0 {
		r.forward(append([]byte{ESC}, data[:pendingAltLen]...), h)
		return pendingAltLen
	}
	if passThroughPrefix(next) {
		r.forward([]byte{ESC, next}, h)
		return 1
	}
	r.forward([]byte{ESC}, h)
	return 0
}

func (r *Router) retainESC(h Handler, altBytes ...[]byte) {
	r.mu.Lock()
	r.pending = true
	r.pendingAlt = nil
	r.pendingHandler = h
	if len(altBytes) > 0 {
		r.pendingAlt = append([]byte(nil), altBytes[0]...)
	}
	r.timer = r.clock.NewTimer(r.delay)
	r.pendingDone = make(chan struct{})
	timer, done := r.timer, r.pendingDone
	r.mu.Unlock()
	go func(timer ports.Timer, done <-chan struct{}) {
		select {
		case <-timer.C():
		case <-done:
			return
		}
		var (
			data []byte
			h    Handler
		)
		r.mu.Lock()
		if r.pending && r.timer == timer {
			r.pending = false
			data = append([]byte{ESC}, r.pendingAlt...)
			h = r.pendingHandler
			r.pendingAlt = nil
			r.pendingHandler = nil
			r.timer = nil
			r.pendingDone = nil
		}
		r.mu.Unlock()
		if h != nil {
			r.forward(data, h)
		}
	}(timer, done)
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

func (r *Router) forward(data []byte, h Handler) {
	cp := append([]byte(nil), data...)
	h.Forward(cp)
}

func passThroughPrefix(b byte) bool { return b == '[' || b == 'O' }

func (r *Router) currentBindings() *Bindings {
	if r.bindings == nil {
		return defaultBindings
	}
	bindings := r.bindings.Load()
	if bindings == nil {
		return defaultBindings
	}
	return bindings
}

func (r *Router) altArrowCSI(data []byte) (Action, int, bool, bool) {
	const seqLen = len("[1;3A")
	if len(data) < seqLen {
		return 0, 0, false, hasAltArrowCSIPrefix(data)
	}
	seq := data[:seqLen]
	if seq[0] != '[' || seq[1] != '1' || seq[2] != ';' || (seq[3] != '3' && seq[3] != '9') {
		return 0, 0, false, false
	}
	if action, ok := r.currentBindings().actionForAltArrow(seq[4]); ok {
		return action, seqLen, true, false
	}
	return 0, 0, false, false
}

func hasAltArrowCSIPrefix(data []byte) bool {
	if len(data) == 0 || len(data) >= len("[1;3A") {
		return false
	}
	return matchesPrefix(data, "[1;3") || matchesPrefix(data, "[1;9")
}

func matchesPrefix(data []byte, want string) bool {
	if len(data) > len(want) {
		return false
	}
	for i := range data {
		if data[i] != want[i] {
			return false
		}
	}
	return true
}

func (r *Router) binding(data []byte) (Action, int, bool) {
	return r.currentBindings().actionForAltBytes(data)
}

var topRowDigitAliases = [][]rune{
	{'1', '&'},
	{'2', 'é'},
	{'3', '"'},
	{'4', '\''},
	{'5', '('},
	{'6', '-', '§'},
	{'7', 'è'},
	{'8', '_', '!'},
	{'9', 'ç'},
}

// topRowDigitIndex maps symbols emitted by physical top-row digit keys to
// zero-based digit positions. It is modifier-agnostic so Alt+digit and future
// Ctrl+digit bindings can share the same layout support.
func topRowDigitIndex(key rune) (int, bool) {
	for idx, aliases := range topRowDigitAliases {
		if slices.Contains(aliases, key) {
			return idx, true
		}
	}
	return 0, false
}
