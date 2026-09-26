// Package keys routes terminal input bytes into vev Alt/ESC bindings or PTY
// passthrough bytes.
package keys

import (
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
	ActionConsumeOrExpelPaneLeft
	ActionConsumeOrExpelPaneRight
)

// Handler receives router outputs. Forward is called only for bytes that should
// reach the PTY; Action receives intercepted bindings and the exact consumed
// terminal bytes. Both byte slices are owned copies.
type Handler interface {
	Forward([]byte)
	Action(Action, []byte)
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
		tok := scanEscape(data[i:])
		if tok.kind == escIncomplete {
			flush()
			r.retainESC(h, tok.raw[1:])
			return
		}
		if action, ok := r.tokenAction(tok); ok {
			flush()
			r.action(action, tok.raw, h)
			i += len(tok.raw)
			continue
		}
		switch tok.kind {
		case escPaste, escAltArrow, escControlPrefix:
			// Pastes are forwarded verbatim so embedded Alt lookalikes never
			// fire; unbound arrows and control prefixes pass through.
			buf = append(buf, tok.raw...)
			i += len(tok.raw)
		default:
			// Unbound Alt rune or bare ESC: forward the ESC alone and route
			// the following bytes normally.
			buf = append(buf, ESC)
			i++
		}
	}
	flush()
}

// routeAfterPendingESC routes data that follows a retained ESC. data excludes
// that ESC; pendingAltLen bytes of data were retained with it. It returns how
// many bytes of data it consumed.
func (r *Router) routeAfterPendingESC(data []byte, pendingAltLen int, h Handler) int {
	tok := scanEscape(append([]byte{ESC}, data...))
	if tok.kind == escIncomplete {
		r.retainESC(h, data)
		return len(data)
	}
	if action, ok := r.tokenAction(tok); ok {
		r.action(action, tok.raw, h)
		return len(tok.raw) - 1
	}
	if _, size := utf8.DecodeRune(data); size > 1 {
		r.forward(append([]byte{ESC}, data[:size]...), h)
		return size
	}
	if pendingAltLen > 0 {
		r.forward(append([]byte{ESC}, data[:pendingAltLen]...), h)
		return pendingAltLen
	}
	if next := data[0]; next == '[' || next == 'O' {
		r.forward([]byte{ESC, next}, h)
		return 1
	}
	r.forward([]byte{ESC}, h)
	return 0
}

// tokenAction resolves a lexed unit against the current bindings.
func (r *Router) tokenAction(tok escToken) (Action, bool) {
	bindings := r.currentBindings()
	switch tok.kind {
	case escAltArrow:
		return bindings.actionForAltArrow(tok.arrow)
	case escAltRune:
		return bindings.actionForAltRune(tok.rune)
	default:
		return 0, false
	}
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

func (r *Router) forward(data []byte, h Handler) {
	cp := append([]byte(nil), data...)
	h.Forward(cp)
}

func (r *Router) action(action Action, raw []byte, h Handler) {
	cp := append([]byte(nil), raw...)
	h.Action(action, cp)
}

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
