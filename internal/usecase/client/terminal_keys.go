package client

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/keys/kittykey"
)

// Held Ctrl.
//
// With the kitty keyboard protocol the client asks its terminal for every key
// event, modifier presses and releases included (kittykey.RequestFlags). The
// terminal input pump rewrites that stream back to the encoding the rest of
// vev reads (kittykey.OuterFlags) and records whether Ctrl is held on its own
// in a terminalCtrlState. The attached loop reports a hold that lasted
// ctrlHoldDelay to the daemon, which then shows recent-session ranks.

// ctrlHoldDelay keeps a quick Ctrl shortcut from flashing the ranks.
const ctrlHoldDelay = 150 * time.Millisecond

// terminalKeys is the pump's key normalization. Only the pump's reader
// goroutine calls normalize; enable may run on any goroutine.
type terminalKeys struct {
	enabled    atomic.Bool
	normalizer kittykey.Normalizer
	ctrl       *terminalCtrlState
}

func newTerminalKeys() *terminalKeys {
	return &terminalKeys{ctrl: newTerminalCtrlState()}
}

// enable starts normalizing. Call it before asking the terminal for
// kittykey.RequestFlags; input still in the old encoding passes unchanged.
func (k *terminalKeys) enable() { k.enabled.Store(true) }

// disable stops normalizing, for a terminal that refused the flags.
func (k *terminalKeys) disable() { k.enabled.Store(false) }

// normalize rewrites one read. focus is the terminal focus after the read:
// a window that lost focus may never report the Ctrl release.
func (k *terminalKeys) normalize(data []byte, focus domain.TerminalFocus) []byte {
	if !k.enabled.Load() {
		return data
	}
	out := k.normalizer.Normalize(data)
	if focus == domain.TerminalFocusUnfocused {
		k.normalizer.ReleaseCtrl()
	}
	k.ctrl.set(k.normalizer.CtrlHeld())
	return out
}

// terminalCtrlState is the client's one held-Ctrl state. The terminal input
// pump is its only writer; it outlives attachments.
type terminalCtrlState struct {
	mu      sync.Mutex
	held    bool
	changed chan struct{}
}

func newTerminalCtrlState() *terminalCtrlState {
	return &terminalCtrlState{changed: make(chan struct{})}
}

// Watch returns whether Ctrl is held and a channel closed on the next change.
// A nil state is never held and never changes.
func (s *terminalCtrlState) Watch() (bool, <-chan struct{}) {
	if s == nil {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held, s.changed
}

func (s *terminalCtrlState) set(held bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if held == s.held {
		return
	}
	s.held = held
	close(s.changed)
	s.changed = make(chan struct{})
}

// attachmentCtrlForeground is the optional seam through which an attached
// loop reads the held-Ctrl state.
type attachmentCtrlForeground interface {
	terminalCtrl() *terminalCtrlState
}

func (f *attachmentForeground) terminalCtrl() *terminalCtrlState {
	if f == nil || f.input == nil {
		return nil
	}
	return f.input.keys.ctrl
}

func attachmentTerminalCtrl(fg AttachmentForeground) *terminalCtrlState {
	if ctrl, ok := fg.(attachmentCtrlForeground); ok {
		return ctrl.terminalCtrl()
	}
	return nil
}

// ctrlReporter turns the held-Ctrl state into ModifierState messages: a hold
// is reported once it lasted ctrlHoldDelay, a release at once, and only after
// a reported hold.
type ctrlReporter struct {
	state   *terminalCtrlState
	clock   ports.Clock
	sent    bool
	changed <-chan struct{}
	timer   ports.Timer
}

func newCtrlReporter(state *terminalCtrlState, clock ports.Clock) *ctrlReporter {
	r := &ctrlReporter{state: state, clock: clock}
	r.next()
	return r
}

// timerC fires when a pending hold is due.
func (r *ctrlReporter) timerC() <-chan time.Time {
	if r.timer == nil {
		return nil
	}
	return r.timer.C()
}

// next handles a state change and returns the message to send now, if any.
func (r *ctrlReporter) next() (protocol.ModifierState, bool) {
	held, changed := r.state.Watch()
	r.changed = changed
	if held {
		if !r.sent && r.timer == nil {
			r.timer = r.clock.NewTimer(ctrlHoldDelay)
		}
		return protocol.ModifierState{}, false
	}
	r.stop()
	if !r.sent {
		return protocol.ModifierState{}, false
	}
	r.sent = false
	return protocol.ModifierState{Ctrl: false}, true
}

// due handles the hold timer and returns the hold to report, if still held.
func (r *ctrlReporter) due() (protocol.ModifierState, bool) {
	r.stop()
	if held, _ := r.state.Watch(); !held || r.sent {
		return protocol.ModifierState{}, false
	}
	r.sent = true
	return protocol.ModifierState{Ctrl: true}, true
}

func (r *ctrlReporter) stop() {
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
}
