package client

import (
	"time"

	"github.com/bnema/vev/internal/usecase/keys/kittykey"
)

// Input ownership while a lost attachment reconnects.
//
// From the loss of a live attachment until its replacement commits a first
// frame, the supervisor owns terminal input instead of the picker or an
// attachment foreground. Every human byte read in that window is discarded, so
// keystrokes typed while the screen says "Reconnecting…" are never queued and
// replayed into the remote shell after the session returns. The only bytes the
// supervisor interprets are the cancel keys (Escape and Ctrl+C, raw or kitty
// CSI-u encoded); the first one latches resumeCancel closed, which the resume
// loop observes to return to the picker.
//
// The gate sits in the pump's enqueue, the single place a completed terminal
// read becomes visible to a consumer, so no read can slip past it between an
// attachment's teardown and the next attempt's claim.

// resumeCancelGrace is how long after a cancel the pump keeps swallowing
// cancel keys. The cancel returns to the picker, whose own exit key quits vev,
// so a quick second Esc/Ctrl+C (a double press, or key repeat) must not leak
// through and close the program the user only meant to leave the resume of.
const resumeCancelGrace = 400 * time.Millisecond

// beginResume opens the resume input window and returns its cancel signal. It
// is idempotent while the window is open, so every attempt of one resume loop
// shares the same latched signal. Input read before the window opened but not
// yet claimed belonged to the lost attachment and is dropped.
func (p *terminalInputPump) beginResume() <-chan struct{} {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.resumeActive {
		cancel := p.resumeCancel
		p.mu.Unlock()
		return cancel
	}
	p.resumeActive = true
	p.resumeCancelledAt = time.Time{}
	p.resumeCancel = make(chan struct{})
	cancel := p.resumeCancel
	dropped := false
	if p.consumer == 0 {
		dropped = p.pending != nil
		p.pending = nil
		p.residual = nil
	}
	p.mu.Unlock()
	if dropped {
		select {
		case p.space <- struct{}{}:
		default:
		}
	}
	return cancel
}

// endResume closes the resume input window: later reads reach the next owner
// again. It is a no-op when no window is open.
func (p *terminalInputPump) endResume() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.resumeActive = false
	p.resumeCancel = nil
	p.mu.Unlock()
}

// discardResumeInputLocked drops the human bytes of result and latches the
// cancel signal when they contain a cancel key. Every cancel key, not only the
// first, restarts the grace, so a held key's auto-repeat keeps sliding it.
// Callers hold p.mu.
func (p *terminalInputPump) discardResumeInputLocked(result *terminalReadResult) {
	if isResumeCancelKey(result.data) {
		select {
		case <-p.resumeCancel:
		default:
			close(p.resumeCancel)
		}
		if p.clock != nil {
			p.resumeCancelledAt = p.clock.Now()
		}
	}
	result.data = nil
}

// swallowGraceCancelLocked reports whether result is exactly one cancel key
// read within resumeCancelGrace of the last cancel, while the picker (or no
// consumer yet) owns input, and drops its bytes when it is. A swallowed key
// restarts the grace. Every other read passes through untouched, including a
// read that merely contains a cancel key among other input. Callers hold p.mu.
func (p *terminalInputPump) swallowGraceCancelLocked(result *terminalReadResult) bool {
	if p.clock == nil || p.resumeCancelledAt.IsZero() || result.err != nil || len(result.data) == 0 {
		return false
	}
	if p.consumer != 0 && p.consumer != p.pickerConsumer {
		return false
	}
	if !isSingleCancelKey(result.data) {
		return false
	}
	now := p.clock.Now()
	if now.Sub(p.resumeCancelledAt) >= resumeCancelGrace {
		p.resumeCancelledAt = time.Time{}
		return false
	}
	p.resumeCancelledAt = now
	result.data = nil
	return true
}

// isSingleCancelKey reports whether one terminal read is exactly one cancel
// key press: a bare Escape or Ctrl+C byte, or a single kitty CSI-u Escape or
// Ctrl+C press event spanning the whole read.
func isSingleCancelKey(data []byte) bool {
	if len(data) == 1 {
		return data[0] == 0x1b || data[0] == 0x03
	}
	ev, n, ok, _ := kittykey.Parse(data)
	return ok && n == len(data) && isKittyCancelPress(ev)
}

// isResumeCancelKey reports whether one terminal read contains Escape or
// Ctrl+C. A bare 0x1b is Escape only as the whole read: followed by more bytes
// it starts an escape sequence or an Alt chord. With the kitty keyboard
// protocol Escape is CSI 27 u and Ctrl+C is CSI 99;5 u; key releases are not
// presses. A 0x03 inside a pasted block also cancels; that is harmless because
// the input is discarded anyway.
func isResumeCancelKey(data []byte) bool {
	if len(data) == 1 && data[0] == 0x1b {
		return true
	}
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 0x03:
			return true
		case 0x1b:
			ev, n, ok, _ := kittykey.Parse(data[i:])
			if !ok {
				continue
			}
			if isKittyCancelPress(ev) {
				return true
			}
			i += n - 1
		}
	}
	return false
}

func isKittyCancelPress(ev kittykey.Event) bool {
	return !ev.Release && (isKittyEscape(ev) || isKittyCtrlC(ev))
}

func isKittyEscape(ev kittykey.Event) bool {
	return ev.Code == kittykey.KeyEscape && ev.Mods == 0
}

func isKittyCtrlC(ev kittykey.Event) bool {
	return ev.Mods == kittykey.ModCtrl && (ev.Code == 'c' || ev.Base == 'c')
}
