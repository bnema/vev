package daemon

import (
	"time"

	"github.com/bnema/vev/internal/ports"
)

const (
	copyScrollFrame = 16 * time.Millisecond
	copyScrollTail  = 120 * time.Millisecond
)

// copyMu owns the animation and its single cancellable overlay-input timer.
// Easing distributes requested rows over frames; it never invents extra travel.
type copyScrollAnimation struct {
	remaining int
	lastInput time.Time
	timer     pendingByteTimer
}

func (rt *overlayRuntime) stopCopyScrollLocked() {
	rt.copyScroll.timer.stop()
	rt.copyScroll = copyScrollAnimation{}
}

func (d *Daemon) smoothCopyWheel(sess *session, ac *attachedClient, delta int) {
	if delta == 0 {
		return
	}
	rt := ac.overlays
	if d.clock == nil || d.currentCopyConfig().ReduceMotion {
		rt.copyMu.Lock()
		rt.stopCopyScrollLocked()
		rt.copyMu.Unlock()
		d.copyWheel(sess, ac, delta)
		return
	}
	rt.copyMu.Lock()
	if rt.copyMode == nil || rt.copyDocument == nil || rt.copyPane == nil {
		rt.copyMu.Unlock()
		return
	}
	// Selection stays exact and immediate. Reversal cancels the old tail so
	// the next frame always follows the user's latest direction.
	if rt.copyMode.Selection().Enabled || rt.copyDocument.Len() <= rt.copyDocument.Height() {
		rt.stopCopyScrollLocked()
		rt.copyMu.Unlock()
		d.copyWheel(sess, ac, delta)
		return
	}
	if rt.copyScroll.remaining != 0 && (rt.copyScroll.remaining < 0) != (delta < 0) {
		rt.stopCopyScrollLocked()
	}
	motion := &rt.copyScroll
	limit := rt.copyDocument.Len()
	motion.remaining += min(max(delta, -limit-motion.remaining), limit-motion.remaining)
	motion.lastInput = d.clock.Now()
	changed, exit := false, false
	if motion.timer.timer == nil {
		changed, exit = d.advanceCopyScrollLocked(sess, ac)
	}
	rt.copyMu.Unlock()
	if changed || exit {
		d.invalidateRender(sess, ac, exit, "copy-scroll-animation")
	}
}

func (d *Daemon) advanceCopyScrollLocked(sess *session, ac *attachedClient) (bool, bool) {
	rt := ac.overlays
	motion := &rt.copyScroll
	remaining := motion.remaining
	if remaining == 0 {
		return false, false
	}
	magnitude := remaining
	if magnitude < 0 {
		magnitude = -magnitude
	}
	step := (magnitude + 3) / 4
	if d.clock.Now().Sub(motion.lastInput) >= copyScrollTail {
		step = magnitude
	}
	if remaining < 0 {
		step = -step
	}
	motion.remaining -= step
	changed, exit := rt.moveCopyWheelLocked(step)
	if !changed || exit {
		rt.stopCopyScrollLocked()
		return changed, exit
	}
	if motion.remaining != 0 {
		mode := rt.copyMode
		motion.timer.retain(d.clock, copyScrollFrame, func(timer ports.Timer) {
			// Check presentation ownership before entering the copy lock. A
			// callback never locks another overlay or a session under copyMu.
			visible := ac.currentSession() == sess && !rt.promptActive() && !rt.paletteActive() &&
				!rt.pickerActive() && !rt.noticesActive() && !rt.resizeModeActive()
			rt.copyMu.Lock()
			if rt.copyScroll.timer.timer != timer {
				rt.copyMu.Unlock()
				return
			}
			rt.copyScroll.timer.timer = nil
			rt.copyScroll.timer.done = nil
			if !visible || rt.copyMode != mode || rt.copySearch != nil {
				rt.stopCopyScrollLocked()
				rt.copyMu.Unlock()
				return
			}
			changed, exit := d.advanceCopyScrollLocked(sess, ac)
			rt.copyMu.Unlock()
			if changed || exit {
				d.invalidateRender(sess, ac, exit, "copy-scroll-animation")
			}
		})
	}
	return changed, exit
}
