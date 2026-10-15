package daemon

import "github.com/bnema/vev/internal/protocol"

// Held modifiers.
//
// A client whose terminal reports modifier key events tells the daemon when
// Ctrl is held on its own. While it is, the attachment's status bar prefixes
// each recent session with the rank Ctrl+1..9 jumps to.

// applyModifierStateForAttachment records a reported Ctrl state and repaints
// the attachment's bar when it changed.
func (d *Daemon) applyModifierStateForAttachment(effect *attachmentEffect, message protocol.ModifierState) {
	if effect == nil || effect.sess == nil || effect.ac == nil {
		return
	}
	if effect.ac.ctrlHeld.Swap(message.Ctrl) == message.Ctrl {
		return
	}
	d.invalidateRender(effect.sess, effect.ac, false, "modifier_state.go")
}
