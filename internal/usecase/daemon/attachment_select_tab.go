package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// selectTabForAttachment shows one exact tab of the attached session on this
// attachment's own view. It is the client picker's in-place tab switch: the
// attachment stays live and only its view moves. A tab the session no longer
// holds is refused without touching the view, so the picker never lands on a
// repaired neighbour it did not choose.
func (d *Daemon) selectTabForAttachment(effect *attachmentEffect, request protocol.SelectTab) {
	if request.Validate() != nil || effect == nil || !effect.current() || effect.sess == nil || effect.ac == nil {
		return
	}
	sess, ac := effect.sess, effect.ac
	var selected *tab
	_ = sess.runMutation(func() error {
		sess.mu.Lock()
		for _, candidate := range sess.tabs {
			if candidate == nil || domain.TabStableID(candidate.stableID) != request.TabID {
				continue
			}
			if sess.selectAttachmentTabLocked(ac, request.TabID) {
				selected = candidate
			}
			break
		}
		sess.mu.Unlock()
		if selected != nil {
			d.activateTabAfterResizeForLease(sess, selected, false, ac, nil)
		}
		return nil
	})
	if selected == nil {
		return
	}
	d.invalidateRender(sess, ac, true, "attachment_select_tab.go")
}
