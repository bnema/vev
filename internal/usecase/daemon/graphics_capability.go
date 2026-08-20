package daemon

import (
	"github.com/bnema/vev/internal/domain"
)

// sessionHasKittyGraphics reports whether a session currently contains any
// decoded Kitty scene. It is used only for an attachment-local warning; the
// renderer still suppresses graphics for attachments that did not declare
// direct support.
func sessionHasKittyGraphics(sess *session) bool {
	if sess == nil {
		return false
	}
	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	for _, tb := range tabs {
		if tb == nil {
			continue
		}
		tb.mu.Lock()
		panes := make([]*pane, 0, len(tb.panes)+1)
		for _, p := range tb.panes {
			panes = append(panes, p)
		}
		if tb.floating.pane != nil {
			panes = append(panes, tb.floating.pane)
		}
		tb.mu.Unlock()
		for _, p := range panes {
			if p == nil {
				continue
			}
			p.mu.Lock()
			graphics := p.screen.GraphicsSnapshot()
			p.mu.Unlock()
			if graphics != nil && graphics.Usage().Placements != 0 {
				return true
			}
		}
	}
	return false
}

func (d *Daemon) warnUnsupportedGraphics(ac *attachedClient) {
	if d == nil || ac == nil || ac.graphicsUnsupportedWarned.Swap(true) {
		return
	}
	sess := ac.currentAttachmentSession()
	if sess == nil {
		return
	}
	d.publishToast(ac, domain.Notification{
		Code:      domain.NoticeUser,
		Severity:  domain.NoticeWarn,
		Message:   "Kitty graphics are unavailable on this attachment; images are suppressed.",
		Time:      d.clock.Now(),
		Count:     1,
		SessionID: sess.id,
	})
}
