package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/notices"
	"github.com/bnema/vev/internal/usecase/ui"
)

var noticesModal = ui.Modal{WidthPct: 70, HeightPct: 70, MinWidth: 40, MinHeight: 8, Title: " Notifications ", Anchor: domain.AnchorCenter}

// enterNotices opens the notification history overlay from the daemon's full
// notice center history, newest first.
func (d *Daemon) enterNotices(sess *session, ac *attachedClient) {
	history := d.notices.history()
	now := d.clock.Now()
	ac.overlays.noticeMu.Lock()
	ac.overlays.noticesOverlay = notices.New(history, now)
	ac.overlays.noticesPending = nil
	ac.overlays.noticeMu.Unlock()
	d.invalidateRender(sess, ac, true, "notices_overlay.go")
}

func (d *Daemon) handleNoticesInput(ac *attachedClient, data []byte) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	ac.overlays.noticeMu.Lock()
	if ac.overlays.noticesOverlay == nil {
		ac.overlays.noticesPending = nil
		d.stopNoticesPendingTimerLocked(ac)
		ac.overlays.noticeMu.Unlock()
		return
	}
	if len(ac.overlays.noticesPending) > 0 {
		d.stopNoticesPendingTimerLocked(ac)
		combined := make([]byte, 0, len(ac.overlays.noticesPending)+len(data))
		combined = append(combined, ac.overlays.noticesPending...)
		combined = append(combined, data...)
		data = combined
		ac.overlays.noticesPending = nil
	}
	changed := false
	exit := false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'j':
			ac.overlays.noticesOverlay.Down()
			changed = true
		case 'k':
			ac.overlays.noticesOverlay.Up()
			changed = true
		case 'q', 0x03, 0x1b:
			if data[i] == 0x1b {
				tail := data[i:]
				consumed, ok := routeNoticesEscape(ac.overlays.noticesOverlay, tail)
				if ok {
					i += consumed - 1
					changed = true
					continue
				}
				if len(tail) == 1 {
					d.retainNoticesESCLocked(ac)
					break
				}
				if isPickerEscapePrefix(tail) {
					ac.overlays.noticesPending = append(ac.overlays.noticesPending[:0], tail...)
					break
				}
			}
			exit = true
		}
	}
	ac.overlays.noticeMu.Unlock()

	if exit {
		d.closeNotices(ac)
	}
	if exit || changed {
		d.invalidateRender(sess, ac, true, "notices_overlay.go")
	}
}

func (d *Daemon) retainNoticesESCLocked(ac *attachedClient) {
	ac.overlays.noticesPending = append(ac.overlays.noticesPending[:0], keys.ESC)
	ac.overlays.noticesESC.retain(d.clock, keys.ESCDelay, func(timer ports.Timer) {
		ac.overlays.noticeMu.Lock()
		if ac.overlays.noticesESC.timer != timer || len(ac.overlays.noticesPending) != 1 || ac.overlays.noticesPending[0] != keys.ESC || ac.overlays.noticesOverlay == nil {
			ac.overlays.noticeMu.Unlock()
			return
		}
		ac.overlays.noticesPending = nil
		ac.overlays.noticesESC.timer = nil
		ac.overlays.noticesESC.done = nil
		ac.overlays.noticesOverlay = nil
		ac.overlays.noticeMu.Unlock()

		if sess := ac.currentSession(); sess != nil {
			d.invalidateRender(sess, ac, true, "notices_overlay.go")
		}
	})
}

func (d *Daemon) stopNoticesPendingTimerLocked(ac *attachedClient) {
	ac.overlays.noticesESC.stop()
}

func (d *Daemon) closeNotices(ac *attachedClient) {
	ac.overlays.noticeMu.Lock()
	ac.overlays.noticesOverlay = nil
	ac.overlays.noticesPending = nil
	d.stopNoticesPendingTimerLocked(ac)
	ac.overlays.noticeMu.Unlock()
}

func routeNoticesEscape(m *notices.Model, data []byte) (int, bool) {
	if len(data) >= 3 && (data[1] == '[' || data[1] == 'O') {
		switch data[2] {
		case 'A':
			m.Up()
			return 3, true
		case 'B':
			m.Down()
			return 3, true
		}
	}
	return 0, false
}
