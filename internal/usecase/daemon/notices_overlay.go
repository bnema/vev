package daemon

import (
	"fmt"
	"strings"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
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
	yank := false
	var yankTarget domain.Notification
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'j':
			ac.overlays.noticesOverlay.Down()
			changed = true
		case 'k':
			ac.overlays.noticesOverlay.Up()
			changed = true
		case 'y':
			if n, ok := ac.overlays.noticesOverlay.Selected(); ok {
				yankTarget = n
				yank = true
			}
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
	if yank {
		// yankNotice sends over the wire and repaints itself, so it must run
		// with noticeMu released; a quick yank doesn't close the overlay the
		// way copy mode's own 'y' commits and exits.
		d.yankNotice(sess, ac, yankTarget)
	}
	if exit || changed {
		d.invalidateRender(sess, ac, true, "notices_overlay.go")
	}
}

// yankNotice copies n's formatted details to the client's clipboard via
// OSC52, mirroring copy mode's own yank path (copymode.go handleCopyInput):
// send each chunk, then leave a one-shot status-bar confirmation that the
// next repaint clears.
func (d *Daemon) yankNotice(sess *session, ac *attachedClient, n domain.Notification) {
	chunks := scopy.OSC52(noticeYankPayload(n))
	for _, chunk := range chunks {
		failed, err := d.boundedSendOutputErrTransport(ac, chunk)
		if err != nil {
			d.detachOnSendError(sess, ac, failed)
			return
		}
	}
	ac.overlays.copyMu.Lock()
	if len(chunks) > 0 {
		ac.overlays.copyFeedback = "copied notification details"
	} else {
		ac.overlays.copyFeedback = "notification too large to copy"
	}
	ac.overlays.copyMu.Unlock()
	d.invalidateRender(sess, ac, true, "notices_overlay.go")
}

// noticeYankPayload formats n the way the yank commands put it on the
// clipboard: a header line with timestamp, severity, code slug, and coalesce
// count, then the message, then the full cause chain when present.
func noticeYankPayload(n domain.Notification) string {
	b := &strings.Builder{}
	fmt.Fprintf(b, "[%s] %s %s", n.Time.Format(time.RFC3339), severityWord(n.Severity), n.Code.String())
	if n.Count > 1 {
		fmt.Fprintf(b, " ×%d", n.Count)
	}
	fmt.Fprintf(b, "\n%s\n", n.Message)
	if n.Details != "" {
		fmt.Fprintf(b, "details: %s\n", n.Details)
	}
	return b.String()
}

// severityWord renders sev as the word used in yank payloads, distinct from
// the single-letter form the history list draws (notices.severityLetter).
func severityWord(sev domain.NoticeSeverity) string {
	switch sev {
	case domain.NoticeWarn:
		return "warn"
	case domain.NoticeError:
		return "error"
	default:
		return "info"
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
