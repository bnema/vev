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

// listInputState owns the common pending-ESC lifecycle for list overlays.
// The caller holds the overlay lock while handleListInputLocked runs. The
// timer callback takes the same lock before checking its identity, so a later
// input chunk cannot let a stale lone-ESC close a reopened overlay.
type listInputState struct {
	pending     *[]byte
	esc         *pendingByteTimer
	moveUp      func()
	moveDown    func()
	lock        func()
	unlock      func()
	active      func() bool
	closeLocked func()
	afterClose  func()
}

type listInputResult struct {
	changed bool
	exit    bool
	action  byte
	stop    bool
}

func handleListInputLocked(clock ports.Clock, data []byte, state listInputState, action func(byte) listInputResult) listInputResult {
	if len(*state.pending) > 0 {
		state.esc.stop()
		combined := make([]byte, 0, len(*state.pending)+len(data))
		combined = append(combined, (*state.pending)...)
		combined = append(combined, data...)
		data = combined
		*state.pending = nil
	}

	var result listInputResult
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'j':
			state.moveDown()
			result.changed = true
		case 'k':
			state.moveUp()
			result.changed = true
		case 'q', 0x03:
			result.exit = true
		case keys.ESC:
			tail := data[i:]
			if consumed, move := routeListEscape(tail); consumed > 0 {
				switch move {
				case 'A':
					state.moveUp()
					result.changed = true
				case 'B':
					state.moveDown()
					result.changed = true
				}
				i += consumed - 1
				continue
			}
			if len(tail) == 1 {
				retainListESCLocked(clock, state)
				break
			}
			if isListEscapePrefix(tail) {
				*state.pending = append((*state.pending)[:0], tail...)
				i += len(tail) - 1
				continue
			}
			result.exit = true
		default:
			custom := action(data[i])
			if custom.action != 0 {
				result.action = custom.action
			}
			result.exit = result.exit || custom.exit
			result.stop = custom.stop
			if result.stop {
				return result
			}
		}
	}
	return result
}

// routeListEscape consumes one complete CSI or SS3 sequence. It returns its
// byte length and the simple arrow final when it is an Up/Down sequence. Other
// complete terminal sequences are deliberately ignored so their bytes cannot
// trigger list actions or close the overlay.
func routeListEscape(data []byte) (consumed int, move byte) {
	if len(data) < 2 || data[0] != keys.ESC || (data[1] != '[' && data[1] != 'O') {
		return 0, 0
	}
	for i := 2; i < len(data); i++ {
		if data[i] < 0x40 || data[i] > 0x7e {
			continue
		}
		if i == 2 && (data[i] == 'A' || data[i] == 'B') {
			return i + 1, data[i]
		}
		return i + 1, 0
	}
	return 0, 0
}

func isListEscapePrefix(data []byte) bool {
	return len(data) >= 2 && data[0] == keys.ESC && (data[1] == '[' || data[1] == 'O')
}

func retainListESCLocked(clock ports.Clock, state listInputState) {
	*state.pending = append((*state.pending)[:0], keys.ESC)
	state.esc.retain(clock, keys.ESCDelay, func(timer ports.Timer) {
		state.lock()
		if state.esc.timer != timer || len(*state.pending) != 1 || (*state.pending)[0] != keys.ESC || !state.active() {
			state.unlock()
			return
		}
		*state.pending = nil
		state.esc.timer = nil
		state.esc.done = nil
		state.closeLocked()
		state.unlock()
		state.afterClose()
	})
}

func (d *Daemon) noticesListInputState(ac *attachedClient) listInputState {
	rt := ac.overlays
	return listInputState{
		pending:  &rt.noticesPending,
		esc:      &rt.noticesESC,
		moveUp:   rt.noticesOverlay.Up,
		moveDown: rt.noticesOverlay.Down,
		lock:     rt.noticeMu.Lock,
		unlock:   rt.noticeMu.Unlock,
		active:   func() bool { return rt.noticesOverlay != nil },
		closeLocked: func() {
			rt.noticesOverlay = nil
		},
		afterClose: func() {
			if sess := ac.currentSession(); sess != nil {
				d.invalidateRender(sess, ac, true, "notices_overlay.go")
			}
		},
	}
}

func (d *Daemon) handleNoticesInput(ac *attachedClient, data []byte) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	rt := ac.overlays
	rt.noticeMu.Lock()
	if rt.noticesOverlay == nil {
		rt.noticesPending = nil
		rt.noticesESC.stop()
		rt.noticeMu.Unlock()
		return
	}
	var yankTarget domain.Notification
	var yank bool
	result := handleListInputLocked(d.clock, data, d.noticesListInputState(ac), func(b byte) listInputResult {
		if b == 'y' {
			yankTarget, yank = rt.noticesOverlay.Selected()
			return listInputResult{action: b}
		}
		return listInputResult{}
	})
	rt.noticeMu.Unlock()

	if result.exit {
		d.closeNotices(ac)
	}
	if yank {
		// yankNotice sends over the wire and repaints itself, so it must run
		// with noticeMu released; a quick yank doesn't close the overlay the
		// way copy mode's own 'y' commits and exits.
		d.yankNotice(sess, ac, yankTarget)
	}
	if result.exit || result.changed {
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
		ac.overlays.statusFeedback = "copied notification details"
	} else {
		ac.overlays.statusFeedback = "notification too large to copy"
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

func (d *Daemon) closeNotices(ac *attachedClient) {
	ac.overlays.noticeMu.Lock()
	ac.overlays.noticesOverlay = nil
	ac.overlays.noticesPending = nil
	ac.overlays.noticesESC.stop()
	ac.overlays.noticeMu.Unlock()
}
