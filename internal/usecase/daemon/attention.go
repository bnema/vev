package daemon

import (
	"context"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/picker"
)

// noteAttention records attention for a tab. If the tab is currently visible,
// it is kept long enough to render one pulse before paint acknowledges it. The
// caller must not hold tab.mu.
func (d *Daemon) noteAttention(sess *session, tb *tab) {
	if sess == nil || tb == nil {
		return
	}

	now := d.clock.Now()
	if now.IsZero() {
		now = time.Now()
	}

	sess.mu.Lock()
	if !tb.attention {
		tb.attentionAt = now
	}
	tb.attention = true
	for _, attachment := range sess.snapshotAttachmentsLocked() {
		view := attachment.viewSnapshot()
		if domain.TabStableID(tb.stableID) == view.tabID {
			tb.attentionVisiblePaint = true
			break
		}
	}
	sess.mu.Unlock()

	// Do not repaint here: this runs on the PTY reader goroutine, and paint
	// can block on a slow client's socket (Transport.Send has no deadline) —
	// that would let one wedged client stall a different session's reader.
	// The animator's first tick (<=120ms) delivers the repaint, and pulse
	// frame 0 renders the bell as blank anyway, so nothing is visibly lost
	// by not painting immediately.
	d.pokeAttentionTicker()
}

// jumpAttention switches to the oldest pending attention target, local tab
// first and then another session's. Returning nil never implies a switch
// happened — it also covers "no target exists", which is routine and not an
// error. Only a failure to reach a target that does exist is a genuine error.
func (d *Daemon) jumpAttention(sess *session, ac *attachedClient) error {
	return d.jumpAttentionForRole(sess, ac, attachmentRoleToken{})
}

func (d *Daemon) jumpAttentionForRole(sess *session, ac *attachedClient, token attachmentRoleToken) error {
	if sess == nil || ac == nil {
		return nil
	}
	if idx, ok := oldestAttentionTab(sess); ok {
		if sess.switchAttachmentTab(ac, idx) {
			d.activateTabAfterResizeForLease(sess, sess.tabForAttachment(ac), false, ac, token.lease)
			d.invalidateRender(sess, ac, true, "attention.go")
		}
		return nil
	}

	target, ok := d.oldestOtherSessionAttention(sess)
	if !ok {
		return nil
	}
	pickerTarget := picker.Target{Session: target.sessionID, TabIndex: target.tabIndex}
	if token.ac == nil {
		return d.switchToTarget(sess, ac, pickerTarget)
	}
	return d.switchActiveTargetForRole(token, pickerTarget)
}

func oldestAttentionTab(sess *session) (int, bool) {
	idx, _, ok := oldestAttentionTabWithTime(sess)
	return idx, ok
}

type attentionTarget struct {
	sessionID domain.SessionID
	tabIndex  int
	at        time.Time
}

func (d *Daemon) oldestOtherSessionAttention(current *session) (attentionTarget, bool) {
	d.mu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, entry := range d.sessions {
		if sess, ok := localSession(entry); ok && sess != current {
			sessions = append(sessions, sess)
		}
	}
	d.mu.Unlock()

	var target attentionTarget
	found := false
	for _, sess := range sessions {
		idx, at, ok := oldestAttentionTabWithTime(sess)
		if !ok {
			continue
		}
		if !found || at.Before(target.at) {
			target = attentionTarget{sessionID: sess.id, tabIndex: idx, at: at}
			found = true
		}
	}
	return target, found
}

func oldestAttentionTabWithTime(sess *session) (int, time.Time, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	idx := -1
	var oldest time.Time
	for i, tb := range sess.tabs {
		if !tb.attention {
			continue
		}
		if idx == -1 || tb.attentionAt.Before(oldest) {
			idx = i
			oldest = tb.attentionAt
		}
	}
	return idx, oldest, idx != -1
}

func (d *Daemon) repaintAttachedClients(sess *session) {
	for _, ac := range sess.snapshotAttachments() {
		d.invalidateRender(sess, ac, false, "attention.go")
	}
}

func (d *Daemon) repaintAllAttachedClients() {
	d.mu.Lock()
	sessions := localSessionsSnapshot(d.sessions)
	d.mu.Unlock()

	for _, sess := range sessions {
		d.repaintAttachedClients(sess)
	}
}

func (s *session) ackAttention(tb *tab, ac *attachedClient, visible bool) bool {
	if tb == nil || ac == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	view := ac.viewSnapshot()
	if domain.TabStableID(tb.stableID) != view.tabID || !tb.attention {
		return false
	}
	if tb.attentionVisiblePaint && !visible {
		return false
	}
	tb.attention = false
	tb.attentionAt = time.Time{}
	tb.attentionVisiblePaint = false
	return true
}

func (s *session) anyAttention() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tb := range s.tabs {
		if tb.attention {
			return true
		}
	}
	return false
}

func (d *Daemon) pokeAttentionTicker() {
	if d == nil || d.animWake == nil {
		return
	}
	select {
	case d.animWake <- struct{}{}:
	default:
	}
}

func (d *Daemon) attentionFrame() int {
	if d == nil {
		return 0
	}
	d.attnMu.Lock()
	defer d.attnMu.Unlock()
	return d.animFrame
}

func (d *Daemon) attentionAnimator(ctx context.Context) {
	active := false
	var timer ports.Timer
	for {
		if !active {
			if !d.anyAttention() {
				d.setAttentionFrame(0)
				select {
				case <-ctx.Done():
					return
				case <-d.animWake:
					continue
				}
			}
			active = true
		}

		if timer == nil {
			timer = d.clock.NewTimer(pulseFrameInterval)
		} else {
			timer.Reset(pulseFrameInterval)
		}

		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-d.animWake:
			if !timer.Stop() {
				select {
				case <-timer.C():
				default:
				}
			}
		case <-timer.C():
			d.advanceAttentionFrame()
			d.repaintAllAttachedClients()
		}

		if !d.anyAttention() {
			d.setAttentionFrame(0)
			d.repaintAllAttachedClients()
			active = false
		}
	}
}

func (d *Daemon) anyAttention() bool {
	d.mu.Lock()
	sessions := localSessionsSnapshot(d.sessions)
	d.mu.Unlock()

	for _, sess := range sessions {
		if sess.anyAttention() {
			return true
		}
	}
	return false
}

func (d *Daemon) advanceAttentionFrame() {
	d.attnMu.Lock()
	d.animFrame = (d.animFrame + 1) % pulseFrameCount
	d.attnMu.Unlock()
}

func (d *Daemon) setAttentionFrame(frame int) {
	d.attnMu.Lock()
	d.animFrame = frame
	d.attnMu.Unlock()
}
