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
	visible := sess.client != nil && sess.active >= 0 && sess.active < len(sess.tabs) && sess.tabs[sess.active] == tb
	if !tb.attention {
		tb.attentionAt = now
	}
	tb.attention = true
	if visible {
		tb.attentionVisiblePaint = true
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

func (d *Daemon) jumpAttention(sess *session, ac *attachedClient) {
	if sess == nil || ac == nil {
		return
	}
	if idx, ok := oldestAttentionTab(sess); ok {
		if sess.switchTab(idx) {
			d.activateTab(sess, sess.activeTab())
			d.paint(sess, ac, true)
		}
		return
	}

	target, ok := d.oldestOtherSessionAttention(sess)
	if !ok {
		return
	}
	d.switchToTarget(sess, ac, picker.Target{Session: target.sessionID, TabIndex: target.tabIndex})
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
	for _, sess := range d.sessions {
		if sess != current {
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
	sess.mu.Lock()
	ac := sess.client
	sess.mu.Unlock()
	if ac != nil {
		d.paint(sess, ac, false)
	}
}

func (d *Daemon) repaintAllAttachedClients() {
	d.mu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, sess := range d.sessions {
		sessions = append(sessions, sess)
	}
	d.mu.Unlock()

	for _, sess := range sessions {
		d.repaintAttachedClients(sess)
	}
}

func (s *session) ackAttention(tb *tab, visible bool) bool {
	if tb == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active < 0 || s.active >= len(s.tabs) || s.tabs[s.active] != tb || !tb.attention {
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
	sessions := make([]*session, 0, len(d.sessions))
	for _, sess := range d.sessions {
		sessions = append(sessions, sess)
	}
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
