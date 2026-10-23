package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/agentactivity"
	"github.com/bnema/vev/internal/usecase/picker"
)

// noteAttention records attention for a tab. If the tab is currently visible,
// it is kept long enough to render one pulse before paint acknowledges it. The
// caller must not hold tab.mu.
func (d *Daemon) noteAttention(sess *session, tb *tab) {
	if sess == nil || tb == nil {
		return
	}

	now := d.daemonNow()

	sess.mu.Lock()
	if !tb.attention {
		tb.attentionAt = now
	}
	tb.attention = true
	for _, attachment := range sess.snapshotAttachmentsLocked() {
		view := attachment.viewSnapshot()
		if domain.TabStableID(tb.stableID) == view.tabID && attachment.terminalFocus().MaySee() {
			tb.attentionVisiblePaint = true
			break
		}
	}
	sess.mu.Unlock()

	// Do not repaint here: this runs on the PTY reader goroutine or the agent
	// activity scheduler, and a synchronous paint can block on a slow
	// client's socket (Transport.Send has no deadline), stalling the caller.
	// The animator's first tick (<=120ms) delivers the repaint, and pulse
	// frame 0 renders the bell as blank anyway, so nothing is visibly lost
	// by not painting immediately.
	d.pokeAttentionTicker()
}

// jumpAttention switches to the oldest pending attention target: a tab of the
// attached session first, then another session of this daemon, then the
// oldest bell among the client's routes on any other daemon, which the client
// navigates to. Without any bell it switches to the agent tab that finished
// first and is still unseen: in the attached session first, then in another
// session of this daemon. Done never crosses the wire, so finished agents on
// other daemons are not reached. Returning nil never implies a switch happened —
// it also covers "no target exists", which is routine and not an error. Only
// a failure to reach a target that does exist is a genuine error.
func (d *Daemon) jumpAttention(sess *session, ac *attachedClient) error {
	return d.jumpAttentionForAttachment(sess, ac, nil)
}

func (d *Daemon) jumpAttentionForAttachment(sess *session, ac *attachedClient, effect *attachmentEffect) error {
	if sess == nil || ac == nil {
		return nil
	}
	if idx, ok := oldestAttentionTab(sess); ok {
		d.jumpLocalTab(sess, ac, effect, idx)
		return nil
	}
	if target, ok := d.oldestOtherSessionAttention(sess); ok {
		return d.jumpOtherSession(sess, ac, effect, target)
	}
	if handled, err := d.jumpRouteAttention(sess, ac, effect); handled {
		return err
	}

	// Published Done checks outlive a reload that turns activity off until
	// the scheduler clears them; they are not drawn then, so not targets.
	if !d.currentRuntimeConfig().agents.Activity {
		return nil
	}
	// The viewed tab is skipped: its Done clears through the paint ack, which
	// may still be queued, so a repeated jump moves on to the next target.
	if idx, _, ok := oldestDoneTab(sess, ac.viewSnapshot().tabID); ok {
		d.jumpLocalTab(sess, ac, effect, idx)
		return nil
	}
	if target, ok := d.oldestOtherSessionDone(sess); ok {
		return d.jumpOtherSession(sess, ac, effect, target)
	}
	return nil
}

// jumpLocalTab switches ac to tab idx of its attached session.
func (d *Daemon) jumpLocalTab(sess *session, ac *attachedClient, effect *attachmentEffect, idx int) {
	if !sess.switchAttachmentTab(ac, idx) {
		return
	}
	var lease *attachmentLease
	if effect != nil {
		lease = effect.lease
	}
	d.activateTabAfterResizeForLease(sess, sess.tabForAttachment(ac), false, ac, lease)
	d.invalidateRender(sess, ac, true, "attention.go")
}

// jumpOtherSession moves ac to target, a tab of another local session.
func (d *Daemon) jumpOtherSession(sess *session, ac *attachedClient, effect *attachmentEffect, target attentionTarget) error {
	pickerTarget := picker.Target{Session: target.sessionID, TabIndex: target.tabIndex}
	if effect == nil {
		return d.switchToTarget(sess, ac, pickerTarget)
	}
	return d.switchActiveTargetForAttachment(effect, pickerTarget)
}

// jumpRouteAttention asks the client to navigate to the oldest attention
// route it published for another daemon. The serving daemon's own sessions
// were already considered, so a local route of this daemon is skipped.
// handled reports that such a route existed.
func (d *Daemon) jumpRouteAttention(sess *session, ac *attachedClient, effect *attachmentEffect) (handled bool, err error) {
	entry, ok := oldestRouteAttention(ac.routeSnapshotCopy(), d.ownsLifecycle)
	if !ok {
		return false, nil
	}
	exec := paletteExec{d: d, sess: sess, attachment: sess, ac: ac, effect: effect}
	err = exec.NavigateRecentRoute(entry)
	if errors.Is(err, errAttachmentTransition) {
		return true, nil
	}
	return true, err
}

// oldestRouteAttention selects the route whose attention the client observed
// first. owned reports lifecycles this daemon serves itself.
func oldestRouteAttention(snapshot protocol.RecentRouteSnapshot, owned func(domain.SessionLifecycleID) bool) (protocol.RouteNavigationAction, bool) {
	var best protocol.RecentRouteEntry
	found := false
	for _, entry := range snapshot.Entries {
		if !entry.Attention || entry.AttentionSeq == 0 || owned(entry.Target.LifecycleID) {
			continue
		}
		if !found || entry.AttentionSeq < best.AttentionSeq {
			best, found = entry, true
		}
	}
	if !found || snapshot.Generation == 0 {
		return protocol.RouteNavigationAction{}, false
	}
	return protocol.RouteNavigationAction{SnapshotGeneration: snapshot.Generation, Key: best.Key, Generation: best.Generation}, true
}

// ownsLifecycle reports whether this daemon holds the lifecycle, live or
// stopped.
func (d *Daemon) ownsLifecycle(lifecycle domain.SessionLifecycleID) bool {
	d.mu.Lock()
	sessions := sessionsSnapshot(d.sessions)
	stopped := false
	for _, inactive := range d.inactive {
		stopped = stopped || inactive.incarnation == lifecycle
	}
	d.mu.Unlock()
	if stopped {
		return true
	}
	for _, sess := range sessions {
		sess.mu.Lock()
		owned := sess.incarnation == lifecycle
		sess.mu.Unlock()
		if owned {
			return true
		}
	}
	return false
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
		if sess != nil && sess != current {
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

// oldestOtherSessionDone returns the tab of another local session whose agent
// finished first and is still unseen. Nothing is a target while activity is
// off.
func (d *Daemon) oldestOtherSessionDone(current *session) (attentionTarget, bool) {
	if !d.currentRuntimeConfig().agents.Activity {
		return attentionTarget{}, false
	}
	d.mu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, sess := range d.sessions {
		if sess != nil && sess != current {
			sessions = append(sessions, sess)
		}
	}
	d.mu.Unlock()

	var target attentionTarget
	var best uint64
	found := false
	for _, sess := range sessions {
		idx, seq, ok := oldestDoneTab(sess, "")
		if !ok {
			continue
		}
		if !found || seq < best {
			target, best, found = attentionTarget{sessionID: sess.id, tabIndex: idx}, seq, true
		}
	}
	return target, found
}

// oldestDoneTab returns the tab of sess showing a Done check whose agent
// finished first, skipping the tab skip.
func oldestDoneTab(sess *session, skip domain.TabStableID) (int, uint64, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	idx := -1
	var oldest uint64
	for i, tb := range sess.tabs {
		if skip != "" && domain.TabStableID(tb.stableID) == skip {
			continue
		}
		seq, ok := tb.agentDoneSeq()
		if !ok {
			continue
		}
		if idx == -1 || seq < oldest {
			idx, oldest = i, seq
		}
	}
	return idx, oldest, idx != -1
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

// refreshClientPickers republishes the picker source of every attachment whose
// interaction the client presents. A bell raised or cleared while that modal is
// open must reach it, and client-owned presentation suppresses the daemon's own
// paints: only a newer source revision updates the client's rows. Unchanged
// line sets publish nothing, so the attention pulse stays cheap.
func (d *Daemon) refreshClientPickers() {
	d.mu.Lock()
	sessions := sessionsSnapshot(d.sessions)
	d.mu.Unlock()

	for _, sess := range sessions {
		for _, ac := range sess.snapshotAttachments() {
			if ac.overlays == nil || !ac.overlays.pickerClientActive() {
				continue
			}
			d.refreshPickerSnapshot(ac)
		}
	}
}

func (d *Daemon) repaintAllAttachedClients() {
	d.mu.Lock()
	sessions := sessionsSnapshot(d.sessions)
	d.mu.Unlock()

	for _, sess := range sessions {
		d.repaintAttachedClients(sess)
	}
}

// ackAttention clears the bell of the tab ac shows. A client whose terminal
// window reported losing focus is not looked at, so it never clears a bell:
// the bell stays for every client until one that may be seen shows the tab.
func (s *session) ackAttention(tb *tab, ac *attachedClient, visible bool) bool {
	if tb == nil || ac == nil || !ac.terminalFocus().MaySee() {
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

// attentionFrame returns the bell pulse frame derived from the animator tick.
func (d *Daemon) attentionFrame() int {
	return int(d.animationTick() % pulseFrameCount)
}

// animationTick returns the monotonic animator tick. The bell pulse uses it
// modulo pulseFrameCount and the spinner modulo its frame count, so neither
// wraps inside the other's cycle.
func (d *Daemon) animationTick() uint64 {
	if d == nil {
		return 0
	}
	d.attnMu.Lock()
	defer d.attnMu.Unlock()
	return d.animTick
}

// animationTargets says what a pulse tick must repaint. A pending bell may
// show in any attachment's route entries, so it repaints every attachment
// (all). Agent animation (a Blocked agent's bell, a Working spinner, or an
// unseen Done check) shows in its own session's tab bar and in the session
// entry of every attachment whose route snapshot lists it, so it repaints
// those sessions only.
type animationTargets struct {
	all      bool
	sessions []*session
}

func (t animationTargets) active() bool { return t.all || len(t.sessions) > 0 }

// animatorActive reports whether some displayed cell animates. A Done check
// pulses until a paint acknowledges it.
func (d *Daemon) animatorActive() bool { return d.animationTargets().active() }

// animationTargets reads every session, attachment, and route snapshot, so
// the animator computes it once per tick.
func (d *Daemon) animationTargets() animationTargets {
	if d.anyAttention() {
		return animationTargets{all: true}
	}
	return animationTargets{sessions: d.agentAnimatingSessions()}
}

// agentAnimatingSessions lists the sessions with an animating agent tab and
// the sessions with an attachment whose bar lists an animating session.
func (d *Daemon) agentAnimatingSessions() []*session {
	cfg := d.currentRuntimeConfig().agents
	if !cfg.Activity {
		return nil
	}
	spinner := cfg.SpinnerEnabled()
	d.mu.Lock()
	sessions := sessionsSnapshot(d.sessions)
	d.mu.Unlock()
	animating := d.agentAnimatingLifecycles(spinner)
	out := make([]*session, 0, len(sessions))
	for _, sess := range sessions {
		if sess.agentAnimating(spinner) || sessionViewsLifecycle(sess, animating) {
			out = append(out, sess)
		}
	}
	return out
}

// agentAnimatingLifecycles returns the lifecycles whose session entry
// animates: a Blocked bell, a Done check, or a Working spinner when spinner
// is set.
func (d *Daemon) agentAnimatingLifecycles(spinner bool) map[domain.SessionLifecycleID]struct{} {
	if d.agents == nil {
		return nil
	}
	p := d.agents.sessions.Load()
	if p == nil {
		return nil
	}
	var out map[domain.SessionLifecycleID]struct{}
	for lifecycle, indicator := range *p {
		if indicator == agentactivity.IndicatorBlocked || indicator == agentactivity.IndicatorDone ||
			(spinner && indicator == agentactivity.IndicatorWorking) {
			if out == nil {
				out = make(map[domain.SessionLifecycleID]struct{})
			}
			out[lifecycle] = struct{}{}
		}
	}
	return out
}

// sessionViewsLifecycle reports whether an attachment of sess lists one of
// lifecycles in its bar.
func sessionViewsLifecycle(sess *session, lifecycles map[domain.SessionLifecycleID]struct{}) bool {
	if len(lifecycles) == 0 {
		return false
	}
	for _, ac := range sess.snapshotAttachments() {
		if routeShowsLifecycle(ac, lifecycles) {
			return true
		}
	}
	return false
}

func (d *Daemon) repaintAnimationTargets(targets animationTargets) {
	if targets.all {
		d.repaintAllAttachedClients()
		return
	}
	for _, sess := range targets.sessions {
		d.repaintAttachedClients(sess)
	}
}

// attentionAnimator advances the pulse and spinner tick while some cell
// animates and parks otherwise. It computes the animation targets once per
// wake or tick and uses them both to repaint and to decide whether to park.
// Open pickers are refreshed while a bell is pending and once more on the
// wake or tick that finds the last bell cleared, so they drop its mark.
func (d *Daemon) attentionAnimator(ctx context.Context) {
	var timer ports.Timer
	targets := d.animationTargets()
	prevAll := targets.all
	// next recomputes the targets and refreshes the pickers when a bell is
	// or was pending.
	next := func() {
		targets = d.animationTargets()
		if targets.all || prevAll {
			d.refreshClientPickers()
		}
		prevAll = targets.all
	}
	for {
		d.refreshAgentPickers()
		if !targets.active() {
			d.setAttentionFrame(0)
			select {
			case <-ctx.Done():
				return
			case <-d.animWake:
				next()
				continue
			}
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
			next()
		case <-timer.C():
			d.advanceAttentionFrame()
			// Picker marks are static except the pulsing bell: an agent's
			// label change arrives through refreshAgentPickers.
			next()
			d.repaintAnimationTargets(targets)
		}

		if !targets.active() {
			d.setAttentionFrame(0)
			d.repaintAllAttachedClients()
		}
	}
}

// refreshAgentPickers republishes open pickers once after a session's agent
// aggregate changed their labels. Ticks refresh them only while a bell is
// pending and on the tick that clears the last one.
func (d *Daemon) refreshAgentPickers() {
	if d.agentPickerRefresh.Swap(false) {
		d.refreshClientPickers()
	}
}

func (d *Daemon) anyAttention() bool {
	d.mu.Lock()
	sessions := sessionsSnapshot(d.sessions)
	d.mu.Unlock()

	for _, sess := range sessions {
		if sess.anyAttention() {
			return true
		}
		for _, ac := range sess.snapshotAttachments() {
			snapshot := ac.routeSnapshotCopy()
			for _, entry := range snapshot.Entries {
				if entry.Attention {
					return true
				}
			}
		}
	}
	return false
}

func (d *Daemon) advanceAttentionFrame() {
	d.attnMu.Lock()
	d.animTick++
	d.attnMu.Unlock()
}

// setAttentionFrame sets the animator tick; the animator resets it to 0 when
// it parks.
func (d *Daemon) setAttentionFrame(frame int) {
	d.attnMu.Lock()
	d.animTick = uint64(max(frame, 0))
	d.attnMu.Unlock()
}
