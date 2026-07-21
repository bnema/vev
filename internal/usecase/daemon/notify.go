package daemon

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	noticeHistoryCap = 200
	noticePendingCap = 32
	// maxVisibleToasts bounds the stack drawn over a client's screen; anything
	// trimmed is counted in noticeOverflow and stays reachable in history.
	maxVisibleToasts = 3
)

// errNoNeighbor reports a directional focus move with nothing to move to. It is
// an ordinary outcome of navigation, never a user-facing failure.
var errNoNeighbor = errors.New("no pane in that direction")

// noticeCenter owns the daemon-wide notification history and the queue of
// global notices awaiting a first attached client. Its mu is leaf-level: no
// other lock is ever taken while holding it.
type noticeCenter struct {
	mu sync.Mutex
	// routingMu serializes global deliver-or-queue with the first-paint drain.
	// Attachment removal also takes it, so a client selected for a global toast
	// remains current until that toast is published.
	routingMu sync.Mutex
	ring      []domain.Notification
	pending   []domain.Notification
	// beforeQueueGlobal is a deterministic test seam after routing has observed
	// no attachments and before it queues the notice.
	beforeQueueGlobal func()
	// beforeGlobalDelivery is a deterministic test seam after targets have been
	// selected and before their toast stacks are touched.
	beforeGlobalDelivery func()
}

func newNoticeCenter() *noticeCenter { return &noticeCenter{} }

func (nc *noticeCenter) record(n domain.Notification) domain.Notification {
	if n.Count == 0 {
		n.Count = 1
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.ring = append(nc.ring, n)
	if len(nc.ring) > noticeHistoryCap {
		nc.ring = nc.ring[len(nc.ring)-noticeHistoryCap:]
	}
	return n
}

func (nc *noticeCenter) history() []domain.Notification {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	out := make([]domain.Notification, len(nc.ring))
	for i, n := range nc.ring {
		out[len(nc.ring)-1-i] = n
	}
	return out
}

func (nc *noticeCenter) latest() (domain.Notification, bool) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if len(nc.ring) == 0 {
		return domain.Notification{}, false
	}
	return nc.ring[len(nc.ring)-1], true
}

func (nc *noticeCenter) queueGlobal(n domain.Notification) {
	if n.Count == 0 {
		n.Count = 1
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	for i := range nc.pending {
		if nc.pending[i].Code == n.Code {
			nc.pending[i].Count += n.Count
			nc.pending[i].Time = n.Time
			return
		}
	}
	if len(nc.pending) >= noticePendingCap {
		return // history already has it via record(); toast is dropped
	}
	nc.pending = append(nc.pending, n)
}

func (nc *noticeCenter) drainPending() []domain.Notification {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	out := nc.pending
	nc.pending = nil
	return out
}

// noticeToast is one entry in a client's visible toast stack. seq identifies the
// entry across coalescing so a stale expiry timer removes only its own toast.
type noticeToast struct {
	n     domain.Notification
	seq   uint64
	timer pendingByteTimer
}

// noticeTTL is how long a toast of this severity stays visible.
func noticeTTL(sev domain.NoticeSeverity) time.Duration {
	switch sev {
	case domain.NoticeInfo:
		return 4 * time.Second
	case domain.NoticeWarn:
		return 6 * time.Second
	default:
		return 8 * time.Second
	}
}

// slogLevelFor maps a notice severity to the slog level it should log at, so
// routine info notices (e.g. a successful clipboard copy) don't drown out
// real warnings at the default VEV_LOG=info verbosity.
func slogLevelFor(sev domain.NoticeSeverity) slog.Level {
	switch sev {
	case domain.NoticeInfo:
		return slog.LevelInfo
	case domain.NoticeWarn:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

// noticeDetails renders the full Unwrap chain for the yank/history views. The
// toast itself never shows this — only the UserError's own message.
func noticeDetails(err error) string {
	if err == nil {
		return ""
	}
	var parts []string
	for e := err; e != nil; e = errors.Unwrap(e) {
		parts = append(parts, e.Error())
	}
	return strings.Join(parts, " ← ")
}

// benignNoticeError reports errors that are expected control flow rather than
// something the user needs to be told about.
func benignNoticeError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, errNoNeighbor) ||
		errors.Is(err, ports.ErrNoClipboardImage)
}

// persistShutdownSnapshotFailure records a terminal snapshot failure that
// cannot be toasted because daemon shutdown has already begun. It belongs to
// notification ownership so session teardown only decides whether shutdown
// makes the failure non-retryable.
func (d *Daemon) persistShutdownSnapshotFailure(name string, cause error) {
	if d.noticeStore == nil {
		return
	}
	if err := d.noticeStore.Append(domain.Notification{
		Code:     domain.NoticeSnapshotWrite,
		Severity: domain.NoticeError,
		Message:  "session " + name + " shut down without saving terminal state",
		Details:  noticeDetails(cause),
		Time:     d.clock.Now(),
	}); err != nil {
		d.log.Warn("persisting shutdown notice failed", "err", err, "session", name)
	}
}

// reportError turns any error into a user-facing notice. Unclassified errors
// become NoticeInternal: an error reaching here is never silently dropped
// unless benignNoticeError says it is routine.
func (d *Daemon) reportError(sess *session, err error) {
	if err == nil || benignNoticeError(err) {
		return
	}
	var ue *domain.UserError
	if errors.As(err, &ue) {
		d.notify(sess, ue.Severity, ue.Code, ue.Msg, ue.Err)
		return
	}
	d.notify(sess, domain.NoticeError, domain.NoticeInternal, "internal error", err)
}

// notify records a notice and routes it to whoever can see it. A nil sess means
// daemon-global: every attached client gets it, and if none is attached the
// notice waits in the pending queue for the next attach.
//
// Locking: sess.mu and d.mu are only held to snapshot; showToast is always
// called with no daemon, session, or pane lock held. Global routing holds the
// notice center's routingMu across selection and publication, which serializes
// it with attachment detach and pending-queue drains.
func (d *Daemon) notify(sess *session, sev domain.NoticeSeverity, code domain.NoticeCode, msg string, cause error) {
	n := domain.Notification{
		Code:     code,
		Severity: sev,
		Message:  msg,
		Details:  noticeDetails(cause),
		Time:     d.clock.Now(),
	}
	if sess != nil {
		n.SessionID = sess.id
	}
	n = d.notices.record(n)
	d.log.Log(d.serveCtx, slogLevelFor(sev), "user notice", "code", code.String(), "severity", sev, "msg", msg, "err", cause)

	if sess != nil {
		sess.mu.Lock()
		ac := sess.client
		sess.mu.Unlock()
		if ac != nil {
			d.showToast(ac, n)
		}
		return
	}

	d.deliverGlobal(n)
}

// deliverGlobal atomically selects all current attachments and either publishes
// to all of them or queues the notice. routingMu is deliberately held through
// showToast, but never with d.mu or sess.mu held: detach cannot turn a selected
// target stale between selection and publication, and firstPaint cannot drain
// before an unattached route has queued its notice.
func (d *Daemon) deliverGlobal(n domain.Notification) {
	d.mu.Lock()
	d.notices.routingMu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, s := range d.sessions {
		sessions = append(sessions, s)
	}
	d.mu.Unlock()
	defer d.notices.routingMu.Unlock()

	if d.notices.beforeGlobalDelivery != nil {
		d.notices.beforeGlobalDelivery()
	}
	delivered := false
	for _, s := range sessions {
		s.mu.Lock()
		ac := s.client
		s.mu.Unlock()
		if ac == nil {
			continue
		}
		d.showToast(ac, n)
		delivered = true
	}
	if !delivered {
		if d.notices.beforeQueueGlobal != nil {
			d.notices.beforeQueueGlobal()
		}
		d.notices.queueGlobal(n)
	}
}

// drainPendingForFirstPaint publishes pending globals only while ac remains the
// session's current attachment. It shares routingMu with deliverGlobal, so an
// unattached global route cannot queue after this drain has already happened.
func (d *Daemon) drainPendingForFirstPaint(sess *session, ac *attachedClient) {
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()

	sess.mu.Lock()
	current := sess.client == ac
	sess.mu.Unlock()
	if !current {
		return
	}
	for _, n := range d.notices.drainPending() {
		d.showToast(ac, n)
	}
}

// NotifyGlobal raises a daemon-wide notice from outside the daemon package.
func (d *Daemon) NotifyGlobal(sev domain.NoticeSeverity, code domain.NoticeCode, msg string, cause error) {
	d.notify(nil, sev, code, msg, cause)
}

// showToast publishes a notice into one client's toast stack. An identical
// code within the same scope coalesces into the existing entry and restarts its
// TTL; otherwise the notice is prepended and the stack trimmed. The repaint is
// issued after noticeMu is released.
func (d *Daemon) showToast(ac *attachedClient, n domain.Notification) {
	if ac == nil {
		return
	}
	ac.initOverlays()
	rt := ac.overlays

	rt.noticeMu.Lock()
	if i := rt.indexOfToastLocked(n.Code, n.SessionID); i >= 0 {
		rt.noticeToasts[i].n.Count += n.Count
		rt.noticeToasts[i].n.Time = n.Time
		rt.noticeToasts[i].n.Details = n.Details
		rt.noticeToasts[i].timer.stop()
		d.retainToastTimerLocked(ac, &rt.noticeToasts[i])
		rt.noticeMu.Unlock()
		d.repaintForNotice(ac)
		return
	}

	toast := noticeToast{n: n}
	rt.noticeToasts = append([]noticeToast{toast}, rt.noticeToasts...)
	if len(rt.noticeToasts) > maxVisibleToasts {
		for i := maxVisibleToasts; i < len(rt.noticeToasts); i++ {
			rt.noticeToasts[i].timer.stop()
			rt.noticeOverflow++
		}
		rt.noticeToasts = rt.noticeToasts[:maxVisibleToasts]
	}
	d.retainToastTimerLocked(ac, &rt.noticeToasts[0])
	rt.noticeMu.Unlock()
	d.repaintForNotice(ac)
}

// indexOfToastLocked finds a visible toast with the same code and scope.
// Callers must hold noticeMu.
func (rt *overlayRuntime) indexOfToastLocked(code domain.NoticeCode, sid domain.SessionID) int {
	for i := range rt.noticeToasts {
		if rt.noticeToasts[i].n.Code == code && rt.noticeToasts[i].n.SessionID == sid {
			return i
		}
	}
	return -1
}

// dismissToast removes only the specified visible toast. In particular, a
// link-connected event must not clear clipboard or other notice state.
func (d *Daemon) dismissToast(ac *attachedClient, code domain.NoticeCode, sid domain.SessionID) {
	if ac == nil || ac.overlays == nil {
		return
	}
	rt := ac.overlays
	rt.noticeMu.Lock()
	i := rt.indexOfToastLocked(code, sid)
	if i < 0 {
		rt.noticeMu.Unlock()
		return
	}
	rt.noticeToasts[i].timer.stop()
	rt.noticeToasts = append(rt.noticeToasts[:i], rt.noticeToasts[i+1:]...)
	if len(rt.noticeToasts) == 0 {
		rt.noticeOverflow = 0
	}
	rt.noticeMu.Unlock()
	d.repaintForNotice(ac)
}

// retainToastTimerLocked arms the toast's TTL. Callers must hold noticeMu; the
// timer goroutine re-acquires it and releases it before repainting, so the
// callback never runs with noticeMu held.
func (d *Daemon) retainToastTimerLocked(ac *attachedClient, t *noticeToast) {
	rt := ac.overlays
	rt.noticeSeq++
	t.seq = rt.noticeSeq
	seq := t.seq
	t.timer.retain(d.clock, noticeTTL(t.n.Severity), func(ports.Timer) {
		rt.noticeMu.Lock()
		kept := rt.noticeToasts[:0]
		for _, tt := range rt.noticeToasts {
			if tt.seq == seq {
				continue
			}
			kept = append(kept, tt)
		}
		rt.noticeToasts = kept
		if len(rt.noticeToasts) == 0 {
			rt.noticeOverflow = 0
		}
		rt.noticeMu.Unlock()
		d.repaintForNotice(ac)
	})
}

// repaintForNotice asks for an urgent redraw. It must be called with noticeMu
// released: invalidateRender can paint inline, which takes sendMu.
func (d *Daemon) repaintForNotice(ac *attachedClient) {
	sess := ac.currentSession()
	if sess == nil {
		// Mid-handoff or torn down: whoever attaches next repaints anyway, and
		// invalidateRender cannot paint without a session.
		return
	}
	// Toast footprints carry their own old/new damage, so a normal notice
	// transition can remain incremental. First paint and transport recovery
	// still provide their independent reset invariants.
	d.invalidateRender(sess, ac, false, "notify.go")
}
