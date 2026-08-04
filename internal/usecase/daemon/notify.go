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
// global or session-scoped notices awaiting an eligible attached client. Its
// mu is leaf-level: no other lock is ever taken while holding it.
type noticeCenter struct {
	mu sync.Mutex
	// routingMu serializes notice deliver-or-queue with the first-paint drain.
	// Attachment removal also takes it, so a client selected for a toast remains
	// current until that toast is published.
	routingMu sync.Mutex
	ring      []domain.Notification
	pending   []domain.Notification
	// beforeQueueGlobal is a deterministic test seam after routing has observed
	// no attachments and before it queues the notice.
	beforeQueueGlobal func()
	// beforeGlobalDelivery is a deterministic test seam after targets have been
	// selected and before their toast stacks are touched.
	beforeGlobalDelivery func()
	// beforeSessionDelivery is a deterministic test seam after a session client
	// has been selected and before its toast stack is touched.
	beforeSessionDelivery func()
	// beforeClientNoticeMutation is a deterministic test seam after a client
	// notice's ownership validation and before its toast state is changed.
	beforeClientNoticeMutation func()
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

// recordSnapshotFailure coalesces a repeated stable snapshot failure in the
// history. Details contains the signature rather than raw error text, so this
// comparison cannot be affected by volatile paths or snapshot content.
func (nc *noticeCenter) recordSnapshotFailure(n domain.Notification) (domain.Notification, bool) {
	if n.Count == 0 {
		n.Count = 1
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	for i := len(nc.ring) - 1; i >= 0; i-- {
		previous := &nc.ring[i]
		if previous.Code != domain.NoticeSnapshotWrite || previous.SessionID != n.SessionID || previous.Details != n.Details {
			continue
		}
		previous.Count += n.Count
		previous.Time = n.Time
		return *previous, true
	}
	nc.ring = append(nc.ring, n)
	if len(nc.ring) > noticeHistoryCap {
		nc.ring = nc.ring[len(nc.ring)-noticeHistoryCap:]
	}
	return n, false
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
		if sameToastNotice(nc.pending[i], n) {
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
	d.shutdownNoticeMu.Lock()
	if d.shutdownNoticedSessions == nil {
		d.shutdownNoticedSessions = make(map[string]struct{})
	}
	if _, exists := d.shutdownNoticedSessions[name]; exists {
		d.shutdownNoticeMu.Unlock()
		return
	}
	d.shutdownNoticedSessions[name] = struct{}{}
	d.shutdownNoticeMu.Unlock()
	if err := d.noticeStore.Append(domain.Notification{
		Code:     domain.NoticeSnapshotWrite,
		Severity: domain.NoticeError,
		Message:  "session " + name + " shut down without saving terminal state",
		Details:  noticeDetails(cause),
		Time:     d.clock.Now(),
	}); err != nil {
		d.shutdownNoticeMu.Lock()
		delete(d.shutdownNoticedSessions, name)
		d.shutdownNoticeMu.Unlock()
		d.log.Warn("persisting shutdown notice failed", "err", err, "session", name, "action", "final-checkpoint-timeout")
	}
}

// reportError turns any error into a user-facing notice. Unclassified errors
// become NoticeInternal: an error reaching here is never silently dropped
// unless benignNoticeError says it is routine.
func (d *Daemon) reportError(sess *session, err error) {
	if err == nil || benignNoticeError(err) {
		return
	}
	if ue, ok := errors.AsType[*domain.UserError](err); ok {
		d.notify(sess, ue.Severity, ue.Code, ue.Msg, ue.Err)
		return
	}
	d.notify(sess, domain.NoticeError, domain.NoticeInternal, "internal error", err)
}

func (d *Daemon) reportAttachmentError(entry attachmentSession, err error) {
	local, _ := localSession(entry)
	d.reportError(local, err)
}

// notify records a notice and routes it to whoever can see it. A nil sess means
// daemon-global: every attached client gets it, and if none is attached the
// notice waits in the pending queue for the next attach. A detached session's
// notice waits for that session's next attach.
//
// Locking: sess.mu and d.mu are only held to snapshot; showToast is always
// called with no daemon, session, or pane lock held. Routing holds the notice
// center's routingMu across selection and publication, which serializes it with
// attachment detach and pending-queue drains. The order is routingMu -> sess.mu.
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
		d.notices.routingMu.Lock()
		sess.mu.Lock()
		attachments := sess.snapshotAttachmentsLocked()
		sess.mu.Unlock()
		if len(attachments) == 0 {
			d.notices.queueGlobal(n)
			d.notices.routingMu.Unlock()
			return
		}
		if d.notices.beforeSessionDelivery != nil {
			d.notices.beforeSessionDelivery()
		}
		published := make([]*attachedClient, 0, len(attachments))
		for _, ac := range attachments {
			if d.publishToast(ac, n) {
				published = append(published, ac)
			}
		}
		d.notices.routingMu.Unlock()
		for _, ac := range published {
			d.repaintForNotice(ac)
		}
		return
	}

	d.deliverGlobal(n)
}

// deliverGlobal atomically selects all current attachments and either publishes
// to all of them or queues the notice. routingMu is held through toast-state
// publication, but never through repaint: detach cannot turn a selected target
// stale before publication, while rendering remains free to re-enter routing.
func (d *Daemon) deliverGlobal(n domain.Notification) {
	d.mu.Lock()
	d.notices.routingMu.Lock()
	sessions := make([]attachmentSession, 0, len(d.sessions))
	for _, entry := range d.sessions {
		sessions = append(sessions, entry)
	}
	d.mu.Unlock()

	if d.notices.beforeGlobalDelivery != nil {
		d.notices.beforeGlobalDelivery()
	}
	targets := make([]*attachedClient, 0, len(sessions))
	for _, s := range sessions {
		for _, ac := range snapshotAttachmentSession(s) {
			if !d.publishToast(ac, n) {
				continue
			}
			targets = append(targets, ac)
		}
	}
	if len(targets) == 0 {
		if d.notices.beforeQueueGlobal != nil {
			d.notices.beforeQueueGlobal()
		}
		d.notices.queueGlobal(n)
	}
	d.notices.routingMu.Unlock()

	for _, ac := range targets {
		d.repaintForNotice(ac)
	}
}

// drainPendingForFirstPaint publishes pending globals and notices scoped to
// sess only while ac remains the session's current attachment. It shares
// routingMu with notice routing, so an unattached route cannot queue after this
// drain has already happened.
func (d *Daemon) drainPendingForFirstPaint(sess *session, ac *attachedClient) {
	d.notices.routingMu.Lock()

	sess.mu.Lock()
	current := attachmentRegisteredLocked(sess, ac)
	sess.mu.Unlock()
	if !current {
		d.notices.routingMu.Unlock()
		return
	}
	published := false
	var keep []domain.Notification
	for _, n := range d.notices.drainPending() {
		if n.SessionID == "" || n.SessionID == sess.id {
			published = d.publishToast(ac, n) || published
			continue
		}
		keep = append(keep, n)
	}
	for _, n := range keep {
		d.notices.queueGlobal(n)
	}
	d.notices.routingMu.Unlock()
	if published {
		d.repaintForNotice(ac)
	}
}

// NotifyGlobal raises a daemon-wide notice from outside the daemon package.
func (d *Daemon) NotifyGlobal(sev domain.NoticeSeverity, code domain.NoticeCode, msg string, cause error) {
	d.notify(nil, sev, code, msg, cause)
}

// publishToast durably mutates one client's toast state without repainting.
// Routing paths call it while routingMu excludes detach, then release routingMu
// before rendering so a render failure can safely route another notice.
func (d *Daemon) publishToast(ac *attachedClient, n domain.Notification) bool {
	if ac == nil {
		return false
	}
	ac.initOverlays()
	rt := ac.overlays

	rt.noticeMu.Lock()
	defer rt.noticeMu.Unlock()
	if i := rt.indexOfMatchingToastLocked(n); i >= 0 {
		rt.noticeToasts[i].n.Count += n.Count
		rt.noticeToasts[i].n.Time = n.Time
		rt.noticeToasts[i].timer.stop()
		d.retainToastTimerLocked(ac, &rt.noticeToasts[i])
		return true
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
	return true
}

func sameToastNotice(a, b domain.Notification) bool {
	if a.Code != b.Code || a.SessionID != b.SessionID {
		return false
	}
	// NoticeUser is intentionally generic: its message and selected severity
	// are the notice identity. Typed daemon notices retain their established
	// code-and-scope coalescing behavior.
	if a.Code != domain.NoticeUser {
		return true
	}
	return a.Severity == b.Severity &&
		a.Message == b.Message &&
		a.Details == b.Details
}

// indexOfMatchingToastLocked finds a visible toast with the code-specific
// coalescing identity. Count and time are occurrence metadata, not identity.
// Callers must hold noticeMu.
func (rt *overlayRuntime) indexOfMatchingToastLocked(n domain.Notification) int {
	for i := range rt.noticeToasts {
		if sameToastNotice(rt.noticeToasts[i].n, n) {
			return i
		}
	}
	return -1
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
func (d *Daemon) dismissToastWithoutRepaint(ac *attachedClient, code domain.NoticeCode, sid domain.SessionID) bool {
	if ac == nil || ac.overlays == nil {
		return false
	}
	rt := ac.overlays
	rt.noticeMu.Lock()
	defer rt.noticeMu.Unlock()
	i := rt.indexOfToastLocked(code, sid)
	if i < 0 {
		return false
	}
	rt.noticeToasts[i].timer.stop()
	rt.noticeToasts = append(rt.noticeToasts[:i], rt.noticeToasts[i+1:]...)
	if len(rt.noticeToasts) == 0 {
		rt.noticeOverflow = 0
	}
	return true
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
