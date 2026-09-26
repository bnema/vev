package daemon

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/ui"
)

const (
	noticeHistoryCap = 200
	noticePendingCap = 32
	// maxVisibleToasts bounds the stack drawn over a client's screen; later
	// notices wait for a free slot and are counted as "+N more" meanwhile.
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

// benignNoticeError reports unclassified errors that are expected control flow
// rather than something the user needs to be told about. A caller that wraps one
// of these in an explicit *domain.UserError still reports its own message; this
// filter only keeps an unclassified occurrence from surfacing as an
// internal-error notice.
func benignNoticeError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, errNoNeighbor) ||
		errors.Is(err, ports.ErrNoClipboardImage) ||
		// A concurrent detach or token resume that invalidated a teardown's
		// participant snapshot is expected control flow: the caller observes an
		// explicit error and retries or reports it, so it must not also surface
		// as an internal-error notice.
		errors.Is(err, errSessionKillParticipantsChanged)
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

// reportError turns any error into a user-facing notice. An explicitly
// classified *domain.UserError always wins: the caller already decided what the
// user should see, so it is reported even when the cause chain also matches a
// routine sentinel (for example a retryable teardown abort). Any other error is
// dropped only when benignNoticeError says it is routine; unclassified errors
// become NoticeInternal.
func (d *Daemon) reportError(sess *session, err error) {
	if err == nil {
		return
	}
	if ue, ok := errors.AsType[*domain.UserError](err); ok {
		d.notify(sess, ue.Severity, ue.Code, ue.Msg, ue.Err)
		return
	}
	if benignNoticeError(err) {
		return
	}
	d.notify(sess, domain.NoticeError, domain.NoticeInternal, "internal error", err)
}

func (d *Daemon) reportAttachmentError(entry *session, err error) {
	d.reportError(entry, err)
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
	sessions := make([]*session, 0, len(d.sessions))
	for _, entry := range d.sessions {
		sessions = append(sessions, entry)
	}
	d.mu.Unlock()

	if d.notices.beforeGlobalDelivery != nil {
		d.notices.beforeGlobalDelivery()
	}
	targets := make([]*attachedClient, 0, len(sessions))
	for _, s := range sessions {
		if s == nil {
			continue
		}
		for _, ac := range s.snapshotAttachments() {
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
// A notice matching a visible or waiting one (sameToastNotice) adds to its
// count and restarts its lifetime; otherwise it takes a free slot or waits.
func (d *Daemon) publishToast(ac *attachedClient, n domain.Notification) bool {
	if ac == nil {
		return false
	}
	ac.initOverlays()
	rt := ac.overlays

	rt.noticeMu.Lock()
	defer rt.noticeMu.Unlock()
	if rt.noticeQueue == nil {
		rt.noticeQueue = ui.NewToastQueue[domain.Notification](ui.ToastQueueOptions{MaxVisible: maxVisibleToasts, MaxPending: noticePendingCap})
	}
	id := toastNoticeID(n)
	if prev, ok := rt.noticeQueue.Get(id); ok {
		prev.Value.Count += n.Count
		prev.Value.Time = n.Time
		n = prev.Value
	}
	rt.noticeQueue.Push(d.clock.Now(), ui.QueuedToast[domain.Notification]{ID: id, Value: n, Duration: noticeTTL(n.Severity)})
	d.armToastTimerLocked(ac)
	return true
}

// toastNoticeID is a notice's coalescing identity. Generic user notices and
// endpoint-scoped remote observations use their visible message and details;
// other typed daemon notices coalesce by code and session.
func toastNoticeID(n domain.Notification) string {
	id := strconv.Itoa(int(n.Code)) + "\x00" + string(n.SessionID)
	if n.Code != domain.NoticeUser && n.Code != domain.NoticeRemoteObservation {
		return id
	}
	return id + "\x00" + strconv.Itoa(int(n.Severity)) + "\x00" + n.Message + "\x00" + n.Details
}

// sameToastNotice reports whether a and b coalesce into one toast.
func sameToastNotice(a, b domain.Notification) bool {
	return toastNoticeID(a) == toastNoticeID(b)
}

// dismissToastWithoutRepaint removes only the specified toast. In particular, a
// link-connected event must not clear clipboard or other notice state.
func (d *Daemon) dismissToastWithoutRepaint(ac *attachedClient, code domain.NoticeCode, sid domain.SessionID) bool {
	if ac == nil || ac.overlays == nil {
		return false
	}
	rt := ac.overlays
	rt.noticeMu.Lock()
	defer rt.noticeMu.Unlock()
	if rt.noticeQueue == nil || !rt.noticeQueue.Dismiss(d.clock.Now(), toastNoticeID(domain.Notification{Code: code, SessionID: sid})) {
		return false
	}
	d.armToastTimerLocked(ac)
	return true
}

// armToastTimerLocked points the client's single toast timer at the queue's
// next expiry. Callers must hold noticeMu; the timer goroutine re-acquires it
// and releases it before repainting, so the callback never runs with noticeMu
// held.
func (d *Daemon) armToastTimerLocked(ac *attachedClient) {
	rt := ac.overlays
	due, ok := rt.noticeQueue.NextDeadline()
	if ok && due.Equal(rt.noticeDue) && rt.noticeTimer.timer != nil {
		return
	}
	rt.noticeTimer.stop()
	rt.noticeDue = time.Time{}
	if !ok {
		return
	}
	rt.noticeDue = due
	rt.noticeTimer.retain(d.clock, max(due.Sub(d.clock.Now()), 0), func(fired ports.Timer) {
		rt.noticeMu.Lock()
		rt.noticeQueue.Visible(d.clock.Now())
		// A fire that lost the race to a re-arm leaves the newer timer alone.
		if rt.noticeTimer.timer == fired {
			rt.noticeTimer.timer, rt.noticeTimer.done = nil, nil
			d.armToastTimerLocked(ac)
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
