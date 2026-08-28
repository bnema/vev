package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func TestReportAttachmentErrorPreservesLocalOrNilScope(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	local := &session{sessionCore: sessionCore{id: "local", name: "local"}}
	d.reportAttachmentError(local, domain.UserWarn(domain.NoticeClipboard, "local failure", nil))

	history := d.notices.history()
	require.Len(t, history, 1)
	require.Equal(t, local.id, history[0].SessionID)
}

func TestClientNoticeMapsFixedActionsAndDismissesOnlyConnectionToast(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, &noticeClock{})
	d.handleClientNotice(sess, ac, protocol.ClientNotice{Action: protocol.ClientNoticeClipboardFallback})
	d.handleClientNotice(sess, ac, protocol.ClientNotice{Action: protocol.ClientNoticeClipboardTooLarge})
	d.handleClientNotice(sess, ac, protocol.ClientNotice{Action: protocol.ClientNoticeLinkDegraded})

	toasts, _ := visibleToasts(ac)
	require.Len(t, toasts, 3)
	require.Equal(t, []domain.Notification{
		{Code: domain.NoticeConnection, Severity: domain.NoticeWarn, Message: "connection degraded", SessionID: sess.id, Count: 1},
		{Code: domain.NoticeClipboardTooLarge, Severity: domain.NoticeWarn, Message: "image too large to paste", SessionID: sess.id, Count: 1},
		{Code: domain.NoticeClipboard, Severity: domain.NoticeError, Message: "image paste failed; sent Ctrl+V", SessionID: sess.id, Count: 1},
	}, stripNoticeTimes(toasts))

	d.handleClientNotice(sess, ac, protocol.ClientNotice{Action: protocol.ClientNoticeLinkConnected})
	toasts, _ = visibleToasts(ac)
	require.Len(t, toasts, 2)
	for _, toast := range toasts {
		require.NotEqual(t, domain.NoticeConnection, toast.Code)
	}
}

func TestMalformedClientNoticeIsIgnored(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, &noticeClock{})
	d.attachCoordinator(sess, nil, ac, true)
	tr, ok := ac.tr.(*portsmocks.MockTransport)
	require.True(t, ok, "attached client transport must be a MockTransport")
	frames := []wire.Frame{
		{Type: wire.MsgClientNotice, Payload: []byte{0xff}},
		{Type: wire.MsgDetach, Payload: wire.MarshalDetach(protocol.Detach{})},
	}
	tr.EXPECT().Recv().RunAndReturn(func() (wire.Frame, error) {
		frame := frames[0]
		frames = frames[1:]
		return frame, nil
	}).Times(2)
	d.runConnLoop(ac)
	toasts, _ := visibleToasts(ac)
	require.Empty(t, toasts)
	require.Empty(t, d.notices.history())
}

func stripNoticeTimes(notices []domain.Notification) []domain.Notification {
	out := append([]domain.Notification(nil), notices...)
	for i := range out {
		out[i].Time = time.Time{}
	}
	return out
}

func notice(code domain.NoticeCode, msg string, sid domain.SessionID) domain.Notification {
	return domain.Notification{Code: code, Severity: domain.NoticeError, Message: msg, Time: time.Unix(1, 0), SessionID: sid}
}

func TestNoticeCenterRingEviction(t *testing.T) {
	nc := newNoticeCenter()
	for i := 0; i < 205; i++ {
		nc.record(notice(domain.NoticeInternal, fmt.Sprintf("n%d", i), ""))
	}
	h := nc.history()
	if len(h) != 200 {
		t.Fatalf("history len = %d, want 200", len(h))
	}
	if h[0].Message != "n204" {
		t.Fatalf("history[0] = %q, want n204 (newest insert)", h[0].Message)
	}
	if h[199].Message != "n5" {
		t.Fatalf("history[199] = %q, want n5 (oldest retained after eviction)", h[199].Message)
	}
	// Every slot must hold exactly the expected message: strictly newest-first,
	// no gaps, and none of the evicted n0..n4 leaked back in.
	for i, n := range h {
		want := fmt.Sprintf("n%d", 204-i)
		if n.Message != want {
			t.Fatalf("history[%d] = %q, want %q", i, n.Message, want)
		}
	}
}

func TestNoticeCenterHistoryNewestFirst(t *testing.T) {
	nc := newNoticeCenter()
	nc.record(notice(domain.NoticePaneSpawn, "first", ""))
	nc.record(notice(domain.NoticeTabSpawn, "second", ""))
	h := nc.history()
	if h[0].Message != "second" || h[1].Message != "first" {
		t.Fatalf("history order = %q,%q; want second,first", h[0].Message, h[1].Message)
	}
	last, ok := nc.latest()
	if !ok || last.Message != "second" {
		t.Fatalf("latest = %v,%v", last.Message, ok)
	}
}

func TestNoticeCenterPendingQueue(t *testing.T) {
	t.Run("dedup coalesces by code without growing the queue", func(t *testing.T) {
		nc := newNoticeCenter()
		for i := 0; i < 40; i++ {
			nc.queueGlobal(notice(domain.NoticeSnapshotRestore, "restore failed", ""))
		}
		nc.queueGlobal(notice(domain.NoticePersistDisabled, "persistence disabled", ""))
		pending := nc.drainPending()
		if len(pending) != 2 {
			t.Fatalf("pending len = %d, want 2 (deduped by code)", len(pending))
		}
		if pending[0].Count != 40 {
			t.Fatalf("coalesced Count = %d, want 40", pending[0].Count)
		}
		if got := nc.drainPending(); len(got) != 0 {
			t.Fatalf("second drain len = %d, want 0", len(got))
		}
	})

	t.Run("cap overflow drops the 33rd distinct code", func(t *testing.T) {
		nc := newNoticeCenter()
		for i := 0; i < 33; i++ {
			nc.queueGlobal(notice(domain.NoticeCode(i), fmt.Sprintf("code %d", i), ""))
		}
		pending := nc.drainPending()
		if len(pending) != 32 {
			t.Fatalf("pending len = %d, want 32 (bounded at cap)", len(pending))
		}
		for i, n := range pending {
			if n.Code != domain.NoticeCode(i) {
				t.Fatalf("pending[%d].Code = %v, want %v (first 32 distinct codes retained, in order)", i, n.Code, domain.NoticeCode(i))
			}
		}
		for _, n := range pending {
			if n.Code == domain.NoticeCode(32) {
				t.Fatalf("pending contains code %v, which should have been dropped on overflow", domain.NoticeCode(32))
			}
		}
	})

	t.Run("dedup on an already-queued code still increments Count at the cap", func(t *testing.T) {
		nc := newNoticeCenter()
		for i := 0; i < 32; i++ {
			nc.queueGlobal(notice(domain.NoticeCode(i), fmt.Sprintf("code %d", i), ""))
		}
		// The queue is now full. Re-queuing an identical notice must coalesce
		// in place, not hit the overflow-drop branch.
		nc.queueGlobal(notice(domain.NoticeCode(0), "code 0", ""))
		nc.queueGlobal(notice(domain.NoticeCode(0), "code 0", ""))
		pending := nc.drainPending()
		if len(pending) != 32 {
			t.Fatalf("pending len = %d, want 32 (dedup must not grow past cap)", len(pending))
		}
		if pending[0].Code != domain.NoticeCode(0) || pending[0].Count != 3 {
			t.Fatalf("pending[0] = {Code:%v Count:%d}, want {Code:%v Count:3}", pending[0].Code, pending[0].Count, domain.NoticeCode(0))
		}
	})
}

// --- Task 3: routing, per-client toast state, attach drain ------------------

// noticeClock hands out timers the test fires explicitly by advancing time.
// Toast TTLs are asserted from the durations recorded here, so the tests never
// depend on wall-clock progress.
type noticeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*noticeTimer
}

type noticeTimer struct {
	ch       chan time.Time
	duration time.Duration
	deadline time.Time
	stopped  atomic.Bool
}

func newNoticeClock() *noticeClock {
	return &noticeClock{now: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)}
}

func (c *noticeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *noticeClock) NewTimer(d time.Duration) ports.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &noticeTimer{ch: make(chan time.Time, 1), duration: d, deadline: c.now.Add(d)}
	c.timers = append(c.timers, t)
	return t
}

func (t *noticeTimer) C() <-chan time.Time { return t.ch }

func (t *noticeTimer) Reset(time.Duration) bool { return false }

func (t *noticeTimer) Stop() bool {
	t.stopped.Store(true)
	return true
}

// advance moves the clock forward and fires every live timer that came due.
// Sends are non-blocking on a buffered channel, so no timer goroutine can wedge
// the test goroutine.
func (c *noticeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*noticeTimer, 0, len(c.timers))
	kept := make([]*noticeTimer, 0, len(c.timers))
	for _, t := range c.timers {
		if !t.stopped.Load() && !t.deadline.After(c.now) {
			t.stopped.Store(true)
			due = append(due, t)
			continue
		}
		kept = append(kept, t)
	}
	c.timers = kept
	now := c.now
	c.mu.Unlock()
	for _, t := range due {
		select {
		case t.ch <- now:
		default:
		}
	}
}

// durations returns every TTL requested so far, in creation order.
func (c *noticeClock) durations() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, 0, len(c.timers))
	for _, t := range c.timers {
		out = append(out, t.duration)
	}
	return out
}

// newNoticeFixture builds a manual session owned by one attached client, with a
// caller-supplied clock so toast TTLs are deterministic. The session has no
// render coordinator, so invalidateRender paints directly and every repaint is
// observable as a frame on the returned channel.
func newNoticeFixture(t *testing.T, clk ports.Clock) (*Daemon, *session, *attachedClient, chan wire.Frame) {
	t.Helper()
	p, _ := newBlockingPTY(t)
	d := newTestDaemon(t, nil, clk)
	tr, sends := newCapturingTransport(t)
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	sctx, cancel := context.WithCancel(d.serveCtx)
	wctx, wcancel := context.WithCancel(sctx)
	tb := newTab(p, domain.Size{Cols: 80, Rows: 23})
	tb.ctx, tb.cancel = wctx, wcancel
	for _, pane := range tb.panes {
		pane.ctx, pane.cancel = wctx, wcancel
	}
	sess := &session{sessionCore: sessionCore{id: "manual", name: "work", attachments: map[*attachedClient]struct{}{ac: {}}}, ctx: sctx, cancel: cancel, tabs: []*tab{tb}}
	ac.setSession(sess)
	d.sessions[sess.id] = sess
	t.Cleanup(cancel)
	return d, sess, ac, sends
}

// visibleToasts copies the client's toast state under noticeMu.
func visibleToasts(ac *attachedClient) ([]domain.Notification, int) {
	rt := ac.overlays
	rt.noticeMu.Lock()
	defer rt.noticeMu.Unlock()
	out := make([]domain.Notification, 0, len(rt.noticeToasts))
	for _, toast := range rt.noticeToasts {
		out = append(out, toast.n)
	}
	return out, rt.noticeOverflow
}

// awaitToastCount waits for the toast slice to reach want. The polled condition
// only reads state; assertions stay on the test goroutine.
func awaitToastCount(t *testing.T, ac *attachedClient, want int) []domain.Notification {
	t.Helper()
	require.Eventually(t, func() bool {
		ns, _ := visibleToasts(ac)
		return len(ns) == want
	}, 2*time.Second, time.Millisecond, "toast count never reached %d", want)
	got, _ := visibleToasts(ac)
	return got
}

func TestReportErrorUserErrorBecomesNotification(t *testing.T) {
	tests := []struct {
		name    string
		err     *domain.UserError
		wantSev domain.NoticeSeverity
	}{
		{
			name:    "error severity",
			err:     domain.UserErr(domain.NoticePaneSpawn, "could not open pane", errors.New("fork/exec: no such file")),
			wantSev: domain.NoticeError,
		},
		{
			name:    "warn severity",
			err:     domain.UserWarn(domain.NoticeConfigReload, "config reload skipped", errors.New("parse: line 4")),
			wantSev: domain.NoticeWarn,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())

			d.reportError(sess, tc.err)

			hist := d.notices.history()
			require.Len(t, hist, 1)
			require.Equal(t, tc.err.Code, hist[0].Code)
			require.Equal(t, tc.wantSev, hist[0].Severity)
			require.Equal(t, tc.err.Msg, hist[0].Message)
			require.Equal(t, sess.id, hist[0].SessionID)
			require.Equal(t, 1, hist[0].Count)
			// Details carry the cause chain; the message never does.
			require.Contains(t, hist[0].Details, tc.err.Err.Error())
			require.NotContains(t, hist[0].Message, tc.err.Err.Error())

			toasts := awaitToastCount(t, ac, 1)
			require.Equal(t, tc.err.Msg, toasts[0].Message)
			require.Equal(t, tc.err.Code, toasts[0].Code)
		})
	}
}

func TestReportErrorDetailsJoinUnwrapChain(t *testing.T) {
	d, sess, _, _ := newNoticeFixture(t, newNoticeClock())
	root := errors.New("permission denied")
	mid := fmt.Errorf("open /dev/pts/3: %w", root)

	d.reportError(sess, domain.UserErr(domain.NoticePaneSpawn, "could not open pane", mid))

	hist := d.notices.history()
	require.Len(t, hist, 1)
	require.Equal(t, mid.Error()+" ← "+root.Error(), hist[0].Details)
}

func TestReportErrorUnknownErrorBecomesInternal(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())
	raw := errors.New("something unclassified broke")

	d.reportError(sess, raw)

	hist := d.notices.history()
	require.Len(t, hist, 1)
	require.Equal(t, domain.NoticeInternal, hist[0].Code)
	require.Equal(t, domain.NoticeError, hist[0].Severity)
	require.Equal(t, "internal error", hist[0].Message)
	require.Equal(t, raw.Error(), hist[0].Details)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, "internal error", toasts[0].Message)
}

func TestReportErrorBenignFiltered(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "nil", err: nil},
		{name: "context canceled", err: context.Canceled},
		{name: "wrapped context canceled", err: fmt.Errorf("pane read: %w", context.Canceled)},
		{name: "no neighbor", err: errNoNeighbor},
		{name: "wrapped no neighbor", err: fmt.Errorf("focus: %w", errNoNeighbor)},
		{name: "no clipboard image", err: ports.ErrNoClipboardImage},
		{name: "wrapped no clipboard image", err: fmt.Errorf("yank: %w", ports.ErrNoClipboardImage)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())

			d.reportError(sess, tc.err)

			require.Empty(t, d.notices.history())
			toasts, _ := visibleToasts(ac)
			require.Empty(t, toasts)
		})
	}
}

// TestReportErrorNonBenignIsNeverDropped guards the inverse of the filter: an
// error that merely reads like a sentinel must still reach history.
func TestReportErrorNonBenignIsNeverDropped(t *testing.T) {
	d, sess, _, _ := newNoticeFixture(t, newNoticeClock())

	d.reportError(sess, errors.New("no pane in that direction"))

	require.Len(t, d.notices.history(), 1, "a look-alike error must not be filtered by message text")
}

func TestNotifyRoutesToSessionClientOnly(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())

	other := &attachedClient{output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	other.initOverlays()
	sess2 := &session{sessionCore: sessionCore{id: "manual-2", name: "other", attachments: map[*attachedClient]struct{}{other: {}}}, ctx: sess.ctx, cancel: func() {}}
	other.setSession(sess2)
	d.mu.Lock()
	d.sessions[sess2.id] = sess2
	d.mu.Unlock()

	d.notify(sess, domain.NoticeInfo, domain.NoticeClipboard, "copied", nil)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, sess.id, toasts[0].SessionID)
	otherToasts, _ := visibleToasts(other)
	require.Empty(t, otherToasts, "another session's client must be untouched by session-scoped notices")
}

func TestNotifySessionScopedSerializesDetachThroughToastPublication(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())

	selected := make(chan bool, 1)
	releasePublication := make(chan struct{})
	d.notices.beforeSessionDelivery = func() {
		unlocked := d.notices.routingMu.TryLock()
		if unlocked {
			d.notices.routingMu.Unlock()
		}
		selected <- !unlocked
		<-releasePublication
	}
	t.Cleanup(func() {
		select {
		case <-releasePublication:
		default:
			close(releasePublication)
		}
	})

	notifyDone := make(chan struct{})
	go func() {
		d.notify(sess, domain.NoticeInfo, domain.NoticeUser, "hello", nil)
		close(notifyDone)
	}()
	require.True(t, <-selected, "routingMu must remain held from attachment selection through toast publication")

	detachStarted := make(chan struct{})
	detachDone := make(chan bool, 1)
	go func() {
		close(detachStarted)
		detachDone <- d.detachIfCurrent(sess, ac)
	}()
	<-detachStarted
	select {
	case <-detachDone:
		t.Fatal("detach completed before the selected toast was durably published")
	default:
	}

	close(releasePublication)
	<-notifyDone
	require.True(t, <-detachDone)

	toasts, _ := visibleToasts(ac)
	require.Len(t, toasts, 1)
	require.Equal(t, "hello", toasts[0].Message)
	require.Empty(t, d.notices.drainPending(), "a toast published before detach must not also be queued")
}

func TestNotifySessionScopedReleasesRoutingBeforeRepaint(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())

	repaintObserved := make(chan bool, 1)
	ac.renderStages.capture = func() {
		unlocked := d.notices.routingMu.TryLock()
		if unlocked {
			d.notices.routingMu.Unlock()
		}
		repaintObserved <- unlocked
	}

	d.notify(sess, domain.NoticeInfo, domain.NoticeUser, "hello", nil)

	require.True(t, <-repaintObserved, "notice repaint must run without routingMu held")
}

func TestSessionScopedNoticeQueuedWhileDetached(t *testing.T) {
	d, sessA, oldA, _ := newNoticeFixture(t, newNoticeClock())
	sessA.mu.Lock()
	clearAttachmentsForTestLocked(sessA)
	sessA.mu.Unlock()

	clientB := &attachedClient{output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	clientB.initOverlays()
	sessB := &session{sessionCore: sessionCore{id: "manual-2", name: "other"}, ctx: sessA.ctx, cancel: func() {}}
	clientB.setSession(sessB)
	d.mu.Lock()
	d.sessions[sessB.id] = sessB
	d.mu.Unlock()

	d.notify(sessA, domain.NoticeInfo, domain.NoticeUser, "hello from a script", nil)
	// Use the same code globally to prove pending coalescing includes scope.
	d.NotifyGlobal(domain.NoticeWarn, domain.NoticeUser, "global announcement", nil)

	history := d.notices.history()
	require.Len(t, history, 2)
	require.Equal(t, domain.NoticeUser, history[1].Code)
	oldToasts, _ := visibleToasts(oldA)
	require.Empty(t, oldToasts, "a detached session must not paint a toast")

	sessB.mu.Lock()
	sessB.registerAttachmentLocked(clientB)
	sessB.mu.Unlock()
	d.drainPendingForFirstPaint(sessB, clientB)
	bToasts, _ := visibleToasts(clientB)
	require.Len(t, bToasts, 1, "another session receives only the global notice")
	require.Equal(t, domain.NoticeUser, bToasts[0].Code)
	require.Empty(t, bToasts[0].SessionID)
	require.Equal(t, "global announcement", bToasts[0].Message)

	sessA.mu.Lock()
	sessA.registerAttachmentLocked(oldA)
	sessA.mu.Unlock()
	d.drainPendingForFirstPaint(sessA, oldA)
	aToasts, _ := visibleToasts(oldA)
	require.Len(t, aToasts, 1)
	require.Equal(t, domain.NoticeUser, aToasts[0].Code)
	require.Equal(t, sessA.id, aToasts[0].SessionID)
	require.Equal(t, "hello from a script", aToasts[0].Message)
	require.Equal(t, 1, aToasts[0].Count)

	secondA := &attachedClient{output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	secondA.initOverlays()
	secondA.setSession(sessA)
	sessA.mu.Lock()
	sessA.registerAttachmentLocked(secondA)
	sessA.mu.Unlock()
	d.drainPendingForFirstPaint(sessA, secondA)
	secondToasts, _ := visibleToasts(secondA)
	require.Empty(t, secondToasts, "a drained session notice must not be delivered twice")
}

func TestNotifyGlobalFansOutToAttachedClients(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())

	second := &attachedClient{output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	second.initOverlays()
	sess2 := &session{sessionCore: sessionCore{id: "manual-2", name: "other", attachments: map[*attachedClient]struct{}{second: {}}}, ctx: sess.ctx, cancel: func() {}}
	second.setSession(sess2)
	d.mu.Lock()
	d.sessions[sess2.id] = sess2
	d.mu.Unlock()

	d.NotifyGlobal(domain.NoticeWarn, domain.NoticeConfigReload, "config reload failed", nil)

	first := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.SessionID(""), first[0].SessionID, "global notices carry an empty session id")
	secondToasts := awaitToastCount(t, second, 1)
	require.Equal(t, domain.NoticeConfigReload, secondToasts[0].Code)
	require.Empty(t, d.notices.drainPending(), "delivered globals must not also be queued")
}

func TestNotifyGlobalDoesNotStrandNoticeWhenFirstPaintRacesQueue(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())
	sess.mu.Lock()
	clearAttachmentsForTestLocked(sess)
	sess.mu.Unlock()

	routeObserved := make(chan struct{})
	releaseRoute := make(chan struct{})
	d.notices.beforeQueueGlobal = func() {
		close(routeObserved)
		<-releaseRoute
	}
	globalDone := make(chan struct{})
	go func() {
		d.NotifyGlobal(domain.NoticeError, domain.NoticeSnapshotWrite, "snapshot write failed", nil)
		close(globalDone)
	}()
	<-routeObserved // global routing has observed the detached session

	// Publish the attachment and start firstPaint while global routing still
	// owns its gate. firstPaint must wait for the queue, then drain it.
	sess.mu.Lock()
	sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	paintDone := make(chan struct{})
	go func() {
		d.firstPaint(sess, ac)
		close(paintDone)
	}()

	close(releaseRoute)
	<-globalDone
	<-paintDone
	toasts, _ := visibleToasts(ac)
	require.Len(t, toasts, 1)
	require.Equal(t, domain.NoticeSnapshotWrite, toasts[0].Code)
	require.Empty(t, d.notices.drainPending())
}

func TestNotifyGlobalSerializesDetachWithDelivery(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())

	routeObserved := make(chan struct{})
	releaseRoute := make(chan struct{})
	d.notices.beforeGlobalDelivery = func() {
		close(routeObserved)
		<-releaseRoute
	}
	globalDone := make(chan struct{})
	go func() {
		d.NotifyGlobal(domain.NoticeWarn, domain.NoticeConfigReload, "config reload failed", nil)
		close(globalDone)
	}()
	<-routeObserved // ac is selected and routingMu is held

	detached := make(chan bool, 1)
	go func() { detached <- d.detachIfCurrent(sess, ac) }()
	close(releaseRoute)
	<-globalDone
	require.True(t, <-detached)

	// The selected attachment received the toast before it could become stale.
	toasts, _ := visibleToasts(ac)
	require.Len(t, toasts, 1)
	sess.mu.Lock()
	require.Empty(t, sess.snapshotAttachmentsLocked())
	sess.mu.Unlock()
}

func TestDetachedNoticeCoalescingPreservesDistinctContentAndSeverity(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())
	sess.mu.Lock()
	clearAttachmentsForTestLocked(sess)
	sess.mu.Unlock()

	d.notify(sess, domain.NoticeInfo, domain.NoticeUser, "build started", nil)
	d.notify(sess, domain.NoticeWarn, domain.NoticeUser, "build delayed", nil)
	d.notify(sess, domain.NoticeWarn, domain.NoticeUser, "build delayed", nil)

	sess.mu.Lock()
	sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	d.drainPendingForFirstPaint(sess, ac)

	toasts := awaitToastCount(t, ac, 2)
	require.Equal(t, "build delayed", toasts[0].Message)
	require.Equal(t, domain.NoticeWarn, toasts[0].Severity)
	require.Equal(t, 2, toasts[0].Count, "identical detached notices coalesce")
	require.Equal(t, "build started", toasts[1].Message)
	require.Equal(t, domain.NoticeInfo, toasts[1].Severity)
	require.Equal(t, 1, toasts[1].Count)
}

func TestNotifyGlobalQueuesWhenUnattached(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())
	sess.mu.Lock()
	clearAttachmentsForTestLocked(sess)
	sess.mu.Unlock()

	d.NotifyGlobal(domain.NoticeError, domain.NoticeSnapshotWrite, "snapshot write failed", nil)

	require.Len(t, d.notices.history(), 1)
	toasts, _ := visibleToasts(ac)
	require.Empty(t, toasts, "no attached client means nothing to paint yet")

	// Re-attach and let firstPaint drain the pending queue.
	sess.mu.Lock()
	sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	d.firstPaint(sess, ac)

	drained := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticeSnapshotWrite, drained[0].Code)
	require.Equal(t, "snapshot write failed", drained[0].Message)
	require.Empty(t, d.notices.drainPending(), "firstPaint must consume the queue")
}

// TestNotifyGlobalPersistDisabledDrainsAtFirstAttach guards the store-open
// failure contract (Task 13): app/run.go queues this exact code and message
// via NotifyGlobal before any client has attached, and the first attach must
// see it via firstPaint's pending-queue drain.
func TestNotifyGlobalPersistDisabledDrainsAtFirstAttach(t *testing.T) {
	d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())
	sess.mu.Lock()
	clearAttachmentsForTestLocked(sess)
	sess.mu.Unlock()

	cause := errors.New("open store.db: permission denied")
	d.NotifyGlobal(domain.NoticeWarn, domain.NoticePersistDisabled,
		"session persistence is disabled; sessions will not survive daemon restarts", cause)

	require.Len(t, d.notices.history(), 1)
	toasts, _ := visibleToasts(ac)
	require.Empty(t, toasts, "no attached client means nothing to paint yet")

	// Re-attach and let firstPaint drain the pending queue.
	sess.mu.Lock()
	sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	d.firstPaint(sess, ac)

	drained := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticePersistDisabled, drained[0].Code)
	require.Equal(t, domain.NoticeWarn, drained[0].Severity)
	require.Equal(t, "session persistence is disabled; sessions will not survive daemon restarts", drained[0].Message)
	require.Empty(t, d.notices.drainPending(), "firstPaint must consume the queue")
}

// levelCaptureHandler records the slog.Level of every record it's handed, so
// tests can assert on log verbosity without parsing formatted output.
type levelCaptureHandler struct {
	mu     sync.Mutex
	levels []slog.Level
}

func (h *levelCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *levelCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.levels = append(h.levels, r.Level)
	return nil
}

func (h *levelCaptureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *levelCaptureHandler) WithGroup(string) slog.Handler      { return h }

func TestNotifyLogsAtSeverityLevel(t *testing.T) {
	tests := []struct {
		name string
		sev  domain.NoticeSeverity
		want slog.Level
	}{
		{"info severity logs at info", domain.NoticeInfo, slog.LevelInfo},
		{"warn severity logs at warn", domain.NoticeWarn, slog.LevelWarn},
		{"error severity logs at error", domain.NoticeError, slog.LevelError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, newNoticeClock())
			capture := &levelCaptureHandler{}
			d.log = slog.New(capture)

			d.notify(nil, tt.sev, domain.NoticeClipboard, "msg", nil)

			require.Equal(t, []slog.Level{tt.want}, capture.levels)
		})
	}
}

func TestShowToastCoalesceAndTrim(t *testing.T) {
	t.Run("identical notices coalesce and renew their ttl", func(t *testing.T) {
		clk := newNoticeClock()
		d, sess, ac, _ := newNoticeFixture(t, clk)

		d.notify(sess, domain.NoticeInfo, domain.NoticeUser, "build finished", nil)
		d.notify(sess, domain.NoticeInfo, domain.NoticeUser, "build finished", nil)

		toasts := awaitToastCount(t, ac, 1)
		require.Equal(t, 2, toasts[0].Count)
		require.Equal(t, []time.Duration{4 * time.Second, 4 * time.Second}, clk.durations(), "coalescing renews the identical notice's ttl")
		_, overflow := visibleToasts(ac)
		require.Zero(t, overflow)
		require.Len(t, d.notices.history(), 2, "coalescing is display-only; history keeps both")
	})

	t.Run("same code and scope preserve distinct message and severity", func(t *testing.T) {
		clk := newNoticeClock()
		d, sess, ac, _ := newNoticeFixture(t, clk)

		d.notify(sess, domain.NoticeInfo, domain.NoticeUser, "build started", nil)
		d.notify(sess, domain.NoticeWarn, domain.NoticeUser, "build delayed", nil)

		toasts := awaitToastCount(t, ac, 2)
		require.Equal(t, "build delayed", toasts[0].Message)
		require.Equal(t, domain.NoticeWarn, toasts[0].Severity)
		require.Equal(t, "build started", toasts[1].Message)
		require.Equal(t, domain.NoticeInfo, toasts[1].Severity)
		require.Equal(t, []time.Duration{4 * time.Second, 6 * time.Second}, clk.durations(), "each distinct notice keeps its severity ttl")
	})

	t.Run("same code different scope does not coalesce", func(t *testing.T) {
		d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())

		d.notify(sess, domain.NoticeError, domain.NoticePaneSpawn, "could not open pane", nil)
		d.notify(nil, domain.NoticeError, domain.NoticePaneSpawn, "could not open pane", nil)

		toasts := awaitToastCount(t, ac, 2)
		require.Equal(t, 1, toasts[0].Count)
		require.Equal(t, 1, toasts[1].Count)
	})

	t.Run("four distinct codes trim to three newest first", func(t *testing.T) {
		d, sess, ac, _ := newNoticeFixture(t, newNoticeClock())
		codes := []domain.NoticeCode{
			domain.NoticePaneSpawn,
			domain.NoticeTabSpawn,
			domain.NoticeSnapshotWrite,
			domain.NoticeClipboard,
		}
		for _, c := range codes {
			d.notify(sess, domain.NoticeError, c, c.String(), nil)
		}

		toasts := awaitToastCount(t, ac, maxVisibleToasts)
		require.Equal(t, []domain.NoticeCode{codes[3], codes[2], codes[1]}, []domain.NoticeCode{
			toasts[0].Code, toasts[1].Code, toasts[2].Code,
		}, "newest first, oldest evicted")
		_, overflow := visibleToasts(ac)
		require.Equal(t, 1, overflow)
	})
}

func TestNoticeTTLPerSeverity(t *testing.T) {
	tests := []struct {
		name string
		sev  domain.NoticeSeverity
		want time.Duration
	}{
		{name: "info", sev: domain.NoticeInfo, want: 4 * time.Second},
		{name: "warn", sev: domain.NoticeWarn, want: 6 * time.Second},
		{name: "error", sev: domain.NoticeError, want: 8 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, noticeTTL(tc.sev))

			clk := newNoticeClock()
			d, sess, ac, _ := newNoticeFixture(t, clk)
			d.notify(sess, tc.sev, domain.NoticeClipboard, "msg", nil)
			awaitToastCount(t, ac, 1)
			require.Equal(t, []time.Duration{tc.want}, clk.durations(), "the retained toast timer uses the severity TTL")
		})
	}
}

func TestToastExpiresOnFakeClock(t *testing.T) {
	clk := newNoticeClock()
	d, sess, ac, sends := newNoticeFixture(t, clk)

	d.notify(sess, domain.NoticeError, domain.NoticePaneSpawn, "could not open pane", nil)
	awaitToastCount(t, ac, 1)
	shown := awaitFrame(t, sends, wire.MsgOutput)
	shownOutput, err := wire.UnmarshalOutput(shown.Payload)
	require.NoError(t, err)
	terminal := vt.NewScreen(80, 25)
	terminal.Write(shownOutput.Data)

	// Short of the 8s error TTL nothing expires.
	clk.advance(7 * time.Second)
	toasts, _ := visibleToasts(ac)
	require.Len(t, toasts, 1, "toast must survive until its TTL elapses")

	clk.advance(2 * time.Second)
	awaitToastCount(t, ac, 0)
	_, overflow := visibleToasts(ac)
	require.Zero(t, overflow, "overflow resets once no toast is visible")

	expired := awaitFrame(t, sends, wire.MsgOutput)
	expiredOutput, err := wire.UnmarshalOutput(expired.Payload)
	require.NoError(t, err)
	terminal.Write(expiredOutput.Data)
	ac.sendMu.Lock()
	base := ac.pipelineCache.frame.Clone()
	ac.sendMu.Unlock()
	require.Equal(t, frameRows(base), frameRows(terminal.Frame), "fake-clock expiry must restore every old toast cell")
}

// TestToastExpiryOnlyRemovesItsOwnEntry proves a fired timer cannot dismiss a
// toast that outlived it.
func TestToastExpiryOnlyRemovesItsOwnEntry(t *testing.T) {
	clk := newNoticeClock()
	d, sess, ac, _ := newNoticeFixture(t, clk)

	d.notify(sess, domain.NoticeInfo, domain.NoticeClipboard, "copied", nil) // 4s TTL
	awaitToastCount(t, ac, 1)
	d.notify(sess, domain.NoticeError, domain.NoticePaneSpawn, "boom", nil) // 8s TTL
	awaitToastCount(t, ac, 2)

	clk.advance(5 * time.Second)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticePaneSpawn, toasts[0].Code, "only the expired info toast is removed")
}

// TestToastExpiryRemovesOnlyTheEntryItArmed pins the removal predicate: an
// expiry timer identifies its toast by entry sequence, not by code and scope.
// Coalescing re-arms the entry under a fresh sequence, so a timer that was
// stopped but had already slipped past its select cannot dismiss the refreshed
// toast that replaced the one it belonged to.
func TestToastExpiryRemovesOnlyTheEntryItArmed(t *testing.T) {
	clk := newNoticeClock()
	d, sess, ac, _ := newNoticeFixture(t, clk)

	d.notify(sess, domain.NoticeInfo, domain.NoticeClipboard, "copied", nil)
	awaitToastCount(t, ac, 1)

	// Re-stamp the live entry's sequence, exactly as coalescing does, without
	// disturbing the already-armed timer. Its code and scope are unchanged.
	rt := ac.overlays
	rt.noticeMu.Lock()
	rt.noticeSeq++
	rt.noticeToasts[0].seq = rt.noticeSeq
	rt.noticeMu.Unlock()

	clk.advance(4 * time.Second)

	require.Never(t, func() bool {
		ns, _ := visibleToasts(ac)
		return len(ns) == 0
	}, 200*time.Millisecond, 5*time.Millisecond, "expiry must remove only the entry it armed")
}
