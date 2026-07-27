package daemon

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/visualsearch"
)

// transactionalResizePTY is deliberately channel-scripted: a test can stop a
// transaction at PTY.Resize without relying on scheduler timing.
type transactionalResizePTY struct {
	mu        sync.Mutex
	sizes     []domain.Size
	writeData [][]byte
	errs      []error
	onResize  func()
	onWrite   func([]byte)
}

func (p *transactionalResizePTY) Resize(size domain.Size) error {
	p.mu.Lock()
	p.sizes = append(p.sizes, size)
	n := len(p.sizes)
	hook := p.onResize
	var err error
	if n <= len(p.errs) {
		err = p.errs[n-1]
	}
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
	return err
}
func (*transactionalResizePTY) Read([]byte) (int, error) { return 0, io.EOF }
func (p *transactionalResizePTY) Write(b []byte) (int, error) {
	p.mu.Lock()
	copied := append([]byte(nil), b...)
	p.writeData = append(p.writeData, copied)
	hook := p.onWrite
	p.mu.Unlock()
	if hook != nil {
		hook(copied)
	}
	return len(b), nil
}
func (*transactionalResizePTY) Close() error                 { return nil }
func (*transactionalResizePTY) Pid() int                     { return 0 }
func (*transactionalResizePTY) ForegroundPgid() (int, error) { return 0, nil }
func (p *transactionalResizePTY) requested() []domain.Size {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Size(nil), p.sizes...)
}

func (p *transactionalResizePTY) writes() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	writes := make([][]byte, len(p.writeData))
	for i := range p.writeData {
		writes[i] = append([]byte(nil), p.writeData[i]...)
	}
	return writes
}

// resizeReaderPTY is a deterministic child: Resize releases its redraw only
// after the daemon has installed resizeApplying, and waits until ptyReader has
// accepted that read. It deliberately has no timing dependency.
type resizeReaderPTY struct {
	redraw    []byte
	reads     chan []byte
	accepted  chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	applying  func() bool
	delivered bool
}

func newResizeReaderPTY(redraw []byte) *resizeReaderPTY {
	return &resizeReaderPTY{redraw: redraw, reads: make(chan []byte), accepted: make(chan struct{}), closed: make(chan struct{})}
}

func (p *resizeReaderPTY) Resize(domain.Size) error {
	p.reads <- p.redraw
	<-p.accepted
	// A second, empty read cannot begin until ptyReader has processed the
	// redraw from the first read. This is a channel rendezvous, not a timing
	// assumption, and proves the redraw entered resizePending before Resize
	// returns to replay it.
	p.reads <- nil
	<-p.accepted
	p.mu.Lock()
	p.delivered = p.applying != nil && p.applying()
	p.mu.Unlock()
	return nil
}
func (p *resizeReaderPTY) Read(dst []byte) (int, error) {
	select {
	case data := <-p.reads:
		n := copy(dst, data)
		p.accepted <- struct{}{}
		return n, nil
	case <-p.closed:
		return 0, io.EOF
	}
}
func (*resizeReaderPTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*resizeReaderPTY) Pid() int                     { return 0 }
func (*resizeReaderPTY) ForegroundPgid() (int, error) { return 0, nil }
func (p *resizeReaderPTY) Close() error               { p.close(); return nil }
func (p *resizeReaderPTY) close()                     { p.closeOnce.Do(func() { close(p.closed) }) }
func (p *resizeReaderPTY) deliveredWhileApplying() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.delivered
}

func TestReplayResizePendingBuffersSuccessFailureAndBatchOrder(t *testing.T) {
	t.Run("success resizes then replays", func(t *testing.T) {
		pty := &transactionalResizePTY{}
		d, sess, _, _ := newManualSessionWithPTYs(t, pty)
		tb, p := sess.activeTab(), sess.activeTab().focusedPane()
		p.resizeApplying = true
		p.resizePending = []byte("A\x1b[1;81HB")
		d.replayResizePending(sess, tb, p, true, domain.Rect{Width: 120, Height: 23})
		require.Equal(t, 120, p.screen.Frame.Width)
		require.Equal(t, 'B', p.screen.Frame.At(80, 0).Rune)
		require.False(t, p.resizeApplying)
	})
	t.Run("failure retains old parser width", func(t *testing.T) {
		pty := &transactionalResizePTY{}
		d, sess, _, _ := newManualSessionWithPTYs(t, pty)
		tb, p := sess.activeTab(), sess.activeTab().focusedPane()
		p.resizeApplying = true
		p.resizePending = []byte("\x1b[1;81HB")
		d.replayResizePending(sess, tb, p, false, domain.Rect{Width: 120, Height: 23})
		require.Equal(t, 80, p.screen.Frame.Width)
		require.Equal(t, 'B', p.screen.Frame.At(79, 0).Rune)
	})
	t.Run("batches retain read order", func(t *testing.T) {
		pty := &transactionalResizePTY{}
		d, sess, _, _ := newManualSessionWithPTYs(t, pty)
		tb, p := sess.activeTab(), sess.activeTab().focusedPane()
		p.screen.OnResponse = func([]byte) { p.ptyResponses = append(p.ptyResponses, 'r') }
		pty.onWrite = func([]byte) {
			p.mu.Lock()
			p.resizePending = append(p.resizePending, 'B')
			p.mu.Unlock()
		}
		p.resizeApplying = true
		p.resizePending = []byte("A\x1b[6n")
		d.replayResizePending(sess, tb, p, true, domain.Rect{Width: 80, Height: 23})
		require.Equal(t, "AB", screenLineText(p.screen, 0)[:2])
	})
}

func TestProcessPTYDataRetainsCallbacksDuringResizeReplay(t *testing.T) {
	responses := make(chan []byte, 1)
	pty := &transactionalResizePTY{onWrite: func(b []byte) { responses <- b }}
	d, sess, ac, sends := newManualSessionWithPTYs(t, pty)
	publishActiveClipboardCapability(d, sess, ac, ac.transport())
	tb, p := sess.activeTab(), sess.activeTab().focusedPane()
	p.screen.OnResponse = func(b []byte) { p.ptyResponses = append(p.ptyResponses, b...) }
	p.screen.OnBell = func() { p.ptyAttention = true }
	p.screen.OnClipboard = func(b64 string) { p.ptyClipboards = append(p.ptyClipboards, b64) }
	p.resizeApplying = true
	p.resizePending = []byte("\x1b]2;resized\a\a\x1b]52;c;YQ==\a\x1b[6n")
	d.replayResizePending(sess, tb, p, true, domain.Rect{Width: 80, Height: 23})

	p.mu.Lock()
	title := p.title.terminalTitle
	p.mu.Unlock()
	require.Equal(t, "resized", title)
	require.True(t, tb.attention)
	require.NotEmpty(t, <-responses, "DSR response must be flushed back to the child")
	// DSR is flushed back to the child and OSC 52 is forwarded through the
	// normal asynchronous client path, proving replay uses processPTYData.
	var clipboardOutput string
	for range 3 {
		frame := awaitFrame(t, sends, ports.MsgOutput)
		out, err := ports.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		clipboardOutput = string(out.Data)
		if strings.Contains(clipboardOutput, "\x1b]52;c;YQ==\a") {
			break
		}
	}
	require.Contains(t, clipboardOutput, "\x1b]52;c;YQ==\a")
}

// S3 acceptance: prepare may inspect layout under its locks, but apply must
// invoke PTY.Resize after all session/tab/pane locks are released. This is the
// deadlock boundary with PTY readers and scripts which synchronously re-enter
// daemon state.
func TestApplySessionLayoutStopsRetryWhenSessionCanceled(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, _, _ := newManualSessionWithPTYs(t, pty)
	tb, p := sess.activeTab(), sess.activeTab().focusedPane()
	d.beforeSessionResizePublication = sess.cancel

	_, ok := d.applySessionLayout(sess, domain.Size{Cols: 100, Rows: 30}, nil, nil)

	require.False(t, ok)
	require.Len(t, pty.requested(), 1, "cancellation must stop before a stale retry applies another PTY resize")
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 80, Rows: 23}, tb.size, "canceled session published a tab size")
	tb.mu.Unlock()
	p.mu.Lock()
	require.Equal(t, domain.Rect{Width: 80, Height: 23}, p.rect, "canceled session published a pane rectangle")
	require.Equal(t, domain.Size{Cols: 80, Rows: 23}, domain.Size{Cols: p.screen.Frame.Width, Rows: p.screen.Frame.Height}, "canceled session published a VT size")
	p.mu.Unlock()
}

func TestTransactionalResizeApplyDoesNotHoldTabOrPaneLocks(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, _, _ := newManualSessionWithPTYs(t, pty)
	tb := sess.activeTab()
	p := tb.focusedPane()
	var tabUnlocked, paneUnlocked bool
	pty.onResize = func() {
		tabUnlocked = tb.mu.TryLock()
		if tabUnlocked {
			tb.mu.Unlock()
		}
		paneUnlocked = p.mu.TryLock()
		if paneUnlocked {
			p.mu.Unlock()
		}
	}

	d.resize(sess, nil, domain.Size{Cols: 100, Rows: 30})

	require.True(t, tabUnlocked, "PTY.Resize must run outside tab.mu")
	require.True(t, paneUnlocked, "PTY.Resize must run outside pane.mu")
}

// S3 acceptance: a failed member never publishes speculative parser/screen or
// rectangle state. Successful members and the client layout commit together;
// composition clips/pads the retained failed screen against that new layout.
func TestApplySessionLayoutReportsResizeFailuresOnce(t *testing.T) {
	for _, tc := range []struct {
		name              string
		failureCount      int
		wantOK            bool
		wantFailed        int
		wantFailureNotice int
	}{
		{name: "zero failures", wantOK: true},
		{name: "one failure", failureCount: 1, wantOK: true, wantFailed: 1, wantFailureNotice: 1},
		{name: "two failures", failureCount: 2, wantOK: true, wantFailed: 2, wantFailureNotice: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := &transactionalResizePTY{}
			second := &transactionalResizePTY{}
			if tc.failureCount >= 1 {
				first.errs = []error{errors.New("first failure")}
			}
			if tc.failureCount >= 2 {
				second.errs = []error{errors.New("second failure")}
			}
			d, sess, _, _ := newManualSessionWithPTYs(t, first, second)

			failed, ok := d.applySessionLayout(sess, domain.Size{Cols: 100, Rows: 30}, nil, nil)

			require.Equal(t, tc.wantOK, ok)
			require.Len(t, failed, tc.wantFailed)
			count := 0
			for _, notification := range d.notices.history() {
				if notification.Code == domain.NoticeResizeFailed {
					count++
				}
			}
			require.Equal(t, tc.wantFailureNotice, count)
		})
	}
}

func TestTransactionalResizePartialFailureCommitsOnlySuccessfulPTYState(t *testing.T) {
	ok := &transactionalResizePTY{}
	failed := &transactionalResizePTY{errs: []error{errors.New("scripted resize failure")}}
	d, sess, ac, _ := newManualSessionWithPTYs(t, ok)
	tb := sess.activeTab()
	second := newPane("pane-2", failed, domain.Size{Cols: 80, Rows: 23})
	tb.mu.Lock()
	tb.panes[second.id] = second
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-1"}
	first := tb.panes["pane-1"]
	oldFailedRect := second.rect
	oldFailedScreen := domain.Size{Cols: second.screen.Frame.Width, Rows: second.screen.Frame.Height}
	tb.mu.Unlock()

	d.resize(sess, ac, domain.Size{Cols: 60, Rows: 20})

	require.Equal(t, domain.Size{Cols: 60, Rows: 18}, tb.size, "client layout commits as one epoch")
	require.Equal(t, domain.Rect{Width: 30, Height: 18}, first.rect)
	require.Equal(t, domain.Rect{X: 31, Width: 29, Height: 18}, second.rect,
		"all pane rectangles publish with the committed layout")
	require.Equal(t, []domain.Size{{Cols: 30, Rows: 18}}, ok.requested())
	require.Equal(t, []domain.Size{{Cols: 29, Rows: 18}}, failed.requested())
	require.Equal(t, oldFailedScreen, domain.Size{Cols: second.screen.Frame.Width, Rows: second.screen.Frame.Height},
		"failed PTY retains its parser/screen size for clipped padded composition")
	require.NotEqual(t, oldFailedRect, second.rect, "failed PTY rectangle still follows the committed layout")
}

// A retry changes VT state after the committed layout, so it needs a distinct
// snapshot generation. A failed retry must retain the prior screen and dirty
// generation.
func TestTransactionalResizeRetryMarksNamedSnapshotDirtyOnlyOnSuccess(t *testing.T) {
	resizeErr := errors.New("scripted resize failure")
	for _, tc := range []struct {
		name           string
		errs           []error
		wantGeneration uint64
		wantScreen     domain.Size
	}{
		{name: "success", errs: []error{resizeErr, nil}, wantGeneration: 2, wantScreen: domain.Size{Cols: 100, Rows: 28}},
		{name: "failure", errs: []error{resizeErr, resizeErr}, wantGeneration: 1, wantScreen: domain.Size{Cols: 80, Rows: 23}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pty := &transactionalResizePTY{errs: tc.errs}
			d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
			sess.snapEligible.Store(true)
			tb, p := sess.activeTab(), sess.activeTab().focusedPane()

			require.True(t, d.requestTransactionalResize(sess, ac, domain.Size{Cols: 100, Rows: 30}, true))
			rc := sess.renderCoordinator()
			snapshot := rc.resizeSnapshot()
			d.retryResizeMembers(sess, ac, rc.attachmentLease(ac), snapshot.epoch, []resizeMember{{session: sess, tab: tb, pane: p, rect: p.rect}})

			sess.snapshotMu.Lock()
			generation := sess.snapshotGeneration
			sess.snapshotMu.Unlock()
			require.Equal(t, tc.wantGeneration, generation)
			require.True(t, sess.snapDirty.Load())
			require.Equal(t, tc.wantScreen, domain.Size{Cols: p.screen.Frame.Width, Rows: p.screen.Frame.Height})
		})
	}
}

// S3 acceptance: a failed member is retried only for the newest committed
// epoch. An intervening request supersedes the old retry, and a successful
// retry must resize its VT before the forced full-frame emission.
func TestTransactionalResizeRetryTargetsNewestCommittedEpoch(t *testing.T) {
	ok := &transactionalResizePTY{}
	failed := &transactionalResizePTY{errs: []error{errors.New("first epoch fails"), nil}}
	d, sess, ac, _ := newManualSessionWithPTYs(t, ok)
	tb := sess.activeTab()
	retry := newPane("retry", failed, domain.Size{Cols: 80, Rows: 23})
	tb.mu.Lock()
	tb.panes[retry.id] = retry
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("retry")}}, Focus: "pane-1"}
	tb.mu.Unlock()

	d.resize(sess, ac, domain.Size{Cols: 100, Rows: 30}) // failed epoch
	d.resize(sess, ac, domain.Size{Cols: 120, Rows: 34}) // supersedes it

	// The failed pane must not be retried at 49x28 from the obsolete epoch.
	// S3 retries 59x32, then commits the parser/screen and requests a reset.
	require.Equal(t, []domain.Size{{Cols: 49, Rows: 28}, {Cols: 59, Rows: 32}}, failed.requested(),
		"the failed old apply is historical; only the authoritative newest epoch retries it")
	require.Equal(t, domain.Size{Cols: 59, Rows: 32}, domain.Size{Cols: retry.screen.Frame.Width, Rows: retry.screen.Frame.Height})
	require.Equal(t, uint64(2), sess.renderCoordinator().resizeSnapshot().committed,
		"the retry belongs to the newest committed epoch")
}

// Stale lifecycle callbacks may never advance a transaction owned by a
// replacement attachment. The coordinator test clock drives callbacks through
// channels elsewhere in this package; this ownership assertion is deliberately
// synchronous so it cannot hide a race behind a sleep.
func TestTransactionalResizeEpochLifecycleAndRetryContract(t *testing.T) {
	// Each closure is a callback captured before its lifecycle transition.  The
	// coordinator must reject it, rather than merely coalescing its paint.
	for _, transition := range []struct {
		name string
		run  func(*renderCoordinator, *attachedClient, *attachedClient)
	}{
		{"replace", func(rc *renderCoordinator, old, next *attachedClient) { rc.noteReplace(old, next) }},
		{"detach", func(rc *renderCoordinator, old, _ *attachedClient) { rc.noteDetach(old) }},
		{"park", func(rc *renderCoordinator, old, _ *attachedClient) { rc.notePark(old) }},
		{"resume", func(rc *renderCoordinator, old, next *attachedClient) { rc.notePark(old); rc.attach(next) }},
	} {
		t.Run(transition.name, func(t *testing.T) {
			rc := newRenderCoordinator(renderCoordinatorOptions{})
			old, next := &attachedClient{}, &attachedClient{}
			rc.attach(old)
			require.Equal(t, uint64(1), rc.recordResizeRequest(domain.Size{Cols: 90, Rows: 24}, old))
			stale := func() uint64 { return rc.recordResizeRequest(domain.Size{Cols: 20, Rows: 5}, old) }
			transition.run(rc, old, next)
			require.Zero(t, stale(), "a stale %s callback must not publish", transition.name)
		})
	}
}

// S3 acceptance: three callbacks can be delivered even after their timers were
// stopped.  Only the newest prepared epoch may apply, commit, and emit one full
// frame. coordinatorMockClock is a generated MockClock/MockTimer harness; its
// channels make callback order explicit and do not use wall-clock waits.
func TestTransactionalResizeRejectsNewerEpochBeforeSessionPublication(t *testing.T) {
	first, second := &transactionalResizePTY{}, &transactionalResizePTY{}
	d, sess, ac, _ := newManualSessionWithPTYs(t, first, second)
	observer := &daemonRuntimeObserver{}
	d.runtimeObserver = observer
	rc := d.attachCoordinator(sess, nil, ac, true)
	lease := rc.attachmentLease(ac)
	var newer uint64
	d.beforeSessionResizePublication = func() {
		// Final external PTY validation is complete, but epoch admission and
		// every session-visible publication must still be pending.
		for _, tb := range sess.tabs {
			require.Equal(t, domain.Size{Cols: 80, Rows: 23}, tb.size)
			p := tb.focusedPane()
			p.mu.Lock()
			require.Equal(t, domain.Rect{Width: 80, Height: 23}, p.rect)
			require.Equal(t, domain.Size{Cols: 80, Rows: 23}, domain.Size{Cols: p.screen.Frame.Width, Rows: p.screen.Frame.Height})
			p.mu.Unlock()
		}
		require.False(t, sess.snapDirty.Load())
		for _, mark := range observer.marks {
			require.NotEqual(t, ports.RuntimeResizeCommitted, mark.Kind)
		}
		newer = rc.recordResizeRequestForLease(domain.Size{Cols: 120, Rows: 34}, ac, lease)
		require.NotZero(t, newer)
	}

	require.False(t, d.requestTransactionalResize(sess, ac, domain.Size{Cols: 100, Rows: 30}, true))
	d.beforeSessionResizePublication = nil
	require.False(t, sess.snapDirty.Load(), "rejected epoch must not dirty the snapshot")
	for _, tb := range sess.tabs {
		require.Equal(t, domain.Size{Cols: 80, Rows: 23}, tb.size, "rejected epoch published a tab size")
		p := tb.focusedPane()
		require.Equal(t, domain.Rect{Width: 80, Height: 23}, p.rect, "rejected epoch published a pane rectangle")
		require.Equal(t, domain.Size{Cols: 80, Rows: 23}, domain.Size{Cols: p.screen.Frame.Width, Rows: p.screen.Frame.Height}, "rejected epoch published a VT size")
	}

	require.True(t, d.runResizeTransaction(sess, ac, lease, newer))
	for _, tb := range sess.tabs {
		require.Equal(t, domain.Size{Cols: 120, Rows: 32}, tb.size)
		p := tb.focusedPane()
		require.Equal(t, domain.Rect{Width: 120, Height: 32}, p.rect)
		require.Equal(t, domain.Size{Cols: 120, Rows: 32}, domain.Size{Cols: p.screen.Frame.Width, Rows: p.screen.Frame.Height})
	}
	committed := 0
	for _, mark := range observer.marks {
		if mark.Kind == ports.RuntimeResizeCommitted {
			committed++
		}
	}
	require.Equal(t, 1, committed, "only the current epoch emits commit telemetry")
	require.Equal(t, []domain.Size{{Cols: 100, Rows: 28}, {Cols: 120, Rows: 32}}, first.requested())
	require.Equal(t, []domain.Size{{Cols: 100, Rows: 28}, {Cols: 120, Rows: 32}}, second.requested())
}

func TestStaleRemovedMemberGateIsCanceledOnceByFreshPlan(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, _, _ := newManualSessionWithPTYs(t, pty)
	tb := sess.activeTab()
	p := tb.focusedPane()
	p.resizeApplying = true
	p.resizePending = []byte("x")
	plan := preparedTabLayout{members: []resizeMember{{session: sess, tab: tb, pane: p, rect: p.rect, screenResized: true}}}
	tb.mu.Lock()
	delete(tb.panes, p.id)
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()

	d.finishPreparedTabMembers(&plan, false)
	p.mu.Lock()
	require.True(t, p.resizeApplying, "stale finalization must leave cancellation to the fresh-plan path")
	p.mu.Unlock()
	d.cancelStalePreparedGates(sess, tb, &plan)
	p.mu.Lock()
	require.False(t, p.resizeApplying)
	require.Empty(t, p.resizePending)
	p.mu.Unlock()
}

func TestHeadlessResizeRetriesInvalidatedPlan(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, _, _ := newManualSessionWithPTYs(t, pty)
	tb := sess.activeTab()
	var invalidate sync.Once
	pty.onResize = func() {
		invalidate.Do(func() {
			tb.mu.Lock()
			tb.bumpLayoutGenerationLocked()
			tb.mu.Unlock()
		})
	}

	require.True(t, d.requestTransactionalResize(sess, nil, domain.Size{Cols: 100, Rows: 30}, true))
	require.Equal(t, []domain.Size{{Cols: 100, Rows: 28}, {Cols: 100, Rows: 28}}, pty.requested())
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 100, Rows: 28}, tb.size)
	tb.mu.Unlock()
}

func TestTransactionalResizeRetriesAcceptedFloatingSlotByIdentity(t *testing.T) {
	popupPTY := &transactionalResizePTY{errs: []error{errors.New("first popup resize fails")}}
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	tb := sess.activeTab()
	popup := newPane("popup", popupPTY, domain.Size{Cols: 80, Rows: 23})
	tb.mu.Lock()
	tb.floating = floatingSlot{state: floatingVisible, pane: popup, generation: 7}
	tb.mu.Unlock()

	rc := d.attachCoordinator(sess, nil, ac, true)
	lease := rc.attachmentLease(ac)
	epoch := rc.recordResizeRequestForLease(domain.Size{Cols: 80, Rows: 24}, ac, lease)
	require.NotZero(t, epoch)
	require.True(t, d.runResizeTransaction(sess, ac, lease, epoch))
	first := popupPTY.requested()
	require.Len(t, first, 1)

	d.retryResizeMembers(sess, ac, lease, epoch, []resizeMember{{session: sess, tab: tb, pane: popup, isFloating: true, floatingGeneration: 7}})
	retried := popupPTY.requested()
	require.Len(t, retried, 2)
	require.Equal(t, first[0], retried[1], "retry must use the current validated floating slot geometry")
	popup.mu.Lock()
	require.False(t, popup.resizeRetry)
	popup.mu.Unlock()

	replacementPTY := &transactionalResizePTY{}
	replacement := newPane("replacement", replacementPTY, domain.Size{Cols: 80, Rows: 23})
	replacement.resizeRetry = true
	tb.mu.Lock()
	tb.floating = floatingSlot{state: floatingVisible, pane: replacement, generation: 8}
	tb.mu.Unlock()
	d.retryResizeMembers(sess, ac, lease, epoch, []resizeMember{{session: sess, tab: tb, pane: popup, isFloating: true, floatingGeneration: 7}})
	require.Empty(t, replacementPTY.requested(), "a replaced floating slot must not inherit an old retry")
}

func TestTransactionalResizeObsoleteTimerCallbacksCommitOnlyLatestEpoch(t *testing.T) {
	clock := newCoordinatorMockClock(t, 8)
	pty := &transactionalResizePTY{}
	d, sess, ac, frames := newManualSessionWithPTYs(t, pty)
	d.clock = clock.clock

	for _, size := range []domain.Size{{Cols: 90, Rows: 24}, {Cols: 100, Rows: 28}, {Cols: 120, Rows: 32}} {
		d.resize(sess, ac, size)
	}
	// Epoch scheduling leaves PTY geometry untouched until the final
	// prepare/apply/commit callback.
	require.Empty(t, pty.requested(), "obsolete requests must not resize the PTY")

	var timers []*coordinatorMockTimer
	for len(timers) < 3 {
		timers = append(timers, <-clock.timers)
	}
	done := captureResizeCallbackDone(t, sess.renderCoordinator())
	for _, timer := range timers {
		timer.ch <- time.Time{}
	}
	awaitTestCompletion(t, done, "latest resize callback did not complete")
	require.Equal(t, []domain.Size{{Cols: 120, Rows: 30}}, pty.requested())
	snapshot := sess.renderCoordinator().resizeSnapshot()
	require.Equal(t, uint64(3), snapshot.epoch)
	require.Equal(t, snapshot.epoch, snapshot.committed)
	// The resize deadline is the only debounce: commit fires its sticky reset
	// through S2 immediately (subject to ACK/sync gates).
	frame := <-frames
	require.Equal(t, ports.MsgOutput, frame.Type, "the committed epoch emits a full frame")
	output, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.Zero(t, output.BaseStateNum, "the accepted resize frame must reset the renderer state")
	select {
	case extra := <-frames:
		t.Fatalf("obsolete epoch emitted an additional frame: %#v", extra)
	default:
	}
}

// Entering copy mode is invalidated at transaction prepare time, while a
// blocked apply keeps the old committed pane rectangle visible to mouse and
// composition. Channels, rather than a delay, make the publication boundary
// deterministic.
func TestTransactionalResizeFloatingMouseCopyAndSearchContract(t *testing.T) {
	for _, tc := range []struct {
		name string
		errs []error
	}{
		{name: "success"},
		{name: "partial failure", errs: []error{errors.New("resize failed")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			pty := &transactionalResizePTY{errs: tc.errs, onResize: func() { close(entered); <-release }}
			d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
			tb := sess.activeTab()
			p := tb.focusedPane()
			old := p.rect
			d.enterCopyMode(sess, ac)
			ac.overlays.copyMu.Lock()
			ac.overlays.copySearch = &visualsearch.Model{}
			ac.overlays.copyMu.Unlock()
			done := make(chan struct{})
			go func() {
				d.resize(sess, ac, domain.Size{Cols: 100, Rows: 30})
				close(done)
			}()
			<-entered
			require.False(t, ac.overlays.copyActive(), "prepare invalidates copy mode")
			require.False(t, ac.overlays.copySearchActive(), "prepare invalidates visual search")
			require.Equal(t, old, p.rect, "mouse uses old committed geometry before publication")
			close(release)
			<-done
		})
	}
}
