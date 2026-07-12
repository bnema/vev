package daemon

import (
	"errors"
	"io"
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
	mu       sync.Mutex
	sizes    []domain.Size
	errs     []error
	onResize func()
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
func (*transactionalResizePTY) Read([]byte) (int, error)     { return 0, io.EOF }
func (*transactionalResizePTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*transactionalResizePTY) Close() error                 { return nil }
func (*transactionalResizePTY) Pid() int                     { return 0 }
func (*transactionalResizePTY) ForegroundPgid() (int, error) { return 0, nil }
func (p *transactionalResizePTY) requested() []domain.Size {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Size(nil), p.sizes...)
}

// S3 acceptance: prepare may inspect layout under its locks, but apply must
// invoke PTY.Resize after all session/tab/pane locks are released. This is the
// deadlock boundary with PTY readers and scripts which synchronously re-enter
// daemon state.
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
	for _, timer := range timers {
		timer.ch <- time.Time{}
	}
	awaitResizeCallback(t, sess.renderCoordinator())
	require.Equal(t, []domain.Size{{Cols: 120, Rows: 30}}, pty.requested())
	snapshot := sess.renderCoordinator().resizeSnapshot()
	require.Equal(t, uint64(3), snapshot.epoch)
	require.Equal(t, snapshot.epoch, snapshot.committed)
	// The resize deadline is the only debounce: commit fires its sticky reset
	// through S2 immediately (subject to ACK/sync gates).
	frame := <-frames
	require.Equal(t, ports.MsgOutput, frame.Type, "the committed epoch emits a full frame")
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
