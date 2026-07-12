package daemon

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
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

// Stale lifecycle callbacks may never advance a transaction owned by a
// replacement attachment. The coordinator test clock drives callbacks through
// channels elsewhere in this package; this ownership assertion is deliberately
// synchronous so it cannot hide a race behind a sleep.
func TestTransactionalResizeEpochLifecycleAndRetryContract(t *testing.T) {
	rc := newRenderCoordinator(renderCoordinatorOptions{})
	old, replacement := &attachedClient{}, &attachedClient{}
	rc.attach(old)
	require.Equal(t, uint64(1), rc.recordResizeRequest(domain.Size{Cols: 90, Rows: 24}, old))
	require.Equal(t, uint64(2), rc.recordResizeRequest(domain.Size{Cols: 100, Rows: 26}, old))
	require.Equal(t, uint64(3), rc.recordResizeRequest(domain.Size{Cols: 120, Rows: 30}, old))
	rc.noteReplace(old, replacement)
	rc.attach(replacement)
	require.Zero(t, rc.recordResizeRequest(domain.Size{Cols: 20, Rows: 5}, old),
		"obsolete attach/replace/detach/park/resume callbacks cannot publish")
	snap := rc.resizeSnapshot()
	require.Equal(t, uint64(3), snap.epoch)
	require.Equal(t, domain.Size{Cols: 120, Rows: 30}, snap.size)
}

// Entering copy mode is invalidated at transaction prepare time, while a
// blocked apply keeps the old committed pane rectangle visible to mouse and
// composition. Channels, rather than a delay, make the publication boundary
// deterministic.
func TestTransactionalResizeFloatingMouseCopyAndSearchContract(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	pty := &transactionalResizePTY{onResize: func() { close(entered); <-release }}
	d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
	tb := sess.activeTab()
	p := tb.focusedPane()
	old := p.rect
	d.enterCopyMode(sess, ac)
	done := make(chan struct{})
	go func() {
		d.resize(sess, ac, domain.Size{Cols: 100, Rows: 30})
		close(done)
	}()
	<-entered
	require.False(t, ac.overlays.copyActive(), "prepare invalidates copy mode and visual search")
	require.Equal(t, old, p.rect, "mouse uses the old committed geometry before publication")
	close(release)
	<-done
}
