package daemon

import (
	"context"
	"sync"
	"sync/atomic"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

// paneProcessLifetime owns the context passed to PTYFactory.Open. Its daemon
// parent lets an installed pane move between sessions without changing process
// lifetime. Temporary opening parents bound an unpublished Open; publish
// atomically detaches those parents before transferring cancellation to pane.
type paneProcessLifetime struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu             sync.Mutex
	opening        bool
	openingParents []context.Context
	stopOpening    []func() bool
}

func (d *Daemon) newPaneProcessLifetime(openingParents ...context.Context) *paneProcessLifetime {
	parent := d.paneProcessCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, rawCancel := context.WithCancel(parent)
	var cancelOnce sync.Once
	lifetime := &paneProcessLifetime{
		ctx:            ctx,
		cancel:         func() { cancelOnce.Do(rawCancel) },
		opening:        true,
		openingParents: append([]context.Context(nil), openingParents...),
	}
	for _, openingParent := range lifetime.openingParents {
		if openingParent == nil {
			continue
		}
		lifetime.stopOpening = append(lifetime.stopOpening, context.AfterFunc(openingParent, func() {
			lifetime.mu.Lock()
			if lifetime.opening {
				lifetime.cancel()
			}
			lifetime.mu.Unlock()
		}))
	}
	return lifetime
}

// publish transfers a successfully opened process to p. A cancellation that
// wins before this call prevents publication; one that starts afterward no
// longer belongs to the source session or tab.
func (l *paneProcessLifetime) publish(p *pane) bool {
	if l == nil || p == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, parent := range l.openingParents {
		if parent != nil && parent.Err() != nil {
			l.opening = false
			l.stopOpeningParentsLocked()
			l.cancel()
			return false
		}
	}
	if l.ctx.Err() != nil {
		l.opening = false
		l.stopOpeningParentsLocked()
		l.cancel()
		return false
	}
	l.opening = false
	l.stopOpeningParentsLocked()
	p.ctx = l.ctx
	p.cancel = l.cancel
	return true
}

func (l *paneProcessLifetime) abort() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.opening = false
	l.stopOpeningParentsLocked()
	l.cancel()
	l.mu.Unlock()
}

func (l *paneProcessLifetime) stopOpeningParentsLocked() {
	for _, stop := range l.stopOpening {
		stop()
	}
	l.stopOpening = nil
	l.openingParents = nil
}

// pane owns one PTY and its terminal screen state.
// Lock order when multiple locks are held is:
// attachedClient.sendMu > Daemon.mu > session.mu > tab.mu > pane.mu.
// The PTY reader takes only pane.mu, so child output never waits on client IO.
type pane struct {
	id       layout.PaneID
	stableID string
	pty      ports.PTY
	mu       sync.Mutex // guards screen, history, syncGen, rect, geometry, resize state, PTY side effects, title, and owner publication
	owner    atomic.Pointer[paneOwner]
	// ownerGeneration is advanced only while mu is held. Readers consume the
	// generation through the immutable owner pointer above.
	ownerGeneration uint64
	resizeMu        sync.Mutex // serializes PTY resizes without holding mu
	screen          *vt.Screen
	history         *vt.History
	syncGen         uint64
	rect            domain.Rect
	geometry        domain.Geometry
	// resizeApplying gates VT parsing across PTY.Resize. The reader continues
	// draining into resizePending so output is replayed against the target (or
	// retained old) screen only after apply resolves.
	resizeApplying   bool
	resizeRetry      bool // PTY resize failed for the committed rectangle; retry on the next accepted plan.
	resizePending    []byte
	ptyResponses     []byte
	ptyClipboards    []string
	ptyAttention     bool
	popupGeometry    floatingGeometry // last geometry committed after a successful floating resize
	title            paneTitleState
	ctx              context.Context
	cancel           context.CancelFunc
	processCloseOnce sync.Once
	// onExit is set before the reader starts and never changed. Floating panes
	// use it to reap their independent slot without touching the layout tree.
	onExit func()
}

func closePaneProcess(p *pane) {
	if p == nil {
		return
	}
	p.processCloseOnce.Do(func() {
		// Revocation shares pane.mu with VT parsing and owner publication, so no
		// effect lease remains current once terminal teardown begins.
		p.mu.Lock()
		p.clearOwnerLocked()
		p.mu.Unlock()
		if p.cancel != nil {
			p.cancel()
		}
		if p.pty != nil {
			_ = p.pty.Close()
		}
	})
}

func newPane(id layout.PaneID, pty ports.PTY, sz domain.Size) *pane {
	return newPaneWithStableID(id, fallbackStableID("p"), pty, sz)
}

func newPaneWithStableID(id layout.PaneID, stableID string, pty ports.PTY, sz domain.Size) *pane {
	return newPaneWithStableIDAndTitle(id, stableID, pty, sz, defaultShellTitle)
}

func newPaneWithStableIDAndTitle(id layout.PaneID, stableID string, pty ports.PTY, sz domain.Size, title string) *pane {
	screen := vt.NewScreenWithHistory(sz.Cols, sz.Rows, vt.HistoryConfig{MaxRows: defaultScrollbackRows, MaxCells: defaultScrollbackCells})
	return &pane{
		id:       id,
		stableID: stableID,
		pty:      pty,
		screen:   screen,
		history:  screen.History(),
		rect:     domain.Rect{Width: sz.Cols, Height: sz.Rows},
		title:    paneTitleState{displayFallback: title},
	}
}
