package daemon

import (
	"context"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/vt"
)

// pane owns one PTY, its terminal screen state, and render scheduling channels.
// Lock order when multiple locks are held is:
// attachedClient.sendMu > Daemon.mu > session.mu > tab.mu > pane.mu.
// The PTY reader takes only pane.mu, so child output never waits on client IO.
type pane struct {
	id                layout.PaneID
	stableID          string
	pty               ports.PTY
	mu                sync.Mutex // guards screen, scrollback, syncGen, rect, and title
	screen            *vt.Screen
	scrollback        *scopy.Scrollback
	dirty             chan struct{}
	flush             chan struct{}
	syncGen           uint64
	rect              domain.Rect
	title             paneTitleState
	ctx               context.Context
	cancel            context.CancelFunc
	floatingCloseOnce sync.Once // used only by floating lifecycle teardown paths
	// onExit is set before the reader starts and never changed. Floating panes
	// use it to reap their independent slot without touching the layout tree.
	onExit func()
}

func newPane(id layout.PaneID, pty ports.PTY, sz domain.Size) *pane {
	return newPaneWithStableID(id, fallbackStableID("p"), pty, sz)
}

func newPaneWithStableID(id layout.PaneID, stableID string, pty ports.PTY, sz domain.Size) *pane {
	sb := scopy.NewScrollback(defaultScrollbackRows)
	screen := vt.NewScreen(sz.Cols, sz.Rows)
	screen.OnLineEvicted = sb.Append
	return &pane{
		id:         id,
		stableID:   stableID,
		pty:        pty,
		screen:     screen,
		scrollback: sb,
		dirty:      make(chan struct{}, 1),
		flush:      make(chan struct{}, 1),
		rect:       domain.Rect{Width: sz.Cols, Height: sz.Rows},
		title:      paneTitleState{displayFallback: "sh"},
	}
}

func paneDone(p *pane) <-chan struct{} {
	if p.ctx == nil {
		return nil
	}
	return p.ctx.Done()
}
