package daemon

import (
	"context"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/vt"
)

// pane owns one PTY and its terminal screen state.
// Lock order when multiple locks are held is:
// attachedClient.sendMu > Daemon.mu > session.mu > tab.mu > pane.mu.
// The PTY reader takes only pane.mu, so child output never waits on client IO.
type pane struct {
	id       layout.PaneID
	stableID string
	pty      ports.PTY
	mu       sync.Mutex // guards screen, history, syncGen, rect, resizeApplying, resizePending, PTY side effects, and title
	resizeMu sync.Mutex // serializes PTY resizes without holding mu
	screen   *vt.Screen
	history  *vt.History
	syncGen  uint64
	rect     domain.Rect
	// resizeApplying gates VT parsing across PTY.Resize. The reader continues
	// draining into resizePending so output is replayed against the target (or
	// retained old) screen only after apply resolves.
	resizeApplying    bool
	resizePending     []byte
	ptyResponses      []byte
	ptyClipboards     []string
	ptyAttention      bool
	popupGeometry     floatingGeometry // last geometry committed after a successful floating resize
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
	screen := vt.NewScreenWithHistory(sz.Cols, sz.Rows, vt.HistoryConfig{MaxRows: defaultScrollbackRows})
	return &pane{
		id:       id,
		stableID: stableID,
		pty:      pty,
		screen:   screen,
		history:  screen.History(),
		rect:     domain.Rect{Width: sz.Cols, Height: sz.Rows},
		title:    paneTitleState{displayFallback: "sh"},
	}
}
