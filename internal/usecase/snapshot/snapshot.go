package snapshot

import (
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
)

// Session is the durable transfer representation for a named daemon session.
type Session struct {
	Name      string
	CreatedAt uint64
	Active    uint16
	Tabs      []Tab
}

// Tab captures one tab's layout, dimensions, and pane snapshots.
type Tab struct {
	Cols       uint16
	Rows       uint16
	NextPaneID uint64
	Focus      layout.PaneID
	Tree       *layout.Tree
	Panes      []Pane
}

// Pane captures terminal state needed to restore a fresh shell in the same cwd
// with its scrollback and final visible primary screen.
type Pane struct {
	ID         layout.PaneID
	Cwd        string
	Scrollback [][]renderer.Cell
	Visible    [][]renderer.Cell
}
