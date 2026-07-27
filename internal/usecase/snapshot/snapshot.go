package snapshot

import "github.com/bnema/vev/internal/usecase/layout"

// Session is the durable transfer representation for a named daemon session.
type Session struct {
	Name      string
	CreatedAt uint64
	Active    uint16
	Tabs      []Tab
}

// Tab captures one tab's layout, dimensions, and pane snapshots.
type Tab struct {
	StableID   string
	Cols       uint16
	Rows       uint16
	NextPaneID uint64
	Focus      layout.PaneID
	Tree       *layout.Tree
	Panes      []Pane
}

// Pane captures terminal state as opaque, canonical VT blobs. Snapshot owns
// their length framing and order; pkg/vt owns their contents and validation.
type Pane struct {
	ID           layout.PaneID
	StableID     string
	Cwd          string
	SealedChunks [][]byte // oldest-first, each a self-contained canonical history blob
	Tail         []byte   // mandatory nonempty canonical history blob
	Transcript   []byte   // mandatory nonempty canonical history blob
	Process      *Process
}

// Process captures restorable foreground process metadata.
type Process struct {
	Argv     []string
	Strategy string
	Opts     ProcessOpts
}

// ProcessOpts captures strategy-specific process restore options.
type ProcessOpts struct {
	AgentSessionID string
}
