package vt

import "github.com/bnema/vev/pkg/renderer"

// CursorSnapshot is the cursor state captured with a visible screen snapshot.
type CursorSnapshot struct {
	Row      int
	Col      int
	Visible  bool
	Style    int
	StyleSet bool
}

// ModeSnapshot is the renderer-relevant VT mode state captured with a screen.
type ModeSnapshot struct {
	AlternateScreen    bool
	BracketedPaste     bool
	SynchronizedUpdate bool
	ColorSchemeMode    bool
	MouseTracking      int
	MouseSGR           bool
}

// ScreenSnapshot is an owned immutable-by-convention capture of the active
// terminal viewport. Row returns caller-owned storage; BorrowedRow must not be
// mutated and remains valid while the snapshot is retained.
type ScreenSnapshot struct {
	frame     renderer.Frame
	bounds    []LineBound
	rowIDs    []RowID
	cursor    CursorSnapshot
	modes     ModeSnapshot
	title     string
	nextRowID RowID
}

// Snapshot captures the active visible viewport without mutating Screen,
// history, or pending damage. Screen remains single-owner; callers must
// serialize Snapshot with Write, Resize, and host mutations.
func (s *Screen) Snapshot() ScreenSnapshot {
	if s == nil {
		return ScreenSnapshot{}
	}
	cursorStyle, cursorStyleSet := s.CursorStyle()
	mouseTracking, mouseSGR := s.MouseMode()
	nextRowID := s.nextRowID
	if nextRowID < ^RowID(0) {
		nextRowID++
	}
	return ScreenSnapshot{
		frame:     s.Frame.Clone(),
		bounds:    s.LineBounds(),
		rowIDs:    s.RowIDs(),
		nextRowID: nextRowID,
		cursor: CursorSnapshot{
			Row:      s.CursorRow(),
			Col:      s.CursorCol(),
			Visible:  s.CursorVisible(),
			Style:    cursorStyle,
			StyleSet: cursorStyleSet,
		},
		modes: ModeSnapshot{
			AlternateScreen:    s.AltScreenActive(),
			BracketedPaste:     s.BracketedPasteMode(),
			SynchronizedUpdate: s.SyncUpdateActive(),
			ColorSchemeMode:    s.ColorSchemeMode(),
			MouseTracking:      mouseTracking,
			MouseSGR:           mouseSGR,
		},
		title: s.TerminalTitle(),
	}
}

func (s ScreenSnapshot) Columns() int { return s.frame.Width }
func (s ScreenSnapshot) Rows() int    { return s.frame.Height }

func (s ScreenSnapshot) Row(y int) []renderer.Cell {
	return append([]renderer.Cell(nil), s.BorrowedRow(y)...)
}

func (s ScreenSnapshot) BorrowedRow(y int) []renderer.Cell {
	if y < 0 || y >= s.frame.Height {
		return nil
	}
	return s.frame.Row(y)
}

func (s ScreenSnapshot) Bound(y int) LineBound {
	if y < 0 || y >= len(s.bounds) {
		return LineBound{}
	}
	return s.bounds[y]
}

// RowIDs returns an owned copy of the visible physical-row identities.
func (s ScreenSnapshot) RowIDs() []RowID { return append([]RowID(nil), s.rowIDs...) }

// RowID returns the identity of visible row y, or zero when out of range.
func (s ScreenSnapshot) RowID(y int) RowID {
	if y < 0 || y >= len(s.rowIDs) {
		return 0
	}
	return s.rowIDs[y]
}

// NextRowID returns the next identity allocated by the captured screen.
func (s ScreenSnapshot) NextRowID() RowID {
	if s.nextRowID == 0 {
		return 1
	}
	return s.nextRowID
}

func (s ScreenSnapshot) Cursor() CursorSnapshot { return s.cursor }
func (s ScreenSnapshot) Modes() ModeSnapshot    { return s.modes }
func (s ScreenSnapshot) Title() string          { return s.title }
