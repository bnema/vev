package copy

import "github.com/bnema/vev/pkg/renderer"

// Scrollback stores evicted terminal rows in a fixed-size ring buffer.
type Scrollback struct {
	rows [][]renderer.Cell
	head int
	len  int
}

// NewScrollback constructs a scrollback ring capped at capacity rows.
func NewScrollback(capacity int) *Scrollback {
	if capacity < 0 {
		capacity = 0
	}
	return &Scrollback{rows: make([][]renderer.Cell, capacity)}
}

// Cap returns the maximum number of rows retained.
func (s *Scrollback) Cap() int { return len(s.rows) }

// Len returns the number of retained rows.
func (s *Scrollback) Len() int { return s.len }

// Head returns the physical ring slot holding the oldest retained row.
func (s *Scrollback) Head() int { return s.head }

// Append stores a copy of row, evicting the oldest row when the ring is full.
func (s *Scrollback) Append(row []renderer.Cell) {
	if len(s.rows) == 0 {
		return
	}
	cp := append([]renderer.Cell(nil), row...)
	if s.len < len(s.rows) {
		s.rows[(s.head+s.len)%len(s.rows)] = cp
		s.len++
		return
	}
	s.rows[s.head] = cp
	s.head = (s.head + 1) % len(s.rows)
}

// Snapshot returns retained rows oldest-first.
//
// The returned outer slice is a copy, but row slices are shared with the
// scrollback storage. Callers must treat the returned rows as read-only.
func (s *Scrollback) Snapshot() [][]renderer.Cell {
	if s.len == 0 || len(s.rows) == 0 {
		return nil
	}
	rows := make([][]renderer.Cell, s.len)
	for i := range s.len {
		rows[i] = s.rows[(s.head+i)%len(s.rows)]
	}
	return rows
}

// HistoryView is a frozen oldest-first view of scrollback rows.
//
// Its rows share cell slices with the scrollback at the time View is called.
// Callers must treat rows returned by Row as read-only.
type HistoryView struct {
	rows [][]renderer.Cell
}

// View returns a frozen scrollback view. Appending to the scrollback cannot
// alter the view because Append replaces ring slots rather than mutating rows.
func (s *Scrollback) View() HistoryView {
	return HistoryView{rows: s.Snapshot()}
}

// Len returns the number of rows in the view.
func (v HistoryView) Len() int { return len(v.rows) }

// Row returns the row at logical index i, oldest first, or nil when i is out
// of range. Returned rows are shared with the view and must be read-only.
func (v HistoryView) Row(i int) []renderer.Cell {
	if i < 0 || i >= len(v.rows) {
		return nil
	}
	return v.rows[i]
}
