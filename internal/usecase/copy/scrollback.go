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

// Row returns a copy of the retained row at logical index i, oldest first.
func (s *Scrollback) Row(i int) []renderer.Cell {
	if i < 0 || i >= s.len || len(s.rows) == 0 {
		return nil
	}
	row := s.rows[(s.head+i)%len(s.rows)]
	return append([]renderer.Cell(nil), row...)
}
