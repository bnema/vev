package vt

import "github.com/bnema/vev/pkg/renderer"

// PrimaryVisibleRows returns a deep copy of the visible primary-screen rows.
// When the alternate screen is active, it snapshots the saved primary screen
// rather than the currently displayed alternate frame.
func (s *Screen) PrimaryVisibleRows() [][]renderer.Cell {
	frame := s.Frame
	if s.alternate != nil {
		frame = s.alternate.frame
	}
	rows := make([][]renderer.Cell, frame.Height)
	for y := range frame.Height {
		rows[y] = append([]renderer.Cell(nil), frame.Row(y)...)
	}
	return rows
}
