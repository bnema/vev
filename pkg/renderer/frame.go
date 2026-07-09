package renderer

import "fmt"

// Frame is a fixed-size grid of cells. Logical rows are decoupled from their
// physical storage through lineOffset: logical row y lives at
// Cells[lineOffset[y] : lineOffset[y]+Width]. This indirection lets full-width
// scrolls rotate offsets instead of copying cell memory (NeoVim's grid
// technique; cf. nvim grid.c line_offset[]). All cell access must go through
// the accessors (At/Set/Row) so callers never assume a physical layout.
type Frame struct {
	Width  int
	Height int
	Cells  []Cell
	// lineOffset maps a logical row index to its physical base index into
	// Cells. It is always a permutation of the canonical offsets y*Width.
	lineOffset []int
}

func NewFrame(width, height int) Frame {
	cells := make([]Cell, width*height)
	for i := range cells {
		cells[i] = BlankCell()
	}
	lineOffset := make([]int, height)
	for y := range lineOffset {
		lineOffset[y] = y * width
	}
	return Frame{Width: width, Height: height, Cells: cells, lineOffset: lineOffset}
}

// Clone returns an independent copy preserving logical row contents. The clone
// uses canonical row storage so later scrolls or writes to the source frame do
// not affect it.
func (f Frame) Clone() Frame {
	clone := NewFrame(f.Width, f.Height)
	for y := range f.Height {
		copy(clone.Row(y), f.Row(y))
	}
	return clone
}

func (f Frame) Validate() error {
	if f.Width <= 0 || f.Height <= 0 {
		return fmt.Errorf("invalid frame size %dx%d", f.Width, f.Height)
	}
	if len(f.Cells) != f.Width*f.Height {
		return fmt.Errorf("invalid cell count: got %d want %d", len(f.Cells), f.Width*f.Height)
	}
	if len(f.lineOffset) != f.Height {
		return fmt.Errorf("invalid lineOffset length: got %d want %d", len(f.lineOffset), f.Height)
	}
	return nil
}

// CheckInvariants verifies that lineOffset is a permutation of the canonical
// row offsets: every entry is a non-negative multiple of Width within bounds,
// and every logical row maps to a distinct physical row. It is used by tests
// and assertions; the hot path uses the lighter Validate.
func (f Frame) CheckInvariants() error {
	if f.Width <= 0 {
		return fmt.Errorf("invalid width %d", f.Width)
	}
	if len(f.lineOffset) != f.Height {
		return fmt.Errorf("lineOffset length %d != height %d", len(f.lineOffset), f.Height)
	}
	seen := make([]bool, f.Height)
	for y, off := range f.lineOffset {
		if off < 0 || off%f.Width != 0 {
			return fmt.Errorf("row %d: offset %d is not a non-negative multiple of width %d", y, off, f.Width)
		}
		phys := off / f.Width
		if phys >= f.Height {
			return fmt.Errorf("row %d: offset %d maps to physical row %d out of range [0,%d)", y, off, phys, f.Height)
		}
		if seen[phys] {
			return fmt.Errorf("row %d: physical row %d already mapped by another logical row", y, phys)
		}
		seen[phys] = true
	}
	return nil
}

func (f Frame) At(x, y int) Cell {
	return f.Cells[f.lineOffset[y]+x]
}

func (f Frame) Set(x, y int, cell Cell) {
	f.Cells[f.lineOffset[y]+x] = cell
}

// Row returns the backing slice for logical row y. Mutating the returned slice
// mutates the frame in place. The slice is valid until the next scroll or
// resize that reassigns offsets.
func (f Frame) Row(y int) []Cell {
	base := f.lineOffset[y]
	return f.Cells[base : base+f.Width]
}

// ScrollUp scrolls the logical rows in [top,bottom] up by n lines by rotating
// their physical offsets: no per-cell copying. Rows scrolled off the top are
// recycled to the bottom and blanked in place. It assumes the full frame width
// and 0 <= top <= bottom < Height and 0 < n <= bottom-top+1 (the caller
// clamps). The receiver is a value, but lineOffset and Cells alias the caller's
// backing arrays, so in-place mutations are visible to the caller.
func (f Frame) ScrollUp(top, bottom, n int) {
	for ; n > 0; n-- {
		recycled := f.lineOffset[top]
		for y := top; y < bottom; y++ {
			f.lineOffset[y] = f.lineOffset[y+1]
		}
		f.lineOffset[bottom] = recycled
		blankRow(f.Cells[recycled : recycled+f.Width])
	}
}

// ScrollDown scrolls the logical rows in [top,bottom] down by n lines by
// rotating their physical offsets. Rows scrolled off the bottom are recycled to
// the top and blanked in place. See ScrollUp for preconditions.
func (f Frame) ScrollDown(top, bottom, n int) {
	for ; n > 0; n-- {
		recycled := f.lineOffset[bottom]
		for y := bottom; y > top; y-- {
			f.lineOffset[y] = f.lineOffset[y-1]
		}
		f.lineOffset[top] = recycled
		blankRow(f.Cells[recycled : recycled+f.Width])
	}
}

func blankRow(row []Cell) {
	for i := range row {
		row[i] = BlankCell()
	}
}
