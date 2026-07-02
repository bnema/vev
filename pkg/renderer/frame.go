package renderer

import "fmt"

type Frame struct {
	Width  int
	Height int
	Cells  []Cell
}

func NewFrame(width, height int) Frame {
	cells := make([]Cell, width*height)
	for i := range cells {
		cells[i] = BlankCell()
	}
	return Frame{Width: width, Height: height, Cells: cells}
}

func (f Frame) Validate() error {
	if f.Width <= 0 || f.Height <= 0 {
		return fmt.Errorf("invalid frame size %dx%d", f.Width, f.Height)
	}
	if len(f.Cells) != f.Width*f.Height {
		return fmt.Errorf("invalid cell count: got %d want %d", len(f.Cells), f.Width*f.Height)
	}
	return nil
}

func (f Frame) At(x, y int) Cell {
	return f.Cells[y*f.Width+x]
}

func (f Frame) Set(x, y int, cell Cell) {
	f.Cells[y*f.Width+x] = cell
}
