package ui

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

func TestDrawSeparator(t *testing.T) {
	style := renderer.DefaultStyle()
	style.Attrs = renderer.AttrDim
	cases := []struct {
		name        string
		orientation SeparatorOrientation
		rune        rune
		rect        domain.Rect
	}{
		{"horizontal", SeparatorHorizontal, '─', domain.Rect{X: 1, Y: 2, Width: 3, Height: 1}},
		{"vertical", SeparatorVertical, '│', domain.Rect{X: 2, Y: 1, Width: 1, Height: 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := renderer.NewFrame(6, 6)
			DrawSeparator(f, tc.rect, tc.orientation, style)
			for y := 0; y < 6; y++ {
				for x := 0; x < 6; x++ {
					want := renderer.BlankCell()
					if (tc.orientation == SeparatorHorizontal && y == tc.rect.Y && x >= tc.rect.X && x < tc.rect.X+tc.rect.Width) ||
						(tc.orientation == SeparatorVertical && x == tc.rect.X && y >= tc.rect.Y && y < tc.rect.Y+tc.rect.Height) {
						want = renderer.Cell{Rune: tc.rune, Style: style}
					}
					if !f.At(x, y).Equal(want) {
						t.Fatalf("cell %d,%d = %#v, want %#v", x, y, f.At(x, y), want)
					}
				}
			}
		})
	}
}

func TestDrawSeparatorClipsAndRejectsInvalid(t *testing.T) {
	style := renderer.DefaultStyle()
	f := renderer.NewFrame(3, 3)
	for y := range f.Height {
		for x := range f.Width {
			f.Set(x, y, renderer.Cell{Rune: 'x', Style: style})
		}
	}
	DrawSeparator(f, domain.Rect{X: -2, Y: 1, Width: 8, Height: 1}, SeparatorHorizontal, style)
	for x := 0; x < 3; x++ {
		if f.At(x, 1).Rune != '─' {
			t.Fatalf("clipping failed at %d", x)
		}
	}
	before := f.Clone()
	for _, rect := range []domain.Rect{{Width: 0, Height: 1}, {Width: 1, Height: 0}} {
		DrawSeparator(f, rect, SeparatorHorizontal, style)
	}
	DrawSeparator(f, domain.Rect{Width: 1, Height: 1}, SeparatorOrientation(99), style)
	if !framesEqual(f, before) {
		t.Fatal("invalid rectangles/orientation changed frame")
	}
}

func TestDrawSeparatorInvalidFrame(t *testing.T) {
	f := renderer.Frame{Width: 2, Height: 2, Cells: make([]renderer.Cell, 3)}
	DrawSeparator(f, domain.Rect{Width: 2, Height: 1}, SeparatorHorizontal, renderer.DefaultStyle())
}

func framesEqual(a, b renderer.Frame) bool {
	if a.Width != b.Width || a.Height != b.Height {
		return false
	}
	for y := 0; y < a.Height; y++ {
		for x := 0; x < a.Width; x++ {
			if !a.At(x, y).Equal(b.At(x, y)) {
				return false
			}
		}
	}
	return true
}
