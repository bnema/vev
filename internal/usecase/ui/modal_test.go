package ui

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

func TestModalBounds(t *testing.T) {
	tests := []struct {
		name string
		base domain.Size
		m    Modal
		want domain.Rect
	}{
		{
			name: "eighty percent centered",
			base: domain.Size{Cols: 100, Rows: 40},
			m:    Modal{WidthPct: 80, HeightPct: 80},
			want: domain.Rect{X: 10, Y: 4, Width: 80, Height: 32},
		},
		{
			name: "minimum clamps to base",
			base: domain.Size{Cols: 20, Rows: 6},
			m:    Modal{WidthPct: 50, HeightPct: 50, MinWidth: 40, MinHeight: 12},
			want: domain.Rect{X: 0, Y: 0, Width: 20, Height: 6},
		},
		{
			name: "odd centering rounds down",
			base: domain.Size{Cols: 11, Rows: 7},
			m:    Modal{WidthPct: 50, HeightPct: 50},
			want: domain.Rect{X: 3, Y: 2, Width: 5, Height: 3},
		},
		{
			name: "zero value fills base centered",
			base: domain.Size{Cols: 12, Rows: 8},
			m:    Modal{},
			want: domain.Rect{X: 0, Y: 0, Width: 12, Height: 8},
		},
		{
			name: "bottom anchor honors bottom margin",
			base: domain.Size{Cols: 100, Rows: 40},
			m:    Modal{WidthPct: 50, HeightPct: 25, Anchor: AnchorBottom, BottomMargin: 3},
			want: domain.Rect{X: 25, Y: 27, Width: 50, Height: 10},
		},
		{
			name: "fixed dimensions override percentages",
			base: domain.Size{Cols: 100, Rows: 40},
			m:    Modal{WidthPct: 80, HeightPct: 80, FixedWidth: 30, FixedHeight: 12},
			want: domain.Rect{X: 35, Y: 14, Width: 30, Height: 12},
		},
		{
			name: "right anchor honors right margin",
			base: domain.Size{Cols: 100, Rows: 40},
			m: Modal{
				FixedWidth:       30,
				FixedHeight:      12,
				HorizontalAnchor: HorizontalAnchorRight,
				RightMargin:      3,
			},
			want: domain.Rect{X: 67, Y: 14, Width: 30, Height: 12},
		},
		{
			name: "right margin clamps to screen",
			base: domain.Size{Cols: 20, Rows: 10},
			m: Modal{
				FixedWidth:       8,
				FixedHeight:      4,
				HorizontalAnchor: HorizontalAnchorRight,
				RightMargin:      20,
			},
			want: domain.Rect{X: 0, Y: 3, Width: 8, Height: 4},
		},
		{
			name: "right anchored fixed dimensions clamp on small terminal",
			base: domain.Size{Cols: 10, Rows: 4},
			m: Modal{
				FixedWidth:       30,
				FixedHeight:      12,
				HorizontalAnchor: HorizontalAnchorRight,
				RightMargin:      3,
			},
			want: domain.Rect{X: 0, Y: 0, Width: 10, Height: 4},
		},
		{
			name: "fixed dimensions clamp on small terminal",
			base: domain.Size{Cols: 10, Rows: 4},
			m:    Modal{FixedWidth: 30, FixedHeight: 12, Anchor: AnchorBottom, BottomMargin: 2},
			want: domain.Rect{X: 0, Y: 0, Width: 10, Height: 4},
		},
		{
			name: "bottom margin clamps to screen",
			base: domain.Size{Cols: 20, Rows: 10},
			m:    Modal{FixedWidth: 8, FixedHeight: 4, Anchor: AnchorBottom, BottomMargin: 20},
			want: domain.Rect{X: 6, Y: 0, Width: 8, Height: 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.Bounds(tt.base); got != tt.want {
				t.Fatalf("Bounds() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestModalInnerDegenerate(t *testing.T) {
	m := Modal{WidthPct: 100, HeightPct: 100}
	got := m.Inner(domain.Size{Cols: 2, Rows: 2})
	want := domain.Rect{X: 1, Y: 1, Width: 0, Height: 0}
	if got != want {
		t.Fatalf("Inner() = %+v, want %+v", got, want)
	}
}

func TestCompositeDrawsModalAndPreservesExterior(t *testing.T) {
	f := renderer.NewFrame(10, 6)
	exterior := renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()}
	FillRect(f, domain.Rect{X: 0, Y: 0, Width: 10, Height: 6}, exterior)

	style := renderer.Style{Bold: true, Foreground: 2, Background: -1}
	m := Modal{WidthPct: 60, HeightPct: 80, Title: "Hi"}
	inner := m.Composite(f, style)
	wantInner := domain.Rect{X: 3, Y: 2, Width: 4, Height: 2}
	if inner != wantInner {
		t.Fatalf("Composite() inner = %+v, want %+v", inner, wantInner)
	}

	assertRune(t, f, 2, 1, '┌')
	assertRune(t, f, 7, 1, '┐')
	assertRune(t, f, 2, 4, '└')
	assertRune(t, f, 7, 4, '┘')
	assertRune(t, f, 3, 1, '─')
	assertRune(t, f, 2, 2, '│')
	assertRune(t, f, 4, 1, 'H')
	assertRune(t, f, 5, 1, 'i')

	for y := inner.Y; y < inner.Y+inner.Height; y++ {
		for x := inner.X; x < inner.X+inner.Width; x++ {
			if got := f.At(x, y); !got.Equal(renderer.BlankCell()) {
				t.Fatalf("interior cell (%d,%d) = %+v, want blank", x, y, got)
			}
		}
	}
	assertCell(t, f, 0, 0, exterior)
	assertCell(t, f, 9, 5, exterior)
}

func TestFillRectClips(t *testing.T) {
	f := renderer.NewFrame(3, 2)
	cell := renderer.Cell{Rune: 'z', Style: renderer.DefaultStyle()}
	FillRect(f, domain.Rect{X: -1, Y: -1, Width: 3, Height: 3}, cell)
	assertCell(t, f, 0, 0, cell)
	assertCell(t, f, 1, 1, cell)
	assertCell(t, f, 2, 1, renderer.BlankCell())
}

func TestDrawTextClipsAndDropsWideRuneCrossingClip(t *testing.T) {
	style := renderer.Style{Inverse: true, Foreground: -1, Background: -1}
	t.Run("clips ascii", func(t *testing.T) {
		f := renderer.NewFrame(5, 1)
		next := DrawText(f, 1, 0, 3, "abcd", style)
		if next != 3 {
			t.Fatalf("DrawText next = %d, want 3", next)
		}
		assertRune(t, f, 1, 0, 'a')
		assertRune(t, f, 2, 0, 'b')
		assertCell(t, f, 3, 0, renderer.BlankCell())
	})
	t.Run("draws wide rune when it fits", func(t *testing.T) {
		f := renderer.NewFrame(5, 1)
		next := DrawText(f, 1, 0, 3, "你", style)
		if next != 3 {
			t.Fatalf("DrawText next = %d, want 3", next)
		}
		assertRune(t, f, 1, 0, '你')
		if got := f.At(2, 0); !got.Continuation {
			t.Fatalf("wide continuation = %+v, want continuation", got)
		}
	})
	t.Run("drops wide rune crossing clip", func(t *testing.T) {
		f := renderer.NewFrame(5, 1)
		next := DrawText(f, 2, 0, 3, "你A", style)
		if next != 2 {
			t.Fatalf("DrawText next = %d, want 2", next)
		}
		assertCell(t, f, 2, 0, renderer.BlankCell())
		assertCell(t, f, 3, 0, renderer.BlankCell())
	})
}

func assertRune(t *testing.T, f renderer.Frame, x, y int, want rune) {
	t.Helper()
	if got := f.At(x, y).Rune; got != want {
		t.Fatalf("cell (%d,%d) rune = %q, want %q", x, y, got, want)
	}
}

func assertCell(t *testing.T, f renderer.Frame, x, y int, want renderer.Cell) {
	t.Helper()
	if got := f.At(x, y); !got.Equal(want) {
		t.Fatalf("cell (%d,%d) = %+v, want %+v", x, y, got, want)
	}
}
