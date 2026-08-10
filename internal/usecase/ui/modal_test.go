package ui

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestModalResolveBounds(t *testing.T) {
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
			name: "minimum clamps to drawer availability",
			base: domain.Size{Cols: 20, Rows: 6},
			m:    Modal{WidthPct: 50, HeightPct: 50, MinWidth: 40, MinHeight: 12},
			want: domain.Rect{X: 0, Y: 3, Width: 20, Height: 2},
		},
		{
			name: "narrow preferred height becomes drawer height",
			base: domain.Size{Cols: 11, Rows: 7},
			m:    Modal{WidthPct: 50, HeightPct: 50},
			want: domain.Rect{X: 0, Y: 3, Width: 11, Height: 3},
		},
		{
			name: "zero value fills available drawer area",
			base: domain.Size{Cols: 12, Rows: 8},
			m:    Modal{},
			want: domain.Rect{X: 0, Y: 3, Width: 12, Height: 4},
		},
		{
			name: "bottom anchor honors bottom margin",
			base: domain.Size{Cols: 100, Rows: 40},
			m:    Modal{WidthPct: 50, HeightPct: 25, Anchor: domain.AnchorBottom, Margins: Margins{Bottom: 3}},
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
				FixedWidth:  30,
				FixedHeight: 12,
				Anchor:      domain.AnchorRight,
				Margins:     Margins{Right: 3},
			},
			want: domain.Rect{X: 67, Y: 14, Width: 30, Height: 12},
		},
		{
			name: "drawer ignores right margin",
			base: domain.Size{Cols: 20, Rows: 10},
			m: Modal{
				FixedWidth:  8,
				FixedHeight: 4,
				Anchor:      domain.AnchorRight,
				Margins:     Margins{Right: 20},
			},
			want: domain.Rect{X: 0, Y: 5, Width: 20, Height: 4},
		},
		{
			name: "right anchored drawer can have zero height",
			base: domain.Size{Cols: 10, Rows: 4},
			m: Modal{
				FixedWidth:  30,
				FixedHeight: 12,
				Anchor:      domain.AnchorRight,
				Margins:     Margins{Right: 3},
			},
			want: domain.Rect{X: 0, Y: 3, Width: 10, Height: 0},
		},
		{
			name: "fixed drawer dimensions clamp on small terminal",
			base: domain.Size{Cols: 10, Rows: 4},
			m:    Modal{FixedWidth: 30, FixedHeight: 12, Anchor: domain.AnchorBottom, Margins: Margins{Bottom: 2}},
			want: domain.Rect{X: 0, Y: 3, Width: 10, Height: 0},
		},
		{
			name: "drawer ignores bottom margin",
			base: domain.Size{Cols: 20, Rows: 10},
			m:    Modal{FixedWidth: 8, FixedHeight: 4, Anchor: domain.AnchorBottom, Margins: Margins{Bottom: 20}},
			want: domain.Rect{X: 0, Y: 5, Width: 20, Height: 4},
		},
		{
			name: "bottom right anchor honors combined margins",
			base: domain.Size{Cols: 100, Rows: 40},
			m: Modal{
				FixedWidth:  30,
				FixedHeight: 12,
				Anchor:      domain.AnchorBottomRight,
				Margins:     Margins{Bottom: 3, Right: 5},
			},
			want: domain.Rect{X: 65, Y: 25, Width: 30, Height: 12},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.Resolve(tt.base).Bounds; got != tt.want {
				t.Fatalf("Resolve().Bounds = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestModalResolveInnerDegenerate(t *testing.T) {
	got := (Modal{WidthPct: 100, HeightPct: 100}).Resolve(domain.Size{Cols: 2, Rows: 2}).Inner
	require.Equal(t, domain.Rect{X: 0, Y: 1, Width: 2, Height: 0}, got)
}

func TestModalResolve(t *testing.T) {
	tests := []struct {
		name string
		base domain.Size
		want Presentation
	}{
		{
			name: "narrow modal resolves to drawer",
			base: domain.Size{Cols: 20, Rows: 10},
			want: Presentation{
				Mode:    PresentationDrawer,
				Bounds:  domain.Rect{X: 0, Y: 5, Width: 20, Height: 4},
				Inner:   domain.Rect{X: 0, Y: 6, Width: 20, Height: 3},
				Borders: BorderTop,
			},
		},
		{
			name: "complete frame preserves floating modal geometry",
			base: domain.Size{Cols: 80, Rows: 10},
			want: Presentation{
				Mode:    PresentationFloating,
				Bounds:  domain.Rect{X: 20, Y: 3, Width: 40, Height: 4},
				Inner:   domain.Rect{X: 21, Y: 4, Width: 38, Height: 2},
				Borders: BorderAll,
			},
		},
	}

	modal := Modal{WidthPct: 50, FixedHeight: 4, Title: " Rename "}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, modal.Resolve(tt.base))
		})
	}
}

func TestCompositePresentationDrawsFloatingChrome(t *testing.T) {
	modal := Modal{WidthPct: 50, FixedHeight: 4, Title: " Rename "}
	presentation := modal.Resolve(domain.Size{Cols: 80, Rows: 10})
	frame := renderer.NewFrame(80, 10)
	exterior := renderer.Cell{Rune: 'x', Style: renderer.Style{Foreground: 7, Background: 8}}
	FillRect(frame, domain.Rect{Width: 80, Height: 10}, exterior)
	border := renderer.Style{Bold: true, Foreground: 2, Background: -1}
	interior := renderer.Style{Foreground: 3, Background: 4}

	inner := modal.CompositePresentation(frame, presentation, border, interior)
	require.Equal(t, presentation.Inner, inner)
	require.Equal(t, '┌', frame.At(presentation.Bounds.X, presentation.Bounds.Y).Rune)
	require.Equal(t, '┐', frame.At(presentation.Bounds.X+presentation.Bounds.Width-1, presentation.Bounds.Y).Rune)
	require.Equal(t, '└', frame.At(presentation.Bounds.X, presentation.Bounds.Y+presentation.Bounds.Height-1).Rune)
	require.Equal(t, '┘', frame.At(presentation.Bounds.X+presentation.Bounds.Width-1, presentation.Bounds.Y+presentation.Bounds.Height-1).Rune)
	require.Equal(t, 'R', frame.At(37, presentation.Bounds.Y).Rune)
	require.True(t, frame.At(inner.X, inner.Y).Style.Equal(interior))
	assertCell(t, frame, presentation.Bounds.X-1, presentation.Bounds.Y, exterior)
}

func TestCompositePresentationUsesBordersRatherThanMode(t *testing.T) {
	border := renderer.Style{Bold: true, Foreground: 2, Background: -1}
	exterior := renderer.Cell{Rune: 'x', Style: renderer.Style{Foreground: 7, Background: 8}}
	bounds := domain.Rect{X: 1, Y: 1, Width: 8, Height: 4}
	inner := domain.Rect{X: 2, Y: 2, Width: 6, Height: 2}

	tests := []struct {
		name         string
		presentation Presentation
		assert       func(*testing.T, renderer.Frame)
	}{
		{
			name: "all edges render for drawer mode",
			presentation: Presentation{
				Mode: PresentationDrawer, Bounds: bounds, Inner: inner, Borders: BorderAll,
			},
			assert: func(t *testing.T, frame renderer.Frame) {
				require.Equal(t, '┌', frame.At(1, 1).Rune)
				require.Equal(t, '│', frame.At(1, 2).Rune)
				require.Equal(t, '┘', frame.At(8, 4).Rune)
				require.Equal(t, 'T', frame.At(2, 1).Rune)
			},
		},
		{
			name: "top edge alone renders for floating mode",
			presentation: Presentation{
				Mode: PresentationFloating, Bounds: bounds, Inner: inner, Borders: BorderTop,
			},
			assert: func(t *testing.T, frame renderer.Frame) {
				require.Equal(t, '─', frame.At(1, 1).Rune)
				require.Equal(t, 'T', frame.At(2, 1).Rune)
				assertCell(t, frame, 1, 2, exterior)
				assertCell(t, frame, 8, 4, exterior)
			},
		},
		{
			name: "title is hidden without top edge",
			presentation: Presentation{
				Mode: PresentationFloating, Bounds: bounds, Inner: inner, Borders: BorderLeft,
			},
			assert: func(t *testing.T, frame renderer.Frame) {
				require.Equal(t, '│', frame.At(1, 1).Rune)
				require.Equal(t, '│', frame.At(1, 4).Rune)
				assertCell(t, frame, 4, 1, exterior)
				assertCell(t, frame, 8, 4, exterior)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := renderer.NewFrame(10, 6)
			FillRect(frame, domain.Rect{Width: 10, Height: 6}, exterior)
			(Modal{Title: "Title"}).CompositePresentation(frame, tt.presentation, border, renderer.DefaultStyle())
			tt.assert(t, frame)
		})
	}
}

func TestCompositeDrawerChromeAndPreservesExterior(t *testing.T) {
	modal := Modal{FixedHeight: 4, Title: " Rename "}
	presentation := modal.Resolve(domain.Size{Cols: 20, Rows: 10})
	frame := renderer.NewFrame(20, 10)
	exterior := renderer.Cell{Rune: 'x', Style: renderer.Style{Foreground: 7, Background: 8}}
	FillRect(frame, domain.Rect{Width: 20, Height: 10}, exterior)
	border := renderer.Style{Bold: true, Foreground: 2, Background: -1}
	interior := renderer.Style{Foreground: 3, Background: 4}

	inner := modal.CompositePresentation(frame, presentation, border, interior)
	require.Equal(t, presentation.Inner, inner)
	require.Equal(t, '─', frame.At(0, 5).Rune)
	require.True(t, frame.At(0, 5).Style.Equal(border))
	require.Equal(t, 'R', frame.At(7, 5).Rune)
	require.NotEqual(t, '│', frame.At(0, 6).Rune)
	require.Equal(t, ' ', frame.At(0, 6).Rune)
	require.NotEqual(t, '─', frame.At(0, 8).Rune)
	require.Equal(t, ' ', frame.At(0, 8).Rune)

	for _, y := range []int{0, 1, 2, 3, 4, 9} {
		for x := range frame.Width {
			assertCell(t, frame, x, y, exterior)
		}
	}
}

func TestCompositeDrawerClipsDoubleWidthTitle(t *testing.T) {
	modal := Modal{FixedHeight: 4, Title: "你你"}
	presentation := modal.Resolve(domain.Size{Cols: 4, Rows: 10})
	frame := renderer.NewFrame(4, 10)
	border := renderer.Style{Foreground: 2, Background: -1}

	modal.CompositePresentation(frame, presentation, border, renderer.DefaultStyle())

	require.Equal(t, '你', frame.At(1, presentation.Bounds.Y).Rune)
	require.True(t, frame.At(2, presentation.Bounds.Y).Continuation)
	require.Equal(t, '─', frame.At(3, presentation.Bounds.Y).Rune)
	require.False(t, frame.At(3, presentation.Bounds.Y).Continuation, "wide title must not leave an orphan continuation at the right clip")
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

func TestCompositePresentationPreservesZeroHeightDrawer(t *testing.T) {
	border := renderer.Style{Foreground: 1, Background: 2}
	interior := renderer.Style{Foreground: 3, Background: 4}
	frame := renderer.NewFrame(2, 2)
	modal := Modal{WidthPct: 100, HeightPct: 100}
	presentation := modal.Resolve(domain.Size{Cols: 2, Rows: 2})

	got := modal.CompositePresentation(frame, presentation, border, interior)

	require.Equal(t, presentation.Inner, got)
	for y := range frame.Height {
		for x := range frame.Width {
			require.Equal(t, renderer.BlankCell(), frame.At(x, y))
		}
	}
}
