package ui

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestResolvePresentation(t *testing.T) {
	tests := []struct {
		name          string
		base          domain.Size
		bounds, inner domain.Rect
		want          Presentation
	}{
		{
			name:   "79 columns becomes full-width drawer",
			base:   domain.Size{Cols: 79, Rows: 24},
			bounds: domain.Rect{X: 8, Y: 6, Width: 63, Height: 11},
			inner:  domain.Rect{X: 9, Y: 7, Width: 61, Height: 9},
			want: Presentation{
				Mode:    PresentationDrawer,
				Bounds:  domain.Rect{X: 0, Y: 12, Width: 79, Height: 11},
				Inner:   domain.Rect{X: 0, Y: 13, Width: 79, Height: 10},
				Borders: BorderTop,
			},
		},
		{
			name:   "80 columns preserves preferred geometry",
			base:   domain.Size{Cols: 80, Rows: 24},
			bounds: domain.Rect{X: 8, Y: 3, Width: 64, Height: 18},
			inner:  domain.Rect{X: 9, Y: 4, Width: 62, Height: 16},
			want: Presentation{
				Mode:    PresentationFloating,
				Bounds:  domain.Rect{X: 8, Y: 3, Width: 64, Height: 18},
				Inner:   domain.Rect{X: 9, Y: 4, Width: 62, Height: 16},
				Borders: BorderAll,
			},
		},
		{
			name:   "100 percent height preserves three rows and bottom bar",
			base:   domain.Size{Cols: 79, Rows: 24},
			bounds: domain.Rect{Width: 79, Height: 24},
			inner:  domain.Rect{X: 1, Y: 1, Width: 77, Height: 22},
			want: Presentation{
				Mode:    PresentationDrawer,
				Bounds:  domain.Rect{X: 0, Y: 3, Width: 79, Height: 20},
				Inner:   domain.Rect{X: 0, Y: 4, Width: 79, Height: 19},
				Borders: BorderTop,
			},
		},
		{
			name:   "preferred height larger than available is capped",
			base:   domain.Size{Cols: 79, Rows: 10},
			bounds: domain.Rect{X: -100, Y: -100, Width: 1000, Height: 1000},
			inner:  domain.Rect{X: -99, Y: -99, Width: 998, Height: 998},
			want: Presentation{
				Mode:    PresentationDrawer,
				Bounds:  domain.Rect{X: 0, Y: 3, Width: 79, Height: 6},
				Inner:   domain.Rect{X: 0, Y: 4, Width: 79, Height: 5},
				Borders: BorderTop,
			},
		},
		{
			name:   "negative preferred height becomes empty drawer",
			base:   domain.Size{Cols: 79, Rows: 24},
			bounds: domain.Rect{Height: -1},
			inner:  domain.Rect{Height: -3},
			want: Presentation{
				Mode:    PresentationDrawer,
				Bounds:  domain.Rect{X: 0, Y: 23, Width: 79, Height: 0},
				Inner:   domain.Rect{X: 0, Y: 23, Width: 79, Height: 0},
				Borders: BorderTop,
			},
		},
		{
			name:   "four-row terminal reserves all protected rows",
			base:   domain.Size{Cols: 79, Rows: 4},
			bounds: domain.Rect{Width: 79, Height: 4},
			inner:  domain.Rect{X: 1, Y: 1, Width: 77, Height: 2},
			want: Presentation{
				Mode:    PresentationDrawer,
				Bounds:  domain.Rect{X: 0, Y: 3, Width: 79},
				Inner:   domain.Rect{X: 0, Y: 3, Width: 79},
				Borders: BorderTop,
			},
		},
		{
			name:   "one-row terminal is panic-free and empty",
			base:   domain.Size{Cols: 1, Rows: 1},
			bounds: domain.Rect{Width: 1, Height: 1},
			inner:  domain.Rect{X: 1, Y: 1},
			want: Presentation{
				Mode:    PresentationDrawer,
				Bounds:  domain.Rect{X: 0, Y: 0, Width: 1},
				Inner:   domain.Rect{X: 0, Y: 0, Width: 1},
				Borders: BorderTop,
			},
		},
		{name: "zero frame is empty", base: domain.Size{}, want: Presentation{}},
		{name: "negative columns are empty", base: domain.Size{Cols: -1, Rows: 24}, want: Presentation{}},
		{name: "negative rows are empty", base: domain.Size{Cols: 79, Rows: -1}, want: Presentation{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ResolvePresentation(tt.base, tt.bounds, tt.inner))
		})
	}
}
