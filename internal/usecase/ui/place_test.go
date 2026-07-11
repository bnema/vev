package ui

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestPlaceAnchors(t *testing.T) {
	base := domain.Size{Cols: 31, Rows: 19}
	content := domain.Size{Cols: 8, Rows: 6}
	margins := Margins{Top: 2, Right: 3, Bottom: 4, Left: 5}

	tests := []struct {
		anchor domain.Anchor
		want   domain.Rect
	}{
		{domain.AnchorTopLeft, domain.Rect{X: 5, Y: 2, Width: 8, Height: 6}},
		{domain.AnchorTop, domain.Rect{X: 11, Y: 2, Width: 8, Height: 6}},
		{domain.AnchorTopRight, domain.Rect{X: 20, Y: 2, Width: 8, Height: 6}},
		{domain.AnchorLeft, domain.Rect{X: 5, Y: 6, Width: 8, Height: 6}},
		{domain.AnchorCenter, domain.Rect{X: 11, Y: 6, Width: 8, Height: 6}},
		{domain.AnchorRight, domain.Rect{X: 20, Y: 6, Width: 8, Height: 6}},
		{domain.AnchorBottomLeft, domain.Rect{X: 5, Y: 9, Width: 8, Height: 6}},
		{domain.AnchorBottom, domain.Rect{X: 11, Y: 9, Width: 8, Height: 6}},
		{domain.AnchorBottomRight, domain.Rect{X: 20, Y: 9, Width: 8, Height: 6}},
	}
	for _, tt := range tests {
		t.Run(tt.anchor.String(), func(t *testing.T) {
			if got := Place(base, content, tt.anchor, margins); got != tt.want {
				t.Fatalf("Place() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestPlaceNormalizesAndClamps(t *testing.T) {
	tests := []struct {
		name    string
		base    domain.Size
		content domain.Size
		anchor  domain.Anchor
		margins Margins
		want    domain.Rect
	}{
		{
			name:    "center ignores margins and rounds down",
			base:    domain.Size{Cols: 10, Rows: 8},
			content: domain.Size{Cols: 5, Rows: 3},
			anchor:  domain.AnchorCenter,
			margins: Margins{Top: 100, Right: 100, Bottom: 100, Left: 100},
			want:    domain.Rect{X: 2, Y: 2, Width: 5, Height: 3},
		},
		{
			name:    "invalid anchor falls back to center",
			base:    domain.Size{Cols: 10, Rows: 8},
			content: domain.Size{Cols: 5, Rows: 3},
			anchor:  domain.Anchor(99),
			margins: Margins{Top: 100, Right: 100, Bottom: 100, Left: 100},
			want:    domain.Rect{X: 2, Y: 2, Width: 5, Height: 3},
		},
		{
			name:    "nonpositive base is empty",
			base:    domain.Size{Cols: 0, Rows: 8},
			content: domain.Size{Cols: 5, Rows: 3},
			anchor:  domain.AnchorTopLeft,
			want:    domain.Rect{},
		},
		{
			name:    "negative content clamps to zero",
			base:    domain.Size{Cols: 10, Rows: 8},
			content: domain.Size{Cols: -5, Rows: -3},
			anchor:  domain.AnchorBottomRight,
			want:    domain.Rect{X: 10, Y: 8},
		},
		{
			name:    "oversized content clamps to base",
			base:    domain.Size{Cols: 10, Rows: 8},
			content: domain.Size{Cols: 50, Rows: 30},
			anchor:  domain.AnchorTopLeft,
			margins: Margins{Top: 2, Left: 3},
			want:    domain.Rect{Width: 10, Height: 8},
		},
		{
			name:    "negative margins normalize to zero",
			base:    domain.Size{Cols: 10, Rows: 8},
			content: domain.Size{Cols: 5, Rows: 3},
			anchor:  domain.AnchorTopLeft,
			margins: Margins{Top: -2, Left: -3},
			want:    domain.Rect{Width: 5, Height: 3},
		},
		{
			name:    "excessive relevant margins clamp coordinates",
			base:    domain.Size{Cols: 10, Rows: 8},
			content: domain.Size{Cols: 5, Rows: 3},
			anchor:  domain.AnchorBottomRight,
			margins: Margins{Bottom: 20, Right: 20},
			want:    domain.Rect{Width: 5, Height: 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Place(tt.base, tt.content, tt.anchor, tt.margins); got != tt.want {
				t.Fatalf("Place() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
