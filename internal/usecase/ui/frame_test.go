package ui

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

func cell(r rune, style renderer.Style, continuation bool) renderer.Cell {
	return renderer.Cell{Rune: r, Style: style, Continuation: continuation}
}

func TestBlitFrameAnchorsAndClips(t *testing.T) {
	style := renderer.Style{Foreground: 1, Background: 2}
	tests := []struct {
		name                                   string
		anchor                                 VerticalAnchor
		srcH, srcW, rectX, rectY, rectW, rectH int
		want                                   string
	}{
		{"top taller", VerticalAnchorTop, 4, 2, 1, 1, 2, 2, "abcd"},
		{"bottom taller", VerticalAnchorBottom, 4, 2, 1, 1, 2, 2, "efgh"},
		{"top shorter", VerticalAnchorTop, 1, 2, 1, 1, 2, 2, "ab  "},
		{"bottom shorter", VerticalAnchorBottom, 1, 2, 1, 1, 2, 2, "  ab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := FrameView{Width: tt.srcW, Height: tt.srcH, Rows: make([][]renderer.Cell, tt.srcH)}
			for y := range src.Rows {
				src.Rows[y] = make([]renderer.Cell, tt.srcW)
				for x := range src.Rows[y] {
					src.Rows[y][x] = cell(rune('a'+y*tt.srcW+x), style, x == 1)
				}
			}
			dst := renderer.NewFrame(5, 5)
			BlitFrame(dst, domain.Rect{X: tt.rectX, Y: tt.rectY, Width: tt.rectW, Height: tt.rectH}, src, tt.anchor)
			got := ""
			for y := 1; y < 3; y++ {
				for x := 1; x < 3; x++ {
					got += string(dst.At(x, y).Rune)
				}
			}
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestBlitFrameDestinationClippingAndSentinels(t *testing.T) {
	dst := renderer.NewFrame(3, 3)
	mark := cell('!', renderer.Style{Foreground: 3}, false)
	for y := 0; y < 3; y++ {
		for x := 0; x < 3; x++ {
			dst.Set(x, y, mark)
		}
	}
	src := FrameView{Width: 2, Height: 2, Rows: [][]renderer.Cell{{cell('a', renderer.DefaultStyle(), false), cell('b', renderer.DefaultStyle(), true)}, {cell('c', renderer.DefaultStyle(), false), cell('d', renderer.DefaultStyle(), true)}}}
	BlitFrame(dst, domain.Rect{X: -1, Y: -1, Width: 3, Height: 3}, src, VerticalAnchorTop)
	if dst.At(0, 0).Rune != 'd' || dst.At(2, 2).Rune != '!' {
		t.Fatalf("unexpected clipping/sentinel")
	}
}

func TestBlitFrameInvalidNoOp(t *testing.T) {
	dst := renderer.NewFrame(2, 2)
	before := dst.Clone()
	cases := []struct {
		r domain.Rect
		s FrameView
		a VerticalAnchor
	}{
		{domain.Rect{}, FrameView{}, VerticalAnchorTop}, {domain.Rect{Width: 1, Height: 1}, FrameView{Width: 0, Height: 1}, VerticalAnchorTop},
		{domain.Rect{Width: 1, Height: 1}, FrameView{Width: 1, Height: 1, Rows: [][]renderer.Cell{{}}}, VerticalAnchor(99)},
	}
	for _, tc := range cases {
		BlitFrame(dst, tc.r, tc.s, tc.a)
	}
	for y := 0; y < 2; y++ {
		for x := 0; x < 2; x++ {
			if !dst.At(x, y).Equal(before.At(x, y)) {
				t.Fatal("invalid blit changed destination")
			}
		}
	}
}

func TestBlitFrameDocumentsAliasUnsupported(t *testing.T) {}
