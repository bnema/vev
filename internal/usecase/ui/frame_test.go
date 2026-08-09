package ui

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
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
					src.Rows[y][x] = cell(rune('a'+y*tt.srcW+x), style, false)
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

func TestBlitFrameNeverSplitsWidePairsAtClippedEdges(t *testing.T) {
	style := renderer.Style{Foreground: 7, Background: 8, Bold: true}
	wide := []renderer.Cell{
		cell('界', style, false),
		cell(0, style, true),
		cell('x', renderer.DefaultStyle(), false),
	}
	mark := cell('!', renderer.Style{Foreground: 3}, false)
	tests := []struct {
		name string
		rect domain.Rect
		dst  renderer.Frame
		want []renderer.Cell
	}{
		{
			name: "left clipped continuation remains untouched",
			rect: domain.Rect{X: -1, Y: 0, Width: 3, Height: 1},
			dst:  markedFrame(3, 1, mark),
			want: []renderer.Cell{mark, wide[2], mark},
		},
		{
			name: "right clipped leader remains untouched",
			rect: domain.Rect{X: 0, Y: 0, Width: 3, Height: 1},
			dst:  markedFrame(1, 1, mark),
			want: []renderer.Cell{mark},
		},
		{
			name: "complete pair retains exact cells",
			rect: domain.Rect{X: 1, Y: 0, Width: 3, Height: 1},
			dst:  markedFrame(4, 1, mark),
			want: []renderer.Cell{mark, wide[0], wide[1], wide[2]},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			BlitFrame(tt.dst, tt.rect, FrameView{Rows: [][]renderer.Cell{wide}, Width: len(wide), Height: 1}, VerticalAnchorTop)
			assertRowEqual(t, tt.dst, 0, tt.want)
		})
	}
}

func TestBlitFrameBoundsExtremeRectanglesToVisibleIntersection(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	minInt := -maxInt - 1
	style := renderer.Style{Foreground: 9}
	src := FrameView{Rows: [][]renderer.Cell{{cell('a', style, false)}}, Width: 1, Height: 1}
	mark := cell('!', renderer.Style{Background: 4}, false)
	tests := []struct {
		name string
		rect domain.Rect
		want []renderer.Cell
	}{
		{"huge visible width copies only source span", domain.Rect{X: 0, Y: 0, Width: maxInt, Height: maxInt}, []renderer.Cell{src.Rows[0][0], mark}},
		{"max coordinate overflow is outside frame", domain.Rect{X: maxInt - 1, Y: maxInt - 1, Width: maxInt, Height: maxInt}, []renderer.Cell{mark, mark}},
		{"min coordinate overflow is outside frame", domain.Rect{X: minInt, Y: minInt, Width: maxInt, Height: maxInt}, []renderer.Cell{mark, mark}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := markedFrame(2, 1, mark)
			BlitFrame(dst, tt.rect, src, VerticalAnchorTop)
			assertRowEqual(t, dst, 0, tt.want)
		})
	}
}

func TestBlitFrameUsesLogicalDestinationRowsAndPreservesClippedSentinels(t *testing.T) {
	mark := cell('!', renderer.Style{Foreground: 3}, false)
	dst := renderer.NewFrame(3, 3)
	dst.ScrollUp(0, 2, 1)
	for y := range dst.Height {
		for x := range dst.Width {
			dst.Set(x, y, mark)
		}
	}
	src := FrameView{Rows: [][]renderer.Cell{
		{cell('a', renderer.Style{Foreground: 1}, false), cell('b', renderer.Style{Foreground: 2}, false)},
		{cell('c', renderer.Style{Foreground: 4}, false), cell('d', renderer.Style{Foreground: 5}, false)},
	}, Width: 2, Height: 2}

	BlitFrame(dst, domain.Rect{X: -1, Y: 1, Width: 3, Height: 2}, src, VerticalAnchorTop)

	assertRowEqual(t, dst, 0, []renderer.Cell{mark, mark, mark})
	assertRowEqual(t, dst, 1, []renderer.Cell{src.Rows[0][1], mark, mark})
	assertRowEqual(t, dst, 2, []renderer.Cell{src.Rows[1][1], mark, mark})
}

func TestBlitFrameClipsEveryEdgeAndPreservesUntouchedSentinels(t *testing.T) {
	mark := cell('!', renderer.Style{Foreground: 3}, false)
	src := FrameView{Rows: [][]renderer.Cell{
		{cell('a', renderer.DefaultStyle(), false), cell('b', renderer.DefaultStyle(), false), cell('c', renderer.DefaultStyle(), false)},
		{cell('d', renderer.DefaultStyle(), false), cell('e', renderer.DefaultStyle(), false), cell('f', renderer.DefaultStyle(), false)},
		{cell('g', renderer.DefaultStyle(), false), cell('h', renderer.DefaultStyle(), false), cell('i', renderer.DefaultStyle(), false)},
	}, Width: 3, Height: 3}

	leftTop := markedFrame(3, 3, mark)
	BlitFrame(leftTop, domain.Rect{X: -1, Y: -1, Width: 3, Height: 3}, src, VerticalAnchorTop)
	assertRowEqual(t, leftTop, 0, []renderer.Cell{src.Rows[1][1], src.Rows[1][2], mark})
	assertRowEqual(t, leftTop, 1, []renderer.Cell{src.Rows[2][1], src.Rows[2][2], mark})
	assertRowEqual(t, leftTop, 2, []renderer.Cell{mark, mark, mark})

	rightBottom := markedFrame(3, 3, mark)
	BlitFrame(rightBottom, domain.Rect{X: 2, Y: 2, Width: 3, Height: 3}, src, VerticalAnchorTop)
	assertRowEqual(t, rightBottom, 0, []renderer.Cell{mark, mark, mark})
	assertRowEqual(t, rightBottom, 1, []renderer.Cell{mark, mark, mark})
	assertRowEqual(t, rightBottom, 2, []renderer.Cell{mark, mark, src.Rows[0][0]})
}

func TestBlitFrameInvalidNoOp(t *testing.T) {
	dst := renderer.NewFrame(2, 2)
	before := dst.Clone()
	cases := []struct {
		r domain.Rect
		s FrameView
		a VerticalAnchor
	}{
		{domain.Rect{}, FrameView{}, VerticalAnchorTop},
		{domain.Rect{Width: 1, Height: 1}, FrameView{Width: 0, Height: 1}, VerticalAnchorTop},
		{domain.Rect{Width: 1, Height: 1}, FrameView{Width: 1, Height: 1, Rows: [][]renderer.Cell{{}}}, VerticalAnchor(99)},
	}
	for _, tc := range cases {
		BlitFrame(dst, tc.r, tc.s, tc.a)
	}
	if !framesEqual(dst, before) {
		t.Fatal("invalid blit changed destination")
	}
}

func markedFrame(width, height int, mark renderer.Cell) renderer.Frame {
	frame := renderer.NewFrame(width, height)
	for y := range height {
		for x := range width {
			frame.Set(x, y, mark)
		}
	}
	return frame
}

func assertRowEqual(t *testing.T, frame renderer.Frame, y int, want []renderer.Cell) {
	t.Helper()
	if len(want) != frame.Width {
		t.Fatalf("want row length %d, frame width %d", len(want), frame.Width)
	}
	for x := range want {
		if got := frame.At(x, y); !got.Equal(want[x]) {
			t.Fatalf("cell %d,%d = %#v, want %#v", x, y, got, want[x])
		}
	}
}
