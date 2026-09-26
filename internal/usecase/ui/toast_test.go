package ui

import (
	"fmt"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

func TestTruncateText(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		maxWidth int
		want     string
	}{
		{"no truncation needed", "hello", 10, "hello"},
		{"truncation with ellipsis", "hello world", 8, "hello w…"},
		{"maxWidth zero", "hello", 0, ""},
		// The ellipsis itself is one cell wide, so any non-positive maxWidth is
		// also "smaller than the ellipsis" and collapses to the same empty result.
		{"maxWidth smaller than ellipsis", "hello world", -1, ""},
		{"wide runes", "日本語のテキスト", 5, "日本…"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TruncateText(tt.text, tt.maxWidth); got != tt.want {
				t.Fatalf("TruncateText(%q, %d) = %q, want %q", tt.text, tt.maxWidth, got, tt.want)
			}
		})
	}
}

func TestToastBoundsAnchorsMarginsAndTinyFrames(t *testing.T) {
	message := "Reconnection attempts…"
	width := textWidth(message) + 4
	if width != 26 {
		t.Fatalf("test message width = %d, want 26", width)
	}

	tests := []struct {
		name  string
		base  domain.Size
		toast Toast
		want  domain.Rect
	}{
		{"top left", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message, Anchor: domain.AnchorTopLeft}, domain.Rect{X: 2, Y: 2, Width: 26, Height: 3}},
		{"top", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message, Anchor: domain.AnchorTop}, domain.Rect{X: 27, Y: 2, Width: 26, Height: 3}},
		{"top right", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message, Anchor: domain.AnchorTopRight}, domain.Rect{X: 52, Y: 2, Width: 26, Height: 3}},
		{"left", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message, Anchor: domain.AnchorLeft}, domain.Rect{X: 2, Y: 10, Width: 26, Height: 3}},
		{"center", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message, Anchor: domain.AnchorCenter}, domain.Rect{X: 27, Y: 10, Width: 26, Height: 3}},
		{"right", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message, Anchor: domain.AnchorRight}, domain.Rect{X: 52, Y: 10, Width: 26, Height: 3}},
		{"bottom left", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message, Anchor: domain.AnchorBottomLeft}, domain.Rect{X: 2, Y: 19, Width: 26, Height: 3}},
		{"bottom", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message, Anchor: domain.AnchorBottom}, domain.Rect{X: 27, Y: 19, Width: 26, Height: 3}},
		{"bottom right", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message, Anchor: domain.AnchorBottomRight}, domain.Rect{X: 52, Y: 19, Width: 26, Height: 3}},
		{"omitted anchor centers", domain.Size{Cols: 80, Rows: 24}, Toast{Message: message}, domain.Rect{X: 27, Y: 10, Width: 26, Height: 3}},
		{"zero frame", domain.Size{}, Toast{Message: "tiny"}, domain.Rect{}},
		{"one by one clamps", domain.Size{Cols: 1, Rows: 1}, Toast{Message: "tiny"}, domain.Rect{Width: 1, Height: 1}},
		{"narrow frame clamps width and margin", domain.Size{Cols: 8, Rows: 2}, Toast{Message: "long message", Anchor: domain.AnchorBottomRight}, domain.Rect{Width: 8, Height: 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToastBounds(tt.base, tt.toast); got != tt.want {
				t.Fatalf("ToastBounds() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDrawToastDrawsBorderedToastAndTruncates(t *testing.T) {
	styles := ToastStyles{Text: renderer.Style{Foreground: 3, Background: -1}, Box: renderer.Style{Bold: true, Foreground: 4, Background: -1}}

	t.Run("bordered centered toast preserves exterior", func(t *testing.T) {
		f := renderer.NewFrame(20, 7)
		exterior := renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()}
		FillRect(f, domain.Rect{Width: 20, Height: 7}, exterior)

		DrawToast(f, Toast{Message: "Hi", Anchor: domain.AnchorCenter}, styles)
		bounds := ToastBounds(domain.Size{Cols: 20, Rows: 7}, Toast{Message: "Hi", Anchor: domain.AnchorCenter})
		if bounds != (domain.Rect{X: 7, Y: 2, Width: 6, Height: 3}) {
			t.Fatalf("bounds = %+v, want centered 6x3 box", bounds)
		}
		assertRune(t, f, bounds.X, bounds.Y, '┌')
		assertRune(t, f, bounds.X+bounds.Width-1, bounds.Y, '┐')
		assertRune(t, f, bounds.X, bounds.Y+bounds.Height-1, '└')
		assertRune(t, f, bounds.X+bounds.Width-1, bounds.Y+bounds.Height-1, '┘')
		assertRune(t, f, bounds.X+1, bounds.Y, '─')
		assertRune(t, f, bounds.X, bounds.Y+1, '│')
		assertRune(t, f, bounds.X+2, bounds.Y+1, 'H')
		assertRune(t, f, bounds.X+3, bounds.Y+1, 'i')
		assertCell(t, f, 0, 0, exterior)
		assertCell(t, f, 19, 6, exterior)
	})

	t.Run("narrow toast truncates with ellipsis", func(t *testing.T) {
		f := renderer.NewFrame(8, 3)
		DrawToast(f, Toast{Message: "abcdef", Anchor: domain.AnchorCenter, MaxWidth: 8}, styles)
		assertRune(t, f, 0, 0, '┌')
		assertRune(t, f, 7, 0, '┐')
		assertRune(t, f, 2, 1, 'a')
		assertRune(t, f, 3, 1, 'b')
		assertRune(t, f, 4, 1, 'c')
		assertRune(t, f, 5, 1, '…')
	})
}

func TestDrawToastHandlesTinyFrames(t *testing.T) {
	styles := ToastStyles{Text: renderer.DefaultStyle(), Box: renderer.DefaultStyle()}
	for _, size := range []domain.Size{{}, {Cols: 1, Rows: 1}, {Cols: 2, Rows: 1}, {Cols: 1, Rows: 2}} {
		t.Run(fmt.Sprintf("%dx%d", size.Cols, size.Rows), func(t *testing.T) {
			f := renderer.NewFrame(size.Cols, size.Rows)
			DrawToast(f, Toast{Message: "abcdef", Anchor: domain.AnchorCenter}, styles)
		})
	}
}
