package ui

import (
	"fmt"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

func TestToastManagerDurationDismissAndClear(t *testing.T) {
	now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

	t.Run("show returns active immediately", func(t *testing.T) {
		m := NewToastManager()
		m.Show(now, Toast{ID: "one", Message: "hello"})
		active := m.Active(now)
		if len(active) != 1 || active[0].ID != "one" {
			t.Fatalf("Active() = %+v, want one active toast", active)
		}
	})

	t.Run("zero duration remains until dismissed", func(t *testing.T) {
		m := NewToastManager()
		m.Show(now, Toast{ID: "persistent", Duration: 0})
		if !m.HasActive(now.Add(time.Hour)) {
			t.Fatalf("HasActive() = false, want persistent toast after one hour")
		}
	})

	t.Run("positive duration expires at deadline", func(t *testing.T) {
		m := NewToastManager()
		m.Show(now, Toast{ID: "timed", Duration: time.Minute})
		if !m.HasActive(now.Add(time.Minute - time.Nanosecond)) {
			t.Fatalf("HasActive() before deadline = false, want true")
		}
		if m.HasActive(now.Add(time.Minute)) {
			t.Fatalf("HasActive() at deadline = true, want false")
		}
		if got := m.Active(now.Add(2 * time.Minute)); len(got) != 0 {
			t.Fatalf("Active() after deadline = %+v, want none", got)
		}
	})

	t.Run("dismiss removes only matching toast", func(t *testing.T) {
		m := NewToastManager()
		m.Show(now, Toast{ID: "keep"})
		m.Show(now, Toast{ID: "remove"})
		m.Dismiss("remove")
		active := m.Active(now)
		if len(active) != 1 || active[0].ID != "keep" {
			t.Fatalf("Active() after dismiss = %+v, want only keep", active)
		}
	})

	t.Run("empty IDs use named anchors and replace per anchor", func(t *testing.T) {
		anchors := []domain.Anchor{
			domain.AnchorCenter, domain.AnchorTopLeft, domain.AnchorTop, domain.AnchorTopRight,
			domain.AnchorLeft, domain.AnchorRight, domain.AnchorBottomLeft, domain.AnchorBottom, domain.AnchorBottomRight,
		}
		for _, anchor := range anchors {
			m := NewToastManager()
			m.Show(now, Toast{Message: "first", Anchor: anchor})
			m.Show(now.Add(time.Second), Toast{Message: "second", Anchor: anchor})
			active := m.Active(now.Add(time.Second))
			if len(active) != 1 || active[0].Message != "second" || active[0].ID != "anchor:"+anchor.String() {
				t.Fatalf("Active() for %s = %+v, want named replacement", anchor, active)
			}
		}
	})

	t.Run("clear removes all toasts", func(t *testing.T) {
		m := NewToastManager()
		m.Show(now, Toast{ID: "one"})
		m.Show(now, Toast{ID: "two"})
		m.Clear()
		if got := m.Active(now); len(got) != 0 {
			t.Fatalf("Active() after Clear() = %+v, want none", got)
		}
	})
}

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

func TestCompositeToastsDrawsBorderedToastAndTruncates(t *testing.T) {
	styles := ToastStyles{Text: renderer.Style{Foreground: 3, Background: -1}, Box: renderer.Style{Bold: true, Foreground: 4, Background: -1}}

	t.Run("bordered centered toast preserves exterior", func(t *testing.T) {
		f := renderer.NewFrame(20, 7)
		exterior := renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()}
		FillRect(f, domain.Rect{Width: 20, Height: 7}, exterior)

		CompositeToasts(f, []ActiveToast{{Toast: Toast{ID: "center", Message: "Hi", Anchor: domain.AnchorCenter}}}, styles, nil)
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
		CompositeToasts(f, []ActiveToast{{Toast: Toast{ID: "narrow", Message: "abcdef", Anchor: domain.AnchorCenter, MaxWidth: 8}}}, styles, nil)
		assertRune(t, f, 0, 0, '┌')
		assertRune(t, f, 7, 0, '┐')
		assertRune(t, f, 2, 1, 'a')
		assertRune(t, f, 3, 1, 'b')
		assertRune(t, f, 4, 1, 'c')
		assertRune(t, f, 5, 1, '…')
	})
}

func TestCompositeToastsLatestToastPerAnchor(t *testing.T) {
	f := renderer.NewFrame(30, 5)
	now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	styles := ToastStyles{Text: renderer.Style{Foreground: 3, Background: -1}, Box: renderer.Style{Foreground: 4, Background: -1}}

	CompositeToasts(f, []ActiveToast{
		{Toast: Toast{ID: "old", Message: "Old", Anchor: domain.AnchorTopLeft}, ShownAt: now},
		{Toast: Toast{ID: "new", Message: "New", Anchor: domain.AnchorTopLeft}, ShownAt: now.Add(time.Second)},
		{Toast: Toast{ID: "center", Message: "Mid", Anchor: domain.AnchorCenter}, ShownAt: now},
	}, styles, nil)

	assertRune(t, f, 4, 3, 'N')
	assertRune(t, f, 5, 3, 'e')
	assertRune(t, f, 6, 3, 'w')
	for y := 0; y < f.Height; y++ {
		for x := 0; x < f.Width; x++ {
			if f.At(x, y).Rune == 'O' {
				t.Fatalf("old toast rendered at (%d,%d), want only latest toast per anchor", x, y)
			}
		}
	}
}

func TestLatestToastsByAnchorTieAndOrder(t *testing.T) {
	now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	anchors := []domain.Anchor{
		domain.AnchorTopLeft, domain.AnchorTopRight, domain.AnchorBottomLeft,
		domain.AnchorBottomRight, domain.AnchorCenter, domain.AnchorTop,
		domain.AnchorLeft, domain.AnchorRight, domain.AnchorBottom,
	}
	toasts := make([]ActiveToast, 0, len(anchors)+1)
	for _, anchor := range anchors {
		toasts = append(toasts, ActiveToast{Toast: Toast{ID: anchor.String(), Anchor: anchor}, ShownAt: now})
	}
	toasts = append(toasts, ActiveToast{Toast: Toast{ID: "later-input", Anchor: domain.AnchorTopLeft}, ShownAt: now})

	visible := latestToastsByAnchor(toasts)
	if len(visible) != len(anchors) {
		t.Fatalf("latestToastsByAnchor() count = %d, want %d", len(visible), len(anchors))
	}
	for i, anchor := range anchors {
		if visible[i].Anchor != anchor {
			t.Fatalf("visible[%d].Anchor = %s, want %s", i, visible[i].Anchor, anchor)
		}
	}
	if visible[0].ID != "later-input" {
		t.Fatalf("equal-time tie selected %q, want later input", visible[0].ID)
	}
}

func TestCompositeToastsDimsBackgroundWithoutClearingContent(t *testing.T) {
	f := renderer.NewFrame(14, 5)
	baseStyle := renderer.Style{Foreground: 1, Background: 2}
	dimStyle := renderer.Style{Foreground: 8, Background: 9, Inverse: true}
	for y := 0; y < f.Height; y++ {
		for x := 0; x < f.Width; x++ {
			f.Set(x, y, renderer.Cell{Rune: rune('a' + y), Style: baseStyle})
		}
	}
	styles := ToastStyles{Text: renderer.Style{Foreground: 3, Background: -1}, Box: renderer.Style{Bold: true, Foreground: 4, Background: -1}}

	CompositeToasts(f, []ActiveToast{{Toast: Toast{ID: "dim", Message: "Hi", Anchor: domain.AnchorCenter, DimBackground: true}}}, styles, func(renderer.Style) renderer.Style {
		return dimStyle
	})

	outside := f.At(0, 0)
	if outside.Rune != 'a' {
		t.Fatalf("outside rune = %q, want preserved 'a'", outside.Rune)
	}
	if !outside.Style.Equal(dimStyle) {
		t.Fatalf("outside style = %+v, want dimmed %+v", outside.Style, dimStyle)
	}
	bounds := ToastBounds(domain.Size{Cols: 14, Rows: 5}, Toast{Message: "Hi", Anchor: domain.AnchorCenter})
	boxCell := f.At(bounds.X, bounds.Y)
	if boxCell.Rune != '┌' || !boxCell.Style.Equal(styles.Box) {
		t.Fatalf("box cell = %+v, want toast box style", boxCell)
	}
	textCell := f.At(bounds.X+2, bounds.Y+1)
	if textCell.Rune != 'H' || !textCell.Style.Equal(styles.Text) {
		t.Fatalf("text cell = %+v, want toast text style", textCell)
	}
}

func TestCompositeToastsHandlesTinyFrames(t *testing.T) {
	styles := ToastStyles{Text: renderer.DefaultStyle(), Box: renderer.DefaultStyle()}
	for _, size := range []domain.Size{{}, {Cols: 1, Rows: 1}, {Cols: 2, Rows: 1}, {Cols: 1, Rows: 2}} {
		t.Run(fmt.Sprintf("%dx%d", size.Cols, size.Rows), func(t *testing.T) {
			f := renderer.NewFrame(size.Cols, size.Rows)
			CompositeToasts(f, []ActiveToast{{Toast: Toast{Message: "abcdef", Anchor: domain.AnchorCenter}}}, styles, nil)
		})
	}
}
