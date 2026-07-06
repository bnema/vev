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

	t.Run("empty IDs replace per anchor", func(t *testing.T) {
		m := NewToastManager()
		m.Show(now, Toast{Message: "first", Anchor: ToastCenter})
		m.Show(now.Add(time.Second), Toast{Message: "second", Anchor: ToastCenter})
		active := m.Active(now.Add(time.Second))
		if len(active) != 1 || active[0].Message != "second" {
			t.Fatalf("Active() after same-anchor replacement = %+v, want only second toast", active)
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

func TestToastBoundsAnchorsMarginsAndTinyFrames(t *testing.T) {
	message := "Reconnection attempts…"
	width := textWidth(message) + 4
	if width != 26 {
		t.Fatalf("test message width = %d, want 26", width)
	}

	tests := []struct {
		name string
		base domain.Size
		make func() Toast
		want domain.Rect
	}{
		{
			name: "center",
			base: domain.Size{Cols: 80, Rows: 24},
			make: func() Toast { return Toast{Message: "Reconnection attempts…", Anchor: ToastCenter} },
			want: domain.Rect{X: 27, Y: 10, Width: 26, Height: 3},
		},
		{
			name: "top left has two cell margins",
			base: domain.Size{Cols: 80, Rows: 24},
			make: func() Toast { return Toast{Message: "Reconnection attempts…", Anchor: ToastTopLeft} },
			want: domain.Rect{X: 2, Y: 2, Width: 26, Height: 3},
		},
		{
			name: "top right has two cell margins",
			base: domain.Size{Cols: 80, Rows: 24},
			make: func() Toast { return Toast{Message: "Reconnection attempts…", Anchor: ToastTopRight} },
			want: domain.Rect{X: 52, Y: 2, Width: 26, Height: 3},
		},
		{
			name: "bottom left has two cell margins",
			base: domain.Size{Cols: 80, Rows: 24},
			make: func() Toast { return Toast{Message: "Reconnection attempts…", Anchor: ToastBottomLeft} },
			want: domain.Rect{X: 2, Y: 19, Width: 26, Height: 3},
		},
		{
			name: "bottom right has two cell margins",
			base: domain.Size{Cols: 80, Rows: 24},
			make: func() Toast { return Toast{Message: "Reconnection attempts…", Anchor: ToastBottomRight} },
			want: domain.Rect{X: 52, Y: 19, Width: 26, Height: 3},
		},
		{
			name: "zero frame",
			base: domain.Size{},
			make: func() Toast { return Toast{Message: "tiny", Anchor: ToastCenter} },
			want: domain.Rect{},
		},
		{
			name: "one by one clamps",
			base: domain.Size{Cols: 1, Rows: 1},
			make: func() Toast { return Toast{Message: "tiny", Anchor: ToastCenter} },
			want: domain.Rect{X: 0, Y: 0, Width: 1, Height: 1},
		},
		{
			name: "narrow frame clamps width and margin",
			base: domain.Size{Cols: 8, Rows: 2},
			make: func() Toast { return Toast{Message: "long message", Anchor: ToastBottomRight} },
			want: domain.Rect{X: 0, Y: 0, Width: 8, Height: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToastBounds(tt.base, tt.make()); got != tt.want {
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

		CompositeToasts(f, []ActiveToast{{Toast: Toast{ID: "center", Message: "Hi", Anchor: ToastCenter}}}, styles, nil)
		bounds := ToastBounds(domain.Size{Cols: 20, Rows: 7}, Toast{Message: "Hi", Anchor: ToastCenter})
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
		CompositeToasts(f, []ActiveToast{{Toast: Toast{ID: "narrow", Message: "abcdef", Anchor: ToastCenter, MaxWidth: 8}}}, styles, nil)
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
		{Toast: Toast{ID: "old", Message: "Old", Anchor: ToastTopLeft}, ShownAt: now},
		{Toast: Toast{ID: "new", Message: "New", Anchor: ToastTopLeft}, ShownAt: now.Add(time.Second)},
		{Toast: Toast{ID: "center", Message: "Mid", Anchor: ToastCenter}, ShownAt: now},
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

	CompositeToasts(f, []ActiveToast{{Toast: Toast{ID: "dim", Message: "Hi", Anchor: ToastCenter, DimBackground: true}}}, styles, func(renderer.Style) renderer.Style {
		return dimStyle
	})

	outside := f.At(0, 0)
	if outside.Rune != 'a' {
		t.Fatalf("outside rune = %q, want preserved 'a'", outside.Rune)
	}
	if !outside.Style.Equal(dimStyle) {
		t.Fatalf("outside style = %+v, want dimmed %+v", outside.Style, dimStyle)
	}
	bounds := ToastBounds(domain.Size{Cols: 14, Rows: 5}, Toast{Message: "Hi", Anchor: ToastCenter})
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
			CompositeToasts(f, []ActiveToast{{Toast: Toast{Message: "abcdef", Anchor: ToastCenter}}}, styles, nil)
		})
	}
}
