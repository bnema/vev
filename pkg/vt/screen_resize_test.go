package vt

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestResize(t *testing.T) {
	setRow := func(s *Screen, y int, text string) {
		for x, r := range []rune(text) {
			if x >= s.Frame.Width {
				break
			}
			s.Frame.Set(x, y, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
		}
	}
	rowString := func(s *Screen, y int) string {
		runes := make([]rune, s.Frame.Width)
		for x := range s.Frame.Width {
			runes[x] = s.Frame.At(x, y).Rune
		}
		return string(runes)
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "same size preserves content and skips FullRedraw",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.ClearDamage()
				setRow(s, 0, "Hello")

				s.Resize(5, 3)

				if got := rowString(s, 0); got != "Hello" {
					t.Fatalf("row 0 = %q, want Hello", got)
				}
				if d := s.Damage(); len(d) != 0 {
					t.Fatalf("same-size resize damage = %v, want none", damageKinds(d))
				}
			},
		},
		{
			name: "grow preserves visible content and cursor",
			run: func(t *testing.T) {
				s := NewScreen(5, 2)
				setRow(s, 0, "Hello")
				setRow(s, 1, "World")
				s.Row, s.Col = 1, 4

				s.Resize(8, 4)

				if got := rowString(s, 0); got != "Hello   " {
					t.Fatalf("row 0 = %q", got)
				}
				if got := rowString(s, 1); got != "World   " {
					t.Fatalf("row 1 = %q", got)
				}
				if s.Row != 1 || s.Col != 4 {
					t.Fatalf("cursor = (%d,%d), want (1,4)", s.Row, s.Col)
				}
				if !hasDamageKind(s.Damage(), renderer.DamageFullRedraw) {
					t.Fatalf("resize damage = %v, want FullRedraw", damageKinds(s.Damage()))
				}
			},
		},
		{
			name: "height shrink evicts top lines and follows cursor",
			run: func(t *testing.T) {
				s := NewScreen(4, 5)
				for y, text := range []string{"0000", "1111", "2222", "3333", "4444"} {
					setRow(s, y, text)
				}
				s.Row, s.Col = 4, 2
				var evicted []string
				s.OnLineEvicted = func(row []renderer.Cell) {
					runes := make([]rune, len(row))
					for i, c := range row {
						runes[i] = c.Rune
					}
					evicted = append(evicted, string(runes))
				}

				s.Resize(4, 3)

				if got, want := evicted, []string{"0000", "1111"}; strings.Join(got, ",") != strings.Join(want, ",") {
					t.Fatalf("evicted = %v, want %v", got, want)
				}
				if got := rowString(s, 0); got != "2222" {
					t.Fatalf("row 0 = %q", got)
				}
				if s.Row != 2 || s.Col != 2 {
					t.Fatalf("cursor = (%d,%d), want (2,2)", s.Row, s.Col)
				}
				if !hasDamageKind(s.Damage(), renderer.DamageFullRedraw) {
					t.Fatalf("shrink damage = %v, want FullRedraw", damageKinds(s.Damage()))
				}
			},
		},
		{
			name: "soft wrapped lines reflow on shrink and growth",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("abcdefgh"))

				s.Resize(3, 3)
				if got := []string{rowString(s, 0), rowString(s, 1), rowString(s, 2)}; strings.Join(got, ",") != "abc,def,gh " {
					t.Fatalf("shrink rows = %q", got)
				}

				s.Resize(5, 3)
				if got := []string{rowString(s, 0), rowString(s, 1)}; strings.Join(got, ",") != "abcde,fgh  " {
					t.Fatalf("growth rows = %q", got)
				}
			},
		},
		{
			name: "erase preserves a soft wrap boundary",
			run: func(t *testing.T) {
				s := NewScreen(4, 2)
				s.Write([]byte("abcdef"))
				s.Write([]byte("\x1b[1;1H\x1b[2K"))

				s.Resize(3, 2)
				if got := rowString(s, 0); got != "ef " {
					t.Fatalf("erased soft line reflow = %q", got)
				}
			},
		},
		{
			name: "wide edge padding does not become logical content",
			run: func(t *testing.T) {
				s := NewScreen(4, 3)
				s.Write([]byte("abc界x"))

				s.Resize(3, 3)
				if got := rowString(s, 0); got != "abc" {
					t.Fatalf("first reflow row = %q", got)
				}
				assertCell(t, s, 0, 1, '界')
				if !cellAt(s, 1, 1).Continuation {
					t.Fatal("wide continuation was not retained")
				}
				assertCell(t, s, 2, 1, 'x')
				assertNoOrphanWideCells(t, s)
			},
		},
		{
			name: "width truncates and pads without reflow",
			run: func(t *testing.T) {
				s := NewScreen(6, 2)
				setRow(s, 0, "abcdef")
				setRow(s, 1, "ghijkl")

				s.Resize(3, 2)
				if got := rowString(s, 0); got != "abc" {
					t.Fatalf("truncated row = %q", got)
				}
				s.Resize(5, 2)
				if got := rowString(s, 1); got != "ghi  " {
					t.Fatalf("padded row = %q", got)
				}
			},
		},
		{
			name: "width shrink repairs truncated wide head",
			run: func(t *testing.T) {
				s := NewScreen(4, 1)
				s.Write([]byte("AB界"))

				s.Resize(3, 1)

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 0, 'B')
				assertBlank(t, s, 2, 0)
				assertNoOrphanWideCells(t, s)
			},
		},
		{
			name: "random wide resize repairs copied rows",
			run: func(t *testing.T) {
				rng := rand.New(rand.NewSource(1))
				for range 200 {
					s := NewScreen(8, 4)
					s.Write([]byte("a界b好c語d"))
					s.Row = rng.Intn(s.Frame.Height)

					s.Resize(rng.Intn(8)+1, rng.Intn(4)+1)

					assertNoOrphanWideCells(t, s)
				}
			},
		},
		{
			name: "cursor and saved cursor clamp",
			run: func(t *testing.T) {
				s := NewScreen(6, 4)
				s.Row, s.Col = 3, 5
				s.saveCursor()

				s.Resize(3, 2)

				if s.Row != 1 || s.Col != 2 {
					t.Fatalf("cursor = (%d,%d), want (1,2)", s.Row, s.Col)
				}
				s.Row, s.Col = 0, 0
				s.restoreCursor()
				if s.Row != 1 || s.Col != 2 {
					t.Fatalf("restored cursor = (%d,%d), want (1,2)", s.Row, s.Col)
				}
			},
		},
		{
			name: "alternate resize keeps saved normal content",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				setRow(s, 0, "shell")
				s.Row = 0
				s.Write([]byte("\x1b[?1049h"))
				setRow(s, 0, "alt!!")

				s.Resize(7, 4)
				s.Write([]byte("\x1b[?1049l"))

				if got := rowString(s, 0); got != "shell  " {
					t.Fatalf("normal row after alt resize = %q", got)
				}
			},
		},
		{
			name: "alternate resize shifts active and saved cursors",
			run: func(t *testing.T) {
				s := NewScreen(5, 5)
				s.Row, s.Col = 3, 4
				s.saveCursor()
				s.scrollTop, s.scrollBottom = 1, 4
				s.Row = 4
				s.Write([]byte("\x1b[?1049h"))
				s.Row, s.Col = 3, 4
				s.saveCursor()
				s.Row = 4

				// The expected rows account for the resize shift that keeps the
				// cursor visible after NewScreen -> saveCursor -> Resize -> restoreCursor,
				// not just width/height clamping.
				s.Resize(3, 3)

				if s.Row != 2 || s.Col != 2 {
					t.Fatalf("active alt cursor = (%d,%d), want (2,2)", s.Row, s.Col)
				}
				s.Row, s.Col = 0, 0
				s.restoreCursor()
				if s.Row != 1 || s.Col != 2 {
					t.Fatalf("active alt saved cursor = (%d,%d), want (1,2)", s.Row, s.Col)
				}

				s.Write([]byte("\x1b[?1049l"))
				if s.scrollTop != 0 || s.scrollBottom != 2 {
					t.Fatalf("normal scroll region = (%d,%d), want (0,2)", s.scrollTop, s.scrollBottom)
				}
				s.Row, s.Col = 0, 0
				s.restoreCursor()
				if s.Row != 1 || s.Col != 2 {
					t.Fatalf("normal saved cursor = (%d,%d), want (1,2)", s.Row, s.Col)
				}
			},
		},
		{
			name: "style mouse cursor and bracketed paste modes survive",
			run: func(t *testing.T) {
				s := NewScreen(5, 2)
				s.Write([]byte("\x1b[1m\x1b[?1000h\x1b[?1006h\x1b[?2004h\x1b[?25l\x1b[3 q"))

				s.Resize(6, 3)

				if !s.Style.Bold {
					t.Fatal("bold style was reset")
				}
				if mode, sgr := s.MouseMode(); mode != 1000 || !sgr {
					t.Fatalf("mouse mode = (%d,%v), want (1000,true)", mode, sgr)
				}
				if !s.BracketedPasteMode() {
					t.Fatal("bracketed paste mode was reset")
				}
				if s.CursorVisible() {
					t.Fatal("cursor visibility was reset")
				}
				if style, ok := s.CursorStyle(); style != 3 || !ok {
					t.Fatalf("cursor style = (%d,%v), want (3,true)", style, ok)
				}
				s.Write([]byte("\x1b[?2004l"))
				if s.BracketedPasteMode() {
					t.Fatal("bracketed paste mode remained enabled after reset")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
