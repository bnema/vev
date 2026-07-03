package vt

import (
	"strings"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func damageKinds(d []renderer.Damage) []renderer.DamageKind {
	ks := make([]renderer.DamageKind, len(d))
	for i, dd := range d {
		ks[i] = dd.Kind
	}
	return ks
}

func hasDamageKind(d []renderer.Damage, kind renderer.DamageKind) bool {
	for _, dd := range d {
		if dd.Kind == kind {
			return true
		}
	}
	return false
}

func cellAt(s *Screen, x, y int) renderer.Cell {
	return s.Frame.At(x, y)
}

func assertCell(t *testing.T, s *Screen, x, y int, expected rune) {
	t.Helper()
	c := cellAt(s, x, y)
	if c.Rune != expected {
		t.Errorf("cell(%d,%d) rune = %q, want %q", x, y, c.Rune, expected)
	}
	if c.Continuation {
		t.Errorf("cell(%d,%d) unexpectedly marked as continuation", x, y)
	}
}

// assertContinuation asserts the cell at (x,y) is the right half of a
// wide-character pair (Continuation set, Rune 0).
func assertContinuation(t *testing.T, s *Screen, x, y int) {
	t.Helper()
	c := cellAt(s, x, y)
	if !c.Continuation {
		t.Errorf("cell(%d,%d) expected continuation, got rune=%q continuation=%v", x, y, c.Rune, c.Continuation)
	}
	if c.Rune != 0 {
		t.Errorf("cell(%d,%d) continuation rune = %q, want 0", x, y, c.Rune)
	}
}

// assertBlank asserts the cell at (x,y) is a blank default cell.
func assertBlank(t *testing.T, s *Screen, x, y int) {
	t.Helper()
	c := cellAt(s, x, y)
	if c.Rune != ' ' || c.Continuation {
		t.Errorf("cell(%d,%d) = {rune:%q cont:%v}, want blank space", x, y, c.Rune, c.Continuation)
	}
}

func lineText(s *Screen, y int) string {
	out := make([]rune, s.Frame.Width)
	for x := range s.Frame.Width {
		out[x] = s.Frame.At(x, y).Rune
	}
	return string(out)
}

// ---------------------------------------------------------------------------
// Printable ASCII / UTF-8
// ---------------------------------------------------------------------------

func TestWritePrintableAndUTF8(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "printable ASCII advances cursor and records damage",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("Hi"))

				assertCell(t, s, 0, 0, 'H')
				assertCell(t, s, 1, 0, 'i')
				if s.Col != 2 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=2 row=0", s.Col, s.Row)
				}

				// Should have damage.
				d := s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage after writing")
				}
			},
		},
		{
			name: "UTF-8 multi-byte runes occupy one cell each",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("aäéø©"))

				assertCell(t, s, 0, 0, 'a')
				assertCell(t, s, 1, 0, 'ä')
				assertCell(t, s, 2, 0, 'é')
				assertCell(t, s, 3, 0, 'ø')
				assertCell(t, s, 4, 0, '©')
			},
		},
		{
			name: "printable beyond width wraps to next line",
			run: func(t *testing.T) {
				s := NewScreen(4, 2)
				s.Write([]byte("ABCDE"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 0, 'B')
				assertCell(t, s, 2, 0, 'C')
				assertCell(t, s, 3, 0, 'D')
				// E wrapped to next line.
				assertCell(t, s, 0, 1, 'E')
			},
		},
		{
			name: "printable beyond screen height scrolls",
			run: func(t *testing.T) {
				s := NewScreen(3, 2)
				// Fill both lines.
				s.Write([]byte("ABC"))
				s.Write([]byte("DEF"))
				// Row is now 1 (bottom). One more char should scroll.
				s.Write([]byte("G"))

				// After scroll: old row 0 = "DEF", new row 1 = "G  ".
				assertCell(t, s, 0, 0, 'D')
				assertCell(t, s, 1, 0, 'E')
				assertCell(t, s, 2, 0, 'F')
				assertCell(t, s, 0, 1, 'G')
				assertCell(t, s, 1, 1, ' ')

				// Check scroll damage.
				d := s.Damage()
				if !hasDamageKind(d, renderer.DamageScrollUp) {
					t.Error("expected scroll damage")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// Carriage Return / Line Feed / Backspace / Tab
// ---------------------------------------------------------------------------

func TestCursorControlChars(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "CR moves column back to 0",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("Hello"))
				// CR moves column back to 0.
				s.Write([]byte("\rWorld"))

				assertCell(t, s, 0, 0, 'W')
				assertCell(t, s, 1, 0, 'o')
				assertCell(t, s, 2, 0, 'r')
				assertCell(t, s, 3, 0, 'l')
				assertCell(t, s, 4, 0, 'd')
				if s.Col != 5 {
					t.Errorf("col after CR + World = %d, want 5", s.Col)
				}
			},
		},
		{
			name: "LF with CR moves to next line column 0",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("A\r\nB\r\nC"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 0, 1, 'B')
				assertCell(t, s, 0, 2, 'C')
				if s.Row != 2 || s.Col != 1 {
					t.Errorf("cursor at row=%d col=%d, want row=2 col=1", s.Row, s.Col)
				}
			},
		},
		{
			name: "LF alone does not reset column",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("AB\nC"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 0, 'B')
				assertCell(t, s, 0, 1, ' ')
				assertCell(t, s, 2, 1, 'C')
				if s.Row != 1 || s.Col != 3 {
					t.Errorf("cursor at row=%d col=%d, want row=1 col=3", s.Row, s.Col)
				}
			},
		},
		{
			name: "LF at pending wrap boundary advances exactly one line",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("ABCDE\nZ"))

				assertCell(t, s, 4, 0, 'E')
				assertCell(t, s, 4, 1, 'Z')
				if s.Row != 1 || s.Col != 5 {
					t.Errorf("cursor at row=%d col=%d, want row=1 col=5", s.Row, s.Col)
				}
			},
		},
		{
			name: "LF scrolls when both lines are full",
			run: func(t *testing.T) {
				s := NewScreen(5, 2)
				// Fill both lines then CRLF to scroll and return to column 0.
				s.Write([]byte("AAAAA"))
				s.Write([]byte("BBBBB"))
				s.Write([]byte("\r\nCCCCC"))

				assertCell(t, s, 0, 0, 'B')
				assertCell(t, s, 4, 1, 'C')
			},
		},
		{
			name: "BS moves cursor back without touching the cell",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("Hello\b"))

				if s.Col != 4 {
					t.Errorf("col after BS = %d, want 4", s.Col)
				}
				assertCell(t, s, 4, 0, 'o') // cell unchanged, just cursor moved
			},
		},
		{
			name: "BS at column zero is a no-op",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("\b")) // BS at col 0 should be no-op

				if s.Col != 0 {
					t.Errorf("col = %d, want 0", s.Col)
				}
			},
		},
		{
			name: "tab advances to next 8-column boundary",
			run: func(t *testing.T) {
				s := NewScreen(20, 3)
				s.Write([]byte("A\tB"))

				assertCell(t, s, 0, 0, 'A')
				// Tab advances to next 8-column boundary (col 8).
				for i := 1; i < 8; i++ {
					assertCell(t, s, i, 0, ' ')
				}
				assertCell(t, s, 8, 0, 'B')
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// CSI SGR
// ---------------------------------------------------------------------------

func TestSGR(t *testing.T) {
	tests := []struct {
		name  string
		seq   string
		check func(t *testing.T, s *Screen)
	}{
		{
			name: "SGR 0 resets to default style",
			seq:  "\x1b[0m",
			check: func(t *testing.T, s *Screen) {
				if s.Style != (renderer.DefaultStyle()) {
					t.Errorf("style after SGR 0 = %+v, want default", s.Style)
				}
			},
		},
		{
			name: "SGR 1 sets bold",
			seq:  "\x1b[1m",
			check: func(t *testing.T, s *Screen) {
				if !s.Style.Bold {
					t.Error("expected bold")
				}
			},
		},
		{
			name: "SGR 7 sets inverse",
			seq:  "\x1b[7m",
			check: func(t *testing.T, s *Screen) {
				if !s.Style.Inverse {
					t.Error("expected inverse")
				}
			},
		},
		{
			name: "SGR 31 sets red foreground",
			seq:  "\x1b[31m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != 1 {
					t.Errorf("foreground = %d, want 1 (red)", s.Style.Foreground)
				}
			},
		},
		{
			name: "SGR 44 sets blue background",
			seq:  "\x1b[44m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != 4 {
					t.Errorf("background = %d, want 4 (blue)", s.Style.Background)
				}
			},
		},
		{
			name: "SGR 38;5 sets 256-color foreground",
			seq:  "\x1b[38;5;82m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != 82 {
					t.Errorf("foreground = %d, want 82", s.Style.Foreground)
				}
			},
		},
		{
			name: "SGR 48;5 sets 256-color background",
			seq:  "\x1b[48;5;200m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != 200 {
					t.Errorf("background = %d, want 200", s.Style.Background)
				}
			},
		},
		{
			name: "SGR 91 sets bright red foreground",
			seq:  "\x1b[91m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != 9 {
					t.Errorf("foreground = %d, want 9 (bright red)", s.Style.Foreground)
				}
			},
		},
		{
			name: "SGR 107 sets bright white background",
			seq:  "\x1b[107m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != 15 {
					t.Errorf("background = %d, want 15 (bright white)", s.Style.Background)
				}
			},
		},
		{
			name: "SGR 22 resets bold",
			seq:  "\x1b[1mHello\x1b[22mWorld",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Bold {
					t.Error("bold should be reset after 22")
				}
			},
		},
		{
			name: "SGR 27 resets inverse",
			seq:  "\x1b[7m\x1b[27m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Inverse {
					t.Error("inverse should be reset after 27")
				}
			},
		},
		{
			name: "SGR 39 resets foreground to default",
			seq:  "\x1b[31m\x1b[39m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != -1 {
					t.Errorf("foreground should be default after 39, got %d", s.Style.Foreground)
				}
			},
		},
		{
			name: "SGR 49 resets background to default",
			seq:  "\x1b[44m\x1b[49m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != -1 {
					t.Errorf("background should be default after 49, got %d", s.Style.Background)
				}
			},
		},
		{
			name: "SGR style is applied to written cells",
			seq:  "\x1b[1;31mX",
			check: func(t *testing.T, s *Screen) {
				c := cellAt(s, 0, 0)
				if !c.Style.Bold {
					t.Error("cell should be bold")
				}
				if c.Style.Foreground != 1 {
					t.Errorf("cell foreground = %d, want 1", c.Style.Foreground)
				}
			},
		},
		{
			name: "multiple sequential SGR sequences accumulate",
			seq:  "\x1b[31m\x1b[1m\x1b[44mX",
			check: func(t *testing.T, s *Screen) {
				c := cellAt(s, 0, 0)
				if c.Style.Foreground != 1 || !c.Style.Bold || c.Style.Background != 4 {
					t.Errorf("cell style = %+v, want fg=1 bold bg=4", c.Style)
				}
			},
		},
		{
			name: "empty SGR params reset style",
			seq:  "\x1b[31m\x1b[1m\x1b[m", // empty params → reset
			check: func(t *testing.T, s *Screen) {
				if s.Style != (renderer.DefaultStyle()) {
					t.Errorf("style after empty SGR = %+v, want default", s.Style)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 3)
			s.Write([]byte(tt.seq))
			tt.check(t, s)
		})
	}
}

func TestSGRTruecolor(t *testing.T) {
	tests := []struct {
		name  string
		seq   string
		check func(t *testing.T, s *Screen)
	}{
		{
			name: "SGR 38;2 sets truecolor foreground on style and cell",
			seq:  "\x1b[38;2;12;34;56mX",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 12, G: 34, B: 56}
				if !s.Style.HasForegroundRGB || s.Style.ForegroundRGB != want {
					t.Errorf("foreground RGB = (%v, %+v), want true/%+v", s.Style.HasForegroundRGB, s.Style.ForegroundRGB, want)
				}
				if got := cellAt(s, 0, 0).Style.ForegroundRGB; got != want {
					t.Errorf("cell foreground RGB = %+v, want %+v", got, want)
				}
			},
		},
		{
			name: "SGR 48;2 sets truecolor background",
			seq:  "\x1b[48;2;200;100;50m",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 200, G: 100, B: 50}
				if !s.Style.HasBackgroundRGB || s.Style.BackgroundRGB != want {
					t.Errorf("background RGB = (%v, %+v), want true/%+v", s.Style.HasBackgroundRGB, s.Style.BackgroundRGB, want)
				}
			},
		},
		{
			name: "SGR 39 after truecolor clears truecolor foreground",
			seq:  "\x1b[38;2;12;34;56m\x1b[39m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.HasForegroundRGB || s.Style.Foreground != -1 {
					t.Errorf("foreground after reset = rgb:%v index:%d, want default", s.Style.HasForegroundRGB, s.Style.Foreground)
				}
			},
		},
		{
			name: "truecolor components are clamped to 0-255",
			seq:  "\x1b[38;2;-1;300;42m",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 0, G: 255, B: 42}
				if s.Style.ForegroundRGB != want {
					t.Errorf("foreground RGB = %+v, want %+v", s.Style.ForegroundRGB, want)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 3)
			s.Write([]byte(tt.seq))
			tt.check(t, s)
		})
	}
}

func TestSGRColorModeTransitionsClearInactiveState(t *testing.T) {
	tests := []struct {
		name          string
		seq           string
		foreground    int
		background    int
		hasForeground bool
		hasBackground bool
	}{
		{name: "foreground rgb to bright", seq: "\x1b[38;2;1;2;3m\x1b[91m", foreground: 9, background: -1},
		{name: "foreground rgb to indexed", seq: "\x1b[38;2;1;2;3m\x1b[38;5;82m", foreground: 82, background: -1},
		{name: "foreground indexed to rgb", seq: "\x1b[38;5;82m\x1b[38;2;4;5;6m", foreground: -1, background: -1, hasForeground: true},
		{name: "background rgb to bright", seq: "\x1b[48;2;1;2;3m\x1b[107m", foreground: -1, background: 15},
		{name: "background rgb to indexed", seq: "\x1b[48;2;1;2;3m\x1b[48;5;200m", foreground: -1, background: 200},
		{name: "background rgb to default", seq: "\x1b[48;2;1;2;3m\x1b[49m", foreground: -1, background: -1},
		{name: "background indexed to rgb", seq: "\x1b[48;5;200m\x1b[48;2;4;5;6m", foreground: -1, background: -1, hasBackground: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 3)
			s.Write([]byte(tt.seq))

			if s.Style.Foreground != tt.foreground || s.Style.HasForegroundRGB != tt.hasForeground {
				t.Errorf("foreground = %d rgb:%v, want %d rgb:%v", s.Style.Foreground, s.Style.HasForegroundRGB, tt.foreground, tt.hasForeground)
			}
			if s.Style.Background != tt.background || s.Style.HasBackgroundRGB != tt.hasBackground {
				t.Errorf("background = %d rgb:%v, want %d rgb:%v", s.Style.Background, s.Style.HasBackgroundRGB, tt.background, tt.hasBackground)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Clear screen / clear line / erase modes
// ---------------------------------------------------------------------------

func TestClearAndErase(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "CSI 2J clears the whole screen",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("HelloWorldABCDE"))
				// Clear damage from writing.
				s.ClearDamage()

				s.Write([]byte("\x1b[2J"))

				// All cells should be blank.
				for y := range 3 {
					for x := range 5 {
						if c := cellAt(s, x, y); c.Rune != ' ' {
							t.Errorf("cell(%d,%d) = %q, want space after clear", x, y, c.Rune)
						}
					}
				}

				d := s.Damage()
				if !hasDamageKind(d, renderer.DamageClear) {
					t.Error("expected DamageClear after CSI 2 J")
				}
			},
		},
		{
			name: "CSI K clears from cursor to end of line",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("HelloWorld"))
				s.ClearDamage()

				// Position cursor at row 1 col 6 (0-indexed: 0, 5) then clear to end.
				s.Write([]byte("\x1b[1;6H"))
				s.Write([]byte("\x1b[K"))

				for x := 5; x < 10; x++ {
					if c := cellAt(s, x, 0); c.Rune != ' ' {
						t.Errorf("cell(%d,0) = %q, want space after clear line", x, c.Rune)
					}
				}
				// First 5 chars should remain.
				assertCell(t, s, 0, 0, 'H')

				d := s.Damage()
				if !hasDamageKind(d, renderer.DamageClear) {
					t.Error("expected DamageClear after CSI K")
				}
			},
		},
		{
			name: "erase display modes 0/1/2 (implicit, 1, 2/3)",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("abcdefghijklmno"))
				s.Write([]byte("\x1b[2;3H"))
				s.ClearDamage()
				s.Write([]byte("\x1b[J"))
				assertCell(t, s, 0, 0, 'a')
				assertCell(t, s, 1, 1, 'g')
				if c := cellAt(s, 2, 1); c.Rune != ' ' {
					t.Fatalf("cursor-to-end clear left %q at cursor", c.Rune)
				}

				s = NewScreen(5, 3)
				s.Write([]byte("abcdefghijklmno"))
				s.Write([]byte("\x1b[2;3H"))
				s.Write([]byte("\x1b[1J"))
				if c := cellAt(s, 0, 0); c.Rune != ' ' {
					t.Fatalf("start-to-cursor clear left %q at start", c.Rune)
				}
				assertCell(t, s, 3, 1, 'i')
			},
		},
		{
			name: "erase line modes 0/1/2",
			run: func(t *testing.T) {
				s := NewScreen(5, 2)
				s.Write([]byte("abcde"))
				s.Write([]byte("\x1b[1;3H"))
				s.Write([]byte("\x1b[1K"))
				if c := cellAt(s, 0, 0); c.Rune != ' ' {
					t.Fatalf("line start clear left %q", c.Rune)
				}
				assertCell(t, s, 3, 0, 'd')

				s = NewScreen(5, 2)
				s.Write([]byte("abcde"))
				s.Write([]byte("\x1b[1;3H"))
				s.Write([]byte("\x1b[2K"))
				for x := range 5 {
					if c := cellAt(s, x, 0); c.Rune != ' ' {
						t.Fatalf("line clear left %q at %d", c.Rune, x)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// Cursor move
// ---------------------------------------------------------------------------

func TestCursorMove(t *testing.T) {
	tests := []struct {
		name    string
		seq     string
		initRow int
		initCol int
		wantRow int
		wantCol int
	}{
		{name: "CSI H moves to explicit row/col", seq: "\x1b[3;4H", wantRow: 2, wantCol: 3},
		{name: "CSI H clamps to bottom-right when out of bounds", seq: "\x1b[100;200H", wantRow: 4, wantCol: 9},
		{name: "CSI f moves to explicit row/col", seq: "\x1b[2;8f", wantRow: 1, wantCol: 7},
		{name: "CSI C moves forward after CR", seq: "\r\x1b[3C", wantRow: 0, wantCol: 3},
		{name: "CSI H with no params moves to (1,1)", seq: "\x1b[H", wantRow: 0, wantCol: 0},
		{name: "CSI ;5H uses default row with explicit column", seq: "\x1b[;5H", initRow: 3, initCol: 4, wantRow: 0, wantCol: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 5)
			s.Row, s.Col = tt.initRow, tt.initCol
			s.Write([]byte(tt.seq))

			if s.Row != tt.wantRow || s.Col != tt.wantCol {
				t.Errorf("cursor at row=%d col=%d, want row=%d col=%d", s.Row, s.Col, tt.wantRow, tt.wantCol)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ESC sequences
// ---------------------------------------------------------------------------

func TestESCSequences(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "ESC = (DECKPAM) is ignored, following text is printable",
			run: func(t *testing.T) {
				s := NewScreen(10, 2)
				s.Write([]byte("\x1b=abc"))

				assertCell(t, s, 0, 0, 'a')
				assertCell(t, s, 1, 0, 'b')
				assertCell(t, s, 2, 0, 'c')
				if s.Col != 3 || s.Row != 0 {
					t.Errorf("cursor at row=%d col=%d, want row=0 col=3", s.Row, s.Col)
				}
			},
		},
		{
			name: "ESC 7 / ESC 8 save and restore the cursor",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("abc\x1b7\x1b[2;5HZZ\x1b8X"))

				assertCell(t, s, 0, 0, 'a')
				assertCell(t, s, 1, 0, 'b')
				assertCell(t, s, 2, 0, 'c')
				assertCell(t, s, 3, 0, 'X')
				assertCell(t, s, 4, 1, 'Z')
				assertCell(t, s, 5, 1, 'Z')
				if s.Row != 0 || s.Col != 4 {
					t.Errorf("cursor at row=%d col=%d, want row=0 col=4", s.Row, s.Col)
				}
			},
		},
		{
			name: "ESC D (index), ESC E (next line), ESC M (reverse index)",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("A\x1bDB\x1bEC"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 1, 'B')
				assertCell(t, s, 0, 2, 'C')

				s = NewScreen(5, 3)
				s.Write([]byte("\x1b[2;1Hmid\r\x1bMtop"))
				assertCell(t, s, 0, 0, 't')
				assertCell(t, s, 0, 1, 'm')
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// CSI editing sequences (cursor moves, ICH/DCH, IL/DL, scroll region, alt screen)
// ---------------------------------------------------------------------------

func TestCSIEditingSequences(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "directional cursor moves (A/B/C/D)",
			run: func(t *testing.T) {
				s := NewScreen(10, 4)
				s.Write([]byte("\x1b[3;5HX\x1b[2D<\x1b[1A^\x1b[1Bv\x1b[3C>"))

				assertCell(t, s, 4, 2, 'X')
				assertCell(t, s, 3, 2, '<')
				assertCell(t, s, 4, 1, '^')
				assertCell(t, s, 5, 2, 'v')
				assertCell(t, s, 9, 2, '>')
			},
		},
		{
			name: "insert and delete characters (@ and P)",
			run: func(t *testing.T) {
				s := NewScreen(8, 2)
				s.Write([]byte("abcdef"))
				s.Write([]byte("\x1b[1;3H\x1b[2@XY"))
				if got := lineText(s, 0); got != "abXYcdef" {
					t.Fatalf("line after ICH = %q, want %q", got, "abXYcdef")
				}

				s.Write([]byte("\x1b[1;4H\x1b[3P"))
				if got := lineText(s, 0); got != "abXef   " {
					t.Fatalf("line after DCH = %q, want %q", got, "abXef   ")
				}
			},
		},
		{
			name: "insert and delete lines (L and M)",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("11111222223333344444"))
				s.Write([]byte("\x1b[2;1H\x1b[L"))
				if got := lineText(s, 1); got != "     " {
					t.Fatalf("inserted line = %q, want blank", got)
				}
				if got := lineText(s, 2); got != "22222" {
					t.Fatalf("shifted line = %q, want 22222", got)
				}

				s.Write([]byte("\x1b[2;1H\x1b[M"))
				if got := lineText(s, 1); got != "22222" {
					t.Fatalf("line after delete = %q, want 22222", got)
				}
			},
		},
		{
			name: "scroll region (CSI r) confines LF scrolling",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("11111222223333344444"))
				s.Write([]byte("\x1b[2;3r\x1b[3;1H\n"))

				if got := lineText(s, 0); got != "11111" {
					t.Fatalf("row outside region changed: %q", got)
				}
				if got := lineText(s, 1); got != "33333" {
					t.Fatalf("region did not scroll up, row 1 = %q", got)
				}
				if got := lineText(s, 2); got != "     " {
					t.Fatalf("region bottom not blanked, row 2 = %q", got)
				}
				if got := lineText(s, 3); got != "44444" {
					t.Fatalf("row below region changed: %q", got)
				}
			},
		},
		{
			name: "private alternate screen mode restores normal screen on exit",
			run: func(t *testing.T) {
				s := NewScreen(20, 5)
				s.Write([]byte("normal-line"))
				s.Write([]byte("\x1b[?1049h\x1b[2J\x1b[HLOCKED\x1b[3;4HAPP"))
				s.Write([]byte("\x1b[?1049lafter"))

				if got := lineText(s, 0); got != "normal-lineafter    " {
					t.Fatalf("row 0 = %q, want restored normal screen with post-exit text", got)
				}
				if got := lineText(s, 2); got != "                    " {
					t.Fatalf("alternate content leaked into row 2: %q", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// Integration scenarios
// ---------------------------------------------------------------------------

func TestIntegrationScenarios(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "renderer stays in sync after VT edit damage",
			run: func(t *testing.T) {
				s := NewScreen(8, 2)
				r := renderer.New(renderer.Capabilities{})
				s.Write([]byte("abcdef"))
				if _, err := r.Draw(s.Frame, s.Damage()); err != nil {
					t.Fatal(err)
				}
				s.ClearDamage()

				s.Write([]byte("\x1b[1;3H\x1b[2@XY"))
				if _, err := r.Draw(s.Frame, s.Damage()); err != nil {
					t.Fatal(err)
				}
				s.ClearDamage()

				out, err := r.Draw(s.Frame, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(out) != 0 {
					t.Fatalf("renderer emitted stale follow-up output after VT edit damage: %q", string(out))
				}
			},
		},
		{
			name: "DCS sequence terminated by ST is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1bP+q696e646e\x1b\\prompt"))

				assertCell(t, s, 0, 0, 'p')
				assertCell(t, s, 1, 0, 'r')
				assertCell(t, s, 2, 0, 'o')
				assertCell(t, s, 3, 0, 'm')
				assertCell(t, s, 4, 0, 'p')
				assertCell(t, s, 5, 0, 't')
				if s.Col != 6 || s.Row != 0 {
					t.Errorf("cursor at row=%d col=%d, want row=0 col=6", s.Row, s.Col)
				}
			},
		},
		{
			name: "fish-like prompt redraw preserves typed characters",
			run: func(t *testing.T) {
				s := NewScreen(10, 2)
				s.Write([]byte("> "))
				s.Write([]byte("a\r\x1b[3C"))
				s.Write([]byte("\x1b[91mb\x1b[39m\x1b[K\r\x1b[4C"))

				assertCell(t, s, 0, 0, '>')
				assertCell(t, s, 1, 0, ' ')
				assertCell(t, s, 2, 0, 'a')
				assertCell(t, s, 3, 0, 'b')
				if s.Row != 0 || s.Col != 4 {
					t.Errorf("cursor at row=%d col=%d, want row=0 col=4", s.Row, s.Col)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// Scroll damage
// ---------------------------------------------------------------------------

func TestScrollDamage(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "scroll produces both DamageScrollUp and DamageText",
			run: func(t *testing.T) {
				s := NewScreen(5, 2)
				// Fill screen then cause a scroll.
				s.Write([]byte("AAAAABBBBB")) // both rows filled
				s.ClearDamage()

				s.Write([]byte("CCCCC")) // forces a scroll up

				d := s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage after scroll")
				}

				if !hasDamageKind(d, renderer.DamageScrollUp) {
					t.Errorf("expected DamageScrollUp in %v", damageKinds(d))
				}
				if !hasDamageKind(d, renderer.DamageText) {
					t.Errorf("expected DamageText in %v", damageKinds(d))
				}
			},
		},
		{
			name: "scroll damage coordinates match the full-width scrolled region",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("AAAAABBBBBCCCCC")) // fill all 3 rows
				s.ClearDamage()

				// Write another char to force scroll.
				s.Write([]byte("D"))

				d := s.Damage()
				var scrollDamage *renderer.Damage
				for i, dd := range d {
					if dd.Kind == renderer.DamageScrollUp {
						scrollDamage = &d[i]
						break
					}
				}
				if scrollDamage == nil {
					t.Fatal("expected scroll damage")
				}
				if scrollDamage.X != 0 || scrollDamage.Y != 0 {
					t.Errorf("scroll position: (%d,%d), want (0,0)", scrollDamage.X, scrollDamage.Y)
				}
				if scrollDamage.Width != 5 || scrollDamage.Height != 3 {
					t.Errorf("scroll size: %dx%d, want 5x3", scrollDamage.Width, scrollDamage.Height)
				}
				if scrollDamage.Count != 1 {
					t.Errorf("scroll count = %d, want 1", scrollDamage.Count)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// Damage coalescing
// ---------------------------------------------------------------------------

func TestDamageCoalescingBehavior(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "adjacent single-cell writes coalesce into one damage entry",
			run: func(t *testing.T) {
				s := NewScreen(20, 3)
				s.Write([]byte("ABC")) // Three adjacent chars on one line

				d := s.Damage()
				// Should be coalesced into a single DamageText.
				if len(d) != 1 {
					t.Fatalf("expected 1 coalesced damage, got %d: %+v", len(d), d)
				}
				if d[0].Kind != renderer.DamageText || d[0].X != 0 || d[0].Y != 0 || d[0].Width != 3 || d[0].Height != 1 {
					t.Errorf("unexpected coalesced damage: %+v", d[0])
				}
			},
		},
		{
			name: "newline breaks coalescing across lines",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("AB\nCD"))

				d := s.Damage()
				// "AB" on line 0, "CD" on line 1 → two separate DamageText items.
				textCount := 0
				for _, dd := range d {
					if dd.Kind == renderer.DamageText {
						textCount++
					}
				}
				if textCount < 2 {
					t.Errorf("expected at least 2 DamageText items (one per line), got %d", textCount)
				}
			},
		},
		{
			name: "writing replaces the initial FullRedraw with text damage",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				// NewScreen already has FullRedraw in damage.
				d := s.Damage()
				if len(d) != 1 || d[0].Kind != renderer.DamageFullRedraw {
					t.Fatalf("expected single FullRedraw, got %+v", d)
				}

				// Writing should replace it with text damage.
				s.Write([]byte("X"))
				d = s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage after write")
				}
				if d[0].Kind == renderer.DamageFullRedraw {
					t.Error("FullRedraw should have been replaced by text damage")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// Resize
// ---------------------------------------------------------------------------

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
			name: "style mouse and cursor modes survive",
			run: func(t *testing.T) {
				s := NewScreen(5, 2)
				s.Write([]byte("\x1b[1m\x1b[?1000h\x1b[?1006h\x1b[?25l\x1b[3 q"))

				s.Resize(6, 3)

				if !s.Style.Bold {
					t.Fatal("bold style was reset")
				}
				if mode, sgr := s.MouseMode(); mode != 1000 || !sgr {
					t.Fatalf("mouse mode = (%d,%v), want (1000,true)", mode, sgr)
				}
				if s.CursorVisible() {
					t.Fatal("cursor visibility was reset")
				}
				if style, ok := s.CursorStyle(); style != 3 || !ok {
					t.Fatalf("cursor style = (%d,%v), want (3,true)", style, ok)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// ClearDamage
// ---------------------------------------------------------------------------

func TestClearDamage(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "ClearDamage empties the damage list"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(5, 3)
			s.Write([]byte("Hello"))
			s.ClearDamage()
			d := s.Damage()
			if len(d) != 0 {
				t.Fatalf("expected empty damage after ClearDamage, got %+v", d)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Non-printable / invalid bytes
// ---------------------------------------------------------------------------

func TestInvalidInputIgnored(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "invalid UTF-8 continuation byte without start byte",
			input: []byte{0x80, 0x81},
		},
		{
			name:  "unhandled control characters below 0x20",
			input: []byte{0x01, 0x02, 0x03, 0x05, 0x06, 0x0b, 0x0c, 0x0e, 0x0f},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 3)
			s.Write(tt.input)
			// Should not panic, and no cells should be modified.
			for y := range 3 {
				for x := range 10 {
					if c := cellAt(s, x, y); c.Rune != ' ' {
						t.Errorf("cell(%d,%d) = %q, want space", x, y, c.Rune)
					}
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// No overflow on edge writes
// ---------------------------------------------------------------------------

func TestWriteAtEdgeNoPanic(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "writing past a 1x1 screen scrolls instead of panicking"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(1, 1)
			// Fill the only cell then try to write more (should scroll).
			s.Write([]byte("ABC"))
			// Should not panic.
			_ = cellAt(s, 0, 0)
		})
	}
}

// ---------------------------------------------------------------------------
// Style persistence across writes
// ---------------------------------------------------------------------------

func TestStylePersists(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "bold persists across separate Write calls"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 3)
			s.Write([]byte("\x1b[1mAB"))
			s.Write([]byte("CD"))

			c1 := cellAt(s, 0, 0)
			c2 := cellAt(s, 2, 0)
			if !c1.Style.Bold || !c2.Style.Bold {
				t.Error("bold should persist across Write calls")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Rune width (CJK, emoji, combining marks, zero-width)
// ---------------------------------------------------------------------------

func TestWideAndZeroWidthRunes(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "CJK writes wide left cell plus continuation",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				// Two CJK characters, each width 2: left cell holds the rune,
				// right cell is a continuation marker.
				s.Write([]byte("你好"))

				assertCell(t, s, 0, 0, '你')
				assertContinuation(t, s, 1, 0)
				assertCell(t, s, 2, 0, '好')
				assertContinuation(t, s, 3, 0)
				if s.Col != 4 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=4 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "CJK mixed with ASCII",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("A你B"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 0, '你')
				assertContinuation(t, s, 2, 0)
				assertCell(t, s, 3, 0, 'B')
				if s.Col != 4 {
					t.Errorf("cursor at col=%d, want 4", s.Col)
				}
			},
		},
		{
			name: "emoji writes wide pair",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("\U0001f600")) // 😀 grinning face

				assertCell(t, s, 0, 0, '\U0001f600')
				assertContinuation(t, s, 1, 0)
				if s.Col != 2 {
					t.Errorf("cursor at col=%d, want 2", s.Col)
				}
			},
		},
		{
			name: "combining mark is skipped without advancing cursor",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				// 'A' followed by combining acute accent U+0301
				s.Write([]byte("A\xcc\x81"))

				assertCell(t, s, 0, 0, 'A')
				// Combining mark should be skipped — no cell written, no cursor advance.
				if s.Col != 1 {
					t.Errorf("cursor at col=%d, want 1 (combining mark should not advance)", s.Col)
				}
			},
		},
		{
			name: "zero-width characters are skipped without advancing cursor",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				// Zero-width space U+200B
				s.Write([]byte("A\xe2\x80\x8bB"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 0, 'B')
				if s.Col != 2 {
					t.Errorf("cursor at col=%d, want 2 (zero-width should not advance)", s.Col)
				}
			},
		},
		{
			name: "CJK surrounded by ASCII on both sides",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("a你b好c"))

				assertCell(t, s, 0, 0, 'a')
				assertCell(t, s, 1, 0, '你')
				assertContinuation(t, s, 2, 0)
				assertCell(t, s, 3, 0, 'b')
				assertCell(t, s, 4, 0, '好')
				assertContinuation(t, s, 5, 0)
				assertCell(t, s, 6, 0, 'c')
				if s.Col != 7 {
					t.Errorf("cursor at col=%d, want 7", s.Col)
				}
			},
		},
		{
			name: "wide char at last column wraps to next line, abandoned cell cleared",
			run: func(t *testing.T) {
				s := NewScreen(3, 2)
				// Fill the first two columns; cursor lands on the last column.
				s.Write([]byte("AB")) // cells [A B _], Col=2
				// A wide char at the last column cannot straddle the edge: it
				// wraps to the next line, clearing the abandoned last cell.
				s.Write([]byte("你"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 0, 'B')
				assertBlank(t, s, 2, 0) // abandoned last cell cleared
				assertCell(t, s, 0, 1, '你')
				assertContinuation(t, s, 1, 1)
				if s.Col != 2 || s.Row != 1 {
					t.Errorf("cursor at col=%d row=%d, want col=2 row=1", s.Col, s.Row)
				}
			},
		},
		{
			name: "CJK damage width is 2",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.ClearDamage()
				s.Write([]byte("你"))

				d := s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage")
				}
				if d[0].Width != 2 {
					t.Errorf("damage width = %d, want 2", d[0].Width)
				}
			},
		},
		{
			name: "mixed ASCII, CJK, emoji, and combining marks keep cursor aligned",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				// ASCII 'a', CJK '你', emoji '😀', ASCII 'b', combining acute.
				s.Write([]byte("a你\U0001f600b\xcc\x81"))

				assertCell(t, s, 0, 0, 'a')
				assertCell(t, s, 1, 0, '你')
				assertContinuation(t, s, 2, 0)
				assertCell(t, s, 3, 0, '\U0001f600')
				assertContinuation(t, s, 4, 0)
				assertCell(t, s, 5, 0, 'b')
				// Combining mark skipped, no cell at col 6.
				if s.Col != 6 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=6 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "overwrite left half of wide pair with narrow clears both",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("你"))         // (0)=你 (1)=cont
				s.Write([]byte("\x1b[1;1H")) // cursor home
				s.Write([]byte("X"))         // overwrite the wide left half

				assertCell(t, s, 0, 0, 'X')
				assertBlank(t, s, 1, 0) // orphaned continuation cleared
			},
		},
		{
			name: "overwrite right half (continuation) with narrow clears both",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("你"))         // (0)=你 (1)=cont
				s.Write([]byte("\x1b[1;2H")) // cursor to col 1 (continuation)
				s.Write([]byte("X"))         // overwrite the continuation

				assertBlank(t, s, 0, 0) // orphaned wide left cleared
				assertCell(t, s, 1, 0, 'X')
			},
		},
		{
			name: "overwrite half of wide pair with a new wide char",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("你好"))        // (0)你 (1)cont (2)好 (3)cont
				s.Write([]byte("\x1b[1;2H")) // cursor to col 1 (cont of 你)
				s.Write([]byte("学"))         // wide write at cols 1,2

				assertBlank(t, s, 0, 0) // 你 left half orphaned → cleared
				assertCell(t, s, 1, 0, '学')
				assertContinuation(t, s, 2, 0)
				assertBlank(t, s, 3, 0) // 好 continuation orphaned → cleared
			},
		},
		{
			name: "erase to end of line covering a continuation clears its wide left half",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("A你B"))       // A(0) 你(1) cont(2) B(3)
				s.Write([]byte("\x1b[1;3H")) // cursor to col 2 (continuation)
				s.Write([]byte("\x1b[K"))    // erase from col 2 to end of line

				assertCell(t, s, 0, 0, 'A')
				assertBlank(t, s, 1, 0) // 你 left half orphaned by erase → cleared
				assertBlank(t, s, 2, 0)
				assertBlank(t, s, 3, 0)
			},
		},
		{
			name: "erase to start of line covering a wide left clears its continuation",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("A你B"))       // A(0) 你(1) cont(2) B(3)
				s.Write([]byte("\x1b[1;2H")) // cursor to col 1 (wide left)
				s.Write([]byte("\x1b[1K"))   // erase from start to col 1 inclusive

				assertBlank(t, s, 0, 0)
				assertBlank(t, s, 1, 0)
				assertBlank(t, s, 2, 0) // continuation orphaned by erase → cleared
				assertCell(t, s, 3, 0, 'B')
			},
		},
		{
			name: "wrap abandoning a continuation cell clears its pair and extends damage",
			run: func(t *testing.T) {
				s := NewScreen(4, 2)
				s.Write([]byte("AB你"))       // A(0) B(1) 你(2) cont(3)
				s.Write([]byte("\x1b[1;4H")) // cursor to col 3: the continuation cell
				s.ClearDamage()
				// A wide rune at the last column wraps; the abandoned last cell
				// is a continuation, so its wide left half (col 2) must be
				// cleared too and the damage must span both columns.
				s.Write([]byte("好"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 0, 'B')
				assertBlank(t, s, 2, 0) // orphaned wide left cleared
				assertBlank(t, s, 3, 0) // abandoned continuation cleared
				assertCell(t, s, 0, 1, '好')
				assertContinuation(t, s, 1, 1)
				if s.Col != 2 || s.Row != 1 {
					t.Errorf("cursor at col=%d row=%d, want col=2 row=1", s.Col, s.Row)
				}

				d := s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage")
				}
				// First damage item covers the cleared pair on row 0.
				if d[0].X != 2 || d[0].Y != 0 || d[0].Width != 2 {
					t.Errorf("wrap damage = {X:%d Y:%d W:%d}, want {X:2 Y:0 W:2}", d[0].X, d[0].Y, d[0].Width)
				}
			},
		},
		{
			name: "insert chars at a continuation splits the pair and repairs orphans",
			run: func(t *testing.T) {
				s := NewScreen(6, 2)
				s.Write([]byte("你好"))        // 你(0) cont(1) 好(2) cont(3)
				s.Write([]byte("\x1b[1;4H")) // cursor to col 3: continuation of 好
				s.ClearDamage()
				s.Write([]byte("\x1b[1@")) // ICH 1: shift right from col 3

				// The shift splits 好/cont: 好 stays at col 2, its continuation
				// moves to col 4. Both orphans must be repaired to blanks.
				assertCell(t, s, 0, 0, '你')
				assertContinuation(t, s, 1, 0)
				assertBlank(t, s, 2, 0) // 好 orphaned by the split → cleared
				assertBlank(t, s, 3, 0) // inserted blank
				assertBlank(t, s, 4, 0) // shifted continuation orphaned → cleared
				assertBlank(t, s, 5, 0)

				d := s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage")
				}
				// Damage must extend one column left to cover the repaired 好.
				if d[0].X != 2 || d[0].Width != 4 {
					t.Errorf("ICH damage = {X:%d W:%d}, want {X:2 W:4}", d[0].X, d[0].Width)
				}
			},
		},
		{
			name: "insert chars shifting a wide pair off the right edge repairs the left half",
			run: func(t *testing.T) {
				s := NewScreen(4, 2)
				s.Write([]byte("你好"))        // 你(0) cont(1) 好(2) cont(3)
				s.Write([]byte("\x1b[1;1H")) // cursor home
				s.Write([]byte("\x1b[1@"))   // ICH 1: shift the row right by 1

				// 好 lands on the last column with its continuation pushed off
				// the edge; the orphaned wide left must be blanked.
				assertBlank(t, s, 0, 0)
				assertCell(t, s, 1, 0, '你')
				assertContinuation(t, s, 2, 0)
				assertBlank(t, s, 3, 0) // 好 lost its continuation → cleared
			},
		},
		{
			name: "delete char at the left half of a wide pair repairs the orphan",
			run: func(t *testing.T) {
				s := NewScreen(6, 2)
				s.Write([]byte("你好AB"))      // 你(0) cont(1) 好(2) cont(3) A(4) B(5)
				s.Write([]byte("\x1b[1;1H")) // cursor to col 0: wide left of 你
				s.ClearDamage()
				s.Write([]byte("\x1b[1P")) // DCH 1

				// 你's continuation shifts to col 0 with no left half → repaired.
				assertBlank(t, s, 0, 0)
				assertCell(t, s, 1, 0, '好')
				assertContinuation(t, s, 2, 0)
				assertCell(t, s, 3, 0, 'A')
				assertCell(t, s, 4, 0, 'B')
				assertBlank(t, s, 5, 0)

				d := s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage")
				}
				if d[0].X != 0 || d[0].Width != 6 {
					t.Errorf("DCH damage = {X:%d W:%d}, want {X:0 W:6}", d[0].X, d[0].Width)
				}
			},
		},
		{
			name: "delete char at a continuation repairs the wide left and extends damage",
			run: func(t *testing.T) {
				s := NewScreen(6, 2)
				s.Write([]byte("你好AB"))      // 你(0) cont(1) 好(2) cont(3) A(4) B(5)
				s.Write([]byte("\x1b[1;2H")) // cursor to col 1: continuation of 你
				s.ClearDamage()
				s.Write([]byte("\x1b[1P")) // DCH 1: delete the continuation

				// 你 at col 0 loses its continuation → repaired to blank.
				assertBlank(t, s, 0, 0)
				assertCell(t, s, 1, 0, '好')
				assertContinuation(t, s, 2, 0)
				assertCell(t, s, 3, 0, 'A')
				assertCell(t, s, 4, 0, 'B')
				assertBlank(t, s, 5, 0)

				d := s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage")
				}
				// Damage must extend one column left (to col 0) to cover the
				// repaired wide left half.
				if d[0].X != 0 || d[0].Width != 6 {
					t.Errorf("DCH damage = {X:%d W:%d}, want {X:0 W:6}", d[0].X, d[0].Width)
				}
			},
		},
		{
			name: "wide char on a width-1 screen degrades to a single cell",
			run: func(t *testing.T) {
				s := NewScreen(1, 2)
				// A wide rune cannot fit on a 1-column screen; it must not write
				// an out-of-bounds continuation cell.
				s.Write([]byte("你"))

				assertCell(t, s, 0, 0, '你')
				if s.Col != 1 {
					t.Errorf("cursor at col=%d, want 1", s.Col)
				}
			},
		},
		{
			name: "wide pair survives a scroll",
			run: func(t *testing.T) {
				s := NewScreen(4, 2)
				s.Write([]byte("你\r\n好")) // row0: 你, row1: 好
				s.Write([]byte("\r\n"))   // bottom row → scroll up

				// After scrolling up, row1's 好 moves to row0 intact.
				assertCell(t, s, 0, 0, '好')
				assertContinuation(t, s, 1, 0)
			},
		},
		{
			name: "combining mark alone writes no cell and produces no damage",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.ClearDamage()
				s.Write([]byte("\xcc\x81")) // combining acute accent alone (no base char)

				// No cells should be modified.
				for y := range 3 {
					for x := range 10 {
						if c := cellAt(s, x, y); c.Rune != ' ' {
							t.Errorf("cell(%d,%d) = %q, want space", x, y, c.Rune)
						}
					}
				}
				if s.Col != 0 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=0 row=0", s.Col, s.Row)
				}
				d := s.Damage()
				if len(d) != 0 {
					t.Errorf("expected no damage for combining mark alone, got %+v", d)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestRuneWidth(t *testing.T) {
	tests := []struct {
		name  string
		r     rune
		width int
	}{
		{"space", ' ', 1},
		{"lowercase ascii letter", 'a', 1},
		{"uppercase ascii letter", 'A', 1},
		{"ascii digit", '1', 1},
		{"soft hyphen", 0x00AD, 0},
		{"combining grapheme joiner", 0x034F, 0},
		{"combining acute accent", 0x0301, 0},
		{"combining grave accent", 0x0300, 0},
		{"zero-width space", 0x200B, 0},
		{"zero-width non-joiner", 0x200C, 0},
		{"zero-width joiner", 0x200D, 0},
		{"BOM / zero-width no-break space", 0xFEFF, 0},
		{"CJK Unified Ideograph (一)", 0x4E00, 2},
		{"CJK (二)", 0x4E8C, 2},
		{"CJK end of BMP", 0x9FFF, 2},
		{"CJK Extension A", 0x3400, 2},
		{"Hangul Syllable (가)", 0xAC00, 2},
		{"Hangul Syllable end", 0xD7AF, 2},
		{"CJK Ideographic Space", 0x3000, 2},
		{"Fullwidth Exclamation Mark", 0xFF01, 2},
		{"Fullwidth Cent Sign", 0xFFE0, 2},
		{"grinning face emoji", 0x1F600, 2},
		{"rocket emoji", 0x1F680, 2},
		{"hiragana a", 0x3042, 2},
		{"katakana a", 0x30A2, 2},
		{"CJK Extension B", 0x20000, 2},
		{"supplemental symbols (robot)", 0x1F916, 2},
		{"symbols extended-A (sari)", 0x1FA71, 2},
		{"ideographic full stop", 0x3002, 2},
		{"NUL", '\x00', 0},
		{"SOH", '\x01', 0},
		{"ESC", '\x1b', 0},
		{"DEL", 0x7F, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := renderer.RuneWidth(tt.r)
			if got != tt.width {
				t.Errorf("renderer.RuneWidth(%U %q) = %d, want %d", tt.r, tt.r, got, tt.width)
			}
		})
	}
}

func TestDamageReturnsReference(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "Damage() returns a reference to the internal slice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Damage() should return a reference to internal slice (no copy).
			s := NewScreen(10, 3)
			s.Write([]byte("X"))
			d1 := s.Damage()
			// Internal slice should be the same (or at least reference the same backing).
			if len(d1) == 0 {
				t.Fatal("expected damage")
			}
			// Write more to trigger record.
			s.Write([]byte("Y"))
			d2 := s.Damage()
			_ = d2
			// Both d1 and d2 reference the internal slice; we just don't crash.
		})
	}
}

// ---------------------------------------------------------------------------
// CSI S / SU — Scroll Up
// ---------------------------------------------------------------------------

func TestCSIScrollUp(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "CSI S with no param scrolls up by 1 and preserves cursor",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("AAAAABBBBBCCCCCDDDDD")) // fill all 4 rows
				s.ClearDamage()

				// CSI S with no param → scroll up by 1.
				s.Write([]byte("\x1b[S"))

				// Content should have shifted up: row 0 = BBBBB, row 1 = CCCCC, row 2 = DDDDD, row 3 = blank.
				assertCell(t, s, 0, 0, 'B')
				assertCell(t, s, 0, 1, 'C')
				assertCell(t, s, 0, 2, 'D')
				assertCell(t, s, 0, 3, ' ')

				// Cursor should NOT have moved.
				if s.Col != 5 || s.Row != 3 {
					t.Errorf("cursor at col=%d row=%d, want col=5 row=3", s.Col, s.Row)
				}

				d := s.Damage()
				if !hasDamageKind(d, renderer.DamageScrollUp) {
					t.Errorf("expected DamageScrollUp, got %v", damageKinds(d))
				}
			},
		},
		{
			name: "CSI 2S scrolls up by an explicit count",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("AAAAABBBBBCCCCCDDDDD")) // fill all 4 rows
				s.ClearDamage()

				// CSI 2 S → scroll up by 2.
				s.Write([]byte("\x1b[2S"))

				// Content: row 0 = CCCCC, row 1 = DDDDD, row 2 = blank, row 3 = blank.
				assertCell(t, s, 0, 0, 'C')
				assertCell(t, s, 0, 1, 'D')
				assertCell(t, s, 0, 2, ' ')
				assertCell(t, s, 0, 3, ' ')

				// Cursor preserved.
				if s.Col != 5 || s.Row != 3 {
					t.Errorf("cursor at col=%d row=%d, want col=5 row=3", s.Col, s.Row)
				}
			},
		},
		{
			name: "CSI 100S on a 3-row screen clamps to the full screen",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("AAAAABBBBBCCCCC")) // fill 3 rows
				s.ClearDamage()

				// CSI 100 S on a 3-row screen → clamp to 3 (entire screen blank).
				s.Write([]byte("\x1b[100S"))

				// All rows should be blank.
				for y := range 3 {
					for x := range 5 {
						assertCell(t, s, x, y, ' ')
					}
				}
			},
		},
		{
			name: "CSI 0S defaults to a scroll count of 1",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("AAAAABBBBBCCCCC")) // fill 3 rows
				s.ClearDamage()

				// CSI 0 S → default to 1 (parameter 0 means default in VT spec).
				s.Write([]byte("\x1b[0S"))

				// Row 0 should have shifted up: row 0 = BBBBB, row 1 = CCCCC, row 2 = blank.
				assertCell(t, s, 0, 0, 'B')
				assertCell(t, s, 0, 1, 'C')
				assertCell(t, s, 0, 2, ' ')
			},
		},
		{
			name: "CSI 3S reports scroll count 3 and blanked-row text damage",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("AAAAABBBBBCCCCCDDDDD")) // fill all 4 rows
				s.ClearDamage()

				s.Write([]byte("\x1b[3S")) // scroll up by 3

				d := s.Damage()
				var scrollDamage *renderer.Damage
				for i, dd := range d {
					if dd.Kind == renderer.DamageScrollUp {
						scrollDamage = &d[i]
						break
					}
				}
				if scrollDamage == nil {
					t.Fatal("expected DamageScrollUp")
				}
				if scrollDamage.Count != 3 {
					t.Errorf("scroll count = %d, want 3", scrollDamage.Count)
				}
				if scrollDamage.Width != 5 || scrollDamage.Height != 4 {
					t.Errorf("scroll size = %dx%d, want 5x4", scrollDamage.Width, scrollDamage.Height)
				}

				// Should also have text damage for the blanked rows (bottom 3 rows).
				if !hasDamageKind(d, renderer.DamageText) {
					t.Error("expected DamageText for blanked rows")
				}
			},
		},
		{
			name: "cursor position is preserved across a scroll-up",
			run: func(t *testing.T) {
				s := NewScreen(10, 5)
				s.Write([]byte("AAAAA\nBBBBB\nCCCCC\nDDDDD\nEEEEE")) // fill all 5 rows
				s.ClearDamage()

				// Move cursor to middle of row 2.
				s.Write([]byte("\x1b[3;3H")) // row=3, col=3
				s.Write([]byte("\x1b[2S"))   // scroll up by 2

				// Cursor should still be at 1-indexed (3,3) = 0-indexed (2,2).
				if s.Row != 2 || s.Col != 2 {
					t.Errorf("cursor at row=%d col=%d, want row=2 col=2", s.Row, s.Col)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// OSC sequences
// ---------------------------------------------------------------------------

func TestOSCSequences(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "BEL-terminated OSC is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]0;~/Projects/ymux - fish\x07fish$ "))

				assertCell(t, s, 0, 0, 'f')
				assertCell(t, s, 1, 0, 'i')
				assertCell(t, s, 2, 0, 's')
				assertCell(t, s, 3, 0, 'h')
				assertCell(t, s, 4, 0, '$')
				assertCell(t, s, 5, 0, ' ')
				if s.Col != 6 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=6 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "ST-terminated OSC is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]133;A;click_events=1\x1b\\> "))

				assertCell(t, s, 0, 0, '>')
				assertCell(t, s, 1, 0, ' ')
				if s.Col != 2 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=2 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "multiple OSC sequences in a row are all ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]7;file://host/tmp\x07\x1b]11;?\\=\x07\x1b]133;B\x1b\\prompt"))

				assertCell(t, s, 0, 0, 'p')
				assertCell(t, s, 1, 0, 'r')
				assertCell(t, s, 2, 0, 'o')
				assertCell(t, s, 3, 0, 'm')
				assertCell(t, s, 4, 0, 'p')
				assertCell(t, s, 5, 0, 't')
				if s.Col != 6 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=6 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "BEL-terminated OSC split across writes is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]0;~/Projects/ymux"))
				s.Write([]byte(" - fish\x07fish$ "))

				assertCell(t, s, 0, 0, 'f')
				assertCell(t, s, 1, 0, 'i')
				assertCell(t, s, 2, 0, 's')
				assertCell(t, s, 3, 0, 'h')
				assertCell(t, s, 4, 0, '$')
				assertCell(t, s, 5, 0, ' ')
				if s.Col != 6 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=6 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "ST-terminated OSC split across writes is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]133;A;click_events=1\x1b"))
				s.Write([]byte("\\> "))

				assertCell(t, s, 0, 0, '>')
				assertCell(t, s, 1, 0, ' ')
				if s.Col != 2 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=2 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "unterminated OSC sequence is bounded and dropped",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				payload := strings.Repeat("x", maxEscapeBufferLen+1)
				s.Write([]byte("\x1b]0;" + payload))
				s.Write([]byte("OK"))

				assertCell(t, s, 0, 0, 'O')
				assertCell(t, s, 1, 0, 'K')
				if len(s.escapeBuf) != 0 {
					t.Fatalf("escapeBuf length = %d, want 0 after overlong unterminated OSC", len(s.escapeBuf))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// C1 Unicode controls (U+0080-U+009F)
// ---------------------------------------------------------------------------

func TestC1Controls(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "C1 controls are dropped without moving the cursor",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte{0xC2, 0x80}) // U+0080 PAD
				s.Write([]byte{0xC2, 0x9F}) // U+009F APC

				// No cells should be modified.
				for y := range 3 {
					for x := range 10 {
						if c := cellAt(s, x, y); c.Rune != ' ' {
							t.Errorf("cell(%d,%d) = %q, want space after C1 controls", x, y, c.Rune)
						}
					}
				}
				// Cursor should not have moved.
				if s.Col != 0 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=0 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "the full C1 control range U+0080-U+009F is dropped",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				// Send all C1 control runes U+0080 through U+009F.
				for r := 0x80; r <= 0x9F; r++ {
					s.Write([]byte{byte(0xC0 | r>>6), byte(0x80 | r&0x3F)})
				}

				// No cells should be modified.
				for y := range 3 {
					for x := range 10 {
						if c := cellAt(s, x, y); c.Rune != ' ' {
							t.Errorf("cell(%d,%d) = %q, want space after C1 range", x, y, c.Rune)
						}
					}
				}
				if s.Col != 0 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=0 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "printable characters still work after C1 controls",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				// Printable characters should still work after C1 controls.
				s.Write([]byte{0xC2, 0x80}) // U+0080 (dropped)
				s.Write([]byte("AB"))
				s.Write([]byte{0xC2, 0x9F}) // U+009F (dropped)
				s.Write([]byte("CD"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 0, 'B')
				assertCell(t, s, 2, 0, 'C')
				assertCell(t, s, 3, 0, 'D')
				if s.Col != 4 {
					t.Errorf("cursor at col=%d, want 4", s.Col)
				}
			},
		},
		{
			name: "C1 controls mixed with CR preserve CR behavior",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				// C1 controls mixed with CR/LF should preserve CR/LF behavior.
				s.Write([]byte{0xC2, 0x80}) // U+0080 (dropped)
				s.Write([]byte("A\rB"))     // CR moves cursor back, overwrite A
				s.Write([]byte{0xC2, 0x9F}) // U+009F (dropped)

				assertCell(t, s, 0, 0, 'B')
			},
		},
		{
			name: "C1 controls mixed with LF preserve LF behavior",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte{0xC2, 0x80}) // U+0080 (dropped)
				s.Write([]byte("A\nB"))

				assertCell(t, s, 0, 0, 'A')
				assertCell(t, s, 1, 1, 'B')
				if s.Row != 1 || s.Col != 2 {
					t.Errorf("cursor at row=%d col=%d, want row=1 col=2 (LF advances row without CR)", s.Row, s.Col)
				}
			},
		},
		{
			name: "C1 controls produce no damage",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.ClearDamage()
				// C1 controls should produce no damage.
				s.Write([]byte{0xC2, 0x80, 0xC2, 0x81, 0xC2, 0x9F})

				d := s.Damage()
				if len(d) != 0 {
					t.Errorf("expected no damage from C1 controls, got %+v", d)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// Line-offset rotation scroll (M1c)
// ---------------------------------------------------------------------------

// TestWideCharSurvivesRotatedScroll proves a wide-character pair travels intact
// with its line when a full-width scroll rotates line offsets rather than
// copying cells.
func TestWideCharSurvivesRotatedScroll(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "wide pair moves up one row on newline scroll",
			run: func(t *testing.T) {
				s := NewScreen(4, 3)
				// Place a wide rune (世, width 2) on the bottom row, then scroll up.
				s.Write([]byte("\x1b[3;1H世"))
				assertCell(t, s, 0, 2, '世')
				assertContinuation(t, s, 1, 2)
				// Newline at the bottom row scrolls the region up by one.
				s.Write([]byte("\n"))
				// The wide pair is now on row 1, intact; bottom row blank.
				assertCell(t, s, 0, 1, '世')
				assertContinuation(t, s, 1, 1)
				assertBlank(t, s, 0, 2)
				assertBlank(t, s, 1, 2)
				if err := s.Frame.CheckInvariants(); err != nil {
					t.Fatalf("invariants: %v", err)
				}
			},
		},
		{
			name: "wide pair moves down on reverse index",
			run: func(t *testing.T) {
				s := NewScreen(4, 3)
				s.Write([]byte("\x1b[1;1H世")) // top row
				assertCell(t, s, 0, 0, '世')
				assertContinuation(t, s, 1, 0)
				s.Write([]byte("\x1b[1;1H\x1bM")) // cursor top, reverse index -> scroll down
				assertCell(t, s, 0, 1, '世')
				assertContinuation(t, s, 1, 1)
				assertBlank(t, s, 0, 0)
				if err := s.Frame.CheckInvariants(); err != nil {
					t.Fatalf("invariants: %v", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// TestLineOffsetInvariantAfterVTSequences drives interleaved writes and scrolls
// through the VT layer and asserts the frame's line-offset stays a valid
// permutation.
func TestLineOffsetInvariantAfterVTSequences(t *testing.T) {
	s := NewScreen(10, 6)
	seqs := [][]byte{
		[]byte("line0\r\nline1\r\nline2\r\nline3\r\nline4\r\nline5"),
		[]byte("\x1b[2;5r"),       // scroll region rows 2..5
		[]byte("\x1b[5;1Hmore\n"), // scroll within region
		[]byte("\x1b[1;1H\x1bM"),  // reverse index at top
		[]byte("\x1b[3S"),         // scroll up 3
		[]byte("\x1b[2T"),         // scroll down 2
		[]byte("\x1b[r"),          // reset region
		[]byte("\x1b[6;1Hbottom\n"),
	}
	for i, seq := range seqs {
		s.Write(seq)
		s.ClearDamage()
		if err := s.Frame.CheckInvariants(); err != nil {
			t.Fatalf("after seq %d: invariants broken: %v", i, err)
		}
	}
}

func TestScreenReportsSynchronizedUpdateMode(t *testing.T) {
	s := NewScreen(10, 2)
	s.Write([]byte("\x1b[?2026h"))
	if !s.SyncUpdateActive() {
		t.Fatal("sync update mode should be active")
	}
	s.Write([]byte("\x1b[?2026l"))
	if s.SyncUpdateActive() {
		t.Fatal("sync update mode should be inactive")
	}
}

func TestForceSyncEnd(t *testing.T) {
	s := NewScreen(10, 2)
	s.Write([]byte("\x1b[?2026h"))
	if !s.SyncUpdateActive() {
		t.Fatal("sync update mode should be active before forcing end")
	}
	s.ForceSyncEnd()
	if s.SyncUpdateActive() {
		t.Fatal("sync update mode should be inactive after forcing end")
	}
}

func TestScreenCursorAndMouseStateAccessors(t *testing.T) {
	tests := []struct {
		name  string
		seq   string
		check func(t *testing.T, s *Screen)
	}{
		{
			name: "cursor position and visibility are exposed",
			seq:  "\x1b[2;3H\x1b[?25l",
			check: func(t *testing.T, s *Screen) {
				if s.CursorRow() != 1 || s.CursorCol() != 2 {
					t.Fatalf("cursor = row %d col %d, want row 1 col 2", s.CursorRow(), s.CursorCol())
				}
				if s.CursorVisible() {
					t.Fatal("cursor should be hidden")
				}
				s.Write([]byte("\x1b[?25h"))
				if !s.CursorVisible() {
					t.Fatal("cursor should be visible")
				}
			},
		},
		{
			name: "mouse mode and SGR are exposed",
			seq:  "\x1b[?1002h\x1b[?1006h",
			check: func(t *testing.T, s *Screen) {
				mode, sgr := s.MouseMode()
				if mode != 1002 || !sgr {
					t.Fatalf("MouseMode() = (%d, %v), want (1002, true)", mode, sgr)
				}
			},
		},
		{
			name: "reset restores cursor and mouse defaults",
			seq:  "\x1b[?25l\x1b[?1003h\x1b[?1006h\x1bc",
			check: func(t *testing.T, s *Screen) {
				mode, sgr := s.MouseMode()
				if !s.CursorVisible() || mode != 0 || sgr {
					t.Fatalf("after reset CursorVisible=%v MouseMode=(%d,%v), want true,(0,false)", s.CursorVisible(), mode, sgr)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 4)
			s.Write([]byte(tt.seq))
			tt.check(t, s)
		})
	}
}

func TestScreenMouseModeDisableInactiveIsNoOp(t *testing.T) {
	s := NewScreen(10, 2)
	s.Write([]byte("\x1b[?1002h"))
	s.Write([]byte("\x1b[?1000l"))
	mode, _ := s.MouseMode()
	if mode != 1002 {
		t.Fatalf("mouse mode after disabling inactive 1000 = %d, want 1002", mode)
	}
	s.Write([]byte("\x1b[?1002l"))
	mode, _ = s.MouseMode()
	if mode != 0 {
		t.Fatalf("mouse mode after disabling active 1002 = %d, want 0", mode)
	}
}

func TestScreenCursorStyleDECSCUSR(t *testing.T) {
	tests := []struct {
		name      string
		seq       string
		wantStyle int
		wantSet   bool
	}{
		{name: "explicit style", seq: "\x1b[5 q", wantStyle: 5, wantSet: true},
		{name: "blank style parameter", seq: "\x1b[ q", wantStyle: 0, wantSet: true},
		{name: "invalid low style is ignored", seq: "\x1b[-1 q", wantStyle: 0, wantSet: false},
		{name: "invalid high style is ignored", seq: "\x1b[7 q", wantStyle: 0, wantSet: false},
		{name: "XTVERSION is ignored", seq: "\x1b[>0q", wantStyle: 0, wantSet: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 2)
			s.Write([]byte(tt.seq))
			style, set := s.CursorStyle()
			if style != tt.wantStyle || set != tt.wantSet {
				t.Fatalf("CursorStyle() = (%d, %v), want (%d, %v)", style, set, tt.wantStyle, tt.wantSet)
			}
		})
	}
}

func TestScreenInvalidCursorStyleDoesNotOverwriteCurrentStyle(t *testing.T) {
	s := NewScreen(10, 2)
	s.Write([]byte("\x1b[5 q"))
	s.Write([]byte("\x1b[99 q"))
	style, set := s.CursorStyle()
	if style != 5 || !set {
		t.Fatalf("CursorStyle() = (%d, %v), want (5, true)", style, set)
	}
}

func TestScreenAltScreenActiveAccessor(t *testing.T) {
	s := NewScreen(10, 2)
	if s.AltScreenActive() {
		t.Fatal("alt screen should start inactive")
	}
	s.Write([]byte("\x1b[?1049h"))
	if !s.AltScreenActive() {
		t.Fatal("alt screen should be active")
	}
	s.Write([]byte("\x1b[?1049l"))
	if s.AltScreenActive() {
		t.Fatal("alt screen should be inactive after exit")
	}
}
