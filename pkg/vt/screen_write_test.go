package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

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
			name: "tab moves cursor without changing cells or damage",
			run: func(t *testing.T) {
				s := NewScreen(10, 2)
				for y := range s.Frame.Height {
					for x := range s.Frame.Width {
						s.Frame.Set(x, y, renderer.Cell{Rune: rune('a' + y*s.Frame.Width + x)})
					}
				}
				before := s.Frame.Clone()
				s.Row, s.Col = 1, 6
				s.ClearDamage()

				s.Write([]byte("\t"))
				if s.Row != 1 || s.Col != 8 {
					t.Errorf("cursor after tab = row=%d col=%d, want row=1 col=8", s.Row, s.Col)
				}
				assertFramesEqual(t, s.Frame, before)
				if got := s.Damage(); len(got) != 0 {
					t.Errorf("tab damage = %+v, want none", got)
				}

				// A tab whose next stop lies past the edge clamps at the last
				// column; it must not wrap or scroll the bottom row.
				s.Write([]byte("\t"))
				if s.Row != 1 || s.Col != 9 {
					t.Errorf("cursor after clamped tab = row=%d col=%d, want row=1 col=9", s.Row, s.Col)
				}
				assertFramesEqual(t, s.Frame, before)
				if got := s.Damage(); len(got) != 0 {
					t.Errorf("clamped tab damage = %+v, want none", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

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
			name: "IRM inserts narrow printable cells and reset restores overwrite",
			run: func(t *testing.T) {
				s := NewScreen(8, 1)
				s.Write([]byte("abcd\x1b[1;3H\x1b[4hX"))

				if got := lineText(s, 0); got != "abXcd   " {
					t.Fatalf("IRM inserted line = %q, want %q", got, "abXcd   ")
				}
				s.Write([]byte("\x1b[4l\x1b[1;3HY"))
				if got := lineText(s, 0); got != "abYcd   " {
					t.Fatalf("IRM reset overwrite line = %q, want %q", got, "abYcd   ")
				}
			},
		},
		{
			name: "IRM inserts wide printable cells and repairs evicted wide head",
			run: func(t *testing.T) {
				s := NewScreen(6, 1)
				s.Write([]byte("ab界cd"))
				s.Write([]byte("\x1b[1;3H\x1b[4h好"))

				assertCell(t, s, 0, 0, 'a')
				assertCell(t, s, 1, 0, 'b')
				assertCell(t, s, 2, 0, '好')
				assertContinuation(t, s, 3, 0)
				assertCell(t, s, 4, 0, '界')
				assertContinuation(t, s, 5, 0)
				assertNoOrphanWideCells(t, s)
			},
		},
		{
			name: "IRM wide insert at right edge repairs orphan",
			run: func(t *testing.T) {
				s := NewScreen(5, 1)
				s.Write([]byte("abc界"))
				s.Write([]byte("\x1b[1;2H\x1b[4h好"))

				assertCell(t, s, 0, 0, 'a')
				assertCell(t, s, 1, 0, '好')
				assertContinuation(t, s, 2, 0)
				assertCell(t, s, 3, 0, 'b')
				assertCell(t, s, 4, 0, 'c')
				assertNoOrphanWideCells(t, s)
			},
		},
		{
			name: "wide char on a width-1 screen becomes a narrow replacement rune",
			run: func(t *testing.T) {
				s := NewScreen(1, 2)
				// A wide rune cannot fit on a 1-column screen. Store a narrow
				// replacement instead so renderer width matches the cell layout.
				s.Write([]byte("你"))

				assertCell(t, s, 0, 0, '\uFFFD')
				if got := renderer.RuneWidth(cellAt(s, 0, 0).Rune); got != 1 {
					t.Errorf("stored rune width = %d, want 1", got)
				}
				if cellAt(s, 0, 0).Continuation {
					t.Error("width-1 replacement must not be a continuation")
				}
				assertNoOrphanWideCells(t, s)
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

func TestScreenLineBoundsDescribesTheLiveGrid(t *testing.T) {
	tests := []struct {
		name   string
		width  int
		height int
		write  string
		want   []LineBound
		// checkOwned exercises the caller-owned-copy guarantee: it only makes
		// sense to check once per LineBounds() call, so it lives on the case
		// whose write actually produces a soft-wrapped row 0 to corrupt.
		checkOwned bool
	}{
		{
			name:   "wraps after column width",
			width:  4,
			height: 3,
			write:  "abcdef", // wraps after column 4
			want: []LineBound{
				{End: 4, Soft: true},  // row 0: filled the row and wrapped
				{End: 2, Soft: false}, // row 1: "ef", no wrap
				{End: 0, Soft: false}, // row 2: untouched
			},
			checkOwned: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(tt.width, tt.height)
			s.Write([]byte(tt.write))

			bounds := s.LineBounds()
			require.Equal(t, tt.want, bounds)

			if tt.checkOwned {
				// The result is owned by the caller.
				bounds[0] = LineBound{}
				require.True(t, s.LineBounds()[0].Soft, "mutating the result changed the screen's own boundaries")
			}
		})
	}
}
