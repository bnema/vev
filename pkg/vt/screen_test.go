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

func TestWritePrintable(t *testing.T) {
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
}

func TestWriteUTF8(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("aäéø©"))

	assertCell(t, s, 0, 0, 'a')
	assertCell(t, s, 1, 0, 'ä')
	assertCell(t, s, 2, 0, 'é')
	assertCell(t, s, 3, 0, 'ø')
	assertCell(t, s, 4, 0, '©')
}

func TestWritePrintableBeyondWidthWraps(t *testing.T) {
	s := NewScreen(4, 2)
	s.Write([]byte("ABCDE"))

	assertCell(t, s, 0, 0, 'A')
	assertCell(t, s, 1, 0, 'B')
	assertCell(t, s, 2, 0, 'C')
	assertCell(t, s, 3, 0, 'D')
	// E wrapped to next line.
	assertCell(t, s, 0, 1, 'E')
}

func TestWritePrintableBeyondScreenScrolls(t *testing.T) {
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
}

// ---------------------------------------------------------------------------
// Carriage Return
// ---------------------------------------------------------------------------

func TestCR(t *testing.T) {
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
}

// ---------------------------------------------------------------------------
// Line Feed
// ---------------------------------------------------------------------------

func TestLF(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("A\r\nB\r\nC"))

	assertCell(t, s, 0, 0, 'A')
	assertCell(t, s, 0, 1, 'B')
	assertCell(t, s, 0, 2, 'C')
	if s.Row != 2 || s.Col != 1 {
		t.Errorf("cursor at row=%d col=%d, want row=2 col=1", s.Row, s.Col)
	}
}

func TestLFDoesNotResetColumn(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("AB\nC"))

	assertCell(t, s, 0, 0, 'A')
	assertCell(t, s, 1, 0, 'B')
	assertCell(t, s, 0, 1, ' ')
	assertCell(t, s, 2, 1, 'C')
	if s.Row != 1 || s.Col != 3 {
		t.Errorf("cursor at row=%d col=%d, want row=1 col=3", s.Row, s.Col)
	}
}

func TestLFAtPendingWrapBoundaryAdvancesOneLine(t *testing.T) {
	s := NewScreen(5, 4)
	s.Write([]byte("ABCDE\nZ"))

	assertCell(t, s, 4, 0, 'E')
	assertCell(t, s, 4, 1, 'Z')
	if s.Row != 1 || s.Col != 5 {
		t.Errorf("cursor at row=%d col=%d, want row=1 col=5", s.Row, s.Col)
	}
}

func TestLFScroll(t *testing.T) {
	s := NewScreen(5, 2)
	// Fill both lines then CRLF to scroll and return to column 0.
	s.Write([]byte("AAAAA"))
	s.Write([]byte("BBBBB"))
	s.Write([]byte("\r\nCCCCC"))

	assertCell(t, s, 0, 0, 'B')
	assertCell(t, s, 4, 1, 'C')
}

// ---------------------------------------------------------------------------
// Backspace
// ---------------------------------------------------------------------------

func TestBS(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("Hello\b"))

	if s.Col != 4 {
		t.Errorf("col after BS = %d, want 4", s.Col)
	}
	assertCell(t, s, 4, 0, 'o') // cell unchanged, just cursor moved
}

func TestBSAtColumnZero(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\b")) // BS at col 0 should be no-op

	if s.Col != 0 {
		t.Errorf("col = %d, want 0", s.Col)
	}
}

// ---------------------------------------------------------------------------
// Tab
// ---------------------------------------------------------------------------

func TestTab(t *testing.T) {
	s := NewScreen(20, 3)
	s.Write([]byte("A\tB"))

	assertCell(t, s, 0, 0, 'A')
	// Tab advances to next 8-column boundary (col 8).
	for i := 1; i < 8; i++ {
		assertCell(t, s, i, 0, ' ')
	}
	assertCell(t, s, 8, 0, 'B')
}

// ---------------------------------------------------------------------------
// CSI SGR
// ---------------------------------------------------------------------------

func TestSGRDefault(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[0m"))

	if s.Style != (renderer.DefaultStyle()) {
		t.Errorf("style after SGR 0 = %+v, want default", s.Style)
	}
}

func TestSGRBold(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[1m"))

	if !s.Style.Bold {
		t.Error("expected bold")
	}
}

func TestSGRInverse(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[7m"))

	if !s.Style.Inverse {
		t.Error("expected inverse")
	}
}

func TestSGRForeground(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[31m")) // red

	if s.Style.Foreground != 1 {
		t.Errorf("foreground = %d, want 1 (red)", s.Style.Foreground)
	}
}

func TestSGRBackground(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[44m")) // blue background

	if s.Style.Background != 4 {
		t.Errorf("background = %d, want 4 (blue)", s.Style.Background)
	}
}

func TestSGR256Color(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[38;5;82m")) // 256-color foreground

	if s.Style.Foreground != 82 {
		t.Errorf("foreground = %d, want 82", s.Style.Foreground)
	}
}

func TestSGR256Background(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[48;5;200m")) // 256-color background

	if s.Style.Background != 200 {
		t.Errorf("background = %d, want 200", s.Style.Background)
	}
}

func TestSGRBrightForeground(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[91m")) // bright red

	if s.Style.Foreground != 9 {
		t.Errorf("foreground = %d, want 9 (bright red)", s.Style.Foreground)
	}
}

func TestSGRBrightBackground(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[107m")) // bright white background

	if s.Style.Background != 15 {
		t.Errorf("background = %d, want 15 (bright white)", s.Style.Background)
	}
}

func TestSGRTruecolorForeground(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[38;2;12;34;56mX"))

	want := renderer.RGB{R: 12, G: 34, B: 56}
	if !s.Style.HasForegroundRGB || s.Style.ForegroundRGB != want {
		t.Errorf("foreground RGB = (%v, %+v), want true/%+v", s.Style.HasForegroundRGB, s.Style.ForegroundRGB, want)
	}
	if got := cellAt(s, 0, 0).Style.ForegroundRGB; got != want {
		t.Errorf("cell foreground RGB = %+v, want %+v", got, want)
	}
}

func TestSGRTruecolorBackground(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[48;2;200;100;50m"))

	want := renderer.RGB{R: 200, G: 100, B: 50}
	if !s.Style.HasBackgroundRGB || s.Style.BackgroundRGB != want {
		t.Errorf("background RGB = (%v, %+v), want true/%+v", s.Style.HasBackgroundRGB, s.Style.BackgroundRGB, want)
	}
}

func TestSGRDefaultForegroundClearsTruecolor(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[38;2;12;34;56m\x1b[39m"))

	if s.Style.HasForegroundRGB || s.Style.Foreground != -1 {
		t.Errorf("foreground after reset = rgb:%v index:%d, want default", s.Style.HasForegroundRGB, s.Style.Foreground)
	}
}

func TestSGRTruecolorClampsComponents(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[38;2;-1;300;42m"))

	want := renderer.RGB{R: 0, G: 255, B: 42}
	if s.Style.ForegroundRGB != want {
		t.Errorf("foreground RGB = %+v, want %+v", s.Style.ForegroundRGB, want)
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

func TestSGRResetBold(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[1mHello\x1b[22mWorld"))

	if s.Style.Bold {
		t.Error("bold should be reset after 22")
	}
}

func TestSGRResetInverse(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[7m\x1b[27m"))

	if s.Style.Inverse {
		t.Error("inverse should be reset after 27")
	}
}

func TestSGRDefaultForeground(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[31m\x1b[39m"))

	if s.Style.Foreground != -1 {
		t.Errorf("foreground should be default after 39, got %d", s.Style.Foreground)
	}
}

func TestSGRDefaultBackground(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[44m\x1b[49m"))

	if s.Style.Background != -1 {
		t.Errorf("background should be default after 49, got %d", s.Style.Background)
	}
}

func TestSGRAppliesToCells(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[1;31mX"))

	c := cellAt(s, 0, 0)
	if !c.Style.Bold {
		t.Error("cell should be bold")
	}
	if c.Style.Foreground != 1 {
		t.Errorf("cell foreground = %d, want 1", c.Style.Foreground)
	}
}

// ---------------------------------------------------------------------------
// Clear screen
// ---------------------------------------------------------------------------

func TestClearScreen(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("HelloWorldABCDE"))
	// Clear damage from writing.
	s.ClearDamage()

	s.Write([]byte("\x1b[2J"))

	// All cells should be blank.
	for y := 0; y < 3; y++ {
		for x := 0; x < 5; x++ {
			if c := cellAt(s, x, y); c.Rune != ' ' {
				t.Errorf("cell(%d,%d) = %q, want space after clear", x, y, c.Rune)
			}
		}
	}

	d := s.Damage()
	if !hasDamageKind(d, renderer.DamageClear) {
		t.Error("expected DamageClear after CSI 2 J")
	}
}

// ---------------------------------------------------------------------------
// Clear line
// ---------------------------------------------------------------------------

func TestClearLine(t *testing.T) {
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
}

// ---------------------------------------------------------------------------
// Cursor move
// ---------------------------------------------------------------------------

func TestCursorMoveH(t *testing.T) {
	s := NewScreen(10, 5)
	s.Write([]byte("\x1b[3;4H"))

	if s.Row != 2 || s.Col != 3 {
		t.Errorf("cursor at row=%d col=%d, want row=2 col=3", s.Row, s.Col)
	}
}

func TestCursorMoveOutsideBounds(t *testing.T) {
	s := NewScreen(10, 5)
	s.Write([]byte("\x1b[100;200H"))

	// Should clamp to bottom-right.
	if s.Row != 4 || s.Col != 9 {
		t.Errorf("cursor at row=%d col=%d, want row=4 col=9", s.Row, s.Col)
	}
}

func TestCursorMoveF(t *testing.T) {
	s := NewScreen(10, 5)
	s.Write([]byte("\x1b[2;8f"))

	if s.Row != 1 || s.Col != 7 {
		t.Errorf("cursor at row=%d col=%d, want row=1 col=7", s.Row, s.Col)
	}
}

func TestCursorMoveForwardC(t *testing.T) {
	s := NewScreen(10, 5)
	s.Write([]byte("\r\x1b[3C"))

	if s.Row != 0 || s.Col != 3 {
		t.Errorf("cursor at row=%d col=%d, want row=0 col=3", s.Row, s.Col)
	}
}

func TestESC_DECKPAMIgnored(t *testing.T) {
	s := NewScreen(10, 2)
	s.Write([]byte("\x1b=abc"))

	assertCell(t, s, 0, 0, 'a')
	assertCell(t, s, 1, 0, 'b')
	assertCell(t, s, 2, 0, 'c')
	if s.Col != 3 || s.Row != 0 {
		t.Errorf("cursor at row=%d col=%d, want row=0 col=3", s.Row, s.Col)
	}
}

func TestESC_SaveRestoreCursor(t *testing.T) {
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
}

func TestESC_IndexNextLineAndReverseIndex(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("A\x1bDB\x1bEC"))

	assertCell(t, s, 0, 0, 'A')
	assertCell(t, s, 1, 1, 'B')
	assertCell(t, s, 0, 2, 'C')

	s = NewScreen(5, 3)
	s.Write([]byte("\x1b[2;1Hmid\r\x1bMtop"))
	assertCell(t, s, 0, 0, 't')
	assertCell(t, s, 0, 1, 'm')
}

func TestCSI_CursorDirectionalMoves(t *testing.T) {
	s := NewScreen(10, 4)
	s.Write([]byte("\x1b[3;5HX\x1b[2D<\x1b[1A^\x1b[1Bv\x1b[3C>"))

	assertCell(t, s, 4, 2, 'X')
	assertCell(t, s, 3, 2, '<')
	assertCell(t, s, 4, 1, '^')
	assertCell(t, s, 5, 2, 'v')
	assertCell(t, s, 9, 2, '>')
}

func TestCSI_InsertDeleteCharacters(t *testing.T) {
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
}

func TestCSI_InsertDeleteLines(t *testing.T) {
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
}

func TestCSI_ScrollRegion(t *testing.T) {
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
}

func TestCSI_PrivateAlternateScreenRestoresNormalScreen(t *testing.T) {
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
}

func TestRendererStaysInSyncAfterVTEditDamage(t *testing.T) {
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
}

func TestDCS_STTerminatedIgnored(t *testing.T) {
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
}

func TestFishLikePromptRedrawPreservesTypedCharacters(t *testing.T) {
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
}

// ---------------------------------------------------------------------------
// Scroll damage
// ---------------------------------------------------------------------------

func TestScrollDamageKind(t *testing.T) {
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
}

func TestScrollDamageCoordinates(t *testing.T) {
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
}

// ---------------------------------------------------------------------------
// Damage coalescing
// ---------------------------------------------------------------------------

func TestDamageCoalescing(t *testing.T) {
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
}

func TestDamageCoalescingNewlineBreaks(t *testing.T) {
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
}

func TestDamageCoalescingFullRedraw(t *testing.T) {
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
}

// ---------------------------------------------------------------------------
// Resize
// ---------------------------------------------------------------------------

func TestResizeLarger(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("Hello"))
	s.Resize(10, 5)

	if s.Frame.Width != 10 || s.Frame.Height != 5 {
		t.Errorf("frame size = %dx%d, want 10x5", s.Frame.Width, s.Frame.Height)
	}
	// Should be blank (new frame).
	for x := 0; x < 10; x++ {
		assertCell(t, s, x, 0, ' ')
	}
	// Cursor reset.
	if s.Row != 0 || s.Col != 0 {
		t.Errorf("cursor at (%d,%d), want (0,0)", s.Row, s.Col)
	}
	// Should have FullRedraw.
	d := s.Damage()
	if !hasDamageKind(d, renderer.DamageFullRedraw) {
		t.Errorf("expected FullRedraw after resize, got %v", damageKinds(d))
	}
}

func TestResizeSameSize(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("Hello"))

	s.Resize(5, 3) // same size

	assertCell(t, s, 0, 0, 'H') // content preserved
	// No FullRedraw.
	d := s.Damage()
	if len(d) != 0 && d[0].Kind == renderer.DamageFullRedraw {
		t.Error("same-size resize should not produce FullRedraw")
	}
}

// ---------------------------------------------------------------------------
// ClearDamage
// ---------------------------------------------------------------------------

func TestClearDamage(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("Hello"))
	s.ClearDamage()
	d := s.Damage()
	if len(d) != 0 {
		t.Fatalf("expected empty damage after ClearDamage, got %+v", d)
	}
}

// ---------------------------------------------------------------------------
// Non-printable / invalid bytes
// ---------------------------------------------------------------------------

func TestInvalidBytes(t *testing.T) {
	s := NewScreen(5, 3)
	// Invalid UTF-8 continuation byte without a start byte.
	s.Write([]byte{0x80, 0x81})
	// Should not panic, and no cells should be modified.
	for y := 0; y < 3; y++ {
		for x := 0; x < 5; x++ {
			if c := cellAt(s, x, y); c.Rune != ' ' {
				t.Errorf("cell(%d,%d) = %q, want space", x, y, c.Rune)
			}
		}
	}
}

func TestControlChars(t *testing.T) {
	s := NewScreen(10, 3)
	// Control characters below 0x20 that are not handled (CR/LF/BS/TAB) should be ignored.
	ctrl := []byte{0x01, 0x02, 0x03, 0x05, 0x06, 0x0b, 0x0c, 0x0e, 0x0f}
	s.Write(ctrl)
	// No cells should be modified.
	for y := 0; y < 3; y++ {
		for x := 0; x < 10; x++ {
			if c := cellAt(s, x, y); c.Rune != ' ' {
				t.Errorf("cell(%d,%d) = %q, want space after control chars", x, y, c.Rune)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Multiple sequential CSI sequences
// ---------------------------------------------------------------------------

func TestMultipleSGR(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[31m\x1b[1m\x1b[44mX"))

	c := cellAt(s, 0, 0)
	if c.Style.Foreground != 1 || !c.Style.Bold || c.Style.Background != 4 {
		t.Errorf("cell style = %+v, want fg=1 bold bg=4", c.Style)
	}
}

func TestSGREmptyParamsReset(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[31m\x1b[1m\x1b[m")) // empty params → reset

	if s.Style != (renderer.DefaultStyle()) {
		t.Errorf("style after empty SGR = %+v, want default", s.Style)
	}
}

// ---------------------------------------------------------------------------
// Cursor move with zero / missing params
// ---------------------------------------------------------------------------

func TestCursorMoveZeroParams(t *testing.T) {
	s := NewScreen(10, 5)
	s.Write([]byte("\x1b[H")) // no params → (1,1)

	if s.Row != 0 || s.Col != 0 {
		t.Errorf("cursor at (%d,%d), want (0,0)", s.Row, s.Col)
	}
}

func TestCursorMovePartialParams(t *testing.T) {
	s := NewScreen(10, 5)
	s.Row, s.Col = 3, 4
	s.Write([]byte("\x1b[;5H")) // row=1 (default), col=5

	if s.Row != 0 || s.Col != 4 {
		t.Errorf("cursor at (%d,%d), want (0,4)", s.Row, s.Col)
	}
}

// ---------------------------------------------------------------------------
// No overflow on edge writes
// ---------------------------------------------------------------------------

func TestWriteAtEdgeNoPanic(t *testing.T) {
	s := NewScreen(1, 1)
	// Fill the only cell then try to write more (should scroll).
	s.Write([]byte("ABC"))
	// Should not panic.
	_ = cellAt(s, 0, 0)
}

// ---------------------------------------------------------------------------
// Style persistence across writes
// ---------------------------------------------------------------------------

func TestStylePersists(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("\x1b[1mAB"))
	s.Write([]byte("CD"))

	c1 := cellAt(s, 0, 0)
	c2 := cellAt(s, 2, 0)
	if !c1.Style.Bold || !c2.Style.Bold {
		t.Error("bold should persist across Write calls")
	}
}

// ---------------------------------------------------------------------------
// Rune width (CJK, emoji, combining marks, zero-width)
// ---------------------------------------------------------------------------

func TestCJKWideChars(t *testing.T) {
	s := NewScreen(10, 3)
	// Write two CJK characters that are width 2 in a real terminal.
	// They should be rendered as '?' placeholders, advancing cursor by 1 each.
	s.Write([]byte("\xe4\xbd\xa0\xe5\xa5\xbd")) // 你好

	assertCell(t, s, 0, 0, '?')
	assertCell(t, s, 1, 0, '?')
	if s.Col != 2 || s.Row != 0 {
		t.Errorf("cursor at col=%d row=%d, want col=2 row=0", s.Col, s.Row)
	}
}

func TestCJKMixedWithASCII(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("A\xe4\xbd\xa0B")) // A你B

	assertCell(t, s, 0, 0, 'A')
	assertCell(t, s, 1, 0, '?')
	assertCell(t, s, 2, 0, 'B')
	if s.Col != 3 {
		t.Errorf("cursor at col=%d, want 3", s.Col)
	}
}

func TestEmojiRenderedAsPlaceholder(t *testing.T) {
	s := NewScreen(10, 3)
	em := []byte("\U0001f600") // 😀 grinning face
	s.Write(em)

	// Wide emoji is rendered as '?' placeholder.
	assertCell(t, s, 0, 0, '?')
	if s.Col != 1 {
		t.Errorf("cursor at col=%d, want 1", s.Col)
	}
}

func TestCombiningMarkSkipped(t *testing.T) {
	s := NewScreen(10, 3)
	// 'A' followed by combining acute accent U+0301
	s.Write([]byte("A\xcc\x81"))

	assertCell(t, s, 0, 0, 'A')
	// Combining mark should be skipped — no cell written, no cursor advance.
	if s.Col != 1 {
		t.Errorf("cursor at col=%d, want 1 (combining mark should not advance)", s.Col)
	}
}

func TestZeroWidthCharsSkipped(t *testing.T) {
	s := NewScreen(10, 3)
	// Zero-width space U+200B
	s.Write([]byte("A\xe2\x80\x8bB"))

	assertCell(t, s, 0, 0, 'A')
	assertCell(t, s, 1, 0, 'B')
	if s.Col != 2 {
		t.Errorf("cursor at col=%d, want 2 (zero-width should not advance)", s.Col)
	}
}

func TestRuneWidth(t *testing.T) {
	tests := []struct {
		r     rune
		width int
	}{
		{' ', 1},
		{'a', 1},
		{'A', 1},
		{'1', 1},
		{0x00AD, 0},  // soft hyphen
		{0x034F, 0},  // combining grapheme joiner
		{0x0301, 0},  // combining acute accent
		{0x0300, 0},  // combining grave accent
		{0x200B, 0},  // zero-width space
		{0x200C, 0},  // zero-width non-joiner
		{0x200D, 0},  // zero-width joiner
		{0xFEFF, 0},  // BOM / zero-width no-break space
		{0x4E00, 2},  // CJK Unified Ideograph (一)
		{0x4E8C, 2},  // CJK (二)
		{0x9FFF, 2},  // CJK end of BMP
		{0x3400, 2},  // CJK Extension A
		{0xAC00, 2},  // Hangul Syllable (가)
		{0xD7AF, 2},  // Hangul Syllable end
		{0x3000, 2},  // CJK Ideographic Space
		{0xFF01, 2},  // Fullwidth Exclamation Mark
		{0xFFE0, 2},  // Fullwidth Cent Sign
		{0x1F600, 2}, // 😀
		{0x1F680, 2}, // 🚀
		{'\x00', 0},
		{'\x01', 0},
		{'\x1b', 0},
		{0x7F, 0},
	}

	for _, tt := range tests {
		got := runeWidth(tt.r)
		if got != tt.width {
			t.Errorf("runeWidth(%U %q) = %d, want %d", tt.r, tt.r, got, tt.width)
		}
	}
}

func TestDamageReturnsReference(t *testing.T) {
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
}

func TestCJKSurroundedByASCII(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte("a\xe4\xbd\xa0b\xe5\xa5\xbdc")) // a你b好c

	assertCell(t, s, 0, 0, 'a')
	assertCell(t, s, 1, 0, '?')
	assertCell(t, s, 2, 0, 'b')
	assertCell(t, s, 3, 0, '?')
	assertCell(t, s, 4, 0, 'c')
	if s.Col != 5 {
		t.Errorf("cursor at col=%d, want 5", s.Col)
	}
}

func TestCJKAtEdgeWraps(t *testing.T) {
	s := NewScreen(2, 1)
	// Write two ASCII chars to fill the line.
	s.Write([]byte("AB")) // Cells = [A B], Col=2
	// CJK at col 2 (edge) triggers newline → scroll on 1-row screen.
	// scrollUp blanks the only row, then '?' is written at col 0.
	s.Write([]byte("\xe4\xbd\xa0")) // 你

	assertCell(t, s, 0, 0, '?') // CJK placeholder after scroll+blank
	assertCell(t, s, 1, 0, ' ') // untouched
	if s.Col != 1 || s.Row != 0 {
		t.Errorf("cursor at col=%d row=%d, want col=1 row=0", s.Col, s.Row)
	}
}

func TestCJKDamageWidthNormalized(t *testing.T) {
	s := NewScreen(10, 3)
	s.ClearDamage()
	s.Write([]byte("\xe4\xbd\xa0")) // 你

	d := s.Damage()
	if len(d) == 0 {
		t.Fatal("expected damage")
	}
	// After normalization, damage width should be 1, not 2.
	if d[0].Width != 1 {
		t.Errorf("damage width = %d, want 1 (normalized)", d[0].Width)
	}
}

func TestEmojiDamageWidthNormalized(t *testing.T) {
	s := NewScreen(10, 3)
	s.ClearDamage()
	s.Write([]byte("\U0001f600")) // 😀

	d := s.Damage()
	if len(d) == 0 {
		t.Fatal("expected damage")
	}
	if d[0].Width != 1 {
		t.Errorf("damage width = %d, want 1 (normalized)", d[0].Width)
	}
}

func TestMixedCursorAlignment(t *testing.T) {
	s := NewScreen(10, 3)
	// ASCII 'a', CJK '你', emoji '😀', ASCII 'b', combining acute '◌́'
	s.Write([]byte("a\xe4\xbd\xa0\U0001f600b\xcc\x81"))

	assertCell(t, s, 0, 0, 'a') // 'a'
	assertCell(t, s, 1, 0, '?') // 你 normalized
	assertCell(t, s, 2, 0, '?') // 😀 normalized
	assertCell(t, s, 3, 0, 'b') // 'b'
	// Combining mark skipped, no cell at col 4.
	// Cursor at col 4 (advanced by a, CJK, emoji, b).
	if s.Col != 4 || s.Row != 0 {
		t.Errorf("cursor at col=%d row=%d, want col=4 row=0", s.Col, s.Row)
	}
}

func TestCombiningMarkNoCellWritten(t *testing.T) {
	s := NewScreen(10, 3)
	s.ClearDamage()
	s.Write([]byte("\xcc\x81")) // combining acute accent alone (no base char)

	// No cells should be modified.
	for y := 0; y < 3; y++ {
		for x := 0; x < 10; x++ {
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
}

// ---------------------------------------------------------------------------
// CSI S / SU — Scroll Up
// ---------------------------------------------------------------------------

func TestCSI_SCrollUpDefault(t *testing.T) {
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
}

func TestCSI_SCrollUpWithCount(t *testing.T) {
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
}

func TestCSI_SCrollUpClamp(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("AAAAABBBBBCCCCC")) // fill 3 rows
	s.ClearDamage()

	// CSI 100 S on a 3-row screen → clamp to 3 (entire screen blank).
	s.Write([]byte("\x1b[100S"))

	// All rows should be blank.
	for y := 0; y < 3; y++ {
		for x := 0; x < 5; x++ {
			assertCell(t, s, x, y, ' ')
		}
	}
}

func TestCSI_SCrollUpCountZero(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("AAAAABBBBBCCCCC")) // fill 3 rows
	s.ClearDamage()

	// CSI 0 S → default to 1 (parameter 0 means default in VT spec).
	s.Write([]byte("\x1b[0S"))

	// Row 0 should have shifted up: row 0 = BBBBB, row 1 = CCCCC, row 2 = blank.
	assertCell(t, s, 0, 0, 'B')
	assertCell(t, s, 0, 1, 'C')
	assertCell(t, s, 0, 2, ' ')
}

func TestCSI_SCrollUpDamageCount(t *testing.T) {
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
}

func TestCSI_SCursorPreserved(t *testing.T) {
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
}

// ---------------------------------------------------------------------------
// OSC sequences
// ---------------------------------------------------------------------------

func TestOSC_BELTerminatedIgnored(t *testing.T) {
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
}

func TestOSC_STTerminatedIgnored(t *testing.T) {
	s := NewScreen(20, 2)
	s.Write([]byte("\x1b]133;A;click_events=1\x1b\\> "))

	assertCell(t, s, 0, 0, '>')
	assertCell(t, s, 1, 0, ' ')
	if s.Col != 2 || s.Row != 0 {
		t.Errorf("cursor at col=%d row=%d, want col=2 row=0", s.Col, s.Row)
	}
}

func TestOSC_MultipleSequencesIgnored(t *testing.T) {
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
}

func TestOSC_BELTerminatedSplitAcrossWritesIgnored(t *testing.T) {
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
}

func TestOSC_STTerminatedSplitAcrossWritesIgnored(t *testing.T) {
	s := NewScreen(20, 2)
	s.Write([]byte("\x1b]133;A;click_events=1\x1b"))
	s.Write([]byte("\\> "))

	assertCell(t, s, 0, 0, '>')
	assertCell(t, s, 1, 0, ' ')
	if s.Col != 2 || s.Row != 0 {
		t.Errorf("cursor at col=%d row=%d, want col=2 row=0", s.Col, s.Row)
	}
}

func TestOSC_UnterminatedSequenceIsBoundedAndDropped(t *testing.T) {
	s := NewScreen(20, 2)
	payload := strings.Repeat("x", maxEscapeBufferLen+1)
	s.Write([]byte("\x1b]0;" + payload))
	s.Write([]byte("OK"))

	assertCell(t, s, 0, 0, 'O')
	assertCell(t, s, 1, 0, 'K')
	if len(s.escapeBuf) != 0 {
		t.Fatalf("escapeBuf length = %d, want 0 after overlong unterminated OSC", len(s.escapeBuf))
	}
}

// ---------------------------------------------------------------------------
// C1 Unicode controls (U+0080-U+009F)
// ---------------------------------------------------------------------------

func TestC1ControlsDropped(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte{0xC2, 0x80}) // U+0080 PAD
	s.Write([]byte{0xC2, 0x9F}) // U+009F APC

	// No cells should be modified.
	for y := 0; y < 3; y++ {
		for x := 0; x < 10; x++ {
			if c := cellAt(s, x, y); c.Rune != ' ' {
				t.Errorf("cell(%d,%d) = %q, want space after C1 controls", x, y, c.Rune)
			}
		}
	}
	// Cursor should not have moved.
	if s.Col != 0 || s.Row != 0 {
		t.Errorf("cursor at col=%d row=%d, want col=0 row=0", s.Col, s.Row)
	}
}

func TestC1ControlsRange(t *testing.T) {
	s := NewScreen(10, 3)
	// Send all C1 control runes U+0080 through U+009F.
	for r := 0x80; r <= 0x9F; r++ {
		s.Write([]byte{byte(0xC0 | r>>6), byte(0x80 | r&0x3F)})
	}

	// No cells should be modified.
	for y := 0; y < 3; y++ {
		for x := 0; x < 10; x++ {
			if c := cellAt(s, x, y); c.Rune != ' ' {
				t.Errorf("cell(%d,%d) = %q, want space after C1 range", x, y, c.Rune)
			}
		}
	}
	if s.Col != 0 || s.Row != 0 {
		t.Errorf("cursor at col=%d row=%d, want col=0 row=0", s.Col, s.Row)
	}
}

func TestC1ControlsAfterPrintable(t *testing.T) {
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
}

func TestC1ControlsPreserveCRLF(t *testing.T) {
	s := NewScreen(10, 3)
	// C1 controls mixed with CR/LF should preserve CR/LF behavior.
	s.Write([]byte{0xC2, 0x80}) // U+0080 (dropped)
	s.Write([]byte("A\rB"))     // CR moves cursor back, overwrite A
	s.Write([]byte{0xC2, 0x9F}) // U+009F (dropped)

	assertCell(t, s, 0, 0, 'B')
}

func TestC1ControlsPreserveLF(t *testing.T) {
	s := NewScreen(10, 3)
	s.Write([]byte{0xC2, 0x80}) // U+0080 (dropped)
	s.Write([]byte("A\nB"))

	assertCell(t, s, 0, 0, 'A')
	assertCell(t, s, 1, 1, 'B')
	if s.Row != 1 || s.Col != 2 {
		t.Errorf("cursor at row=%d col=%d, want row=1 col=2 (LF advances row without CR)", s.Row, s.Col)
	}
}

func TestC1ControlsNoDamage(t *testing.T) {
	s := NewScreen(10, 3)
	s.ClearDamage()
	// C1 controls should produce no damage.
	s.Write([]byte{0xC2, 0x80, 0xC2, 0x81, 0xC2, 0x9F})

	d := s.Damage()
	if len(d) != 0 {
		t.Errorf("expected no damage from C1 controls, got %+v", d)
	}
}

func TestEraseDisplayModes(t *testing.T) {
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
}

func TestEraseLineModes(t *testing.T) {
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
	for x := 0; x < 5; x++ {
		if c := cellAt(s, x, 0); c.Rune != ' ' {
			t.Fatalf("line clear left %q at %d", c.Rune, x)
		}
	}
}
