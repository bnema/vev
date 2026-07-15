package vt

import (
	"testing"
)

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
			name: "ESC 7 / ESC 8 restore origin and insert modes",
			run: func(t *testing.T) {
				s := NewScreen(6, 5)
				s.Write([]byte("\x1b[2;4r\x1b[?6h\x1b[4h\x1b7"))
				s.Write([]byte("\x1b[?6l\x1b[4l\x1b8"))

				if !s.originMode {
					t.Fatal("origin mode was not restored")
				}
				if !s.insertMode {
					t.Fatal("insert mode was not restored")
				}

				s.Write([]byte("\x1b[1;1Habcd\x1b[1;3HXY"))
				if s.Row != 1 || s.Col != 4 {
					t.Fatalf("cursor after restored origin and insert modes = (%d,%d), want (1,4)", s.Row, s.Col)
				}
				if got := lineText(s, 1); got != "abXYcd" {
					t.Fatalf("line after restored insert mode = %q, want %q", got, "abXYcd")
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
