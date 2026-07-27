package vt

import "testing"

func TestCollapsedResizeUpdatesSavedPrimaryAndClearsEscape(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("shell"))
	s.Write([]byte("\x1b[?1049h"))
	s.Write([]byte("\x1b["))

	s.Resize(0, 0)

	if len(s.escapeBuf) != 0 {
		t.Fatalf("partial escape survives collapsed resize: %q", s.escapeBuf)
	}
	if s.Frame.Width != 0 || s.Frame.Height != 0 {
		t.Fatalf("active frame = %dx%d, want 0x0", s.Frame.Width, s.Frame.Height)
	}
	s.Write([]byte("\x1b[?1049l"))
	if s.Frame.Width != 0 || s.Frame.Height != 0 {
		t.Fatalf("saved primary frame = %dx%d, want 0x0", s.Frame.Width, s.Frame.Height)
	}
}

func TestResizeReflowDropsAbandonedWideEdgePadding(t *testing.T) {
	s := NewScreen(4, 3)
	s.Write([]byte("ab界"))
	s.Write([]byte("\x1b[1;4H界x"))

	s.Resize(6, 3)

	assertCell(t, s, 0, 0, 'a')
	assertCell(t, s, 1, 0, 'b')
	assertCell(t, s, 2, 0, '界')
	if !cellAt(s, 3, 0).Continuation {
		t.Fatal("reflowed wide rune lost continuation")
	}
	assertCell(t, s, 4, 0, 'x')
}

func TestInsertModeRetainsShiftedTailInReflowExtent(t *testing.T) {
	s := NewScreen(6, 2)
	s.Write([]byte("abcd"))
	s.Write([]byte("\x1b[1;2H\x1b[4hX"))
	// Simulate the subsequent logical continuation that causes the row to be
	// reflowed. Its meaningful extent must include the shifted tail.
	s.buffer.continueRow(0)

	s.Resize(5, 2)

	for x, r := range []rune("aXbcd") {
		assertCell(t, s, x, 0, r)
	}
}
