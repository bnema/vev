package vt

import "testing"

func TestResizeReflowMapsActiveAndSavedCursorAtNewRowBoundary(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("abcdefgh"))
	s.Row, s.Col = 0, 3
	s.saveCursor()

	s.Resize(3, 3)

	if s.Row != 1 || s.Col != 0 {
		t.Fatalf("active cursor = (%d,%d), want (1,0)", s.Row, s.Col)
	}
	s.Row, s.Col = 0, 0
	s.restoreCursor()
	if s.Row != 1 || s.Col != 0 {
		t.Fatalf("saved cursor = (%d,%d), want (1,0)", s.Row, s.Col)
	}
}
