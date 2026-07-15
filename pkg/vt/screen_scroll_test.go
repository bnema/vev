package vt

import (
	"testing"
)

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
