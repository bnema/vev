package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func evictedTexts(rows [][]renderer.Cell) []string {
	out := make([]string, len(rows))
	for i, row := range rows {
		runes := make([]rune, len(row))
		for x, c := range row {
			runes[x] = c.Rune
		}
		out[i] = string(runes)
	}
	return out
}

func TestOnLineEvicted(t *testing.T) {
	tests := []struct {
		name      string
		write     []byte
		wantRows  []string
		wantFrame []string
	}{
		{
			name:      "newline scroll evicts top row",
			write:     []byte("AAAA\r\nBBBB\r\nCCCC\r\n"),
			wantRows:  []string{"AAAA"},
			wantFrame: []string{"BBBB", "CCCC", "    "},
		},
		{
			name:      "CSI S count evicts each top row in order",
			write:     []byte("AAAA\r\nBBBB\r\nCCCC\x1b[2S"),
			wantRows:  []string{"AAAA", "BBBB"},
			wantFrame: []string{"CCCC", "    ", "    "},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(4, 3)
			var evicted [][]renderer.Cell
			s.OnLineEvicted = func(row []renderer.Cell) {
				evicted = append(evicted, append([]renderer.Cell(nil), row...))
			}

			s.Write(tt.write)

			if got := evictedTexts(evicted); !equalStrings(got, tt.wantRows) {
				t.Fatalf("evicted rows = %#v, want %#v", got, tt.wantRows)
			}
			for y, want := range tt.wantFrame {
				if got := lineText(s, y); got != want {
					t.Fatalf("line %d = %q, want %q", y, got, want)
				}
			}
		})
	}
}

func TestOnLineEvictedAltScreenAndRotation(t *testing.T) {
	tests := []struct {
		name     string
		write    []byte
		wantRows []string
	}{
		{
			name:     "alternate screen scroll does not evict",
			write:    []byte("main\x1b[?1049hAAAA\r\nBBBB\r\nCCCC\r\nDDDD\x1b[?1049lEEEE\r\n"),
			wantRows: nil,
		},
		{
			name:     "evicted row follows logical top after prior lineOffset rotation",
			write:    []byte("AAAA\r\nBBBB\r\nCCCC\r\nDDDD\r\n"),
			wantRows: []string{"AAAA", "BBBB"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(4, 3)
			var evicted [][]renderer.Cell
			s.OnLineEvicted = func(row []renderer.Cell) {
				evicted = append(evicted, append([]renderer.Cell(nil), row...))
			}

			s.Write(tt.write)

			if got := evictedTexts(evicted); !equalStrings(got, tt.wantRows) {
				t.Fatalf("evicted rows = %#v, want %#v", got, tt.wantRows)
			}
			if err := s.Frame.CheckInvariants(); err != nil {
				t.Fatalf("frame invariants after scrollback callback: %v", err)
			}
		})
	}
}

func TestEvictedLinesCarryTheirBound(t *testing.T) {
	tests := []struct {
		name     string
		evict    func(s *Screen)
		wantSoft bool
	}{
		{
			name: "scroll evicts a soft-wrapped row",
			evict: func(s *Screen) {
				// Filling all four columns only leaves deferred wrap pending. The
				// fifth printable character triggers the soft wrap before scrolling.
				s.Write([]byte("abcde"))
				s.Write([]byte("\r\n\r\n\r\n\r\n"))
			},
			wantSoft: true,
		},
		{
			name: "scroll evicts a hard row",
			evict: func(s *Screen) {
				s.Write([]byte("ab\r\n\r\n\r\n\r\n"))
			},
			wantSoft: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScreenWithHistory(4, 2, HistoryConfig{MaxRows: 16, MaxCells: 1024})
			tc.evict(s)
			view := s.History().SealAndView()
			if view.Len() == 0 {
				t.Fatal("nothing was evicted to history")
			}
			if got := view.Bound(0).Soft; got != tc.wantSoft {
				t.Errorf("Bound(0).Soft = %v, want %v", got, tc.wantSoft)
			}
		})
	}
}

func TestReflowEvictedLinesCarryTheirBound(t *testing.T) {
	s := NewScreenWithHistory(8, 3, HistoryConfig{MaxRows: 16, MaxCells: 1024})
	// Fill the grid with wrapped content, then shrink so rows are evicted by
	// the reflow path in buffer.resize rather than by scrolling.
	s.Write([]byte("aaaaaaaabbbbbbbbcccccccc"))
	s.Resize(4, 2)

	view := s.History().SealAndView()
	if view.Len() == 0 {
		t.Fatal("reflow evicted nothing to history")
	}
	for i := range view.Len() {
		bound := view.Bound(i)
		if bound.End == 0 {
			t.Errorf("Bound(%d).End = 0, want the reflowed row extent", i)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
