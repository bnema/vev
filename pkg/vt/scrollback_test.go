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
