package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestNewTabInitializesScrollback(t *testing.T) {
	tests := []struct {
		name    string
		write   []byte
		wantLen int
		wantRow string
	}{
		{
			name:    "primary evictions enter screen owned history",
			write:   []byte("AAAA\r\nBBBB\r\nCCCC\r\n"),
			wantLen: 1,
			wantRow: "AAAA",
		},
		{
			name:    "alternate screen evictions do not enter pane history",
			write:   []byte("AAAA\r\nBBBB\r\nCCCC\r\n\x1b[?1049h1111\r\n2222\r\n3333\r\n4444\x1b[?1049l"),
			wantLen: 1,
			wantRow: "AAAA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			win := newTab(nil, domain.Size{Cols: 4, Rows: 3})
			if win.focusedPane().history == nil {
				t.Fatal("scrollback is nil")
			}
			if win.focusedPane().history != win.focusedPane().screen.History() {
				t.Fatal("pane history is not owned by its screen")
			}
			if win.focusedPane().history.Cap() != defaultScrollbackRows {
				t.Fatalf("scrollback cap = %d, want %d", win.focusedPane().history.Cap(), defaultScrollbackRows)
			}

			win.focusedPane().screen.Write(tt.write)

			if got := win.focusedPane().history.Len(); got != tt.wantLen {
				t.Fatalf("scrollback len = %d, want %d", got, tt.wantLen)
			}
			row := win.focusedPane().history.View().Row(0)
			runes := make([]rune, len(row))
			for i, c := range row {
				runes[i] = c.Rune
			}
			if got := string(runes); got != tt.wantRow {
				t.Fatalf("scrollback row = %q, want %q", got, tt.wantRow)
			}
		})
	}
}
