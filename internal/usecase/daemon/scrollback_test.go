package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestNewWindowInitializesScrollback(t *testing.T) {
	tests := []struct {
		name    string
		write   []byte
		wantLen int
		wantRow string
	}{
		{
			name:    "window owns scrollback wired to vt eviction callback",
			write:   []byte("AAAA\r\nBBBB\r\nCCCC\r\n"),
			wantLen: 1,
			wantRow: "AAAA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			win := newWindow(nil, domain.Size{Cols: 4, Rows: 3})
			if win.scrollback == nil {
				t.Fatal("scrollback is nil")
			}
			if win.scrollback.Cap() != defaultScrollbackRows {
				t.Fatalf("scrollback cap = %d, want %d", win.scrollback.Cap(), defaultScrollbackRows)
			}

			win.screen.Write(tt.write)

			if got := win.scrollback.Len(); got != tt.wantLen {
				t.Fatalf("scrollback len = %d, want %d", got, tt.wantLen)
			}
			row := win.scrollback.Row(0)
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
