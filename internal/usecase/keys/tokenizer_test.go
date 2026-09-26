package keys

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanEscape(t *testing.T) {
	paste := "\x1b[200~\x1bj\x1b[1;3A\x1b[201~"
	tests := []struct {
		name  string
		in    string
		kind  escKind
		raw   string
		rune  rune
		arrow byte
	}{
		{name: "lone esc", in: "\x1b", kind: escIncomplete, raw: "\x1b"},
		{name: "alt letter", in: "\x1bj", kind: escAltRune, raw: "\x1bj", rune: 'j'},
		{name: "alt letter trailing bytes", in: "\x1bjxyz", kind: escAltRune, raw: "\x1bj", rune: 'j'},
		{name: "alt space", in: "\x1b ", kind: escAltRune, raw: "\x1b ", rune: ' '},
		{name: "alt utf8", in: "\x1bé", kind: escAltRune, raw: "\x1bé", rune: 'é'},
		{name: "alt utf8 truncated", in: "\x1b\xc3", kind: escIncomplete, raw: "\x1b\xc3"},
		{name: "alt invalid utf8", in: "\x1b\xc3X", kind: escBare, raw: "\x1b"},
		{name: "alt stray continuation", in: "\x1b\xa9", kind: escBare, raw: "\x1b"},
		{name: "double esc", in: "\x1b\x1b", kind: escAltRune, raw: "\x1b\x1b", rune: rune(ESC)},
		{name: "alt arrow", in: "\x1b[1;3A", kind: escAltArrow, raw: "\x1b[1;3A", arrow: 'A'},
		{name: "meta arrow trailing", in: "\x1b[1;9Dzz", kind: escAltArrow, raw: "\x1b[1;9D", arrow: 'D'},
		{name: "alt arrow prefix 2", in: "\x1b[", kind: escIncomplete, raw: "\x1b["},
		{name: "alt arrow prefix 3", in: "\x1b[1", kind: escIncomplete, raw: "\x1b[1"},
		{name: "alt arrow prefix 5", in: "\x1b[1;3", kind: escIncomplete, raw: "\x1b[1;3"},
		{name: "alt arrow wrong final", in: "\x1b[1;3E", kind: escControlPrefix, raw: "\x1b[", rune: '['},
		{name: "ctrl arrow", in: "\x1b[1;5A", kind: escControlPrefix, raw: "\x1b[", rune: '['},
		{name: "bare arrow", in: "\x1b[A", kind: escControlPrefix, raw: "\x1b[", rune: '['},
		{name: "ss3", in: "\x1bOP", kind: escControlPrefix, raw: "\x1bO", rune: 'O'},
		{name: "paste complete", in: paste + "tail", kind: escPaste, raw: paste},
		{name: "paste unterminated", in: "\x1b[200~hello", kind: escControlPrefix, raw: "\x1b[", rune: '['},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scanEscape([]byte(tt.in))
			require.Equal(t, tt.kind, got.kind)
			require.Equal(t, []byte(tt.raw), got.raw)
			require.Equal(t, tt.rune, got.rune)
			require.Equal(t, tt.arrow, got.arrow)
		})
	}
}

func TestScanEscapeEveryAltArrowPrefixIsIncomplete(t *testing.T) {
	for _, full := range []string{"\x1b[1;3A", "\x1b[1;9C"} {
		for n := 1; n < len(full); n++ {
			got := scanEscape([]byte(full[:n]))
			require.Equal(t, escIncomplete, got.kind, "prefix %q", full[:n])
			require.Equal(t, []byte(full[:n]), got.raw)
		}
	}
}
