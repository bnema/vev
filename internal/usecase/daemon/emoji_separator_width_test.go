package daemon

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	vt "bnema.dev/vev-vt"
	renderer "bnema.dev/vev-vt/ansi"
	"github.com/rivo/uniseg"
)

// Use an independent width oracle: replay into vev-vt would repeat its width bug.
func TestEmojiSeparatorWidth(t *testing.T) {
	ansiCSI := regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)
	for _, tc := range []struct {
		name, text string
		edge       bool
	}{
		{"ascii", "plain text", false},
		{"CJK", "界", false},
		{"check", "• ✅ Tout ce qu'on a décidé est écrit", false},
		{"cross", "• ❌ Tout ce qu'on a décidé est écrit", false},
		{"VS16", "• ✅\uFE0F Tout ce qu'on a décidé est écrit", false},
		{"check-last-column", "✅", true},
		{"cross-last-column", "❌", true},
		{"VS16-last-column", "✅\uFE0F", true},
		{"CJK-last-column", "界", true},
	} {
		for _, resized := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resized=%v", tc.name, resized), func(t *testing.T) {
				const initial = 48
				n := initial
				screen := vt.NewScreen(n, 3)
				if resized {
					// Exercise resize/reflow on existing content before replacing the line.
					screen.Write([]byte(tc.text))
					n = 44
					screen.Resize(n, 3)
					screen.Write([]byte("\x1b[2J\x1b[H"))
				}
				text := tc.text
				if tc.edge {
					text = strings.Repeat("x", n-1) + text
				}
				screen.Write([]byte(text))
				if tc.edge {
					if got := screen.Cell(0, 1).Rune; got != []rune(tc.text)[0] {
						t.Errorf("wide glyph at last column did not wrap: next row head=%q; cursor=(%d,%d)", got, screen.Col, screen.Row)
					}
				}
				for y := 0; y < 3; y++ {
					frame := renderer.NewFrame(n+5, 1)
					for x := 0; x < n; x++ {
						frame.Set(x, 0, screen.Cell(x, y))
					}
					frame.Set(n, 0, renderer.Cell{Rune: '│', Style: renderer.DefaultStyle()})
					frame.Set(n+1, 0, renderer.Cell{Rune: 'R', Style: renderer.DefaultStyle()})
					out, err := renderer.New(renderer.Capabilities{}).Draw(frame, []renderer.Damage{renderer.FullRedraw()})
					if err != nil {
						t.Fatal(err)
					}
					visible := ansiCSI.ReplaceAllString(string(out), "")
					prefix, _, ok := strings.Cut(visible, "│")
					if !ok {
						t.Fatalf("missing separator: %q", out)
					}
					if got := uniseg.StringWidth(prefix); got != n {
						t.Errorf("row %d separator at zero-based column %d, want %d (cursor advance mismatch); output=%q", y, got, n, out)
					}
				}
			})
		}
	}
}
