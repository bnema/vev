package copy

import (
	"fmt"
	"strings"
	"testing"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
)

var benchmarkMatchesSink []SearchMatch

// BenchmarkFindMatches12K measures one copy-search keystroke over a full
// default-sized scrollback (12k rows x 160 columns), with hot and idle
// (compressed) history pages.
func BenchmarkFindMatches12K(b *testing.B) {
	const rows, width = 12_000, 160
	for _, cold := range []bool{false, true} {
		b.Run(fmt.Sprintf("cold-%v", cold), func(b *testing.B) {
			history := vt.NewHistory(vt.HistoryConfig{MaxRows: rows, ChunkRows: 256})
			words := strings.Fields("build test vet lint daemon client render diff pane tab session snapshot")
			for i := range rows {
				var line strings.Builder
				fmt.Fprintf(&line, "line %06d", i)
				for j := 0; line.Len() < width-12; j++ {
					line.WriteString(" " + words[(i+j)%len(words)])
				}
				cells := row(line.String())
				if err := history.Append(cells, vt.LineBound{End: len(cells)}); err != nil {
					b.Fatal(err)
				}
			}
			if cold {
				history.SealAndView()
				for range 2 {
					if _, err := history.CompressIdle(rows); err != nil {
						b.Fatal(err)
					}
				}
			}
			doc := NewDocument(NewSnapshot(history, renderer.NewFrame(width, 45), nil, nil), "")
			b.ReportAllocs()
			for b.Loop() {
				benchmarkMatchesSink = FindMatches(doc, "snapsh")
			}
			if len(benchmarkMatchesSink) == 0 {
				b.Fatal("expected matches")
			}
		})
	}
}
