package copy

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func documentCells(text string) []renderer.Cell {
	cells := make([]renderer.Cell, 0, len([]rune(text)))
	for _, r := range text {
		cells = append(cells, renderer.Cell{Rune: r})
	}
	return cells
}

func documentFromStrings(lines []string, separators string) *Document {
	rows := make([][]renderer.Cell, len(lines))
	for i, line := range lines {
		rows[i] = documentCells(line)
	}
	return NewDocument(NewSnapshotFromRows(rows, 32, 4), separators)
}

func TestDocumentNormalize(t *testing.T) {
	wide := []renderer.Cell{{Rune: 'a'}, {Rune: '界'}, {Continuation: true}, {Rune: ' '}, {Rune: 'b'}}
	doc := NewDocument(NewSnapshotFromRows([][]renderer.Cell{wide, {}}, 5, 1), " -_@")

	tests := []struct {
		name string
		in   Pos
		want Pos
		ok   bool
	}{
		{"glyph head", Pos{0, 1}, Pos{0, 1}, true},
		{"wide continuation", Pos{0, 2}, Pos{0, 1}, true},
		{"empty row stable position", Pos{1, 0}, Pos{1, 0}, true},
		{"negative row", Pos{-1, 0}, Pos{}, false},
		{"past row", Pos{2, 0}, Pos{}, false},
		{"negative column", Pos{0, -1}, Pos{}, false},
		{"past column", Pos{0, 5}, Pos{}, false},
		{"empty row past column", Pos{1, 1}, Pos{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := doc.Normalize(tt.in)
			require.Equal(t, tt.ok, ok)
			if ok {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestDocumentGlyphNavigation(t *testing.T) {
	wide := []renderer.Cell{{Rune: 'a'}, {Rune: '界'}, {Continuation: true}, {Rune: ' '}, {Rune: 'b'}}
	doc := NewDocument(NewSnapshotFromRows([][]renderer.Cell{wide}, 5, 1), " -_@")

	tests := []struct {
		name string
		fn   func(Pos) (Pos, bool)
		in   Pos
		want Pos
		ok   bool
	}{
		{"next narrow to wide", doc.NextGlyph, Pos{0, 0}, Pos{0, 1}, true},
		{"next skips continuation", doc.NextGlyph, Pos{0, 1}, Pos{0, 3}, true},
		{"next at end", doc.NextGlyph, Pos{0, 4}, Pos{}, false},
		{"previous skips continuation", doc.PrevGlyph, Pos{0, 3}, Pos{0, 1}, true},
		{"previous continuation normalizes", doc.PrevGlyph, Pos{0, 2}, Pos{0, 0}, true},
		{"previous at start", doc.PrevGlyph, Pos{0, 0}, Pos{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.fn(tt.in)
			require.Equal(t, tt.ok, ok)
			if ok {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestDocumentWordBounds(t *testing.T) {
	doc := documentFromStrings([]string{"alpha/beta gamma", "été\u2003東京", ""}, "/")

	tests := []struct {
		name  string
		in    Pos
		start Pos
		end   Pos
		ok    bool
	}{
		{"configured separator", Pos{0, 5}, Pos{}, Pos{}, false},
		{"second word", Pos{0, 8}, Pos{0, 6}, Pos{0, 9}, true},
		{"unicode whitespace", Pos{1, 3}, Pos{}, Pos{}, false},
		{"unicode word", Pos{1, 5}, Pos{1, 4}, Pos{1, 5}, true},
		{"empty row", Pos{2, 0}, Pos{}, Pos{}, false},
		{"out of range", Pos{4, 0}, Pos{}, Pos{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, ok := doc.WordBounds(tt.in)
			require.Equal(t, tt.ok, ok)
			if ok {
				require.Equal(t, tt.start, start)
				require.Equal(t, tt.end, end)
			}
		})
	}
}

func TestDocumentWordMovement(t *testing.T) {
	doc := documentFromStrings([]string{"alpha/beta", " gamma", "delta"}, "/")

	tests := []struct {
		name string
		fn   func(Pos) (Pos, bool)
		in   Pos
		want Pos
	}{
		{"next skips current word and separator", doc.NextWordStart, Pos{0, 2}, Pos{0, 6}},
		{"next crosses rows", doc.NextWordStart, Pos{0, 7}, Pos{1, 1}},
		{"previous returns current word start", doc.PreviousWordStart, Pos{1, 3}, Pos{1, 1}},
		{"previous crosses rows", doc.PreviousWordStart, Pos{1, 1}, Pos{0, 6}},
		{"next end returns current word end", doc.NextWordEnd, Pos{0, 2}, Pos{0, 4}},
		{"next end skips separators", doc.NextWordEnd, Pos{0, 5}, Pos{0, 9}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.fn(tt.in)
			require.True(t, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDocumentNextWordEndAdvancesPastCompletedWord(t *testing.T) {
	doc := documentFromStrings([]string{"alpha beta"}, "/")

	got, ok := doc.NextWordEnd(Pos{Row: 0, Col: 4})

	require.True(t, ok)
	require.Equal(t, Pos{Row: 0, Col: 9}, got)
}

func TestDocumentNextWordEndCrossesEmptyRow(t *testing.T) {
	doc := documentFromStrings([]string{"alpha", "", "beta"}, "/")

	got, ok := doc.NextWordEnd(Pos{Row: 0, Col: 4})

	require.True(t, ok)
	require.Equal(t, Pos{Row: 2, Col: 3}, got)
}

func TestDocumentWordMovementPreservesWideSeparatorsEmptyRowsAndEndProgression(t *testing.T) {
	wide := []renderer.Cell{{Rune: 'a'}, {Rune: '界'}, {Continuation: true}, {Rune: '/'}, {Rune: 'b'}}
	doc := NewDocument(NewSnapshotFromRows([][]renderer.Cell{wide, {}, documentCells(" cd")}, 5, 3), "/")

	tests := []struct {
		name string
		move func(Pos) (Pos, bool)
		in   Pos
		want Pos
	}{
		{"next start skips wide word and separator", doc.NextWordStart, Pos{Row: 0, Col: 1}, Pos{Row: 0, Col: 4}},
		{"previous start crosses separator", doc.PreviousWordStart, Pos{Row: 0, Col: 4}, Pos{Row: 0, Col: 0}},
		{"end advances from wide word", doc.NextWordEnd, Pos{Row: 0, Col: 1}, Pos{Row: 0, Col: 4}},
		{"next start crosses empty row", doc.NextWordStart, Pos{Row: 0, Col: 4}, Pos{Row: 2, Col: 1}},
		{"previous start crosses empty row", doc.PreviousWordStart, Pos{Row: 2, Col: 1}, Pos{Row: 0, Col: 4}},
		{"end crosses empty row", doc.NextWordEnd, Pos{Row: 0, Col: 4}, Pos{Row: 2, Col: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.move(tt.in)
			require.True(t, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDocumentWordMovementDoesNotAllocateWholeDocument(t *testing.T) {
	lines := make([]string, 8_192)
	lines[0] = "alpha/beta"
	for i := 1; i < len(lines); i++ {
		lines[i] = "unrelated"
	}
	doc := documentFromStrings(lines, "/")

	tests := []struct {
		name string
		move func(Pos) (Pos, bool)
		in   Pos
		want Pos
	}{
		{"next start", doc.NextWordStart, Pos{Row: 0, Col: 2}, Pos{Row: 0, Col: 6}},
		{"previous start", doc.PreviousWordStart, Pos{Row: 0, Col: 2}, Pos{Row: 0, Col: 0}},
		{"next end", doc.NextWordEnd, Pos{Row: 0, Col: 2}, Pos{Row: 0, Col: 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, func() {
				got, ok := tt.move(tt.in)
				if !ok || got != tt.want {
					t.Fatalf("move() = (%v, %t), want (%v, true)", got, ok, tt.want)
				}
			})
			require.Zero(t, allocs, "local word movement must not materialize the document")
		})
	}
}

func TestDocumentExtractRanges(t *testing.T) {
	wide := []renderer.Cell{{Rune: 'a'}, {Rune: '界'}, {Continuation: true}, {Rune: ' '}, {Rune: 'b'}}
	doc := NewDocument(NewSnapshotFromRows([][]renderer.Cell{documentCells("alpha  "), documentCells("beta"), wide, {}}, 7, 4), "")

	tests := []struct {
		name     string
		ranges   []CellRange
		linewise bool
		want     string
	}{
		{"inclusive partial multiline preserves spaces", []CellRange{{Row: 0, Start: 2, End: 6}, {Row: 1, Start: 0, End: 1}}, false, "pha  \nbe"},
		{"linewise trims trailing spaces", []CellRange{{Row: 0, Start: 0, End: 6}}, true, "alpha"},
		{"wide glyph emitted once", []CellRange{{Row: 2, Start: 1, End: 2}}, false, "界"},
		{"continuation endpoint normalizes to head", []CellRange{{Row: 2, Start: 2, End: 2}}, false, "界"},
		{"empty row", []CellRange{{Row: 3, Start: 0, End: 0}}, false, ""},
		{"out of range ignored", []CellRange{{Row: 8, Start: 0, End: 1}}, false, ""},
		{"no ranges", nil, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, doc.Extract(tt.ranges, tt.linewise))
		})
	}
}

var benchmarkDocumentWordPos Pos

func BenchmarkDocumentWordMovementLocal(b *testing.B) {
	lines := make([]string, 32_768)
	lines[0] = "alpha/beta"
	for i := 1; i < len(lines); i++ {
		lines[i] = "unrelated"
	}
	doc := documentFromStrings(lines, "/")
	moves := []func(Pos) (Pos, bool){
		doc.NextWordStart,
		doc.PreviousWordStart,
		doc.NextWordEnd,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, move := range moves {
			pos, ok := move(Pos{Row: 0, Col: 2})
			if !ok {
				b.Fatal("local word movement failed")
			}
			benchmarkDocumentWordPos = pos
		}
	}
}

func TestDocumentAccessorsExposeImmutableSnapshot(t *testing.T) {
	rows := [][]renderer.Cell{documentCells("one")}
	doc := NewDocument(NewSnapshotFromRows(rows, 9, 2), "_")
	rows[0][0].Rune = 'x'

	require.Equal(t, 1, doc.Len())
	require.Equal(t, 9, doc.Width())
	require.Equal(t, 2, doc.Height())
	require.Equal(t, "one", doc.LineText(0))
	require.Equal(t, doc.Snapshot().Row(0), doc.Row(0))
	require.Nil(t, doc.Row(1))
}
