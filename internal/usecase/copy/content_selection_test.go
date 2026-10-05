package copy

import (
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestSelectionContentBoundedRanges(t *testing.T) {
	tests := []struct {
		name      string
		rows      [][]vt.Cell
		bounds    []vt.LineBound
		selection Selection
		target    int
		want      CellRange
		text      string
	}{
		{"line", [][]vt.Cell{padRow("ab", 8)}, nil, Selection{Anchor: Pos{0, 0}, Active: Pos{0, 0}, Granularity: Line, Enabled: true}, 0, CellRange{Row: 0, End: 1}, "ab"},
		{"character middle", [][]vt.Cell{padRow("x", 8), padRow("ab", 8), padRow("z", 8)}, nil, Selection{Anchor: Pos{0, 0}, Active: Pos{2, 0}, Granularity: Character, Enabled: true}, 1, CellRange{Row: 1, End: 1}, "x\nab\nz"},
		{"character end in padding", [][]vt.Cell{padRow("ab", 8)}, nil, Selection{Anchor: Pos{0, 0}, Active: Pos{0, 6}, Granularity: Character, Enabled: true}, 0, CellRange{End: 1}, "ab"},
		{"soft trailing spaces", [][]vt.Cell{padRow("abc     ", 8), padRow("de", 8)}, []vt.LineBound{{End: 8, Soft: true}, {End: 2}}, Selection{Anchor: Pos{0, 0}, Active: Pos{1, 1}, Granularity: Character, Enabled: true}, 0, CellRange{End: 7}, "abc     de"},
		{"soft partial bound", [][]vt.Cell{padRow("abc", 8), padRow("de", 8)}, []vt.LineBound{{End: 5, Soft: true}, {End: 2}}, Selection{Anchor: Pos{0, 0}, Active: Pos{1, 1}, Granularity: Character, Enabled: true}, 0, CellRange{End: 4}, "abc  de"},
		{"blank middle", [][]vt.Cell{padRow("ab", 8), padRow("", 8), padRow("cd", 8)}, nil, Selection{Anchor: Pos{0, 0}, Active: Pos{2, 1}, Granularity: Character, Enabled: true}, 1, CellRange{Row: 1, End: -1}, "ab\n\ncd"},
		{"anchor in padding", [][]vt.Cell{padRow("ab", 8)}, nil, Selection{Anchor: Pos{0, 5}, Active: Pos{0, 6}, Granularity: Character, Enabled: true}, 0, CellRange{Start: 5, End: 4}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := NewDocument(NewSnapshotFromLines(tt.rows, tt.bounds, 8, len(tt.rows)), "")
			got, ok := tt.selection.RangeForRow(doc, tt.target)
			require.True(t, ok)
			require.Equal(t, tt.want, got)
			require.Contains(t, tt.selection.Ranges(doc), got)
			require.Equal(t, tt.text, tt.selection.Text(doc))
		})
	}
}

func TestDocumentExtractSelectedEmptyRanges(t *testing.T) {
	doc := NewDocument(NewSnapshotFromRows([][]vt.Cell{padRow("ab", 8), padRow("", 8), padRow("cd", 8)}, 8, 3), "")
	tests := []struct {
		name   string
		ranges []CellRange
		want   string
	}{
		{"empty", []CellRange{{Row: 0, End: -1}}, ""},
		{"blank middle", []CellRange{{Row: 0, End: 1}, {Row: 1, End: -1}, {Row: 2, End: 1}}, "ab\n\ncd"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, doc.Extract(tt.ranges)) })
	}
}
