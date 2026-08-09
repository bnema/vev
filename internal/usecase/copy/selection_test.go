package copy

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestSelectionRanges(t *testing.T) {
	doc := documentFromStrings([]string{"alpha", "bravo", "charlie"}, " -_@")

	tests := []struct {
		name      string
		selection Selection
		want      []CellRange
	}{
		{
			name:      "line forward",
			selection: Selection{Anchor: Pos{0, 2}, Active: Pos{1, 1}, Granularity: Line, Enabled: true},
			want:      []CellRange{{Row: 0, Start: 0, End: 4}, {Row: 1, Start: 0, End: 4}},
		},
		{
			name:      "line reverse",
			selection: Selection{Anchor: Pos{1, 1}, Active: Pos{0, 2}, Granularity: Line, Enabled: true},
			want:      []CellRange{{Row: 0, Start: 0, End: 4}, {Row: 1, Start: 0, End: 4}},
		},
		{
			name:      "character forward",
			selection: Selection{Anchor: Pos{0, 2}, Active: Pos{1, 1}, Granularity: Character, Enabled: true},
			want:      []CellRange{{Row: 0, Start: 2, End: 4}, {Row: 1, Start: 0, End: 1}},
		},
		{
			name:      "character reverse",
			selection: Selection{Anchor: Pos{1, 1}, Active: Pos{0, 2}, Granularity: Character, Enabled: true},
			want:      []CellRange{{Row: 0, Start: 2, End: 4}, {Row: 1, Start: 0, End: 1}},
		},
		{
			name:      "word forward",
			selection: Selection{Anchor: Pos{0, 2}, Active: Pos{1, 1}, Granularity: Word, Enabled: true},
			want:      []CellRange{{Row: 0, Start: 0, End: 4}, {Row: 1, Start: 0, End: 4}},
		},
		{
			name:      "word reverse",
			selection: Selection{Anchor: Pos{1, 1}, Active: Pos{0, 2}, Granularity: Word, Enabled: true},
			want:      []CellRange{{Row: 0, Start: 0, End: 4}, {Row: 1, Start: 0, End: 4}},
		},
		{name: "disabled", selection: Selection{}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.selection.Ranges(doc))
		})
	}
}

func TestSelectionRangeForRowMatchesCanonicalRanges(t *testing.T) {
	wide := []renderer.Cell{{Rune: 'a'}, {Rune: '界'}, {Continuation: true}, {Rune: 'b'}}
	doc := NewDocument(NewSnapshotFromRows([][]renderer.Cell{row("alpha"), wide, row("bravo")}, 5, 3), " -_@")
	tests := []struct {
		name      string
		selection Selection
		row       int
		want      CellRange
		ok        bool
	}{
		{"line reverse", Selection{Anchor: Pos{2, 1}, Active: Pos{0, 2}, Granularity: Line, Enabled: true}, 1, CellRange{Row: 1, End: 3}, true},
		{"character wide", Selection{Anchor: Pos{0, 2}, Active: Pos{1, 1}, Granularity: Character, Enabled: true}, 1, CellRange{Row: 1, End: 2}, true},
		{"word", Selection{Anchor: Pos{0, 2}, Active: Pos{2, 1}, Granularity: Word, Enabled: true}, 1, CellRange{Row: 1, End: 3}, true},
		{"outside selection", Selection{Anchor: Pos{0, 2}, Active: Pos{1, 1}, Granularity: Character, Enabled: true}, 2, CellRange{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.selection.RangeForRow(doc, tt.row)
			require.Equal(t, tt.ok, ok)
			if ok {
				require.Equal(t, tt.want, got)
				require.Contains(t, tt.selection.Ranges(doc), got)
			}
		})
	}
}

func TestSelectionOrdered(t *testing.T) {
	tests := []struct {
		name      string
		selection Selection
		start     Pos
		end       Pos
	}{
		{"forward", Selection{Anchor: Pos{0, 2}, Active: Pos{1, 1}}, Pos{0, 2}, Pos{1, 1}},
		{"reverse", Selection{Anchor: Pos{1, 1}, Active: Pos{0, 2}}, Pos{0, 2}, Pos{1, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := tt.selection.Ordered()
			require.Equal(t, tt.start, start)
			require.Equal(t, tt.end, end)
		})
	}
}

func TestSelectionRangesEmptyRowsAndWideGlyphs(t *testing.T) {
	wide := []renderer.Cell{{Rune: 'a'}, {Rune: '界'}, {Continuation: true}, {Rune: 'b'}}
	doc := NewDocument(NewSnapshotFromRows([][]renderer.Cell{{}, wide, {}}, 4, 3), " -_@")

	tests := []struct {
		name      string
		selection Selection
		want      []CellRange
	}{
		{
			name:      "line includes empty rows",
			selection: Selection{Anchor: Pos{0, 0}, Active: Pos{2, 0}, Granularity: Line, Enabled: true},
			want:      []CellRange{{Row: 0, Start: 0, End: 0}, {Row: 1, Start: 0, End: 3}, {Row: 2, Start: 0, End: 0}},
		},
		{
			name:      "character expands wide glyph continuation",
			selection: Selection{Anchor: Pos{1, 1}, Active: Pos{1, 2}, Granularity: Character, Enabled: true},
			want:      []CellRange{{Row: 1, Start: 1, End: 2}},
		},
		{
			name:      "character crosses empty row",
			selection: Selection{Anchor: Pos{0, 0}, Active: Pos{1, 1}, Granularity: Character, Enabled: true},
			want:      []CellRange{{Row: 0, Start: 0, End: 0}, {Row: 1, Start: 0, End: 2}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.selection.Ranges(doc))
		})
	}
}

func TestWordSelectionRejectsSeparator(t *testing.T) {
	doc := documentFromStrings([]string{"alpha/beta"}, "/")

	selection, ok := NewWordSelection(doc, Pos{0, 5})
	require.False(t, ok)
	require.False(t, selection.Enabled)
}

func TestSelectionConstructorsAndMutators(t *testing.T) {
	doc := documentFromStrings([]string{"alpha", "bravo"}, " -_@")

	selection := NewLineSelection(Pos{0, 2})
	require.True(t, selection.Enabled)
	require.Equal(t, Line, selection.Granularity)
	require.Equal(t, Pos{0, 2}, selection.Anchor)

	selection.Extend(Pos{1, 1})
	selection.AsCharacter()
	require.Equal(t, Character, selection.Granularity)
	require.Equal(t, "pha\nbr", selection.Text(doc))

	word, ok := NewWordSelection(doc, Pos{1, 2})
	require.True(t, ok)
	require.Equal(t, "bravo", word.Text(doc))
	word.Extend(Pos{0, 2})
	require.Equal(t, "alpha\nbravo", word.Text(doc))
}
