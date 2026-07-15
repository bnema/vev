package copy

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

func row(text string) []renderer.Cell {
	cells := make([]renderer.Cell, 0, len([]rune(text)))
	for _, r := range text {
		cells = append(cells, renderer.Cell{Rune: r})
	}
	return cells
}
func modeFor(lines []string, height int) *Mode {
	rows := make([][]renderer.Cell, len(lines))
	for i := range lines {
		rows[i] = row(lines[i])
	}
	return NewMode(NewDocument(NewSnapshotFromRows(rows, 16, height), domain.DefaultWordSeparators))
}

func TestCopyModeKeyboardSelection(t *testing.T) {
	m := modeFor([]string{"alpha", "xy", "bravo", "charlie"}, 2)
	require.Equal(t, Pos{Row: 3}, m.Cursor())
	m.Top()
	m.ToggleLineSelection()
	m.Down()
	require.Equal(t, Line, m.Selection().Granularity)
	require.Equal(t, "alpha\nxy", m.SelectedText())
	m.Right()
	require.Equal(t, Character, m.Selection().Granularity)
	require.Equal(t, Pos{Row: 0}, m.Selection().Anchor)
	require.Equal(t, Pos{Row: 1, Col: 1}, m.Selection().Active)
	require.True(t, m.Left())
	require.Equal(t, Pos{Row: 1}, m.Cursor())
	require.True(t, m.Right())
	require.True(t, m.Up())
	require.Equal(t, Pos{Row: 0, Col: 1}, m.Cursor())
	require.True(t, m.Down())
	require.Equal(t, Pos{Row: 1, Col: 1}, m.Cursor())
	require.True(t, m.WordNext())
	require.Equal(t, Pos{Row: 2}, m.Cursor())
	require.True(t, m.WordEnd())
	require.Equal(t, Pos{Row: 2, Col: 4}, m.Cursor())
	require.True(t, m.WordBackward())
	require.Equal(t, Pos{Row: 2}, m.Cursor())
}

func TestCopyModePreferredColumnAndPartialSpaces(t *testing.T) {
	m := modeFor([]string{"abcdef", "xy", "abcdef"}, 2)
	require.True(t, m.SetPosition(Pos{Row: 0, Col: 5}))
	m.Down()
	require.Equal(t, Pos{Row: 1, Col: 1}, m.Cursor())
	m.Down()
	require.Equal(t, Pos{Row: 2, Col: 5}, m.Cursor())
	m.SetPosition(Pos{Row: 0, Col: 1})
	require.True(t, m.StartCharacterSelection(Pos{Row: 0, Col: 1}))
	require.True(t, m.ExtendCharacterSelection(Pos{Row: 0, Col: 4}))
	require.Equal(t, "bcde", m.SelectedText())
}

func TestCopyModeRenderSelectionWinsSearchAndWideGlyph(t *testing.T) {
	cells := []renderer.Cell{{Rune: '界'}, {Continuation: true}, {Rune: 'a'}}
	m := NewMode(NewDocument(NewSnapshotFromRows([][]renderer.Cell{cells}, 3, 1), ""))
	require.True(t, m.SetSearchMatches("界", []SearchMatch{{Row: 0, Start: 0, End: 2}}, 0))
	require.True(t, m.StartCharacterSelection(Pos{Row: 0, Col: 0}))
	f := m.Render(renderer.DefaultStyle(), renderer.Style{HasBackgroundRGB: true, BackgroundRGB: renderer.RGB{R: 1}})
	require.True(t, f.At(0, 0).Style.HasBackgroundRGB)
	require.True(t, f.At(1, 0).Style.HasBackgroundRGB)
}

func TestCopyModeSearchNavigationExtendsActiveSelection(t *testing.T) {
	matches := []SearchMatch{{Row: 1, Start: 1, End: 2}, {Row: 2, Start: 2, End: 3}}
	m := modeFor([]string{"zero", "one", "two"}, 3)

	m.SetPosition(Pos{Row: 0})
	m.ToggleLineSelection()
	require.True(t, m.SetSearchMatches("match", matches, 0))
	require.Equal(t, Selection{Anchor: Pos{Row: 0}, Active: Pos{Row: 1, Col: 1}, Granularity: Line, Enabled: true}, m.Selection())

	require.True(t, m.Right())
	require.Equal(t, Character, m.Selection().Granularity)
	require.True(t, m.NextSearchMatch(1))
	require.Equal(t, Selection{Anchor: Pos{Row: 0}, Active: Pos{Row: 2, Col: 2}, Granularity: Character, Enabled: true}, m.Selection())
}

func TestCopyModeSearchExtendsSelectionWhenMatchIsAlreadyAtCursor(t *testing.T) {
	m := modeFor([]string{"zero", "one"}, 2)
	require.True(t, m.StartCharacterSelection(Pos{Row: 0}))
	require.True(t, m.ExtendCharacterSelection(Pos{Row: 1, Col: 1}))
	require.True(t, m.SetPosition(Pos{Row: 0}))

	require.True(t, m.SetSearchMatches("zero", []SearchMatch{{Row: 0, Start: 0, End: 4}}, 0))
	require.Equal(t, Pos{Row: 0}, m.Selection().Active)
}

func TestCopyModeSearchNilMode(t *testing.T) {
	var mode *Mode
	require.False(t, mode.Search("anything"))
}

func TestCopyModeRenderBoundsCursorToRenderedRow(t *testing.T) {
	m := NewMode(NewDocument(NewSnapshotFromRows([][]renderer.Cell{row("alpha")}, 2, 1), ""))
	require.True(t, m.SetPosition(Pos{Row: 0, Col: 4}), "cursor remains valid in the document despite a narrow render frame")

	selection := renderer.Style{HasBackgroundRGB: true, BackgroundRGB: renderer.RGB{R: 1}}
	require.NotPanics(t, func() { _ = m.Render(renderer.DefaultStyle(), selection) })
	frame := m.Render(renderer.DefaultStyle(), selection)
	require.True(t, frame.At(1, 0).Style.HasBackgroundRGB, "a valid off-frame cursor is rendered at the visible edge")
}

func TestCopyModeRenderKeepsPassiveCursorOutsidePartialSameRowSelection(t *testing.T) {
	cells := []renderer.Cell{{Rune: '界'}, {Continuation: true}, {Rune: 'a'}, {Rune: 'b'}}
	m := NewMode(NewDocument(NewSnapshotFromRows([][]renderer.Cell{cells}, 4, 1), ""))
	require.True(t, m.StartCharacterSelection(Pos{Row: 0, Col: 2}))
	require.True(t, m.SetPosition(Pos{Row: 0, Col: 1})) // Normalize the wide-glyph continuation to its head.

	selection := renderer.Style{HasBackgroundRGB: true, BackgroundRGB: renderer.RGB{R: 1}}
	f := m.Render(renderer.DefaultStyle(), selection)
	require.True(t, f.At(0, 0).Style.HasBackgroundRGB)
	require.True(t, f.At(2, 0).Style.HasBackgroundRGB)
}

func TestFindMatchesUsesExclusiveDisplayCellOffsets(t *testing.T) {
	cells := []renderer.Cell{{Rune: '界'}, {Continuation: true}, {Rune: 'a'}, {Rune: 'l'}, {Rune: 'p'}, {Rune: 'h'}, {Rune: 'a'}}
	doc := NewDocument(NewSnapshotFromRows([][]renderer.Cell{cells}, 7, 1), "")
	require.Equal(t, []SearchMatch{{Row: 0, Start: 2, End: 7, Text: "界alpha"}}, FindMatches(doc, "alpha"))
}

func TestFindMatchesRepeatedUnicodeDisplayCells(t *testing.T) {
	cells := []renderer.Cell{
		{Rune: 'Ä'}, {Rune: '界'}, {Continuation: true},
		{Rune: 'ä'}, {Rune: '界'}, {Continuation: true},
		{Rune: 'Ä'}, {Rune: '界'}, {Continuation: true},
	}
	doc := NewDocument(NewSnapshotFromRows([][]renderer.Cell{row("aaaa"), cells}, 9, 2), "")

	for _, tt := range []struct {
		name, query string
		want        []SearchMatch
	}{
		{
			name:  "repeated non-overlapping",
			query: "aa",
			want: []SearchMatch{
				{Row: 0, Start: 0, End: 2, Text: "aaaa"},
				{Row: 0, Start: 2, End: 4, Text: "aaaa"},
			},
		},
		{
			name:  "unicode display cells",
			query: "Ä界",
			want: []SearchMatch{
				{Row: 1, Start: 0, End: 3, Text: "Ä界ä界Ä界"},
				{Row: 1, Start: 3, End: 6, Text: "Ä界ä界Ä界"},
				{Row: 1, Start: 6, End: 9, Text: "Ä界ä界Ä界"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, FindMatches(doc, tt.query))
		})
	}
}

func TestFindMatchesUsesSealedScrollbackWithoutGlobalCopy(t *testing.T) {
	const rows = 10_000
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: rows + 1, ChunkRows: 256})
	for range rows {
		history.Append(row("unmatched"))
	}
	history.Append(row("target"))
	snapshot := NewSnapshot(history, renderer.NewFrame(16, 1))
	view := history.SealAndView()
	require.Same(t, view.Chunk(0), snapshot.history.Chunk(0))

	doc := NewDocument(snapshot, "")
	require.Equal(t, []SearchMatch{{Row: rows, Start: 0, End: 6, Text: "target"}}, FindMatches(doc, "target"))
}

func TestCopyModeRenderSelectionAllocationsBoundedToViewport(t *testing.T) {
	const (
		rows      = 10_000
		height    = 4
		maxAllocs = 20
	)
	lines := make([][]renderer.Cell, rows)
	for i := range lines {
		lines[i] = row("selection")
	}
	m := NewMode(NewDocument(NewSnapshotFromRows(lines, 16, height), ""))
	require.True(t, m.StartCharacterSelection(Pos{}))
	require.True(t, m.ExtendCharacterSelection(Pos{Row: rows - 1, Col: 8}))

	allocs := testing.AllocsPerRun(10, func() {
		_ = m.Render()
	})
	require.LessOrEqual(t, allocs, float64(maxAllocs), "rendering a %d-line selection must only allocate for its %d-line viewport", rows, height)
}

func BenchmarkCopyModeRenderLargeSelection(b *testing.B) {
	const (
		rows   = 10_000
		height = 4
	)
	lines := make([][]renderer.Cell, rows)
	for i := range lines {
		lines[i] = row("selection")
	}
	m := NewMode(NewDocument(NewSnapshotFromRows(lines, 16, height), ""))
	if !m.StartCharacterSelection(Pos{}) || !m.ExtendCharacterSelection(Pos{Row: rows - 1, Col: 8}) {
		b.Fatal("could not create large selection")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = m.Render()
	}
}

func TestCopyModeOSC52SelectionMatrix(t *testing.T) {
	cases := []struct {
		name     string
		selectFn func(*Mode)
		want     string
	}{
		{"line", func(m *Mode) { m.SetPosition(Pos{}); m.ToggleLineSelection(); m.Down() }, "alpha\nbravo"},
		{"horizontal", func(m *Mode) { m.StartCharacterSelection(Pos{Col: 1}); m.ExtendCharacterSelection(Pos{Col: 3}) }, "lph"},
		{"multiline", func(m *Mode) { m.StartCharacterSelection(Pos{Col: 2}); m.ExtendCharacterSelection(Pos{Row: 1, Col: 1}) }, "pha\nbr"},
		{"reverse", func(m *Mode) { m.StartCharacterSelection(Pos{Row: 1, Col: 1}); m.ExtendCharacterSelection(Pos{Col: 2}) }, "pha\nbr"},
		{"word", func(m *Mode) { m.SelectWordAt(Pos{Row: 1, Col: 2}) }, "bravo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := modeFor([]string{"alpha", "bravo"}, 2)
			tc.selectFn(m)
			got := OSC52(m.SelectedText())
			require.Len(t, got, 1)
			want := []byte("\x1b]52;c;" + base64.StdEncoding.EncodeToString([]byte(tc.want)) + "\x07")
			require.True(t, bytes.Equal(want, got[0]))
		})
	}
}

func TestOSC52LimitsAndBase64(t *testing.T) {
	require.Nil(t, OSC52(strings.Repeat("x", OSC52MaxPayloadBytes+1)))
	encoded := base64.StdEncoding.EncodeToString([]byte("hello"))
	require.Equal(t, []byte("\x1b]52;c;"+encoded+"\x07"), OSC52FromBase64(encoded))
}
