package copy

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
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

func TestNewSnapshotFromRowsPreservesWideRows(t *testing.T) {
	wide := make([]renderer.Cell, 321)
	for i := range wide {
		wide[i] = renderer.Cell{Rune: rune('a' + i%26)}
	}
	rows := [][]renderer.Cell{wide, row("tail")}

	snapshot := NewSnapshotFromRows(rows, len(wide), len(rows))

	require.Equal(t, len(rows), snapshot.history.Len())
	for i, want := range rows {
		require.Equal(t, want, snapshot.history.Row(i))
	}
}

func TestSnapshotCarriesImmutableRowIDsAndDocumentsResolveBookmarks(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 8, MaxCells: 1024})
	require.NoError(t, history.AppendWithID(row("hist"), vt.LineBound{End: 4}, vt.RowID(11)))
	require.NoError(t, history.AppendWithID(row("tail"), vt.LineBound{End: 4}, vt.RowID(12)))
	screen := renderer.NewFrame(4, 2)
	copy(screen.Row(0), row("live"))
	copy(screen.Row(1), row("more"))
	rowIDs := []vt.RowID{21, 22}

	snapshot := NewSnapshot(history, screen, []vt.LineBound{{End: 4}, {End: 4}}, rowIDs)
	rowIDs[0] = 99
	doc := NewDocument(snapshot, "")

	require.Equal(t, []vt.RowID{11, 12, 21, 22}, snapshot.RowIDs())
	ids := snapshot.RowIDs()
	ids[0] = 100
	require.Equal(t, vt.RowID(11), snapshot.RowID(0))
	require.Equal(t, vt.RowID(21), doc.RowID(2))
	require.Equal(t, 2, doc.FindRowID(vt.RowID(21)))
	require.Equal(t, 3, doc.FindRowID(vt.RowID(22)))
	require.Equal(t, -1, doc.FindRowID(vt.RowID(99)))
}

func TestSnapshotBoundDispatchesLikeRow(t *testing.T) {
	row := func(s string) []renderer.Cell {
		cells := make([]renderer.Cell, 0, len(s))
		for _, r := range s {
			cells = append(cells, renderer.Cell{Rune: r})
		}
		return cells
	}

	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 8, MaxCells: 1024})
	if err := history.Append(row("hist"), vt.LineBound{End: 4, Soft: true}); err != nil {
		t.Fatalf("append: %v", err)
	}
	screen := renderer.NewFrame(4, 2)
	copy(screen.Row(0), row("live"))

	snap := NewSnapshot(history, screen, []vt.LineBound{{End: 4}, {End: 0, Soft: true}}, nil)

	tests := []struct {
		name string
		i    int
		want vt.LineBound
	}{
		{name: "history row", i: 0, want: vt.LineBound{End: 4, Soft: true}},
		{name: "first screen row", i: 1, want: vt.LineBound{End: 4}},
		{name: "second screen row", i: 2, want: vt.LineBound{End: 0, Soft: true}},
		{name: "past the end", i: 3, want: vt.LineBound{}},
		{name: "negative", i: -1, want: vt.LineBound{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := snap.Bound(tc.i); got != tc.want {
				t.Errorf("Bound(%d) = %+v, want %+v", tc.i, got, tc.want)
			}
		})
	}
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
	unmatched, target := row("unmatched"), row("target")
	for range rows {
		require.NoError(t, history.Append(unmatched, vt.LineBound{End: len(unmatched)}))
	}
	require.NoError(t, history.Append(target, vt.LineBound{End: len(target)}))
	snapshot := NewSnapshot(history, renderer.NewFrame(16, 1), nil, nil)
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

func TestCopyModeRenderStylesStatusFiller(t *testing.T) {
	m := modeFor([]string{"alpha"}, 5)
	status := renderer.Style{Foreground: 1, Background: 2}
	selection := renderer.Style{Foreground: 3, Background: 4}
	frame := m.Render(status, selection)

	require.True(t, frame.At(4, frame.Height-1).Style.Equal(status), "status filler keeps cached status surface")
}

func TestSelectedTextJoinsAcrossTheHistoryScreenBoundary(t *testing.T) {
	row := func(s string, w int) []renderer.Cell {
		cells := make([]renderer.Cell, w)
		for i := range cells {
			cells[i] = renderer.BlankCell()
		}
		for i, r := range []rune(s) {
			if i >= w {
				break
			}
			cells[i] = renderer.Cell{Rune: r}
		}
		return cells
	}

	const width = 4
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 8, MaxCells: 1024})
	// The last history row wrapped into what is now the first live screen row.
	if err := history.Append(row("abcd", width), vt.LineBound{End: width, Soft: true}); err != nil {
		t.Fatalf("append: %v", err)
	}
	screen := renderer.NewFrame(width, 2)
	copy(screen.Row(0), row("ef", width))
	copy(screen.Row(1), row("gh", width))

	snapshot := NewSnapshot(history, screen, []vt.LineBound{{End: 2}, {End: 2}}, nil)
	doc := NewDocument(snapshot, "")

	ranges := []CellRange{
		{Row: 0, Start: 0, End: width - 1},
		{Row: 1, Start: 0, End: width - 1},
		{Row: 2, Start: 0, End: width - 1},
	}
	const want = "abcdef\ngh"
	if got := doc.Extract(ranges); got != want {
		t.Errorf("Extract() = %q, want %q", got, want)
	}
}

// TestSelectedTextJoinsRowsWrappedByTheVT drives the whole chain with no
// hand-written bounds: a real vt.Screen wraps one logical line itself, its
// LineBound travels through eviction into history or stays on the live grid,
// and Extract must give the line back exactly as the application printed it.
func TestSelectedTextJoinsRowsWrappedByTheVT(t *testing.T) {
	const (
		width  = 8
		height = 4
		line   = "abcdefghij"
	)
	cases := []struct {
		name string
		// input is written verbatim to the VT; the wrap and every scroll it
		// implies are the emulator's own decisions.
		input string
		// wantHistoryLen pins where the history/screen seam falls, so a change
		// in scroll behavior fails loudly instead of silently retargeting the
		// case onto rows the seam no longer separates.
		wantHistoryLen int
		// wrapRow is the first snapshot row of the wrapped logical line.
		wrapRow int
	}{
		{
			name:           "wrapped line straddles the history and screen seam",
			input:          line + "\r\nx\r\ny\r\n",
			wantHistoryLen: 1,
			wrapRow:        0,
		},
		{
			name:           "wrapped line sits entirely in sealed history",
			input:          line + "\r\nx\r\ny\r\nz\r\n",
			wantHistoryLen: 2,
			wrapRow:        0,
		},
		{
			name:           "wrapped line sits entirely on the live screen",
			input:          "1\r\n2\r\n3\r\n4\r\n5\r\n\x1b[2;1H" + line,
			wantHistoryLen: 2,
			wrapRow:        3,
		},
		{
			// The common case: the pane is full, so the cursor is on the last
			// row and the wrap scrolls the screen as it happens. This is what a
			// long line printed at a shell prompt actually does.
			name:           "wrapped line wraps out of the bottom row of a full pane",
			input:          "1\r\n2\r\n3\r\n4\r\n5\r\n" + line,
			wantHistoryLen: 3,
			wrapRow:        5,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			screen := vt.NewScreenWithHistory(width, height, vt.HistoryConfig{MaxRows: 64, MaxCells: 4096})
			screen.Write([]byte(tc.input))

			snapshot := NewSnapshot(screen.History(), screen.Frame, screen.LineBounds(), nil)
			require.Equal(t, tc.wantHistoryLen, snapshot.history.Len(), "history/screen seam moved")
			doc := NewDocument(snapshot, domain.DefaultWordSeparators)

			got := doc.Extract([]CellRange{
				{Row: tc.wrapRow, Start: 0, End: width - 1},
				{Row: tc.wrapRow + 1, Start: 0, End: width - 1},
			})
			require.Equal(t, line, got, "the two physical rows must rejoin without a newline or grid padding")
		})
	}
}
