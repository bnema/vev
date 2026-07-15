package copy

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
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
