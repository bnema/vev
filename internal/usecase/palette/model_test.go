package palette

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestModelInsertBackspaceAndSelectionClamp(t *testing.T) {
	m := New([]command.Command{
		cmd("ABC", "Alpha", "first"),
		cmd("DEF", "Delta", "second"),
		cmd("AXY", "Other", "third"),
	})

	require.Equal(t, "", m.Query())
	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, "ABC", selected.Code)

	m.Down()
	m.Down()
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "AXY", selected.Code)

	m.Insert('d')
	require.Equal(t, "d", m.Query())
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "DEF", selected.Code, "selection clamps to only match after query changes")

	m.Up()
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "DEF", selected.Code, "up at first match clamps")

	m.Backspace()
	require.Equal(t, "", m.Query())
	require.Len(t, m.Matches(), 3)
}

func TestModelNoMatchesClearsSelection(t *testing.T) {
	m := New([]command.Command{cmd("ABC", "Alpha", "first")})
	m.Insert('z')

	_, ok := m.Selected()
	require.False(t, ok)
	require.Empty(t, m.Matches())
}

func TestModelMatchesDeepCopiesPositions(t *testing.T) {
	m := New([]command.Command{cmd("ABC", "Alpha", "first")})
	m.Insert('a')

	matches := m.Matches()
	require.Len(t, matches, 1)
	require.Equal(t, []int{0}, matches[0].Positions)

	matches[0].Positions[0] = 2

	fresh := m.Matches()
	require.Equal(t, []int{0}, fresh[0].Positions)
}

func TestRenderDrawsInputCaretSelectedLineAndCodeHighlight(t *testing.T) {
	m := New([]command.Command{
		cmd("CPY", "Copy", "Enter copy mode"),
		cmd("CNT", "New", "Create tab"),
	})
	m.Insert('c')
	m.Insert('y')
	frame := m.Render(domain.Size{Cols: 28, Rows: 3})

	require.Equal(t, '>', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(1, 0).Rune)
	require.Equal(t, 'c', frame.At(2, 0).Rune)
	require.Equal(t, 'y', frame.At(3, 0).Rune)
	require.True(t, frame.At(4, 0).Style.Inverse, "caret is reverse-video after query")

	require.Equal(t, 'C', frame.At(0, 1).Rune)
	require.Equal(t, 'P', frame.At(1, 1).Rune)
	require.Equal(t, 'Y', frame.At(2, 1).Rune)
	require.True(t, frame.At(0, 1).Style.Inverse, "selected line is inverse")
	require.True(t, frame.At(5, 1).Style.Inverse, "selected line padding/text remains inverse")
	require.True(t, frame.At(0, 1).Style.Bold, "matched C is bold")
	require.False(t, frame.At(1, 1).Style.Bold, "unmatched P is not highlighted")
	require.True(t, frame.At(2, 1).Style.Bold, "matched Y is bold")
}

func TestRenderUsesSelectionStyleForFuzzyHighlights(t *testing.T) {
	m := New([]command.Command{cmd("CPY", "Copy", "Enter copy mode")})
	m.Insert('c')
	m.Insert('y')
	accent := renderer.DefaultStyle()
	accent.HasBackgroundRGB = true
	accent.BackgroundRGB = renderer.RGB{R: 1, G: 2, B: 3}

	frame := m.Render(domain.Size{Cols: 28, Rows: 3}, accent)

	matched := frame.At(0, 1).Style
	require.True(t, matched.Bold)
	require.False(t, matched.Inverse)
	require.True(t, matched.HasBackgroundRGB)
	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, matched.BackgroundRGB)
}
