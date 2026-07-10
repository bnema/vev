package palette

import (
	"fmt"
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

func TestRenderDrawsOnlyCodeAndDescriptionWithStyles(t *testing.T) {
	m := New([]command.Command{
		cmd("CPY", "Copy", "Enter copy mode"),
		cmd("CNT", "New", "Create tab"),
	})
	m.Insert('c')
	m.Insert('y')
	frame := m.Render(domain.Size{Cols: 28, Rows: 3}, DefaultRenderStyles())

	require.Equal(t, '>', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(1, 0).Rune)
	require.Equal(t, 'c', frame.At(2, 0).Rune)
	require.Equal(t, 'y', frame.At(3, 0).Rune)
	require.True(t, frame.At(4, 0).Style.Inverse, "caret is reverse-video after query")

	require.Equal(t, "CPY Enter copy mode         ", frameRow(frame, 1))
	require.NotContains(t, frameRow(frame, 1), "Copy")
	require.NotContains(t, frameRow(frame, 1), "—")
	require.True(t, frame.At(0, 1).Style.Inverse, "selected line is inverse")
	require.True(t, frame.At(5, 1).Style.Inverse, "selected line padding/text remains inverse")
	require.True(t, frame.At(0, 1).Style.Bold, "command code C is bold")
	require.True(t, frame.At(1, 1).Style.Bold, "command code P is bold")
	require.True(t, frame.At(2, 1).Style.Bold, "command code Y is bold")
	require.True(t, frame.At(4, 1).Style.Italic, "description is italic")
}

func TestRenderSafelyClipsVisibleFieldsAtNarrowWidths(t *testing.T) {
	m := New([]command.Command{cmd("CPY", "Copy", "Enter copy mode")})
	want := []rune("CPY Enter copy mode")

	for _, cols := range []int{0, 1, 3, 4, 7} {
		t.Run(fmt.Sprintf("cols_%d", cols), func(t *testing.T) {
			frame := m.Render(domain.Size{Cols: cols, Rows: 2}, DefaultRenderStyles())
			require.Equal(t, cols, frame.Width)
			if cols > 0 {
				require.Equal(t, want[:min(cols, len(want))], []rune(frameRow(frame, 1)))
			}
		})
	}
}

func frameRow(frame renderer.Frame, y int) string {
	row := make([]rune, frame.Width)
	for x := range frame.Width {
		row[x] = frame.At(x, y).Rune
	}
	return string(row)
}

func TestRenderUsesConfiguredStyles(t *testing.T) {
	tests := []struct {
		name   string
		styles func() RenderStyles
		x      int
		assert func(t *testing.T, style renderer.Style)
	}{
		{
			name: "selection style for fuzzy highlights",
			styles: func() RenderStyles {
				accent := renderer.DefaultStyle()
				accent.HasBackgroundRGB = true
				accent.BackgroundRGB = renderer.RGB{R: 1, G: 2, B: 3}
				styles := DefaultRenderStyles()
				styles.Selection = accent
				return styles
			},
			x: 0,
			assert: func(t *testing.T, style renderer.Style) {
				require.True(t, style.Bold)
				require.False(t, style.Inverse)
				require.True(t, style.HasBackgroundRGB)
				require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, style.BackgroundRGB)
			},
		},
		{
			name: "muted italic description style",
			styles: func() RenderStyles {
				muted := renderer.DefaultStyle()
				muted.Italic = true
				muted.HasForegroundRGB = true
				muted.ForegroundRGB = renderer.RGB{R: 10, G: 20, B: 30}
				styles := DefaultRenderStyles()
				styles.Description = muted
				return styles
			},
			x: 4,
			assert: func(t *testing.T, style renderer.Style) {
				require.True(t, style.Italic)
				require.True(t, style.HasForegroundRGB)
				require.Equal(t, -1, style.Foreground)
				require.Equal(t, renderer.RGB{R: 10, G: 20, B: 30}, style.ForegroundRGB)
			},
		},
		{
			name: "description italic is configurable",
			styles: func() RenderStyles {
				styles := DefaultRenderStyles()
				styles.Description.Italic = false
				return styles
			},
			x: 4,
			assert: func(t *testing.T, style renderer.Style) {
				require.False(t, style.Italic)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New([]command.Command{cmd("CPY", "Copy", "Enter copy mode")})
			m.Insert('c')
			m.Insert('y')
			frame := m.Render(domain.Size{Cols: 28, Rows: 3}, tt.styles())

			tt.assert(t, frame.At(tt.x, 1).Style)
		})
	}
}
