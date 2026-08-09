package prompt

import (
	"strings"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestModelPrefillAndEditingClearsError(t *testing.T) {
	m := New(" Rename session ", "0")
	require.Equal(t, " Rename session ", m.Title())
	require.Equal(t, "0", m.Value())

	m.SetError("name already in use")
	m.Insert('x')
	require.Equal(t, "0x", m.Value())
	require.Empty(t, m.errMsg)

	m.SetError("bad")
	m.Backspace()
	require.Equal(t, "0", m.Value())
	require.Empty(t, m.errMsg)
}

func TestModelRenderDrawsInputAndError(t *testing.T) {
	m := New(" Rename session ", "work")
	m.SetError("name already in use")

	frame := m.Render(domain.Size{Cols: 24, Rows: 2})

	require.Equal(t, "> work                  ", promptRowText(frame.Row(0)))
	require.True(t, frame.At(6, 0).Style.Inverse, "caret follows prefilled value")
	require.Equal(t, "name already in use     ", promptRowText(frame.Row(1)))
	require.True(t, frame.At(0, 1).Style.Inverse, "error line is visually distinct")
}

func promptRowText(row []renderer.Cell) string {
	var b strings.Builder
	for _, c := range row {
		if c.Rune == 0 {
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(c.Rune)
	}
	return b.String()
}

func TestModelRenderHandlesTinySize(t *testing.T) {
	m := New(" Rename session ", "work")

	frame := m.Render(domain.Size{Cols: 0, Rows: 0})

	require.Equal(t, 0, frame.Width)
	require.Equal(t, 0, frame.Height)
}

func TestModelRenderStyledFillsBaseAndSelection(t *testing.T) {
	m := New(" Rename ", "x")
	base := renderer.Style{Foreground: 1, Background: 2}
	selection := renderer.Style{Foreground: 3, Background: 4}
	frame := m.RenderStyled(domain.Size{Cols: 12, Rows: 2}, RenderStyles{Base: base, Selection: selection})

	require.True(t, frame.At(11, 1).Style.Equal(base), "blank filler keeps base surface")
	require.True(t, frame.At(3, 0).Style.Equal(selection), "caret keeps selection surface")
}
