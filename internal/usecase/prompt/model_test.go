package prompt

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
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
