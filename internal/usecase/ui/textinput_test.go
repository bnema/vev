package ui

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestTextInputEditsValue(t *testing.T) {
	var input TextInput

	input.Insert('w')
	input.Insert('ø')
	input.Insert('k')
	require.Equal(t, "wøk", input.Value())

	input.Backspace()
	require.Equal(t, "wø", input.Value())

	input.SetValue("ship")
	require.Equal(t, "ship", input.Value())
}

func TestDrawInputLineDrawsPrefixValueAndCaret(t *testing.T) {
	frame := renderer.NewFrame(6, 1)
	style := renderer.DefaultStyle()

	DrawInputLine(frame, 0, "> ", "abc", style)

	require.Equal(t, '>', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(1, 0).Rune)
	require.Equal(t, 'a', frame.At(2, 0).Rune)
	require.Equal(t, 'b', frame.At(3, 0).Rune)
	require.Equal(t, 'c', frame.At(4, 0).Rune)
	require.Equal(t, ' ', frame.At(5, 0).Rune)
	require.True(t, frame.At(5, 0).Style.Inverse)
}

func TestDrawInputLineClipsCaret(t *testing.T) {
	frame := renderer.NewFrame(4, 1)
	style := renderer.DefaultStyle()

	DrawInputLine(frame, 0, "> ", "abcdef", style)

	require.Equal(t, '>', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(1, 0).Rune)
	require.Equal(t, 'a', frame.At(2, 0).Rune)
	require.Equal(t, 'b', frame.At(3, 0).Rune)
	require.False(t, frame.At(3, 0).Style.Inverse, "caret beyond clip should not overwrite final value cell")
}

func TestDrawInputLineIgnoresInvalidRow(t *testing.T) {
	frame := renderer.NewFrame(4, 1)

	DrawInputLine(frame, 2, "> ", "abc", renderer.DefaultStyle())

	require.Equal(t, ' ', frame.At(0, 0).Rune)
}
