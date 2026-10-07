package ui

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
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

func TestDrawInputLineDrawsPrefixAndValueWithoutCaret(t *testing.T) {
	frame := renderer.NewFrame(6, 1)
	style := renderer.DefaultStyle()

	DrawInputLine(frame, 0, "> ", "abc", style)

	require.Equal(t, '>', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(1, 0).Rune)
	require.Equal(t, 'a', frame.At(2, 0).Rune)
	require.Equal(t, 'b', frame.At(3, 0).Rune)
	require.Equal(t, 'c', frame.At(4, 0).Rune)
	require.False(t, frame.At(5, 0).Style.Inverse, "the real terminal cursor is the caret")
}

func TestInputCaret(t *testing.T) {
	for _, tt := range []struct {
		name          string
		width, rows   int
		prefix, value string
		wantCol       int
		wantOK        bool
	}{
		{name: "after value", width: 6, rows: 1, prefix: "> ", value: "abc", wantCol: 5, wantOK: true},
		{name: "empty value", width: 6, rows: 1, prefix: "/", wantCol: 1, wantOK: true},
		{name: "wide runes count two columns", width: 8, rows: 1, prefix: "> ", value: "日本", wantCol: 6, wantOK: true},
		{name: "value fills the line", width: 5, rows: 1, prefix: "> ", value: "abc", wantCol: 5, wantOK: false},
		{name: "value clipped", width: 4, rows: 1, prefix: "> ", value: "abcdef", wantCol: 8, wantOK: false},
		{name: "no row", width: 6, rows: 0, prefix: "> ", value: "a", wantCol: 3, wantOK: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			col, ok := InputCaret(domain.Size{Cols: tt.width, Rows: tt.rows}, tt.prefix, tt.value)
			require.Equal(t, tt.wantCol, col)
			require.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestDrawInputLineIgnoresInvalidRow(t *testing.T) {
	frame := renderer.NewFrame(4, 1)

	DrawInputLine(frame, 2, "> ", "abc", renderer.DefaultStyle())

	require.Equal(t, ' ', frame.At(0, 0).Rune)
}
