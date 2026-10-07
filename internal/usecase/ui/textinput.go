package ui

import renderer "github.com/bnema/vev-vt"

// TextInput stores editable text as runes for terminal overlay input models.
type TextInput struct {
	runes []rune
}

func (t *TextInput) Insert(r rune) { t.runes = append(t.runes, r) }

func (t *TextInput) Backspace() {
	if len(t.runes) == 0 {
		return
	}
	t.runes = t.runes[:len(t.runes)-1]
}

func (t *TextInput) Value() string { return string(t.runes) }

func (t *TextInput) SetValue(value string) { t.runes = []rune(value) }

// DrawInputLine draws prefix + value. The caret is not drawn: the daemon
// places the real terminal cursor at InputCaret, so client predictive echo
// can type ahead on the input line.
func DrawInputLine(f renderer.Frame, y int, prefix, value string, style renderer.Style) {
	if y < 0 || y >= f.Height {
		return
	}
	DrawText(f, 0, y, f.Width, prefix+value, style)
}

// InputCaret is the caret column of an input line drawn by DrawInputLine in a
// frame width columns wide. It reports false when prefix + value does not
// leave room for the caret, so a clipped line never shows a misplaced cursor.
func InputCaret(width int, prefix, value string) (int, bool) {
	col := 0
	for _, r := range prefix + value {
		col += renderer.RuneWidth(r)
	}
	return col, col < width
}
