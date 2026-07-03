package ui

import "github.com/bnema/vev/pkg/renderer"

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

// DrawInputLine draws prefix + value followed by a reverse-video caret cell.
func DrawInputLine(f renderer.Frame, y int, prefix, value string, style renderer.Style) {
	if y < 0 || y >= f.Height {
		return
	}
	x := DrawText(f, 0, y, f.Width, prefix+value, style)
	if x >= 0 && x < f.Width {
		caret := style
		caret.Inverse = true
		f.Set(x, y, renderer.Cell{Rune: ' ', Style: caret})
	}
}
