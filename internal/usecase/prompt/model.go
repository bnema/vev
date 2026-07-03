package prompt

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

type Model struct {
	title  string
	text   ui.TextInput
	errMsg string
}

func New(title, initial string) *Model {
	m := &Model{title: title}
	m.text.SetValue(initial)
	return m
}

func (m *Model) Insert(r rune) {
	if m == nil {
		return
	}
	m.errMsg = ""
	m.text.Insert(r)
}

func (m *Model) Backspace() {
	if m == nil {
		return
	}
	m.errMsg = ""
	m.text.Backspace()
}

func (m *Model) Value() string {
	if m == nil {
		return ""
	}
	return m.text.Value()
}

func (m *Model) Title() string {
	if m == nil {
		return ""
	}
	return m.title
}

func (m *Model) SetError(msg string) {
	if m != nil {
		m.errMsg = msg
	}
}

func (m *Model) Render(inner domain.Size, accentStyle ...renderer.Style) renderer.Frame {
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	if frame.Width == 0 || frame.Height == 0 {
		return frame
	}
	base := renderer.DefaultStyle()
	accent := base
	accent.Inverse = true
	if len(accentStyle) > 0 {
		accent = accentStyle[0]
	}
	ui.FillRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, renderer.Cell{Rune: ' ', Style: base})
	ui.DrawInputLine(frame, 0, "> ", m.Value(), base, accent)
	if frame.Height > 1 && m != nil && m.errMsg != "" {
		ui.DrawText(frame, 0, 1, frame.Width, m.errMsg, accent)
	}
	return frame
}
