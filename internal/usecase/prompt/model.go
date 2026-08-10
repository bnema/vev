package prompt

import (
	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
)

// RenderStyles contains daemon-supplied semantic chrome roles. The zero value
// is intentionally not used by RenderStyled; isolated Render callers retain
// the historic neutral defaults.
type RenderStyles struct {
	Base      renderer.Style
	Selection renderer.Style
}

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
	base := renderer.DefaultStyle()
	selection := base
	selection.Inverse = true
	if len(accentStyle) > 0 {
		selection = accentStyle[0]
	}
	return m.RenderStyled(inner, RenderStyles{Base: base, Selection: selection})
}

// RenderStyled renders with explicit chrome roles captured by the daemon.
func (m *Model) RenderStyled(inner domain.Size, styles RenderStyles) renderer.Frame {
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	if frame.Width == 0 || frame.Height == 0 {
		return frame
	}
	ui.FillRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, renderer.Cell{Rune: ' ', Style: styles.Base})
	ui.DrawInputLine(frame, 0, "> ", m.Value(), styles.Base, styles.Selection)
	if frame.Height > 1 && m != nil && m.errMsg != "" {
		ui.DrawText(frame, 0, 1, frame.Width, m.errMsg, styles.Selection)
	}
	return frame
}
