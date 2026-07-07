package palette

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

type RenderStyles struct {
	Selection   renderer.Style
	Description renderer.Style
}

type Model struct {
	commands []command.Command
	input    ui.TextInput
	matches  []Match
	selected int
	scroll   int
}

func New(commands []command.Command) *Model {
	m := &Model{commands: append([]command.Command(nil), commands...)}
	m.refresh()
	return m
}

func NewRegistry() *Model { return New(command.Registry()) }

func DefaultRenderStyles() RenderStyles {
	selection := renderer.DefaultStyle()
	selection.Inverse = true
	description := renderer.DefaultStyle()
	description.Italic = true
	return RenderStyles{Selection: selection, Description: description}
}

func (m *Model) Insert(r rune) {
	if m != nil {
		m.input.Insert(r)
		m.selected = 0
		m.scroll = 0
		m.refresh()
	}
}
func (m *Model) Backspace() {
	if m != nil && m.input.Value() != "" {
		m.input.Backspace()
		m.selected = 0
		m.scroll = 0
		m.refresh()
	}
}
func (m *Model) Query() string {
	if m == nil {
		return ""
	}
	return m.input.Value()
}
func (m *Model) Up() {
	if m != nil && m.selected > 0 {
		m.selected--
		m.clamp()
	}
}
func (m *Model) Down() {
	if m != nil && m.selected+1 < len(m.matches) {
		m.selected++
		m.clamp()
	}
}
func (m *Model) Selected() (command.Command, bool) {
	if m == nil || m.selected < 0 || m.selected >= len(m.matches) {
		return command.Command{}, false
	}
	return m.matches[m.selected].Command, true
}
func (m *Model) Matches() []Match {
	if m == nil {
		return nil
	}
	out := make([]Match, len(m.matches))
	for i, match := range m.matches {
		out[i] = match
		out[i].Positions = append([]int(nil), match.Positions...)
	}
	return out
}

func (m *Model) refresh() { m.matches = Fuzzy(m.commands, m.input.Value()); m.clamp() }
func (m *Model) clamp() {
	if len(m.matches) == 0 {
		m.selected = -1
		m.scroll = 0
		return
	}
	if m.selected < 0 {
		m.selected = 0
	}
	if m.selected >= len(m.matches) {
		m.selected = len(m.matches) - 1
	}
	if m.scroll < 0 {
		m.scroll = 0
	}
	if m.scroll > m.selected {
		m.scroll = m.selected
	}
}

func (m *Model) Render(inner domain.Size, styles RenderStyles) renderer.Frame {
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	if frame.Width == 0 || frame.Height == 0 {
		return frame
	}
	base := renderer.DefaultStyle()
	selection := styles.Selection
	desc := styles.Description
	desc.Italic = true
	ui.FillRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, renderer.Cell{Rune: ' ', Style: base})
	if m == nil {
		ui.DrawInputLine(frame, 0, "> ", "", base, selection)
		return frame
	}
	ui.DrawInputLine(frame, 0, "> ", m.Query(), base, selection)
	visible := frame.Height - 1
	if visible <= 0 {
		return frame
	}
	if len(m.matches) == 0 {
		return frame
	}
	if m.selected < m.scroll {
		m.scroll = m.selected
	}
	if m.selected >= m.scroll+visible {
		m.scroll = m.selected - visible + 1
	}
	codeWidth := 0
	for _, match := range m.matches {
		if len([]rune(match.Command.Code)) > codeWidth {
			codeWidth = len([]rune(match.Command.Code))
		}
	}
	for y := range visible {
		idx := m.scroll + y
		if idx >= len(m.matches) {
			break
		}
		match := m.matches[idx]
		style := base
		if idx == m.selected {
			style = selection
		}
		ui.FillRect(frame, domain.Rect{Y: y + 1, Width: frame.Width, Height: 1}, renderer.Cell{Rune: ' ', Style: style})
		x := 0
		highlight := map[int]bool{}
		for _, p := range match.Positions {
			highlight[p] = true
		}
		for i, r := range []rune(match.Command.Code) {
			cellStyle := style
			cellStyle.Bold = true
			if highlight[i] {
				cellStyle = selection
				cellStyle.Bold = true
			}
			if x < frame.Width {
				frame.Set(x, y+1, renderer.Cell{Rune: r, Style: cellStyle})
			}
			x++
		}
		for x < codeWidth+1 && x < frame.Width {
			frame.Set(x, y+1, renderer.Cell{Rune: ' ', Style: style})
			x++
		}
		x = ui.DrawText(frame, x, y+1, frame.Width, match.Command.Name+" — ", style)
		ui.DrawText(frame, x, y+1, frame.Width, match.Command.Desc, mergePaletteDescStyle(style, desc))
	}
	return frame
}

func mergePaletteDescStyle(line, desc renderer.Style) renderer.Style {
	out := line
	out.Italic = true
	if desc.HasForegroundRGB {
		out.HasForegroundRGB = true
		out.ForegroundRGB = desc.ForegroundRGB
		out.Foreground = desc.Foreground
	} else if desc.Foreground >= 0 {
		out.HasForegroundRGB = false
		out.Foreground = desc.Foreground
	}
	return out
}
