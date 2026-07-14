package palette

import (
	"unicode"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

type RenderStyles struct {
	Selection   renderer.Style
	Description renderer.Style
}

// RenderOptions contains presentation supplied by the daemon for one render.
// Guidance is substituted only into the exact contextual command row; it never
// adds rows or changes fuzzy matching geometry.
type RenderOptions struct {
	Styles   RenderStyles
	Guidance string
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

// CompleteSelected replaces the query with the selected command's effective
// code. Required-argument commands retain the typed argument bytes.
func (m *Model) CompleteSelected() bool {
	selected, ok := m.Selected()
	if !ok {
		return false
	}

	query := m.Query()
	completed := selected.Code
	if selected.Arguments == command.ArgumentsRequired {
		token, args, hasSeparator := completionParts(query)
		if token == selected.Code && hasSeparator && args != "" {
			return false
		}
		completed += " "
		if args != "" {
			completed += args
		}
	}
	if completed == query {
		return false
	}

	m.input.SetValue(completed)
	m.selected = 0
	m.scroll = 0
	m.refresh()
	return true
}

// completionParts splits the first token at Unicode whitespace and returns
// the argument beginning at its first non-whitespace byte.
func completionParts(query string) (token, args string, hasSeparator bool) {
	for i, r := range query {
		if !unicode.IsSpace(r) {
			continue
		}
		for j, argumentRune := range query[i:] {
			if !unicode.IsSpace(argumentRune) {
				return query[:i], query[i+j:], true
			}
		}
		return query[:i], "", true
	}
	return query, "", false
}

// ArgumentCommand returns the exact argument-taking command being entered.
func (m *Model) ArgumentCommand() (command.Command, bool) {
	if m == nil {
		return command.Command{}, false
	}
	return ArgumentCommand(m.commands, m.Query())
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

func (m *Model) refresh() {
	query := m.input.Value()
	m.matches = Fuzzy(m.commands, query)
	// Once an argument-taking token is exact, retain its row while its
	// arguments make ordinary fuzzy matching inapplicable.
	if cmd, ok := ArgumentCommand(m.commands, query); ok {
		m.prependMatch(cmd)
	} else if len(m.matches) == 0 {
		// Arguments are not part of static command matching. When they would
		// otherwise clear the list, keep required-argument candidates matched by
		// the partial first token so Tab can complete them.
		token, _, hasSeparator := completionParts(query)
		if hasSeparator {
			for _, match := range Fuzzy(m.commands, token) {
				if match.Command.Arguments == command.ArgumentsRequired {
					m.matches = append(m.matches, match)
				}
			}
		}
	}
	m.clamp()
}

func (m *Model) prependMatch(cmd command.Command) {
	for i, match := range m.matches {
		if match.Command.Code != cmd.Code {
			continue
		}
		if i > 0 {
			copy(m.matches[1:i+1], m.matches[:i])
			m.matches[0] = match
		}
		return
	}
	m.matches = append([]Match{{Command: cmd}}, m.matches...)
}
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

func (m *Model) Render(inner domain.Size, opts RenderOptions) renderer.Frame {
	styles := opts.Styles
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	if frame.Width == 0 || frame.Height == 0 {
		return frame
	}
	base := renderer.DefaultStyle()
	selection := styles.Selection
	desc := styles.Description
	ui.FillRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, renderer.Cell{Rune: ' ', Style: base})
	if m == nil {
		ui.DrawInputLine(frame, 0, "> ", "", base, selection)
		return frame
	}
	ui.DrawInputLine(frame, 0, "> ", m.Query(), base, selection)
	start := 1
	visible := frame.Height - start
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
	activeCmd, activeOK := m.ArgumentCommand()
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
		ui.FillRect(frame, domain.Rect{Y: y + start, Width: frame.Width, Height: 1}, renderer.Cell{Rune: ' ', Style: style})
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
				frame.Set(x, y+start, renderer.Cell{Rune: r, Style: cellStyle})
			}
			x++
		}
		for x < codeWidth+1 && x < frame.Width {
			frame.Set(x, y+start, renderer.Cell{Rune: ' ', Style: style})
			x++
		}
		description := match.Command.Desc
		if opts.Guidance != "" && activeOK && match.Command.ContextHint != command.ContextHintNone && activeCmd.Code == match.Command.Code {
			description = opts.Guidance
		}
		ui.DrawText(frame, x, y+start, frame.Width, description, mergePaletteDescStyle(style, desc))
	}
	return frame
}

func mergePaletteDescStyle(line, desc renderer.Style) renderer.Style {
	out := line
	out.Italic = desc.Italic
	if desc.HasForegroundRGB {
		out.HasForegroundRGB = true
		out.ForegroundRGB = desc.ForegroundRGB
		out.Foreground = -1
	} else if desc.Foreground >= 0 {
		out.HasForegroundRGB = false
		out.Foreground = desc.Foreground
	}
	return out
}
