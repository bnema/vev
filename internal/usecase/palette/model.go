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
	results  []Result
	commands []command.Command
	input    ui.TextInput
	matches  []Match
	selected int
	scroll   int
}

// New accepts typed results. []command.Command remains supported while callers
// migrate, and is immediately converted to immutable CommandResults.
func New(items any) *Model {
	m := &Model{results: paletteResults(items)}
	for _, result := range m.results {
		if cmd, ok := resultCommand(result); ok {
			m.commands = append(m.commands, cmd)
		}
	}
	m.refresh()
	return m
}

func paletteResults(items any) []Result {
	switch values := items.(type) {
	case []Result:
		return append([]Result(nil), values...)
	case []command.Command:
		return commandResults(values)
	default:
		return nil
	}
}

func NewRegistry() *Model { return New(commandResults(command.Registry())) }

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
		m.selected, m.scroll = 0, 0
		m.refresh()
	}
}
func (m *Model) Backspace() {
	if m != nil && m.input.Value() != "" {
		m.input.Backspace()
		m.selected, m.scroll = 0, 0
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

// Selected returns the typed immutable target, not an executable command.
func (m *Model) Selected() (Result, bool) {
	if m == nil || m.selected < 0 || m.selected >= len(m.matches) {
		return nil, false
	}
	return m.matches[m.selected].Result, true
}

// CompleteSelected only completes static command results. Session targets can
// never be parsed, completed, or otherwise treated as commands.
func (m *Model) CompleteSelected() bool {
	selected, ok := m.Selected()
	if !ok {
		return false
	}
	cmd, ok := resultCommand(selected)
	if !ok {
		return false
	}

	query := m.Query()
	completed := cmd.Code
	if cmd.Arguments == command.ArgumentsRequired {
		token, args, hasSeparator := completionParts(query)
		if token == cmd.Code && hasSeparator && args != "" {
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
	m.selected, m.scroll = 0, 0
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

// ArgumentCommand returns the exact argument-taking static command being entered.
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
	m.matches = fuzzyResults(m.results, query)
	// Once an argument-taking static token is exact, retain its row while its
	// arguments make ordinary fuzzy matching inapplicable.
	if cmd, ok := ArgumentCommand(m.commands, query); ok {
		m.prependMatch(cmd)
	} else if len(m.matches) == 0 {
		// Arguments are not part of static command matching. When they would
		// otherwise clear the list, keep required-argument candidates matched by
		// the partial first token so Tab can complete them.
		token, _, hasSeparator := completionParts(query)
		if hasSeparator {
			for _, match := range fuzzyResults(commandResults(m.commands), token) {
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
	m.matches = append([]Match{{Result: NewCommandResult(cmd), Command: cmd}}, m.matches...)
}
func (m *Model) clamp() {
	if len(m.matches) == 0 {
		m.selected, m.scroll = -1, 0
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
	base, selection, desc := renderer.DefaultStyle(), styles.Selection, styles.Description
	ui.FillRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, renderer.Cell{Rune: ' ', Style: base})
	if m == nil {
		ui.DrawInputLine(frame, 0, "> ", "", base, selection)
		return frame
	}
	ui.DrawInputLine(frame, 0, "> ", m.Query(), base, selection)
	start, visible := 1, frame.Height-1
	if visible <= 0 || len(m.matches) == 0 {
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
		if cmd, ok := resultCommand(match.Result); ok && len([]rune(cmd.Code)) > codeWidth {
			codeWidth = len([]rune(cmd.Code))
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
		if cmd, ok := resultCommand(match.Result); ok {
			m.renderCommand(frame, y+start, style, selection, desc, codeWidth, cmd, match.Positions, activeCmd, activeOK, opts.Guidance)
			continue
		}
		m.renderSession(frame, y+start, style, selection, match)
	}
	return frame
}

func (m *Model) renderCommand(frame renderer.Frame, y int, style, selection, desc renderer.Style, codeWidth int, cmd command.Command, positions []int, activeCmd command.Command, activeOK bool, guidance string) {
	x, highlight := 0, map[int]bool{}
	for _, position := range positions {
		highlight[position] = true
	}
	for i, r := range []rune(cmd.Code) {
		cellStyle := style
		cellStyle.Bold = true
		if highlight[i] {
			cellStyle = selection
			cellStyle.Bold = true
		}
		if x < frame.Width {
			frame.Set(x, y, renderer.Cell{Rune: r, Style: cellStyle})
		}
		x++
	}
	for x < codeWidth+1 && x < frame.Width {
		frame.Set(x, y, renderer.Cell{Rune: ' ', Style: style})
		x++
	}
	description := cmd.Desc
	if guidance != "" && activeOK && cmd.ContextHint != command.ContextHintNone && activeCmd.Code == cmd.Code {
		description = guidance
	}
	ui.DrawText(frame, x, y, frame.Width, description, mergePaletteDescStyle(style, desc))
}

func (m *Model) renderSession(frame renderer.Frame, y int, style, selection renderer.Style, match Match) {
	text := []rune(match.Result.DisplayText())
	highlight := map[int]bool{}
	prefix := len([]rune("Switch to session "))
	for _, position := range match.Positions {
		highlight[prefix+position] = true
	}
	for x, r := range text {
		cellStyle := style
		if highlight[x] {
			cellStyle = selection
			cellStyle.Bold = true
		}
		if x < frame.Width {
			frame.Set(x, y, renderer.Cell{Rune: r, Style: cellStyle})
		}
	}
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
