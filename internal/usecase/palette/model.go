package palette

import (
	"strings"
	"unicode"
	"unicode/utf8"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/ui"
)

type RenderStyles struct {
	// Base fills unused modal interior cells; Row is the ordinary inactive
	// result surface. They are separate to retain the chrome hierarchy.
	Base                 renderer.Style
	Row                  renderer.Style
	Selection            renderer.Style
	Description          renderer.Style
	SelectionDescription renderer.Style
}

// RenderOptions contains presentation supplied by the daemon for one render.
// Guidance is substituted only into the exact contextual command row; it never
// adds rows or changes fuzzy matching geometry.
type RenderOptions struct {
	Styles   RenderStyles
	Guidance string
	// Preview is the current exact command's action description. It replaces
	// the selected row's static description unless Feedback is present.
	Preview string
	// Feedback is interaction-scoped daemon feedback. It replaces the selected
	// row's contextual detail without adding a searchable result or row.
	Feedback string
}

type Model struct {
	results            []Result
	argumentResults    []Result
	createDestinations []Result
	input              ui.TextInput
	matches            []Match
	selected           int
	scroll             int
	destinationMode    bool
	selectionMade      bool
}

// New accepts typed immutable palette results.
func New(results []Result) *Model {
	ordinary, destinations := splitResults(results)
	m := &Model{
		results:            ordinary,
		argumentResults:    requiredArgumentResults(ordinary),
		createDestinations: destinations,
	}
	m.refresh()
	return m
}

func NewRegistry() *Model { return New(CommandResults(command.PaletteRegistry())) }

// ReplaceResults replaces the model's immutable result source while preserving
// the current query and exact selected target when it remains available.
func (m *Model) ReplaceResults(results []Result) {
	if m == nil {
		return
	}
	wasDestinationMode := m.destinationMode
	wasSelectionMade := m.selectionMade
	selected, hadSelection := m.Selected()
	selectedDestination := hadSelection && selected.Kind() == ResultKindCreateSessionDestination
	m.results, m.createDestinations = splitResults(results)
	m.argumentResults = requiredArgumentResults(m.results)
	m.refresh()
	if !hadSelection {
		if m.destinationMode || wasDestinationMode && !wasSelectionMade {
			m.selectionMade = false
		}
		return
	}
	for i, match := range m.matches {
		if match.Result.sameTarget(selected) {
			m.selected = i
			m.selectionMade = true
			m.clamp()
			return
		}
	}
	if selectedDestination || m.destinationMode {
		m.selectionMade = false
		m.selected, m.scroll = 0, 0
		m.clamp()
	}
}

func DefaultRenderStyles() RenderStyles {
	selection := renderer.DefaultStyle()
	selection.Inverse = true
	description := renderer.DefaultStyle()
	description.Italic = true
	selectionDescription := selection
	selectionDescription.Italic = true
	base := renderer.DefaultStyle()
	return RenderStyles{Base: base, Row: base, Selection: selection, Description: description, SelectionDescription: selectionDescription}
}

func (m *Model) Insert(r rune) {
	if m != nil {
		m.input.Insert(r)
		m.selected, m.scroll, m.selectionMade = 0, 0, false
		m.refresh()
	}
}
func (m *Model) Backspace() {
	if m != nil && m.input.Value() != "" {
		m.input.Backspace()
		m.selected, m.scroll, m.selectionMade = 0, 0, false
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
	if m == nil || len(m.matches) == 0 {
		return
	}
	if !m.selectionMade {
		m.selected = len(m.matches) - 1
		m.selectionMade = true
		m.clamp()
		return
	}
	if m.selected > 0 {
		m.selected--
		m.clamp()
	}
}
func (m *Model) Down() {
	if m == nil || len(m.matches) == 0 {
		return
	}
	if !m.selectionMade {
		m.selected = 0
		m.selectionMade = true
		m.clamp()
		return
	}
	if m.selected+1 < len(m.matches) {
		m.selected++
		m.clamp()
	}
}

// Selected returns the typed immutable target, not an executable command.
func (m *Model) Selected() (Result, bool) {
	if m == nil || !m.selectionMade || m.selected < 0 || m.selected >= len(m.matches) {
		return Result{}, false
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
	cmd, ok := selected.Command()
	if !ok {
		return false
	}

	query := m.Query()
	completed := cmd.Code
	if cmd.Arguments != command.ArgumentsNone {
		query = strings.TrimLeftFunc(query, unicode.IsSpace)
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
	m.selected, m.scroll, m.selectionMade = 0, 0, false
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
	return ArgumentCommand(m.results, m.Query())
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
	wasDestinationMode := m.destinationMode
	if destinations, ok := m.cnsDestinationMatches(query); ok {
		m.destinationMode = true
		if !wasDestinationMode {
			m.selectionMade = false
		}
		m.matches = destinations
		m.clamp()
		return
	}
	m.destinationMode = false
	m.selectionMade = true
	m.matches = Fuzzy(m.results, query)
	// Keep an exact command row selected over fuzzy session matches.
	if result, ok := ExactCommandResult(m.results, query); ok {
		m.prependMatch(result)
		// Once an argument-taking static token is exact, retain its row while its
		// arguments make ordinary fuzzy matching inapplicable.
	} else if result, _, ok := argumentResult(m.results, query); ok {
		m.prependMatch(result)
	} else if len(m.matches) == 0 {
		// Arguments are not part of static command matching. When they would
		// otherwise clear the list, keep argument-capable candidates matched by
		// the partial first token so Tab can complete them.
		token, _, hasSeparator := completionParts(query)
		if hasSeparator {
			m.matches = Fuzzy(m.argumentResults, token)
		}
	}
	m.clamp()
}

func splitResults(results []Result) (ordinary, destinations []Result) {
	ordinary = make([]Result, 0, len(results))
	for _, result := range results {
		if result.Kind() == ResultKindCreateSessionDestination {
			destinations = append(destinations, result)
			continue
		}
		ordinary = append(ordinary, result)
	}
	return ordinary, destinations
}

func (m *Model) cnsDestinationMatches(query string) ([]Match, bool) {
	if len(m.createDestinations) < 2 {
		return nil, false
	}
	fields := strings.Fields(query)
	if len(fields) == 0 || len(fields) > 2 {
		return nil, false
	}
	cmd, ok := ArgumentCommand(m.results, query)
	if !ok || cmd.Slug != "new-session" {
		return nil, false
	}
	name := ""
	if len(fields) == 2 {
		name = fields[1]
		if domain.ValidateSessionName(name) != nil {
			return nil, false
		}
	}
	matches := make([]Match, len(m.createDestinations))
	for i, destination := range m.createDestinations {
		matches[i] = newMatch(destination.withCreateSessionName(name), 0)
	}
	return matches, true
}

func requiredArgumentResults(results []Result) []Result {
	commands := make([]Result, 0, len(results))
	for _, result := range results {
		cmd, ok := result.Command()
		if ok && cmd.Arguments != command.ArgumentsNone {
			commands = append(commands, result)
		}
	}
	return commands
}

func (m *Model) prependMatch(result Result) {
	cmd, _ := result.Command()
	for i, match := range m.matches {
		candidate, ok := match.Result.Command()
		if !ok || candidate.Code != cmd.Code {
			continue
		}
		if i > 0 {
			copy(m.matches[1:i+1], m.matches[:i])
			m.matches[0] = match
		}
		return
	}
	m.matches = append([]Match{newMatch(result, 0)}, m.matches...)
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
	if styles == (RenderStyles{}) {
		styles = DefaultRenderStyles()
	}
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	if frame.Width == 0 || frame.Height == 0 {
		return frame
	}
	base, row, selection, desc := styles.Base, styles.Row, styles.Selection, styles.Description
	selectionDesc := styles.SelectionDescription
	if selectionDesc == (renderer.Style{}) {
		selectionDesc = mergePaletteDescStyle(selection, desc)
	}
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
		if cmd, ok := match.Result.Command(); ok && utf8.RuneCountInString(cmd.Code) > codeWidth {
			codeWidth = utf8.RuneCountInString(cmd.Code)
		}
	}
	activeCmd, activeOK := m.ArgumentCommand()
	for y := range visible {
		idx := m.scroll + y
		if idx >= len(m.matches) {
			break
		}
		match := m.matches[idx]
		style := row
		rowSelected := idx == m.selected && m.selectionMade
		if rowSelected {
			style = selection
		}
		ui.FillRect(frame, domain.Rect{Y: y + start, Width: frame.Width, Height: 1}, renderer.Cell{Rune: ' ', Style: style})
		if cmd, ok := match.Result.Command(); ok {
			m.renderCommand(frame, y+start, style, selection, desc, selectionDesc, codeWidth, cmd, match.Positions, activeCmd, activeOK, opts.Guidance, opts.Preview, opts.Feedback, rowSelected)
			continue
		}
		showFeedback := rowSelected || !m.selectionMade && idx == 0
		m.renderSession(frame, y+start, style, selection, match, opts.Feedback, showFeedback)
	}
	return frame
}

func (m *Model) renderCommand(frame renderer.Frame, y int, style, selection, desc, selectionDesc renderer.Style, codeWidth int, cmd command.Command, positions []int, activeCmd command.Command, activeOK bool, guidance, preview, feedback string, selected bool) {
	x, nextHighlight := 0, 0
	for _, r := range cmd.Code {
		cellStyle := style
		cellStyle.Bold = true
		if nextHighlight < len(positions) && positions[nextHighlight] == x {
			cellStyle = selection
			cellStyle.Bold = true
			nextHighlight++
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
	if feedback != "" && selected {
		description = feedback
	} else if preview != "" && selected {
		description = preview
	} else if guidance != "" && activeOK && cmd.ContextHint != command.ContextHintNone && activeCmd.Code == cmd.Code {
		description = guidance
	}
	descriptionStyle := mergePaletteDescStyle(style, desc)
	if selected {
		descriptionStyle = selectionDesc
	}
	ui.DrawText(frame, x, y, frame.Width, description, descriptionStyle)
}

func (m *Model) renderSession(frame renderer.Frame, y int, style, selection renderer.Style, match Match, feedback string, selected bool) {
	x, nextHighlight := 0, 0
	for _, r := range match.Result.DisplayText() {
		cellStyle := style
		if nextHighlight < len(match.Positions) && match.Positions[nextHighlight] == x {
			cellStyle = selection
			cellStyle.Bold = true
			nextHighlight++
		}
		if x < frame.Width {
			frame.Set(x, y, renderer.Cell{Rune: r, Style: cellStyle})
		}
		x++
	}
	if feedback == "" || !selected {
		return
	}
	if x < frame.Width {
		frame.Set(x, y, renderer.Cell{Rune: ' ', Style: style})
	}
	x++
	for _, r := range feedback {
		if x < frame.Width {
			frame.Set(x, y, renderer.Cell{Rune: r, Style: style})
		}
		x++
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
