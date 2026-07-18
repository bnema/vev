package palette

import (
	"fmt"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestModelInsertBackspaceAndSelectionClamp(t *testing.T) {
	m := New(CommandResults([]command.Command{
		cmd("ABC", "Alpha", "first"),
		cmd("DEF", "Delta", "second"),
		cmd("AXY", "Other", "third"),
	}))

	require.Equal(t, "", m.Query())
	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, "ABC", selectedCommandCode(t, selected))

	m.Down()
	m.Down()
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "AXY", selectedCommandCode(t, selected))

	m.Insert('d')
	require.Equal(t, "d", m.Query())
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "DEF", selectedCommandCode(t, selected), "selection clamps to only match after query changes")

	m.Up()
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "DEF", selectedCommandCode(t, selected), "up at first match clamps")

	m.Backspace()
	require.Equal(t, "", m.Query())
	require.Len(t, m.Matches(), 3)
}

func TestModelNoMatchesClearsSelection(t *testing.T) {
	m := New(CommandResults([]command.Command{cmd("ABC", "Alpha", "first")}))
	m.Insert('z')

	_, ok := m.Selected()
	require.False(t, ok)
	require.Empty(t, m.Matches())
}

func TestModelMatchesDeepCopiesPositions(t *testing.T) {
	m := New(CommandResults([]command.Command{cmd("ABC", "Alpha", "first")}))
	m.Insert('a')

	matches := m.Matches()
	require.Len(t, matches, 1)
	require.Equal(t, []int{0}, matches[0].Positions)

	matches[0].Positions[0] = 2

	fresh := m.Matches()
	require.Equal(t, []int{0}, fresh[0].Positions)
}

func TestModelExactArgumentMatchMovesExistingFuzzyMatchToFront(t *testing.T) {
	zzz := cmd("ZZZ", "", "ZZZ 1")
	zzz.Arguments = command.ArgumentsRequired
	commands := []command.Command{
		cmd("AAA", "", "ZZZ 1"),
		zzz,
	}
	want := Fuzzy(CommandResults(commands), "ZZZ 1")[1]
	m := New(CommandResults(commands))

	for _, r := range "ZZZ 1" {
		m.Insert(r)
	}

	matches := m.Matches()
	require.Len(t, matches, 2)
	require.Equal(t, "ZZZ", matchCommandCode(t, matches[0]))
	require.Equal(t, want, matches[0], "the existing fuzzy match must retain its metadata")
	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, "ZZZ", selectedCommandCode(t, selected))
}

func TestModelExactArgumentMatchKeepsAbsentAndFirstBehavior(t *testing.T) {
	for _, tt := range []struct {
		name     string
		commands func(zzz command.Command) []command.Command
		want     Match
		wantLen  int
	}{
		{
			name: "absent fuzzy match is prepended",
			commands: func(zzz command.Command) []command.Command {
				return []command.Command{cmd("AAA", "", "ZZZ 1"), zzz}
			},
			want: newMatch(NewCommandResult(command.Command{
				Code:      "ZZZ",
				Desc:      "unmatched",
				Arguments: command.ArgumentsRequired,
			}), 0),
			wantLen: 2,
		},
		{
			name: "first fuzzy match is unchanged",
			commands: func(zzz command.Command) []command.Command {
				zzz.Desc = "ZZZ 1"
				return []command.Command{zzz, cmd("ZZZZ", "", "ZZZ 1")}
			},
			want: func() Match {
				zzz := cmd("ZZZ", "", "ZZZ 1")
				zzz.Arguments = command.ArgumentsRequired
				return Fuzzy(CommandResults([]command.Command{zzz, cmd("ZZZZ", "", "ZZZ 1")}), "ZZZ 1")[0]
			}(),
			wantLen: 2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			zzz := cmd("ZZZ", "", "unmatched")
			zzz.Arguments = command.ArgumentsRequired
			m := New(CommandResults(tt.commands(zzz)))
			for _, r := range "ZZZ 1" {
				m.Insert(r)
			}

			matches := m.Matches()
			require.Len(t, matches, tt.wantLen)
			require.Equal(t, tt.want, matches[0])
		})
	}
}

func TestRenderDrawsOnlyCodeAndDescriptionWithStyles(t *testing.T) {
	m := New(CommandResults([]command.Command{
		cmd("CPY", "Copy", "Enter copy mode"),
		cmd("CNT", "New", "Create tab"),
	}))
	m.Insert('c')
	m.Insert('y')
	frame := m.Render(domain.Size{Cols: 28, Rows: 3}, RenderOptions{Styles: DefaultRenderStyles()})

	require.Equal(t, '>', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(1, 0).Rune)
	require.Equal(t, 'c', frame.At(2, 0).Rune)
	require.Equal(t, 'y', frame.At(3, 0).Rune)
	require.True(t, frame.At(4, 0).Style.Inverse, "caret is reverse-video after query")

	require.Equal(t, "CPY Enter copy mode         ", frameRow(frame, 1))
	require.NotContains(t, frameRow(frame, 1), "Copy")
	require.NotContains(t, frameRow(frame, 1), "—")
	require.True(t, frame.At(0, 1).Style.Inverse, "selected line is inverse")
	require.True(t, frame.At(5, 1).Style.Inverse, "selected line padding/text remains inverse")
	require.True(t, frame.At(0, 1).Style.Bold, "command code C is bold")
	require.True(t, frame.At(1, 1).Style.Bold, "command code P is bold")
	require.True(t, frame.At(2, 1).Style.Bold, "command code Y is bold")
	require.True(t, frame.At(4, 1).Style.Italic, "description is italic")
}

func TestRenderSafelyClipsVisibleFieldsAtNarrowWidths(t *testing.T) {
	m := New(CommandResults([]command.Command{cmd("CPY", "Copy", "Enter copy mode")}))
	want := []rune("CPY Enter copy mode")

	for _, cols := range []int{0, 1, 3, 4, 7} {
		t.Run(fmt.Sprintf("cols_%d", cols), func(t *testing.T) {
			frame := m.Render(domain.Size{Cols: cols, Rows: 2}, RenderOptions{Styles: DefaultRenderStyles()})
			require.Equal(t, cols, frame.Width)
			if cols > 0 {
				require.Equal(t, want[:min(cols, len(want))], []rune(frameRow(frame, 1)))
			}
		})
	}
}

func selectedCommandCode(t *testing.T, result Result) string {
	t.Helper()
	cmd, ok := result.Command()
	require.True(t, ok, "selected result is a command")
	return cmd.Code
}

func matchCommandCode(t *testing.T, match Match) string {
	t.Helper()
	return selectedCommandCode(t, match.Result)
}

func frameRow(frame renderer.Frame, y int) string {
	row := make([]rune, frame.Width)
	for x := range frame.Width {
		row[x] = frame.At(x, y).Rune
	}
	return string(row)
}

func TestRenderUsesConfiguredStyles(t *testing.T) {
	tests := []struct {
		name   string
		styles func() RenderStyles
		x      int
		assert func(t *testing.T, style renderer.Style)
	}{
		{
			name: "selection style for fuzzy highlights",
			styles: func() RenderStyles {
				accent := renderer.DefaultStyle()
				accent.HasBackgroundRGB = true
				accent.BackgroundRGB = renderer.RGB{R: 1, G: 2, B: 3}
				styles := DefaultRenderStyles()
				styles.Selection = accent
				return styles
			},
			x: 0,
			assert: func(t *testing.T, style renderer.Style) {
				require.True(t, style.Bold)
				require.False(t, style.Inverse)
				require.True(t, style.HasBackgroundRGB)
				require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, style.BackgroundRGB)
			},
		},
		{
			name: "muted italic description style",
			styles: func() RenderStyles {
				muted := renderer.DefaultStyle()
				muted.Italic = true
				muted.HasForegroundRGB = true
				muted.ForegroundRGB = renderer.RGB{R: 10, G: 20, B: 30}
				styles := DefaultRenderStyles()
				styles.Description = muted
				return styles
			},
			x: 4,
			assert: func(t *testing.T, style renderer.Style) {
				require.True(t, style.Italic)
				require.True(t, style.HasForegroundRGB)
				require.Equal(t, -1, style.Foreground)
				require.Equal(t, renderer.RGB{R: 10, G: 20, B: 30}, style.ForegroundRGB)
			},
		},
		{
			name: "description italic is configurable",
			styles: func() RenderStyles {
				styles := DefaultRenderStyles()
				styles.Description.Italic = false
				return styles
			},
			x: 4,
			assert: func(t *testing.T, style renderer.Style) {
				require.False(t, style.Italic)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New(CommandResults([]command.Command{cmd("CPY", "Copy", "Enter copy mode")}))
			m.Insert('c')
			m.Insert('y')
			frame := m.Render(domain.Size{Cols: 28, Rows: 3}, RenderOptions{Styles: tt.styles()})

			tt.assert(t, frame.At(tt.x, 1).Style)
		})
	}
}

func TestModelCompleteSelected(t *testing.T) {
	jrs := cmd("JRS", "Jump", "Jump to recent session")
	jrs.Arguments = command.ArgumentsRequired
	registryFirst := command.Registry()[0]

	tests := []struct {
		name       string
		commands   []command.Command
		registry   bool
		query      string
		down       int
		want       string
		wantChange bool
		rendered   string
	}{
		{name: "nil model", want: "", wantChange: false},
		{name: "empty model", commands: nil, want: "", wantChange: false},
		{name: "registry first from empty query", registry: true, want: registryFirst.Code, wantChange: true, rendered: registryFirst.Code},
		{name: "prefix match", commands: []command.Command{cmd("CPY", "Copy", "Enter copy mode")}, query: "cp", want: "CPY", wantChange: true},
		{name: "fuzzy match", commands: []command.Command{cmd("CPY", "Copy", "Enter copy mode")}, query: "cy", want: "CPY", wantChange: true},
		{name: "description match", commands: []command.Command{cmd("CPY", "Copy", "Enter copy mode")}, query: "enter", want: "CPY", wantChange: true},
		{name: "navigated selection", commands: []command.Command{cmd("AAA", "", ""), cmd("BBB", "", "")}, down: 1, want: "BBB", wantChange: true},
		{name: "static command replaces query", commands: []command.Command{cmd("CPY", "Copy", "Enter copy mode")}, query: "copy", want: "CPY", wantChange: true},
		{name: "argument command appends required space", commands: []command.Command{jrs}, query: "jrs", want: "JRS ", wantChange: true},
		{name: "partial token preserves unicode whitespace argument bytes", commands: []command.Command{jrs}, query: "jr\u2003 \tα  β", want: "JRS α  β", wantChange: true},
		{name: "exact code and argument is unchanged", commands: []command.Command{jrs}, query: "JRS\u2003  α  β", want: "JRS\u2003  α  β", wantChange: false},
		{name: "no match", commands: []command.Command{cmd("CPY", "Copy", "Enter copy mode")}, query: "zzz", want: "zzz", wantChange: false},
		{name: "effective override code", commands: []command.Command{{Slug: "new-tab", Code: "NT", Desc: "Create tab"}}, query: "n", want: "NT", wantChange: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m *Model
			if tt.name != "nil model" {
				if tt.registry {
					m = NewRegistry()
				} else {
					m = New(CommandResults(tt.commands))
				}
				for _, r := range tt.query {
					m.Insert(r)
				}
				for range tt.down {
					m.Down()
				}
			}

			changed := m.CompleteSelected()
			require.Equal(t, tt.wantChange, changed)
			if m == nil {
				require.Equal(t, tt.want, "")
				return
			}
			require.Equal(t, tt.want, m.Query())
			if tt.rendered != "" {
				frame := m.Render(domain.Size{Cols: 32, Rows: 2}, RenderOptions{Styles: DefaultRenderStyles()})
				require.Equal(t, "> "+tt.rendered, frameRow(frame, 0)[:len([]rune(tt.rendered))+2])
			}
		})
	}
}

func TestModelUsesDefensiveTypedResultsAndKeepsSessionsCommandInert(t *testing.T) {
	created := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC)
	results := []Result{
		NewCommandResult(cmd("JRS", "Jump", "Jump to recent session")),
		NewActiveSessionResult("work", created, "work-id"),
		NewStoppedSessionResult("work", created),
	}
	m := New(results)
	results[1] = NewActiveSessionResult("changed", created, "changed-id")

	for _, r := range "work" {
		m.Insert(r)
	}
	matches := m.Matches()
	require.Len(t, matches, 2)
	require.Equal(t, ResultKindActiveSession, matches[0].Result.Kind())
	matches[0].Result = NewCommandResult(cmd("BAD", "", ""))
	require.Equal(t, ResultKindActiveSession, m.Matches()[0].Result.Kind())

	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, ResultKindActiveSession, selected.Kind())
	id, active := selected.SessionID()
	require.True(t, active)
	require.Equal(t, domain.SessionID("work-id"), id)
	require.False(t, m.CompleteSelected(), "sessions never participate in tab completion")
	require.Equal(t, "work", m.Query())
	_, argument := m.ArgumentCommand()
	require.False(t, argument)

	frame := m.Render(domain.Size{Cols: 28, Rows: 2}, RenderOptions{Styles: DefaultRenderStyles()})
	require.Equal(t, "Switch to session work      ", frameRow(frame, 1))
}

func TestRenderStoppedSessionHighlightsNameAfterResumePrefix(t *testing.T) {
	m := New([]Result{NewStoppedSessionResult("work", time.Unix(0, 1))})
	m.Insert('w')
	m.Insert('k')

	frame := m.Render(domain.Size{Cols: 28, Rows: 2}, RenderOptions{Styles: DefaultRenderStyles()})

	require.Equal(t, "Resume session work         ", frameRow(frame, 1))
	require.False(t, frame.At(14, 1).Style.Bold, "resume prefix is not highlighted")
	require.True(t, frame.At(15, 1).Style.Bold, "first matched session rune is highlighted")
	require.True(t, frame.At(18, 1).Style.Bold, "last matched session rune is highlighted")
}

func TestRenderFeedbackUsesSelectedSessionRowWithoutAddingResult(t *testing.T) {
	m := New([]Result{NewActiveSessionResult("work", time.Unix(0, 1), "work")})
	frame := m.Render(domain.Size{Cols: 64, Rows: 3}, RenderOptions{Styles: DefaultRenderStyles(), Feedback: "requested session is unavailable"})
	require.Len(t, m.Matches(), 1)
	require.Contains(t, frameRow(frame, 1), "requested session is unavailable")
}

func TestRenderGuidanceReplacesOnlyExactContextualRow(t *testing.T) {
	jrs := cmd("JRS", "Jump", "Jump to recent session")
	jrs.Arguments = command.ArgumentsRequired
	jrs.ContextHint = command.ContextHintRecentSessions
	m := New(CommandResults([]command.Command{jrs}))
	for _, r := range "JRS 1" {
		m.Insert(r)
	}

	frame := m.Render(domain.Size{Cols: 28, Rows: 4}, RenderOptions{Styles: DefaultRenderStyles(), Guidance: "jump to recent session 1"})
	if got := frameRow(frame, 1); got != "JRS jump to recent session 1" {
		t.Fatalf("command row = %q, want contextual guidance without a feedback row", got)
	}
}

func TestRenderStylesFillBaseRowsAndSelection(t *testing.T) {
	m := New(CommandResults([]command.Command{cmd("ABC", "Alpha", "first")}))
	base := renderer.Style{Foreground: 1, Background: 2}
	row := renderer.Style{Foreground: 3, Background: 4}
	selection := renderer.Style{Foreground: 5, Background: 6}
	frame := m.Render(domain.Size{Cols: 24, Rows: 4}, RenderOptions{Styles: RenderStyles{Base: base, Row: row, Selection: selection, Description: row}})

	require.True(t, frame.At(23, 3).Style.Equal(base), "unused interior keeps modal base")
	require.True(t, frame.At(23, 0).Style.Equal(base), "input row keeps base surface")
	require.True(t, frame.At(23, 1).Style.Equal(selection), "selected result owns active surface")
}
