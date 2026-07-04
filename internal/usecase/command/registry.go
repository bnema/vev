package command

import "strings"

// Registry returns all commands in display order.
func Registry() []Command {
	commands := []Command{
		{Code: "CNT", Name: "New tab", Desc: "Create a new tab", Run: func(ctx Context) error { return ctx.CreateTab() }},
		{Code: "CNS", Name: "New session", Desc: "Create and switch to a named session", Run: func(ctx Context) error { return ctx.CreateSession() }},
		{Code: "CLT", Name: "Close tab", Desc: "Close the current tab", Run: func(ctx Context) error { return ctx.CloseTab() }},
		{Code: "SPR", Name: "Split right", Desc: "Split the focused pane to the right", Run: func(ctx Context) error { return ctx.SplitRight() }},
		{Code: "SPL", Name: "Split left", Desc: "Split the focused pane to the left", Run: func(ctx Context) error { return ctx.SplitLeft() }},
		{Code: "SPU", Name: "Split up", Desc: "Split the focused pane upward", Run: func(ctx Context) error { return ctx.SplitUp() }},
		{Code: "SPD", Name: "Split down", Desc: "Split the focused pane downward", Run: func(ctx Context) error { return ctx.SplitDown() }},
		{Code: "STP", Name: "Stack pane", Desc: "Create a new pane in a stack", Run: func(ctx Context) error { return ctx.StackPane() }},
		{Code: "TST", Name: "Toggle stack", Desc: "Toggle the focused pane stack", Run: func(ctx Context) error { return ctx.ToggleStack() }},
		{Code: "CLP", Name: "Close pane", Desc: "Close the focused pane", Run: func(ctx Context) error { return ctx.ClosePane() }},
		{Code: "FPL", Name: "Focus pane left", Desc: "Focus the pane to the left", Run: func(ctx Context) error { return ctx.FocusPaneLeft() }},
		{Code: "FPR", Name: "Focus pane right", Desc: "Focus the pane to the right", Run: func(ctx Context) error { return ctx.FocusPaneRight() }},
		{Code: "FPU", Name: "Focus pane up", Desc: "Focus the pane above", Run: func(ctx Context) error { return ctx.FocusPaneUp() }},
		{Code: "FPD", Name: "Focus pane down", Desc: "Focus the pane below", Run: func(ctx Context) error { return ctx.FocusPaneDown() }},
		{Code: "NXT", Name: "Next tab", Desc: "Switch to the next tab", Run: func(ctx Context) error { return ctx.NextTab() }},
		{Code: "PVT", Name: "Previous tab", Desc: "Switch to the previous tab", Run: func(ctx Context) error { return ctx.PrevTab() }},
		{Code: "SSP", Name: "Session picker", Desc: "Open the session picker", Run: func(ctx Context) error { return ctx.OpenSessionPicker() }},
		{Code: "VIS", Name: "Visual mode", Desc: "Enter visual mode", Run: func(ctx Context) error { return ctx.EnterVisualMode() }},
		{Code: "RNS", Name: "Rename session", Desc: "Rename the session (an ephemeral session becomes named)", Run: func(ctx Context) error { return ctx.RenameSession() }},
		{Code: "DET", Name: "Detach", Desc: "Detach from the session", Run: func(ctx Context) error { return ctx.Detach() }},
	}

	return commands
}

// ByCode returns the command matching code, case-insensitively.
func ByCode(code string) (Command, bool) {
	code = strings.ToUpper(code)
	for _, cmd := range Registry() {
		if cmd.Code == code {
			return cmd, true
		}
	}

	return Command{}, false
}
