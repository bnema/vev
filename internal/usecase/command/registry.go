package command

import "strings"

// Registry returns all commands in display order.
func Registry() []Command {
	commands := []Command{
		{Slug: "new-tab", Code: "CNT", Name: "New tab", Desc: "Create a new tab", Run: func(ctx Context) error { return ctx.CreateTab() }},
		{Slug: "new-session", Code: "CNS", Name: "New session", Desc: "Create and switch to a named session", Run: func(ctx Context) error { return ctx.CreateSession() }},
		{Slug: "close-tab", Code: "CLT", Name: "Close tab", Desc: "Close the current tab", Run: func(ctx Context) error { return ctx.CloseTab() }},
		{Slug: "split-right", Code: "SPR", Name: "Split right", Desc: "Split the focused pane to the right", Run: func(ctx Context) error { return ctx.SplitRight() }},
		{Slug: "split-left", Code: "SPL", Name: "Split left", Desc: "Split the focused pane to the left", Run: func(ctx Context) error { return ctx.SplitLeft() }},
		{Slug: "split-up", Code: "SPU", Name: "Split up", Desc: "Split the focused pane upward", Run: func(ctx Context) error { return ctx.SplitUp() }},
		{Slug: "split-down", Code: "SPD", Name: "Split down", Desc: "Split the focused pane downward", Run: func(ctx Context) error { return ctx.SplitDown() }},
		{Slug: "stack-pane", Code: "STP", Name: "Stack pane", Desc: "Create a new pane in a stack", Run: func(ctx Context) error { return ctx.StackPane() }},
		{Slug: "toggle-stack", Code: "TST", Name: "Toggle stack", Desc: "Toggle the focused pane stack", Run: func(ctx Context) error { return ctx.ToggleStack() }},
		{Slug: "toggle-floating-pane", Code: "FLT", Name: "Toggle floating pane", Desc: "Toggle the floating pane", Run: func(ctx Context) error { return ctx.ToggleFloatingPane() }},
		{Slug: "close-pane", Code: "CLP", Name: "Close pane", Desc: "Close the focused pane", Run: func(ctx Context) error { return ctx.ClosePane() }},
		{Slug: "focus-pane-left", Code: "FPL", Name: "Focus pane left", Desc: "Focus the pane to the left", Run: func(ctx Context) error { return ctx.FocusPaneLeft() }},
		{Slug: "focus-pane-right", Code: "FPR", Name: "Focus pane right", Desc: "Focus the pane to the right", Run: func(ctx Context) error { return ctx.FocusPaneRight() }},
		{Slug: "focus-pane-up", Code: "FPU", Name: "Focus pane up", Desc: "Focus the pane above", Run: func(ctx Context) error { return ctx.FocusPaneUp() }},
		{Slug: "focus-pane-down", Code: "FPD", Name: "Focus pane down", Desc: "Focus the pane below", Run: func(ctx Context) error { return ctx.FocusPaneDown() }},
		{Slug: "next-tab", Code: "NXT", Name: "Next tab", Desc: "Switch to the next tab", Run: func(ctx Context) error { return ctx.NextTab() }},
		{Slug: "previous-tab", Code: "PVT", Name: "Previous tab", Desc: "Switch to the previous tab", Run: func(ctx Context) error { return ctx.PrevTab() }},
		{Slug: "back-session", Code: "BSK", Name: "Previous session", Desc: "Toggle the previously active session", Run: func(ctx Context) error { return ctx.BackSession() }},
		{Slug: "session-picker", Code: "SSP", Name: "Session picker", Desc: "Open the session picker", Run: func(ctx Context) error { return ctx.OpenSessionPicker() }},
		{Slug: "visual-mode", Code: "VIS", Name: "Visual mode", Desc: "Enter visual mode", Run: func(ctx Context) error { return ctx.EnterVisualMode() }},
		{Slug: "rename-session", Code: "RNS", Name: "Rename session", Desc: "Rename the session (an ephemeral session becomes named)", Run: func(ctx Context) error { return ctx.RenameSession() }},
		{Slug: "rename-tab", Code: "RNT", Name: "Rename tab", Desc: "Rename the current tab", Run: func(ctx Context) error { return ctx.RenameTab() }},
		{Slug: "detach", Code: "DET", Name: "Detach", Desc: "Detach from the session", Run: func(ctx Context) error { return ctx.Detach() }},
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
