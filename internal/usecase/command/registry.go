package command

import "strings"

// Registry returns all commands in display order.
func Registry() []Command {
	commands := []Command{
		{Code: "CNT", Name: "New tab", Desc: "Create a new tab", Run: func(ctx Context) error { return ctx.CreateTab() }},
		{Code: "CLT", Name: "Close tab", Desc: "Close the current tab", Run: func(ctx Context) error { return ctx.CloseTab() }},
		{Code: "NXT", Name: "Next tab", Desc: "Switch to the next tab", Run: func(ctx Context) error { return ctx.NextTab() }},
		{Code: "PVT", Name: "Previous tab", Desc: "Switch to the previous tab", Run: func(ctx Context) error { return ctx.PrevTab() }},
		{Code: "SSP", Name: "Session picker", Desc: "Open the session picker", Run: func(ctx Context) error { return ctx.OpenSessionPicker() }},
		{Code: "CPY", Name: "Copy mode", Desc: "Enter copy mode", Run: func(ctx Context) error { return ctx.EnterCopyMode() }},
		{Code: "RNS", Name: "Promote session", Desc: "Promote ephemeral session to named", Run: func(ctx Context) error { return ctx.RenameSession() }},
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
