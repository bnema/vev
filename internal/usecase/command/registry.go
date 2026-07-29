package command

import "strings"

// Registry returns all palette and control commands in stable display order.
func Registry() []Command {
	commands := []Command{
		paletteControl("new-tab", "CNT", "New tab", "Create a new tab", TargetSession, func(ctx Context, _ []string) error { return ctx.CreateTab() }, func(ctx ControlContext) error { return ctx.CreateTab() }),
		paletteControlOne("new-session", "CNS", "New session", "Create and switch to a named session", TargetSession, func(ctx Context, _ []string) error { return ctx.CreateSession() }, func(ctx ControlContext, name string) error { return ctx.CreateSessionNamed(name) }),
		paletteControl("close-tab", "CLT", "Close tab", "Close the current tab", TargetTab, func(ctx Context, _ []string) error { return ctx.CloseTab() }, func(ctx ControlContext) error { return ctx.CloseTab() }),
		paletteControl("split-right", "SPR", "Split right", "Split the focused pane to the right", TargetPane, func(ctx Context, _ []string) error { return ctx.SplitRight() }, func(ctx ControlContext) error { return ctx.SplitRight() }),
		paletteControl("split-left", "SPL", "Split left", "Split the focused pane to the left", TargetPane, func(ctx Context, _ []string) error { return ctx.SplitLeft() }, func(ctx ControlContext) error { return ctx.SplitLeft() }),
		paletteControl("split-up", "SPU", "Split up", "Split the focused pane upward", TargetPane, func(ctx Context, _ []string) error { return ctx.SplitUp() }, func(ctx ControlContext) error { return ctx.SplitUp() }),
		paletteControl("split-down", "SPD", "Split down", "Split the focused pane downward", TargetPane, func(ctx Context, _ []string) error { return ctx.SplitDown() }, func(ctx ControlContext) error { return ctx.SplitDown() }),
		paletteControl("consume-or-expel-pane-left", "CEL", "Consume or expel pane left", "Move the focused pane into or out of the column on the left", TargetPane, func(ctx Context, _ []string) error { return ctx.ConsumeOrExpelPaneLeft() }, func(ctx ControlContext) error { return ctx.ConsumeOrExpelPaneLeft() }),
		paletteControl("consume-or-expel-pane-right", "CER", "Consume or expel pane right", "Move the focused pane into or out of the column on the right", TargetPane, func(ctx Context, _ []string) error { return ctx.ConsumeOrExpelPaneRight() }, func(ctx ControlContext) error { return ctx.ConsumeOrExpelPaneRight() }),
		paletteControl("stack-pane", "STP", "Stack pane", "Create a new pane in a stack", TargetPane, func(ctx Context, _ []string) error { return ctx.StackPane() }, func(ctx ControlContext) error { return ctx.StackPane() }),
		paletteControl("toggle-stack", "TST", "Toggle stack", "Toggle the focused pane stack", TargetPane, func(ctx Context, _ []string) error { return ctx.ToggleStack() }, func(ctx ControlContext) error { return ctx.ToggleStack() }),
		paletteOnly("toggle-floating-pane", "FLT", "Toggle floating pane", "Toggle the floating pane", func(ctx Context, _ []string) error { return ctx.ToggleFloatingPane() }),
		paletteControl("close-pane", "CLP", "Close pane", "Close the focused pane", TargetPane, func(ctx Context, _ []string) error { return ctx.ClosePane() }, func(ctx ControlContext) error { return ctx.ClosePane() }),
		movePaneCommand(),
		moveTabCommand(),
		paletteControl("focus-pane-left", "FPL", "Focus pane left", "Focus the pane to the left", TargetPane, func(ctx Context, _ []string) error { return ctx.FocusPaneLeft() }, func(ctx ControlContext) error { return ctx.FocusPaneLeft() }),
		paletteControl("focus-pane-right", "FPR", "Focus pane right", "Focus the pane to the right", TargetPane, func(ctx Context, _ []string) error { return ctx.FocusPaneRight() }, func(ctx ControlContext) error { return ctx.FocusPaneRight() }),
		paletteControl("focus-pane-up", "FPU", "Focus pane up", "Focus the pane above", TargetPane, func(ctx Context, _ []string) error { return ctx.FocusPaneUp() }, func(ctx ControlContext) error { return ctx.FocusPaneUp() }),
		paletteControl("focus-pane-down", "FPD", "Focus pane down", "Focus the pane below", TargetPane, func(ctx Context, _ []string) error { return ctx.FocusPaneDown() }, func(ctx ControlContext) error { return ctx.FocusPaneDown() }),
		paletteOnly("resize-pane", "RSZ", "Resize pane", "Enter pane resize mode", func(ctx Context, _ []string) error { return ctx.EnterResizeMode() }),
		paletteControl("grow-pane-width", "GPW", "Grow pane width", "Grow the pane by two columns", TargetPane, func(ctx Context, _ []string) error { return ctx.GrowPaneWidth() }, func(ctx ControlContext) error { return ctx.GrowPaneWidth() }),
		paletteControl("shrink-pane-width", "SPW", "Shrink pane width", "Shrink the pane by two columns", TargetPane, func(ctx Context, _ []string) error { return ctx.ShrinkPaneWidth() }, func(ctx ControlContext) error { return ctx.ShrinkPaneWidth() }),
		paletteControl("grow-pane-height", "GPH", "Grow pane height", "Grow the pane by one row", TargetPane, func(ctx Context, _ []string) error { return ctx.GrowPaneHeight() }, func(ctx ControlContext) error { return ctx.GrowPaneHeight() }),
		paletteControl("shrink-pane-height", "SPH", "Shrink pane height", "Shrink the pane by one row", TargetPane, func(ctx Context, _ []string) error { return ctx.ShrinkPaneHeight() }, func(ctx ControlContext) error { return ctx.ShrinkPaneHeight() }),
		paletteControl("equalize-panes", "EQP", "Equalize panes", "Restore equal pane shares in the tab", TargetTab, func(ctx Context, _ []string) error { return ctx.EqualizePanes() }, func(ctx ControlContext) error { return ctx.EqualizePanes() }),
		paletteControl("next-tab", "NXT", "Next tab", "Switch to the next tab", TargetSession, func(ctx Context, _ []string) error { return ctx.NextTab() }, func(ctx ControlContext) error { return ctx.NextTab() }),
		paletteControl("previous-tab", "PVT", "Previous tab", "Switch to the previous tab", TargetSession, func(ctx Context, _ []string) error { return ctx.PrevTab() }, func(ctx ControlContext) error { return ctx.PrevTab() }),
		paletteOnly("back-session", "BSK", "Previous session", "Toggle the previously active session", func(ctx Context, _ []string) error { return ctx.BackSession() }),
		{Slug: "jump-recent-session", Code: "JRS", Name: "Jump to recent session", Desc: "Jump to a recent session by rank", Usage: "jump-recent-session <rank>", PaletteVisible: true, Arguments: ArgumentsRequired, ContextHint: ContextHintRecentSessions, Run: func(ctx Context, args []string) error {
			rank, err := ParsePositiveDecimal(args)
			if err != nil {
				return err
			}
			return ctx.JumpRecentSession(rank)
		}},
		paletteOnly("session-picker", "SSP", "Session picker", "Open the session picker", func(ctx Context, _ []string) error { return ctx.OpenSessionPicker() }),
		paletteOnly("notifications", "NTC", "Notifications", "Show notification history", func(ctx Context, _ []string) error { return ctx.OpenNotifications() }),
		paletteOnly("yank-last-notification", "YLN", "Yank last notification", "Copy the most recent notification's details to the clipboard", func(ctx Context, _ []string) error { return ctx.YankLastNotification() }),
		paletteOnly("visual-mode", "VIS", "Visual mode", "Enter visual mode", func(ctx Context, _ []string) error { return ctx.EnterVisualMode() }),
		paletteControlOne("rename-session", "RNS", "Rename session", "Rename the session (an ephemeral session becomes named)", TargetSession, func(ctx Context, _ []string) error { return ctx.RenameSession() }, func(ctx ControlContext, name string) error { return ctx.RenameSessionTo(name) }),
		paletteControlOne("rename-tab", "RNT", "Rename tab", "Rename the current tab", TargetTab, func(ctx Context, _ []string) error { return ctx.RenameTab() }, func(ctx ControlContext, name string) error { return ctx.RenameTabTo(name) }),
		paletteOnly("detach", "DET", "Detach", "Detach from the session", func(ctx Context, _ []string) error { return ctx.Detach() }),
		toastCommand(),
		listCommand("list-sessions", "List sessions", "List sessions with active markers", TargetNone, func(ctx ControlContext, json bool) (string, error) { return ctx.ListSessions(json) }),
		listCommand("list-tabs", "List tabs", "List tabs in the target session", TargetSession, func(ctx ControlContext, json bool) (string, error) { return ctx.ListTabs(json) }),
		listCommand("list-panes", "List panes", "List panes in the target tab", TargetTab, func(ctx ControlContext, json bool) (string, error) { return ctx.ListPanes(json) }),
		remoteCatalogCommand(),
		sessionRecoveryCommand(),
	}
	return commands
}

func paletteOnly(slug, code, name, desc string, run func(Context, []string) error) Command {
	return Command{Slug: slug, Code: code, Name: name, Desc: desc, Usage: slug, PaletteVisible: true, Run: run}
}

func paletteControl(slug, code, name, desc string, target TargetKind, run func(Context, []string) error, control func(ControlContext) error) Command {
	return Command{Slug: slug, Code: code, Name: name, Desc: desc, Usage: slug, PaletteVisible: true, Scriptable: true, Target: target, Run: run, Control: func(ctx ControlContext, args []string, _ ControlOptions) (ControlResult, error) {
		if len(args) != 0 {
			return ControlResult{}, ErrInvalidArguments
		}
		return ControlResult{}, control(ctx)
	}}
}

func paletteControlOne(slug, code, name, desc string, target TargetKind, run func(Context, []string) error, control func(ControlContext, string) error) Command {
	cmd := Command{Slug: slug, Code: code, Name: name, Desc: desc, Usage: slug + " <name>", PaletteVisible: true, Scriptable: true, Target: target, Run: run}
	cmd.Control = func(ctx ControlContext, args []string, _ ControlOptions) (ControlResult, error) {
		if len(args) != 1 || args[0] == "" {
			return ControlResult{}, ErrInvalidArguments
		}
		return ControlResult{}, control(ctx, args[0])
	}
	return cmd
}

func movePaneCommand() Command {
	return Command{
		Slug: "move-pane", Code: "MPN", Name: "Move pane to tab",
		Desc: "Move the focused pane to another live tab", Usage: "move-pane <destination-session> <destination-tab-id>",
		PaletteVisible: true, Scriptable: true, Target: TargetPane, Scope: CommandScopeCrossSession,
		Run: func(ctx Context, _ []string) error { return ctx.OpenMovePanePicker() },
		Control: func(ctx ControlContext, args []string, _ ControlOptions) (ControlResult, error) {
			if len(args) != 2 || args[0] == "" || args[1] == "" {
				return ControlResult{}, ErrInvalidArguments
			}
			return ControlResult{}, ctx.MovePane(args[0], args[1])
		},
	}
}

func moveTabCommand() Command {
	return Command{
		Slug: "move-tab", Code: "MTB", Name: "Move tab to session",
		Desc: "Move the active tab to another live session", Usage: "move-tab <destination-session>",
		PaletteVisible: true, Scriptable: true, Target: TargetTab, Scope: CommandScopeCrossSession,
		Run: func(ctx Context, _ []string) error { return ctx.OpenMoveTabPicker() },
		Control: func(ctx ControlContext, args []string, _ ControlOptions) (ControlResult, error) {
			if len(args) != 1 || args[0] == "" {
				return ControlResult{}, ErrInvalidArguments
			}
			return ControlResult{}, ctx.MoveTab(args[0])
		},
	}
}

func toastCommand() Command {
	return Command{Slug: "toast", Name: "Toast", Desc: "Show a toast notification in the target session", Usage: "toast [-l info|warn|error] <message>", Scriptable: true, Target: TargetSession, Control: func(ctx ControlContext, args []string, _ ControlOptions) (ControlResult, error) {
		severity := "info"
		if len(args) >= 2 && args[0] == "-l" {
			severity = args[1]
			args = args[2:]
		}
		if len(args) != 1 || args[0] == "" {
			return ControlResult{}, ErrInvalidArguments
		}
		return ControlResult{}, ctx.Toast(severity, args[0])
	}}
}

func sessionRecoveryCommand() Command {
	return Command{
		Slug: "session-recovery", Name: "Session recovery", Desc: "Discard a broken durable session's persisted state",
		Usage:      "session-recovery discard",
		Scriptable: true, Target: TargetNone,
		Control: func(ctx ControlContext, args []string, _ ControlOptions) (ControlResult, error) {
			if len(args) != 1 || args[0] != "discard" {
				return ControlResult{}, ErrInvalidArguments
			}
			output, err := ctx.SessionRecovery("discard")
			return ControlResult{Output: output}, err
		},
	}
}

func remoteCatalogCommand() Command {
	return Command{
		Slug: "remote-catalog", Name: "Remote catalog",
		Desc:  "Return the versioned session catalog for remote discovery",
		Usage: "remote-catalog --json", Scriptable: true, Target: TargetNone,
		Control: func(ctx ControlContext, args []string, opts ControlOptions) (ControlResult, error) {
			if len(args) != 0 || !opts.JSON {
				return ControlResult{}, ErrInvalidArguments
			}
			output, err := ctx.RemoteCatalog(true)
			return ControlResult{Output: output}, err
		},
	}
}

func listCommand(slug, name, desc string, target TargetKind, list func(ControlContext, bool) (string, error)) Command {
	return Command{Slug: slug, Name: name, Desc: desc, Usage: slug + " [--json]", Scriptable: true, Target: target, Control: func(ctx ControlContext, args []string, opts ControlOptions) (ControlResult, error) {
		if len(args) != 0 {
			return ControlResult{}, ErrInvalidArguments
		}
		out, err := list(ctx, opts.JSON)
		return ControlResult{Output: out}, err
	}}
}

// ByCode returns the command matching code, case-insensitively.
func ByCode(code string) (Command, bool) {
	code = strings.ToUpper(code)
	for _, cmd := range PaletteRegistry() {
		if cmd.Code == code {
			return cmd, true
		}
	}
	return Command{}, false
}

// BySlug returns the exact command slug. Slugs are stable CLI identifiers.
func BySlug(slug string) (Command, bool) {
	for _, cmd := range Registry() {
		if cmd.Slug == slug {
			return cmd, true
		}
	}
	return Command{}, false
}

// PaletteRegistry returns commands visible in the in-band palette.
func PaletteRegistry() []Command {
	all := Registry()
	out := make([]Command, 0, len(all))
	for _, cmd := range all {
		if cmd.PaletteVisible {
			out = append(out, cmd)
		}
	}
	return out
}
