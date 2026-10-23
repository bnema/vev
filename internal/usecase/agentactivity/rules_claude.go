package agentactivity

import "regexp"

// Claude Code rules, checked against 2.1.308.
//
// The prompt and the selection cursor are both ❯. Conditions skip ❯ lines
// and the composer lines below the prompt, unless they target the prompt or
// a menu option, so text the user is typing cannot trigger a rule.
var (
	// A permission question: "Do you want to proceed?", "Do you want to
	// make this edit to x.go?", "Do you want to allow Claude to fetch this
	// content?".
	claudeQuestionRe = regexp.MustCompile(`^\s*(?:│\s*)?Do you want to .+\?\s*(?:│\s*)?$`)
	claudeYesRe      = regexp.MustCompile(`^\s*(?:│\s*)?(?:❯\s*)?1\.\s+Yes\b`)
	claudeNoRe       = regexp.MustCompile(`^\s*(?:│\s*)?(?:❯\s*)?\d\.\s+No\b`)
	// Title spinner: braille (≤ 2.1.227) or half circles (≥ 2.1.228).
	claudeTitleSpinRe = regexp.MustCompile(`^[\x{2800}-\x{28FF}\x{25D0}-\x{25D3}] `)
	claudeTitleIdleRe = regexp.MustCompile(`^✳ `)
	// Activity line "✻ Thinking… (12s · ↓ 1.2k tokens · esc to interrupt)",
	// alone or with its parenthetical. The completed form "✻ Cooked for
	// 1m 2s" has no ellipsis, and a list item that merely contains one is
	// not followed by a parenthetical at end of line. The line starts at
	// column zero (one space is tolerated); chat text quoting it is indented
	// under its ⏺ or ⎿ marker. A narrow pane wraps the parenthetical, so the
	// first row may end before its closing parenthesis. A pane too narrow to
	// keep the verb and its ellipsis on one row loses the line; the title
	// spinner still reports Working.
	claudeActivityRe = regexp.MustCompile(`^ ?[·✢✳✶✻✽]\s+\S[^…]*…(?:\s+\([^)]*\)?)?\s*$`)
	// The activity line carrying the interrupt hint in its parenthetical.
	claudeInterruptRe = regexp.MustCompile(`^ ?[·✢✳✶✻✽]\s+\S[^…]*…\s+\([^)]*esc to interrupt\)\s*$`)
	// The input prompt: ❯ alone or followed by typed text, but not the
	// "❯ 1. " selection cursor of a menu.
	claudeInputRe = regexp.MustCompile(`^\s*❯\s*(?:$|[^\s\d]|\d+(?:[^.\d]|$))`)
)

// claudeNoInput guards blockers: dialogs replace the input prompt, so a
// visible prompt means the dialog text is quoted chat history.
var claudeNoInput = conds(inputPrompt())

var claudeTable = table{
	prompt:         "❯",
	composerBorder: "──",
	composerPrompt: claudeInputRe,
	rules: []rule{
		{signal: blocked("claude.permission"), all: conds(
			line(12, claudeQuestionRe), lineAny(12, claudeYesRe), lineAny(12, claudeNoRe),
		), not: claudeNoInput},
		// AskUserQuestion footers: "Enter to select · ↑/↓ to navigate · Esc
		// to cancel", with "←/→ to switch" for multi-question forms. The
		// user-opened model picker shares the footer and is excluded.
		{signal: blocked("claude.question"), all: conds(
			text(8, "Esc to cancel"), text(8, "Enter to select"), text(8, "to navigate"),
		), not: append(conds(text(BottomRows, "Select model")), claudeNoInput...)},
		{signal: blocked("claude.question-review"), all: conds(
			text(8, "Esc to cancel"), text(8, "to switch"), textAny(BottomRows, "Submit answers"),
		), not: claudeNoInput},
		{signal: blocked("claude.permission-wait"), all: conds(text(8, "Waiting for permission")), not: claudeNoInput},
		{signal: hold("claude.transcript"), all: conds(text(3, "Showing detailed transcript"))},
		// The agent-set title is authoritative over the weak screen rules
		// below, whose text may linger after a turn.
		{signal: workingStrong("claude.title-spinner"), all: conds(title(claudeTitleSpinRe))},
		{signal: idleStrong("claude.title-idle"), all: conds(title(claudeTitleIdleRe))},
		{signal: working("claude.interrupt-hint"), all: conds(line(12, claudeInterruptRe))},
		{signal: working("claude.activity"), all: conds(line(12, claudeActivityRe))},
		{signal: working("claude.background"), all: conds(text(12, "Waiting for background work to finish"))},
		{signal: idle("claude.prompt"), all: conds(inputPrompt())},
	},
}
