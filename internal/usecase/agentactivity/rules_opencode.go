package agentactivity

import "regexp"

// opencode rules, checked against v2.0.29 (binary strings). opencode sets no
// terminal title, so every rule reads the screen, and it draws no stable
// idle marker: idle is the absence of any other rule.
//
// User messages and the input box are drawn behind a ┃ bar; working
// conditions skip those lines so typed text cannot match. Dialogs may share
// that border, so blocker conditions use textAny.
var (
	// The interrupt hint is "<keybind> interrupt", or "esc again to
	// interrupt" once armed. The keybind is remappable.
	opencodeInterruptRe = regexp.MustCompile(`(?i)(?:^|\s)(?:esc|[a-z]+(?:\+[a-z0-9]+)+) (?:again to )?interrupt(?:\s|$)`)
	// Animated progress bar of filled and empty squares.
	opencodeProgressRe = regexp.MustCompile(`(?:^|\s)[■⬝]{4,}(?:\s|$)`)
)

const opencodePermissionFooter = "select  enter confirm  esc "

var opencodeTable = table{
	prompt: "┃",
	rules: []rule{
		// "⇆ select  enter confirm  esc reject" (esc cancel on the always
		// stage) is the live dialog footer, with or without the "Permission
		// required" body in the window; question footers use " · "
		// separators instead. An answered dialog keeps its text in the chat
		// but loses the footer.
		{signal: blocked("opencode.permission"), all: conds(textAny(3, opencodePermissionFooter))},
		{signal: blocked("opencode.question"), all: conds(
			textAny(3, "enter confirm · esc cancel"), textAny(BottomRows, "Type your own answer"),
		)},
		{signal: blocked("opencode.question-answer"), all: conds(textAny(3, "enter submit · esc back"))},
		{signal: blocked("opencode.question-form"), all: conds(
			textAny(3, "enter choose"), textAny(3, "tab next"), textAny(3, "esc dismiss"),
		)},
		// The footer is the lowest line; the status row may sit above it.
		{signal: working("opencode.interrupt-hint"), all: conds(line(3, opencodeInterruptRe))},
		{signal: working("opencode.progress"), all: conds(line(3, opencodeProgressRe))},
		{signal: idle("opencode.quiet")},
	},
}
