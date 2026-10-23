package agentactivity

import "regexp"

// Codex rules, checked against 0.163.0 (binary strings and the codex-rs TUI
// sources).
//
// Composer lines start with › and conditions skip them, so text the user is
// typing cannot match. Selection lists also mark the focused option with ›,
// so option and composer conditions use lineAny and match the marker
// explicitly.
var (
	codexYesRe   = regexp.MustCompile(`^\s*(?:›\s*)?1\.\s+Yes\b`)
	codexTrustRe = regexp.MustCompile(`^\s*(?:›\s*)?1\.\s+Trust and continue\b`)
	// "Question 1/2 (1 unanswered)" heads a request_user_input form.
	codexQuestionRe = regexp.MustCompile(`^\s*Question \d+/\d+\b`)
	codexActionRe   = regexp.MustCompile(`Action Required`)
	// Title activity item frames.
	codexTitleSpinRe = regexp.MustCompile(`(?:^|\s)[⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏](?:\s|$)`)
	// Status row "• Working (1m 05s • esc to interrupt)" at column zero: a
	// blinking •/◦ indicator (absent with reduced motion), a header, and the
	// elapsed parenthetical, optionally followed by " · <message>". User
	// messages start with › and wrapped text is indented, so neither can
	// match. The interrupt form is specific enough to search deeper; the
	// bare "(12s)" form, shown when the hint is hidden, is only trusted
	// directly above the composer. A narrow pane truncates the row with an
	// ellipsis; it is still recognized while "(<elapsed> • " survives.
	// Narrower truncation loses the row; the title spinner still reports
	// Working.
	codexStatusRe    = regexp.MustCompile(`^(?:[•◦] )?[^\s(•◦›└│][^(]*\((?:\d+h )?(?:\d+m )?\d+s • (?:[^)]*to interrupt\)(?:\s*$| · )|[^)]*…\s*$)`)
	codexElapsedRe   = regexp.MustCompile(`^(?:[•◦] )?[^\s(•◦›└│][^(]*\((?:\d+h )?(?:\d+m )?\d+s\)(?:\s*$| · )`)
	codexReconnectRe = regexp.MustCompile(`^(?:[•◦] )?Reconnect failed`)
	// The composer: › at column zero alone or followed by text, but not the
	// "› 1. " selection cursor of a list.
	codexComposerRe = regexp.MustCompile(`^›\s*(?:$|[^\s\d]|\d+(?:[^.\d]|$))`)
	// Rows that may sit between the status row and the composer: indented
	// status details ("  └ Adding the flag") and the queued-messages block.
	codexBelowStatusRe = regexp.MustCompile(`^(?:\s|[•◦] Queued follow-up inputs)`)
)

// codexNoComposer guards blockers: approval and question views replace the
// composer, so a visible composer means the dialog text is quoted history.
var codexNoComposer = conds(inputPrompt())

var codexTable = table{
	prompt:         "›",
	composerPrompt: codexComposerRe,
	rules: []rule{
		{signal: blocked("codex.title-action"), all: conds(title(codexActionRe))},
		{signal: blocked("codex.approval"), all: conds(
			text(12, "to confirm"), text(12, "to cancel"), lineAny(12, codexYesRe),
		), not: codexNoComposer},
		{signal: blocked("codex.question"), all: conds(
			text(12, "to submit answer"), line(BottomRows, codexQuestionRe),
		), not: codexNoComposer},
		{signal: blocked("codex.trust"), all: conds(
			lineAny(BottomRows, codexTrustRe), text(BottomRows, "Trust this folder"),
		), not: codexNoComposer},
		{signal: blocked("codex.trust-footer"), all: conds(
			lineAny(BottomRows, codexTrustRe), text(3, "esc quit"),
		), not: codexNoComposer},
		{signal: hold("codex.transcript"), all: conds(text(4, "to scroll"), text(4, "to page"))},
		{signal: workingStrong("codex.title-spinner"), all: conds(title(codexTitleSpinRe))},
		// Queued messages and status details may sit between the status row
		// and the composer.
		{signal: working("codex.status"), all: conds(line(10, codexStatusRe))},
		// History rows such as "• Tests pass (12s)" share the bare form;
		// only the row directly above the composer, past its details and
		// queued messages, is the live status.
		{signal: working("codex.status-elapsed"), all: conds(abovePrompt(codexElapsedRe, codexBelowStatusRe)), not: conds(line(4, codexReconnectRe))},
		// The composer stays visible during a turn and the title items are
		// user-configurable, so idle is always weak and debounced.
		{signal: idle("codex.composer"), all: codexNoComposer},
	},
}
