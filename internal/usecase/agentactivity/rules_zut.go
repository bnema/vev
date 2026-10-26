package agentactivity

import "regexp"

// zut rules, checked against zut HEAD source (packages/agent/modes) and
// fixtures captured or derived from zut's rendering source.
//
// The input prompt is the ▌ accent bar; conditions skip it. zut's title
// carries no busy marker. zut renders inline: under a short transcript its
// status bar and prompt sit near the top of the grid with blank rows below,
// which Window skips. Its working line sits directly above or below the
// input, alone or as the status bar prefix, so it is always near the bottom
// of the content.
//
// Resident subagents run in the background after the parent turn ends, so
// the parent shows no working line while they work. zut lists each queued or
// running child directly below the input instead
// (modes/resident_subagent_indicator.go); a running child counts as working.
var (
	// The confirm dialog is a floating pane. Wide panes box it, so its
	// header "╭──── confirm tool call ────╮" and body rows "│  1. yes …│"
	// may follow chat text the pane is drawn over; narrow panes use a
	// full-width drawer whose header "──── confirm tool call ────" starts
	// the row. The title may continue with "(N more queued)".
	zutConfirmRe = regexp.MustCompile(`(?:^|╭)─+ confirm tool call(?:\s|$)`)
	zutYesRe     = regexp.MustCompile(`(?:^|│)\s*1\.\s+yes\b`)
	// Busy prefix "<spinner> <activity label> - <elapsed>", where elapsed is
	// a time.Duration rounded to seconds ("0s", "1m5s", "1h2m3s"). The
	// spinner frames are theme-defined, so the exact label phrases of
	// activity.go anchor the match. The status bar may continue after it.
	// Markdown bullets, quote bars, and check or cross marks are excluded as
	// frames so chat text cannot match.
	zutWorkingRe = regexp.MustCompile(`^\s*` + zutFrame + ` (?:Preparing request|Preparing tool: |Sending request to |Sending tool results to |Waiting for .+ to respond|Receiving response from |Running tool: |Running shell command|Retrying request|Compacting conversation history|Condensing conversation history).* - ` + zutElapsed)
	// The busy prefix while a tool call waits for confirmation. It stays
	// visible when a tall pane centres the dialog above the window.
	zutApprovalRe = regexp.MustCompile(`^\s*` + zutFrame + ` Waiting for approval: .* - ` + zutElapsed)
	// The ask_user_question dialog (modes/question_dialog.go) is a floating
	// pane like the confirm dialog. Its title row "Question N/M" can sit
	// above the window when the question has many options, so its footer is
	// matched instead: the key hints while choosing, or while typing a custom
	// answer. The footer starts at the pane's content edge: column 0 in a
	// drawer, right after the │ border in a box. Chat text is indented, so a
	// quoted footer does not match. The footer may wrap; only its first
	// segment is matched.
	zutQuestionFooterRe = regexp.MustCompile(`(?:^|│)(?:↑/↓ j/k move · 1–9 pick · |Enter submit · Esc back/cancel · )`)
	// The busy prefix while the question tool runs. zut skips the confirm
	// gate for it, so the tool runs only while the dialog waits for answers.
	// It stays visible when a tall pane centres the dialog above the window.
	zutAskingRe = regexp.MustCompile(`^\s*` + zutFrame + ` Running tool: ask_user_question - ` + zutElapsed)
	// The prompt marker ▌ also prefixes chat messages and the editor.
	zutPromptRe = regexp.MustCompile(`^\s*▌`)
	// A running resident child: "<spinner> <name> · <activity> · activity
	// <age> ago · <age>", where activity is "running" or, while the child
	// waits for its model, one frame of "Zzzz". Usage metadata may follow
	// on the row. Queued children show "… <name> · queued" and do not
	// match. A pane too narrow for the row cuts it to the width and ends it
	// with "...". The full status word must survive the cut: the status
	// word followed by anything ending in "..." matches ("⠋ worker ·
	// running ...", "⠋ worker · running · activity 3s ag..."). A row cut
	// inside or before the status word ("⠋ worker · runn...") stays
	// undetected.
	zutSubagentRe = regexp.MustCompile(`^\s*` + zutFrame + ` .+? · (?:running|Zzzz|zZzz|zzZz|zzzZ)(?: · activity ` + zutAge + ` ago · ` + zutAge + `(?:\s|$)|.*\.\.\.$)`)
)

const (
	zutFrame   = `[^\s\w•◦*+\-·>#|┃│✓✔✗✘⏺]{1,2}`
	zutElapsed = `(?:\d+h)?(?:\d+m)?\d+s(?:\s|$)`
	// A resident child age: "12s", "1m05s", "2h03m", "1d02h", or "-" when
	// unknown.
	zutAge = `(?:\d+s|\d+m\d{2}s|\d+h\d{2}m|\d+d\d{2}h|-)`
)

var zutTable = table{
	prompt:         "▌",
	composerPrompt: zutPromptRe,
	// The resident panel below the prompt can be arbitrarily tall.
	anyPromptRow: true,
	rules: []rule{
		// The dialog may be drawn over prompt rows of the transcript, so its
		// rows are inspected even when they start with the prompt marker.
		{signal: blocked("zut.confirm"), all: conds(
			lineAny(BottomRows, zutConfirmRe), lineAny(12, zutYesRe), textAny(12, "esc to refuse"),
		)},
		{signal: blocked("zut.approval"), all: conds(line(8, zutApprovalRe))},
		// The box may be drawn over prompt rows of the transcript.
		{signal: blocked("zut.question"), all: conds(lineAny(BottomRows, zutQuestionFooterRe))},
		{signal: blocked("zut.asking"), all: conds(line(8, zutAskingRe))},
		{signal: working("zut.busy"), all: conds(line(8, zutWorkingRe))},
		// zut draws the panel directly below the input prompt, so only rows
		// below the prompt count; quoted rows in chat above it do not. zut
		// sorts children newest-first, so queued rows (and their usage
		// rows) can sit above running ones: the whole region below the
		// prompt is searched. Known limits: zut hides the panel while the
		// /subagents dialog or a child view is open, and input_style lines
		// or block clears the ▌ prompt, so the panel cannot be located.
		{signal: working("zut.subagent"), all: conds(belowPrompt(zutSubagentRe))},
		{signal: idle("zut.quiet")},
	},
}
