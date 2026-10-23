package agentactivity

import "regexp"

// zut rules, checked against zut HEAD source (packages/agent/modes) and
// fixtures captured from its own redraw.
//
// The input prompt is the ▌ accent bar; conditions skip it. zut's title
// carries no busy marker. zut renders inline: under a short transcript its
// status bar and prompt sit near the top of the grid with blank rows below,
// which Window skips. Its working line sits directly above or below the
// input, alone or as the status bar prefix, so it is always near the bottom
// of the content.
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
)

const (
	zutFrame   = `[^\s\w•◦*+\-·>#|┃│✓✔✗✘⏺]{1,2}`
	zutElapsed = `(?:\d+h)?(?:\d+m)?\d+s(?:\s|$)`
)

var zutTable = table{
	prompt: "▌",
	rules: []rule{
		// The dialog may be drawn over prompt rows of the transcript, so its
		// rows are inspected even when they start with the prompt marker.
		{signal: blocked("zut.confirm"), all: conds(
			lineAny(BottomRows, zutConfirmRe), lineAny(12, zutYesRe), textAny(12, "esc to refuse"),
		)},
		{signal: blocked("zut.approval"), all: conds(line(8, zutApprovalRe))},
		{signal: working("zut.busy"), all: conds(line(8, zutWorkingRe))},
		{signal: idle("zut.quiet")},
	},
}
