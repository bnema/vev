package agentactivity

import (
	"bytes"
	"regexp"
	"strings"
)

// Classify returns the first matching rule of agent a for one observation.
// It returns the zero Signal when no rule matches or a is None.
func Classify(a Agent, in Input) Signal {
	if int(a) >= len(tables) || tables[a] == nil {
		return Signal{}
	}
	t := tables[a]
	c := t.composer(&in)
	for i := range t.rules {
		if t.rules[i].matches(t, &in, c) {
			return t.rules[i].signal
		}
	}
	return Signal{}
}

// tables holds one rule table per agent, indexed by Agent.
var tables = [...]*table{
	Claude:   &claudeTable,
	Codex:    &codexTable,
	OpenCode: &opencodeTable,
	Zut:      &zutTable,
}

// table is an ordered rule list: blockers first, then holds, working, and
// idle. The first rule whose conditions all hold wins.
type table struct {
	// prompt is the agent's input prompt marker. Lines starting with it hold
	// user-typed text and are ignored by text conditions.
	prompt string
	// composerPrompt matches the input prompt line, as opposed to a
	// selection cursor that shares the prompt marker.
	composerPrompt *regexp.Regexp
	// composerBorder, when set, is the prefix of the border line that closes
	// the composer below the input prompt. The wrapped input lines between
	// them are ignored like the prompt line.
	composerBorder string
	// anyPromptRow accepts the lowest prompt-marker line as the live prompt
	// wherever it sits in the window, for agents that draw variable-height
	// chrome below the prompt.
	anyPromptRow bool
	rules        []rule
}

// promptRows bounds how low a borderless input prompt, or the border closing
// a composer, must sit to count as the live prompt.
const promptRows = 6

// lineSpan is a half-open range of Input.Lines indexes.
type lineSpan struct{ lo, hi int }

func (s lineSpan) has(i int) bool { return i >= s.lo && i < s.hi }

// composerView locates the live input prompt of one observation.
type composerView struct {
	// prompt is the Input.Lines index of the input prompt line, or -1 when
	// none is visible.
	prompt int
	// wrapped holds the wrapped input lines between the prompt and the
	// composer border below it.
	wrapped lineSpan
}

// composer finds the lowest prompt-marker line. It is the live input prompt
// when it matches composerPrompt and either sits among the lowest promptRows
// lines or is boxed by composer borders: one directly above it and one among
// the lowest promptRows lines below its wrapped input. Lines run lowest
// first, so wrapped input lines sit between the lower border and the prompt.
func (t *table) composer(in *Input) composerView {
	none := composerView{prompt: -1}
	if t.composerPrompt == nil {
		return none
	}
	// lowest and nearest are the lowest border line and the one closest
	// below the prompt.
	lowest, nearest := -1, -1
	for i, line := range in.Lines {
		switch {
		case t.composerBorder != "" && isPromptLine(line, t.composerBorder):
			if lowest < 0 {
				lowest = i
			}
			nearest = i
		case isPromptLine(line, t.prompt):
			if !t.composerPrompt.Match(line) {
				return none
			}
			view := composerView{prompt: i}
			if nearest >= 0 {
				view.wrapped = lineSpan{lo: nearest + 1, hi: i}
			}
			if i < promptRows || t.anyPromptRow {
				return view
			}
			boxed := lowest >= 0 && lowest < promptRows &&
				i+1 < len(in.Lines) && isPromptLine(in.Lines[i+1], t.composerBorder)
			if boxed {
				return view
			}
			return none
		}
	}
	return none
}

// rule fires when every cond in all holds and no cond in not holds. A rule
// with no conds always fires and serves as the table's fallback.
type rule struct {
	signal Signal
	all    []cond
	not    []cond
}

func (r *rule) matches(t *table, in *Input, c composerView) bool {
	for i := range r.all {
		if !r.all[i].eval(t, in, c) {
			return false
		}
	}
	for i := range r.not {
		if r.not[i].eval(t, in, c) {
			return false
		}
	}
	return true
}

type condKind uint8

const (
	// condText: a line in the window contains text, ASCII case-folded.
	condText condKind = iota
	// condLine: a line in the window matches re.
	condLine
	// condTitle: the agent-set title matches re.
	condTitle
	// condPrompt: the live input prompt is visible.
	condPrompt
	// condAbovePrompt: the first line above the input prompt that pass does
	// not match is re's match.
	condAbovePrompt
	// condBelowPrompt: re matches a line below the live input prompt.
	condBelowPrompt
)

type cond struct {
	kind condKind
	// rows is the number of lowest non-blank lines inspected.
	rows int
	// prompts also inspects lines that start with the table's prompt
	// marker and the composer lines below it. Off by default so user-typed
	// text cannot match.
	prompts bool
	text    string
	re      *regexp.Regexp
	// pass matches the lines condAbovePrompt steps over.
	pass *regexp.Regexp
}

func (c *cond) eval(t *table, in *Input, view composerView) bool {
	switch c.kind {
	case condTitle:
		return in.TitleFromAgent && c.re.MatchString(in.Title)
	case condPrompt:
		return view.prompt >= 0
	case condAbovePrompt:
		if view.prompt < 0 {
			return false
		}
		for _, line := range in.Lines[view.prompt+1:] {
			if c.pass == nil || !c.pass.Match(line) {
				return c.re.Match(line)
			}
		}
		return false
	}
	skip := view.wrapped
	lines := in.Lines
	if c.kind == condBelowPrompt {
		if view.prompt < 0 {
			return false
		}
		lines = lines[:view.prompt]
	} else if len(lines) > c.rows {
		lines = lines[:c.rows]
	}
	for i, line := range lines {
		if !c.prompts && (skip.has(i) || isPromptLine(line, t.prompt)) {
			continue
		}
		switch c.kind {
		case condText:
			if containsFold(line, c.text) {
				return true
			}
		case condLine, condBelowPrompt:
			if c.re.Match(line) {
				return true
			}
		}
	}
	return false
}

// text builds a case-insensitive substring condition over the lowest rows
// lines.
func text(rows int, s string) cond {
	return cond{kind: condText, rows: rows, text: strings.ToLower(s)}
}

// line builds a regexp condition over the lowest rows lines.
func line(rows int, re *regexp.Regexp) cond {
	return cond{kind: condLine, rows: rows, re: re}
}

// lineAny is line that also inspects prompt lines: prompt markers that
// double as selection cursors, and the prompt itself.
func lineAny(rows int, re *regexp.Regexp) cond {
	return cond{kind: condLine, rows: rows, prompts: true, re: re}
}

// textAny is text that also inspects prompt lines, for dialogs drawn inside
// the prompt's border.
func textAny(rows int, s string) cond {
	c := text(rows, s)
	c.prompts = true
	return c
}

func title(re *regexp.Regexp) cond { return cond{kind: condTitle, re: re} }

// inputPrompt holds while the agent's live input prompt is visible.
func inputPrompt() cond { return cond{kind: condPrompt} }

// abovePrompt holds when re matches the first line above the input prompt,
// stepping over lines that pass, when non-nil, matches. Blank rows are
// already dropped.
func abovePrompt(re, pass *regexp.Regexp) cond {
	return cond{kind: condAbovePrompt, re: re, pass: pass}
}

// belowPrompt holds when the live input prompt is visible and re matches a
// line below it. Lines run lowest first, so those are the lines before the
// prompt. Wrapped input lines and prompt-marker lines are skipped like in
// line. The search is bounded by the prompt, not by a row count.
func belowPrompt(re *regexp.Regexp) cond {
	return cond{kind: condBelowPrompt, re: re}
}

func blocked(id string) Signal { return Signal{State: Blocked, Strong: true, Rule: id} }
func hold(id string) Signal    { return Signal{Hold: true, Rule: id} }
func working(id string) Signal { return Signal{State: Working, Rule: id} }
func workingStrong(id string) Signal {
	return Signal{State: Working, Strong: true, Rule: id}
}
func idle(id string) Signal       { return Signal{State: Idle, Rule: id} }
func idleStrong(id string) Signal { return Signal{State: Idle, Strong: true, Rule: id} }

func conds(c ...cond) []cond { return c }

// isPromptLine reports whether line starts, after indentation, with marker.
func isPromptLine(line []byte, marker string) bool {
	if marker == "" {
		return false
	}
	return bytes.HasPrefix(bytes.TrimLeft(line, " "), []byte(marker))
}

// containsFold reports whether hay contains the lowercase needle, folding
// ASCII letters of hay. Non-ASCII bytes compare exactly.
func containsFold(hay []byte, needle string) bool {
	n := len(needle)
	if n == 0 {
		return true
	}
	for i := 0; i+n <= len(hay); i++ {
		if lowerASCII(hay[i]) != needle[0] {
			continue
		}
		j := 1
		for j < n && lowerASCII(hay[i+j]) == needle[j] {
			j++
		}
		if j == n {
			return true
		}
	}
	return false
}

func lowerASCII(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}
