package agentactivity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type classifyCase struct {
	fixture string
	want    Signal
}

var classifyCases = map[Agent][]classifyCase{
	Claude: {
		{"working-interrupt", working("claude.interrupt-hint")},
		{"working-activity", working("claude.activity")},
		{"working-title", workingStrong("claude.title-spinner")},
		{"idle-prompt", idle("claude.prompt")},
		{"idle-title", idleStrong("claude.title-idle")},
		{"blocked-permission", blocked("claude.permission")},
		{"blocked-edit", blocked("claude.permission")},
		{"blocked-question", blocked("claude.question")},
		{"hold-transcript", hold("claude.transcript")},
		// Negatives.
		{"model-picker", Signal{}},
		{"quoted-permission", idle("claude.prompt")},
		{"user-typed", idle("claude.prompt")},
		{"scrollback-working", idle("claude.prompt")},
		{"user-typed-multiline", idle("claude.prompt")},
		{"quoted-status", idle("claude.prompt")},
		{"quoted-activity", idle("claude.prompt")},
		{"user-typed-wrapped", idle("claude.prompt")},
		{"working-narrow", working("claude.activity")},
	},
	Codex: {
		{"working-status", working("codex.status")},
		{"working-queued", working("codex.status")},
		{"working-elapsed", working("codex.status-elapsed")},
		{"working-title", workingStrong("codex.title-spinner")},
		{"idle-composer", idle("codex.composer")},
		{"blocked-approval", blocked("codex.approval")},
		{"blocked-question", blocked("codex.question")},
		{"blocked-trust", blocked("codex.trust")},
		{"blocked-title", blocked("codex.title-action")},
		{"hold-transcript", hold("codex.transcript")},
		// Negatives.
		{"quoted-approval", idle("codex.composer")},
		{"user-typed", idle("codex.composer")},
		{"shell-title", idle("codex.composer")},
		{"history-elapsed", idle("codex.composer")},
		{"working-narrow", working("codex.status")},
	},
	OpenCode: {
		{"working-interrupt", working("opencode.interrupt-hint")},
		{"working-armed", working("opencode.interrupt-hint")},
		{"working-remapped", working("opencode.interrupt-hint")},
		{"working-progress", working("opencode.progress")},
		{"idle-quiet", idle("opencode.quiet")},
		{"blocked-permission", blocked("opencode.permission")},
		{"blocked-question", blocked("opencode.question")},
		// Negatives.
		{"settings-dialog", idle("opencode.quiet")},
		{"scrollback-permission", idle("opencode.quiet")},
		{"answered-permission", idle("opencode.quiet")},
		{"user-typed", idle("opencode.quiet")},
	},
	Zut: {
		{"working-above-input", working("zut.busy")},
		{"working-below-input", working("zut.busy")},
		{"working-status-prefix", working("zut.busy")},
		{"working-fresh", working("zut.busy")},
		{"working-narrow-split", working("zut.busy")},
		{"working-subagent", working("zut.subagent")},
		{"working-subagent-model", working("zut.subagent")},
		{"working-subagent-narrow", working("zut.subagent")},
		{"working-subagent-deep", working("zut.subagent")},
		{"blocked-confirm", blocked("zut.confirm")},
		{"blocked-confirm-drawer", blocked("zut.confirm")},
		{"blocked-approval", blocked("zut.approval")},
		{"blocked-question", blocked("zut.question")},
		{"blocked-question-drawer", blocked("zut.question")},
		{"blocked-question-answer", blocked("zut.question")},
		{"blocked-question-tall", blocked("zut.question")},
		{"blocked-question-status", blocked("zut.asking")},
		{"idle-quiet", idle("zut.quiet")},
		{"idle-fresh", idle("zut.quiet")},
		// Negatives.
		{"chat-bullets", idle("zut.quiet")},
		{"user-typed", idle("zut.quiet")},
		{"quoted-confirm", idle("zut.quiet")},
		{"quoted-question", idle("zut.quiet")},
		{"chat-checkmarks", idle("zut.quiet")},
		{"chat-subagent", idle("zut.quiet")},
		{"subagent-queued", idle("zut.quiet")},
	},
}

func TestClassifyFixtures(t *testing.T) {
	for agent, cases := range classifyCases {
		for _, tc := range cases {
			t.Run(agent.String()+"/"+tc.fixture, func(t *testing.T) {
				var e Extractor
				in := loadFixture(t, agent.String(), tc.fixture).feed(&e)
				if got := Classify(agent, in); got != tc.want {
					t.Fatalf("Classify = %+v, want %+v", got, tc.want)
				}
			})
		}
	}
}

// TestClassifyFixturesCovered keeps the fixture directory and the case table
// in sync, and checks every rule of every table has a positive fixture.
func TestClassifyFixturesCovered(t *testing.T) {
	for agent, cases := range classifyCases {
		listed := make(map[string]bool, len(cases))
		fired := make(map[string]bool)
		for _, tc := range cases {
			listed[tc.fixture] = true
			fired[tc.want.Rule] = true
		}
		files, err := filepath.Glob(filepath.Join("testdata", agent.String(), "*.txt"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if name := strings.TrimSuffix(filepath.Base(f), ".txt"); !listed[name] {
				t.Errorf("%s: fixture %s has no test case", agent, name)
			}
		}
		for name := range listed {
			if _, err := os.Stat(filepath.Join("testdata", agent.String(), name+".txt")); err != nil {
				t.Errorf("%s: case %s has no fixture", agent, name)
			}
		}
		for _, r := range tables[agent].rules {
			if !fired[r.signal.Rule] && !inlineCovered[r.signal.Rule] {
				t.Errorf("%s: rule %s has no positive case", agent, r.signal.Rule)
			}
		}
	}
}

// inlineCovered lists rules exercised by TestClassifyInline instead of a
// fixture.
var inlineCovered = map[string]bool{}

func init() {
	for _, tc := range inlineCases {
		inlineCovered[tc.want.Rule] = true
	}
}

var inlineCases = []struct {
	name  string
	agent Agent
	lines []string
	title string
	from  bool
	want  Signal
}{
	{
		name:  "claude question review",
		agent: Claude,
		lines: []string{
			"Review your answers",
			"",
			" ● Which clock should the test use?",
			"   → Fake clock",
			"",
			"❯ 1. Submit answers",
			"  2. Cancel",
			"",
			"←/→ to switch · Esc to cancel",
		},
		want: blocked("claude.question-review"),
	},
	{
		name:  "claude remote permission wait",
		agent: Claude,
		lines: []string{"⏺ Bash(make deploy)", "  Waiting for permission…"},
		want:  blocked("claude.permission-wait"),
	},
	{
		name:  "claude idle title beats a lingering activity line",
		agent: Claude,
		lines: []string{"✶ Reticulating…", "", "──────", "❯ ", "──────"},
		title: "✳ Fix flaky retry test",
		from:  true,
		want:  idleStrong("claude.title-idle"),
	},
	{
		name:  "claude interrupt hint outside the activity line",
		agent: Claude,
		lines: []string{"⏺ Press esc to interrupt a running turn.", "", "──────", "❯ ", "──────"},
		want:  idle("claude.prompt"),
	},
	{
		name:  "claude ascii star list item is not activity",
		agent: Claude,
		lines: []string{"  * Loading…", "", "──────", "❯ ", "──────"},
		want:  idle("claude.prompt"),
	},
	{
		name:  "claude background work",
		agent: Claude,
		lines: []string{"⏺ Done.", "  Waiting for background work to finish", "❯ "},
		want:  working("claude.background"),
	},
	{
		name:  "claude braille title spinner (<= 2.1.227)",
		agent: Claude,
		lines: []string{"❯ "},
		title: "⠂ Fix flaky retry test",
		from:  true,
		want:  workingStrong("claude.title-spinner"),
	},
	{
		name:  "claude spinner title not set by the agent",
		agent: Claude,
		lines: []string{"❯ "},
		title: "◐ fish",
		want:  idle("claude.prompt"),
	},
	{
		name:  "claude menu cursor is not the prompt",
		agent: Claude,
		lines: []string{"❯ 1. Default", "  2. Sonnet"},
		want:  Signal{},
	},
	{
		name:  "claude prompt with a typed number",
		agent: Claude,
		lines: []string{"❯ 42 is the answer"},
		want:  idle("claude.prompt"),
	},
	{
		name:  "codex trust footer without the question text",
		agent: Codex,
		lines: []string{"› 1. Trust and continue", "  2. Quit", "", "  enter continue and create sandbox · esc quit"},
		want:  blocked("codex.trust-footer"),
	},
	{
		name:  "codex finished elapsed in history is indented",
		agent: Codex,
		lines: []string{"  Context compacted · 12s", "  ran tests (12s)", "› "},
		want:  idle("codex.composer"),
	},
	{
		name:  "codex elapsed status above details and queued messages",
		agent: Codex,
		lines: []string{
			"Running tests (3m 07s)",
			"  └ go test ./cmd/...",
			"",
			"• Queued follow-up inputs",
			"  ↳ also update the docs",
			"",
			"› Ask Codex to do anything",
		},
		want: working("codex.status-elapsed"),
	},
	{
		name:  "codex composer far above the bottom is not the live composer",
		agent: Codex,
		lines: []string{"› old message", "", "a", "b", "c", "d", "e", "f"},
		want:  Signal{},
	},
	{
		name:  "claude prompt far above the bottom without a border is not live",
		agent: Claude,
		lines: []string{"❯ old prompt", "a", "b", "c", "d", "e", "f"},
		want:  Signal{},
	},
	{
		name:  "codex reconnect line is not working",
		agent: Codex,
		lines: []string{"• Reconnect failed (12s)", "", "› Ask Codex to do anything"},
		want:  idle("codex.composer"),
	},
	{
		name:  "codex working with inline message",
		agent: Codex,
		lines: []string{"• Thinking (4s • esc to interrupt) · 1 background terminal running", "", "› "},
		want:  working("codex.status"),
	},
	{
		name:  "opencode permission footer alone",
		agent: OpenCode,
		lines: []string{"  ┃  $ rm -rf build", "   ⇆ select  enter confirm  esc reject"},
		want:  blocked("opencode.permission"),
	},
	{
		name:  "opencode question answer editor",
		agent: OpenCode,
		lines: []string{"  ┃  Type your own answer", "  ┃  > json lines please", "   enter submit · esc back"},
		want:  blocked("opencode.question-answer"),
	},
	{
		name:  "opencode multi-question form",
		agent: OpenCode,
		lines: []string{"  ┃  Pick an output format", "   up/down select   enter choose   tab next   esc dismiss"},
		want:  blocked("opencode.question-form"),
	},
	{
		name:  "zut working line with hours",
		agent: Zut,
		lines: []string{"  ⣾ Compacting conversation history - 1h2m3s"},
		want:  working("zut.busy"),
	},
	{
		name:  "zut busy line wins over the subagent rule",
		agent: Zut,
		lines: []string{"  ⠙ Running tool: subagent - 2m14s  [1] gpt-5.4  ctx0%/272k"},
		want:  working("zut.busy"),
	},
	{
		name:  "zut subagent waiting for its model",
		agent: Zut,
		lines: []string{"▌", "  ⣾ explorer · zzzZ · activity 1d02h ago · 1d02h"},
		want:  working("zut.subagent"),
	},
	{
		name:  "zut subagent row truncated after the status word",
		agent: Zut,
		lines: []string{"▌", "  ⠋ worker · running ..."},
		want:  working("zut.subagent"),
	},
	{
		name:  "zut subagent row truncated inside the status word is a documented miss",
		agent: Zut,
		lines: []string{"▌", "  ⠋ worker · runn..."},
		want:  idle("zut.quiet"),
	},
	{
		name:  "zut subagent row above the prompt is chat",
		agent: Zut,
		lines: []string{"  ⠋ worker · running · activity 3s ago · 5s", "▌"},
		want:  idle("zut.quiet"),
	},
	{
		name:  "zut subagent overflow line alone",
		agent: Zut,
		lines: []string{"  … 4 more active subagents"},
		want:  idle("zut.quiet"),
	},
	{
		name:  "zut subagent typed into the input",
		agent: Zut,
		lines: []string{"▌ ⠋ worker · running · activity 3s ago · 5s"},
		want:  idle("zut.quiet"),
	},
	{
		name:  "none classifies nothing",
		agent: None,
		lines: []string{"esc to interrupt"},
		want:  Signal{},
	},
}

func TestClassifyInline(t *testing.T) {
	for _, tc := range inlineCases {
		t.Run(tc.name, func(t *testing.T) {
			var e Extractor
			e.Reset()
			for _, l := range tc.lines {
				addLine(&e, l)
			}
			if got := Classify(tc.agent, e.Input(tc.title, tc.from)); got != tc.want {
				t.Fatalf("Classify = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestClassifyRuleIDsUnique(t *testing.T) {
	seen := make(map[string]bool)
	for a, tbl := range tables {
		if tbl == nil {
			continue
		}
		for _, r := range tbl.rules {
			id := r.signal.Rule
			if id == "" {
				t.Errorf("%s: rule without an id", Agent(a))
			}
			if !strings.HasPrefix(id, Agent(a).String()+".") {
				t.Errorf("%s: rule %q lacks the agent prefix", Agent(a), id)
			}
			if seen[id] {
				t.Errorf("duplicate rule id %q", id)
			}
			seen[id] = true
		}
	}
}

func TestContainsFold(t *testing.T) {
	tests := []struct {
		hay, needle string
		want        bool
	}{
		{"Press ESC to Interrupt", "esc to interrupt", true},
		{"esc to interrup", "esc to interrupt", false},
		{"", "x", false},
		{"anything", "", true},
		{"⇆ select  enter confirm", "⇆ select", true},
		{"É", "é", false}, // non-ASCII compares exactly
	}
	for _, tt := range tests {
		if got := containsFold([]byte(tt.hay), tt.needle); got != tt.want {
			t.Errorf("containsFold(%q, %q) = %t, want %t", tt.hay, tt.needle, got, tt.want)
		}
	}
}
