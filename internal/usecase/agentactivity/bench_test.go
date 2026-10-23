package agentactivity

import (
	"testing"
	"time"
)

// gridFixture lays a fixture out as a width-wide cell grid, as the daemon
// would read it from the VT screen.
func gridFixture(t testing.TB, agent, name string, width int) (rows [][]rune, title string, fromAgent bool) {
	f := loadFixture(t, agent, name)
	for _, l := range f.window() {
		r := make([]rune, width)
		for i := range r {
			r[i] = ' '
		}
		copy(r, []rune(l))
		rows = append(rows, r)
	}
	return rows, f.title, f.titleFromAgent
}

func extractGrid(e *Extractor, rows [][]rune, title string, from bool) Input {
	e.Reset()
	for _, row := range rows {
		e.BeginRow()
		for _, r := range row {
			e.WriteCell(r, false)
		}
		e.EndRow()
	}
	return e.Input(title, from)
}

var benchCases = []struct {
	agent   Agent
	fixture string
}{
	{Claude, "working-interrupt"},
	{Claude, "idle-prompt"},
	{Codex, "working-status"},
	{Codex, "idle-composer"},
	{OpenCode, "idle-quiet"},
	{Zut, "blocked-confirm"},
}

var sink Signal

func BenchmarkClassify(b *testing.B) {
	for _, bc := range benchCases {
		b.Run(bc.agent.String()+"/"+bc.fixture, func(b *testing.B) {
			var e Extractor
			in := loadFixture(b, bc.agent.String(), bc.fixture).feed(&e)
			b.ReportAllocs()
			for b.Loop() {
				sink = Classify(bc.agent, in)
			}
		})
	}
}

func BenchmarkExtractClassify(b *testing.B) {
	rows, title, from := gridFixture(b, "claude", "working-interrupt", 200)
	var e Extractor
	extractGrid(&e, rows, title, from)
	b.ReportAllocs()
	for b.Loop() {
		in := extractGrid(&e, rows, title, from)
		e.Commit(title)
		sink = Classify(Claude, in)
	}
}

func BenchmarkTrackerObserve(b *testing.B) {
	t0 := time.Unix(0, 0)
	var tr Tracker
	tr.Identify(Claude, t0)
	sigs := []Signal{sigWorking, sigIdleWeak, sigWorking, sigIdleWeak}
	now := t0
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		now = now.Add(100 * ms)
		tr.Observe(sigs[i%len(sigs)], now)
		i++
	}
}

// TestSteadyStateAllocs pins the 0-alloc budget of a warmed scan: extract,
// commit, classify, and observe for every agent table.
func TestSteadyStateAllocs(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("allocation counts need a non-race, non-short run")
	}
	for agent, cases := range classifyCases {
		for _, tc := range cases {
			rows, title, from := gridFixture(t, agent.String(), tc.fixture, 160)
			var e Extractor
			var tr Tracker
			t0 := time.Unix(0, 0)
			tr.Identify(agent, t0)
			extractGrid(&e, rows, title, from)
			e.Commit(title)
			now := t0
			allocs := testing.AllocsPerRun(50, func() {
				in := extractGrid(&e, rows, title, from)
				e.Commit(title)
				now = now.Add(10 * ms)
				tr.Observe(Classify(agent, in), now)
			})
			if allocs != 0 {
				t.Errorf("%s/%s: %v allocs per scan, want 0", agent, tc.fixture, allocs)
			}
		}
	}
}
