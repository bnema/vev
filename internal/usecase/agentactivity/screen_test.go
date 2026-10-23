package agentactivity

import (
	"testing"
	"unicode/utf8"
)

type cell struct {
	r    rune
	cont bool
}

func row(s string) []cell {
	var cells []cell
	for _, r := range s {
		cells = append(cells, cell{r: r})
	}
	return cells
}

func TestExtractor(t *testing.T) {
	tests := []struct {
		name string
		rows [][]cell
		want []string // lowest first
	}{
		{
			name: "trailing blanks trimmed and blank rows dropped",
			rows: [][]cell{row("top   "), row("      "), row(""), row("  bottom  ")},
			want: []string{"  bottom", "top"},
		},
		{
			name: "continuation cells skipped",
			rows: [][]cell{{{r: '漢'}, {cont: true}, {r: 'x'}}},
			want: []string{"漢x"},
		},
		{
			name: "control and invalid runes read as spaces",
			rows: [][]cell{{{r: 'a'}, {r: 0}, {r: 0x1b}, {r: 0x85}, {r: 0xD800}, {r: 'b'}}},
			want: []string{"a    b"},
		},
		{
			name: "empty screen",
			rows: [][]cell{row(""), row(" ")},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e Extractor
			e.Reset()
			for _, r := range tt.rows {
				e.BeginRow()
				for _, c := range r {
					e.WriteCell(c.r, c.cont)
				}
				e.EndRow()
			}
			in := e.Input("t", true)
			if len(in.Lines) != len(tt.want) {
				t.Fatalf("got %d lines %q, want %q", len(in.Lines), in.Lines, tt.want)
			}
			for i, l := range in.Lines {
				if string(l) != tt.want[i] {
					t.Errorf("line %d = %q, want %q", i, l, tt.want[i])
				}
			}
			if in.Title != "t" || !in.TitleFromAgent {
				t.Errorf("title = %q/%t", in.Title, in.TitleFromAgent)
			}
		})
	}
}

func TestExtractorLinesDoNotAlias(t *testing.T) {
	var e Extractor
	e.Reset()
	addLine(&e, "one")
	addLine(&e, "two")
	in := e.Input("", false)
	// Appending to a returned line must not overwrite its neighbour.
	_ = append(in.Lines[1], 'X')
	if string(in.Lines[0]) != "two" {
		t.Fatalf("neighbour overwritten: %q", in.Lines[0])
	}
}

func TestWindow(t *testing.T) {
	tests := []struct {
		name               string
		height, blankFrom  int
		wantStart, wantEnd int
	}{
		{name: "empty grid", height: 0, blankFrom: 0, wantStart: 0, wantEnd: 0},
		{name: "all blank", height: 30, blankFrom: 0, wantStart: 0, wantEnd: 0},
		{name: "short full grid", height: 5, blankFrom: 5, wantStart: 0, wantEnd: 5},
		{name: "exactly the window", height: BottomRows, blankFrom: BottomRows, wantStart: 0, wantEnd: BottomRows},
		{name: "one row over", height: BottomRows + 1, blankFrom: BottomRows + 1, wantStart: 1, wantEnd: BottomRows + 1},
		{name: "full tall grid", height: 50, blankFrom: 50, wantStart: 50 - BottomRows, wantEnd: 50},
		// An inline renderer leaves the rows under its prompt blank.
		{name: "content ends above blank tail", height: 52, blankFrom: 10, wantStart: 0, wantEnd: 10},
		{name: "tall content above blank tail", height: 52, blankFrom: 30, wantStart: 30 - BottomRows, wantEnd: 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := Window(tt.height, func(y int) bool { return y >= tt.blankFrom })
			if start != tt.wantStart || end != tt.wantEnd {
				t.Fatalf("Window = [%d, %d), want [%d, %d)", start, end, tt.wantStart, tt.wantEnd)
			}
		})
	}
}

func TestBlankCell(t *testing.T) {
	tests := []struct {
		r            rune
		continuation bool
		want         bool
	}{
		{0, false, true}, {' ', false, true}, {'\t', false, true}, {0x85, false, true},
		{utf8.MaxRune + 1, false, true}, {'x', true, true},
		{'x', false, false}, {'▌', false, false}, {'⠋', false, false},
	}
	for _, tt := range tests {
		if got := BlankCell(tt.r, tt.continuation); got != tt.want {
			t.Errorf("BlankCell(%U, %t) = %t, want %t", tt.r, tt.continuation, got, tt.want)
		}
	}
}

func TestExtractorCommit(t *testing.T) {
	steps := []struct {
		name  string
		lines []string
		title string
		want  bool
	}{
		{"first observation", []string{"a", "b"}, "x", true},
		{"identical", []string{"a", "b"}, "x", false},
		{"title changed", []string{"a", "b"}, "y", true},
		{"text changed", []string{"a", "c"}, "y", true},
		{"same bytes, different row split", []string{"ac"}, "y", true},
		{"trailing spaces only", []string{"ac  "}, "y", false},
	}
	var e Extractor
	for _, s := range steps {
		e.Reset()
		for _, l := range s.lines {
			addLine(&e, l)
		}
		if got := e.Commit(s.title); got != s.want {
			t.Fatalf("%s: Commit = %t, want %t", s.name, got, s.want)
		}
	}
	e.Forget()
	if !e.Commit("y") {
		t.Fatal("Commit after Forget = false, want true")
	}
}
