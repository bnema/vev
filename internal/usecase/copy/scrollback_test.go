package copy

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func row(text string) []renderer.Cell {
	runes := []rune(text)
	cells := make([]renderer.Cell, len(runes))
	for i, r := range runes {
		cells[i] = renderer.Cell{Rune: r, Style: renderer.DefaultStyle()}
	}
	return cells
}

func rowText(cells []renderer.Cell) string {
	runes := make([]rune, len(cells))
	for i, c := range cells {
		runes[i] = c.Rune
	}
	return string(runes)
}

func TestScrollbackRing(t *testing.T) {
	tests := []struct {
		name     string
		cap      int
		append   []string
		wantHead int
		wantLen  int
		wantRows []string
	}{
		{
			name:     "empty ring drops rows",
			cap:      0,
			append:   []string{"aa", "bb"},
			wantHead: 0,
			wantLen:  0,
		},
		{
			name:     "below capacity keeps head at zero",
			cap:      3,
			append:   []string{"aa", "bb"},
			wantHead: 0,
			wantLen:  2,
			wantRows: []string{"aa", "bb"},
		},
		{
			name:     "at capacity fills without rotating head",
			cap:      3,
			append:   []string{"aa", "bb", "cc"},
			wantHead: 0,
			wantLen:  3,
			wantRows: []string{"aa", "bb", "cc"},
		},
		{
			name:     "over capacity evicts oldest and rotates head",
			cap:      3,
			append:   []string{"aa", "bb", "cc", "dd", "ee"},
			wantHead: 2,
			wantLen:  3,
			wantRows: []string{"cc", "dd", "ee"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := NewScrollback(tt.cap)
			for _, r := range tt.append {
				sb.Append(row(r))
			}

			if got := sb.Cap(); got != tt.cap {
				t.Fatalf("Cap() = %d, want %d", got, tt.cap)
			}
			if got := sb.Head(); got != tt.wantHead {
				t.Fatalf("Head() = %d, want %d", got, tt.wantHead)
			}
			if got := sb.Len(); got != tt.wantLen {
				t.Fatalf("Len() = %d, want %d", got, tt.wantLen)
			}
			for i, want := range tt.wantRows {
				if got := rowText(sb.Row(i)); got != want {
					t.Fatalf("Row(%d) = %q, want %q", i, got, want)
				}
			}
		})
	}
}

func TestScrollbackCopiesRows(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "append copies caller row"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := NewScrollback(1)
			r := row("ab")
			sb.Append(r)
			r[0].Rune = 'z'

			got := sb.Row(0)
			got[1].Rune = 'y'
			if text := rowText(sb.Row(0)); text != "ab" {
				t.Fatalf("stored row = %q, want copy unaffected by caller/read mutation", text)
			}
		})
	}
}

func TestScrollbackSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		cap      int
		append   []string
		wantRows []string
	}{
		{
			name:     "empty capacity snapshots no rows",
			cap:      0,
			append:   []string{"aa", "bb"},
			wantRows: nil,
		},
		{
			name:     "oldest first after ring wrap",
			cap:      3,
			append:   []string{"aa", "bb", "cc", "dd"},
			wantRows: []string{"bb", "cc", "dd"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := NewScrollback(tt.cap)
			for _, r := range tt.append {
				sb.Append(row(r))
			}

			got := sb.Snapshot()
			if len(got) != len(tt.wantRows) {
				t.Fatalf("Snapshot() len = %d, want %d", len(got), len(tt.wantRows))
			}
			for i, want := range tt.wantRows {
				if text := rowText(got[i]); text != want {
					t.Fatalf("Snapshot()[%d] = %q, want %q", i, text, want)
				}
			}
			if len(got) > 0 {
				got[0] = row("zz")
				if text := rowText(sb.Row(0)); text != tt.wantRows[0] {
					t.Fatalf("Snapshot() outer slice mutation changed storage: Row(0) = %q, want %q", text, tt.wantRows[0])
				}
			}
		})
	}
}

func TestScrollbackAppendDoesNotMutateSnapshottedRows(t *testing.T) {
	sb := NewScrollback(2)
	sb.Append(row("aa"))
	sb.Append(row("bb"))
	snapshot := sb.Snapshot()

	sb.Append(row("cc"))
	sb.Append(row("dd"))

	if text := rowText(snapshot[0]); text != "aa" {
		t.Fatalf("snapshot[0] after append = %q, want original row", text)
	}
	if text := rowText(snapshot[1]); text != "bb" {
		t.Fatalf("snapshot[1] after append = %q, want original row", text)
	}
}
