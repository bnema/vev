package copy

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestNewSnapshotFreezesHistoryAndVisibleScreen(t *testing.T) {
	sb := NewScrollback(1)
	sb.Append(row("old"))
	screen := renderer.NewFrame(3, 1)
	copy(screen.Row(0), row("one"))

	snapshot := NewSnapshot(sb, screen)
	if &snapshot.Row(0)[0] != &sb.View().Row(0)[0] {
		t.Fatal("NewSnapshot() deep-copied immutable history")
	}
	sb.Append(row("new"))
	screen.Set(0, 0, renderer.Cell{Rune: 'x'})

	if got := snapshot.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	for i, want := range []string{"old", "one"} {
		if got := rowText(snapshot.Row(i)); got != want {
			t.Errorf("Row(%d) = %q, want %q", i, got, want)
		}
	}
	for _, i := range []int{-1, 2} {
		if got := snapshot.Row(i); got != nil {
			t.Errorf("Row(%d) = %v, want nil", i, got)
		}
	}
}

func TestNewSnapshotFromRowsOwnsCallerRows(t *testing.T) {
	rows := [][]renderer.Cell{row("one")}
	snapshot := NewSnapshotFromRows(rows, 3, 1)
	rows[0][0].Rune = 'x'
	if got := rowText(snapshot.Row(0)); got != "one" {
		t.Fatalf("Row(0) = %q, want deep-owned caller row", got)
	}
}
