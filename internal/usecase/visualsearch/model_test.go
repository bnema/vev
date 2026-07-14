package visualsearch

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/pkg/renderer"
)

var benchmarkModelSink *Model

func testSnapshot(lines ...string) scopy.Snapshot {
	rows := make([][]renderer.Cell, len(lines))
	for i, line := range lines {
		runes := []rune(line)
		rows[i] = make([]renderer.Cell, len(runes))
		for x, r := range runes {
			rows[i][x] = renderer.Cell{Rune: r}
		}
	}
	return scopy.NewSnapshotFromRows(rows, 40, 4)
}

func TestVisualSearchModelFiltersAndSelectsLineMatches(t *testing.T) {
	m := New(testSnapshot("alpha", "beta alpha", "gamma"))

	for _, r := range "alpha" {
		m.Insert(r)
	}

	matches := m.Matches()
	require.Len(t, matches, 2)
	require.Equal(t, 0, matches[0].Row)
	require.Equal(t, "alpha", matches[0].Text)
	require.Equal(t, 1, matches[1].Row)
	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, matches[0], selected)

	m.Down()
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, matches[1], selected)
}

func TestVisualSearchNewSnapshotOwnsSourceRows(t *testing.T) {
	rows := [][]renderer.Cell{{{Rune: 'a'}, {Rune: 'l'}, {Rune: 'p'}, {Rune: 'h'}, {Rune: 'a'}}}
	m := New(scopy.NewSnapshotFromRows(rows, 40, 1))

	rows[0][0].Rune = 'z'
	for _, r := range "alpha" {
		m.Insert(r)
	}

	matches := m.Matches()
	require.Len(t, matches, 1)
	require.Equal(t, "alpha", matches[0].Text)
}

func TestVisualSearchCloneRetainsDocumentAndKeepsUIStateIndependent(t *testing.T) {
	m := New(testSnapshot("alpha", "beta alpha"))
	for _, r := range "alpha" {
		m.Insert(r)
	}
	clone := m.Clone()

	require.Equal(t, m.snapshot.Row(0), clone.snapshot.Row(0))
	clone.Down()
	clone.Insert('z')

	require.Equal(t, "alpha", m.Query())
	require.Equal(t, 0, m.SelectedIndex())
	require.Len(t, m.Matches(), 2)
	require.Equal(t, "alphaz", clone.Query())
	require.Equal(t, -1, clone.SelectedIndex())
	require.Len(t, clone.Matches(), 0)
}

func TestVisualSearchRenderDistinguishesMultipleMatchesOnSameLine(t *testing.T) {
	m := New(testSnapshot("alpha alpha"))
	for _, r := range "alpha" {
		m.Insert(r)
	}

	frame := m.Render(domain.Size{Cols: 40, Rows: 4})
	text := rowText(frame.Row(1)) + "\n" + rowText(frame.Row(2))

	require.Contains(t, text, "1:1")
	require.Contains(t, text, "1:7")
}

func TestVisualSearchRenderShowsInputAndLineResults(t *testing.T) {
	m := New(testSnapshot("alpha", "beta alpha", "gamma"))
	for _, r := range "alpha" {
		m.Insert(r)
	}

	frame := m.Render(domain.Size{Cols: 40, Rows: 4})

	text := rowText(frame.Row(0)) + "\n" + rowText(frame.Row(1)) + "\n" + rowText(frame.Row(2))
	require.Contains(t, text, "/alpha")
	require.Contains(t, text, "1:1  alpha")
	require.Contains(t, text, "2:6  beta alpha")
}

func TestVisualSearchCloneAllocationIsWidthIndependent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark-driven allocation gate in short mode")
	}
	narrow := benchmarkVisualSearchCloneBytes(16)
	wide := benchmarkVisualSearchCloneBytes(512)
	assertWidthIndependentCloneAllocations(t, narrow, wide)
}

// The comparison holds row and match counts equal. Model.Clone must copy only
// mutable UI/match state, so a document-row clone is observable as width-scaled
// B/op without relying on a Go-version-specific absolute allocation budget.
func benchmarkVisualSearchCloneBytes(width int) int64 {
	const rows = 256

	line := strings.Repeat("x", width-7) + "needle"
	lines := make([]string, rows)
	for i := range lines {
		lines[i] = line
	}
	model := New(testSnapshot(lines...))
	for _, r := range "needle" {
		model.Insert(r)
	}
	if got := len(model.Matches()); got != rows {
		panic("visual-search allocation gate requires one match per row")
	}

	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchmarkModelSink = model.Clone()
		}
	})
	return result.AllocedBytesPerOp()
}

func assertWidthIndependentCloneAllocations(t *testing.T, narrow, wide int64) {
	t.Helper()
	const conservativeWidthTolerance = 2
	if wide > narrow*conservativeWidthTolerance {
		t.Fatalf("VisualSearch Model.Clone B/op scaled with row width: narrow=%d wide=%d (limit=%d)", narrow, wide, narrow*conservativeWidthTolerance)
	}
}

func BenchmarkVisualSearchClone10KRows(b *testing.B) {
	const historyRows = 10_000

	lines := make([]string, historyRows)
	for i := range lines {
		lines[i] = "immutable history row"
	}
	model := New(testSnapshot(lines...))
	for _, r := range "history" {
		model.Insert(r)
	}
	if got := len(model.Matches()); got != historyRows {
		b.Fatalf("representative query matches = %d, want %d", got, historyRows)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkModelSink = model.Clone()
	}
}

func rowText(row []renderer.Cell) string {
	var b strings.Builder
	for _, c := range row {
		if c.Rune == 0 {
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(c.Rune)
	}
	return b.String()
}
