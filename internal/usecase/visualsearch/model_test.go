package visualsearch

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/pkg/renderer"
)

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

func TestVisualSearchCloneDoesNotAliasInput(t *testing.T) {
	m := New(testSnapshot("alpha", "beta alpha"))
	for _, r := range "alpha" {
		m.Insert(r)
	}
	clone := m.Clone()

	clone.Insert('z')

	require.Equal(t, "alpha", m.Query())
	require.Equal(t, "alphaz", clone.Query())
	require.Len(t, m.Matches(), 2)
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
