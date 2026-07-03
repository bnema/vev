package picker

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestNewFlattensAndSelectsCurrentTab(t *testing.T) {
	m := New([]SessionView{
		{ID: "s1", Name: "one", Tabs: []string{"shell", "logs"}, Active: 0},
		{ID: "s2", Name: "two", Tabs: []string{"api"}, Active: 0},
	}, "s1", 1)

	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 1}, got)
	frame := m.Render(domain.Size{Cols: 20, Rows: 5}, Preview{})
	require.Equal(t, 'o', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(0, 2).Rune)
	require.Equal(t, 'l', frame.At(2, 2).Rune)
	require.True(t, frame.At(0, 2).Style.Inverse)
}

func TestNewFallsBackToActiveThenFirstLeaf(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []string{"shell", "logs"}, Active: 1}}, "missing", 0)
	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 1}, got)

	m = New([]SessionView{{ID: "s1", Name: "one", Tabs: []string{"shell"}, Active: 4}}, "missing", 0)
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 0}, got)
}

func TestUpDownSkipsHeadersClampsAndCrossesSessions(t *testing.T) {
	m := New([]SessionView{
		{ID: "s1", Name: "one", Tabs: []string{"a", "b"}, Active: 0},
		{ID: "s2", Name: "two", Tabs: []string{"c"}, Active: 0},
	}, "s1", 0)

	m.Up()
	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 0}, got, "up at first tab clamps")
	m.Down()
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 1}, got)
	m.Down()
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s2", TabIndex: 0}, got, "down skips second session header")
	m.Down()
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s2", TabIndex: 0}, got, "down at last tab clamps")
	m.Up()
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 1}, got, "up skips second session header")
}

func TestChooseLayoutBoundaries(t *testing.T) {
	layout := ChooseLayout(domain.Size{Cols: 39, Rows: 12})
	require.Equal(t, domain.Rect{Width: 39, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{Y: 5, Width: 39, Height: 7}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 40, Rows: 4})
	require.Equal(t, domain.Rect{Width: 40, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 41, Rows: 4})
	require.Equal(t, domain.Rect{Width: 16, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{X: 17, Width: 24, Height: 4}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 120, Rows: 4})
	require.Equal(t, domain.Rect{Width: MaxListWidth, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{X: MaxListWidth + 1, Width: 87, Height: 4}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 24, Rows: 11})
	require.Equal(t, domain.Rect{Width: 24, Height: 11}, layout.List)
	require.Equal(t, domain.Rect{}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 24, Rows: 12})
	require.Equal(t, domain.Rect{Width: 24, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{Y: 5, Width: 24, Height: 7}, layout.Preview)
}

func TestSelectedMapping(t *testing.T) {
	m := New([]SessionView{
		{ID: "alpha", Name: "alpha", Tabs: []string{"one"}, Active: 0},
		{ID: "beta", Name: "beta", Tabs: []string{"two", "three"}, Active: 0},
	}, "beta", 1)

	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "beta", TabIndex: 1}, got)
}

func TestStoppedSessionSelectableAndRendered(t *testing.T) {
	m := New([]SessionView{{ID: "stopped:work", Name: "work", Tabs: []string{"(stopped)"}, Stopped: true}}, "", 0)
	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "stopped:work", Name: "work", TabIndex: 0, Stopped: true}, got)
	frame := m.Render(domain.Size{Cols: 24, Rows: 4}, Preview{})
	require.Equal(t, 'w', frame.At(0, 0).Rune)
	require.Equal(t, '(', frame.At(5, 0).Rune)
}

func TestRenderPreviewClipsPadsDropsWideRuneAndInvertsSelection(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []string{"tab"}, Active: 0}}, "s1", 0)
	preview := Preview{
		Width:  24,
		Height: 1,
		Rows: [][]renderer.Cell{{
			cell('a'), cell('b'), cell('c'), cell('d'), cell('e'), cell('f'), cell('g'), cell('h'), cell('i'), cell('j'),
			cell('k'), cell('l'), cell('m'), cell('n'), cell('o'), cell('p'), cell('q'), cell('r'), cell('s'), cell('t'),
			cell('u'), cell('v'), cell('w'), {Rune: '界', Style: renderer.DefaultStyle()}, {Continuation: true, Style: renderer.DefaultStyle()},
		}},
	}

	frame := m.Render(domain.Size{Cols: 41, Rows: 4}, preview)
	require.True(t, frame.At(0, 1).Style.Inverse)
	require.Equal(t, 'a', frame.At(17, 0).Rune)
	require.Equal(t, 'w', frame.At(39, 0).Rune)
	require.Equal(t, ' ', frame.At(40, 0).Rune, "wide rune crossing preview pane is dropped")
	require.Equal(t, ' ', frame.At(17, 1).Rune, "preview pane is padded with blanks")
}

func TestRenderListScrollsSelectionIntoView(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []string{"a", "b", "c", "d", "e", "f"}, Active: 0}}, "s1", 0)
	for range 5 {
		m.Down()
	}

	frame := m.Render(domain.Size{Cols: 23, Rows: 4}, Preview{})
	require.Equal(t, ' ', frame.At(0, 3).Rune)
	require.Equal(t, 'f', frame.At(2, 3).Rune)
	require.True(t, frame.At(0, 3).Style.Inverse)
}

func TestRenderListOnlyDoesNotDrawPreview(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []string{"tab"}, Active: 0}}, "s1", 0)
	preview := Preview{Width: 1, Height: 1, Rows: [][]renderer.Cell{{cell('x')}}}

	frame := m.Render(domain.Size{Cols: 23, Rows: 11}, preview)
	require.Equal(t, 'o', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(0, 1).Rune)
	require.NotEqual(t, 'x', frame.At(0, 0).Rune)
}

func cell(r rune) renderer.Cell {
	return renderer.Cell{Rune: r, Style: renderer.DefaultStyle()}
}
