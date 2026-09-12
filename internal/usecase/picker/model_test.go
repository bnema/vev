package picker

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// line builds one selectable session line for the shared fixtures.
func line(key, label string, actions protocol.PickerLineActions) protocol.PickerLine {
	return protocol.PickerLine{Key: key, Kind: protocol.PickerLineSession, Label: label, Focusable: true, Actions: actions}
}

func navLine(key, label string) protocol.PickerLine {
	return line(key, label, protocol.PickerCanNavigate)
}

func section(label string) protocol.PickerLine {
	return protocol.PickerLine{Kind: protocol.PickerLineSection, Label: label, Dim: true}
}

func tabLine(key, label string, actions protocol.PickerLineActions) protocol.PickerLine {
	return protocol.PickerLine{Key: key, Kind: protocol.PickerLineTab, Label: label, Focusable: true, Actions: actions}
}

// inspectionLine builds one row the source keeps reachable without authorising
// any action on it.
func inspectionLine(key, label string) protocol.PickerLine {
	return protocol.PickerLine{Key: key, Kind: protocol.PickerLineHost, Label: label, Dim: true, Focusable: true}
}

func TestNewOrdersLinesAndSelectsTheCursorKey(t *testing.T) {
	m := New([]protocol.PickerLine{
		section("LOCAL"),
		navLine("a/one", "one"),
		tabLine("a/one#t2", "logs", protocol.PickerCanNavigate),
		tabLine("a/one#t1", "shell", protocol.PickerCanNavigate),
	}, Config{Intent: protocol.PickerIntentNavigation, Cursor: protocol.PickerCursor{Key: "a/one#t1", Index: 3}})

	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, "a/one#t1", selected.Key, "the published cursor key wins over the index hint")
}

func TestNewSelectsTheNearestFocusableRowForAnUnknownCursor(t *testing.T) {
	m := New([]protocol.PickerLine{
		section("LOCAL"),
		navLine("a/one", "one"),
		tabLine("a/one#t1", "shell", protocol.PickerCanNavigate),
	}, Config{Intent: protocol.PickerIntentNavigation, Cursor: protocol.PickerCursor{Key: "gone", Index: 2}})

	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, "a/one#t1", selected.Key)
}

func TestUpDownSkipsSectionsAndUnselectableRows(t *testing.T) {
	m := New([]protocol.PickerLine{
		navLine("a/one", "one"),
		// A host status row is focusable but carries no action.
		inspectionLine("b/host", "example.test"),
		navLine("c/three", "three"),
	}, Config{Intent: protocol.PickerIntentNavigation})

	m.SelectNearestRow(0)
	m.Down()
	selected, ok := m.Selected()
	require.False(t, ok, "a row without actions is never selectable")
	m.Down()
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "c/three", selected.Key)
	m.Up()
	cursor, ok := m.Cursor()
	require.True(t, ok, "the cursor parks on the host row without activating it")
	require.Equal(t, "b/host", cursor.Key)
	m.Up()
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "a/one", selected.Key)
}

func TestSortGroupedPullsNamedLinesAheadOfEphemeralOnes(t *testing.T) {
	lines := []protocol.PickerLine{
		{Key: "e/eph", Kind: protocol.PickerLineSession, Label: "eph", Ephemeral: true, Focusable: true, Actions: protocol.PickerCanNavigate},
		navLine("a/named", "named"),
		{Key: "e/eph2", Kind: protocol.PickerLineSession, Label: "eph2", Ephemeral: true, Focusable: true, Actions: protocol.PickerCanNavigate},
	}
	recent := New(lines, Config{Intent: protocol.PickerIntentNavigation})
	first, ok := recent.Cursor()
	require.True(t, ok)
	require.Equal(t, "e/eph", first.Key, "recency keeps the published order")

	grouped := New(lines, Config{Intent: protocol.PickerIntentNavigation, Sort: SortGrouped})
	first, ok = grouped.Cursor()
	require.True(t, ok)
	require.Equal(t, "a/named", first.Key, "grouped moves named sessions ahead")
}

func TestSetSortKeepsTheSelectedKey(t *testing.T) {
	m := New([]protocol.PickerLine{
		{Key: "e/eph", Kind: protocol.PickerLineSession, Label: "eph", Ephemeral: true, Focusable: true, Actions: protocol.PickerCanNavigate},
		navLine("a/named", "named"),
		navLine("b/other", "other"),
	}, Config{Intent: protocol.PickerIntentNavigation, Cursor: protocol.PickerCursor{Key: "b/other", Index: 2}})

	m.SetSort(SortGrouped)
	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, "b/other", selected.Key)
	require.Equal(t, SortGrouped, m.SortMode())

	m.SetSort(SortRecent)
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "b/other", selected.Key)
}

func TestReplaceLinesKeepsSearchAndCursor(t *testing.T) {
	m := New([]protocol.PickerLine{
		navLine("a/one", "one"),
		navLine("b/two", "two"),
	}, Config{Intent: protocol.PickerIntentNavigation, Cursor: protocol.PickerCursor{Key: "b/two", Index: 1}})
	m.EnterSearch()
	m.InsertSearch('t')

	m.ReplaceLines([]protocol.PickerLine{
		navLine("a/one", "one"),
		navLine("b/two", "two"),
		navLine("c/three", "three"),
	}, protocol.PickerCursor{})

	require.True(t, m.SearchActive())
	require.Equal(t, "t", m.Query())
	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, "b/two", selected.Key, "the cursor survives a background refresh by key")
}

func TestSelectedHonoursSearchVisibility(t *testing.T) {
	m := New([]protocol.PickerLine{
		navLine("a/one", "one"),
		navLine("b/two", "two"),
	}, Config{Intent: protocol.PickerIntentNavigation, Cursor: protocol.PickerCursor{Key: "a/one"}})
	m.EnterSearch()
	m.InsertSearch('z')

	_, ok := m.Selected()
	require.False(t, ok, "a cursor hidden by the search is never a commit target")
	require.Equal(t, -1, m.SelectedIndex(), "a query without matches leaves no visible cursor")
}

func TestRowsTheSourceSkippedAreNeverCursorDestinations(t *testing.T) {
	m := New([]protocol.PickerLine{
		// A session header the source did not authorise: rendered, skipped.
		{Key: "a/one", Kind: protocol.PickerLineSession, Label: "one"},
		tabLine("a/one#t1", "shell", protocol.PickerCanNavigate),
		inspectionLine("b/host", "example.test"),
	}, Config{Intent: protocol.PickerIntentNavigation, Cursor: protocol.PickerCursor{Key: "a/one", Index: 0}})

	require.Equal(t, "a/one#t1", mustSelectedKey(t, m), "an unauthorised header is not a destination")
	m.Up()
	require.Equal(t, "a/one#t1", mustSelectedKey(t, m), "navigation never rests on the header")
	m.Down()
	require.Equal(t, "b/host", cursorKey(t, m), "an inspection row stays reachable")
	_, ok := m.Selected()
	require.False(t, ok, "an inspection row never commits")
	m.Down()
	require.Equal(t, "b/host", cursorKey(t, m), "the last eligible row holds the cursor")
}

func TestSearchWithoutMatchesClearsTheCursorAndMovementFollows(t *testing.T) {
	m := New([]protocol.PickerLine{
		navLine("a/one", "one"),
		navLine("b/two", "two"),
		navLine("c/three", "three"),
	}, Config{Intent: protocol.PickerIntentNavigation})
	m.EnterSearch()
	m.InsertSearch('t')
	require.Equal(t, 2, m.MatchCount())
	m.Down()
	require.Equal(t, "c/three", mustSelectedKey(t, m), "search movement only visits matching rows")
	m.InsertSearch('z')
	require.Equal(t, 0, m.MatchCount())
	require.Equal(t, -1, m.SelectedIndex())
	m.Down()
	require.Equal(t, -1, m.SelectedIndex(), "movement cannot resurrect a hidden cursor")
	m.BackspaceSearch()
	require.Equal(t, 2, m.MatchCount())
	require.Equal(t, "b/two", mustSelectedKey(t, m), "editing the query re-places the cursor on a shown row")
	m.ClearSearch()
	require.Equal(t, 3, m.MatchCount(), "an empty query shows every eligible row again")
	require.Equal(t, "b/two", mustSelectedKey(t, m))
}

// mustSelectedKey reports the committed key, failing when the cursor holds no
// committable row.
func mustSelectedKey(t *testing.T, m *Model) string {
	t.Helper()
	selected, ok := m.Selected()
	require.True(t, ok, "the cursor must hold a committable row")
	return selected.Key
}

// cursorKey reports the raw cursor row key independently of committability.
func cursorKey(t *testing.T, m *Model) string {
	t.Helper()
	line, ok := m.Cursor()
	require.True(t, ok)
	return line.Key
}

func TestSelectNearestRowSnapsForwardThenBackward(t *testing.T) {
	m := New([]protocol.PickerLine{
		navLine("a/one", "one"),
		section("LOCAL"),
		navLine("b/two", "two"),
	}, Config{Intent: protocol.PickerIntentNavigation})

	m.SelectNearestRow(1)
	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, "b/two", selected.Key)
	m.SelectNearestRow(40)
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "b/two", selected.Key)
	m.SelectNearestRow(-3)
	selected, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, "a/one", selected.Key)
}

func TestCloneIsIndependent(t *testing.T) {
	m := New([]protocol.PickerLine{navLine("a/one", "one"), navLine("b/two", "two")}, Config{Intent: protocol.PickerIntentNavigation})
	clone := m.Clone()
	clone.Down()
	original, ok := m.Selected()
	require.True(t, ok)
	cloneSelected, ok := clone.Selected()
	require.True(t, ok)
	require.Equal(t, "a/one", original.Key)
	require.Equal(t, "b/two", cloneSelected.Key)
}

func TestIntentIsReported(t *testing.T) {
	m := New([]protocol.PickerLine{tabLine("a/one#t1", "shell", protocol.PickerCanMove)}, Config{Intent: protocol.PickerIntentMovePane})
	require.Equal(t, protocol.PickerIntentMovePane, m.Intent())
	require.Equal(t, " Sessions · recent ", SortRecent.Title())
	require.Equal(t, " Sessions · grouped ", SortGrouped.Title())
}

func TestChooseLayoutResponsiveBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		size    domain.Size
		mode    LayoutMode
		list    domain.Rect
		preview domain.Rect
	}{
		{name: "too small", size: domain.Size{Cols: 0, Rows: 0}, mode: LayoutListOnly},
		{name: "list only", size: domain.Size{Cols: 40, Rows: 10}, mode: LayoutListOnly, list: domain.Rect{Width: 40, Height: 10}},
		{name: "stacked", size: domain.Size{Cols: 40, Rows: 20}, mode: LayoutStacked, list: domain.Rect{Width: 40, Height: 8}, preview: domain.Rect{Y: 9, Width: 40, Height: 11}},
		{name: "horizontal", size: domain.Size{Cols: 120, Rows: 20}, mode: LayoutHorizontal, list: domain.Rect{Width: 44, Height: 20}, preview: domain.Rect{X: 45, Width: 75, Height: 20}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layout := ChooseLayout(tt.size)
			require.Equal(t, tt.mode, layout.Mode)
			require.Equal(t, tt.list, layout.List)
			require.Equal(t, tt.preview, layout.Preview)
		})
	}
}

func TestChooseGeometryReservesTheStatusRow(t *testing.T) {
	geometry := ChooseGeometry(domain.Size{Cols: 80, Rows: 24})
	require.Equal(t, domain.Rect{Width: 80, Height: 23}, geometry.Content)
	require.Equal(t, domain.Rect{Y: 23, Width: 80, Height: 1}, geometry.Status)
}

func TestRenderDrawsStatusBadgesAndStoppedRows(t *testing.T) {
	m := New([]protocol.PickerLine{
		{
			Key: "a/live", Kind: protocol.PickerLineSession, Label: "live", Focusable: true, Actions: protocol.PickerCanNavigate,
			Status: protocol.PickerLineStatusUp, Detail: "up",
		},
		{
			Key: "b/old", Kind: protocol.PickerLineSession, Label: "old", Stopped: true,
			Status: protocol.PickerLineStatusStopped, Detail: "stopped", Focusable: true, Actions: protocol.PickerCanNavigate,
		},
	}, Config{Intent: protocol.PickerIntentNavigation})

	frame := m.Render(domain.Size{Cols: 60, Rows: 8}, Preview{})
	require.Equal(t, 60, frame.Width)
	require.Equal(t, 8, frame.Height)
	require.Contains(t, rowText(frame.Row(0)), "[up]")
	require.Contains(t, rowText(frame.Row(1)), "[stopped]")
}

func TestRenderBlitsThePreviewIntoThePreviewRect(t *testing.T) {
	m := New([]protocol.PickerLine{navLine("a/one", "one")}, Config{Intent: protocol.PickerIntentNavigation})
	preview := Preview{Rows: [][]renderer.Cell{{{Rune: 'X'}}}, Width: 1, Height: 1}
	frame := m.Render(domain.Size{Cols: 120, Rows: 12}, preview)
	geometry := ChooseGeometry(domain.Size{Cols: 120, Rows: 12})
	x := geometry.Preview.X
	y := geometry.Preview.Y + geometry.Preview.Height - 1
	require.Equal(t, 'X', frame.Cell(x, y).Rune)
}

func TestRenderAttentionMarkerFollowsTheTabName(t *testing.T) {
	m := New([]protocol.PickerLine{
		tabLine("a/one#t1", "shell", protocol.PickerCanNavigate),
		{Key: "a/one#t2", Kind: protocol.PickerLineTab, Label: "build", Attention: true, Focusable: true, Actions: protocol.PickerCanNavigate},
	}, Config{Intent: protocol.PickerIntentNavigation})
	frame := m.Render(domain.Size{Cols: 60, Rows: 6}, Preview{})
	require.Contains(t, rowText(frame.Row(1)), "build")
	require.NotEqual(t, rowText(frame.Row(0)), rowText(frame.Row(1)))
}

func TestSearchMatchesLabelsAndDetails(t *testing.T) {
	m := New([]protocol.PickerLine{
		navLine("a/one", "one"),
		navLine("b/two", "two"),
		{Key: "c/three", Kind: protocol.PickerLineTab, Label: "three", Detail: " (vim)", Focusable: true, Actions: protocol.PickerCanNavigate},
	}, Config{Intent: protocol.PickerIntentNavigation})
	m.EnterSearch()
	m.InsertSearch('v')
	m.InsertSearch('i')
	m.InsertSearch('m')

	require.Equal(t, 1, m.MatchCount())
	selected, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, "c/three", selected.Key)
	m.BackspaceSearch()
	require.Equal(t, "vi", m.Query())
	m.ClearSearch()
	require.Equal(t, "", m.Query())
	require.True(t, m.SearchActive(), "clearing the query keeps the search editor open")
	m.ExitSearch()
	require.False(t, m.SearchActive())
}

func rowText(row []renderer.Cell) string {
	out := make([]rune, 0, len(row))
	for _, cell := range row {
		out = append(out, cell.Rune)
	}
	return string(out)
}
