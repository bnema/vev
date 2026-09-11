package client

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/stretchr/testify/require"
)

func pickerSnapshotFixture() protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: 11, SourceID: "serving", SourceRevision: 3, Status: protocol.PickerSourceOK,
		Lines: []protocol.PickerLine{
			{Key: "aa/work", Kind: protocol.PickerLineSession, Label: "work", Detail: "2 tabs", Focusable: true, Actions: protocol.PickerCanNavigate | protocol.PickerCanKill},
			{Key: "bb/perso", Kind: protocol.PickerLineSession, Label: "perso", Detail: "stopped", Stopped: true, Focusable: true, Actions: protocol.PickerCanNavigate},
			{Key: "cc/remote", Kind: protocol.PickerLineSession, Label: "remote@host", Focusable: true, Actions: protocol.PickerCanNavigate},
		},
		Cursor: protocol.PickerCursor{Key: "aa/work", Index: 0},
	}
}

func pickerLoopFixture(t *testing.T) *pickerLoop {
	t.Helper()
	return pickerLoopFromSnapshot(pickerSnapshotFixture(), protocol.PickerIntentNavigation, picker.SortRecent)
}

func pickedKey(t *testing.T, loop *pickerLoop) string {
	t.Helper()
	selection, ok := commitSelection(loop, protocol.PickerActionNavigate, 0)
	require.True(t, ok)
	return selection.Key
}

func TestPickerInteractionAdmitsLatestSourceRevision(t *testing.T) {
	var rel *pickerInteraction
	require.False(t, rel.admitSnapshot(pickerSnapshotFixture(), false))

	rel = &pickerInteraction{}
	snapshot := pickerSnapshotFixture()
	require.False(t, rel.admitSnapshot(snapshot, false), "closed interaction must not admit")

	rel.setOpen(true, snapshot.InteractionID)
	require.True(t, rel.admitSnapshot(snapshot, false))
	require.Equal(t, snapshot.SourceRevision, rel.sourceRevisions[snapshot.SourceID])

	older := snapshot
	older.SourceRevision = snapshot.SourceRevision - 1
	require.False(t, rel.admitSnapshot(older, false), "stale source revision must not step backward")

	foreign := snapshot
	foreign.InteractionID = snapshot.InteractionID + 1
	require.False(t, rel.admitSnapshot(foreign, false), "foreign interaction must drop")

	rel.setOpen(false, snapshot.InteractionID)
	require.False(t, rel.admitSnapshot(snapshot, false), "closed interaction drops late snapshots")
}

func TestPickerInteractionKeepsPerSourceRevisions(t *testing.T) {
	rel := &pickerInteraction{}
	snapshot := pickerSnapshotFixture()
	rel.setOpen(true, snapshot.InteractionID)
	require.True(t, rel.admitSnapshot(snapshot, false))

	// Another source publishes its own first revision: it must be admitted
	// even though the serving source is far ahead.
	other := snapshot
	other.SourceID = "host-2"
	other.SourceRevision = 1
	require.True(t, rel.admitSnapshot(other, false))
	require.Equal(t, uint64(3), rel.sourceRevisions["serving"])
	require.Equal(t, uint64(1), rel.sourceRevisions["host-2"])
}

func TestPickerInteractionRetirementIsPermanent(t *testing.T) {
	snapshot := pickerSnapshotFixture()
	var rel pickerInteraction
	rel.setOpen(true, snapshot.InteractionID)
	require.True(t, rel.admitSnapshot(snapshot, false))

	// Closing retires the namespace: an in-flight snapshot for the same
	// interaction can never reopen it.
	rel.setOpen(false, snapshot.InteractionID)
	require.Equal(t, snapshot.InteractionID, rel.retired)
	require.False(t, rel.admitSnapshot(snapshot, false))
	rel.setOpen(true, snapshot.InteractionID)
	require.False(t, rel.admitSnapshot(snapshot, false), "a retired interaction is never reopened")

	// A newer interaction is a new namespace and admits normally.
	next := snapshot
	next.InteractionID = snapshot.InteractionID + 1
	next.SourceRevision = 1
	rel.setOpen(true, next.InteractionID)
	require.True(t, rel.admitSnapshot(next, false))
	require.False(t, rel.admitSnapshot(snapshot, false), "the retired namespace stays closed")
}

func TestPickerLoopCursorSearchCommit(t *testing.T) {
	loop := pickerLoopFixture(t)
	require.NotNil(t, loop.model)

	// Cursor starts at the published cursor key (first row).
	require.Equal(t, "aa/work", pickedKey(t, loop))

	// Cursor movement selects the non-initial row: the committed key follows
	// the client cursor, never the daemon cursor.
	loop.down()
	require.Equal(t, "bb/perso", pickedKey(t, loop))
	loop.down()
	require.Equal(t, "cc/remote", pickedKey(t, loop))
	loop.up()
	require.Equal(t, "bb/perso", pickedKey(t, loop))
}

func TestPickerLoopSearchExitVsClose(t *testing.T) {
	loop := pickerLoopFixture(t)

	// Text enters search; Escape exits search without closing.
	loop.insert('w')
	require.True(t, loop.model.SearchActive())
	require.False(t, loop.escape(), "search-exit Escape must not close")
	require.False(t, loop.model.SearchActive())

	// Escape with no search reports picker-close.
	require.True(t, loop.escape(), "idle Escape must close")

	// Literal s/x in search are text; backspace deletes.
	loop.insert('s')
	loop.insert('x')
	require.True(t, loop.model.SearchActive())
	require.Equal(t, "sx", loop.model.Query())
	loop.backspace()
	require.Equal(t, "s", loop.model.Query())
}

func TestPickerLoopKillRequiresAuthorisedRow(t *testing.T) {
	loop := pickerLoopFixture(t)

	// The first row authorises destruction; its typed kill carries the
	// displayed source revision and the fatal action.
	selection, ok := killSelection(loop, 0)
	require.True(t, ok)
	require.Equal(t, protocol.PickerActionKill, selection.Action)
	require.Equal(t, "aa/work", selection.Key)
	require.Equal(t, uint64(3), selection.SourceRevision)

	// The stopped row is navigable but not destructible from here.
	loop.down()
	require.Equal(t, "bb/perso", pickedKey(t, loop))
	_, ok = killSelection(loop, 0)
	require.False(t, ok, "a row without the kill action must not be destroyed")
}

func TestPickerLoopKeepsPublishedLineOrderAndKeys(t *testing.T) {
	snapshot := pickerSnapshotFixture()
	loop := pickerLoopFromSnapshot(snapshot, protocol.PickerIntentNavigation, picker.SortRecent)

	// Every published line commits its own opaque key without parsing display
	// text, in published order.
	loop.model.SelectNearestRow(0)
	keys := []string{pickedKey(t, loop)}
	for range len(snapshot.Lines) - 1 {
		loop.down()
		keys = append(keys, pickedKey(t, loop))
	}
	require.Equal(t, []string{"aa/work", "bb/perso", "cc/remote"}, keys)
}

func TestPickerDriverOpReportsActionsAndLocalSort(t *testing.T) {
	loop := pickerLoopFixture(t)

	// Normal mode: arrows and j/k move; Enter asks to commit; x asks to kill.
	op, changed := pickerDriverOp(loop, []string{"Down"}, "")
	require.False(t, op.commit)
	require.False(t, op.close)
	require.True(t, changed)
	require.Equal(t, "bb/perso", pickedKey(t, loop))
	op, _ = pickerDriverOp(loop, []string{"k"}, "")
	require.False(t, op.commit)
	require.Equal(t, "aa/work", pickedKey(t, loop))

	op, _ = pickerDriverOp(loop, []string{"Enter"}, "")
	require.True(t, op.commit)
	op, _ = pickerDriverOp(loop, []string{"x"}, "")
	require.True(t, op.kill)
	require.False(t, op.close)

	// q and Ctrl+C close in normal mode.
	for _, key := range []string{"q", "Ctrl+C"} {
		op, _ = pickerDriverOp(loop, []string{key}, "")
		require.True(t, op.close, "%s must close the picker", key)
	}

	// s reorders locally without asking the daemon, and keeps the selection.
	op, changed = pickerDriverOp(loop, []string{"s"}, "")
	require.False(t, op.commit)
	require.False(t, op.kill)
	require.False(t, op.close)
	require.True(t, changed)
	require.Equal(t, picker.SortGrouped, loop.model.SortMode())
	require.Equal(t, "aa/work", pickedKey(t, loop))

	// While searching, Enter still commits and printable keys are text.
	loop.insert('w')
	_, changed = pickerDriverOp(loop, []string{"o", "r"}, "")
	require.True(t, changed)
	require.Equal(t, "wor", loop.model.Query())
}

// TestPickerPreviewClientTracksTheDisplayedRow pins the client half of the
// preview: one request per settled row, the daemon's own source identity, a
// bounded viewport ask, and a late answer that never replaces the displayed one.
func TestPickerPreviewClientTracksTheDisplayedRow(t *testing.T) {
	var preview pickerPreviewClient
	require.False(t, preview.needsRequest(7, ""), "a row without a key needs no request")
	require.True(t, preview.needsRequest(7, "aa/first"))

	request, ok := preview.requestFor(7, "aa/first", domain.Size{Cols: 500, Rows: 0})
	require.True(t, ok)
	require.Equal(t, protocol.PickerPreviewSchemaVersion, request.Version)
	require.Equal(t, uint64(7), request.InteractionID)
	require.Equal(t, protocol.PickerServingSourceID, request.SourceID)
	require.Equal(t, uint16(protocol.PickerPreviewMaxWidth), request.Width, "the client asks for what it can display")
	require.Equal(t, uint16(1), request.Height, "a degenerate size still asks for one row")

	preview.markSent(7, "aa/first")
	require.False(t, preview.needsRequest(7, "aa/first"), "a row already requested is not asked twice")
	require.True(t, preview.needsRequest(7, "bb/second"))
	require.True(t, preview.needsRequest(8, "aa/first"), "a new interaction asks again")

	viewport := protocol.PickerPreview{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: 7, SourceID: protocol.PickerServingSourceID,
		Key: "aa/first", Status: protocol.PickerPreviewOK, Width: 2, Height: 1,
		Cells: []renderer.Cell{{Rune: 'o'}, {Rune: 'k'}},
	}
	require.False(t, preview.accept(viewport, 7, "bb/second"), "a late answer must not replace the displayed row")
	require.True(t, preview.accept(viewport, 7, "aa/first"))
	require.Equal(t, 2, preview.frame.Width)
	require.Equal(t, 'o', preview.frame.Rows[0][0].Rune)

	stale := viewport
	stale.Status = protocol.PickerPreviewNoSuchTarget
	stale.Width, stale.Height, stale.Cells = 0, 0, nil
	require.True(t, preview.accept(stale, 7, "aa/first"))
	require.Empty(t, preview.frame.Rows, "a rejected row clears the displayed viewport")

	preview.resetFor()
	require.True(t, preview.needsRequest(7, "aa/first"))
	require.Empty(t, preview.frame.Rows)
}
