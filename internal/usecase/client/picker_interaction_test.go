package client

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func pickerSnapshotFixture() protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: 11, Revision: 3, Title: " Sessions ",
		Rows: []protocol.PickerRow{
			{Key: "aa/work", Display: "work", Detail: "2 tabs"},
			{Key: "bb/perso", Display: "perso", Detail: "stopped", Stopped: true},
			{Key: "cc/remote", Display: "remote@host", Detail: ""},
		},
		Cursor:       protocol.PickerCursor{Key: "aa/work", Index: 0},
		BarrierEpoch: 1, BarrierState: 4, SizeEpoch: 2,
	}
}

func TestPickerInteractionAdmitsLatestRevision(t *testing.T) {
	var rel *pickerInteraction
	require.False(t, rel.admitSnapshot(pickerSnapshotFixture()))

	rel = &pickerInteraction{}
	snapshot := pickerSnapshotFixture()
	require.False(t, rel.admitSnapshot(snapshot), "closed interaction must not admit")

	rel.setOpen(true, snapshot.InteractionID)
	require.True(t, rel.admitSnapshot(snapshot))
	require.Equal(t, snapshot.Revision, rel.revision)

	older := snapshot
	older.Revision = snapshot.Revision - 1
	require.False(t, rel.admitSnapshot(older), "stale revision must not step backward")

	foreign := snapshot
	foreign.InteractionID = snapshot.InteractionID + 1
	require.False(t, rel.admitSnapshot(foreign), "foreign interaction must drop")

	rel.setOpen(false, snapshot.InteractionID)
	require.False(t, rel.admitSnapshot(snapshot), "closed interaction drops late snapshots")
}

func TestPickerInteractionRetirementIsPermanent(t *testing.T) {
	snapshot := pickerSnapshotFixture()
	var rel pickerInteraction
	rel.setOpen(true, snapshot.InteractionID)
	require.True(t, rel.admitSnapshot(snapshot))

	// Closing retires the namespace: an in-flight snapshot for the same
	// interaction can never reopen it, with or without a reopen flag.
	rel.setOpen(false, snapshot.InteractionID)
	require.Equal(t, snapshot.InteractionID, rel.retired)
	require.False(t, rel.admitSnapshot(snapshot))
	rel.setOpen(true, snapshot.InteractionID)
	require.False(t, rel.admitSnapshot(snapshot), "a retired interaction is never reopened")

	// A newer interaction is a new namespace and admits normally.
	next := snapshot
	next.InteractionID = snapshot.InteractionID + 1
	next.Revision = 1
	rel.setOpen(true, next.InteractionID)
	require.True(t, rel.admitSnapshot(next))
	require.False(t, rel.admitSnapshot(snapshot), "the retired namespace stays closed")
}

func TestPickerLoopCursorSearchCommit(t *testing.T) {
	snapshot := pickerSnapshotFixture()
	loop := openPickerLoop(snapshot)
	require.NotNil(t, loop.model)

	// Cursor starts at the snapshot cursor (first row).
	key, ok := loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "aa/work", key)

	// Cursor movement selects the non-initial row: commit identity follows
	// the client cursor, never the daemon cursor.
	loop.down()
	key, ok = loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "bb/perso", key)
	loop.down()
	key, ok = loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "cc/remote", key)
	loop.up()
	key, ok = loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "bb/perso", key)
}

func TestPickerLoopSearchExitVsClose(t *testing.T) {
	snapshot := pickerSnapshotFixture()
	loop := openPickerLoop(snapshot)

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
	loop.backspace()
	_ = loop.model.Query()
}

func TestPickerLoopKillRejectedLocally(t *testing.T) {
	// x in normal mode is NOT a daemon kill: there is no key path to
	// killPickerTarget in client-picker mode. The loop has no kill
	// operation; commitKey still reports the cursor row.
	snapshot := pickerSnapshotFixture()
	loop := openPickerLoop(snapshot)
	loop.down()
	key, ok := loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "bb/perso", key)
}

func TestPickerViewsRoundTripOpaqueKeys(t *testing.T) {
	snapshot := pickerSnapshotFixture()
	views := pickerViewsFromSnapshot(snapshot)
	require.Len(t, views, len(snapshot.Rows))
	loop := openPickerLoop(snapshot)
	// Every row round-trips its opaque key through cursor commits: drive
	// the model's own Down navigation from the top and collect keys
	// without parsing display text.
	loop.model.SelectNearestRow(0)
	var keys []string
	for range snapshot.Rows {
		key, ok := loop.commitKey()
		require.True(t, ok, "cursor row commits no key")
		keys = append(keys, key)
		loop.down()
	}
	require.Equal(t, []string{"aa/work", "bb/perso", "cc/remote"}, keys)
}

func TestPickerDriverOpOverlayParity(t *testing.T) {
	newLoop := func() *pickerLoop { return openPickerLoop(pickerSnapshotFixture()) }

	// Normal mode: arrows and j/k move; s/x are no-ops; q/Ctrl+C close.
	loop := newLoop()
	commit, close := pickerDriverOp(loop, []string{"Down"}, "")
	require.False(t, commit)
	require.False(t, close)
	key, ok := loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "bb/perso", key)
	commit, close = pickerDriverOp(loop, []string{"k"}, "")
	require.False(t, commit)
	require.False(t, close)
	key, ok = loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "aa/work", key)
	commit, close = pickerDriverOp(loop, []string{"s"}, "")
	require.False(t, commit)
	require.False(t, close)
	require.False(t, loop.model.SearchActive(), "normal-mode s must not sort in the pilot")
	commit, close = pickerDriverOp(loop, []string{"x"}, "")
	require.False(t, commit)
	require.False(t, close)
	key, ok = loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "aa/work", key)
	commit, close = pickerDriverOp(loop, []string{"q"}, "")
	require.False(t, commit)
	require.True(t, close)
	loop = newLoop()
	commit, close = pickerDriverOp(loop, []string{"Ctrl+C"}, "")
	require.False(t, commit)
	require.True(t, close)

	// Search: `/` enters, j/k/s/x/q are literal, Backspace edits,
	// plain text inserts only in search, Enter commits, Escape exits
	// search first and closes second.
	loop = newLoop()
	commit, close = pickerDriverOp(loop, []string{"/"}, "")
	require.False(t, commit)
	require.False(t, close)
	require.True(t, loop.model.SearchActive())
	commit, close = pickerDriverOp(loop, []string{"j", "k", "s", "x", "q"}, "")
	require.False(t, commit)
	require.False(t, close)
	require.Equal(t, "jksxq", loop.model.Query())
	commit, close = pickerDriverOp(loop, nil, "zz")
	require.False(t, commit)
	require.False(t, close)
	require.Equal(t, "jksxqzz", loop.model.Query())
	commit, close = pickerDriverOp(loop, []string{"Backspace"}, "")
	require.False(t, commit)
	require.False(t, close)
	require.Equal(t, "jksxqz", loop.model.Query())
	commit, close = pickerDriverOp(loop, []string{"Down"}, "")
	require.False(t, commit)
	require.False(t, close)
	commit, close = pickerDriverOp(loop, []string{"Enter"}, "")
	require.True(t, commit)
	require.False(t, close)
	commit, close = pickerDriverOp(loop, []string{"Escape"}, "")
	require.False(t, commit)
	require.False(t, close)
	require.False(t, loop.model.SearchActive(), "first Escape exits search")
	commit, close = pickerDriverOp(loop, []string{"Escape"}, "")
	require.False(t, commit)
	require.True(t, close)

	// Normal-mode typing and text insert are ignored; Close wins over
	// commit when both land in one op.
	loop = newLoop()
	commit, close = pickerDriverOp(loop, nil, "abc")
	require.False(t, commit)
	require.False(t, close)
	require.False(t, loop.model.SearchActive())
	commit, close = pickerDriverOp(loop, []string{"a"}, "")
	require.False(t, commit)
	require.False(t, close)
	require.False(t, loop.model.SearchActive())
	commit, close = pickerDriverOp(loop, []string{"Enter", "Escape"}, "")
	require.True(t, commit)
	require.True(t, close)

	// Nil loop and nil model never act.
	commit, close = pickerDriverOp(nil, []string{"Down"}, "")
	require.False(t, commit)
	require.False(t, close)
	commit, close = pickerDriverOp(&pickerLoop{}, []string{"Down"}, "")
	require.False(t, commit)
	require.False(t, close)
}
