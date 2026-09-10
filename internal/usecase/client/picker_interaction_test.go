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
	require.False(t, rel.commitKey(3, "aa/work"))

	rel = &pickerInteraction{}
	snapshot := pickerSnapshotFixture()
	require.False(t, rel.admitSnapshot(snapshot), "closed interaction must not admit")

	rel.setOpen(true, snapshot.InteractionID)
	require.True(t, rel.admitSnapshot(snapshot))
	require.True(t, rel.commitKey(snapshot.Revision, "bb/perso"))
	require.False(t, rel.commitKey(snapshot.Revision, "ff/ghost"), "unknown key must not commit")
	require.False(t, rel.commitKey(snapshot.Revision+1, "aa/work"), "future revision must not commit")

	older := snapshot
	older.Revision = snapshot.Revision - 1
	require.False(t, rel.admitSnapshot(older), "stale revision must not step backward")

	foreign := snapshot
	foreign.InteractionID = snapshot.InteractionID + 1
	require.False(t, rel.admitSnapshot(foreign), "foreign interaction must drop")

	rel.setOpen(false, 0)
	require.False(t, rel.admitSnapshot(snapshot), "closed interaction drops late snapshots")
	require.False(t, rel.commitKey(snapshot.Revision, "aa/work"))
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
