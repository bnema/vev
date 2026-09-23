package client

import (
	"fmt"
	"strings"
	"testing"

	renderer "github.com/bnema/vev-vt"
	ansirenderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/stretchr/testify/require"
)

func TestPickerRendererClearsPreviousBoundsAcrossResizes(t *testing.T) {
	loop := pickerLoopFixture(t)
	r := newPickerRenderer(ansirenderer.ColorProfileTrueColor)
	screen := renderer.NewScreen(100, 30)
	paintScreen(screen, domain.Size{Cols: 100, Rows: 30}, 'S')

	screen.Write(r.render(loop, domain.Size{Cols: 100, Rows: 30}, emptyPickerPreview()))
	require.NotNil(t, r.prevBounds)
	oldBounds := *r.prevBounds

	screen.Resize(80, 30)
	screen.Write(r.render(loop, domain.Size{Cols: 80, Rows: 30}, emptyPickerPreview()))
	require.NotNil(t, r.prevBounds)
	newBounds := *r.prevBounds

	require.Greater(t, oldBounds.X+oldBounds.Width, newBounds.X+newBounds.Width)
	snapshot := screen.Snapshot()
	for y := oldBounds.Y; y < min(oldBounds.Y+oldBounds.Height, snapshot.Rows()); y++ {
		for x := newBounds.X + newBounds.Width; x < min(oldBounds.X+oldBounds.Width, snapshot.Columns()); x++ {
			require.Equal(t, ' ', snapshot.Row(y)[x].Rune, "stale modal cell at (%d,%d)", x, y)
		}
	}
	require.Equal(t, 'S', snapshot.Row(0)[0].Rune, "render must preserve session content outside picker damage")
	require.Equal(t, '┌', snapshot.Row(newBounds.Y)[newBounds.X].Rune, "new picker border must be rendered")
}

func TestPickerRendererPreservesANSI256ProfileAcrossResize(t *testing.T) {
	loop := pickerLoopFixture(t)
	styles := picker.RenderStyles{
		Selection: rgbPickerStyle(), SelectionName: rgbPickerStyle(), SelectionMuted: rgbPickerStyle(),
		Name: rgbPickerStyle(), Detail: rgbPickerStyle(), Background: rgbPickerStyle(), Base: rgbPickerStyle(),
		Stopped: rgbPickerStyle(), Separator: rgbPickerStyle(), Status: rgbPickerStyle(),
		SearchMatch: rgbPickerStyle(), SelectionMatch: rgbPickerStyle(),
	}

	indexed := newPickerRenderer(ansirenderer.ColorProfileANSI256)
	indexed.renderStyles = []picker.RenderStyles{styles}
	for _, size := range []domain.Size{{Cols: 100, Rows: 30}, {Cols: 80, Rows: 24}} {
		output := string(indexed.render(loop, size, emptyPickerPreview()))
		require.Regexp(t, `(?:38|48);5;`, output, "picker did not emit indexed color at %v", size)
		require.NotRegexp(t, `(?:38|48|58);2;`, output, "picker emitted truecolor at %v", size)
	}

	truecolor := newPickerRenderer(ansirenderer.ColorProfileTrueColor)
	truecolor.renderStyles = []picker.RenderStyles{styles}
	require.Regexp(t, `(?:38|48);2;`, string(truecolor.render(loop, domain.Size{Cols: 100, Rows: 30}, emptyPickerPreview())))
}

// TestPickerRenderersFollowTerminalColorProfile guards #280 on the broker
// client: both the session picker and the move picker render with the
// terminal's detected color profile, so an ANSI-256 terminal never receives
// RGB SGR sequences from a picker box.
func TestPickerRenderersFollowTerminalColorProfile(t *testing.T) {
	loop := pickerLoopFixture(t)
	styles := picker.RenderStyles{
		Selection: rgbPickerStyle(), SelectionName: rgbPickerStyle(), SelectionMuted: rgbPickerStyle(),
		Name: rgbPickerStyle(), Detail: rgbPickerStyle(), Background: rgbPickerStyle(), Base: rgbPickerStyle(),
		Stopped: rgbPickerStyle(), Separator: rgbPickerStyle(), Status: rgbPickerStyle(),
		SearchMatch: rgbPickerStyle(), SelectionMatch: rgbPickerStyle(),
	}
	renderers := map[string]func(trueColor bool) *pickerRenderer{
		"session picker": func(trueColor bool) *pickerRenderer {
			return NewPicker(nil, 0, trueColor).controller.renderer
		},
		"move picker": func(trueColor bool) *pickerRenderer {
			return newMovePickerOverlay(trueColor).renderer
		},
	}
	for name, build := range renderers {
		for _, tc := range []struct {
			trueColor bool
			profile   ansirenderer.ColorProfile
			want      string
			forbidden string
		}{
			{trueColor: false, profile: ansirenderer.ColorProfileANSI256, want: `(?:38|48);5;`, forbidden: `(?:38|48|58);2;`},
			{trueColor: true, profile: ansirenderer.ColorProfileTrueColor, want: `(?:38|48);2;`, forbidden: `(?:38|48);5;`},
		} {
			t.Run(fmt.Sprintf("%s/truecolor=%v", name, tc.trueColor), func(t *testing.T) {
				r := build(tc.trueColor)
				require.Equal(t, tc.profile, r.profile)
				r.renderStyles = []picker.RenderStyles{styles}
				output := string(r.render(loop, domain.Size{Cols: 100, Rows: 30}, emptyPickerPreview()))
				require.Regexp(t, tc.want, output)
				require.NotRegexp(t, tc.forbidden, output)
			})
		}
	}
}

func rgbPickerStyle() renderer.Style {
	style := renderer.DefaultStyle()
	style.HasForegroundRGB = true
	style.ForegroundRGB = renderer.RGB{R: 12, G: 120, B: 231}
	style.HasBackgroundRGB = true
	style.BackgroundRGB = renderer.RGB{R: 23, G: 45, B: 67}
	return style
}

func paintScreen(screen *renderer.Screen, size domain.Size, fill rune) {
	for y := 0; y < size.Rows; y++ {
		screen.Write([]byte(fmt.Sprintf("\x1b[%d;1H%s", y+1, strings.Repeat(string(fill), size.Cols))))
	}
}

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

func TestPickerLoopMergesSourcesAndCommitsOwningIdentity(t *testing.T) {
	local := pickerSnapshotFixture()
	local.SourceID = "authority/local"
	local.Lines = local.Lines[:1]
	local.Cursor = protocol.PickerCursor{Key: "aa/work", Index: 0}
	loop := pickerLoopFromSnapshot(local, protocol.PickerIntentNavigation, picker.SortRecent)

	remote := local
	remote.SourceID = "authority/remote-a"
	remote.SourceRevision = 1
	remote.Lines = []protocol.PickerLine{{Key: "aa/work", Kind: protocol.PickerLineSession, Label: "work@remote-a", Focusable: true, Actions: protocol.PickerCanNavigate}}
	remote.Cursor = protocol.PickerCursor{Key: "aa/work", Index: 0}
	loop.replaceLines(remote)

	loop.down()
	selection, ok := commitSelection(loop, protocol.PickerActionNavigate, 9)
	require.True(t, ok)
	require.Equal(t, "authority/remote-a", selection.SourceID)
	require.Equal(t, uint64(1), selection.SourceRevision)
	require.Equal(t, "aa/work", selection.Key)
}

func TestPickerLoopRefreshesOneSourceWithoutInvalidatingAnother(t *testing.T) {
	local := pickerSnapshotFixture()
	local.SourceID = "authority/local"
	local.Lines = local.Lines[:1]
	local.Cursor = protocol.PickerCursor{Key: "aa/work", Index: 0}
	loop := pickerLoopFromSnapshot(local, protocol.PickerIntentNavigation, picker.SortRecent)

	remote := local
	remote.SourceID = "authority/remote-a"
	remote.SourceRevision = 1
	remote.Lines = []protocol.PickerLine{{Key: "remote/session", Kind: protocol.PickerLineSession, Label: "remote", Focusable: true, Actions: protocol.PickerCanNavigate}}
	remote.Cursor = protocol.PickerCursor{Key: "remote/session", Index: 0}
	loop.replaceLines(remote)
	loop.down()

	local.SourceRevision++
	local.Lines[0].Detail = "refreshed"
	loop.replaceLines(local)
	selection, ok := commitSelection(loop, protocol.PickerActionNavigate, 0)
	require.True(t, ok)
	require.Equal(t, "authority/remote-a", selection.SourceID)
	require.Equal(t, uint64(1), selection.SourceRevision)
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
