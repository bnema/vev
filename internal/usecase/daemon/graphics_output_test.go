package daemon

import (
	"bytes"
	"os"
	"strings"
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev-vt/graphics"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/require"
)

func kittenGraphicsSnapshot(t *testing.T) *graphics.Snapshot {
	t.Helper()
	data, err := os.ReadFile("testdata/kitten-icat-stream-chunk.bin")
	require.NoError(t, err)
	screen := vt.NewScreen(80, 24)
	for len(data) > 0 {
		n := 791
		if n > len(data) {
			n = len(data)
		}
		screen.Write(data[:n])
		data = data[n:]
	}
	snapshot := screen.GraphicsSnapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(1), snapshot.Usage().Assets)
	require.Equal(t, uint64(1), snapshot.Usage().Placements)
	return snapshot
}

func recordIndex(data []byte, record string) int {
	return bytes.Index(data, []byte(record))
}

func TestGraphicsOutputReplaysStaticKittenIcatFixture(t *testing.T) {
	state := newGraphicsOutputState()
	snapshot := kittenGraphicsSnapshot(t)

	first, err := state.prepare(snapshot, true)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Contains(t, string(first.data), "\x1b_Ga=t,i=")
	require.Contains(t, string(first.data), ",f=100")
	require.Contains(t, string(first.data), "\x1b_Ga=p,i=")
	require.Contains(t, string(first.data), ",q=2")
	require.Less(t, recordIndex(first.data, "\x1b_Ga=t,"), recordIndex(first.data, "\x1b_Ga=p,"), "upload must precede placement")
	outer := vt.NewScreen(80, 24)
	outer.Write(first.data)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Assets)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Placements)
	first.commit()

	unchanged, err := state.prepare(snapshot, false)
	require.NoError(t, err)
	require.Empty(t, unchanged.data, "unchanged graphics must not enter ordinary text output")
	unchanged.commit()

	replay, err := state.prepare(snapshot, true)
	require.NoError(t, err)
	text := string(replay.data)
	require.Less(t, strings.Index(text, "a=d,d=p,p="), strings.Index(text, "a=d,d=i,i="), "placement cleanup must precede image cleanup")
	require.Less(t, strings.Index(text, "a=d,d=i,i="), strings.Index(text, "a=t,i="), "delete must precede replay upload")
	require.Less(t, strings.Index(text, "a=t,i="), strings.Index(text, "a=p,i="), "upload must precede replay placement")
}

func TestKittyAttachmentPaintCarriesGraphicsInTheOutputStateRecord(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	ac.terminalCapabilities.KittyGraphics = true
	ac.graphicsOutput = newGraphicsOutputState()
	fixture, err := os.ReadFile("testdata/kitten-icat-stream-chunk.bin")
	require.NoError(t, err)
	sess.tabs[0].focusedPane().screen.Write(fixture)

	d.paint(sess, ac, true, nil)
	frame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "\x1b_Ga=t,i=")
	require.Contains(t, string(output.Data), ",f=100")
	require.Contains(t, string(output.Data), "\x1b_Ga=p,i=")
	require.Contains(t, string(output.Data), ",q=2")

	outer := vt.NewScreen(output.Size.Cols, output.Size.Rows)
	outer.Write(output.Data)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Assets)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Placements)
}

func TestGraphicsOutputIDsAreIsolatedAcrossAttachments(t *testing.T) {
	snapshot := kittenGraphicsSnapshot(t)
	left, right := newGraphicsOutputState(), newGraphicsOutputState()
	leftPrepared, err := left.prepare(snapshot, true)
	require.NoError(t, err)
	rightPrepared, err := right.prepare(snapshot, true)
	require.NoError(t, err)
	var leftID, rightID uint64
	for _, asset := range leftPrepared.state.assets {
		leftID = asset.id
		break
	}
	for _, asset := range rightPrepared.state.assets {
		rightID = asset.id
		break
	}
	require.NotZero(t, leftID)
	require.NotZero(t, rightID)
	require.NotEqual(t, leftID, rightID, "terminal-global image IDs must not be reused by another attachment")
}

func TestGraphicsOutputKeysIncludeSourceSceneContent(t *testing.T) {
	makeSnapshot := func(data []byte) *graphics.Snapshot {
		scene := graphics.NewScene(graphics.Limits{})
		asset, err := scene.AddAsset(graphics.AssetBlob{Encoded: data, Width: 1, Height: 1})
		require.NoError(t, err)
		_, err = scene.PlaceAsset(asset, graphics.PixelRect{Width: 1, Height: 1})
		require.NoError(t, err)
		return scene.Snapshot()
	}
	state := newGraphicsOutputState()
	first, err := state.prepare(makeSnapshot([]byte("first")), true)
	require.NoError(t, err)
	first.commit()
	second, err := state.prepare(makeSnapshot([]byte("second")), false)
	require.NoError(t, err)
	require.Contains(t, string(second.data), "a=t,i=", "a new a1/p1 scene must not alias the stale outer image")
}

func TestGraphicsOutputPreSendAbortDoesNotCreateCleanupRecords(t *testing.T) {
	state := newGraphicsOutputState()
	prepared, err := state.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	prepared.abort()

	cleanup, err := state.prepare(nil, true)
	require.NoError(t, err)
	require.Empty(t, cleanup.data, "IDs allocated by a preparation that never reached the transport are not owned by the terminal")
}

func TestGraphicsOutputOversizeSuppressesGraphicsWithoutCompositionError(t *testing.T) {
	scene := graphics.NewScene(graphics.Limits{})
	asset, err := scene.AddAsset(graphics.AssetBlob{Encoded: bytes.Repeat([]byte{'x'}, maxGraphicsOutputBytes), Width: 1, Height: 1})
	require.NoError(t, err)
	_, err = scene.PlaceAsset(asset, graphics.PixelRect{Width: 1, Height: 1})
	require.NoError(t, err)
	state := &capturedRenderState{panes: []capturedPaneRenderState{{
		graphics:  scene.Snapshot(),
		placement: layout.Placement{Content: domain.Rect{Width: 1, Height: 1}},
	}}}
	ac := &attachedClient{graphicsOutput: newGraphicsOutputState()}
	prepared, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err, "ANSI composition must continue when optional graphics exceed the bound")
	require.Empty(t, prepared.data, "oversized graphics are suppressed")
}

func TestGraphicsOutputComposesPaneOriginAndPixelGeometry(t *testing.T) {
	placement := graphicsOutputPlacement{
		id:     1,
		source: graphics.PixelRect{Width: 20, Height: 10},
		dest:   graphics.PixelRect{X: 100, Y: 50, Width: 20, Height: 10},
		cells:  graphics.CellRect{Width: 2, Height: 1},
	}
	record := placementRecordWithTransform(1, placement, graphicsOutputTransform{
		originX:        3,
		originY:        2,
		sourceGeometry: domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480},
	})
	require.Contains(t, string(record), "a=p,i=1,p=1,x=13,y=4")
}

func TestGraphicsOutputFailedSendRetainsSpeculativeIDsForCleanup(t *testing.T) {
	state := newGraphicsOutputState()
	first, err := state.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	first.markSendAttempted()
	first.abort()

	retry, err := state.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	text := string(retry.data)
	require.Less(t, strings.Index(text, "a=d,d=p,p="), strings.Index(text, "a=d,d=i,i="))
	require.Less(t, strings.Index(text, "a=d,d=i,i="), strings.Index(text, "a=t,i="))
	require.Contains(t, text, "a=p,i=")
}

func TestGraphicsOutputCleanupRemovesPlacementsBeforeImages(t *testing.T) {
	state := newGraphicsOutputState()
	snapshot := kittenGraphicsSnapshot(t)
	prepared, err := state.prepare(snapshot, true)
	require.NoError(t, err)
	prepared.commit()

	// A nil snapshot models the attachment losing its visible graphics scene.
	cleared, err := state.prepare(nil, false)
	require.NoError(t, err)
	require.Less(t, strings.Index(string(cleared.data), "a=d,d=p,p="), strings.Index(string(cleared.data), "a=d,d=i,i="))
	cleared.commit()
	require.Empty(t, state.assets)
	require.Empty(t, state.placements)
}
