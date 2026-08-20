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
	require.Contains(t, string(first.data), "\x1b_Ga=t,i=1,f=100")
	require.Contains(t, string(first.data), "\x1b_Ga=p,i=1,p=1")
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
	require.Less(t, strings.Index(text, "a=d,d=p,p=1"), strings.Index(text, "a=d,d=i,i=1"), "placement cleanup must precede image cleanup")
	require.Less(t, strings.Index(text, "a=d,d=i,i=1"), strings.Index(text, "a=t,i=1"), "delete must precede replay upload")
	require.Less(t, strings.Index(text, "a=t,i=1"), strings.Index(text, "a=p,i=1,p=1"), "upload must precede replay placement")
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
	require.Contains(t, string(output.Data), "\x1b_Ga=t,i=1,f=100")
	require.Contains(t, string(output.Data), "\x1b_Ga=p,i=1,p=1")

	outer := vt.NewScreen(output.Size.Cols, output.Size.Rows)
	outer.Write(output.Data)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Assets)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Placements)
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
	first.abort()

	retry, err := state.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	text := string(retry.data)
	require.Less(t, strings.Index(text, "a=d,d=p,p=1"), strings.Index(text, "a=d,d=i,i=1"))
	require.Less(t, strings.Index(text, "a=d,d=i,i=1"), strings.Index(text, "a=t,i=1"))
	require.Contains(t, text, "a=p,i=1,p=1")
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
	require.Less(t, strings.Index(string(cleared.data), "a=d,d=p,p=1"), strings.Index(string(cleared.data), "a=d,d=i,i=1"))
	cleared.commit()
	require.Empty(t, state.assets)
	require.Empty(t, state.placements)
}
