package daemon

import (
	"bytes"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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

type lateGraphicsCleanupTransport struct {
	started     chan struct{}
	release     chan struct{}
	finished    chan struct{}
	sends       chan ports.Frame
	startedOnce sync.Once
	finishOnce  sync.Once
}

func newLateGraphicsCleanupTransport() *lateGraphicsCleanupTransport {
	return &lateGraphicsCleanupTransport{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
		sends:    make(chan ports.Frame, 1),
	}
}

func (t *lateGraphicsCleanupTransport) Send(frame ports.Frame) error {
	t.startedOnce.Do(func() { close(t.started) })
	<-t.release
	t.sends <- frame
	t.finishOnce.Do(func() { close(t.finished) })
	return nil
}

func (*lateGraphicsCleanupTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (*lateGraphicsCleanupTransport) Close() error               { return nil }

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

func TestGraphicsNamespacesAreDeterministicAndCollisionSafe(t *testing.T) {
	d := &Daemon{}
	d.mu.Lock()
	first := d.reserveGraphicsNamespaceLocked("session:work:attachment")
	second := d.reserveGraphicsNamespaceLocked("session:work:attachment")
	d.mu.Unlock()
	require.NotZero(t, first)
	require.NotZero(t, second)
	require.NotEqual(t, first, second, "equal attachment/session keys still need a collision-safe namespace")
	d.releaseGraphicsNamespace(first)
	d.mu.Lock()
	reused := d.reserveGraphicsNamespaceLocked("session:work:attachment")
	d.mu.Unlock()
	require.Equal(t, first, reused, "released namespaces should deterministically return to their preferred block")
}

func TestGraphicsOutputExhaustionResetsInsideItsNamespace(t *testing.T) {
	base := graphicsIDNamespaceSize + 1
	state := newGraphicsOutputStateWithBase(base)
	state.nextID = base + graphicsIDNamespaceSize - 1
	snapshot := kittenGraphicsSnapshot(t)
	prepared, err := state.prepare(snapshot, false)
	require.NoError(t, err, "namespace exhaustion must recover through an attachment-local replay")
	require.NotNil(t, prepared)
	limit := base + graphicsIDNamespaceSize - 1
	for _, asset := range prepared.state.assets {
		require.GreaterOrEqual(t, asset.id, base)
		require.LessOrEqual(t, asset.id, limit)
	}
	for _, placement := range prepared.state.placements {
		require.GreaterOrEqual(t, placement.id, base)
		require.LessOrEqual(t, placement.id, limit)
	}
	require.Equal(t, base+2, prepared.state.nextID, "the replay should restart at the reserved block base")
}

func TestGraphicsNamespaceQuarantinesOnParkExpiryAndFailedCleanup(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	base := graphicsIDNamespaceSize + 1
	block := (base - 1) / graphicsIDNamespaceSize

	parkedState := newGraphicsOutputStateWithBase(base)
	parked := &parkedAttachment{
		sess: &session{sessionCore: sessionCore{name: "parked"}},
		ac:   &attachedClient{graphicsOutput: parkedState},
		done: make(chan struct{}),
	}
	d.mu.Lock()
	d.graphicsNamespaces[block] = struct{}{}
	d.parked[1] = parked
	d.mu.Unlock()
	d.expireParked(1, parked)
	require.Nil(t, parked.ac.graphicsOutput)
	d.mu.Lock()
	_, reserved := d.graphicsNamespaces[block]
	d.mu.Unlock()
	require.False(t, reserved, "park expiry with no outer objects may release the attachment namespace")

	failedState := newGraphicsOutputStateWithBase(base)
	seed, err := failedState.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	seed.commit()
	failed := &attachedClient{
		tr:             failingOutputTransport{},
		output:         newOutputStateStream(),
		graphicsOutput: failedState,
	}
	d.mu.Lock()
	d.graphicsNamespaces[block] = struct{}{}
	d.mu.Unlock()
	d.cleanupGraphicsOutput(failed)
	require.Nil(t, failed.graphicsOutput)
	d.mu.Lock()
	_, reserved = d.graphicsNamespaces[block]
	d.mu.Unlock()
	require.True(t, reserved, "failed terminal cleanup must quarantine the namespace")
}

func TestGraphicsNamespaceQuarantineFencesTimedOutLateDelete(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)
	key := "session:stale:attachment"
	d.mu.Lock()
	oldBase, oldFence := d.reserveGraphicsNamespaceLeaseLocked(key)
	d.mu.Unlock()
	oldState := newGraphicsOutputStateWithLease(oldBase, oldFence)
	seed, err := oldState.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	seed.commit()
	var oldID uint64
	for _, asset := range oldState.assets {
		oldID = asset.id
		break
	}
	require.NotZero(t, oldID)

	transport := newLateGraphicsCleanupTransport()
	ac := &attachedClient{tr: transport, output: newOutputStateStream(), graphicsOutput: oldState}
	cleanupDone := make(chan struct{})
	go func() {
		d.cleanupGraphicsOutput(ac)
		close(cleanupDone)
	}()
	awaitTestCompletion(t, transport.started, "timed-out cleanup did not reach the stale transport")
	timer := awaitTestValue(t, clock.timers, "cleanup watchdog timer was not armed")
	timer.ch <- time.Time{}
	awaitTestCompletion(t, cleanupDone, "timed-out cleanup did not retire the attachment")

	d.mu.Lock()
	newBase, newFence := d.reserveGraphicsNamespaceLeaseLocked(key)
	_, quarantined := d.graphicsNamespaceQuarantines[(oldBase-1)/graphicsIDNamespaceSize]
	d.mu.Unlock()
	require.True(t, quarantined, "timed-out cleanup must keep its namespace quarantined")
	require.NotEqual(t, oldBase, newBase, "new allocation must skip the quarantined namespace")

	newState := newGraphicsOutputStateWithLease(newBase, newFence)
	fresh, err := newState.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	var newID uint64
	for _, asset := range fresh.state.assets {
		newID = asset.id
		break
	}
	require.NotZero(t, newID)
	require.NotEqual(t, oldID, newID, "fresh attachment IDs must not share the quarantined block")

	close(transport.release)
	staleFrame := awaitTestValue(t, transport.sends, "late stale cleanup did not execute")
	awaitTestCompletion(t, transport.finished, "late stale cleanup did not return")
	staleOutput, err := ports.UnmarshalOutput(staleFrame.Payload)
	require.NoError(t, err)
	require.Contains(t, string(staleOutput.Data), "i="+strconv.FormatUint(oldID, 10))
	require.NotContains(t, string(staleOutput.Data), "i="+strconv.FormatUint(newID, 10), "late cleanup must never target the fresh attachment")
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, retained := d.graphicsNamespaceQuarantines[(oldBase-1)/graphicsIDNamespaceSize]
		return !retained
	}, time.Second, time.Millisecond)
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
	ac := &attachedClient{
		graphicsOutput:       newGraphicsOutputState(),
		terminalCapabilities: ports.TerminalCapabilities{KittyGraphics: true},
	}
	prepared, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err, "ANSI composition must continue when optional graphics exceed the bound")
	require.Empty(t, prepared.data, "oversized graphics are suppressed")
}

func TestGraphicsOutputClipsPlacementToPaneContent(t *testing.T) {
	scene := graphics.NewScene(graphics.Limits{})
	asset, err := scene.AddAsset(graphics.AssetBlob{Encoded: []byte("asset"), Width: 10, Height: 10})
	require.NoError(t, err)
	_, err = scene.PlaceAsset(asset, graphics.PixelRect{X: -2, Y: -1, Width: 10, Height: 10})
	require.NoError(t, err)
	state := &capturedRenderState{panes: []capturedPaneRenderState{{
		graphics:  scene.Snapshot(),
		placement: layout.Placement{Content: domain.Rect{X: 3, Y: 4, Width: 4, Height: 3}},
	}}}
	ac := &attachedClient{
		graphicsOutput:       newGraphicsOutputState(),
		terminalCapabilities: ports.TerminalCapabilities{KittyGraphics: true},
	}
	prepared, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err)
	text := string(prepared.data)
	require.Contains(t, text, ",x=3,y=5")
	require.Contains(t, text, ",X=2,Y=1,w=4,h=3")
	require.NotContains(t, text, ",x=2,y=4", "graphics must not paint the title bar or adjacent area")
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
