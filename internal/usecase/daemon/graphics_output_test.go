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
	ac.graphicsTerminalResetPending = true
	fixture, err := os.ReadFile("testdata/kitten-icat-stream-chunk.bin")
	require.NoError(t, err)
	pane := sess.tabs[0].focusedPane()
	// This fixture contains pixel-space Kitty output without cell metadata;
	// provide the terminal pixel geometry explicitly so attachment clipping can
	// map its placement safely.
	geometry := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 23}, PixelWidth: 800, PixelHeight: 460}
	pane.geometry = geometry
	setScreenGeometry(pane.screen, geometry)
	pane.screen.Write(fixture)

	d.paint(sess, ac, true, nil)
	frame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "\x1b_Ga=t,i=")
	require.Contains(t, string(output.Data), ",f=100")
	require.Contains(t, string(output.Data), "\x1b_Ga=p,i=")
	require.Contains(t, string(output.Data), ",q=2")
	require.Positive(t, output.New, "terminal reset must ride the ordinary state-bearing output")
	require.Less(t, bytes.Index(output.Data, graphicsTerminalResetRecord), bytes.Index(output.Data, []byte("\x1b_Ga=t,")), "restart cleanup must precede fresh ID uploads")

	outer := vt.NewScreen(output.Size.Cols, output.Size.Rows)
	stale := newGraphicsOutputStateWithBase(ac.graphicsOutput.namespaceBase)
	staleOutput, err := stale.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	outer.Write(staleOutput.data)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Placements)
	outer.Write(output.Data)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Assets)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Placements)
}

func TestFreshCapabilityDowngradeClearsTerminalGraphicsInOrdinaryOutput(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	ac.terminalCapabilities.KittyGraphics = true
	ac.graphicsOutput = newGraphicsOutputState()
	ac.graphicsTerminalResetPending = false
	d.mu.Lock()
	d.reapplyAttachmentGraphicsCapability(sess, ac, ports.Hello{KittyDirectGraphics: false})
	d.mu.Unlock()
	require.False(t, ac.terminalCapabilities.SupportsKittyGraphics())
	require.Nil(t, ac.graphicsOutput)
	require.True(t, ac.graphicsTerminalResetPending)

	d.paint(sess, ac, true, nil)
	frame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.Positive(t, output.New, "global cleanup must use the normal output state and ACK")
	require.True(t, bytes.HasPrefix(output.Data, graphicsTerminalResetRecord), "cleanup must precede ANSI on a downgraded connection")

	outer := vt.NewScreen(80, 24)
	stale := newGraphicsOutputState()
	staleOutput, err := stale.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	outer.Write(staleOutput.data)
	require.Equal(t, uint64(1), outer.GraphicsSnapshot().Usage().Placements)
	outer.Write(output.Data)
	require.Zero(t, outer.GraphicsSnapshot().Usage().Assets, "a restarted daemon must not leave predecessor images unmanaged")
	require.Zero(t, outer.GraphicsSnapshot().Usage().Placements)
}

func TestPaneGraphicsCoordinateGeometrySurvivesAttachmentResize(t *testing.T) {
	fixture, err := os.ReadFile("testdata/kitten-icat-stream-chunk.bin")
	require.NoError(t, err)
	p := newPane("pane", nil, domain.Size{Cols: 80, Rows: 24})
	oldGeometry := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480}
	newGeometry := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 1600, PixelHeight: 960}

	p.mu.Lock()
	setScreenGeometry(p.screen, oldGeometry)
	writePaneScreenLocked(p, fixture)
	first := capturePaneRenderStateLocked(p, domain.Rect{Width: 80, Height: 24})
	setScreenGeometry(p.screen, newGeometry)
	second := capturePaneRenderStateLocked(p, domain.Rect{Width: 80, Height: 24})
	p.mu.Unlock()

	require.Equal(t, oldGeometry, first.graphicsGeometry)
	require.Equal(t, oldGeometry, second.graphicsGeometry, "retained scene pixels must not be reinterpreted as the resized cell units")
}

func TestGraphicsOutputChunksLargeRGBUpload(t *testing.T) {
	const width, height = 300, 300
	raw := make([]byte, width*height*3)
	state := uint32(1)
	for i := range raw {
		state = state*1664525 + 1013904223
		raw[i] = byte(state >> 24)
	}
	scene := graphics.NewScene(graphics.Limits{})
	asset, err := scene.AddAsset(graphics.AssetBlob{Encoded: raw, Format: graphics.AssetFormatRGB, Width: width, Height: height})
	require.NoError(t, err)
	_, err = scene.PlaceAsset(asset, graphics.PixelRect{Width: width, Height: height})
	require.NoError(t, err)

	prepared, err := newGraphicsOutputState().prepare(scene.Snapshot(), true)
	require.NoError(t, err)
	var uploadHeaders []string
	for data := prepared.data; ; {
		start := bytes.Index(data, []byte("\x1b_G"))
		if start < 0 {
			break
		}
		end := bytes.Index(data[start+3:], []byte("\x1b\\"))
		require.NotEqual(t, -1, end)
		body := data[start+3 : start+3+end]
		header, payload, _ := bytes.Cut(body, []byte(";"))
		if bytes.Contains(header, []byte("a=t")) {
			uploadHeaders = append(uploadHeaders, string(header))
			require.LessOrEqual(t, len(payload), graphicsUploadChunkBytes)
		}
		data = data[start+3+end+2:]
	}
	require.Greater(t, len(uploadHeaders), 1, "large RGB data must use Kitty continuation chunks")
	require.Contains(t, uploadHeaders[0], "f=24")
	require.Contains(t, uploadHeaders[0], "m=1")
	require.Contains(t, uploadHeaders[len(uploadHeaders)-1], "m=0")
}

func TestGraphicsOutputCompressesLargeRGBUpload(t *testing.T) {
	const width, height = 100, 100
	raw := bytes.Repeat([]byte{0x7f}, width*height*3)
	scene := graphics.NewScene(graphics.Limits{})
	asset, err := scene.AddAsset(graphics.AssetBlob{Encoded: raw, Format: graphics.AssetFormatRGB, Width: width, Height: height})
	require.NoError(t, err)
	_, err = scene.PlaceAsset(asset, graphics.PixelRect{Width: width, Height: height})
	require.NoError(t, err)

	prepared, err := newGraphicsOutputState().prepare(scene.Snapshot(), true)
	require.NoError(t, err)
	require.Contains(t, string(prepared.data), ",f=24,s=100,v=100,o=z")
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

func TestUnsupportedGraphicsWarningIsBoundedForLateSceneCreation(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
	ac.terminalCapabilities.KittyGraphics = false
	fixture, err := os.ReadFile("testdata/kitten-icat-stream-chunk.bin")
	require.NoError(t, err)
	sess.tabs[0].focusedPane().screen.Write(fixture)

	require.True(t, d.warnUnsupportedGraphics(ac), "late graphics creation should explain suppression")
	require.False(t, d.warnUnsupportedGraphics(ac), "suppression warning must be bounded per attachment")
	ac.overlays.noticeMu.Lock()
	require.Len(t, ac.overlays.noticeToasts, 1)
	ac.overlays.noticeMu.Unlock()
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

func TestGraphicsNamespaceStaysQuarantinedAfterSocketSendSuccessWithoutTerminalFlush(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	key := "session:unflushed:attachment"
	d.mu.Lock()
	oldBase, oldFence := d.reserveGraphicsNamespaceLeaseLocked(key)
	d.mu.Unlock()

	state := newGraphicsOutputStateWithLease(oldBase, oldFence)
	seed, err := state.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	seed.markSendAttempted()
	seed.commit()

	// This transport models only successful delivery to the client socket. It
	// deliberately provides no evidence that the client flushed the side effect
	// to its outer terminal before being lost.
	transport := &closeTrackingTransport{}
	ac := &attachedClient{tr: transport, output: newOutputStateStream(), graphicsOutput: state}
	d.cleanupGraphicsOutput(ac)
	require.Nil(t, ac.graphicsOutput)
	sends := transport.Sends()
	require.Len(t, sends, 1)
	cleanup, err := ports.UnmarshalOutput(sends[0].Payload)
	require.NoError(t, err)
	require.Zero(t, cleanup.New, "graphics cleanup is an unacknowledged side-effect frame")

	d.mu.Lock()
	_, quarantined := d.graphicsNamespaceQuarantines[(oldBase-1)/graphicsIDNamespaceSize]
	newBase, _ := d.reserveGraphicsNamespaceLeaseLocked(key)
	d.mu.Unlock()
	require.True(t, quarantined, "socket-send success cannot prove terminal cleanup")
	require.NotEqual(t, oldBase, newBase, "an unflushed cleanup must not expose the old IDs to another attachment")
}

func TestGraphicsNamespaceStaysQuarantinedAfterFinalDeleteAndParkExpiry(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	key := "session:empty-park:attachment"
	d.mu.Lock()
	oldBase, oldFence := d.reserveGraphicsNamespaceLeaseLocked(key)
	d.mu.Unlock()

	state := newGraphicsOutputStateWithLease(oldBase, oldFence)
	seed, err := state.prepare(kittenGraphicsSnapshot(t), true)
	require.NoError(t, err)
	seed.markSendAttempted()
	seed.commit()
	finalDelete, err := state.prepare(nil, false)
	require.NoError(t, err)
	require.NotEmpty(t, finalDelete.data)
	finalDelete.markSendAttempted()
	finalDelete.commit()
	require.Empty(t, state.assets)
	require.Empty(t, state.placements)

	parked := &parkedAttachment{
		sess: &session{sessionCore: sessionCore{name: "parked"}},
		ac:   &attachedClient{graphicsOutput: state},
		done: make(chan struct{}),
	}
	d.mu.Lock()
	d.parked[1] = parked
	d.mu.Unlock()
	d.expireParked(1, parked)

	d.mu.Lock()
	_, quarantined := d.graphicsNamespaceQuarantines[(oldBase-1)/graphicsIDNamespaceSize]
	newBase, _ := d.reserveGraphicsNamespaceLeaseLocked(key)
	d.mu.Unlock()
	require.True(t, quarantined, "an apparently empty parked state still has unacknowledged terminal history")
	require.NotEqual(t, oldBase, newBase, "park expiry must not recycle IDs after the final delete")
}

func TestGraphicsNamespacePoolExhaustionFallsBackToText(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.mu.Lock()
	for block := uint64(0); block < graphicsIDNamespaceCount; block++ {
		d.graphicsNamespaces[block] = struct{}{}
	}
	ac := d.prepareAttachedClientLocked(
		&session{sessionCore: sessionCore{id: "work"}},
		&closeTrackingTransport{},
		domain.Geometry{Size: defaultSize},
		attachClientOptions{
			capabilitiesSet:      true,
			terminalCapabilities: ports.TerminalCapabilities{KittyGraphics: true, ColorMode: ports.TerminalColorTrueColor},
		},
	)
	d.mu.Unlock()

	require.Nil(t, ac.graphicsOutput)
	require.False(t, ac.terminalCapabilities.SupportsKittyGraphics())
	require.NotNil(t, ac.output, "namespace exhaustion must preserve the ordinary text renderer")
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
	d.mu.Lock()
	_, retained := d.graphicsNamespaceQuarantines[(oldBase-1)/graphicsIDNamespaceSize]
	d.mu.Unlock()
	require.True(t, retained, "late transport success is not a terminal ACK and must not release the namespace")
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
		graphics:         scene.Snapshot(),
		graphicsGeometry: domain.Geometry{Size: domain.Size{Cols: 4, Rows: 3}, PixelWidth: 4, PixelHeight: 3},
		placement:        layout.Placement{Content: domain.Rect{X: 3, Y: 4, Width: 4, Height: 3}},
	}}}
	ac := &attachedClient{
		graphicsOutput:       newGraphicsOutputState(),
		terminalCapabilities: ports.TerminalCapabilities{KittyGraphics: true},
	}
	prepared, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err)
	text := string(prepared.data)
	require.Contains(t, text, "\x1b[6;4H", "placement must move to the clipped pane-local destination")
	require.Contains(t, text, ",x=2,y=1,w=4,h=3", "lowercase coordinates must preserve the source crop")
	require.NotContains(t, text, ",X=2,Y=1", "source coordinates must not become destination offsets")
}

func TestGraphicsOutputClipsEachAttachmentToOuterContentFrame(t *testing.T) {
	scene := graphics.NewScene(graphics.Limits{})
	asset, err := scene.AddAsset(graphics.AssetBlob{Encoded: []byte("attachment-frame"), Width: 80, Height: 24})
	require.NoError(t, err)
	_, err = scene.Place(graphics.PlacementSpec{
		Asset:       asset,
		Destination: graphics.PixelRect{Width: 800, Height: 480},
		Cells:       graphics.CellRect{Width: 80, Height: 24},
	})
	require.NoError(t, err)

	makeState := func(window domain.Size) *capturedRenderState {
		return &capturedRenderState{
			window: window,
			layout: capturedTabLayout{area: domain.Rect{Width: 80, Height: 24}, valid: true},
			panes: []capturedPaneRenderState{{
				id:               "pane",
				graphics:         scene.Snapshot(),
				graphicsGeometry: domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480},
				placement:        layout.Placement{ID: "pane", Content: domain.Rect{Width: 80, Height: 24}},
			}},
		}
	}

	small, err := graphicsOutputData(makeState(domain.Size{Cols: 40, Rows: 10}), graphicsPaneTestClient(), true)
	require.NoError(t, err)
	require.Len(t, small.state.placements, 1)
	for _, placement := range small.state.placements {
		require.Equal(t, graphics.PixelRect{Width: 400, Height: 160}, placement.dest, "small attachment must clip pixels to its content frame")
		require.Equal(t, graphics.CellRect{Width: 40, Height: 8}, placement.cells, "status bar row must stay outside the graphics cells")
	}
	require.Contains(t, string(small.data), ",c=40,r=8")
	require.NotContains(t, string(small.data), ",c=80,r=24")

	large, err := graphicsOutputData(makeState(domain.Size{Cols: 100, Rows: 30}), graphicsPaneTestClient(), true)
	require.NoError(t, err)
	require.Len(t, large.state.placements, 1)
	for _, placement := range large.state.placements {
		require.Equal(t, graphics.PixelRect{Width: 800, Height: 480}, placement.dest, "a larger attachment must retain the pane portion it can display")
	}
	require.Contains(t, string(large.data), ",c=80,r=24")
}

func TestGraphicsOutputCellOnlyViewportPreservesSourceMapping(t *testing.T) {
	scene := graphics.NewScene(graphics.Limits{})
	asset, err := scene.AddAsset(graphics.AssetBlob{Encoded: []byte("cell-only"), Width: 100, Height: 50})
	require.NoError(t, err)
	_, err = scene.Place(graphics.PlacementSpec{
		Asset:       asset,
		Destination: graphics.PixelRect{X: 10, Y: 20, Width: 200, Height: 100},
		Cells:       graphics.CellRect{X: 2, Y: 3, Width: 20, Height: 10},
	})
	require.NoError(t, err)

	viewport, ok := graphicsOutputViewportRect(domain.Rect{X: 7, Y: 5, Width: 5, Height: 5}, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
	require.True(t, ok)
	require.Equal(t, graphics.PixelRect{}, viewport.Pixels, "cell-only geometry must not become a pixel viewport")
	require.Equal(t, graphics.CellRect{X: 7, Y: 5, Width: 5, Height: 5}, viewport.Cells)

	_, placements := graphicsSnapshotRecords(scene.Snapshot(), viewport)
	require.Len(t, placements, 1)
	for _, placement := range placements {
		require.Equal(t, graphics.PixelRect{X: 25, Y: 10, Width: 25, Height: 25}, placement.source)
		require.Equal(t, graphics.PixelRect{X: 60, Y: 40, Width: 50, Height: 50}, placement.dest)
	}

	pixelOnly := graphics.NewScene(graphics.Limits{})
	pixelAsset, err := pixelOnly.AddAsset(graphics.AssetBlob{Encoded: []byte("pixel-only"), Width: 10, Height: 10})
	require.NoError(t, err)
	_, err = pixelOnly.PlaceAsset(pixelAsset, graphics.PixelRect{Width: 10, Height: 10})
	require.NoError(t, err)
	_, unsuitable := graphicsSnapshotRecords(pixelOnly.Snapshot(), viewport)
	require.Empty(t, unsuitable, "pixel-space fragments without cell metadata must be suppressed")
}

func TestGraphicsOutputComposesPaneOriginAndPixelGeometry(t *testing.T) {
	placement := graphicsOutputPlacement{
		id:     1,
		source: graphics.PixelRect{Width: 20, Height: 10},
		dest:   graphics.PixelRect{X: 103, Y: 50, Width: 20, Height: 10},
		cells:  graphics.CellRect{Width: 2, Height: 1},
	}
	record := placementRecordWithTransform(1, placement, graphicsOutputTransform{
		originX:        3,
		originY:        2,
		sourceGeometry: domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480},
	})
	text := string(record)
	require.Contains(t, text, "\x1b7\x1b[5;14H", "destination cells must be expressed through cursor position")
	require.Contains(t, text, ",X=3,Y=10", "destination pixel remainder must use uppercase offsets")
	require.NotContains(t, text, ",x=13,y=4", "destination cells must never become source crop controls")
	require.Contains(t, text, ",C=1", "placement must not move or scroll the text cursor")
	require.True(t, strings.HasSuffix(text, "\x1b8"), "placement must restore the outer text cursor")
}

func TestGraphicsOutputPreservesKittenIcatCenteredPlacement(t *testing.T) {
	placement := graphicsOutputPlacement{
		id:     1,
		source: graphics.PixelRect{Width: 103, Height: 128},
		dest:   graphics.PixelRect{X: 343, Width: 103, Height: 128},
	}
	record := placementRecordWithTransform(1, placement, graphicsOutputTransform{
		originY:        1,
		sourceGeometry: domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480},
	})
	text := string(record)
	require.Contains(t, text, "\x1b[2;35H", "icat's centered cursor column must survive composition")
	require.Contains(t, text, ",w=103,h=128", "natural image dimensions must be preserved")
	require.Contains(t, text, ",X=3", "sub-cell destination offset must be preserved")
	require.NotContains(t, text, ",x=34", "center position must not crop the image source")
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

func graphicsPaneTestScene(t *testing.T, data string, destination graphics.PixelRect, layer int64) *graphics.Snapshot {
	t.Helper()
	scene := graphics.NewScene(graphics.Limits{})
	asset, err := scene.AddAsset(graphics.AssetBlob{Encoded: []byte(data), Width: 20, Height: 10})
	require.NoError(t, err)
	_, err = scene.Place(graphics.PlacementSpec{Asset: asset, Destination: destination, Layer: layer, HasLayer: true})
	require.NoError(t, err)
	return scene.Snapshot()
}

func graphicsPaneTestState(tab domain.TabStableID, panes ...capturedPaneRenderState) *capturedRenderState {
	for i := range panes {
		if panes[i].graphicsGeometry.PixelsKnown() || panes[i].placement.Content.Width <= 0 || panes[i].placement.Content.Height <= 0 {
			continue
		}
		content := panes[i].placement.Content
		panes[i].graphicsGeometry = domain.Geometry{
			Size:        domain.Size{Cols: content.Width, Rows: content.Height},
			PixelWidth:  content.Width,
			PixelHeight: content.Height,
		}
	}
	return &capturedRenderState{view: attachmentView{tabID: tab}, panes: panes}
}

func graphicsPaneTestClient() *attachedClient {
	return &attachedClient{
		graphicsOutput:       newGraphicsOutputState(),
		terminalCapabilities: ports.TerminalCapabilities{KittyGraphics: true},
	}
}

func TestGraphicsOutputComposesSplitPanesWithStableOrderAndChildIDIsolation(t *testing.T) {
	left := graphicsPaneTestScene(t, "left", graphics.PixelRect{X: -2, Y: 0, Width: 8, Height: 4}, 2)
	right := graphicsPaneTestScene(t, "right", graphics.PixelRect{X: 1, Y: 0, Width: 8, Height: 4}, -1)
	state := graphicsPaneTestState("tab-a",
		capturedPaneRenderState{id: "left", graphics: left, placement: layout.Placement{ID: "left", Content: domain.Rect{X: 0, Width: 20, Height: 4}}},
		capturedPaneRenderState{id: "right", graphics: right, placement: layout.Placement{ID: "right", Content: domain.Rect{X: 21, Width: 20, Height: 4}}},
	)
	ac := graphicsPaneTestClient()
	prepared, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err)
	require.Len(t, prepared.state.assets, 2, "raw a1 IDs from separate child scenes must not alias")
	require.Len(t, prepared.state.placements, 2)
	text := string(prepared.data)
	require.Less(t, strings.Index(text, "\x1b[2;1H"), strings.Index(text, "\x1b[2;23H"), "pane traversal order must be deterministic")
	var ids []uint64
	for _, asset := range prepared.state.assets {
		ids = append(ids, asset.id)
	}
	require.NotEqual(t, ids[0], ids[1])
}

func TestGraphicsOutputSplitResizeReplacesTranslatedPlacementWithoutSceneMutation(t *testing.T) {
	snapshot := graphicsPaneTestScene(t, "resize", graphics.PixelRect{Width: 8, Height: 4}, 0)
	ac := graphicsPaneTestClient()
	firstState := graphicsPaneTestState("tab-a", capturedPaneRenderState{id: "left", graphics: snapshot, placement: layout.Placement{ID: "left", Content: domain.Rect{Width: 20, Height: 4}}})
	first, err := graphicsOutputData(firstState, ac, true)
	require.NoError(t, err)
	first.commit()

	resized := graphicsPaneTestState("tab-a", capturedPaneRenderState{id: "left", graphics: snapshot, placement: layout.Placement{ID: "left", Content: domain.Rect{X: 7, Width: 20, Height: 4}}})
	second, err := graphicsOutputData(resized, ac, false)
	require.NoError(t, err)
	require.Contains(t, string(second.data), "a=d,d=p,p=")
	require.Contains(t, string(second.data), "a=p,i=")
	require.NotContains(t, string(second.data), "a=t,i=", "layout-only recomputation must retain the immutable asset")
}

func TestGraphicsOutputPaneReorderRetainsAssetsButReplaysPlacementsInLayoutOrder(t *testing.T) {
	left := graphicsPaneTestScene(t, "left-reorder", graphics.PixelRect{Width: 8, Height: 4}, 0)
	right := graphicsPaneTestScene(t, "right-reorder", graphics.PixelRect{Width: 8, Height: 4}, 0)
	ac := graphicsPaneTestClient()
	firstState := graphicsPaneTestState("tab-a",
		capturedPaneRenderState{id: "left", graphics: left, placement: layout.Placement{ID: "left", Content: domain.Rect{Width: 20, Height: 4}}},
		capturedPaneRenderState{id: "right", graphics: right, placement: layout.Placement{ID: "right", Content: domain.Rect{X: 21, Width: 20, Height: 4}}},
	)
	first, err := graphicsOutputData(firstState, ac, true)
	require.NoError(t, err)
	first.commit()

	reordered := graphicsPaneTestState("tab-a",
		capturedPaneRenderState{id: "right", graphics: right, placement: layout.Placement{ID: "right", Content: domain.Rect{Width: 20, Height: 4}}},
		capturedPaneRenderState{id: "left", graphics: left, placement: layout.Placement{ID: "left", Content: domain.Rect{X: 21, Width: 20, Height: 4}}},
	)
	second, err := graphicsOutputData(reordered, ac, false)
	require.NoError(t, err)
	require.NotContains(t, string(second.data), "a=t,i=", "moving panes must not re-upload immutable assets")
	require.Contains(t, string(second.data), "a=d,d=p,p=")
	require.Contains(t, string(second.data), "a=p,i=")
	require.Less(t, strings.Index(string(second.data), "\x1b[2;1H"), strings.Index(string(second.data), "\x1b[2;22H"))
}

func TestGraphicsOutputTabSwitchReplaysSameRawChildIDsInNewSourceNamespace(t *testing.T) {
	snapshot := graphicsPaneTestScene(t, "tab", graphics.PixelRect{Width: 8, Height: 4}, 0)
	ac := graphicsPaneTestClient()
	first, err := graphicsOutputData(graphicsPaneTestState("tab-a", capturedPaneRenderState{id: "pane", graphics: snapshot, placement: layout.Placement{ID: "pane", Content: domain.Rect{Width: 20, Height: 4}}}), ac, true)
	require.NoError(t, err)
	first.commit()
	second, err := graphicsOutputData(graphicsPaneTestState("tab-b", capturedPaneRenderState{id: "pane", graphics: snapshot, placement: layout.Placement{ID: "pane", Content: domain.Rect{Width: 20, Height: 4}}}), ac, false)
	require.NoError(t, err)
	require.Contains(t, string(second.data), "a=d,d=p,p=")
	require.Contains(t, string(second.data), "a=d,d=i,i=")
	require.Contains(t, string(second.data), "a=t,i=")
}

func TestGraphicsOutputFloatingPaneCoversTiledGraphicsAndRetainsItsOwnScene(t *testing.T) {
	tiled := graphicsPaneTestScene(t, "tiled", graphics.PixelRect{Width: 20, Height: 6}, 0)
	floating := graphicsPaneTestScene(t, "floating", graphics.PixelRect{Width: 3, Height: 2}, 3)
	state := graphicsPaneTestState("tab-a", capturedPaneRenderState{id: "tiled", graphics: tiled, placement: layout.Placement{ID: "tiled", Content: domain.Rect{Width: 20, Height: 6}}})
	state.floating = capturedFloatingRenderState{
		visible: true,
		pane: capturedPaneRenderState{id: "floating", graphics: floating, graphicsGeometry: domain.Geometry{
			Size: domain.Size{Cols: 3, Rows: 2}, PixelWidth: 3, PixelHeight: 2,
		}},
		geometry: floatingGeometry{Bounds: domain.Rect{X: 5, Y: 1, Width: 6, Height: 4}, Inner: domain.Rect{X: 6, Y: 2, Width: 3, Height: 2}},
	}
	ac := graphicsPaneTestClient()
	prepared, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err)
	require.Len(t, prepared.state.assets, 2)
	require.GreaterOrEqual(t, len(prepared.state.placements), 2, "floating and uncovered tiled fragments both remain visible")
	for _, placement := range prepared.state.placements {
		if strings.Contains(placement.asset, "floating") {
			continue
		}
		cover := graphics.PixelRect{X: 5, Y: 1, Width: 6, Height: 4}
		_, overlaps := placement.dest.Intersect(cover)
		require.False(t, overlaps, "tiled placement must not survive under floating coverage")
	}
}

func TestGraphicsOutputOverlayDismissalHidesAndRestoresImmutableSnapshots(t *testing.T) {
	snapshot := graphicsPaneTestScene(t, "overlay", graphics.PixelRect{Width: 8, Height: 4}, 0)
	ac := graphicsPaneTestClient()
	state := graphicsPaneTestState("tab-a", capturedPaneRenderState{id: "pane", graphics: snapshot, placement: layout.Placement{ID: "pane", Content: domain.Rect{Width: 20, Height: 4}}})
	first, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err)
	first.commit()

	state.overlays.copyActive = true
	hidden, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err)
	require.Contains(t, string(hidden.data), "a=d,d=p,p=")
	hidden.commit()
	state.overlays.copyActive = false
	restored, err := graphicsOutputData(state, ac, false)
	require.NoError(t, err)
	require.Contains(t, string(restored.data), "a=t,i=")
	require.Equal(t, uint64(1), snapshot.Usage().Assets, "composition must not mutate the child scene")
}

func TestGraphicsOutputMixedTextAndGraphicsKeepsANSIBeforeGraphics(t *testing.T) {
	snapshot := graphicsPaneTestScene(t, "mixed", graphics.PixelRect{Width: 8, Height: 4}, 0)
	ac := graphicsPaneTestClient()
	state := graphicsPaneTestState("tab-a", capturedPaneRenderState{id: "pane", graphics: snapshot, placement: layout.Placement{ID: "pane", Content: domain.Rect{Width: 20, Height: 4}}})
	prepared, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err)
	ansi := []byte("text-before-graphics")
	combined := append(append([]byte(nil), ansi...), prepared.data...)
	require.Less(t, bytes.Index(combined, ansi), bytes.Index(combined, []byte("\x1b_Ga=t,")))
}

func TestGraphicsOutputCollapsedAndHiddenPanesDoNotUploadRetainedAssets(t *testing.T) {
	snapshot := graphicsPaneTestScene(t, "collapsed", graphics.PixelRect{Width: 8, Height: 4}, 0)
	ac := graphicsPaneTestClient()
	state := graphicsPaneTestState("tab-a", capturedPaneRenderState{
		id:        "pane",
		graphics:  snapshot,
		placement: layout.Placement{ID: "pane", Content: domain.Rect{Width: 20, Height: 4}},
	})
	first, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err)
	first.commit()

	state.panes[0].placement.Collapsed = true
	collapsed, err := graphicsOutputData(state, ac, false)
	require.NoError(t, err)
	require.Empty(t, collapsed.state.assets)
	require.Empty(t, collapsed.state.placements)
	require.Contains(t, string(collapsed.data), "a=d,d=p,p=")
	collapsed.commit()

	state.panes = nil
	hidden, err := graphicsOutputData(state, ac, false)
	require.NoError(t, err)
	require.Empty(t, hidden.data, "a collapsed scene was already retired")
	hidden.commit()
}

func TestGraphicsOutputViewportProjectionDropsUnplacedAssets(t *testing.T) {
	scene := graphics.NewScene(graphics.Limits{})
	placed, err := scene.AddAsset(graphics.AssetBlob{Encoded: []byte("placed"), Width: 4, Height: 2})
	require.NoError(t, err)
	_, err = scene.AddAsset(graphics.AssetBlob{Encoded: []byte("retained"), Width: 4, Height: 2})
	require.NoError(t, err)
	_, err = scene.PlaceAsset(placed, graphics.PixelRect{Width: 4, Height: 2})
	require.NoError(t, err)

	state := graphicsPaneTestState("tab-a", capturedPaneRenderState{
		id:        "pane",
		graphics:  scene.Snapshot(),
		placement: layout.Placement{ID: "pane", Content: domain.Rect{Width: 8, Height: 4}},
	})
	ac := graphicsPaneTestClient()
	prepared, err := graphicsOutputData(state, ac, true)
	require.NoError(t, err)
	require.Len(t, prepared.state.assets, 1, "viewport projection must not upload an unplaced retained image")
	require.Len(t, prepared.state.placements, 1)
}
