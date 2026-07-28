package daemon

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
)

func TestInjectClipboardPathTargetsVisibleFloatingPane(t *testing.T) {
	normalWrites := make(chan []byte, 1)
	floatingWrites := make(chan []byte, 1)
	normal, releaseNormal := newBlockingPTYWithWrites(t, normalWrites)
	floatingPTY, releaseFloating := newBlockingPTYWithWrites(t, floatingWrites)
	defer releaseNormal()
	defer releaseFloating()
	d, sess, _, _ := newManualSessionWithPTYs(t, normal)
	floating := newPane("floating", floatingPTY, domain.Size{Cols: 20, Rows: 5})
	floating.screen.Write([]byte("\x1b[?2004h"))
	installTestFloating(sess.activeTab(), floating, true)

	d.injectClipboardPath(sess, "/tmp/image.png")

	requirePTYWrite(t, floatingWrites, []byte("\x1b[200~/tmp/image.png\x1b[201~"))
	requireNoPTYWrite(t, normalWrites)
}

func TestHandleImagePushWritesFileWithExactBytesAndMode0600(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	d.tempDir = t.TempDir()

	data := []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x01, 0x02, 0x03}
	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: data})

	injected := <-writes
	path := string(injected)
	require.FileExists(t, path)
	require.Equal(t, ".png", filepath.Ext(path))
	require.True(t, filepath.IsAbs(path))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, got)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestHandleImagePushInjectsPathWithoutBracketedPasteWrapping(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	d.tempDir = t.TempDir()
	pane := sess.tabs[0].focusedPane()
	require.False(t, pane.screen.BracketedPasteMode())

	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: []byte("x")})

	injected := <-writes
	require.False(t, hasBracketedPasteMarkers(injected), "path must not be wrapped when the pane is not in bracketed-paste mode")
	require.FileExists(t, string(injected))
}

func TestHandleImagePushInjectsPathWithBracketedPasteWrapping(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	d.tempDir = t.TempDir()
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("\x1b[?2004h"))
	require.True(t, pane.screen.BracketedPasteMode())

	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: []byte("x")})

	injected := <-writes
	require.True(t, len(injected) > len(clipPasteOpenMarker)+len(clipPasteCloseMarker))
	require.Equal(t, clipPasteOpenMarker, injected[:len(clipPasteOpenMarker)])
	require.Equal(t, clipPasteCloseMarker, injected[len(injected)-len(clipPasteCloseMarker):])
	path := string(injected[len(clipPasteOpenMarker) : len(injected)-len(clipPasteCloseMarker)])
	require.FileExists(t, path)
}

func hasBracketedPasteMarkers(b []byte) bool {
	return len(b) >= len(clipPasteOpenMarker) &&
		string(b[:len(clipPasteOpenMarker)]) == string(clipPasteOpenMarker)
}

func TestHandleImagePushRejectsOversizedPayloadWithoutWritingOrInjecting(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	dir := t.TempDir()
	d.tempDir = dir

	huge := make([]byte, maxImagePushSize+1)
	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: huge})

	select {
	case w := <-writes:
		t.Fatalf("oversized push must not be injected into the pane, got %q", w)
	default:
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "oversized push must not write a temp file")
}

func TestHandleImagePushIgnoresEmptyPayload(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	dir := t.TempDir()
	d.tempDir = dir

	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: nil})

	select {
	case w := <-writes:
		t.Fatalf("empty push must not be injected, got %q", w)
	default:
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestClipboardExtMapsKnownMimeTypesAndFallsBackToBin(t *testing.T) {
	cases := map[string]string{
		"image/png":                "png",
		"image/jpeg":               "jpg",
		"image/webp":               "webp",
		"image/gif":                "gif",
		"image/bmp":                "bin",
		"":                         "bin",
		"application/octet-stream": "bin",
	}
	for mime, want := range cases {
		require.Equal(t, want, clipboardExt(mime), "mime %q", mime)
	}
}

func TestKillSessionRemovesClipboardTempFiles(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	d.tempDir = t.TempDir()
	// killSession requires the session to be registered in the daemon's
	// registry (newManualSessionWithPTYs already does this via d.sessions).

	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: []byte("x")})
	injected := <-writes
	path := string(injected)
	require.FileExists(t, path)

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	require.NoFileExists(t, path)
}

// chunkReadPTY returns a MockPTY whose Read yields each chunk in order, then
// reports io.EOF — driving ptyReader through exactly len(chunks) reads before
// it unwinds via reapPane, matching the style of TestPTYQueryGetsResponseWrittenBackToPTY.
func chunkReadPTY(t *testing.T, chunks ...[]byte) *portsmocks.MockPTY {
	t.Helper()
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(buf, chunks[0])
		if n == len(chunks[0]) {
			chunks = chunks[1:]
		} else {
			chunks[0] = chunks[0][n:]
		}
		return n, nil
	})
	p.EXPECT().Write(mock.Anything).Return(0, nil).Maybe()
	p.EXPECT().Close().Return(nil).Maybe()
	return p
}

func publishActiveClipboardCapability(d *Daemon, sess *session, ac *attachedClient, tr ports.Transport) {
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.attachmentToken(ac, tr)
	token.lease = rc.attachmentLease(ac)
	ac.publishRoleCapability(token)
}

func clipboardOwnerLease(sess *session) paneEffectLease {
	tb := sess.activeTab()
	tb.mu.Lock()
	p := tb.terminalTargetLocked()
	tb.mu.Unlock()
	return p.effectLease()
}

func TestPTYReaderForwardsOSC52ClipboardToAttachedClient(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := chunkReadPTY(t, []byte("\x1b]52;c;aGVsbG8=\x07"))

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{id: "clip", name: "clip", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	publishTiledPaneOwners(sess, win)
	ac.setSession(sess)
	d.sessions[sess.id] = sess
	publishActiveClipboardCapability(d, sess, ac, tr)

	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	var clipboardOutput string
	for range 3 {
		f := awaitFrame(t, sends, ports.MsgOutput)
		out, err := ports.UnmarshalOutput(f.Payload)
		require.NoError(t, err)
		clipboardOutput = string(out.Data)
		if strings.Contains(clipboardOutput, "\x1b]52;c;aGVsbG8=\x07") {
			break
		}
	}
	require.Contains(t, clipboardOutput, "\x1b]52;c;aGVsbG8=\x07")
}

func TestPTYReaderDropsOversizedClipboardPayload(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	big := strings.Repeat("a", scopy.OSC52MaxPayloadBytes+1)
	b64 := base64.StdEncoding.EncodeToString([]byte(big))
	p := chunkReadPTY(t, []byte("\x1b]52;c;"+b64+"\x07"))

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{id: "clip-big", name: "clip-big", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	publishTiledPaneOwners(sess, win)
	ac.setSession(sess)
	d.sessions[sess.id] = sess

	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case f := <-sends:
		require.NotEqual(t, ports.MsgOutput, f.Type)
	default:
	}
}

func TestPTYReaderDropsInvalidBase64Clipboard(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := chunkReadPTY(t, []byte("\x1b]52;c;not-valid-base64!!\x07"))

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{id: "clip-bad", name: "clip-bad", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	publishTiledPaneOwners(sess, win)
	ac.setSession(sess)
	d.sessions[sess.id] = sess

	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case f := <-sends:
		require.NotEqual(t, ports.MsgOutput, f.Type)
	default:
	}
}

func TestPTYReaderClipboardNoAttachedClientDoesNotPanic(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := chunkReadPTY(t, []byte("\x1b]52;c;aGVsbG8=\x07"))

	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	win := newTestTabWithContext(p, sctx, cancel)
	sess := &session{id: "clip-noclient", name: "clip-noclient", tabs: []*tab{win}, ctx: sctx, cancel: cancel}
	publishTiledPaneOwners(sess, win)
	d.sessions[sess.id] = sess

	d.sessWg.Add(1)
	require.NotPanics(t, func() {
		d.ptyReader(sess, win, win.focusedPane())
	})
}

type staleClipboardErrorTransport struct {
	mu    sync.Mutex
	sends int
}

func (t *staleClipboardErrorTransport) Send(ports.Frame) error {
	t.mu.Lock()
	t.sends++
	t.mu.Unlock()
	return errors.New("stale clipboard send")
}

func (*staleClipboardErrorTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (*staleClipboardErrorTransport) Close() error               { return nil }

func (t *staleClipboardErrorTransport) sendCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sends
}

func TestQueuedClipboardAfterPaneMoveDoesNotSendToFormerOwner(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := &staleClipboardErrorTransport{}
	ac.replaceTransport(oldTransport)
	publishActiveClipboardCapability(d, sess, ac, oldTransport)

	sess.mu.Lock()
	sess.clipboardWorkerRunning = true
	sess.mu.Unlock()
	d.forwardClipboardAsync(clipboardOwnerLease(sess), base64.StdEncoding.EncodeToString([]byte("queued")))

	sourceTab := sess.activeTab()
	p := sourceTab.focusedPane()
	destination := &session{id: "destination", name: "destination"}
	publishPaneOwner(p, destination, &tab{}, 0)
	publishPaneOwner(p, sess, sourceTab, 0)

	d.clipboardWorker(sess)

	require.Zero(t, oldTransport.sendCount(), "clipboard queued by the former pane owner reached its client")
	require.Same(t, ac, sess.client, "stale clipboard send handling detached the former owner's client")
}

type movingClipboardErrorTransport struct {
	started   chan struct{}
	release   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newMovingClipboardErrorTransport() *movingClipboardErrorTransport {
	return &movingClipboardErrorTransport{
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (t *movingClipboardErrorTransport) Send(ports.Frame) error {
	close(t.started)
	<-t.release
	return errors.New("clipboard send failed after pane move")
}

func (*movingClipboardErrorTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }

func (t *movingClipboardErrorTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func TestQueuedClipboardRevalidatesOwnerAfterWaitingForClientSendLock(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := &staleClipboardErrorTransport{}
	ac.replaceTransport(oldTransport)
	publishActiveClipboardCapability(d, sess, ac, oldTransport)

	sess.mu.Lock()
	sess.clipboardWorkerRunning = true
	sess.mu.Unlock()
	d.forwardClipboardAsync(clipboardOwnerLease(sess), base64.StdEncoding.EncodeToString([]byte("queued")))

	ac.sendMu.Lock()
	sendMuLocked := true
	defer func() {
		if sendMuLocked {
			ac.sendMu.Unlock()
		}
	}()
	workerDone := make(chan struct{})
	go func() {
		d.clipboardWorker(sess)
		close(workerDone)
	}()
	require.Eventually(t, func() bool {
		ac.roleEffects.mu.Lock()
		defer ac.roleEffects.mu.Unlock()
		return ac.roleEffects.inFlight == 1
	}, time.Second, time.Millisecond, "clipboard send was not admitted before waiting on sendMu")

	sourceTab := sess.activeTab()
	p := sourceTab.focusedPane()
	destination := &session{id: "destination", name: "destination"}
	publishPaneOwner(p, destination, &tab{}, 0)
	publishPaneOwner(p, sess, sourceTab, 0)
	ac.sendMu.Unlock()
	sendMuLocked = false
	awaitTestCompletion(t, workerDone, "clipboard worker did not finish")

	require.Zero(t, oldTransport.sendCount(), "clipboard send was not revalidated immediately before transport I/O")
	require.Same(t, ac, sess.client)
}

func TestClipboardSendErrorAfterPaneMoveDoesNotDetachFormerOwner(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newMovingClipboardErrorTransport()
	ac.replaceTransport(oldTransport)
	publishActiveClipboardCapability(d, sess, ac, oldTransport)

	sess.mu.Lock()
	sess.clipboardWorkerRunning = true
	sess.mu.Unlock()
	d.forwardClipboardAsync(clipboardOwnerLease(sess), base64.StdEncoding.EncodeToString([]byte("queued")))

	workerDone := make(chan struct{})
	go func() {
		d.clipboardWorker(sess)
		close(workerDone)
	}()
	awaitTestCompletion(t, oldTransport.started, "clipboard send did not start")

	sourceTab := sess.activeTab()
	p := sourceTab.focusedPane()
	destination := &session{id: "destination", name: "destination"}
	publishPaneOwner(p, destination, &tab{}, 0)
	publishPaneOwner(p, sess, sourceTab, 0)
	close(oldTransport.release)
	awaitTestCompletion(t, workerDone, "clipboard worker did not finish")

	require.Same(t, ac, sess.client, "a send error from the pane's retired owner detached its client")
	select {
	case <-oldTransport.closed:
		t.Fatal("a send error from the pane's retired owner closed its client transport")
	default:
	}
}

func TestQueuedClipboardBeforeSnatchDropsExactStaleCapability(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := &staleClipboardErrorTransport{}
	old.replaceTransport(oldTransport)
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.publishRoleCapability(token)

	sess.mu.Lock()
	sess.clipboardWorkerRunning = true
	sess.mu.Unlock()
	d.forwardClipboardAsync(clipboardOwnerLease(sess), base64.StdEncoding.EncodeToString([]byte("queued")))

	newTransport := &closeTrackingTransport{}
	next := &attachedClient{tr: newTransport, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	_, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)

	d.clipboardWorker(sess)
	require.Zero(t, oldTransport.sendCount(), "stale queued work reached its captured failing transport")
	require.Empty(t, newTransport.Sends(), "stale queued work was redirected to the replacement")
	require.Same(t, next, sess.client, "stale clipboard send handling detached the current client")
	require.False(t, newTransport.Closed())
}

func TestForwardClipboardAsyncSerializesClipboardWrites(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	tr := newBlockingClipboardTransport()
	ac.replaceTransport(tr)
	publishActiveClipboardCapability(d, sess, ac, tr)
	owner := clipboardOwnerLease(sess)

	first := base64.StdEncoding.EncodeToString([]byte("first"))
	second := base64.StdEncoding.EncodeToString([]byte("second"))
	d.forwardClipboardAsync(owner, first)
	require.Equal(t, "first", <-tr.started)

	d.forwardClipboardAsync(owner, second)
	select {
	case got := <-tr.started:
		t.Fatalf("second clipboard send started before first completed: %q", got)
	case <-time.After(50 * time.Millisecond):
	}

	close(tr.releaseFirst)
	require.Equal(t, "first", <-tr.sent)
	require.Equal(t, "second", <-tr.started)
	require.Equal(t, "second", <-tr.sent)
}

type blockingClipboardTransport struct {
	started      chan string
	sent         chan string
	releaseFirst chan struct{}
}

func newBlockingClipboardTransport() *blockingClipboardTransport {
	return &blockingClipboardTransport{
		started:      make(chan string, 2),
		sent:         make(chan string, 2),
		releaseFirst: make(chan struct{}),
	}
}

func (tr *blockingClipboardTransport) Send(f ports.Frame) error {
	out, err := ports.UnmarshalOutput(f.Payload)
	if err != nil {
		return err
	}
	data := string(out.Data)
	var label string
	switch {
	case strings.Contains(data, base64.StdEncoding.EncodeToString([]byte("first"))):
		label = "first"
	case strings.Contains(data, base64.StdEncoding.EncodeToString([]byte("second"))):
		label = "second"
	default:
		label = data
	}
	tr.started <- label
	if label == "first" {
		<-tr.releaseFirst
	}
	tr.sent <- label
	return nil
}

func (tr *blockingClipboardTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (tr *blockingClipboardTransport) Close() error               { return nil }
