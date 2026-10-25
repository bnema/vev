package daemon

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	scopy "github.com/bnema/vev/internal/usecase/copy"
)

func TestInjectClipboardPathTargetsVisibleFloatingPane(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		normalWrites := make(chan []byte, 1)
		floatingWrites := make(chan []byte, 1)
		normal, releaseNormal := newBlockingPTYWithWrites(t, normalWrites)
		floatingPTY, releaseFloating := newBlockingPTYWithWrites(t, floatingWrites)
		defer releaseNormal()
		defer releaseFloating()
		d, sess, _, _ := newManualSessionWithPTYs(t, normal)
		floating := newPane("floating", floatingPTY, domain.Size{Cols: 20, Rows: 5})
		floating.screen.Write([]byte("\x1b[?2004h"))
		installTestFloating(testAttachmentTab(sess), floating, true)

		d.injectClipboardPath(sess, "/tmp/image.png")

		requirePTYWrite(t, floatingWrites, []byte("\x1b[200~/tmp/image.png\x1b[201~"))
		requireNoPTYWrite(t, normalWrites)
	})
}

func TestHandleImagePushWritesFileWithExactBytesAndMode0600(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, releasePTY := newBlockingPTYWithWrites(t, writes)
		defer releasePTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		d.tempDir = t.TempDir()

		data := []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x01, 0x02, 0x03}
		d.handleImagePush(sess, protocol.ImagePush{Mime: "image/png", Data: data})

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
	})
}

func TestHandleImagePushInjectsPathWithoutBracketedPasteWrapping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, releasePTY := newBlockingPTYWithWrites(t, writes)
		defer releasePTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		d.tempDir = t.TempDir()
		pane := sess.tabs[0].focusedPane()
		require.False(t, pane.screen.BracketedPasteMode())

		d.handleImagePush(sess, protocol.ImagePush{Mime: "image/png", Data: []byte("x")})

		injected := <-writes
		require.False(t, hasBracketedPasteMarkers(injected), "path must not be wrapped when the pane is not in bracketed-paste mode")
		require.FileExists(t, string(injected))
	})
}

func TestHandleImagePushInjectsPathWithBracketedPasteWrapping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, releasePTY := newBlockingPTYWithWrites(t, writes)
		defer releasePTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		d.tempDir = t.TempDir()
		pane := sess.tabs[0].focusedPane()
		pane.screen.Write([]byte("\x1b[?2004h"))
		require.True(t, pane.screen.BracketedPasteMode())

		d.handleImagePush(sess, protocol.ImagePush{Mime: "image/png", Data: []byte("x")})

		injected := <-writes
		require.True(t, len(injected) > len(clipPasteOpenMarker)+len(clipPasteCloseMarker))
		require.Equal(t, clipPasteOpenMarker, injected[:len(clipPasteOpenMarker)])
		require.Equal(t, clipPasteCloseMarker, injected[len(injected)-len(clipPasteCloseMarker):])
		path := string(injected[len(clipPasteOpenMarker) : len(injected)-len(clipPasteCloseMarker)])
		require.FileExists(t, path)
	})
}

func hasBracketedPasteMarkers(b []byte) bool {
	return len(b) >= len(clipPasteOpenMarker) &&
		string(b[:len(clipPasteOpenMarker)]) == string(clipPasteOpenMarker)
}

func TestHandleImagePushRejectsOversizedPayloadWithoutWritingOrInjecting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, releasePTY := newBlockingPTYWithWrites(t, writes)
		defer releasePTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		dir := t.TempDir()
		d.tempDir = dir

		huge := make([]byte, maxImagePushSize+1)
		d.handleImagePush(sess, protocol.ImagePush{Mime: "image/png", Data: huge})

		select {
		case w := <-writes:
			t.Fatalf("oversized push must not be injected into the pane, got %q", w)
		default:
		}
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries, "oversized push must not write a temp file")
	})
}

func TestHandleImagePushIgnoresEmptyPayload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, releasePTY := newBlockingPTYWithWrites(t, writes)
		defer releasePTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		dir := t.TempDir()
		d.tempDir = dir

		d.handleImagePush(sess, protocol.ImagePush{Mime: "image/png", Data: nil})

		select {
		case w := <-writes:
			t.Fatalf("empty push must not be injected, got %q", w)
		default:
		}
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries)
	})
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
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, releasePTY := newBlockingPTYWithWrites(t, writes)
		defer releasePTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		d.tempDir = t.TempDir()
		// killSession requires the session to be registered in the daemon's
		// registry (newManualSessionWithPTYs already does this via d.sessions).

		d.handleImagePush(sess, protocol.ImagePush{Mime: "image/png", Data: []byte("x")})
		injected := <-writes
		path := string(injected)
		require.FileExists(t, path)

		require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, false))
		require.NoFileExists(t, path)
	})
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

func publishActiveClipboardCapability(d *Daemon, sess *session, ac *attachedClient, tr ports.ServerConnection) {
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, tr)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)
}

func clipboardOwnerLease(t *testing.T, sess *session) paneEffectLease {
	t.Helper()
	tb := testAttachmentTab(sess)
	tb.mu.Lock()
	p := tb.terminalTargetLocked()
	tb.mu.Unlock()
	if p == nil {
		t.Fatal("clipboard owner has no terminal target")
	}
	return p.effectLease()
}

func TestPTYReaderForwardsOSC52ClipboardToAttachedClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, nil, newStubClock(t))
		p := chunkReadPTY(t, []byte("\x1b]52;c;aGVsbG8=\x07"))

		tr, sends := newCapturingTransport(t)
		sctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		win := newTestTabWithContext(p, sctx, cancel)
		ac := &attachedClient{tr: tr, output: newOutputStateStream()}
		ac.initOverlays()
		sess := &session{sessionCore: sessionCore{id: "clip", name: "clip", attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{win}, ctx: sctx, cancel: cancel}
		publishTiledPaneOwners(sess, win)
		ac.setSession(sess)
		d.sessions[sess.id] = sess
		publishActiveClipboardCapability(d, sess, ac, tr)

		d.sessWg.Add(1)
		d.ptyReader(sess, win, win.focusedPane())

		var clipboardOutput string
		for range 3 {
			f := requireFrame(t, sends, "Output")
			out, ok := decodeServerMessage(t, f).(protocol.Output)
			require.True(t, ok)
			clipboardOutput = string(out.Data)
			if strings.Contains(clipboardOutput, "\x1b]52;c;aGVsbG8=\x07") {
				break
			}
		}
		require.Contains(t, clipboardOutput, "\x1b]52;c;aGVsbG8=\x07")
	})
}

func TestPTYReaderDropsOversizedClipboardPayload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, nil, newStubClock(t))
		big := strings.Repeat("a", scopy.OSC52MaxPayloadBytes+1)
		b64 := base64.StdEncoding.EncodeToString([]byte(big))
		p := chunkReadPTY(t, []byte("\x1b]52;c;"+b64+"\x07"))

		tr, sends := newCapturingTransport(t)
		sctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		win := newTestTabWithContext(p, sctx, cancel)
		ac := &attachedClient{tr: tr, output: newOutputStateStream()}
		ac.initOverlays()
		sess := &session{sessionCore: sessionCore{id: "clip-big", name: "clip-big", attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{win}, ctx: sctx, cancel: cancel}
		publishTiledPaneOwners(sess, win)
		ac.setSession(sess)
		d.sessions[sess.id] = sess

		d.sessWg.Add(1)
		d.ptyReader(sess, win, win.focusedPane())

		select {
		case f := <-sends:
			require.NotEqual(t, "Output", envelopeMessageName(t, f.Payload))
		default:
		}
	})
}

func TestPTYReaderDropsInvalidBase64Clipboard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, nil, newStubClock(t))
		p := chunkReadPTY(t, []byte("\x1b]52;c;not-valid-base64!!\x07"))

		tr, sends := newCapturingTransport(t)
		sctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		win := newTestTabWithContext(p, sctx, cancel)
		ac := &attachedClient{tr: tr, output: newOutputStateStream()}
		ac.initOverlays()
		sess := &session{sessionCore: sessionCore{id: "clip-bad", name: "clip-bad", attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{win}, ctx: sctx, cancel: cancel}
		publishTiledPaneOwners(sess, win)
		ac.setSession(sess)
		d.sessions[sess.id] = sess

		d.sessWg.Add(1)
		d.ptyReader(sess, win, win.focusedPane())

		select {
		case f := <-sends:
			require.NotEqual(t, "Output", envelopeMessageName(t, f.Payload))
		default:
		}
	})
}

func TestPTYReaderClipboardNoAttachedClientDoesNotPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, nil, newStubClock(t))
		p := chunkReadPTY(t, []byte("\x1b]52;c;aGVsbG8=\x07"))

		sctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		win := newTestTabWithContext(p, sctx, cancel)
		sess := &session{sessionCore: sessionCore{id: "clip-noclient", name: "clip-noclient"}, tabs: []*tab{win}, ctx: sctx, cancel: cancel}
		publishTiledPaneOwners(sess, win)
		d.sessions[sess.id] = sess

		d.sessWg.Add(1)
		require.NotPanics(t, func() {
			d.ptyReader(sess, win, win.focusedPane())
		})
	})
}

func TestQueuedClipboardAfterPaneMoveDoesNotSendToFormerOwner(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport, oldLog := newTrackedConnection(t, withSendError(errors.New("stale clipboard send")))
	ac.replaceTransport(oldTransport)
	publishActiveClipboardCapability(d, sess, ac, oldTransport)

	sess.mu.Lock()
	sess.clipboardWorkerRunning = true
	sess.mu.Unlock()
	d.forwardClipboardAsync(clipboardOwnerLease(t, sess), base64.StdEncoding.EncodeToString([]byte("queued")))

	sourceTab := testAttachmentTab(sess)
	p := sourceTab.focusedPane()
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination"}}
	publishPaneOwner(p, destination, &tab{}, 0)
	publishPaneOwner(p, sess, sourceTab, 0)

	d.clipboardWorker(sess)

	require.Zero(t, len(oldLog.Sends()), "clipboard queued by the former pane owner reached its client")
	require.True(t, slices.Contains(sess.snapshotAttachments(), ac), "stale clipboard send handling detached the former owner's client")
}

func TestQueuedClipboardRevalidatesOwnerAfterWaitingForClientSendLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock, clockTimers := newSignalClock(t, 8)
		d, sess, ac, _ := newManualSessionWithPTYsClock(t, clock, &transactionalResizePTY{})
		oldTransport, oldLog := newTrackedConnection(t, withSendError(errors.New("stale clipboard send")))
		ac.replaceTransport(oldTransport)
		publishActiveClipboardCapability(d, sess, ac, oldTransport)

		sess.mu.Lock()
		sess.clipboardWorkerRunning = true
		sess.mu.Unlock()
		d.forwardClipboardAsync(clipboardOwnerLease(t, sess), base64.StdEncoding.EncodeToString([]byte("queued")))

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
		// The worker is about to park on sendMu (a sync.Mutex wait is not durably
		// blocked, so synctest.Wait cannot observe it). Its bounded send arms a
		// timer right after admission and before taking sendMu: that is the signal.
		requireTimerArmed(t, clockTimers)
		require.EqualValues(t, 1, ac.lifecycle.inFlightCount(), "clipboard send was not admitted before waiting on sendMu")

		sourceTab := testAttachmentTab(sess)
		p := sourceTab.focusedPane()
		destination := &session{sessionCore: sessionCore{id: "destination", name: "destination"}}
		publishPaneOwner(p, destination, &tab{}, 0)
		publishPaneOwner(p, sess, sourceTab, 0)
		ac.sendMu.Unlock()
		sendMuLocked = false
		requireReceived(t, workerDone, "clipboard worker did not finish")

		require.Zero(t, len(oldLog.Sends()), "clipboard send was not revalidated immediately before transport I/O")
		require.True(t, slices.Contains(sess.snapshotAttachments(), ac))
	})
}

func TestClipboardSendErrorAfterPaneMoveDoesNotDetachFormerOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
		gate := newSendGate()
		oldTransport, oldLog := newTrackedConnection(t, withSendFunc(func(wire.Envelope, *connLog) error {
			gate.enter()
			gate.wait()
			return errors.New("clipboard send failed after pane move")
		}))
		ac.replaceTransport(oldTransport)
		publishActiveClipboardCapability(d, sess, ac, oldTransport)

		sess.mu.Lock()
		sess.clipboardWorkerRunning = true
		sess.mu.Unlock()
		d.forwardClipboardAsync(clipboardOwnerLease(t, sess), base64.StdEncoding.EncodeToString([]byte("queued")))
		sourceTab := testAttachmentTab(sess)
		p := sourceTab.focusedPane()

		workerDone := make(chan struct{})
		go func() {
			d.clipboardWorker(sess)
			close(workerDone)
		}()
		requireReceived(t, gate.entered, "clipboard send did not start")

		destination := &session{sessionCore: sessionCore{id: "destination", name: "destination"}}
		publishPaneOwner(p, destination, &tab{}, 0)
		publishPaneOwner(p, sess, sourceTab, 0)
		gate.open()
		requireReceived(t, workerDone, "clipboard worker did not finish")

		require.True(t, slices.Contains(sess.snapshotAttachments(), ac), "a send error from the pane's retired owner detached its client")
		require.False(t, oldLog.Closed(), "a send error from the pane's retired owner closed its client transport")
	})
}

func TestForwardClipboardAsyncSerializesClipboardWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
		started := make(chan string, 2)
		sent := make(chan string, 2)
		releaseFirst := newSendGate()
		tr, _ := newTrackedConnection(t, withSendFunc(func(f wire.Envelope, log *connLog) error {
			message, err := sessionwire.DecodeServerEnvelope(f.Payload)
			if err != nil {
				return err
			}
			out, ok := message.(protocol.Output)
			if !ok {
				return nil
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
			started <- label
			if label == "first" {
				releaseFirst.wait()
			}
			sent <- label
			return nil
		}))
		ac.replaceTransport(tr)
		publishActiveClipboardCapability(d, sess, ac, tr)
		owner := clipboardOwnerLease(t, sess)

		first := base64.StdEncoding.EncodeToString([]byte("first"))
		second := base64.StdEncoding.EncodeToString([]byte("second"))
		d.forwardClipboardAsync(owner, first)
		require.Equal(t, "first", requireReceived(t, started, "first clipboard send never started"))

		d.forwardClipboardAsync(owner, second)
		synctest.Wait()
		select {
		case got := <-started:
			t.Fatalf("second clipboard send started before first completed: %q", got)
		default:
		}

		releaseFirst.open()
		synctest.Wait()
		require.Equal(t, "first", requireReceived(t, sent, "first clipboard send never completed"))
		require.Equal(t, "second", requireReceived(t, started, "second clipboard send never started"))
		require.Equal(t, "second", requireReceived(t, sent, "second clipboard send never completed"))
	})
}

// requireTimerArmed blocks (durably) until a clock timer is published.
func requireTimerArmed(t *testing.T, timers <-chan *testTimer) *testTimer {
	t.Helper()
	return <-timers
}
