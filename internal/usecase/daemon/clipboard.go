// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-tab PTY reader
// and VT screen, and the per-client debounced render scheduler.
package daemon

import (
	"os"

	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
)

// maxImagePushSize independently caps an accepted MsgImagePush payload,
// defending against an old or foreign client even though the client is
// expected to enforce the same cap before sending. Kept at 1 MiB so one
// ImagePush fits the datagram transport's fragmented payload ceiling.
const maxImagePushSize = 1 << 20 // 1 MiB

// Bracketed-paste markers used to wrap the injected clip path when the
// focused pane has DEC private mode 2004 enabled. Mirrors the client's
// pasteOpenMarker/pasteCloseMarker (internal/usecase/client), kept as a
// separate copy since the daemon has no dependency on the client package.
var (
	clipPasteOpenMarker  = []byte("\x1b[200~")
	clipPasteCloseMarker = []byte("\x1b[201~")
)

// handleSequencedImagePush routes an ImagePush through the same input-sequence
// surface as ordinary keystrokes. Echo prediction is still conservative; the
// sequence is carried so resume/dedup plumbing can order image pushes with
// surrounding input when that state reader grows beyond ordinary input.
func (d *Daemon) handleSequencedImagePush(sess *session, _ *attachedClient, _ uint64, ip ports.ImagePush) {
	d.handleImagePush(sess, ip)
}

// handleImagePush writes an ImagePush's bytes to a temp file and injects the
// file's path into the session's focused pane, as if typed/pasted there.
// Oversized or empty payloads are rejected without panicking. A write
// failure is logged and the push is dropped: the client has already
// consumed the Ctrl+V keystroke that triggered it, so there is nothing left
// to re-trigger from here.
func (d *Daemon) handleImagePush(sess *session, ip ports.ImagePush) {
	if len(ip.Data) == 0 {
		return
	}
	if len(ip.Data) > maxImagePushSize {
		d.log.Warn("rejected oversized clipboard image push", "size", len(ip.Data), "cap", maxImagePushSize)
		return
	}
	path, err := d.writeClipboardImage(sess, ip)
	if err != nil {
		d.log.Error("writing clipboard image failed", "err", err)
		return
	}
	d.injectClipboardPath(sess, path)
}

// writeClipboardImage writes ip's bytes to a new 0600 temp file and records
// the path on sess for best-effort cleanup at session end (killSession).
func (d *Daemon) writeClipboardImage(sess *session, ip ports.ImagePush) (string, error) {
	dir := d.tempDir
	if dir == "" {
		dir = os.TempDir()
	}
	f, err := os.CreateTemp(dir, "vev-clip-*."+clipboardExt(ip.Mime))
	if err != nil {
		return "", err
	}
	path := f.Name()
	if _, err := f.Write(ip.Data); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	sess.mu.Lock()
	sess.clipFiles = append(sess.clipFiles, path)
	sess.mu.Unlock()
	return path, nil
}

// injectClipboardPath writes path's text into sess's focused pane, through
// the same PTY write path as ordinary input, wrapped in bracketed-paste
// markers iff the pane's Screen reports bracketed-paste mode enabled.
func (d *Daemon) injectClipboardPath(sess *session, path string) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	p := tb.focusedPane()
	tb.mu.Unlock()
	if p == nil {
		return
	}

	p.mu.Lock()
	bracketed := p.screen.BracketedPasteMode()
	p.mu.Unlock()

	data := []byte(path)
	if bracketed {
		wrapped := make([]byte, 0, len(clipPasteOpenMarker)+len(data)+len(clipPasteCloseMarker))
		wrapped = append(wrapped, clipPasteOpenMarker...)
		wrapped = append(wrapped, data...)
		wrapped = append(wrapped, clipPasteCloseMarker...)
		data = wrapped
	}
	d.writeToPane(sess, p, data)
}

// clipboardExt maps an ImagePush MIME type to a filename extension. Unknown
// types fall back to "bin" rather than rejecting the push: the daemon copies
// bytes as-is regardless (see the design doc's "no image conversion" scope
// decision).
func clipboardExt(mime string) string {
	switch mime {
	case "image/png":
		return "png"
	case "image/jpeg":
		return "jpg"
	case "image/webp":
		return "webp"
	case "image/gif":
		return "gif"
	default:
		return "bin"
	}
}

type clipboardForward struct {
	ac  *attachedClient
	seq []byte
}

// forwardClipboardAsync re-emits an app-originated OSC 52 clipboard set
// request (already captured off the pane's screen while ptyReader held pane.mu,
// then handed here after that lock is released) to the session's attached
// client, if any. Invalid base64 or an oversized decoded payload is dropped
// silently, matching copy mode's own cap (scopy.OSC52MaxPayloadBytes). The
// caller must not hold pane.mu, tab.mu, or session.mu.
//
// Requests are queued on the session and drained by one worker so clipboard
// writes keep arrival order without making the PTY reader wait on client I/O.
func (d *Daemon) forwardClipboardAsync(sess *session, b64 string) {
	seq := scopy.OSC52FromBase64(b64)
	if seq == nil {
		return
	}

	sess.mu.Lock()
	if sess.ctx != nil {
		select {
		case <-sess.ctx.Done():
			sess.mu.Unlock()
			return
		default:
		}
	}
	ac := sess.client
	if ac == nil {
		sess.mu.Unlock()
		return
	}
	sess.clipboardQueue = append(sess.clipboardQueue, clipboardForward{ac: ac, seq: seq})
	if !sess.clipboardWorkerRunning {
		sess.clipboardWorkerRunning = true
		go d.clipboardWorker(sess)
	}
	sess.mu.Unlock()
}

func (d *Daemon) clipboardWorker(sess *session) {
	for {
		item, ok := nextClipboardForward(sess)
		if !ok {
			return
		}
		failed, err := d.boundedSendOutputErrTransport(item.ac, item.seq)
		if err != nil {
			d.detachOnSendError(sess, item.ac, failed)
		}
	}
}

func nextClipboardForward(sess *session) (clipboardForward, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.ctx != nil {
		select {
		case <-sess.ctx.Done():
			sess.clipboardQueue = nil
			sess.clipboardWorkerRunning = false
			return clipboardForward{}, false
		default:
		}
	}
	if len(sess.clipboardQueue) == 0 {
		sess.clipboardWorkerRunning = false
		return clipboardForward{}, false
	}
	item := sess.clipboardQueue[0]
	copy(sess.clipboardQueue, sess.clipboardQueue[1:])
	var zero clipboardForward
	sess.clipboardQueue[len(sess.clipboardQueue)-1] = zero
	sess.clipboardQueue = sess.clipboardQueue[:len(sess.clipboardQueue)-1]
	return item, true
}
