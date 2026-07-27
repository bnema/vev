// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-tab PTY reader
// and VT screen, and the per-client debounced render coordinator.
package daemon

import (
	"errors"
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

func (d *Daemon) handleSequencedImagePushForRole(token attachmentRoleToken, _ uint64, ip ports.ImagePush) {
	if !token.activeEffect() {
		return
	}
	d.handleImagePushForRole(token, ip)
}

func (d *Daemon) handleImagePushForRole(token attachmentRoleToken, ip ports.ImagePush) {
	if len(ip.Data) == 0 || len(ip.Data) > maxImagePushSize || !token.activeEffect() {
		return
	}
	path, err := d.writeClipboardImageForRole(token, ip)
	if err != nil {
		d.log.Error("writing clipboard image failed", "err", err)
		return
	}
	if !token.activeEffect() {
		_ = os.Remove(path)
		return
	}
	d.injectClipboardPathForRole(token, path)
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
	path, err := d.createClipboardImage(ip)
	if err != nil {
		return "", err
	}
	sess.mu.Lock()
	sess.clipFiles = append(sess.clipFiles, path)
	sess.mu.Unlock()
	return path, nil
}

func (d *Daemon) writeClipboardImageForRole(token attachmentRoleToken, ip ports.ImagePush) (string, error) {
	path, err := d.createClipboardImage(ip)
	if err != nil {
		return "", err
	}
	token.sess.mu.Lock()
	if !token.activeEffectSessionLocked() {
		token.sess.mu.Unlock()
		_ = os.Remove(path)
		return "", errAttachmentTransition
	}
	token.sess.clipFiles = append(token.sess.clipFiles, path)
	token.sess.mu.Unlock()
	return path, nil
}

func (d *Daemon) createClipboardImage(ip ports.ImagePush) (string, error) {
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
	return path, nil
}

// injectClipboardPath writes path's text into sess's focused pane, through
// the same PTY write path as ordinary input, wrapped in bracketed-paste
// markers iff the pane's Screen reports bracketed-paste mode enabled.
func (d *Daemon) injectClipboardPath(sess *session, path string) {
	d.injectClipboardPathToTarget(sess, path, nil)
}

func (d *Daemon) injectClipboardPathForRole(token attachmentRoleToken, path string) {
	if !token.activeEffect() {
		return
	}
	d.injectClipboardPathToTarget(token.sess, path, &token)
}

func (d *Daemon) injectClipboardPathToTarget(sess *session, path string, token *attachmentRoleToken) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	p := tb.terminalTargetLocked()
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
	if token != nil && !token.activeEffect() {
		return
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
	token attachmentRoleToken
	seq   []byte
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
	sess.mu.Unlock()
	if ac == nil {
		return
	}
	transport := ac.transport()
	token := sess.attachmentToken(ac, transport)
	if !token.activeCurrent() {
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
	if sess.client != ac || ac.roleGeneration.Load() != token.generation ||
		!ac.transportSnapshotCurrent(token.transport) || ac.currentSession() != sess {
		sess.mu.Unlock()
		return
	}
	sess.clipboardQueue = append(sess.clipboardQueue, clipboardForward{token: token, seq: seq})
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
		ticket, admitted := item.token.ac.beginRoleEffect(item.token)
		if !admitted {
			continue
		}
		failed, err := d.boundedSendOutputErrTransportForRole(item.token, ticket, item.seq)
		if errors.Is(err, errSendTimedOut) {
			_ = item.token.ac.closeCapturedTransport(failed)
		}
		ticket.End()
		if err != nil {
			d.detachOnRoleSendError(item.token, failed)
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
