// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-tab PTY reader
// and VT screen, and the per-client debounced render scheduler.
package daemon

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bnema/vev/internal/ports"
)

// maxImagePushSize independently caps an accepted MsgImagePush payload,
// defending against an old or foreign client even though the client is
// expected to enforce the same cap before sending
// (docs/superpowers/specs/2026-07-04-clipboard-image-transfer-design.md).
const maxImagePushSize = 10 << 20 // 10 MiB

// Bracketed-paste markers used to wrap the injected clip path when the
// focused pane has DEC private mode 2004 enabled. Mirrors the client's
// pasteOpenMarker/pasteCloseMarker (internal/usecase/client), kept as a
// separate copy since the daemon has no dependency on the client package.
var (
	clipPasteOpenMarker  = []byte("\x1b[200~")
	clipPasteCloseMarker = []byte("\x1b[201~")
)

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
	name := fmt.Sprintf("vev-clip-%d.%s", d.clock.Now().UnixNano(), clipboardExt(ip.Mime))
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, ip.Data, 0o600); err != nil {
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
