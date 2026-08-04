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

func (d *Daemon) handleSequencedImagePushForAttachment(token attachmentConnectionToken, _ uint64, ip ports.ImagePush) {
	if !token.attachmentEffectCurrent() {
		return
	}
	d.handleImagePushForAttachment(token, ip)
}

func (d *Daemon) handleImagePushForAttachment(token attachmentConnectionToken, ip ports.ImagePush) {
	if len(ip.Data) == 0 || len(ip.Data) > maxImagePushSize || !token.attachmentEffectCurrent() {
		return
	}
	path, err := d.writeClipboardImageForAttachment(token, ip)
	if err != nil {
		d.log.Error("writing clipboard image failed", "err", err)
		return
	}
	if !token.attachmentEffectCurrent() {
		_ = os.Remove(path)
		return
	}
	d.injectClipboardPathForAttachment(token, path)
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

func (d *Daemon) writeClipboardImageForAttachment(token attachmentConnectionToken, ip ports.ImagePush) (string, error) {
	path, err := d.createClipboardImage(ip)
	if err != nil {
		return "", err
	}
	sess, ok := localSession(token.sess)
	if !ok {
		_ = os.Remove(path)
		return "", errAttachmentTransition
	}
	sess.mu.Lock()
	if !token.attachmentEffectCurrentSessionLocked() {
		sess.mu.Unlock()
		_ = os.Remove(path)
		return "", errAttachmentTransition
	}
	sess.clipFiles = append(sess.clipFiles, path)
	sess.mu.Unlock()
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

func (d *Daemon) injectClipboardPathForAttachment(token attachmentConnectionToken, path string) {
	sess, ok := localSession(token.sess)
	if !ok || !token.attachmentEffectCurrent() {
		return
	}
	d.injectClipboardPathToTarget(sess, path, &token)
}

func (d *Daemon) injectClipboardPathToTarget(sess *session, path string, token *attachmentConnectionToken) {
	var ac *attachedClient
	if token != nil {
		ac = token.ac
	}
	var tb *tab
	if ac != nil {
		tb = sess.tabForAttachment(ac)
	} else {
		tb = sess.firstTab()
	}
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
	if token != nil && !token.attachmentEffectCurrent() {
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
	owner paneEffectLease
	token attachmentConnectionToken
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
func (d *Daemon) forwardClipboardAsync(owner paneEffectLease, b64 string) {
	seq := scopy.OSC52FromBase64(b64)
	if seq == nil || !owner.Current() {
		return
	}
	sess := owner.owner.session

	sess.mu.Lock()
	if sess.ctx != nil {
		select {
		case <-sess.ctx.Done():
			sess.mu.Unlock()
			return
		default:
		}
	}
	attachments := sess.snapshotAttachmentsLocked()
	sess.mu.Unlock()

	items := make([]clipboardForward, 0, len(attachments))
	for _, ac := range attachments {
		transport := ac.transport()
		token := sess.attachmentToken(ac, transport)
		if token.attachmentCurrent() {
			items = append(items, clipboardForward{owner: owner, token: token, seq: seq})
		}
	}
	if len(items) == 0 {
		return
	}
	sess.mu.Lock()
	sess.clipboardQueue = append(sess.clipboardQueue, items...)
	if !sess.clipboardWorkerRunning {
		sess.clipboardWorkerRunning = true
		go d.clipboardWorker(sess)
	}
	sess.mu.Unlock()
}

func (d *Daemon) boundedSendClipboardForward(item clipboardForward, ticket *attachmentEffectTicket) (ports.Transport, error) {
	token := item.token
	if token.ac == nil || ticket == nil || ticket.ended.Load() || token.transport.transport == nil {
		return token.transport.transport, errAttachmentTransition
	}
	expected := token.transport
	ownedTransport, owned := expected.transport.(ports.OwnedSynchronousTransport)
	send := func() error {
		ac := token.ac
		ac.sendMu.Lock()
		defer ac.sendMu.Unlock()
		if ticket.ended.Load() || !ac.transportSnapshotCurrent(expected) {
			return errAttachmentTransition
		}
		if !beginClipboardOwnerSend(item.owner, ticket, expected) {
			return errAttachmentTransition
		}
		unlockView := ac.output.lockView()
		defer unlockView()
		frame, err := ac.output.sideEffectLocked(item.seq, ac.echoAck.Load())
		if err == nil {
			if owned {
				err = ownedTransport.SendSynchronous(frame)
			} else {
				err = expected.transport.Send(frame)
			}
		}
		if err != nil {
			ticket.reportTransportFailure(expected)
		}
		ticket.endTransportSend()
		return err
	}
	if owned {
		return expected.transport, send()
	}
	return d.boundedSendWith(expected.transport, send)
}

// beginClipboardOwnerSend validates the source pane generation after sendMu
// admission and marks the transport interval while ownership publication is
// excluded. Publication therefore orders either before this check (the send is
// dropped) or after transport admission (the send belongs to the old owner).
func beginClipboardOwnerSend(lease paneEffectLease, ticket *attachmentEffectTicket, expected transportSnapshot) bool {
	if lease.pane == nil || lease.owner == nil || lease.owner.session == nil || lease.owner.tab == nil {
		return false
	}
	owner := lease.owner
	if owner.floatingSlotGeneration != 0 {
		owner.tab.mu.Lock()
		defer owner.tab.mu.Unlock()
	}
	lease.pane.mu.Lock()
	defer lease.pane.mu.Unlock()
	if lease.pane.owner.Load() != owner {
		return false
	}
	if owner.floatingSlotGeneration != 0 &&
		(owner.tab.floating.pane != lease.pane || owner.tab.floating.generation != owner.floatingSlotGeneration ||
			(owner.tab.floating.state != floatingHidden && owner.tab.floating.state != floatingVisible)) {
		return false
	}
	return ticket.beginTransportSend(expected)
}

func (d *Daemon) clipboardWorker(sess *session) {
	for {
		item, ok := nextClipboardForward(sess)
		if !ok {
			return
		}
		if !item.owner.Current() {
			continue
		}
		ticket, admitted := item.token.ac.beginAttachmentEffect(item.token)
		if !admitted {
			continue
		}
		failed, err := d.boundedSendClipboardForward(item, ticket)
		if errors.Is(err, errSendTimedOut) {
			_ = item.token.ac.closeCapturedTransport(failed)
		}
		ticket.End()
		if err != nil && item.owner.Current() {
			d.detachOnAttachmentSendError(item.token, failed)
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
