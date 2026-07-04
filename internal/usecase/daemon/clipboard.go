package daemon

import (
	scopy "github.com/bnema/vev/internal/usecase/copy"
)

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
