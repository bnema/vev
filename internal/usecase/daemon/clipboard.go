package daemon

import (
	scopy "github.com/bnema/vev/internal/usecase/copy"
)

// forwardClipboard re-emits an app-originated OSC 52 clipboard set request
// (already captured off the pane's screen while ptyReader held pane.mu, then
// handed here after that lock is released) to the session's attached client,
// if any. Invalid base64 or an oversized decoded payload is dropped silently,
// matching copy mode's own cap (scopy.OSC52MaxPayloadBytes). The caller must
// not hold pane.mu, tab.mu, or session.mu.
func (d *Daemon) forwardClipboard(sess *session, b64 string) {
	seq := scopy.OSC52FromBase64(b64)
	if seq == nil {
		return
	}

	sess.mu.Lock()
	ac := sess.client
	sess.mu.Unlock()
	if ac == nil {
		return
	}

	failed, err := d.boundedSendOutputErrTransport(ac, seq)
	if err != nil {
		d.detachOnSendError(sess, ac, failed)
	}
}
