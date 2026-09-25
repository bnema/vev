package daemon

import "github.com/bnema/vev/internal/protocol"

// reapAbandonedEphemeral removes an ephemeral session whose last attachment
// ended definitively (a closed client process, or an expired park or
// suspension) when ephemeral.close-on-exit is on. A user detach or picker
// switch never calls it: those keep the session for later reattach. Any live, suspended, parked, or parking
// attachment keeps the session: a network drop parks and can still resume.
// Callers must not hold daemon, session, or attachment locks.
func (d *Daemon) reapAbandonedEphemeral(sess *session) {
	if sess == nil || !d.currentEphemeralConfig().CloseOnExit {
		return
	}
	sess.mu.Lock()
	ephemeral := sess.ephemeral
	sess.mu.Unlock()
	if !ephemeral {
		return
	}
	// killSessionIfEmpty fences on every live, suspended, parked, and parking
	// attachment under the architecture locks, so a racing attachment wins.
	if err := d.killSessionIfEmpty(sess, protocol.ReasonSessionKilled, false); err != nil {
		d.log.Warn("ephemeral close-on-exit failed", "err", err, "session", sess.nameSnapshot())
		return
	}
	if !d.sessionRegistered(sess) {
		d.log.Info("ephemeral session closed on exit", "session", sess.nameSnapshot())
	}
}

// sessionHasPendingResumeLocked reports whether sess still retains a parked or
// in-flight parking attachment that a reconnecting client may resume. Caller
// holds d.mu.
func (d *Daemon) sessionHasPendingResumeLocked(sess *session) bool {
	for _, parked := range d.parked {
		if parked.sess == sess {
			return true
		}
	}
	for _, parking := range d.parking {
		if parking.sess == sess {
			return true
		}
	}
	return false
}
