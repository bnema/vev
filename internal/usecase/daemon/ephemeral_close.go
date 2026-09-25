package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/protocol"
)

// reapAbandonedEphemeral removes an ephemeral session whose last attachment
// ended definitively (a closed client process, or an expired park or
// suspension) when ephemeral.close-on-exit is on. A user detach or picker
// switch never calls it: those keep the session for later reattach.
//
// The emptiness and ephemeral checks run under the architecture locks at
// snapshot and publication, so any live, suspended, parked, or parking
// attachment, or a concurrent rename into a named session, keeps it.
// Callers must not hold daemon, session, or attachment locks.
func (d *Daemon) reapAbandonedEphemeral(sess *session) {
	if sess == nil || !d.currentEphemeralConfig().CloseOnExit {
		return
	}
	err := d.killEphemeralSessionIfEmpty(sess, protocol.ReasonSessionKilled)
	switch {
	case errors.Is(err, errSessionKillParticipantsChanged):
		// An attachment or rename won the race: the session stays by design.
		d.log.Debug("ephemeral close-on-exit skipped", "session", sess.nameSnapshot())
	case err != nil:
		d.log.Warn("ephemeral close-on-exit failed", "err", err, "session", sess.nameSnapshot())
	case !d.sessionRegistered(sess):
		d.log.Info("ephemeral session gone after last attachment ended", "session", sess.nameSnapshot())
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
