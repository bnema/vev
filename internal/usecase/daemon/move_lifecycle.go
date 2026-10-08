package daemon

import (
	"cmp"
	"errors"
	"slices"
	"sync"
)

var errMoveLifecycleUnavailable = errors.New("move lifecycle is no longer available")

// moveLifecycleReservation pins a unique, canonically ordered set of sessions
// plus the daemon move gate. Release is idempotent so every abort path can use
// defer without coordinating ownership with its caller.
type moveLifecycleReservation struct {
	daemon   *Daemon
	sessions []*session
	once     sync.Once
}

// reserveMoveLifecycles admits a move at the daemon gate before touching any
// per-session lifecycle state. All session counts publish atomically while the
// complete ordered teardown lock set is held.
func (d *Daemon) reserveMoveLifecycles(source, destination *session) (*moveLifecycleReservation, error) {
	if d == nil || source == nil || destination == nil {
		return nil, errMoveLifecycleUnavailable
	}

	d.moveLifecycleMu.Lock()
	if d.moveLifecycleClosing {
		d.moveLifecycleMu.Unlock()
		return nil, errMoveLifecycleUnavailable
	}
	d.mu.Lock()
	closing := d.closing
	purgeClosing := d.purgeAdmissionClosing
	if !closing && !purgeClosing {
		// A move transfers live state across the exact lifecycle set a KillAll
		// purge captured, so it reserves the purge admission gate too: the purge
		// drains this reservation before it snapshots, and a move arriving during
		// the purge is rejected instead of racing the set.
		d.purgeAdmissionActive++
	}
	d.mu.Unlock()
	if closing {
		d.moveLifecycleMu.Unlock()
		return nil, errMoveLifecycleUnavailable
	}
	if purgeClosing {
		// A transient KillAll purge owns the exact lifecycle set, so no move can
		// reserve it. Unlike a shutdown this is retry state, so report the same
		// typed admission outcome as the other command paths instead of a
		// permanent "target gone" verdict.
		d.moveLifecycleMu.Unlock()
		return nil, errPurgeAdmissionClosed
	}
	d.moveLifecycleActive++
	d.signalMoveLifecycleChangedLocked()
	d.moveLifecycleMu.Unlock()
	if d.afterMoveLifecycleGateBeforeTeardownLocks != nil {
		d.afterMoveLifecycleGateBeforeTeardownLocks()
	}

	sessions := uniqueMoveSessions(source, destination)
	for _, sess := range sessions {
		sess.teardownMu.Lock()
	}
	for _, sess := range sessions {
		if sess.teardownActive {
			unlockMoveSessions(sessions)
			d.moveLifecycleMu.Lock()
			d.releaseMoveAdmissionLocked()
			d.moveLifecycleMu.Unlock()
			return nil, errMoveLifecycleUnavailable
		}
	}
	for _, sess := range sessions {
		sess.moveReservations++
		sess.signalTeardownChangedLocked()
	}
	unlockMoveSessions(sessions)

	return &moveLifecycleReservation{daemon: d, sessions: sessions}, nil
}

func uniqueMoveSessions(source, destination *session) []*session {
	sessions := []*session{source}
	if destination != source {
		sessions = append(sessions, destination)
	}
	slices.SortFunc(sessions, func(left, right *session) int {
		return cmp.Compare(left.id, right.id)
	})
	return sessions
}

func unlockMoveSessions(sessions []*session) {
	for i := len(sessions) - 1; i >= 0; i-- {
		sessions[i].teardownMu.Unlock()
	}
}

func (r *moveLifecycleReservation) Release() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		r.daemon.moveLifecycleMu.Lock()
		for _, sess := range r.sessions {
			sess.teardownMu.Lock()
		}
		for _, sess := range r.sessions {
			sess.moveReservations--
			sess.signalTeardownChangedLocked()
		}
		unlockMoveSessions(r.sessions)
		r.daemon.releaseMoveAdmissionLocked()
		r.daemon.moveLifecycleMu.Unlock()
	})
}

// closeMoveLifecycles rejects new moves, publishes daemon shutdown to ordinary
// routing, then drains reservations without holding either daemon mutex. It
// deliberately does not cancel pane processes: the caller must first publish the
// shutdown-preservation policy for every live session, then call
// cancelPaneProcesses, so a PTY EOF caused by the cancellation can never be
// mistaken for a natural final-shell exit that purges a named session.
func (d *Daemon) closeMoveLifecycles() {
	if d == nil {
		return
	}
	d.moveLifecycleMu.Lock()
	if !d.moveLifecycleClosing {
		d.moveLifecycleClosing = true
		d.signalMoveLifecycleChangedLocked()
	}
	d.moveLifecycleMu.Unlock()

	// Publish ordinary shutdown admission separately: no move gate mutex is held
	// while taking the architecture registry lock.
	d.mu.Lock()
	d.closing = true
	d.mu.Unlock()

	d.moveLifecycleMu.Lock()
	for d.moveLifecycleActive != 0 {
		changed := d.moveLifecycleChangeLocked()
		d.moveLifecycleMu.Unlock()
		<-changed
		d.moveLifecycleMu.Lock()
	}
	d.moveLifecycleMu.Unlock()
}

// cancelPaneProcesses cancels the daemon-wide pane process lifetime, closing
// every PTY. It must run only after closeMoveLifecycles has drained the final
// move reservation and after every live session has reserved its shutdown
// teardown (see terminateAllForShutdown).
func (d *Daemon) cancelPaneProcesses() {
	if d == nil {
		return
	}
	d.moveLifecycleMu.Lock()
	cancel := d.paneProcessCancel
	d.moveLifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// releaseMoveAdmissionLocked drops one move's move-gate and KillAll purge-gate
// reservations. Caller holds moveLifecycleMu; the purge gate is guarded by mu.
func (d *Daemon) releaseMoveAdmissionLocked() {
	d.moveLifecycleActive--
	d.signalMoveLifecycleChangedLocked()
	d.mu.Lock()
	d.purgeAdmissionActive--
	d.signalPurgeAdmissionChangedLocked()
	d.mu.Unlock()
}

// moveLifecycleChangeLocked requires moveLifecycleMu, not d.mu.
func (d *Daemon) moveLifecycleChangeLocked() chan struct{} {
	if d.moveLifecycleChanged == nil {
		d.moveLifecycleChanged = make(chan struct{})
	}
	return d.moveLifecycleChanged
}

// signalMoveLifecycleChangedLocked requires moveLifecycleMu, not d.mu.
func (d *Daemon) signalMoveLifecycleChangedLocked() {
	close(d.moveLifecycleChangeLocked())
	d.moveLifecycleChanged = make(chan struct{})
}
