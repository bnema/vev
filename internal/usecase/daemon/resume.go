package daemon

import (
	"crypto/rand"
	"encoding/binary"
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

var errResumeTokenLifecycleRace = errors.New("resume token lifecycle race")

func newResumeToken() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand failed generating resume token: " + err.Error())
	}
	return binary.BigEndian.Uint64(b[:])
}

func (d *Daemon) nextResumeTokenLocked() uint64 {
	for {
		tok := newResumeToken()
		if tok == 0 {
			continue
		}
		if _, exists := d.parked[tok]; !exists {
			if _, parking := d.parking[tok]; !parking {
				return tok
			}
		}
	}
}

// markParkingInFlight publishes an observable parking lifecycle so IntentResume
// can wait out the detach→park gap. Callers must publish this before clearing
// the live seat so resume never observes both registries empty for a still-valid
// credential. Under d.mu then sess.mu it advertises only when the daemon still
// registers the exact session and ac remains a registered attachment; otherwise
// it returns 0 and never creates a marker after terminal
// cleanup. Returns the resume token, or 0 when parking cannot be advertised.
func (d *Daemon) markParkingInFlight(sess *session, ac *attachedClient) uint64 {
	return d.markParkingInFlightOwner(sess, ac)
}

// markParkingInFlightOwner advertises a stable attachment binding before its
// live membership is removed. The resume credential identifies this owner
// binding, never a local session name.
func (d *Daemon) markParkingInFlightOwner(owner attachmentOwner, ac *attachedClient) uint64 {
	if normalizeAttachmentOwner(owner) == nil || ac == nil || !ac.resumeCapable {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || !d.attachmentOwnerRegisteredByDaemonLocked(owner) || !attachmentOwnerRegistered(owner, ac) {
		return 0
	}
	if ac.resumeToken == 0 {
		ac.resumeToken = d.nextResumeTokenLocked()
	}
	d.ensureParkingInFlightLocked(owner, ac)
	d.log.Info("parking in flight", "session", attachmentOwnerName(owner))
	return ac.resumeToken
}

// ensureParkingInFlightLocked records or refreshes the in-flight marker for ac.
// Caller holds d.mu and has verified the daemon is not closing.
func (d *Daemon) ensureParkingInFlightLocked(owner attachmentOwner, ac *attachedClient) {
	token := ac.resumeToken
	if token == 0 {
		return
	}
	if existing := d.parking[token]; existing != nil {
		if existing.ac == ac {
			existing.owner = owner
			return
		}
		delete(d.parking, token)
		existing.closeDone()
	}
	d.parking[token] = &parkingAttachment{
		owner: owner,
		ac:    ac,
		done:  make(chan struct{}),
	}
}

func (d *Daemon) clearParkingInFlight(token uint64, ac *attachedClient) {
	if token == 0 {
		return
	}
	d.mu.Lock()
	d.clearParkingInFlightLocked(token, ac)
	d.mu.Unlock()
}

// clearParkingInFlightIfAbandoned drops a pre-detach parking marker when this
// teardown lost the seat while still the live owner (stale transport fence).
// If another path already detached/unrouted, that path
// owns park publication.
func (d *Daemon) clearParkingInFlightIfAbandoned(sess *session, ac *attachedClient, token uint64) {
	d.clearParkingInFlightIfAbandonedOwner(sess, ac, token)
}

func (d *Daemon) clearParkingInFlightIfAbandonedOwner(owner attachmentOwner, ac *attachedClient, token uint64) {
	if token == 0 || ac == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.parked[token] != nil || !d.attachmentOwnerRegisteredByDaemonLocked(owner) || !attachmentOwnerRegistered(owner, ac) {
		return
	}
	d.clearParkingInFlightLocked(token, ac)
}

func (d *Daemon) clearParkingInFlightLocked(token uint64, ac *attachedClient) {
	pending := d.parking[token]
	if pending == nil {
		return
	}
	if ac != nil && pending.ac != ac {
		return
	}
	delete(d.parking, token)
	pending.closeDone()
}

func (d *Daemon) purgeAllParkingLocked() {
	for token, pending := range d.parking {
		delete(d.parking, token)
		pending.closeDone()
	}
}

// purgeParkingForSessionLocked removes and closes every in-flight parking
// marker for sess so same-token waiters fail closed instead of hanging when
// the session is removed before park publication. Caller holds d.mu.
func (d *Daemon) purgeParkingForSessionLocked(sess *session) {
	for token, pending := range d.parking {
		if localSession(pending.owner) == sess {
			delete(d.parking, token)
			pending.closeDone()
		}
	}
}

// purgeParkingForRemoteViewLocked invalidates in-flight resume publication for
// one terminal remote view. At this point the view has no attachment members,
// so clearing the credential cannot race a live owner.
func (d *Daemon) purgeParkingForRemoteViewLocked(view *remoteView) {
	for token, pending := range d.parking {
		if owner, remote := normalizeAttachmentOwner(pending.owner).(*remoteView); remote && owner == view {
			delete(d.parking, token)
			pending.ac.resumeToken = 0
			pending.ac.parked = false
			pending.closeDone()
		}
	}
}

// waitParkingInFlight waits for an identified same-client parking publication
// to finish. Returns true when a matching in-flight entry was observed.
// Unknown and mismatched credentials return false without waiting.
func (d *Daemon) waitParkingInFlight(h ports.Hello) bool {
	if h.ResumeToken == 0 {
		return false
	}
	d.mu.Lock()
	pending := d.parking[h.ResumeToken]
	if pending == nil {
		d.mu.Unlock()
		return false
	}
	if pending.ac.clientID != h.ClientID {
		d.mu.Unlock()
		return false
	}
	if sess := localSession(pending.owner); h.Name != "" && sess != nil && sess.name != h.Name {
		d.mu.Unlock()
		return false
	}
	done := pending.done
	d.mu.Unlock()
	d.log.Info("resume waiting for in-flight parking", "session", h.Name)
	if d.afterParkingWaitArmed != nil {
		d.afterParkingWaitArmed()
	}
	<-done
	return true
}

func (d *Daemon) prepareParkAttachment(sess *session, ac *attachedClient) bool {
	return d.prepareParkAttachmentOwner(sess, ac)
}

func (d *Daemon) prepareParkAttachmentOwner(owner attachmentOwner, ac *attachedClient) bool {
	if normalizeAttachmentOwner(owner) == nil || ac == nil || !ac.resumeCapable {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || !d.attachmentOwnerRegisteredByDaemonLocked(owner) {
		return false
	}
	if ac.resumeToken == 0 {
		ac.resumeToken = d.nextResumeTokenLocked()
	}
	// Do not recreate parking markers here: post-detach publication races
	// terminal owner removal and can strand IntentResume waiters. Callers that
	// need the detach→park gap advertised must publish it while still exact.
	d.log.Info("parking client prepared", "session", attachmentOwnerName(owner))
	return true
}

func (d *Daemon) parkAttachment(sess *session, ac *attachedClient) bool {
	return d.parkAttachmentOwner(sess, ac)
}

func (d *Daemon) parkAttachmentOwner(owner attachmentOwner, ac *attachedClient) bool {
	if !d.prepareParkAttachmentOwner(owner, ac) {
		return false
	}
	// A parked attachment has no session owner. Release pane snapshots before
	// publishing it in the daemon registry so headless pane/session teardown
	// cannot leave closed panes retained through its capture cache. This must
	// remain outside d.mu: sendMu is ordered before the daemon lock.
	ac.clearCaptureFrames()
	var pickerGeneration uint64
	if ac.overlays != nil {
		ac.overlays.pickerMu.Lock()
		pickerGeneration = ac.overlays.pickerGeneration
		ac.overlays.pickerMu.Unlock()
	}
	d.mu.Lock()
	if d.closing || !d.attachmentOwnerRegisteredByDaemonLocked(owner) {
		d.clearParkingInFlightLocked(ac.resumeToken, ac)
		d.mu.Unlock()
		return false
	}
	token := ac.resumeToken
	var (
		oldRetirement *parkedAttachmentRetirement
		oldSame       *parkedAttachment
	)
	if old := d.parked[token]; old != nil {
		if old.ac == ac {
			oldSame = old
		} else {
			retirement := d.retireParkedAttachmentLocked(token, old)
			oldRetirement = &retirement
		}
	}
	grace := d.resumeParkGrace
	timer := d.clock.NewTimer(grace)
	parked := &parkedAttachment{owner: owner, ac: ac, pickerGeneration: pickerGeneration, timer: timer, done: make(chan struct{})}
	ac.parked = true
	d.parked[token] = parked
	d.clearParkingInFlightLocked(token, ac)
	d.mu.Unlock()
	if oldRetirement != nil {
		d.finishParkedAttachmentRetirements([]parkedAttachmentRetirement{*oldRetirement})
	}
	if oldSame != nil {
		if oldSame.timer != nil {
			oldSame.timer.Stop()
		}
		oldSame.closeDone()
	}
	d.log.Info("client parked for resume", "session", attachmentOwnerName(owner), "grace", grace)
	d.watchParkedTimer(token, parked)
	return true
}

func (d *Daemon) watchParkedTimer(token uint64, parked *parkedAttachment) {
	if parked == nil || parked.timer == nil {
		return
	}
	timer := parked.timer
	go func() {
		select {
		case <-timer.C():
			d.expireParked(token, parked)
		case <-parked.done:
		}
	}()
}

func (d *Daemon) expireParked(token uint64, parked *parkedAttachment) {
	d.mu.Lock()
	if d.parked[token] == parked {
		if parked.claimed {
			d.mu.Unlock()
			return
		}
		d.removeParkedLocked(token, parked)
		d.mu.Unlock()
		d.closePickerIfCurrent(parked.ac, nil, parked.pickerGeneration)
		d.log.Warn("parked client expired", "session", attachmentOwnerName(parked.owner))
		return
	}
	d.mu.Unlock()
}

// removeParkedLocked invalidates one parked attachment. Caller holds d.mu and
// has verified d.parked[token] still points at parked when that matters.
func (d *Daemon) removeParkedLocked(token uint64, parked *parkedAttachment) {
	delete(d.parked, token)
	parked.ac.clearPreviousSession()
	parked.ac.resumeToken = 0
	parked.ac.parked = false
	if parked.timer != nil {
		parked.timer.Stop()
	}
	parked.closeDone()
}

func (p *parkedAttachment) closeDone() {
	if p.done == nil {
		return
	}
	p.doneOnce.Do(func() { close(p.done) })
}

// commitResumeClaim consumes the old credential only after Welcome has been
// written successfully. The claim itself is serialized by d.mu, so a racing
// resume cannot acquire the same parked attachment.
func (d *Daemon) commitResumeClaim(ac *attachedClient) bool {
	if d == nil || ac == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	token := ac.resumeClaimToken
	parked := d.parked[token]
	if token == 0 || parked == nil || parked.ac != ac || !parked.claimed {
		return token == 0
	}
	delete(d.parked, token)
	parked.claimed = false
	if parked.timer != nil {
		parked.timer.Stop()
	}
	parked.closeDone()
	ac.resumeClaimToken = 0
	return true
}

// abortResumeClaim restores a parked credential when the transport could not
// complete the pre-claim Welcome handshake. It invalidates the claimed link
// generation before making the credential available to another winner.
func (d *Daemon) abortResumeClaim(ac *attachedClient) bool {
	if d == nil || ac == nil {
		return false
	}
	d.mu.Lock()
	token := ac.resumeClaimToken
	parked := d.parked[token]
	if token == 0 || parked == nil || parked.ac != ac || !parked.claimed {
		d.mu.Unlock()
		return false
	}
	parked.claimed = false
	ac.resumeClaimToken = 0
	ac.resumeToken = token
	ac.parked = true
	ac.connectionGeneration.Add(1)
	sess := localSession(parked.owner)
	view, remote := normalizeAttachmentOwner(parked.owner).(*remoteView)
	if sess != nil {
		sess.mu.Lock()
		sess.unregisterAttachmentLocked(ac)
		sess.mu.Unlock()
	} else if remote {
		view.mu.Lock()
		view.unregisterAttachmentLocked(ac)
		view.mu.Unlock()
	}
	// Recreate the parked entry and its watcher instead of restoring a timer
	// that may already have fired while the claim held the old entry. This also
	// makes a failed handshake's credential expiry deterministic under fake clocks.
	if parked.timer != nil {
		parked.timer.Stop()
	}
	parked.closeDone()
	rearmed := &parkedAttachment{
		owner:            parked.owner,
		ac:               ac,
		pickerGeneration: parked.pickerGeneration,
		timer:            d.clock.NewTimer(d.resumeParkGrace),
		done:             make(chan struct{}),
	}
	d.parked[token] = rearmed
	captured := ac.transportSnapshot().transport
	ac.setSession(nil)
	d.mu.Unlock()
	if sess != nil {
		d.recalculateSessionGeometryAndInvalidate(sess, nil, "resume.go")
	}
	if remote {
		d.parkRemoteViewWarm(view)
	}
	d.watchParkedTimer(token, rearmed)
	if captured != nil {
		_ = ac.closeCapturedTransport(ac.revokeTransport(captured))
	}
	return true
}

type parkedAttachmentRetirement struct {
	parked           *parkedAttachment
	pickerGeneration uint64
	transport        transportSnapshot
}

func (d *Daemon) retireParkedAttachmentLocked(token uint64, parked *parkedAttachment) parkedAttachmentRetirement {
	delete(d.parked, token)
	parked.ac.clearPreviousSession()
	parked.ac.resumeToken = 0
	parked.ac.parked = false
	parked.ac.connectionGeneration.Add(1)
	parked.ac.setSession(nil)
	return parkedAttachmentRetirement{
		parked:           parked,
		pickerGeneration: parked.pickerGeneration,
		transport:        parked.ac.transportSnapshot(),
	}
}

// purgeParkedForSessionLocked invalidates every parked token for sess and
// returns the external resources that must be retired after releasing d.mu.
// It is reserved for terminal session kill and daemon shutdown.
func (d *Daemon) purgeParkedForSessionLocked(sess *session) []parkedAttachmentRetirement {
	var retirements []parkedAttachmentRetirement
	for token, parked := range d.parked {
		if localSession(parked.owner) == sess {
			retirements = append(retirements, d.retireParkedAttachmentLocked(token, parked))
		}
	}
	return retirements
}

// purgeParkedForRemoteViewLocked retires every resume credential owned by the
// exact remote view. The caller completes the returned external cleanup after
// releasing d.mu.
func (d *Daemon) purgeParkedForRemoteViewLocked(view *remoteView) []parkedAttachmentRetirement {
	var retirements []parkedAttachmentRetirement
	for token, parked := range d.parked {
		if owner, remote := normalizeAttachmentOwner(parked.owner).(*remoteView); remote && owner == view {
			retirements = append(retirements, d.retireParkedAttachmentLocked(token, parked))
		}
	}
	return retirements
}

func (d *Daemon) purgeAllParkedLocked() []parkedAttachmentRetirement {
	retirements := make([]parkedAttachmentRetirement, 0, len(d.parked))
	for token, parked := range d.parked {
		retirements = append(retirements, d.retireParkedAttachmentLocked(token, parked))
	}
	return retirements
}

// finishParkedAttachmentRetirements stops timers and retires picker ownership
// and transports without holding daemon, session, coordinator, or pane locks.
func (d *Daemon) finishParkedAttachmentRetirements(retirements []parkedAttachmentRetirement) {
	for _, retirement := range retirements {
		if retirement.parked.timer != nil {
			retirement.parked.timer.Stop()
		}
		retirement.parked.closeDone()
		ac := retirement.parked.ac
		d.closePickerIfCurrent(ac, nil, retirement.pickerGeneration)
		_ = ac.closeCapturedTransport(ac.revokeTransport(retirement.transport.transport))
	}
}

// resumeLiveAttachment recovers a nonzero resume credential that still belongs
// to the named session's active attachment because transport teardown has not
// parked it yet. Only the exact live owner (same session, token, and client ID)
// is accepted; arbitrary unknown tokens stay fail-closed.
func (d *Daemon) resumeLiveAttachment(h ports.Hello, tr ports.Transport, sz domain.Size) (*session, *attachedClient, bool, error) {
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		return nil, nil, false, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
	}
	// Teardown may have parked the credential already.
	if d.parked[h.ResumeToken] != nil {
		d.mu.Unlock()
		return d.resumeParked(h, tr, sz)
	}
	sess := d.findByNameLocked(h.Name)
	var (
		ac              *attachedClient
		oldSnap         transportSnapshot
		credentialMatch bool
		clientMismatch  bool
	)
	if sess != nil {
		sess.mu.Lock()
		for candidate := range sess.attachments {
			if !candidate.resumeCapable || candidate.resumeToken != h.ResumeToken {
				continue
			}
			ac = candidate
			if ac.clientID != h.ClientID {
				clientMismatch = true
			} else {
				oldSnap = ac.transportSnapshot()
				credentialMatch = oldSnap.transport != nil
			}
			break
		}
		sess.mu.Unlock()
	}
	// Teardown may have parked the credential while we resolved the session.
	if d.parked[h.ResumeToken] != nil {
		d.mu.Unlock()
		return d.resumeParked(h, tr, sz)
	}
	d.mu.Unlock()

	if clientMismatch {
		reason := "client id mismatch"
		d.log.Warn("resume rejected", "session", sess.name, "err", reason)
		return nil, nil, false, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	if !credentialMatch {
		// Detach may have won and not published parked yet; wait out that gap
		// for the exact same-client credential, then try the parked path.
		_ = d.waitParkingInFlight(h)
		return d.resumeParked(h, tr, sz)
	}

	if d.beforeMarkParkingInFlight != nil {
		d.beforeMarkParkingInFlight()
	}
	// Advertise parking before detach so a concurrent same-token resume never
	// observes both the live seat and parking/parked registries empty. Late
	// publication after terminal detach/session removal returns 0.
	parkingToken := d.markParkingInFlight(sess, ac)
	if !d.detachIfCurrentTransport(sess, ac, oldSnap) {
		d.clearParkingInFlightIfAbandoned(sess, ac, parkingToken)
		// A newer owner, rebound link, or teardown won; wait if teardown is
		// still publishing the park, then try the parked path.
		_ = d.waitParkingInFlight(h)
		return d.resumeParked(h, tr, sz)
	}
	if d.afterResumeLiveDetach != nil {
		d.afterResumeLiveDetach()
	}
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteDetach(ac)
	}
	d.recalculateSessionGeometryAndInvalidate(sess, nil, "resume.go")
	d.unregisterPreview(ac)
	if !d.parkAttachment(sess, ac) {
		// Detach already published; retire the captured link exactly once so
		// shutdown cannot leave an orphaned open transport with no owner.
		d.clearParkingInFlight(ac.resumeToken, ac)
		d.resetScreenDefaultColors(sess)
		ac.clearPreviousSession()
		_ = ac.closeCapturedTransport(ac.revokeTransport(oldSnap.transport))
		d.mu.Lock()
		closing := d.closing
		d.mu.Unlock()
		if closing {
			return nil, nil, false, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
		}
		return nil, nil, false, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	_ = ac.closeCapturedTransport(oldSnap.transport)
	d.log.Info("live resume credential parked for reconnect", "session", sess.name)
	return d.resumeParked(h, tr, sz)
}

// resumeParked acquires the parked client's output lock before the daemon
// registry lock, preserving the global sendMu > Daemon.mu ordering. The parked
// entry is revalidated after both locks are held because it may expire between
// the initial lookup and lock acquisition.
func (d *Daemon) resumeParked(h ports.Hello, tr ports.Transport, sz domain.Size) (*session, *attachedClient, bool, error) {
	d.mu.Lock()
	parked := d.parked[h.ResumeToken]
	d.mu.Unlock()
	if parked == nil || h.ResumeToken == 0 {
		return nil, nil, false, nil
	}

	ac := parked.ac
	if d.beforeResumeParkedSendMu != nil {
		d.beforeResumeParkedSendMu()
	}
	ac.sendMu.Lock()
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		ac.sendMu.Unlock()
		return nil, nil, false, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
	}
	if d.parked[h.ResumeToken] != parked || parked.claimed {
		if sess := localSession(parked.owner); sess != nil && d.sessions[sess.id] == sess {
			d.mu.Unlock()
			ac.sendMu.Unlock()
			return nil, nil, false, errResumeTokenLifecycleRace
		}
		d.mu.Unlock()
		ac.sendMu.Unlock()
		return nil, nil, false, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	sess, resumed, ok, err := d.resumeParkedLocked(h, tr, sz)
	d.mu.Unlock()
	ac.sendMu.Unlock()
	if err != nil && resumed != nil {
		d.abortResumeClaim(resumed)
	}
	return sess, resumed, ok, err
}

// resumeParkedLocked completes a validated resume. Caller holds both ac.sendMu
// and d.mu in that order.
func (d *Daemon) resumeParkedLocked(h ports.Hello, tr ports.Transport, sz domain.Size) (*session, *attachedClient, bool, error) {
	parked := d.parked[h.ResumeToken]
	if parked == nil || h.ResumeToken == 0 {
		return nil, nil, false, nil
	}
	sess := localSession(parked.owner)
	registered := sess != nil && d.sessions[sess.id] == sess
	if !registered {
		d.removeParkedLocked(h.ResumeToken, parked)
		d.log.Warn("resume rejected", "session", attachmentOwnerName(parked.owner), "registered", false)
		return nil, nil, false, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	ac := parked.ac
	if parked.claimed {
		return nil, nil, false, errResumeTokenLifecycleRace
	}
	if ac.clientID != h.ClientID {
		reason := "client id mismatch"
		d.log.Warn("resume rejected", "session", sess.name, "err", reason)
		return nil, nil, false, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	// Claim the attachment while retaining the old credential until Welcome is
	// accepted. A failed handshake can therefore restore this exact parked
	// entry; another resume sees claimed and loses atomically.
	parked.claimed = true
	ac.resumeClaimToken = h.ResumeToken
	// The parked attachment has no session owner and cannot paint. Retire the
	// abandoned transport's output chain before binding the replacement so the
	// mandatory first paint cannot be blocked by ACKs that died with the link.
	ac.rebaseOutput()
	ac.output.maxOutstanding = uint64(normalizeOutputWindow(h.MaxOutputInFlight))
	ac.output.maxOutstandingAtomic.Store(ac.output.maxOutstanding)
	ac.replaceTransport(tr)
	ac.setSize(sz)
	ac.resumeToken = d.nextResumeTokenLocked()
	ac.parked = false
	// The resumed session's snapshot is the sole source for future PTY children.
	// Existing PTYs retain the environment they were started with.
	sess.mu.Lock()
	sess.env = copyEnvironment(h.Env)
	sess.terminal = terminalEnv{TrueColor: h.TrueColor}
	sess.mu.Unlock()
	// Resume preparation used sendMu -> d.mu, but freeze/drain must run with
	// neither held. Reacquire them before returning to preserve this helper's
	// locked-caller contract.
	d.mu.Unlock()
	ac.sendMu.Unlock()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess,
		next:   ac,

		expectedTransport: ac.transportSnapshot(),
		ready:             false,
	})
	ac.sendMu.Lock()
	d.mu.Lock()
	if err != nil {
		return sess, ac, false, &protoErr{ports.ErrInternal, "resume attachment transition failed"}
	}
	d.deferAttachmentTransitionCleanups(transition)
	d.touchMRU(sess)
	d.log.Info("client resumed", "session", sess.name)
	return sess, ac, true, nil
}
