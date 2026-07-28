package daemon

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

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
			return tok
		}
	}
}

func (d *Daemon) prepareParkAttachment(sess *session, ac *attachedClient) bool {
	if !ac.resumeCapable {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return false
	}
	if ac.resumeToken == 0 {
		ac.resumeToken = d.nextResumeTokenLocked()
	}
	d.log.Info("parking client prepared", "session", sess.name)
	return true
}

func (d *Daemon) parkAttachment(sess *session, ac *attachedClient) bool {
	return d.parkAttachmentAs(sess, ac, attachmentActive)
}

func (d *Daemon) parkAttachmentAs(sess *session, ac *attachedClient, role attachmentRole) bool {
	if !d.prepareParkAttachment(sess, ac) {
		return false
	}
	// A parked attachment has no session owner. Release pane snapshots before
	// publishing it in the daemon registry so headless pane/session teardown
	// cannot leave closed panes retained through its capture cache. This must
	// remain outside d.mu: sendMu is ordered before the daemon lock.
	ac.clearCaptureFrames()
	d.mu.Lock()
	if d.closing || d.sessions[sess.id] != sess {
		d.mu.Unlock()
		return false
	}
	if role == attachmentActive {
		sess.mu.Lock()
		if sess.client != nil {
			role = attachmentSnatched
		}
		sess.mu.Unlock()
	}
	token := ac.resumeToken
	if old := d.parked[token]; old != nil {
		old.timer.Stop()
		old.closeDone()
	}
	grace := d.resumeParkGrace
	timer := d.clock.NewTimer(grace)
	parked := &parkedAttachment{sess: sess, ac: ac, role: role, timer: timer, done: make(chan struct{})}
	ac.parked = true
	d.parked[token] = parked
	d.mu.Unlock()
	d.log.Info("client parked for resume", "session", sess.name, "grace", grace)

	go func(token uint64, parked *parkedAttachment) {
		select {
		case <-timer.C():
			d.expireParked(token, parked)
		case <-parked.done:
		}
	}(token, parked)
	return true
}

func (d *Daemon) expireParked(token uint64, parked *parkedAttachment) {
	d.mu.Lock()
	if d.parked[token] == parked {
		d.removeParkedLocked(token, parked)
		d.mu.Unlock()
		d.log.Warn("parked client expired", "session", parked.sess.name)
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

// demoteParkedActiveForSessionLocked preserves resumable predecessors when a
// new active owner is published. Caller holds d.mu.
func (d *Daemon) demoteParkedActiveForSessionLocked(sess *session) {
	for _, parked := range d.parked {
		if parked.sess == sess && parked.role == attachmentActive {
			parked.role = attachmentSnatched
		}
	}
}

type parkedAttachmentRetirement struct {
	parked    *parkedAttachment
	transport transportSnapshot
}

func (d *Daemon) retireParkedAttachmentLocked(token uint64, parked *parkedAttachment) parkedAttachmentRetirement {
	delete(d.parked, token)
	parked.ac.clearPreviousSession()
	parked.ac.resumeToken = 0
	parked.ac.parked = false
	parked.ac.roleGeneration.Add(1)
	parked.ac.setSession(nil)
	return parkedAttachmentRetirement{
		parked:    parked,
		transport: parked.ac.transportSnapshot(),
	}
}

// purgeParkedForSessionLocked invalidates every parked token for sess and
// returns the external resources that must be retired after releasing d.mu.
// It is reserved for terminal session kill and daemon shutdown.
func (d *Daemon) purgeParkedForSessionLocked(sess *session) []parkedAttachmentRetirement {
	var retirements []parkedAttachmentRetirement
	for token, parked := range d.parked {
		if parked.sess == sess {
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

// finishParkedAttachmentRetirements stops timers and closes transports without
// holding daemon, session, coordinator, or pane locks.
func finishParkedAttachmentRetirements(retirements []parkedAttachmentRetirement) {
	for _, retirement := range retirements {
		if retirement.parked.timer != nil {
			retirement.parked.timer.Stop()
		}
		retirement.parked.closeDone()
		ac := retirement.parked.ac
		_ = ac.closeCapturedTransport(ac.revokeTransport(retirement.transport.transport))
	}
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
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return nil, nil, false, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
	}
	if d.parked[h.ResumeToken] != parked {
		return nil, nil, false, nil
	}
	return d.resumeParkedLocked(h, tr, sz)
}

// resumeParkedLocked completes a validated resume. Caller holds both ac.sendMu
// and d.mu in that order.
func (d *Daemon) resumeParkedLocked(h ports.Hello, tr ports.Transport, sz domain.Size) (*session, *attachedClient, bool, error) {
	parked := d.parked[h.ResumeToken]
	if parked == nil || h.ResumeToken == 0 {
		return nil, nil, false, nil
	}
	sess := parked.sess
	registered := d.sessions[sess.id] == sess
	if !registered {
		d.removeParkedLocked(h.ResumeToken, parked)
		d.log.Warn("resume rejected", "session", sess.name, "registered", false)
		return nil, nil, false, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	sess.mu.Lock()
	active := sess.client != nil
	sess.mu.Unlock()
	if active && parked.role == attachmentActive {
		parked.role = attachmentSnatched
	}
	ac := parked.ac
	if ac.clientID != h.ClientID {
		d.log.Warn("resume rejected", "session", sess.name, "err", "client id mismatch")
		return nil, nil, false, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	delete(d.parked, h.ResumeToken)
	parked.timer.Stop()
	parked.closeDone()
	// The parked attachment has no session owner and cannot paint. Retire the
	// abandoned transport's output chain before binding the replacement so the
	// mandatory first paint cannot be blocked by ACKs that died with the link.
	ac.output.rebase()
	ac.output.maxOutstanding = uint64(normalizeOutputWindow(h.MaxOutputInFlight))
	ac.replaceTransport(tr)
	ac.size = sz
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
		target:            sess,
		next:              ac,
		expectedRole:      attachmentDetached,
		targetRole:        parked.role,
		expectedTransport: ac.transportSnapshot(),
		ready:             false,
	})
	ac.sendMu.Lock()
	d.mu.Lock()
	if err != nil {
		return nil, nil, false, &protoErr{ports.ErrInternal, "resume attachment transition failed"}
	}
	d.deferAttachmentTransitionCleanups(transition)
	d.touchMRU(sess)
	d.log.Info("client resumed", "session", sess.name)
	return sess, ac, true, nil
}
