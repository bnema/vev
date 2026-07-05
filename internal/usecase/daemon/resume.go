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
	sess.mu.Lock()
	ephemeral := sess.ephemeral
	sess.mu.Unlock()
	if ephemeral || !ac.resumeCapable {
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
	if !d.prepareParkAttachment(sess, ac) {
		return false
	}
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		return false
	}
	token := ac.resumeToken
	if old := d.parked[token]; old != nil {
		old.timer.Stop()
		old.closeDone()
	}
	timer := d.clock.NewTimer(resumeParkGrace)
	parked := &parkedAttachment{sess: sess, ac: ac, timer: timer, done: make(chan struct{})}
	ac.parked = true
	d.parked[token] = parked
	d.mu.Unlock()
	d.log.Info("client parked for resume", "session", sess.name, "grace", resumeParkGrace)

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

// purgeParkedForSessionLocked invalidates every parked token for sess. Caller
// holds d.mu. Use before killing or normally attaching to a session so stale
// resume tokens cannot resurrect or replace an active attachment.
func (d *Daemon) purgeParkedForSessionLocked(sess *session) {
	for token, parked := range d.parked {
		if parked.sess == sess {
			d.removeParkedLocked(token, parked)
		}
	}
}

func (d *Daemon) resumeParkedLocked(h ports.Hello, tr ports.Transport, sz domain.Size) (*session, *attachedClient, bool, error) {
	parked := d.parked[h.ResumeToken]
	if parked == nil || h.ResumeToken == 0 {
		return nil, nil, false, nil
	}
	sess := parked.sess
	registered := d.sessions[sess.id] == sess
	sess.mu.Lock()
	active := sess.client != nil
	sess.mu.Unlock()
	if !registered || active {
		d.removeParkedLocked(h.ResumeToken, parked)
		d.log.Warn("resume rejected", "session", sess.name, "registered", registered, "active", active)
		return nil, nil, false, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	ac := parked.ac
	if ac.clientID != h.ClientID {
		d.log.Warn("resume rejected", "session", sess.name, "err", "client id mismatch")
		return nil, nil, false, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	delete(d.parked, h.ResumeToken)
	parked.timer.Stop()
	parked.closeDone()
	ac.replaceTransport(tr)
	ac.size = sz
	ac.resumeToken = d.nextResumeTokenLocked()
	ac.parked = false
	ac.setSession(sess)
	sess.mu.Lock()
	sess.client = ac
	sess.mu.Unlock()
	d.touchMRU(sess)
	d.log.Info("client resumed", "session", sess.name)
	return sess, ac, true, nil
}
