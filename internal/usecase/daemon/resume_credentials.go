package daemon

// resumeCredentials owns every resume credential that is not held by a live
// attachment: attachments between detach and park publication (parking), and
// parked attachments waiting for a reconnect (parked). A token is unique
// across both, and every lookup, publication, and retirement of either goes
// through this type, so the credential lifecycle has one owner.
//
// resumeCredentials is guarded by Daemon.mu; every method requires it held.
// It never takes another lock and never stops timers or closes transports
// beyond closing its own done channels: external retirement stays with the
// caller, after d.mu is released.
type resumeCredentials struct {
	parked  map[uint64]*parkedAttachment
	parking map[uint64]*parkingAttachment
}

func newResumeCredentials() resumeCredentials {
	return resumeCredentials{
		parked:  make(map[uint64]*parkedAttachment),
		parking: make(map[uint64]*parkingAttachment),
	}
}

// issue returns a fresh nonzero token no parked or parking entry holds.
func (c *resumeCredentials) issue() uint64 {
	for {
		token := newResumeToken()
		if token == 0 {
			continue
		}
		if c.parked[token] == nil && c.parking[token] == nil {
			return token
		}
	}
}

// parkedFor returns the parked entry holding token, or nil.
func (c *resumeCredentials) parkedFor(token uint64) *parkedAttachment { return c.parked[token] }

// parkingFor returns the in-flight parking entry holding token, or nil.
func (c *resumeCredentials) parkingFor(token uint64) *parkingAttachment { return c.parking[token] }

// sessionFor returns the session a parked or parking token belongs to.
func (c *resumeCredentials) sessionFor(token uint64) *session {
	if parked := c.parked[token]; parked != nil {
		return parked.sess
	}
	if pending := c.parking[token]; pending != nil {
		return pending.sess
	}
	return nil
}

// retainsSession reports whether any parked or parking credential keeps sess.
func (c *resumeCredentials) retainsSession(sess *session) bool {
	for _, parked := range c.parked {
		if parked.sess == sess {
			return true
		}
	}
	for _, pending := range c.parking {
		if pending.sess == sess {
			return true
		}
	}
	return false
}

// markParking records or refreshes the in-flight marker for ac's token.
func (c *resumeCredentials) markParking(sess *session, ac *attachedClient) {
	token := ac.resumeToken
	if token == 0 {
		return
	}
	if existing := c.parking[token]; existing != nil {
		if existing.ac == ac {
			existing.sess = sess
			return
		}
		delete(c.parking, token)
		existing.closeDone()
	}
	c.parking[token] = &parkingAttachment{sess: sess, ac: ac, done: make(chan struct{})}
}

// clearParking removes token's marker, waking its waiters. A non-nil ac only
// clears its own marker.
func (c *resumeCredentials) clearParking(token uint64, ac *attachedClient) {
	pending := c.parking[token]
	if pending == nil || (ac != nil && pending.ac != ac) {
		return
	}
	delete(c.parking, token)
	pending.closeDone()
}

// purgeParking removes every marker for sess, or every marker when sess is
// nil, so same-token waiters fail closed instead of hanging.
func (c *resumeCredentials) purgeParking(sess *session) {
	for token, pending := range c.parking {
		if sess == nil || pending.sess == sess {
			delete(c.parking, token)
			pending.closeDone()
		}
	}
}

// publishParked stores parked under token, replacing any previous entry, and
// clears the matching in-flight marker.
func (c *resumeCredentials) publishParked(token uint64, parked *parkedAttachment) {
	c.parked[token] = parked
	c.clearParking(token, parked.ac)
}

// takeParked removes token's parked entry when it still holds parked. It
// reports false when another path already replaced or removed it.
func (c *resumeCredentials) takeParked(token uint64, parked *parkedAttachment) bool {
	if parked == nil || c.parked[token] != parked {
		return false
	}
	delete(c.parked, token)
	return true
}

// parkedForSession returns every parked entry for sess, or every parked entry
// when sess is nil, keyed by token.
func (c *resumeCredentials) parkedForSession(sess *session) map[uint64]*parkedAttachment {
	matches := make(map[uint64]*parkedAttachment)
	for token, parked := range c.parked {
		if sess == nil || parked.sess == sess {
			matches[token] = parked
		}
	}
	return matches
}
