package daemon

// paneOwner is an immutable routing snapshot for one published pane. A zero
// floatingSlotGeneration identifies a tiled pane; installed floating panes
// carry the tab slot generation that owns them.
type paneOwner struct {
	session                *session
	tab                    *tab
	generation             uint64
	floatingSlotGeneration uint64
}

// paneEffectLease binds an effect to the exact immutable owner generation that
// existed while the pane was parsed or otherwise inspected under pane.mu.
type paneEffectLease struct {
	pane  *pane
	owner *paneOwner
}

// Current reports whether the lease still names the published pane owner. For
// floating panes it also verifies that the same tab slot and slot generation
// remain installed. Callers must not hold pane.mu or tab.mu.
func (l paneEffectLease) Current() bool {
	return l.pane != nil && l.pane.ownerLeaseCurrent(l)
}

// ownerSnapshot returns the immutable owner visible to lock-free routing
// readers. Ownership publication itself is serialized under pane.mu.
func (p *pane) ownerSnapshot() *paneOwner {
	if p == nil {
		return nil
	}
	return p.owner.Load()
}

// effectLeaseLocked captures the current owner while pane.mu linearizes VT
// parsing against transfer. The caller must hold pane.mu.
func (p *pane) effectLeaseLocked() paneEffectLease {
	if p == nil {
		return paneEffectLease{}
	}
	return paneEffectLease{pane: p, owner: p.owner.Load()}
}

// publishOwnerLocked publishes a new immutable owner generation. The caller
// must hold pane.mu, and must also hold tab.mu when publishing a floating owner.
func (p *pane) publishOwnerLocked(sess *session, tb *tab, floatingSlotGeneration uint64) (paneEffectLease, paneEffectLease) {
	if p == nil {
		return paneEffectLease{}, paneEffectLease{}
	}
	old := paneEffectLease{pane: p, owner: p.owner.Load()}
	p.ownerGeneration++
	next := &paneOwner{
		session:                sess,
		tab:                    tb,
		generation:             p.ownerGeneration,
		floatingSlotGeneration: floatingSlotGeneration,
	}
	p.owner.Store(next)
	return old, paneEffectLease{pane: p, owner: next}
}

// clearOwnerLocked revokes the published owner during terminal teardown. The
// caller must hold pane.mu. The returned lease identifies the retired owner.
func (p *pane) clearOwnerLocked() paneEffectLease {
	if p == nil {
		return paneEffectLease{}
	}
	old := paneEffectLease{pane: p, owner: p.owner.Load()}
	if old.owner != nil {
		p.ownerGeneration++
		p.owner.Store(nil)
	}
	return old
}

func (p *pane) ownerLeaseCurrent(lease paneEffectLease) bool {
	if p == nil || lease.pane != p || lease.owner == nil || p.owner.Load() != lease.owner {
		return false
	}
	owner := lease.owner
	if owner.session == nil || owner.tab == nil {
		return false
	}
	if owner.floatingSlotGeneration == 0 {
		return true
	}
	owner.tab.mu.Lock()
	current := owner.tab.floating.pane == p &&
		owner.tab.floating.generation == owner.floatingSlotGeneration &&
		(owner.tab.floating.state == floatingHidden || owner.tab.floating.state == floatingVisible)
	owner.tab.mu.Unlock()
	return current && p.owner.Load() == owner
}

func publishPaneOwner(p *pane, sess *session, tb *tab, floatingSlotGeneration uint64) paneEffectLease {
	if p == nil {
		return paneEffectLease{}
	}
	p.mu.Lock()
	_, lease := p.publishOwnerLocked(sess, tb, floatingSlotGeneration)
	p.mu.Unlock()
	return lease
}

// publishTiledPaneOwners initializes every pane in an unpublished tab before
// that tab is made visible or any reader starts.
func publishTiledPaneOwners(sess *session, tb *tab) {
	if sess == nil || tb == nil {
		return
	}
	tb.mu.Lock()
	for _, p := range tb.panes {
		publishPaneOwner(p, sess, tb, 0)
	}
	tb.mu.Unlock()
}
