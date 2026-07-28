package daemon

import (
	"cmp"
	"slices"
)

// moveResizeFences excludes concurrent layout publishers while a move
// revalidates and publishes ownership. It is not an architecture lock. Fences
// are always acquired session -> tab -> pane by immutable stable identity and
// released in exact reverse order.
type moveResizeFences struct {
	sessions []*session
	tabs     []*tab
	panes    []*pane
	held     bool
}

// newMovePaneResizeFences covers both sessions, both affected layouts, and
// only the pane whose owner will change. Stable-identity deduplication also
// handles moves between tabs in the same session.
func newMovePaneResizeFences(source, destination *session, sourceTab, destinationTab *tab, moved *pane) *moveResizeFences {
	return newMoveResizeFences(
		[]*session{source, destination},
		[]*tab{sourceTab, destinationTab},
		[]*pane{moved},
	)
}

// newMoveTabResizeFences snapshots every process-bearing pane transferred with
// a tab. The acquire callback must revalidate this exact tiled membership and
// installed floating slot after all fences are held, because this snapshot is
// intentionally taken before waiting on any resize transaction.
func newMoveTabResizeFences(source, destination *session, moved *tab) *moveResizeFences {
	if moved == nil {
		return newMoveResizeFences([]*session{source, destination}, nil, nil)
	}
	moved.mu.Lock()
	panes := make([]*pane, 0, len(moved.panes)+1)
	for _, p := range moved.panes {
		panes = append(panes, p)
	}
	if moved.floating.pane != nil && (moved.floating.state == floatingHidden || moved.floating.state == floatingVisible) {
		panes = append(panes, moved.floating.pane)
	}
	moved.mu.Unlock()
	return newMoveResizeFences([]*session{source, destination}, []*tab{moved}, panes)
}

func newMoveResizeFences(sessions []*session, tabs []*tab, panes []*pane) *moveResizeFences {
	byID := make(map[string]*session, len(sessions))
	for _, sess := range sessions {
		if sess == nil {
			continue
		}
		if _, exists := byID[string(sess.id)]; !exists {
			byID[string(sess.id)] = sess
		}
	}
	ordered := make([]*session, 0, len(byID))
	for _, sess := range byID {
		ordered = append(ordered, sess)
	}
	slices.SortFunc(ordered, func(left, right *session) int {
		return cmp.Compare(left.id, right.id)
	})

	tabsByID := make(map[string]*tab, len(tabs))
	for _, tb := range tabs {
		if tb == nil {
			continue
		}
		if _, exists := tabsByID[tb.stableID]; !exists {
			tabsByID[tb.stableID] = tb
		}
	}
	orderedTabs := make([]*tab, 0, len(tabsByID))
	for _, tb := range tabsByID {
		orderedTabs = append(orderedTabs, tb)
	}
	slices.SortFunc(orderedTabs, func(left, right *tab) int {
		return cmp.Compare(left.stableID, right.stableID)
	})

	panesByID := make(map[string]*pane, len(panes))
	for _, p := range panes {
		if p == nil {
			continue
		}
		if _, exists := panesByID[p.stableID]; !exists {
			panesByID[p.stableID] = p
		}
	}
	orderedPanes := make([]*pane, 0, len(panesByID))
	for _, p := range panesByID {
		orderedPanes = append(orderedPanes, p)
	}
	slices.SortFunc(orderedPanes, func(left, right *pane) int {
		return cmp.Compare(left.stableID, right.stableID)
	})
	return &moveResizeFences{sessions: ordered, tabs: orderedTabs, panes: orderedPanes}
}

// acquire waits for every resize fence before invoking revalidateAndPublish.
// Callers must enter with no daemon, session, tab, or pane architecture lock
// held. The callback is the only architecture-lock window: under the canonical
// architecture lock order it must revalidate the sessions, tabs, tiled pane
// memberships, and installed floating slot captured before this wait; publish
// every changed pane owner; and bump each affected tab's layoutGeneration. A
// false result (or panic) releases every acquired fence in reverse order. A true
// result retains all fences so no resize can observe half-published ownership;
// the caller must Release after architecture locks are dropped and before PTY
// I/O, applyTabLayout, or applySessionLayout.
func (f *moveResizeFences) acquire(revalidateAndPublish func() bool) (accepted bool) {
	if f == nil || f.held {
		return false
	}
	for _, sess := range f.sessions {
		sess.layoutApplyMu.Lock()
	}
	for _, tb := range f.tabs {
		tb.layoutApplyMu.Lock()
	}
	for _, p := range f.panes {
		p.resizeMu.Lock()
	}
	f.held = true
	defer func() {
		if !accepted {
			f.Release()
		}
	}()
	if revalidateAndPublish == nil {
		return false
	}
	accepted = revalidateAndPublish()
	return accepted
}

func (d *Daemon) observeBeforeResizeOwnerPostEffect(effect resizeOwnerPostEffect) {
	if d != nil && d.beforeResizeOwnerPostEffect != nil {
		d.beforeResizeOwnerPostEffect(effect)
	}
}

// acquireResizeOwnerPostEffectFences acquires and validates the complete
// canonical session/tab/pane fence set. It performs no callbacks or external
// operations and returns the fences held on success.
func acquireResizeOwnerPostEffectFences(members []resizeMember) *moveResizeFences {
	if len(members) == 0 {
		return nil
	}
	sessions := make([]*session, 0, len(members))
	tabs := make([]*tab, 0, len(members))
	panes := make([]*pane, 0, len(members))
	for i := range members {
		member := &members[i]
		sessions = append(sessions, member.session)
		tabs = append(tabs, member.tab)
		panes = append(panes, member.pane)
	}
	fences := newMoveResizeFences(sessions, tabs, panes)
	if !fences.acquire(func() bool { return resizeMembersOwnerCurrent(members) }) {
		return nil
	}
	return fences
}

// publishResizeOwnerPostEffect linearizes an in-memory resize effect with pane
// ownership publication. The deterministic seam runs before fence acquisition,
// then the complete canonical session/tab/pane fence set is acquired and every
// immutable owner lease is revalidated. The callback must publish in-memory
// state only; timer, PTY, transport, repository, and renderer work belongs
// after this method returns.
func (d *Daemon) publishResizeOwnerPostEffect(members []resizeMember, effect resizeOwnerPostEffect, publish func()) bool {
	if publish == nil {
		return false
	}
	d.observeBeforeResizeOwnerPostEffect(effect)
	fences := acquireResizeOwnerPostEffectFences(members)
	if fences == nil {
		return false
	}
	publish()
	fences.Release()
	return true
}

func (f *moveResizeFences) Release() {
	if f == nil || !f.held {
		return
	}
	for i := len(f.panes) - 1; i >= 0; i-- {
		f.panes[i].resizeMu.Unlock()
	}
	for i := len(f.tabs) - 1; i >= 0; i-- {
		f.tabs[i].layoutApplyMu.Unlock()
	}
	for i := len(f.sessions) - 1; i >= 0; i-- {
		f.sessions[i].layoutApplyMu.Unlock()
	}
	f.held = false
}
