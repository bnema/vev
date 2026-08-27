package daemon

import (
	"reflect"

	"github.com/bnema/vev/internal/domain"
)

// movePaneRequest identifies both lifecycles and both topology objects by
// immutable identity. Session names are advisory; the ID and incarnation are
// the commit-time authority.
type movePaneRequest struct {
	Attachment           *attachedClient
	AttachmentCapability attachmentCapability
	Source               moveSessionLocator
	SourceTabID          domain.TabStableID
	SourcePaneID         domain.PaneStableID
	Destination          moveSessionLocator
	DestinationTabID     domain.TabStableID
}

type moveTabRequest struct {
	Attachment           *attachedClient
	AttachmentCapability attachmentCapability
	Source               moveSessionLocator
	SourceTabID          domain.TabStableID
	Destination          moveSessionLocator
}

func (d *Daemon) movePane(req movePaneRequest) error {
	return d.executeMove(&movePaneTopology{req: req})
}

// moveSessionForLocatorLocked resolves only live sessions. The name is
// intentionally ignored after the stable ID/incarnation pair is checked.
func moveSessionForLocatorLocked(d *Daemon, locator moveSessionLocator) *session {
	if d == nil {
		return nil
	}
	sess := d.sessions[locator.ID]
	if sess == nil || sess.incarnation != locator.Incarnation {
		return nil
	}
	return sess
}

func moveSessionLocatorCurrentLocked(sess *session, locator moveSessionLocator) bool {
	return sess != nil && sess.id == locator.ID && sess.incarnation == locator.Incarnation
}

func findMoveTabLocked(sess *session, stableID domain.TabStableID) *tab {
	if sess == nil {
		return nil
	}
	for _, tb := range sess.tabs {
		if tb != nil && tb.stableID == string(stableID) {
			return tb
		}
	}
	return nil
}

func indexMoveTabLocked(sess *session, target *tab) int {
	for i, tb := range sess.tabs {
		if tb == target {
			return i
		}
	}
	return -1
}

func moveTabMemberLocked(sess *session, target *tab) bool {
	return indexMoveTabLocked(sess, target) >= 0
}

func lockMoveTabs(a, b *tab) func() {
	if a == nil && b == nil {
		return func() {}
	}
	if a == nil {
		b.mu.Lock()
		return b.mu.Unlock
	}
	if b == nil {
		a.mu.Lock()
		return a.mu.Unlock
	}
	if a == b {
		a.mu.Lock()
		return a.mu.Unlock
	}
	first, second := a, b
	if first.stableID > second.stableID ||
		(first.stableID == second.stableID && reflect.ValueOf(first).Pointer() > reflect.ValueOf(second).Pointer()) {
		first, second = second, first
	}
	first.mu.Lock()
	second.mu.Lock()
	return func() {
		second.mu.Unlock()
		first.mu.Unlock()
	}
}

func lockMoveDispatch(a, b *session) func() {
	if a == b {
		a.dispatchMu.Lock()
		return a.dispatchMu.Unlock
	}
	first, second := a, b
	if first.id > second.id {
		first, second = second, first
	}
	first.dispatchMu.Lock()
	second.dispatchMu.Lock()
	return func() {
		second.dispatchMu.Unlock()
		first.dispatchMu.Unlock()
	}
}
