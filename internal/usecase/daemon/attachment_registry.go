package daemon

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/vt"
)

// attachmentView is the mutable viewport owned by one attachment. Tab and
// pane identity is stable across tab insertion, deletion, and reordering;
// neither field is a session-local index or layout PaneID.
type attachmentView struct {
	tabID      domain.TabStableID
	paneID     domain.PaneStableID
	windowTop  int
	windowRows int
	bookmark   vt.RowID
	liveBottom bool
	revision   uint64
}

// attachmentViewSnapshot is the immutable value returned to callers that need
// to inspect attachment-local state without retaining its mutex.
type attachmentViewSnapshot struct {
	attachment *attachedClient
	view       attachmentView
}

// viewInvalidation records the order in which attachment views observed one
// shared session mutation. The order is stable client-ID order, not map
// iteration or lock-acquisition order, which keeps concurrent attaches
// deterministic.
type viewInvalidation struct {
	attachment *attachedClient
	revision   uint64
}

func (ac *attachedClient) viewSnapshot() attachmentView {
	if ac == nil {
		return attachmentView{}
	}
	ac.viewMu.Lock()
	defer ac.viewMu.Unlock()
	return ac.view
}

func (ac *attachedClient) publishView(view attachmentView) {
	if ac == nil {
		return
	}
	ac.viewMu.Lock()
	ac.view = view
	ac.viewMu.Unlock()
}

// registerAttachmentLocked admits ac exactly once. Caller holds s.mu and must
// have acquired dispatchMu first when the operation came from a client.
func (s *session) registerAttachmentLocked(ac *attachedClient) bool {
	if s == nil || !s.sessionCore.registerAttachmentLocked(ac) {
		return false
	}
	view := ac.viewSnapshot()
	if view.tabID == "" || view.paneID == "" {
		view = s.repairAttachmentViewLocked(ac, view)
		ac.publishView(view)
	}
	return true
}

func (c *sessionCore) registerAttachmentLocked(ac *attachedClient) bool {
	if c == nil || ac == nil {
		return false
	}
	if c.attachments == nil {
		c.attachments = make(map[*attachedClient]struct{})
	}
	if _, exists := c.attachments[ac]; exists {
		return false
	}
	if c.attachmentOrder == nil {
		c.attachmentOrder = make(map[*attachedClient]uint64)
	}
	c.nextAttachmentID++
	c.attachments[ac] = struct{}{}
	c.attachmentOrder[ac] = c.nextAttachmentID
	return true
}

func registerAttachmentSessionLocked(entry attachmentSession, ac *attachedClient) bool {
	if entry == nil || entry.core() == nil {
		return false
	}
	// Transition publication may already hold tab locks (move commit). Only
	// publish collection membership here; target repair runs at the next view
	// snapshot outside that critical section.
	return entry.core().registerAttachmentLocked(ac)
}

func (s *session) registerAttachment(ac *attachedClient) bool {
	if s == nil || ac == nil {
		return false
	}
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registerAttachmentLocked(ac)
}

// unregisterAttachmentLocked removes only ac and leaves every other
// attachment's view intact. Caller holds s.mu.
func (s *session) unregisterAttachmentLocked(ac *attachedClient) bool {
	if s == nil {
		return false
	}
	return s.sessionCore.unregisterAttachmentLocked(ac)
}

func (c *sessionCore) unregisterAttachmentLocked(ac *attachedClient) bool {
	if c == nil || ac == nil || c.attachments == nil {
		return false
	}
	if _, exists := c.attachments[ac]; !exists {
		return false
	}
	delete(c.attachments, ac)
	delete(c.attachmentOrder, ac)
	delete(c.snatched, ac)
	if c.client == ac {
		c.client = nil
	}
	return true
}

func unregisterAttachmentSessionLocked(entry attachmentSession, ac *attachedClient) bool {
	if entry == nil || entry.core() == nil {
		return false
	}
	return entry.core().unregisterAttachmentLocked(ac)
}

func (s *session) unregisterAttachment(ac *attachedClient) bool {
	if s == nil || ac == nil {
		return false
	}
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unregisterAttachmentLocked(ac)
}

// snapshotAttachmentsLocked returns a stable registration-ordered snapshot.
// Callers hold s.mu; returned attachments may be used after unlocking.
func (s *session) snapshotAttachmentsLocked() []*attachedClient {
	if s == nil || len(s.attachments) == 0 {
		return nil
	}
	out := make([]*attachedClient, 0, len(s.attachments))
	for ac := range s.attachments {
		out = append(out, ac)
	}
	slices.SortStableFunc(out, func(a, b *attachedClient) int {
		// ClientID is the stable wire identity, so concurrent registration order
		// cannot affect snapshots or invalidation order.
		if id := bytes.Compare(a.clientID[:], b.clientID[:]); id != 0 {
			return id
		}
		return cmp.Compare(s.attachmentOrder[a], s.attachmentOrder[b])
	})
	return out
}

func (s *session) snapshotAttachments() []*attachedClient {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotAttachmentsLocked()
}

func (s *session) snapshotAttachmentViews() []attachmentViewSnapshot {
	attachments := s.snapshotAttachments()
	if len(attachments) == 0 {
		return nil
	}
	views := make([]attachmentViewSnapshot, 0, len(attachments))
	for _, ac := range attachments {
		views = append(views, attachmentViewSnapshot{attachment: ac, view: ac.viewSnapshot()})
	}
	return views
}

// repairAttachmentViewLocked validates stable targets and chooses the nearest
// surviving target when a tab or pane was removed. Caller holds s.mu.
func (s *session) repairAttachmentViewLocked(ac *attachedClient, view attachmentView) attachmentView {
	_ = ac
	if s == nil {
		return attachmentView{}
	}
	var targetTab *tab
	for _, tb := range s.tabs {
		if tb != nil && domain.TabStableID(tb.stableID) == view.tabID {
			targetTab = tb
			break
		}
	}
	tabRepaired := targetTab == nil
	if targetTab == nil && len(s.tabs) != 0 {
		targetTab = s.tabs[0]
	}
	if targetTab == nil {
		view.tabID = ""
		view.paneID = ""
		view.windowTop = 0
		view.windowRows = 0
		view.bookmark = 0
		view.liveBottom = true
		return view
	}
	view.tabID = domain.TabStableID(targetTab.stableID)

	targetPane := (*pane)(nil)
	targetTab.mu.Lock()
	for _, candidate := range targetTab.panes {
		if candidate != nil && domain.PaneStableID(candidate.stableID) == view.paneID {
			targetPane = candidate
			break
		}
	}
	paneRepaired := targetPane == nil
	if targetPane == nil {
		targetPane = targetTab.focusedPane()
	}
	if targetPane != nil {
		view.paneID = domain.PaneStableID(targetPane.stableID)
	} else {
		view.paneID = ""
	}
	targetTab.mu.Unlock()
	if view.windowRows <= 0 {
		targetTab.mu.Lock()
		view.windowRows = targetTab.size.Rows
		targetTab.mu.Unlock()
	}
	if view.windowTop < 0 {
		view.windowTop = 0
	}
	if tabRepaired || paneRepaired || targetPane == nil {
		view.bookmark = 0
		view.liveBottom = true
	}
	return view
}

// repairAttachmentView repairs ac and increments its revision only when the
// stable target changed. It is deliberately separate from registration so
// layout mutation code can validate every surviving attachment in one ordered
// pass.
func (s *session) repairAttachmentView(ac *attachedClient) bool {
	if s == nil || ac == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.attachments[ac]; !ok {
		return false
	}
	before := ac.viewSnapshot()
	after := s.repairAttachmentViewLocked(ac, before)
	if before.tabID == after.tabID && before.paneID == after.paneID && before.windowTop == after.windowTop && before.windowRows == after.windowRows && before.bookmark == after.bookmark && before.liveBottom == after.liveBottom {
		return false
	}
	after.revision++
	ac.publishView(after)
	return true
}

// invalidateViewsLocked orders all per-attachment invalidations behind the
// shared session mutation. Caller holds s.mu and dispatchMu.
func (s *session) invalidateViewsLocked() []viewInvalidation {
	attachments := s.snapshotAttachmentsLocked()
	invalidations := make([]viewInvalidation, 0, len(attachments))
	for _, ac := range attachments {
		view := ac.viewSnapshot()
		repaired := s.repairAttachmentViewLocked(ac, view)
		repaired.revision++
		ac.publishView(repaired)
		invalidations = append(invalidations, viewInvalidation{attachment: ac, revision: repaired.revision})
	}
	return invalidations
}

// runMutation is the one admission boundary for shared session mutations.
// Callers perform their state mutation and view invalidation inside fn; the
// dispatch lock is acquired before any session, tab, or daemon lock.
func (s *session) runMutation(fn func() error) error {
	if s == nil || fn == nil {
		return nil
	}
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	return fn()
}

// tabForAttachment resolves the attachment's stable target after validating
// it against current membership. It never changes another attachment's view.
func (s *session) tabForAttachment(ac *attachedClient) *tab {
	if s == nil || ac == nil {
		return nil
	}
	s.repairAttachmentView(ac)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.attachments[ac]; !ok {
		return nil
	}
	view := ac.viewSnapshot()
	repaired := s.repairAttachmentViewLocked(ac, view)
	if repaired.tabID != view.tabID || repaired.paneID != view.paneID || repaired.windowTop != view.windowTop || repaired.windowRows != view.windowRows || repaired.bookmark != view.bookmark || repaired.liveBottom != view.liveBottom {
		repaired.revision++
		ac.publishView(repaired)
	}
	for _, tb := range s.tabs {
		if tb != nil && domain.TabStableID(tb.stableID) == repaired.tabID {
			return tb
		}
	}
	return nil
}

// paneForAttachment resolves both stable IDs and repairs a removed pane before
// returning it. The tab remains shared; only the attachment's view moves.
func (s *session) tabForAttachmentOrActive(ac *attachedClient) *tab {
	if tb := s.tabForAttachment(ac); tb != nil {
		return tb
	}
	return s.activeTab()
}

func (s *session) paneForAttachment(ac *attachedClient) (*tab, *pane) {
	tb := s.tabForAttachment(ac)
	if tb == nil {
		return nil, nil
	}
	view := ac.viewSnapshot()
	tb.mu.Lock()
	defer tb.mu.Unlock()
	for _, p := range tb.panes {
		if p != nil && domain.PaneStableID(p.stableID) == view.paneID {
			return tb, p
		}
	}
	return tb, tb.focusedPane()
}

// updateAttachmentView applies one attachment-local view mutation at the
// shared mutation boundary. The revision increments exactly once and target
// IDs are repaired before publication.
func (s *session) updateAttachmentView(ac *attachedClient, update func(*attachmentView)) bool {
	if s == nil || ac == nil || update == nil {
		return false
	}
	changed := false
	_ = s.runMutation(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.attachments[ac]; !ok {
			return nil
		}
		before := ac.viewSnapshot()
		next := before
		update(&next)
		next = s.repairAttachmentViewLocked(ac, next)
		next.revision++
		ac.publishView(next)
		changed = next.tabID != before.tabID || next.paneID != before.paneID || next.windowTop != before.windowTop || next.windowRows != before.windowRows || next.bookmark != before.bookmark || next.liveBottom != before.liveBottom
		return nil
	})
	return changed
}

// selectAttachmentTab changes only one attachment's target. Shared tab order
// and PTY ownership are untouched.
func (s *session) selectAttachmentTab(ac *attachedClient, tabID domain.TabStableID) bool {
	if s == nil || ac == nil {
		return false
	}
	var changed bool
	_ = s.runMutation(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.attachments[ac]; !ok {
			return nil
		}
		view := ac.viewSnapshot()
		view.tabID = tabID
		view.paneID = ""
		view = s.repairAttachmentViewLocked(ac, view)
		view.revision++
		ac.publishView(view)
		changed = view.tabID == tabID
		s.invalidateViewsLocked()
		return nil
	})
	return changed
}

func (s *session) switchAttachmentTab(ac *attachedClient, idx int) bool {
	if s == nil || ac == nil {
		return false
	}
	s.mu.Lock()
	registered := false
	if s.attachments != nil {
		_, registered = s.attachments[ac]
	}
	if registered && idx >= 0 && idx < len(s.tabs) {
		tabID := domain.TabStableID(s.tabs[idx].stableID)
		s.mu.Unlock()
		return s.selectAttachmentTab(ac, tabID)
	}
	s.mu.Unlock()
	return s.switchTab(idx)
}

// repairAttachmentViewsLocked repairs all live attachment targets after a tab
// or pane mutation. It is intended to run at the same mutation linearization
// point as the shared membership change.
func (s *session) repairAttachmentViewsLocked() []viewInvalidation {
	if s == nil {
		return nil
	}
	return s.invalidateViewsLocked()
}
