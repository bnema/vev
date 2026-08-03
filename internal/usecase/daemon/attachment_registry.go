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
	windowSet  bool
	bookmark   vt.RowID
	liveBottom bool
	revision   uint64
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
func (c *sessionCore) snapshotAttachmentsLocked() []*attachedClient {
	if c == nil || len(c.attachments) == 0 {
		return nil
	}
	out := make([]*attachedClient, 0, len(c.attachments))
	for ac := range c.attachments {
		out = append(out, ac)
	}
	slices.SortStableFunc(out, func(a, b *attachedClient) int {
		if id := bytes.Compare(a.clientID[:], b.clientID[:]); id != 0 {
			return id
		}
		return cmp.Compare(c.attachmentOrder[a], c.attachmentOrder[b])
	})
	return out
}

func attachmentRegisteredLocked(entry attachmentSession, ac *attachedClient) bool {
	if entry == nil || ac == nil || entry.core() == nil {
		return false
	}
	_, ok := entry.core().attachments[ac]
	return ok
}

func attachmentRegistered(entry attachmentSession, ac *attachedClient) bool {
	if entry == nil || ac == nil || entry.core() == nil {
		return false
	}
	core := entry.core()
	core.mu.Lock()
	defer core.mu.Unlock()
	return attachmentRegisteredLocked(entry, ac)
}

func (s *session) attachmentRegistered(ac *attachedClient) bool {
	return attachmentRegistered(s, ac)
}

func (s *session) attachmentRegisteredLocked(ac *attachedClient) bool {
	return attachmentRegisteredLocked(s, ac)
}

func (s *session) attachmentViewsTabLocked(tb *tab) bool {
	if s == nil || tb == nil {
		return false
	}
	for ac := range s.attachments {
		if ac.viewSnapshot().tabID == domain.TabStableID(tb.stableID) {
			return true
		}
	}
	return false
}

func snapshotAttachmentSession(entry attachmentSession) []*attachedClient {
	if entry == nil || entry.core() == nil {
		return nil
	}
	core := entry.core()
	core.mu.Lock()
	defer core.mu.Unlock()
	return core.snapshotAttachmentsLocked()
}

func (s *session) snapshotAttachments() []*attachedClient {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotAttachmentsLocked()
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
		view.windowSet = false
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
	if view.windowRows <= 0 {
		view.windowRows = targetTab.size.Rows
	}
	targetTab.mu.Unlock()
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
	if before.tabID == after.tabID && before.paneID == after.paneID && before.windowTop == after.windowTop && before.windowRows == after.windowRows && before.windowSet == after.windowSet && before.bookmark == after.bookmark && before.liveBottom == after.liveBottom {
		return false
	}
	after.revision++
	ac.publishView(after)
	return true
}

// prepareAttachmentViewsForRemovedTabLocked selects the nearest surviving tab
// for attachments that were viewing removed. Caller holds s.mu before tabs is
// mutated; the normal invalidation pass publishes the revised views.
func (s *session) prepareAttachmentViewsForRemovedTabLocked(removed *tab, index int) {
	if s == nil || removed == nil || len(s.tabs) <= 1 || index < 0 || index >= len(s.tabs) {
		return
	}
	replacement := index + 1
	if replacement >= len(s.tabs) {
		replacement = index - 1
	}
	if replacement < 0 || replacement >= len(s.tabs) || s.tabs[replacement] == nil {
		return
	}
	tabID := domain.TabStableID(s.tabs[replacement].stableID)
	removedID := domain.TabStableID(removed.stableID)
	for ac := range s.attachments {
		view := ac.viewSnapshot()
		if view.tabID == removedID {
			view.tabID = tabID
			view.paneID = ""
			view.bookmark = 0
			view.liveBottom = true
			ac.publishView(view)
		}
	}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.attachments[ac]; !ok {
		return nil
	}
	view := ac.viewSnapshot()
	repaired := s.repairAttachmentViewLocked(ac, view)
	if repaired.tabID != view.tabID || repaired.paneID != view.paneID || repaired.windowTop != view.windowTop || repaired.windowRows != view.windowRows || repaired.windowSet != view.windowSet || repaired.bookmark != view.bookmark || repaired.liveBottom != view.liveBottom {
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
func (s *session) tabIndexForAttachment(ac *attachedClient) (int, int) {
	if s == nil {
		return -1, 0
	}
	s.mu.Lock()
	tabs := append([]*tab(nil), s.tabs...)
	s.mu.Unlock()
	if len(tabs) == 0 {
		return -1, 0
	}
	if ac == nil {
		return 0, len(tabs)
	}
	view := ac.viewSnapshot()
	for i, tb := range tabs {
		if tb != nil && domain.TabStableID(tb.stableID) == view.tabID {
			return i, len(tabs)
		}
	}
	return 0, len(tabs)
}

// firstTab returns the first tab in the session's ordered tab collection.
// It is a deterministic session-level default for headless work, never an
// interactive attachment selection.
func (s *session) firstTab() *tab {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tabs) == 0 {
		return nil
	}
	return s.tabs[0]
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

// setAttachmentPaneLocked publishes the initiating attachment's pane target
// after a shared focus mutation. Caller holds s.mu; the structural tree focus
// is not used to resolve any other attachment's target.
func (s *session) setAttachmentPaneLocked(ac *attachedClient, tb *tab, p *pane) bool {
	if s == nil || ac == nil || tb == nil || p == nil || !attachmentRegisteredLocked(s, ac) {
		return false
	}
	view := ac.viewSnapshot()
	before := view
	view.tabID = domain.TabStableID(tb.stableID)
	view.paneID = domain.PaneStableID(p.stableID)
	view = s.repairAttachmentViewLocked(ac, view)
	if view.tabID == before.tabID && view.paneID == before.paneID {
		return false
	}
	view.revision++
	ac.publishView(view)
	return true
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
		changed = next.tabID != before.tabID || next.paneID != before.paneID || next.windowTop != before.windowTop || next.windowRows != before.windowRows || next.windowSet != before.windowSet || next.bookmark != before.bookmark || next.liveBottom != before.liveBottom
		return nil
	})
	return changed
}

// selectAttachmentTab changes only one attachment's target. Shared tab order
// and PTY ownership are untouched.
func (s *session) activateAttachmentViewLocked(ac *attachedClient, idx int) bool {
	if s == nil || ac == nil || idx < 0 || idx >= len(s.tabs) || !attachmentRegisteredLocked(s, ac) {
		return false
	}
	view := ac.viewSnapshot()
	view.tabID = domain.TabStableID(s.tabs[idx].stableID)
	view.paneID = ""
	view = s.repairAttachmentViewLocked(ac, view)
	view.revision++
	ac.publishView(view)
	return true
}

func (s *session) selectAttachmentTabLocked(ac *attachedClient, tabID domain.TabStableID) bool {
	if s == nil || ac == nil || !attachmentRegisteredLocked(s, ac) {
		return false
	}
	view := ac.viewSnapshot()
	view.tabID = tabID
	view.paneID = ""
	view = s.repairAttachmentViewLocked(ac, view)
	view.revision++
	ac.publishView(view)
	s.invalidateViewsLocked()
	return view.tabID == tabID
}

func (s *session) selectAttachmentTab(ac *attachedClient, tabID domain.TabStableID) bool {
	if s == nil || ac == nil {
		return false
	}
	var changed bool
	_ = s.runMutation(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		changed = s.selectAttachmentTabLocked(ac, tabID)
		return nil
	})
	return changed
}

func (s *session) switchAttachmentTabForDispatch(ac *attachedClient, idx int) bool {
	if s == nil || ac == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx < 0 || idx >= len(s.tabs) {
		return false
	}
	return s.selectAttachmentTabLocked(ac, domain.TabStableID(s.tabs[idx].stableID))
}

func (s *session) switchAttachmentTab(ac *attachedClient, idx int) bool {
	if s == nil || ac == nil {
		return false
	}
	s.mu.Lock()
	registered := attachmentRegisteredLocked(s, ac)
	if registered && idx >= 0 && idx < len(s.tabs) {
		tabID := domain.TabStableID(s.tabs[idx].stableID)
		s.mu.Unlock()
		return s.selectAttachmentTab(ac, tabID)
	}
	s.mu.Unlock()
	return false
}

func (s *session) switchAttachmentRelativeForDispatch(ac *attachedClient, delta int) bool {
	position, count := s.tabIndexForAttachment(ac)
	if count < 2 {
		return false
	}
	return s.switchAttachmentTabForDispatch(ac, (position+delta+count)%count)
}

// repairAttachmentViews repairs all live attachment targets after a shared
// topology mutation.
func (s *session) repairAttachmentViews() []viewInvalidation {
	if s == nil {
		return nil
	}
	var invalidations []viewInvalidation
	_ = s.runMutation(func() error {
		s.mu.Lock()
		invalidations = s.invalidateViewsLocked()
		s.mu.Unlock()
		return nil
	})
	return invalidations
}

// repairAttachmentViewsLocked repairs all live attachment targets after a tab
// or pane mutation. Caller holds dispatchMu and s.mu; it is intended to run at
// the same mutation linearization point as the shared membership change.
func (s *session) repairAttachmentViewsLocked() []viewInvalidation {
	if s == nil {
		return nil
	}
	return s.invalidateViewsLocked()
}
