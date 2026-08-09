package daemon

import (
	"bytes"
	"cmp"
	"slices"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
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

func (ac *attachedClient) viewSnapshot() attachmentView {
	if ac == nil {
		return attachmentView{}
	}
	ac.viewMu.Lock()
	defer ac.viewMu.Unlock()
	return ac.view
}

func (ac *attachedClient) publishViewLocked(view attachmentView) {
	// Rebase while viewMu is held, before the new revision becomes visible.
	// Output fences take the same lock, so no side effect can pair the new
	// ViewRevision with the previous output epoch. Test/headless fixtures may
	// leave output.attachment unset; use the stream-local lock for those.
	if ac.output != nil && ac.view.revision != view.revision {
		if ac.output.attachment == ac {
			ac.output.rebaseLocked()
		} else {
			ac.output.stateMu.Lock()
			ac.output.rebaseLocked()
			ac.output.stateMu.Unlock()
		}
	}
	ac.view = view
}

func (ac *attachedClient) publishView(view attachmentView) {
	if ac == nil {
		return
	}
	ac.viewMu.Lock()
	defer ac.viewMu.Unlock()
	ac.publishViewLocked(view)
}

func (ac *attachedClient) publishViewIfCurrent(before, view attachmentView) bool {
	if ac == nil {
		return false
	}
	ac.viewMu.Lock()
	defer ac.viewMu.Unlock()
	if ac.view != before {
		return false
	}
	ac.publishViewLocked(view)
	return true
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
	if c.claimGeometryOwnerLocked(ac, ac.sizeSnapshot()) == 0 {
		c.refreshGeometryOwnerLocked()
	}
	return true
}

// latestValidGeometryOwnerLocked selects the most recent valid attachment,
// excluding one claimant when a rejected request is being released. Caller
// holds the owning session's mu.
func (c *sessionCore) latestValidGeometryOwnerLocked(exclude *attachedClient) *attachedClient {
	var owner *attachedClient
	var ownerClaim, ownerOrder uint64
	for candidate := range c.attachments {
		if candidate == nil || candidate == exclude || !candidate.sizeSnapshot().Valid() || !candidate.geometryClaimSizeSnapshot().Valid() {
			continue
		}
		claim := candidate.geometryClaim.Load()
		order := c.attachmentOrder[candidate]
		if owner == nil || claim > ownerClaim || (claim == ownerClaim && order > ownerOrder) {
			owner = candidate
			ownerClaim = claim
			ownerOrder = order
		}
	}
	return owner
}

// refreshGeometryOwnerLocked repairs a removed or invalid owner without
// inventing a newer claim. Caller holds the owning session's mu.
func (c *sessionCore) refreshGeometryOwnerLocked() {
	if c == nil {
		return
	}
	owner := c.geometryOwner.Load()
	if owner != nil {
		if _, ok := c.attachments[owner]; ok && owner.sizeSnapshot().Valid() && owner.geometryClaimSizeSnapshot().Valid() {
			return
		}
	}
	c.geometryMu.Lock()
	c.geometryOwner.Store(c.latestValidGeometryOwnerLocked(nil))
	c.geometryMu.Unlock()
}

// claimGeometryOwnerLocked records the latest attachment claim. Caller holds
// the owning session's mu; the atomic publication lets resize transactions
// validate the claim while tab locks are held.
func (c *sessionCore) claimGeometryOwnerLocked(ac *attachedClient, size domain.Size) uint64 {
	if c == nil || ac == nil || !size.Valid() {
		return 0
	}
	c.geometryMu.Lock()
	defer c.geometryMu.Unlock()
	c.geometryClaimSeq++
	claim := c.geometryClaimSeq
	ac.publishGeometryClaim(size, claim)
	c.geometryOwner.Store(ac)
	return claim
}

func registerAttachmentSessionLocked(entry *session, ac *attachedClient) bool {
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

// claimGeometryOwner records an explicit resize claim after the attachment
// has already published its new local size. It is also used by callers that
// bypass the wire resize path.
func (s *session) claimGeometryOwner(ac *attachedClient) (uint64, bool) {
	if ac == nil {
		return 0, false
	}
	return s.claimGeometryOwnerForSize(ac, ac.sizeSnapshot())
}

// claimGeometryOwnerForSize records a requested shared geometry independently
// of the attachment-local publication. Transactional resize commits use this
// before the PTY phase so a stale request cannot publish after a newer claim.
func (s *session) claimGeometryOwnerForSize(ac *attachedClient, size domain.Size) (uint64, bool) {
	if s == nil || ac == nil || !size.Valid() {
		return 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.attachments[ac]; !ok {
		return 0, false
	}
	return s.claimGeometryOwnerLocked(ac, size), true
}

// releaseGeometryClaim drops a claim that never reached the coordinator's
// resize request. It never creates a new claim; an older valid peer may become
// the source again, or the session retains its headless geometry.
func (s *session) releaseGeometryClaim(ac *attachedClient, claim uint64) {
	if s == nil || ac == nil || claim == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.geometryOwner.Load() != ac || ac.geometryClaim.Load() != claim {
		return
	}
	ac.clearGeometryClaim()
	s.geometryMu.Lock()
	s.geometryOwner.Store(s.latestValidGeometryOwnerLocked(ac))
	s.geometryMu.Unlock()
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
	c.refreshGeometryOwnerLocked()
	return true
}

func unregisterAttachmentSessionLocked(entry *session, ac *attachedClient) bool {
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

func attachmentRegisteredLocked(entry *session, ac *attachedClient) bool {
	if entry == nil || ac == nil || entry.core() == nil {
		return false
	}
	_, ok := entry.core().attachments[ac]
	return ok
}

func attachmentRegistered(entry *session, ac *attachedClient) bool {
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
	before := ac.viewSnapshot()
	s.mu.Lock()
	if _, ok := s.attachments[ac]; !ok {
		s.mu.Unlock()
		return false
	}
	after := s.repairAttachmentViewLocked(ac, before)
	if before.tabID == after.tabID && before.paneID == after.paneID && before.windowTop == after.windowTop && before.windowRows == after.windowRows && before.windowSet == after.windowSet && before.bookmark == after.bookmark && before.liveBottom == after.liveBottom {
		s.mu.Unlock()
		return false
	}
	after.revision++
	s.mu.Unlock()
	return ac.publishViewIfCurrent(before, after)
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
			view.revision++
			ac.publishView(view)
		}
	}
}

// invalidateViewsLocked orders all per-attachment invalidations behind the
// shared session mutation. Caller holds s.mu and dispatchMu.
func (s *session) invalidateViewsLocked() {
	for _, ac := range s.snapshotAttachmentsLocked() {
		view := ac.viewSnapshot()
		repaired := s.repairAttachmentViewLocked(ac, view)
		repaired.revision++
		ac.publishView(repaired)
	}
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
	for {
		view := ac.viewSnapshot()
		s.mu.Lock()
		if _, ok := s.attachments[ac]; !ok {
			s.mu.Unlock()
			return nil
		}
		repaired := s.repairAttachmentViewLocked(ac, view)
		var target *tab
		for _, tb := range s.tabs {
			if tb != nil && domain.TabStableID(tb.stableID) == repaired.tabID {
				target = tb
				break
			}
		}
		s.mu.Unlock()

		if repaired.tabID != view.tabID || repaired.paneID != view.paneID || repaired.windowTop != view.windowTop || repaired.windowRows != view.windowRows || repaired.windowSet != view.windowSet || repaired.bookmark != view.bookmark || repaired.liveBottom != view.liveBottom {
			repaired.revision++
		}
		if !ac.publishViewIfCurrent(view, repaired) {
			continue
		}
		return target
	}
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
func (s *session) repairAttachmentViews() {
	if s == nil {
		return
	}
	_ = s.runMutation(func() error {
		s.mu.Lock()
		s.invalidateViewsLocked()
		s.mu.Unlock()
		return nil
	})
}

// repairAttachmentViewsLocked repairs all live attachment targets after a tab
// or pane mutation. It is intended to run at the same mutation linearization
// point as the shared membership change.
func (s *session) repairAttachmentViewsLocked() {
	if s == nil {
		return
	}
	s.invalidateViewsLocked()
}
