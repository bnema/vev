package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

type resizeOwnerPostEffect uint8

const (
	resizeOwnerPostSnapshotDirty resizeOwnerPostEffect = iota + 1
	resizeOwnerPostRetrySchedule
	resizeOwnerPostRenderInvalidation
	resizeOwnerPostCommitPublication
)

// resizeMember is an unpublished layout decision.  Prepare only reads guarded
// state; apply performs the external PTY call; commit is the sole publisher.
type resizeMember struct {
	session    *session
	tab        *tab
	pane       *pane
	owner      paneEffectLease
	rect       domain.Rect
	floating   floatingGeometry
	isFloating bool
	// floatingGeneration identifies the exact popup slot accepted by a failed
	// resize. Floating panes are intentionally outside tb.panes, so retries
	// must validate this generation and pointer rather than use tiled lookup.
	floatingGeneration uint64
	retry              bool
	ok                 bool
	err                error
	screenResized      bool
}

type preparedTabLayout struct {
	tab          *tab
	generation   uint64
	size         domain.Size
	previousSize domain.Size
	members      []resizeMember
}

type resizeApplyResult struct {
	members []resizeMember
	failed  []resizeMember
}

func prepareTabLayoutLocked(sess *session, tb *tab) preparedTabLayout {
	return prepareTabLayoutForSizeLocked(sess, tb, tb.size)
}

func prepareTabLayoutForSizeLocked(sess *session, tb *tab, size domain.Size) preparedTabLayout {
	plan := preparedTabLayout{tab: tb, generation: tb.layoutGeneration, size: size, previousSize: tb.size}
	if tb.tree == nil || tb.tree.Root == nil || !size.Valid() {
		return plan
	}
	area := domain.Rect{Width: size.Cols, Height: size.Rows}
	placements, ok := layout.Solve(tb.tree.Root, area)
	if !ok && tb.tree.Root.Kind == layout.Leaf {
		placements = []layout.Placement{{ID: tb.tree.Root.Leaf, Content: area}}
		ok = true
	}
	if !ok {
		return plan
	}
	for _, placement := range placements {
		if placement.Collapsed || placement.Content.Width <= 0 || placement.Content.Height <= 0 {
			continue
		}
		if p := tb.panes[placement.ID]; p != nil {
			p.mu.Lock()
			owner := p.effectLeaseLocked()
			p.mu.Unlock()
			plan.members = append(plan.members, resizeMember{session: sess, tab: tb, pane: p, owner: owner, rect: placement.Content})
		}
	}
	return plan
}

// applyPreparedTabMembers performs only the external PTY phase. A successful
// resize leaves the parser gate closed until the complete plan is validated.
func (d *Daemon) applyPreparedTabMembers(plan *preparedTabLayout) {
	for i := range plan.members {
		member := &plan.members[i]
		p := member.pane
		p.resizeMu.Lock()
		p.mu.Lock()
		if !resizeMemberOwnerCurrentLocked(member) {
			member.retry = p.owner.Load() != nil
			p.resizeRetry = member.retry
			p.mu.Unlock()
			p.resizeMu.Unlock()
			continue
		}
		old := p.rect
		pty := p.pty
		// A rejected earlier attempt deliberately leaves its parser gate open.
		// Reapply even if the committed rectangle already matches: the PTY may
		// still be at that rejected attempt's external size. An accepted failed
		// attempt records resizeRetry for the same reason after its gate closes.
		needsApply := p.resizeApplying || p.resizeRetry || old.Width != member.rect.Width || old.Height != member.rect.Height
		if needsApply && pty != nil {
			p.resizeApplying = true
		}
		p.mu.Unlock()
		if !needsApply || pty == nil {
			member.ok = true
		} else if err := pty.Resize(rectSize(member.rect)); err != nil {
			d.log.Warn("pty resize failed", "err", err)
			p.mu.Lock()
			current := resizeMemberOwnerCurrentLocked(member)
			p.resizeRetry = true
			p.mu.Unlock()
			d.replayResizePending(member.session, member.tab, p, false, member.rect)
			member.retry = true
			if current {
				member.err = err
			}
		} else {
			p.mu.Lock()
			current := resizeMemberOwnerCurrentLocked(member)
			if !current && p.owner.Load() != nil {
				p.resizeRetry = true
				member.retry = true
			}
			p.mu.Unlock()
			member.ok = current
			member.screenResized = true // records that this member owns an open gate
		}
		p.resizeMu.Unlock()
	}
}

func resizeMemberOwnerCurrentLocked(member *resizeMember) bool {
	if member == nil || member.pane == nil {
		return false
	}
	// A zero lease is retained only for focused tests which construct the
	// transactional primitive directly. Every prepared production member owns
	// an immutable generation lease.
	if member.owner.owner == nil {
		return true
	}
	owner := member.pane.owner.Load()
	return owner == member.owner.owner && owner.session == member.session && owner.tab == member.tab
}

func validatePreparedTabLayoutLocked(tb *tab, plan *preparedTabLayout) bool {
	if tb.layoutGeneration != plan.generation || tb.size != plan.previousSize {
		return false
	}
	for i := range plan.members {
		member := &plan.members[i]
		if tb.panes[member.pane.id] != member.pane || !resizeMemberOwnerCurrentLocked(member) {
			return false
		}
	}
	return true
}

func resizeMembersOwnerCurrent(members []resizeMember) bool {
	for i := range members {
		if !resizeMemberOwnerCurrentLocked(&members[i]) {
			return false
		}
	}
	return true
}

func (d *Daemon) publishResizeOwnerInvalidation(members []resizeMember, sess *session, ac *attachedClient, lease *attachmentLease, epoch uint64, inv renderInvalidation) bool {
	var reservation *renderInvalidationReservation
	fallback := false
	if !d.publishResizeOwnerPostEffect(members, resizeOwnerPostRenderInvalidation, func() {
		if rc := sess.renderCoordinator(); rc != nil {
			reservation, _ = rc.reserveInvalidationForLeaseAtResizeEpoch(ac, lease, epoch, inv)
			return
		}
		fallback = ac != nil
	}) {
		return false
	}
	if reservation != nil {
		reservation.finish()
		return true
	}
	if fallback {
		d.paint(sess, ac, inv.reset, nil)
		return true
	}
	return ac == nil
}

func preparedTabOwnerStale(plan *preparedTabLayout) bool {
	return plan != nil && !resizeMembersOwnerCurrent(plan.members)
}

func commitPreparedTabLayoutLocked(plan *preparedTabLayout) bool {
	for i := range plan.members {
		member := &plan.members[i]
		member.pane.mu.Lock()
		if !resizeMemberOwnerCurrentLocked(member) {
			member.pane.resizeRetry = member.pane.owner.Load() != nil
			member.retry = member.pane.resizeRetry
			member.pane.mu.Unlock()
			return false
		}
		member.pane.rect = member.rect
		if member.ok {
			member.pane.resizeRetry = false
			member.pane.screen.Resize(member.rect.Width, member.rect.Height)
		}
		member.pane.mu.Unlock()
	}
	return true
}

// cancelStalePreparedGates releases gates for members which disappeared from
// the next solved layout (for example a stack member collapsed by a focus
// change). Members that remain are deliberately left gated for the retry.
func (d *Daemon) cancelStalePreparedGates(sess *session, tb *tab, plan *preparedTabLayout) {
	tb.mu.Lock()
	latest := prepareTabLayoutLocked(sess, tb)
	tb.mu.Unlock()
	current := make(map[*pane]struct{}, len(latest.members))
	for i := range latest.members {
		current[latest.members[i].pane] = struct{}{}
	}
	for i := range plan.members {
		member := &plan.members[i]
		if !member.screenResized {
			continue
		}
		if _, ok := current[member.pane]; ok {
			continue
		}
		member.pane.resizeMu.Lock()
		d.replayResizePending(member.session, member.tab, member.pane, false, member.rect)
		member.pane.resizeMu.Unlock()
	}
}

func (d *Daemon) finishPreparedTabMembers(plan *preparedTabLayout, accepted bool) {
	if !accepted {
		// cancelStalePreparedGates owns stale-plan cancellation. Keeping this
		// path empty ensures a removed member's pending bytes are replayed once,
		// after the fresh layout determines it cannot survive the retry.
		return
	}
	for i := range plan.members {
		member := &plan.members[i]
		if !member.screenResized {
			continue
		}
		member.pane.resizeMu.Lock()
		d.replayResizePending(member.session, member.tab, member.pane, false, member.rect)
		member.pane.resizeMu.Unlock()
	}
}

// applyTabLayoutTransaction is the canonical tiled-layout publisher. It owns
// one per-tab single-writer loop, but never holds tab or pane state locks while
// invoking PTY.Resize.
func (d *Daemon) applyTabLayoutTransaction(sess *session, tb *tab, current ...func() bool) (resizeApplyResult, bool) {
	return d.applyTabLayoutTransactionWithNotice(sess, tb, true, current...)
}

func (d *Daemon) applyTabLayoutTransactionWithNotice(sess *session, tb *tab, reportFailure bool, current ...func() bool) (resizeApplyResult, bool) {
	if tb == nil {
		return resizeApplyResult{}, false
	}
	tb.layoutApplyMu.Lock()
	defer tb.layoutApplyMu.Unlock()
	for {
		if tb.ctx != nil && tb.ctx.Err() != nil {
			return resizeApplyResult{}, false
		}
		if len(current) != 0 && !current[0]() {
			return resizeApplyResult{}, false
		}
		tb.mu.Lock()
		plan := prepareTabLayoutLocked(sess, tb)
		tb.mu.Unlock()
		d.applyPreparedTabMembers(&plan)

		tb.mu.Lock()
		accepted := validatePreparedTabLayoutLocked(tb, &plan)
		if len(current) != 0 && !current[0]() {
			accepted = false
		}
		if accepted {
			accepted = commitPreparedTabLayoutLocked(&plan)
		}
		tb.mu.Unlock()
		d.finishPreparedTabMembers(&plan, accepted)
		if !accepted {
			d.cancelStalePreparedGates(sess, tb, &plan)
			if preparedTabOwnerStale(&plan) {
				d.releasePreparedSessionGates([]*preparedTabLayout{&plan})
				return resizeApplyResult{}, false
			}
			if len(current) != 0 && !current[0]() {
				for i := range plan.members {
					member := &plan.members[i]
					if !member.screenResized {
						continue
					}
					member.pane.resizeMu.Lock()
					d.replayResizePending(member.session, member.tab, member.pane, false, member.rect)
					member.pane.resizeMu.Unlock()
				}
				return resizeApplyResult{}, false
			}
			continue
		}
		failed := make([]resizeMember, 0)
		for _, member := range plan.members {
			if !member.ok {
				failed = append(failed, member)
			}
		}
		if reportFailure && len(failed) != 0 && resizeMembersOwnerCurrent(failed) {
			d.notify(sess, domain.NoticeWarn, domain.NoticeResizeFailed,
				"pane resize failed; retrying in background", failed[len(failed)-1].err)
		}
		return resizeApplyResult{members: append([]resizeMember(nil), plan.members...), failed: failed}, true
	}
}

func (d *Daemon) applyTabLayout(sess *session, tb *tab) bool {
	result, ok := d.applyTabLayoutTransaction(sess, tb)
	if !ok {
		return false
	}
	if !d.publishResizeOwnerPostEffect(result.members, resizeOwnerPostSnapshotDirty, func() {
		markSnapshotDirty(sess)
	}) {
		return false
	}
	if len(result.failed) != 0 {
		if !d.scheduleAcceptedTabLayoutRetry(sess, tb, result.failed) {
			return false
		}
	}
	return true
}

// replayResizePending completes a pane's apply epoch. It keeps the gate set
// while each buffered batch goes through the normal VT path, so reads arriving
// during replay join the same ordered stream. Failure deliberately omits the
// Screen.Resize, parsing every byte at the old dimensions.
func (d *Daemon) replayResizePending(sess *session, tb *tab, p *pane, resized bool, size domain.Rect) {
	first := true
	for {
		p.mu.Lock()
		if first && resized {
			p.screen.Resize(size.Width, size.Height)
		}
		first = false
		data := append([]byte(nil), p.resizePending...)
		p.resizePending = p.resizePending[:0]
		if len(data) == 0 {
			p.resizeApplying = false
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
		if sess == nil {
			p.mu.Lock()
			p.screen.Write(data)
			p.refreshTerminalTitleLocked()
			p.mu.Unlock()
			continue
		}
		d.processPTYData(sess, tb, p, data, false)
	}
}

// applyVisibleFloatingLayout retains floating panes' existing independent
// lifecycle while routing their external resize through the same lock-free gate
// used for retryable PTY work. Floating state is not part of a tiled layout
// generation, so its slot generation and exact pane identity validate its
// publication instead.
func (d *Daemon) applyVisibleFloatingLayout(sess *session, tb *tab, current func() bool) (resizeApplyResult, bool) {
	return d.applyVisibleFloatingLayoutForMember(sess, tb, current, nil)
}

// applyVisibleFloatingLayoutForMember optionally fences a delayed retry to the
// exact failed floating slot. Unlike tiled panes, a popup is not in tb.panes,
// so accepting a replacement here would silently retry unrelated geometry.
func (d *Daemon) applyVisibleFloatingLayoutForMember(sess *session, tb *tab, current func() bool, expected *resizeMember) (resizeApplyResult, bool) {
	if tb == nil {
		return resizeApplyResult{}, true
	}
	tb.mu.Lock()
	if tb.floating.state != floatingVisible || tb.floating.pane == nil ||
		(expected != nil && (tb.floating.generation != expected.floatingGeneration || tb.floating.pane != expected.pane)) {
		tb.mu.Unlock()
		return resizeApplyResult{}, true
	}
	p := tb.floating.pane
	generation := tb.floating.generation
	size := tb.size
	geometry := calculateContentFloatingGeometry(size, d.currentFloatingConfig())
	tb.mu.Unlock()
	if !geometry.valid() {
		return resizeApplyResult{}, true
	}

	// This keeps successful PTY resizes gated until the
	// floating slot and tab size are revalidated. A newer client resize may
	// otherwise publish this obsolete popup geometry after its PTY call returns.
	p.mu.Lock()
	owner := p.effectLeaseLocked()
	p.mu.Unlock()
	plan := preparedTabLayout{members: []resizeMember{{
		session: sess, tab: tb, pane: p, owner: owner, rect: geometry.Inner, floating: geometry, isFloating: true, floatingGeneration: generation,
	}}}
	d.applyPreparedTabMembers(&plan)

	// The PTY may have accepted an intermediate size, but a hidden, replaced,
	// relaunched, or resized slot must never receive this attempt's geometry.
	tb.mu.Lock()
	currentSlot := tb.floating.state == floatingVisible && tb.floating.generation == generation && tb.floating.pane == p && tb.size == size &&
		resizeMemberOwnerCurrentLocked(&plan.members[0])
	if current != nil && !current() {
		currentSlot = false
	}
	if currentSlot {
		p.mu.Lock()
		currentSlot = resizeMemberOwnerCurrentLocked(&plan.members[0])
		if currentSlot {
			p.rect = geometry.Inner
			p.popupGeometry = geometry
			if plan.members[0].ok {
				p.resizeRetry = false
				p.screen.Resize(geometry.Inner.Width, geometry.Inner.Height)
			}
		}
		p.mu.Unlock()
	}
	tb.mu.Unlock()
	if !currentSlot {
		ownerCurrent := resizeMemberOwnerCurrentLocked(&plan.members[0])
		// This attempt has no tiled transaction loop to retain the gate for, so
		// discard its buffered bytes at the old screen size before returning.
		for i := range plan.members {
			member := &plan.members[i]
			if !member.screenResized {
				continue
			}
			member.pane.resizeMu.Lock()
			d.replayResizePending(member.session, member.tab, member.pane, false, member.rect)
			member.pane.resizeMu.Unlock()
		}
		if !ownerCurrent || (current != nil && !current()) {
			return resizeApplyResult{}, false
		}
		return resizeApplyResult{}, true
	}
	if plan.members[0].ok {
		d.finishPreparedTabMembers(&plan, true)
		return resizeApplyResult{members: append([]resizeMember(nil), plan.members...)}, true
	}
	members := append([]resizeMember(nil), plan.members...)
	return resizeApplyResult{members: members, failed: members}, true
}

// releasePreparedSessionGates abandons a session attempt which cannot be
// retried here (for example because a newer coordinator epoch won). Unlike a
// stale per-tab retry, no succeeding attempt is owned by this call, so every
// successful external resize must reopen its parser gate at the old screen
// size.
func (d *Daemon) releasePreparedSessionGates(plans []*preparedTabLayout) {
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		for i := range plan.members {
			member := &plan.members[i]
			if !member.screenResized {
				continue
			}
			member.pane.resizeMu.Lock()
			d.replayResizePending(member.session, member.tab, member.pane, false, member.rect)
			member.pane.resizeMu.Unlock()
		}
	}
}

// applySessionLayout keeps client resize publication as a two-phase session
// transaction: all PTYs are applied and plans validated first, then the
// coordinator admits the epoch before any tab size, rectangle, VT screen,
// snapshot dirtiness, or resize telemetry becomes visible.
func (d *Daemon) applySessionLayout(sess *session, size domain.Size, current, admit func() bool) (resizeApplyResult, bool) {
	if sess == nil {
		return resizeApplyResult{}, false
	}
	sess.layoutApplyMu.Lock()
	defer sess.layoutApplyMu.Unlock()

	target := tabSize(size)
	for {
		if sess.ctx != nil && sess.ctx.Err() != nil {
			return resizeApplyResult{}, false
		}
		if current != nil && !current() {
			return resizeApplyResult{}, false
		}
		sess.mu.Lock()
		tabs := append([]*tab(nil), sess.tabs...)
		active := sess.active
		sess.mu.Unlock()
		plans := make([]*preparedTabLayout, 0, len(tabs))
		for _, tb := range tabs {
			tb.mu.Lock()
			plan := prepareTabLayoutForSizeLocked(sess, tb, target)
			tb.mu.Unlock()
			plans = append(plans, &plan)
		}
		for _, plan := range plans {
			d.applyPreparedTabMembers(plan)
		}

		// Hold every tab lock across final validation, epoch admission, and all
		// publication. This makes the session commit indivisible to layout
		// mutators while still keeping PTY.Resize outside every state lock.
		for _, tb := range tabs {
			tb.mu.Lock()
		}
		valid := true
		for _, plan := range plans {
			if !validatePreparedTabLayoutLocked(plan.tab, plan) {
				valid = false
				break
			}
		}
		// This is the final external-apply validation boundary. Tests use the
		// seam to install a newer epoch here; that epoch must reject admission
		// before this attempt publishes any session geometry.
		if valid && d.beforeSessionResizePublication != nil {
			d.beforeSessionResizePublication()
		}
		if valid && sess.ctx != nil && sess.ctx.Err() != nil {
			valid = false
		}
		if valid && current != nil && !current() {
			valid = false
		}
		if valid && admit != nil && !admit() {
			valid = false
		}
		if valid {
			for _, plan := range plans {
				if !commitPreparedTabLayoutLocked(plan) {
					valid = false
					break
				}
			}
			if valid {
				for _, plan := range plans {
					plan.tab.size = plan.size
				}
			}
		}
		for i := len(tabs) - 1; i >= 0; i-- {
			tabs[i].mu.Unlock()
		}
		if !valid {
			d.releasePreparedSessionGates(plans)
			for _, plan := range plans {
				if preparedTabOwnerStale(plan) {
					return resizeApplyResult{}, false
				}
			}
			// Headless requests have no attachment epoch. A live tab mutation
			// still invalidates their plan, but must trigger a fresh prepare rather
			// than being mistaken for a canceled request.
			if current != nil && !current() {
				return resizeApplyResult{}, false
			}
			// A layout mutation invalidated the plans while this epoch remains
			// current. Reapply the fresh session geometry before admitting it.
			continue
		}

		members := make([]resizeMember, 0)
		failed := make([]resizeMember, 0)
		for _, plan := range plans {
			d.finishPreparedTabMembers(plan, true)
			members = append(members, plan.members...)
			for _, member := range plan.members {
				if !member.ok {
					failed = append(failed, member)
				}
			}
		}
		if active >= 0 && active < len(tabs) {
			floatingResult, ok := d.applyVisibleFloatingLayout(sess, tabs[active], current)
			if !ok {
				return resizeApplyResult{}, false
			}
			members = append(members, floatingResult.members...)
			failed = append(failed, floatingResult.failed...)
		}
		if len(failed) != 0 && resizeMembersOwnerCurrent(failed) {
			d.notify(sess, domain.NoticeWarn, domain.NoticeResizeFailed,
				"pane resize failed; retrying in background", failed[len(failed)-1].err)
		}
		if len(failed) != 0 && !resizeMembersOwnerCurrent(failed) {
			return resizeApplyResult{}, false
		}
		return resizeApplyResult{members: members, failed: failed}, true
	}
}

// requestTransactionalResize reports reset invalidation completion for immediate
// attached requests, and coordinator schedule acceptance for async requests.
// Headless requests have no reset invalidation and report geometry completion.
func (d *Daemon) requestTransactionalResize(sess *session, ac *attachedClient, size domain.Size, immediate bool) bool {
	if ac == nil {
		return d.requestTransactionalResizeForLease(sess, nil, nil, size, immediate)
	}
	rc := d.attachCoordinator(sess, nil, ac, true)
	return d.requestTransactionalResizeForLease(sess, ac, rc.attachmentLease(ac), size, immediate)
}

// requestTransactionalResizeForLease carries the connection's captured lease
// through the full resize transaction and every delayed retry callback.
func (d *Daemon) requestTransactionalResizeForLease(sess *session, ac *attachedClient, lease *attachmentLease, size domain.Size, immediate bool) bool {
	valid := sess != nil && size.Valid()
	d.observeRuntime(ports.RuntimeResizeRequested, 0, valid)
	if !valid {
		return false
	}
	if ac == nil {
		// Headless geometry has no coordinator/transport to coalesce, but keeps
		// the same prepare/apply/commit ordering.
		result, ok := d.applySessionLayout(sess, size, nil, nil)
		if !ok || !d.publishResizeOwnerPostEffect(result.members, resizeOwnerPostSnapshotDirty, func() {
			markSnapshotDirty(sess)
		}) {
			return false
		}
		return d.publishResizeCommit(result.members, sess, nil, nil, 0, nil, size)
	}
	rc := sess.renderCoordinator()
	if rc == nil || !rc.leaseCurrent(lease, true) {
		return false
	}
	if immediate {
		epoch := rc.recordResizeRequestForLease(size, ac, lease)
		if epoch == 0 {
			return false
		}
		return d.runResizeTransaction(sess, ac, lease, epoch)
	}
	return rc.scheduleResizeForLease(size, ac, lease, func(epoch uint64) { d.runResizeTransaction(sess, ac, lease, epoch) }) != 0
}
