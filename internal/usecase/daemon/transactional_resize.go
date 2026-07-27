package daemon

import (
	"context"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

// resizeMember is an unpublished layout decision.  Prepare only reads guarded
// state; apply performs the external PTY call; commit is the sole publisher.
type resizeMember struct {
	session    *session
	tab        *tab
	pane       *pane
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
			plan.members = append(plan.members, resizeMember{session: sess, tab: tb, pane: p, rect: placement.Content})
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
			p.resizeRetry = true
			p.mu.Unlock()
			d.replayResizePending(member.session, member.tab, p, false, member.rect)
			member.retry = true
			member.err = err
		} else {
			member.ok = true
			member.screenResized = true // records that this member owns an open gate
		}
		p.resizeMu.Unlock()
	}
}

func validatePreparedTabLayoutLocked(tb *tab, plan *preparedTabLayout) bool {
	if tb.layoutGeneration != plan.generation || tb.size != plan.previousSize {
		return false
	}
	for i := range plan.members {
		member := &plan.members[i]
		if tb.panes[member.pane.id] != member.pane {
			return false
		}
	}
	return true
}

func commitPreparedTabLayoutLocked(plan *preparedTabLayout) {
	for i := range plan.members {
		member := &plan.members[i]
		member.pane.mu.Lock()
		member.pane.rect = member.rect
		if member.ok {
			member.pane.resizeRetry = false
			member.pane.screen.Resize(member.rect.Width, member.rect.Height)
		}
		member.pane.mu.Unlock()
	}
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
func (d *Daemon) applyTabLayoutTransaction(sess *session, tb *tab, current ...func() bool) ([]resizeMember, bool) {
	return d.applyTabLayoutTransactionWithNotice(sess, tb, true, current...)
}

func (d *Daemon) applyTabLayoutTransactionWithNotice(sess *session, tb *tab, reportFailure bool, current ...func() bool) ([]resizeMember, bool) {
	if tb == nil {
		return nil, false
	}
	tb.layoutApplyMu.Lock()
	defer tb.layoutApplyMu.Unlock()
	for {
		if tb.ctx != nil && tb.ctx.Err() != nil {
			return nil, false
		}
		if len(current) != 0 && !current[0]() {
			return nil, false
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
			commitPreparedTabLayoutLocked(&plan)
		}
		tb.mu.Unlock()
		d.finishPreparedTabMembers(&plan, accepted)
		if !accepted {
			d.cancelStalePreparedGates(sess, tb, &plan)
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
				return nil, false
			}
			continue
		}
		failed := make([]resizeMember, 0)
		for _, member := range plan.members {
			if !member.ok {
				failed = append(failed, member)
			}
		}
		if reportFailure && len(failed) != 0 {
			d.notify(sess, domain.NoticeWarn, domain.NoticeResizeFailed,
				"pane resize failed; retrying in background", failed[len(failed)-1].err)
		}
		return failed, true
	}
}

func (d *Daemon) applyTabLayout(sess *session, tb *tab) bool {
	failed, ok := d.applyTabLayoutTransaction(sess, tb)
	if !ok {
		return false
	}
	markSnapshotDirty(sess)
	if len(failed) != 0 {
		d.scheduleAcceptedTabLayoutRetry(sess, tb)
	}
	return true
}

const maxAcceptedTabLayoutRetries = 3

// scheduleAcceptedTabLayoutRetry owns one bounded retry worker per tab. The
// worker is deduplicated, derives cancellation from the tab lifecycle, and
// suppresses repeat degradation notices after the accepted initial failure.
func (d *Daemon) scheduleAcceptedTabLayoutRetry(sess *session, tb *tab) {
	if d.clock == nil || tb == nil || tb.ctx == nil {
		return
	}
	tb.layoutRetryMu.Lock()
	if tb.layoutRetryRunning {
		tb.layoutRetryMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(tb.ctx)
	tb.layoutRetryRunning = true
	tb.layoutRetryCancel = cancel
	tb.layoutRetryMu.Unlock()

	go func() {
		defer func() {
			cancel()
			tb.layoutRetryMu.Lock()
			tb.layoutRetryRunning = false
			tb.layoutRetryCancel = nil
			tb.layoutRetryMu.Unlock()
		}()
		for range maxAcceptedTabLayoutRetries {
			timer := d.clock.NewTimer(minOutputRenderDeadline)
			if timer == nil {
				return
			}
			timerC := timer.C()
			if timerC == nil {
				timer.Stop()
				return
			}
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timerC:
			}
			if ctx.Err() != nil {
				return
			}
			failed, ok := d.applyTabLayoutTransactionWithNotice(sess, tb, false, func() bool { return ctx.Err() == nil })
			if !ok || len(failed) == 0 {
				return
			}
			markSnapshotDirty(sess)
			sess.mu.Lock()
			ac := sess.client
			sess.mu.Unlock()
			if ac != nil {
				d.invalidateRender(sess, ac, true, "transactional_resize.go")
			}
		}
	}()
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
func (d *Daemon) applyVisibleFloatingLayout(sess *session, tb *tab, current func() bool) ([]resizeMember, bool) {
	return d.applyVisibleFloatingLayoutForMember(sess, tb, current, nil)
}

// applyVisibleFloatingLayoutForMember optionally fences a delayed retry to the
// exact failed floating slot. Unlike tiled panes, a popup is not in tb.panes,
// so accepting a replacement here would silently retry unrelated geometry.
func (d *Daemon) applyVisibleFloatingLayoutForMember(sess *session, tb *tab, current func() bool, expected *resizeMember) ([]resizeMember, bool) {
	if tb == nil {
		return nil, true
	}
	tb.mu.Lock()
	if tb.floating.state != floatingVisible || tb.floating.pane == nil ||
		(expected != nil && (tb.floating.generation != expected.floatingGeneration || tb.floating.pane != expected.pane)) {
		tb.mu.Unlock()
		return nil, true
	}
	p := tb.floating.pane
	generation := tb.floating.generation
	size := tb.size
	geometry := calculateContentFloatingGeometry(size, d.currentFloatingConfig())
	tb.mu.Unlock()
	if !geometry.committable() {
		return nil, true
	}

	// This keeps successful PTY resizes gated until the
	// floating slot and tab size are revalidated. A newer client resize may
	// otherwise publish this obsolete popup geometry after its PTY call returns.
	plan := preparedTabLayout{members: []resizeMember{{
		session: sess, tab: tb, pane: p, rect: geometry.ptyRect(), floating: geometry, isFloating: true, floatingGeneration: generation,
	}}}
	d.applyPreparedTabMembers(&plan)

	// The PTY may have accepted an intermediate size, but a hidden, replaced,
	// relaunched, or resized slot must never receive this attempt's geometry.
	tb.mu.Lock()
	currentSlot := tb.floating.state == floatingVisible && tb.floating.generation == generation && tb.floating.pane == p && tb.size == size
	if current != nil && !current() {
		currentSlot = false
	}
	if currentSlot {
		p.mu.Lock()
		p.rect = plan.members[0].rect
		p.popupGeometry = geometry
		if plan.members[0].ok {
			p.resizeRetry = false
			p.screen.Resize(plan.members[0].rect.Width, plan.members[0].rect.Height)
		}
		p.mu.Unlock()
	}
	tb.mu.Unlock()
	if !currentSlot {
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
		if current != nil && !current() {
			return nil, false
		}
		return nil, true
	}
	if plan.members[0].ok {
		d.finishPreparedTabMembers(&plan, true)
		return nil, true
	}
	return plan.members, true
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
func (d *Daemon) applySessionLayout(sess *session, size domain.Size, current, admit func() bool) ([]resizeMember, bool) {
	if sess == nil {
		return nil, false
	}
	sess.layoutApplyMu.Lock()
	defer sess.layoutApplyMu.Unlock()

	target := tabSize(size)
	for {
		if sess.ctx != nil && sess.ctx.Err() != nil {
			return nil, false
		}
		if current != nil && !current() {
			return nil, false
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
				plan.tab.size = plan.size
				commitPreparedTabLayoutLocked(plan)
			}
		}
		for i := len(tabs) - 1; i >= 0; i-- {
			tabs[i].mu.Unlock()
		}
		if !valid {
			d.releasePreparedSessionGates(plans)
			// Headless requests have no attachment epoch. A live tab mutation
			// still invalidates their plan, but must trigger a fresh prepare rather
			// than being mistaken for a canceled request.
			if current != nil && !current() {
				return nil, false
			}
			// A layout mutation invalidated the plans while this epoch remains
			// current. Reapply the fresh session geometry before admitting it.
			continue
		}

		failed := make([]resizeMember, 0)
		for _, plan := range plans {
			d.finishPreparedTabMembers(plan, true)
			for _, member := range plan.members {
				if !member.ok {
					failed = append(failed, member)
				}
			}
		}
		if active >= 0 && active < len(tabs) {
			floatingFailed, ok := d.applyVisibleFloatingLayout(sess, tabs[active], current)
			if !ok {
				return nil, false
			}
			failed = append(failed, floatingFailed...)
		}
		if len(failed) != 0 {
			d.notify(sess, domain.NoticeWarn, domain.NoticeResizeFailed,
				"pane resize failed; retrying in background", failed[len(failed)-1].err)
		}
		markSnapshotDirty(sess)
		d.observeRuntime(ports.RuntimeResizeCommitted, 0, true)
		return failed, true
	}
}

func (d *Daemon) runResizeTransaction(sess *session, ac *attachedClient, lease *attachmentLease, epoch uint64) bool {
	rc := sess.renderCoordinator()
	if rc == nil {
		return false
	}
	snap := rc.resizeSnapshot()
	current := func() bool { return rc.resizeCurrentForLease(epoch, ac, lease, false) }
	if !current() {
		return false
	}
	d.exitCopyMode(ac)
	failed, ok := d.applySessionLayout(sess, snap.size, current, func() bool {
		return rc.resizeCurrentForLease(epoch, ac, lease, true)
	})
	if !ok {
		return false
	}
	ac.sendMu.Lock()
	ac.size = snap.size
	ac.sendMu.Unlock()
	if len(failed) != 0 {
		rc.scheduleResizeRetryForLease(epoch, ac, lease, func() { d.retryResizeMembers(sess, ac, lease, epoch, failed) })
	}
	d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
	// A successful transaction publishes exactly one full S2 frame. The
	// coordinator is the only emission route and stale epochs never reach it.
	if !rc.invalidateForLeaseAtResizeEpoch(ac, lease, epoch, renderInvalidation{class: invalidateUrgent, reset: true, producer: "transactional_resize.go"}) {
		return false
	}
	// The resize debounce has already elapsed. Consume this sticky reset now;
	// fire preserves ACK and synchronized-output gates rather than scheduling a
	// second urgent deadline.
	rc.fireCurrent(false)
	return true
}

// retryResizeMembers retries failed committed members through a freshly
// prepared tab transaction. The captured members identify retry candidates
// only: their rectangles must never cross this delayed boundary, because any
// later layout mutation may have changed their geometry or removed them.
func (d *Daemon) retryResizeMembers(sess *session, ac *attachedClient, lease *attachmentLease, epoch uint64, members []resizeMember) {
	rc := sess.renderCoordinator()
	if rc == nil || !rc.retryCurrentForLease(epoch, ac, lease) {
		return
	}

	// Retain tab order for deterministic external PTY ordering while collapsing
	// several failed members from the same tab into one canonical transaction.
	tabs := make([]*tab, 0, len(members))
	seen := make(map[*tab]struct{}, len(members))
	for _, member := range members {
		if member.tab == nil || member.isFloating {
			continue
		}
		if _, ok := seen[member.tab]; ok {
			continue
		}
		seen[member.tab] = struct{}{}
		tabs = append(tabs, member.tab)
	}

	failed := make([]resizeMember, 0, len(members))
	succeeded := false
	for _, tb := range tabs {
		if !rc.retryCurrentForLease(epoch, ac, lease) {
			return
		}
		// A later accepted layout transaction clears resizeRetry. In that case
		// this delayed callback has no remaining work and, importantly, must not
		// replay the obsolete rectangle it captured before the mutation.
		retryPending := false
		tb.mu.Lock()
		for _, member := range members {
			if member.tab != tb || member.pane == nil || tb.panes[member.pane.id] != member.pane {
				continue
			}
			member.pane.mu.Lock()
			retryPending = retryPending || member.pane.resizeRetry
			member.pane.mu.Unlock()
		}
		tb.mu.Unlock()
		if !retryPending {
			continue
		}

		// applyTabLayoutTransaction captures the current generation, pane
		// pointers, and solved rectangles, and validates them again before any
		// VT/rectangle publication. It also preserves the resize gate and
		// degradation notice behavior for another failed external attempt.
		freshFailed, ok := d.applyTabLayoutTransaction(sess, tb, func() bool {
			return rc.retryCurrentForLease(epoch, ac, lease)
		})
		if !ok || !rc.retryCurrentForLease(epoch, ac, lease) {
			return
		}
		failed = append(failed, freshFailed...)
		// A retry success is a formerly failed, still-current target whose
		// canonical apply cleared its retry bit. A collapsed or removed target
		// is neither a retry completion nor a reason to publish a reset.
		tb.mu.Lock()
		for _, member := range members {
			if member.tab != tb || member.pane == nil || tb.panes[member.pane.id] != member.pane {
				continue
			}
			member.pane.mu.Lock()
			succeeded = succeeded || !member.pane.resizeRetry
			member.pane.mu.Unlock()
		}
		tb.mu.Unlock()
	}
	for _, member := range members {
		if !member.isFloating || member.tab == nil || member.pane == nil {
			continue
		}
		if !rc.retryCurrentForLease(epoch, ac, lease) {
			return
		}
		// Floating panes are outside tb.panes. Validate the exact accepted slot
		// before preparing a fresh geometry; applyVisibleFloatingLayout repeats
		// the same pointer/generation check around external PTY.Resize.
		member.tab.mu.Lock()
		currentSlot := member.tab.floating.state == floatingVisible &&
			member.tab.floating.generation == member.floatingGeneration &&
			member.tab.floating.pane == member.pane
		retryPending := false
		if currentSlot {
			member.pane.mu.Lock()
			retryPending = member.pane.resizeRetry
			member.pane.mu.Unlock()
		}
		member.tab.mu.Unlock()
		if !currentSlot || !retryPending {
			continue
		}
		freshFailed, ok := d.applyVisibleFloatingLayoutForMember(sess, member.tab, func() bool {
			return rc.retryCurrentForLease(epoch, ac, lease)
		}, &member)
		if !ok || !rc.retryCurrentForLease(epoch, ac, lease) {
			return
		}
		failed = append(failed, freshFailed...)
		member.tab.mu.Lock()
		stillCurrent := member.tab.floating.state == floatingVisible &&
			member.tab.floating.generation == member.floatingGeneration &&
			member.tab.floating.pane == member.pane
		member.tab.mu.Unlock()
		if stillCurrent && len(freshFailed) == 0 {
			member.pane.mu.Lock()
			succeeded = succeeded || !member.pane.resizeRetry
			member.pane.mu.Unlock()
		}
	}
	if len(failed) != 0 {
		rc.scheduleResizeRetryForLease(epoch, ac, lease, func() { d.retryResizeMembers(sess, ac, lease, epoch, failed) })
	}
	if succeeded {
		// Retry completion changes VT state after the original layout commit.
		// Keep a named session's eventual snapshot generation aligned with it.
		markSnapshotDirty(sess)
		rc.invalidateForLease(ac, lease, renderInvalidation{class: invalidateUrgent, reset: true, producer: "transactional_resize.go"})
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
		_, ok := d.applySessionLayout(sess, size, nil, nil)
		return ok
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
