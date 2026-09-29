package daemon

import (
	"context"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

type resizeOwnerPostEffect uint8

const (
	resizeOwnerPostSnapshotDirty resizeOwnerPostEffect = iota
	resizeOwnerPostRetrySchedule
	resizeOwnerPostRenderInvalidation
	resizeOwnerPostCommitPublication
)

// resizeMember is an unpublished layout decision. Prepare only reads guarded
// state; apply performs the external PTY call; commit is the sole publisher.
type resizeMember struct {
	session    *session
	tab        *tab
	pane       *pane
	owner      paneEffectLease
	rect       domain.Rect
	geometry   domain.Geometry
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

func resizePTYGeometry(pty ports.PTY, geometry domain.Geometry) error {
	return pty.Resize(geometry.NormalizePixels())
}

func setScreenGeometry(screen *vt.Screen, geometry domain.Geometry) {
	if screen == nil {
		return
	}
	screen.SetGeometry(vt.Geometry{
		Cols: geometry.Cols, Rows: geometry.Rows,
		PixelWidth: geometry.PixelWidth, PixelHeight: geometry.PixelHeight,
	})
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
			p.mu.Lock()
			owner := p.effectLeaseLocked()
			p.mu.Unlock()
			paneSize := rectSize(placement.Content)
			plan.members = append(plan.members, resizeMember{
				session: sess, tab: tb, pane: p, owner: owner, rect: placement.Content,
				geometry: sess.geometry.paneGeometry(paneSize),
			})
		}
	}
	return plan
}

// applyPreparedTabMembers performs only the external PTY phase. A successful
// resize leaves the parser gate closed until the complete plan is validated.
func (g *sharedPTYGeometry) applyPreparedTabMembers(d *Daemon, plan *preparedTabLayout) {
	for i := range plan.members {
		member := &plan.members[i]
		if !member.geometry.Valid() {
			member.geometry = domain.Geometry{Size: rectSize(member.rect)}
		}
		p := member.pane
		p.resizeMu.Lock()
		p.mu.Lock()
		if !resizeMemberOwnerCurrent(member) {
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
		pixelGeometryChanged := (p.geometry.PixelsKnown() || member.geometry.PixelsKnown()) && p.geometry != member.geometry
		needsApply := p.resizeApplying || p.resizeRetry || old.Width != member.rect.Width || old.Height != member.rect.Height || pixelGeometryChanged
		if needsApply && pty != nil {
			p.resizeApplying = true
		}
		p.mu.Unlock()
		if !needsApply || pty == nil {
			member.ok = true
		} else if err := resizePTYGeometry(pty, member.geometry); err != nil {
			d.log.Warn("pty resize failed", "err", err)
			p.mu.Lock()
			current := resizeMemberOwnerCurrent(member)
			p.resizeRetry = true
			p.mu.Unlock()
			d.replayResizePending(member.session, member.tab, p, false, member.rect)
			member.retry = true
			if current {
				member.err = err
			}
		} else {
			p.mu.Lock()
			current := resizeMemberOwnerCurrent(member)
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

func resizeMemberOwnerCurrent(member *resizeMember) bool {
	if member == nil || member.pane == nil {
		return false
	}
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
		if tb.panes[member.pane.id] != member.pane || !resizeMemberOwnerCurrent(member) {
			return false
		}
	}
	return true
}

func resizeMembersOwnerCurrent(members []resizeMember) bool {
	for i := range members {
		if !resizeMemberOwnerCurrent(&members[i]) {
			return false
		}
	}
	return true
}

func commitPreparedTabLayoutLocked(plan *preparedTabLayout) bool {
	for i := range plan.members {
		member := &plan.members[i]
		member.pane.mu.Lock()
		current := resizeMemberOwnerCurrent(member)
		if !current {
			member.pane.resizeRetry = member.pane.owner.Load() != nil
			member.retry = member.pane.resizeRetry
		}
		member.pane.mu.Unlock()
		if !current {
			return false
		}
	}
	for i := range plan.members {
		member := &plan.members[i]
		member.pane.mu.Lock()
		member.pane.rect = member.rect
		if member.ok {
			member.pane.resizeRetry = false
			member.pane.geometry = member.geometry
			setScreenGeometry(member.pane.screen, member.geometry)
		}
		member.pane.mu.Unlock()
	}
	return true
}

// cancelStalePreparedGates releases gates for members which disappeared from
// the next solved layout (for example a stack member collapsed by a focus
// change). Members that remain are deliberately left gated for the retry.
func (g *sharedPTYGeometry) cancelStalePreparedGates(d *Daemon, sess *session, tb *tab, plan *preparedTabLayout) {
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

func (g *sharedPTYGeometry) finishPreparedTabMembers(d *Daemon, plan *preparedTabLayout, accepted bool) {
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
func (g *sharedPTYGeometry) applyTabLayoutTransaction(d *Daemon, sess *session, tb *tab, current ...func() bool) ([]resizeMember, bool) {
	return g.applyTabLayoutTransactionWithNotice(d, sess, tb, true, current...)
}

func (g *sharedPTYGeometry) applyTabLayoutTransactionWithNotice(d *Daemon, sess *session, tb *tab, reportFailure bool, current ...func() bool) ([]resizeMember, bool) {
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
		g.applyPreparedTabMembers(d, &plan)

		tb.mu.Lock()
		accepted := validatePreparedTabLayoutLocked(tb, &plan)
		if len(current) != 0 && !current[0]() {
			accepted = false
		}
		if accepted {
			accepted = commitPreparedTabLayoutLocked(&plan)
		}
		tb.mu.Unlock()
		g.finishPreparedTabMembers(d, &plan, accepted)
		if !accepted {
			g.cancelStalePreparedGates(d, sess, tb, &plan)
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
		if reportFailure && len(failed) != 0 && resizeMembersOwnerCurrent(failed) {
			d.notify(sess, domain.NoticeWarn, domain.NoticeResizeFailed,
				"pane resize failed; retrying in background", failed[len(failed)-1].err)
		}
		return failed, true
	}
}

func (g *sharedPTYGeometry) applyTabLayout(d *Daemon, sess *session, tb *tab) bool {
	failed, ok := g.applyTabLayoutTransaction(d, sess, tb)
	if !ok {
		return false
	}
	markSnapshotDirty(sess)
	if len(failed) != 0 {
		g.scheduleAcceptedTabLayoutRetry(d, sess, tb)
	}
	return true
}

const maxAcceptedTabLayoutRetries = 3

// scheduleAcceptedTabLayoutRetry owns one bounded retry worker per tab. The
// worker is deduplicated, derives cancellation from the tab lifecycle, and
// suppresses repeat degradation notices after the accepted initial failure.
func (g *sharedPTYGeometry) scheduleAcceptedTabLayoutRetry(d *Daemon, sess *session, tb *tab) {
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
			failed, ok := g.applyTabLayoutTransactionWithNotice(d, sess, tb, false, func() bool { return ctx.Err() == nil })
			if !ok || len(failed) == 0 {
				return
			}
			markSnapshotDirty(sess)
			for _, ac := range sess.snapshotAttachments() {
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
			writePaneScreenLocked(p, data)
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
func (g *sharedPTYGeometry) applyVisibleFloatingLayout(d *Daemon, sess *session, tb *tab, current func() bool) ([]resizeMember, bool) {
	return g.applyVisibleFloatingLayoutForMember(d, sess, tb, current, nil)
}

// applyVisibleFloatingLayoutForMember optionally fences a delayed retry to the
// exact failed floating slot. Unlike tiled panes, a popup is not in tb.panes,
// so accepting a replacement here would silently retry unrelated geometry.
func (g *sharedPTYGeometry) applyVisibleFloatingLayoutForMember(d *Daemon, sess *session, tb *tab, current func() bool, expected *resizeMember) ([]resizeMember, bool) {
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
	if geometry.Inner.Width <= 0 || geometry.Inner.Height <= 0 {
		// A drawer can validly reserve only its separator row. Publish that
		// presentation without inventing a physical PTY size; the last usable
		// rectangle and screen remain committed until a later resize has content.
		tb.mu.Lock()
		currentSlot := tb.floating.state == floatingVisible && tb.floating.generation == generation &&
			tb.floating.pane == p && tb.size == size
		if current != nil && !current() {
			currentSlot = false
		}
		if currentSlot {
			p.mu.Lock()
			p.popupGeometry = geometry
			p.mu.Unlock()
			tb.mu.Unlock()
			return nil, true
		}
		tb.mu.Unlock()
		if current != nil && !current() {
			return nil, false
		}
		return nil, true
	}

	// This keeps successful PTY resizes gated until the
	// floating slot and tab size are revalidated. A newer client resize may
	// otherwise publish this obsolete popup geometry after its PTY call returns.
	p.mu.Lock()
	owner := p.effectLeaseLocked()
	p.mu.Unlock()
	ptyRect := geometry.ptyRect()
	plan := preparedTabLayout{members: []resizeMember{{
		session: sess, tab: tb, pane: p, owner: owner, rect: ptyRect,
		geometry: sess.geometry.paneGeometry(rectSize(ptyRect)), floating: geometry,
		isFloating: true, floatingGeneration: generation,
	}}}
	g.applyPreparedTabMembers(d, &plan)

	// The PTY may have accepted an intermediate size, but a hidden, replaced,
	// relaunched, or resized slot must never receive this attempt's geometry.
	tb.mu.Lock()
	currentSlot := tb.floating.state == floatingVisible && tb.floating.generation == generation && tb.floating.pane == p && tb.size == size && resizeMemberOwnerCurrent(&plan.members[0])
	if current != nil && !current() {
		currentSlot = false
	}
	if currentSlot && plan.members[0].ok {
		p.mu.Lock()
		currentSlot = resizeMemberOwnerCurrent(&plan.members[0])
		if currentSlot {
			p.rect = plan.members[0].rect
			p.geometry = plan.members[0].geometry
			p.popupGeometry = geometry
			p.resizeRetry = false
			setScreenGeometry(p.screen, plan.members[0].geometry)
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
		g.finishPreparedTabMembers(d, &plan, true)
		return nil, true
	}
	return plan.members, true
}

// releasePreparedSessionGates abandons a session attempt which cannot be
// retried here (for example because a newer coordinator epoch won). Unlike a
// stale per-tab retry, no succeeding attempt is owned by this call, so every
// successful external resize must reopen its parser gate at the old screen
// size.
func (g *sharedPTYGeometry) releasePreparedSessionGates(d *Daemon, plans []*preparedTabLayout) {
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
func (g *sharedPTYGeometry) applySessionLayout(d *Daemon, sess *session, size domain.Size, current, admit func() bool) ([]resizeMember, bool) {
	if sess == nil {
		return nil, false
	}
	sess.layoutApplyMu.Lock()
	defer sess.layoutApplyMu.Unlock()

	target := contentSize(size)
	for {
		if sess.ctx != nil && sess.ctx.Err() != nil {
			return nil, false
		}
		if current != nil && !current() {
			return nil, false
		}
		sess.mu.Lock()
		tabs := append([]*tab(nil), sess.tabs...)
		sess.mu.Unlock()
		plans := make([]*preparedTabLayout, 0, len(tabs))
		for _, tb := range tabs {
			tb.mu.Lock()
			plan := prepareTabLayoutForSizeLocked(sess, tb, target)
			tb.mu.Unlock()
			plans = append(plans, &plan)
		}
		for _, plan := range plans {
			g.applyPreparedTabMembers(d, plan)
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
		// Claim publication and shared geometry commit use one fence. The hook
		// above remains outside the fence so tests and lifecycle callbacks can
		// publish a newer claim; the final validation then rejects this stale plan.
		g.mu.Lock()
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
				plan.tab.size = plan.size
			}
		}
		g.mu.Unlock()
		for i := len(tabs) - 1; i >= 0; i-- {
			tabs[i].mu.Unlock()
		}
		if !valid {
			g.releasePreparedSessionGates(d, plans)
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
			g.finishPreparedTabMembers(d, plan, true)
			for _, member := range plan.members {
				if !member.ok {
					failed = append(failed, member)
				}
			}
		}
		for _, tb := range tabs {
			floatingFailed, ok := g.applyVisibleFloatingLayout(d, sess, tb, current)
			if !ok {
				return nil, false
			}
			failed = append(failed, floatingFailed...)
		}
		failedCurrent := len(failed) == 0 || resizeMembersOwnerCurrent(failed)
		if current != nil && !current() {
			return nil, false
		}
		if len(failed) != 0 && failedCurrent {
			d.notify(sess, domain.NoticeWarn, domain.NoticeResizeFailed,
				"pane resize failed; retrying in background", failed[len(failed)-1].err)
		}
		if len(failed) != 0 && !failedCurrent {
			return nil, false
		}
		if current != nil {
			d.observeBeforeResizeOwnerPostEffect(resizeOwnerPostSnapshotDirty)
			if !current() {
				return nil, false
			}
		}
		markSnapshotDirty(sess)
		d.observeRuntime(ports.RuntimeResizeCommitted, 0, true)
		return failed, true
	}
}
