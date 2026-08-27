package daemon

import (
	"errors"
	"fmt"
	"slices"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

type paneFocusChange struct {
	tab           *tab
	focused       *pane
	span          domain.Rect
	layoutChanged bool
}

func (d *Daemon) splitPane(sess *session, ac *attachedClient, dir layout.Direction) error {
	target := resolveDaemonActionTargetForAttachment(sess, ac)
	change, err := d.splitPaneAt(sess, target.tab, target.pane, dir)
	if err == nil && ac != nil {
		publishPaneFocusForAttachment(sess, ac, change)
		d.invalidateRender(sess, ac, true, "pane_actions.go")
	}
	return err
}

func (d *Daemon) splitPaneAt(sess *session, tb *tab, target *pane, dir layout.Direction) (paneFocusChange, error) {
	after := dir == layout.Right || dir == layout.Down
	return d.spawnPaneOpAt(sess, tb, target, func(tree *layout.Tree, oldFocus, newID layout.PaneID, area domain.Rect) error {
		return tree.Split(oldFocus, dir, after, newID, area)
	})
}

func (d *Daemon) spawnPaneOpAt(
	sess *session,
	tb *tab,
	target *pane,
	mutate func(tree *layout.Tree, oldFocus, newID layout.PaneID, area domain.Rect) error,
) (paneFocusChange, error) {
	if d.ptys == nil {
		return paneFocusChange{}, nil
	}
	if tb == nil || target == nil {
		return paneFocusChange{}, layout.ErrNotFound
	}

	sess.mu.Lock()
	name, cwd := sess.name, sess.cwd
	env := copyEnvironment(sess.env)
	sess.mu.Unlock()

	tb.mu.Lock()
	if tb.tree == nil || tb.tree.Root == nil {
		tb.mu.Unlock()
		return paneFocusChange{}, layout.ErrNotFound
	}
	oldFocus := target.id
	if tb.panes[oldFocus] != target || !layout.ContainsLeaf(tb.tree.Root, oldFocus) {
		tb.mu.Unlock()
		return paneFocusChange{}, layout.ErrNotFound
	}
	newID := layout.PaneID(fmt.Sprintf("pane-%d", tb.nextPaneID))
	area := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	candidate := tb.tree.Clone()
	if err := mutate(candidate, oldFocus, newID, area); err != nil {
		tb.mu.Unlock()
		if errors.Is(err, layout.ErrTooSmall) {
			return paneFocusChange{}, domain.UserWarn(domain.NoticeLayoutTooSmall, "not enough space to split", err)
		}
		return paneFocusChange{}, err
	}
	tb.nextPaneID++
	placements, ok := layout.Solve(candidate.Root, area)
	if !ok {
		tb.mu.Unlock()
		return paneFocusChange{}, domain.UserWarn(domain.NoticeLayoutTooSmall, "not enough space to split", layout.ErrTooSmall)
	}
	newRect := placementContent(placements, newID)
	tabStableID := tb.stableID
	generation := tb.layoutGeneration
	tb.mu.Unlock()
	initialGeometry := sess.geometry.paneGeometry(rectSize(newRect))

	paneStableID, err := newStableID("p")
	if err != nil {
		return paneFocusChange{}, fmt.Errorf("daemon: generating pane identity: %w", err)
	}
	launch := d.shellLaunch(env)
	lifetime := d.newPaneProcessLifetime(tb.ctx)
	pty, err := d.ptys.Open(lifetime.ctx, launch.command, launch.args, childEnvFrom(env, name, tabStableID, paneStableID), cwd, initialGeometry)
	if err != nil {
		lifetime.abort()
		if pty != nil {
			_ = pty.Close()
		}
		d.log.Warn("pty spawn failed", "err", err, "session", name, "pane", newID, "kind", "pane")
		return paneFocusChange{}, domain.UserErr(domain.NoticePaneSpawn, "couldn't open pane: shell failed to start", err)
	}
	p := newPaneWithStableIDAndTitle(newID, paneStableID, pty, rectSize(newRect), launch.title)
	p.geometry = initialGeometry
	setScreenGeometry(p.screen, initialGeometry)
	p.rect = newRect

	tb.mu.Lock()
	if tb.layoutGeneration != generation || tb.panes[oldFocus] != target || tb.tree == nil || !layout.ContainsLeaf(tb.tree.Root, oldFocus) || tb.ctx != nil && tb.ctx.Err() != nil {
		tb.mu.Unlock()
		lifetime.abort()
		_ = pty.Close()
		return paneFocusChange{}, layout.ErrNotFound
	}
	if !lifetime.publish(p) {
		tb.mu.Unlock()
		_ = pty.Close()
		return paneFocusChange{}, layout.ErrNotFound
	}
	// The tab lock excludes membership observers while pane.mu publishes the
	// initial owner generation.
	publishPaneOwner(p, sess, tb, 0)
	tb.tree = candidate
	tb.panes[newID] = p
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()

	sess.geometry.applyTabLayout(d, sess, tb)
	d.startPaneGoroutines(sess, tb, p)
	return paneFocusChange{tab: tb, focused: p, layoutChanged: true}, nil
}

func placementContent(placements []layout.Placement, id layout.PaneID) domain.Rect {
	for _, pl := range placements {
		if pl.ID == id {
			return pl.Content
		}
	}
	return domain.Rect{}
}

func rectSize(r domain.Rect) domain.Size { return domain.Size{Cols: r.Width, Rows: r.Height} }

func (d *Daemon) stackPane(sess *session, ac *attachedClient) error {
	target := resolveDaemonActionTargetForAttachment(sess, ac)
	change, err := d.stackPaneAt(sess, target.tab, target.pane)
	if err == nil && ac != nil {
		publishPaneFocusForAttachment(sess, ac, change)
		d.invalidateRender(sess, ac, true, "pane_actions.go")
	}
	return err
}

func (d *Daemon) stackPaneAt(sess *session, tb *tab, target *pane) (paneFocusChange, error) {
	return d.spawnPaneOpAt(sess, tb, target, func(tree *layout.Tree, oldFocus, newID layout.PaneID, area domain.Rect) error {
		return tree.StackNew(oldFocus, newID, area)
	})
}

func (d *Daemon) toggleStack(sess *session, ac *attachedClient) error {
	target := resolveDaemonActionTargetForAttachment(sess, ac)
	err := d.toggleStackAt(sess, target.tab, target.pane)
	if err == nil && ac != nil {
		d.invalidateRender(sess, ac, true, "pane_actions.go")
	}
	return err
}

func (d *Daemon) toggleStackAt(sess *session, tb *tab, target *pane) error {
	if d.ptys == nil {
		return nil
	}
	if tb == nil || target == nil {
		return layout.ErrNotFound
	}
	tb.mu.Lock()
	if tb.tree == nil {
		tb.mu.Unlock()
		return layout.ErrNotFound
	}
	if tb.panes[target.id] != target || !layout.ContainsLeaf(tb.tree.Root, target.id) {
		tb.mu.Unlock()
		return layout.ErrNotFound
	}
	candidate := tb.tree.Clone()
	err := candidate.ToggleStack(target.id, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
	if err == nil {
		tb.tree = candidate
		tb.bumpLayoutGenerationLocked()
	}
	tb.mu.Unlock()
	if err == nil {
		sess.geometry.applyTabLayout(d, sess, tb)
	}
	return err
}

func (d *Daemon) closeFocusedPane(sess *session, ac *attachedClient) error {
	if d.ptys == nil {
		if ac != nil {
			d.invalidateRender(sess, ac, true, "pane_actions.go")
		}
		return nil
	}
	target := resolveDaemonActionTargetForAttachment(sess, ac)
	tb := target.tab
	if tb == nil {
		return layout.ErrNotFound
	}
	if target.pane == nil {
		return layout.ErrNotFound
	}
	return d.closePane(sess, tb, target.pane.id, ac, true)
}

// reapPane retains the explicit-owner entry point for focused close tests. PTY
// readers use reapPaneOwner so an owner publication cannot redirect exit to a
// replacement that reused the same tab-local pane ID.
func (d *Daemon) reapPane(sess *session, tb *tab, p *pane) {
	if p == nil {
		return
	}
	lease := p.effectLease()
	if lease.owner == nil {
		_ = d.closePane(sess, tb, p.id, nil, true)
		return
	}
	if lease.owner.session != sess || lease.owner.tab != tb {
		return
	}
	d.reapPaneOwner(p)
}

// reapPaneOwner resolves the owner afresh after EOF. A stale attempt retries
// the immutable owner pointer; a successful attempt revokes owner publication
// under pane.mu before releasing membership locks, so a competing close or
// move can never reap the pane from a second owner.
func (d *Daemon) reapPaneOwner(p *pane) {
	for p != nil {
		lease := p.effectLease()
		if lease.owner == nil {
			return
		}
		if d.reapTiledPaneLease(lease) {
			return
		}
	}
}

func (d *Daemon) reapTiledPaneLease(lease paneEffectLease) bool {
	p, owner := lease.pane, lease.owner
	if p == nil || owner == nil || owner.session == nil || owner.tab == nil || owner.floatingSlotGeneration != 0 {
		return true
	}
	sess, tb := owner.session, owner.tab
	// EOF teardown is a shared mutation just like an explicit close. Hold the
	// dispatch boundary before any session/tab lock, then use the locked close
	// helpers below without re-entering it.
	sess.dispatchMu.Lock()
	defer sess.dispatchMu.Unlock()
	sess.mu.Lock()
	ownedTab := slices.Contains(sess.tabs, tb)
	if p.owner.Load() != owner {
		sess.mu.Unlock()
		return false
	}
	if !ownedTab {
		sess.mu.Unlock()
		return true
	}
	attachments := sess.snapshotAttachmentsLocked()
	sessionName := sess.name
	tb.mu.Lock()
	if p.owner.Load() != owner {
		tb.mu.Unlock()
		sess.mu.Unlock()
		return false
	}
	if tb.panes[p.id] != p || tb.tree == nil || !layout.ContainsLeaf(tb.tree.Root, p.id) {
		tb.mu.Unlock()
		sess.mu.Unlock()
		return true
	}
	p.mu.Lock()
	if p.owner.Load() != owner {
		p.mu.Unlock()
		tb.mu.Unlock()
		sess.mu.Unlock()
		return false
	}
	finalPane := len(layout.LeafIDs(tb.tree.Root)) <= 1
	if !finalPane {
		if err := tb.tree.Close(p.id); err != nil {
			p.mu.Unlock()
			tb.mu.Unlock()
			sess.mu.Unlock()
			return true
		}
		delete(tb.panes, p.id)
		tb.bumpLayoutGenerationLocked()
	}
	p.clearOwnerLocked()
	p.mu.Unlock()
	tb.mu.Unlock()
	sess.invalidateViewsLocked()
	sess.mu.Unlock()

	if finalPane {
		for _, ac := range attachments {
			if ac.overlays != nil {
				ac.overlays.clearCopyModeForPane(p)
			}
		}
		_ = d.closeTabLocked(sess, tb, true)
		return true
	}
	sess.geometry.applyTabLayout(d, sess, tb)
	for _, ac := range attachments {
		if ac.overlays != nil {
			ac.overlays.clearCopyModeForPane(p)
		}
		ac.pruneCaptureFrames(p)
	}
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteSyncPaneRemoved(p)
	}
	closePaneProcess(p)
	d.log.Info("pane closed", "session", sessionName, "pane", p.id)
	for _, ac := range attachments {
		d.invalidateRender(sess, ac, true, "pane_actions.go")
	}
	return true
}

// closePane is the public close boundary for callers that are not already
// inside session.runMutation. Mutation dispatchers use closePaneLocked.
func (d *Daemon) closePane(sess *session, tb *tab, id layout.PaneID, ac *attachedClient, repaint bool) error {
	if sess == nil {
		return layout.ErrNotFound
	}
	sess.dispatchMu.Lock()
	defer sess.dispatchMu.Unlock()
	return d.closePaneLocked(sess, tb, id, ac, repaint)
}

// closePaneLocked requires sess.dispatchMu before entering any architecture
// lock. It is used by existing runMutation callers to avoid nested dispatch
// locking.
func (d *Daemon) closePaneLocked(sess *session, tb *tab, id layout.PaneID, ac *attachedClient, repaint bool) error {
	return d.closePaneLockedWithEffect(sess, tb, id, ac, repaint, nil)
}

func (d *Daemon) closePaneLockedWithEffect(sess *session, tb *tab, id layout.PaneID, ac *attachedClient, repaint bool, effect *attachmentEffect) error {
	if tb == nil {
		return layout.ErrNotFound
	}
	// A nil attachment denotes a session-wide close with an explicit pane target.
	attachments := sess.snapshotAttachments()
	tb.mu.Lock()
	p := tb.panes[id]
	if p == nil || tb.tree == nil || !layout.ContainsLeaf(tb.tree.Root, id) {
		tb.mu.Unlock()
		return nil
	}
	if len(layout.LeafIDs(tb.tree.Root)) <= 1 {
		tb.mu.Unlock()
		for _, viewer := range attachments {
			if viewer.overlays != nil {
				viewer.overlays.clearCopyModeForPane(p)
			}
			viewer.pruneCaptureFrames(p)
		}
		return d.closeTabLockedWithEffect(sess, tb, repaint, effect)
	}
	if err := tb.tree.Close(id); err != nil {
		tb.mu.Unlock()
		return err
	}
	delete(tb.panes, id)
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()
	sess.mu.Lock()
	sess.invalidateViewsLocked()
	sess.mu.Unlock()
	sess.geometry.applyTabLayout(d, sess, tb)

	for _, viewer := range attachments {
		if viewer.overlays != nil {
			viewer.overlays.clearCopyModeForPane(p)
		}
		viewer.pruneCaptureFrames(p)
	}

	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteSyncPaneRemoved(p)
	}
	closePaneProcess(p)
	d.log.Info("pane closed", "session", sess.name, "pane", id)
	if repaint {
		if ac != nil {
			d.invalidateRender(sess, ac, true, "pane_actions.go")
		} else {
			for _, viewer := range attachments {
				d.invalidateRender(sess, viewer, true, "pane_actions.go")
			}
		}
	}
	return nil
}

func (d *Daemon) focusDir(sess *session, ac *attachedClient, dir layout.Direction, effect *attachmentEffect) error {
	target := resolveDaemonActionTargetForAttachment(sess, ac)
	oldFocus := layout.PaneID("")
	if target.pane != nil {
		oldFocus = target.pane.id
	}
	change, err := d.focusDirAt(sess, target.tab, target.pane, dir)
	if err == nil {
		if ac != nil {
			publishPaneFocusForAttachment(sess, ac, change)
			d.finishPaneFocusForClient(sess, ac, target.tab, oldFocus, "pane_actions.go")
		}
		return nil
	}
	span := change.span
	if !errors.Is(err, errNoNeighbor) || target.tab == nil {
		return err
	}
	if !overflowSourceEligible(sess, ac, target.tab) {
		return errNoNeighbor
	}

	cfg := d.currentNavConfig()
	if sessionTarget, ok := d.prepareSessionOverflow(sess, dir, cfg); ok {
		if ac == nil {
			return errNoNeighbor
		}
		if effect != nil {
			return d.switchToTargetForAttachment(effect, sessionTarget, sessionHandoffGuard{expectedSource: target.tab}, "overflow-session")
		}
		return d.commitSessionOverflow(sess, ac, target.tab, sessionTarget)
	}

	position, count := sess.tabIndexForAttachment(ac)
	step := resolveOverflow(dir, cfg, position, count)
	if step.kind != overflowTabs {
		return err
	}
	candidate, ok := d.prepareTabOverflowForAttachment(sess, ac, target.tab, dir, span, step.delta)
	if !ok || !d.commitTabOverflowForAttachment(sess, ac, candidate) {
		return errNoNeighbor
	}
	if ac != nil {
		sess.selectAttachmentTab(ac, domain.TabStableID(candidate.target.stableID))
		d.finishPaneFocusForClient(sess, ac, candidate.target, candidate.targetOldFocus, "pane_actions.go")
	}
	return nil
}

func publishPaneFocusForAttachment(sess *session, ac *attachedClient, change paneFocusChange) bool {
	if sess == nil || ac == nil || change.tab == nil || change.focused == nil {
		return false
	}
	sess.mu.Lock()
	changed := sess.setAttachmentPaneLocked(ac, change.tab, change.focused)
	sess.mu.Unlock()
	return changed
}

// finishPaneFocusForClient applies the attachment lifecycle shared by every
// successful directional focus move. Keep copy-mode exit before the optional
// stack title refresh, and both before render invalidation.
func (d *Daemon) finishPaneFocusForClient(sess *session, ac *attachedClient, tb *tab, oldFocus layout.PaneID, producer string) {
	var newFocus layout.PaneID
	var newPane *pane
	var pl layout.Placement
	var hasPlacement bool
	if tb != nil {
		tb.mu.Lock()
		if ac != nil {
			view := ac.viewSnapshot()
			if domain.TabStableID(tb.stableID) == view.tabID {
				for _, candidate := range tb.panes {
					if candidate != nil && domain.PaneStableID(candidate.stableID) == view.paneID {
						newPane = candidate
						break
					}
				}
			}
		} else {
			newPane = tb.focusedPane()
			if newPane != nil {
				newFocus = newPane.id
			}
		}
		if newPane != nil {
			newFocus = newPane.id
			pl, hasPlacement = panePlacementLocked(tb, newFocus)
		}
		tb.mu.Unlock()
	}

	if newFocus != oldFocus {
		d.exitCopyMode(ac)
		if hasPlacement && pl.InStack {
			d.refreshPaneTitleOnFocus(sess, newFocus, tb)
		}
	}
	d.invalidateRender(sess, ac, true, producer)
}

func panePlacementLocked(tb *tab, id layout.PaneID) (layout.Placement, bool) {
	placements, ok := solvedPlacementsLocked(tb)
	if !ok {
		return layout.Placement{}, false
	}
	for _, placement := range placements {
		if placement.ID == id {
			return placement, true
		}
	}
	return layout.Placement{}, false
}

func (d *Daemon) focusDirAt(sess *session, tb *tab, target *pane, dir layout.Direction) (paneFocusChange, error) {
	change := paneFocusChange{tab: tb}
	if tb == nil || target == nil {
		return change, layout.ErrNotFound
	}
	tb.mu.Lock()
	if tb.tree == nil || tb.panes[target.id] != target || !layout.ContainsLeaf(tb.tree.Root, target.id) {
		tb.mu.Unlock()
		return change, layout.ErrNotFound
	}
	candidate := tb.tree.Clone()
	candidate.Focus = target.id
	area := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	change.span, _ = candidate.FocusSpan(area)
	err := candidate.FocusDir(dir, area)
	if err == nil {
		change.focused = tb.panes[candidate.Focus]
		change.layoutChanged = change.focused != nil && (candidate.Focus != tb.tree.Focus || layoutFingerprint(candidate.Root) != layoutFingerprint(tb.tree.Root))
		if change.layoutChanged {
			tb.tree = candidate
			tb.bumpLayoutGenerationLocked()
		}
	}
	tb.mu.Unlock()
	if change.layoutChanged {
		sess.geometry.applyTabLayout(d, sess, tb)
	}
	if errors.Is(err, layout.ErrNoPane) {
		return change, errNoNeighbor
	}
	return change, err
}
