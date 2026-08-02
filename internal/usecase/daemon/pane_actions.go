package daemon

import (
	"errors"
	"fmt"
	"slices"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

func (d *Daemon) splitPane(sess *session, ac *attachedClient, dir layout.Direction) error {
	target := resolveDaemonActionTargetForAttachment(sess, ac)
	err := d.splitPaneAt(sess, target.tab, target.pane, dir)
	if err == nil && ac != nil {
		d.invalidateRender(sess, ac, true, "pane_actions.go")
	}
	return err
}

func (d *Daemon) splitPaneAt(sess *session, tb *tab, target *pane, dir layout.Direction) error {
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
) error {
	if d.ptys == nil {
		return nil
	}
	if tb == nil || target == nil {
		return layout.ErrNotFound
	}

	sess.mu.Lock()
	name, cwd := sess.name, sess.cwd
	term := sess.terminal
	env := copyEnvironment(sess.env)
	sess.mu.Unlock()

	tb.mu.Lock()
	if tb.tree == nil || tb.tree.Root == nil {
		tb.mu.Unlock()
		return layout.ErrNotFound
	}
	oldFocus := target.id
	if tb.panes[oldFocus] != target || !layout.ContainsLeaf(tb.tree.Root, oldFocus) {
		tb.mu.Unlock()
		return layout.ErrNotFound
	}
	newID := layout.PaneID(fmt.Sprintf("pane-%d", tb.nextPaneID))
	area := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	candidate := tb.tree.Clone()
	if err := mutate(candidate, oldFocus, newID, area); err != nil {
		tb.mu.Unlock()
		if errors.Is(err, layout.ErrTooSmall) {
			return domain.UserWarn(domain.NoticeLayoutTooSmall, "not enough space to split", err)
		}
		return err
	}
	tb.nextPaneID++
	placements, ok := layout.Solve(candidate.Root, area)
	if !ok {
		tb.mu.Unlock()
		return domain.UserWarn(domain.NoticeLayoutTooSmall, "not enough space to split", layout.ErrTooSmall)
	}
	newRect := placementContent(placements, newID)
	tabStableID := tb.stableID
	generation := tb.layoutGeneration
	tb.mu.Unlock()

	paneStableID, err := newStableID("p")
	if err != nil {
		return fmt.Errorf("daemon: generating pane identity: %w", err)
	}
	command, args := d.ptyCommand(env)
	lifetime := d.newPaneProcessLifetime(tb.ctx)
	pty, err := d.ptys.Open(lifetime.ctx, command, args, childEnvFrom(env, name, tabStableID, paneStableID, term), cwd, rectSize(newRect))
	if err != nil {
		lifetime.abort()
		if pty != nil {
			_ = pty.Close()
		}
		d.log.Warn("pty spawn failed", "err", err, "session", name, "pane", newID, "kind", "pane")
		return domain.UserErr(domain.NoticePaneSpawn, "couldn't open pane: shell failed to start", err)
	}

	p := newPaneWithStableID(newID, paneStableID, pty, rectSize(newRect))
	p.rect = newRect

	tb.mu.Lock()
	if tb.layoutGeneration != generation || tb.panes[oldFocus] != target || tb.tree == nil || !layout.ContainsLeaf(tb.tree.Root, oldFocus) || tb.ctx != nil && tb.ctx.Err() != nil {
		tb.mu.Unlock()
		lifetime.abort()
		_ = pty.Close()
		return layout.ErrNotFound
	}
	if !lifetime.publish(p) {
		tb.mu.Unlock()
		_ = pty.Close()
		return layout.ErrNotFound
	}
	// The tab lock excludes membership observers while pane.mu publishes the
	// initial owner generation.
	publishPaneOwner(p, sess, tb, 0)
	tb.tree = candidate
	tb.panes[newID] = p
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()

	d.applyTabLayout(sess, tb)
	d.startPaneGoroutines(sess, tb, p)
	return nil
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
	err := d.stackPaneAt(sess, target.tab, target.pane)
	if err == nil && ac != nil {
		d.invalidateRender(sess, ac, true, "pane_actions.go")
	}
	return err
}

func (d *Daemon) stackPaneAt(sess *session, tb *tab, target *pane) error {
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
		d.applyTabLayout(sess, tb)
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
	tb := sess.activeTab()
	if tb == nil {
		return layout.ErrNotFound
	}
	tb.mu.Lock()
	if tb.tree == nil {
		tb.mu.Unlock()
		return layout.ErrNotFound
	}
	id := tb.tree.Focus
	tb.mu.Unlock()
	return d.closePane(sess, tb, id, ac, true)
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
	ac := sess.client
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
	sess.mu.Unlock()

	if finalPane {
		_ = d.closeTab(sess, tb, true)
		return true
	}
	d.applyTabLayout(sess, tb)
	if ac != nil {
		ac.overlays.clearCopyModeForPane(p)
		ac.pruneCaptureFrames(p)
	}
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteSyncPaneRemoved(p)
	}
	closePaneProcess(p)
	d.log.Info("pane closed", "session", sessionName, "pane", p.id)
	if ac != nil {
		d.invalidateRender(sess, ac, true, "pane_actions.go")
	}
	return true
}

func (d *Daemon) closePane(sess *session, tb *tab, id layout.PaneID, ac *attachedClient, repaint bool) error {
	if tb == nil {
		return layout.ErrNotFound
	}
	if ac == nil {
		sess.mu.Lock()
		ac = sess.client
		sess.mu.Unlock()
	}
	tb.mu.Lock()
	p := tb.panes[id]
	if p == nil || tb.tree == nil || !layout.ContainsLeaf(tb.tree.Root, id) {
		tb.mu.Unlock()
		return nil
	}
	if len(layout.LeafIDs(tb.tree.Root)) <= 1 {
		tb.mu.Unlock()
		if ac != nil {
			ac.overlays.clearCopyModeForPane(p)
		}
		return d.closeTab(sess, tb, repaint)
	}
	if err := tb.tree.Close(id); err != nil {
		tb.mu.Unlock()
		return err
	}
	delete(tb.panes, id)
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()
	d.applyTabLayout(sess, tb)

	if ac != nil {
		ac.overlays.clearCopyModeForPane(p)
		ac.pruneCaptureFrames(p)
	}

	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteSyncPaneRemoved(p)
	}
	closePaneProcess(p)
	d.log.Info("pane closed", "session", sess.name, "pane", id)
	if repaint {
		if ac != nil {
			d.invalidateRender(sess, ac, true, "pane_actions.go")
		}
	}
	return nil
}

func (d *Daemon) focusDir(sess *session, ac *attachedClient, dir layout.Direction, effect *roleEffectTicket) error {
	target := resolveDaemonActionTargetForAttachment(sess, ac)
	oldFocus := layout.PaneID("")
	if target.pane != nil {
		oldFocus = target.pane.id
	}
	span, err := d.focusDirAt(sess, target.tab, target.pane, dir)
	if err == nil {
		if ac != nil {
			d.finishPaneFocusForClient(sess, ac, target.tab, oldFocus, "pane_actions.go")
		}
		return nil
	}
	if !errors.Is(err, errNoNeighbor) || target.tab == nil {
		return err
	}
	if !overflowSourceEligible(sess, target.tab) {
		return errNoNeighbor
	}

	cfg := d.currentNavConfig()
	if sessionTarget, ok := d.prepareSessionOverflow(sess, dir, cfg); ok {
		if ac == nil {
			return errNoNeighbor
		}
		if effect != nil {
			return d.switchToTargetForRole(effect.roleToken(), sessionTarget, sessionHandoffGuard{expectedSource: target.tab}, "overflow-session")
		}
		return d.commitSessionOverflow(sess, ac, target.tab, sessionTarget)
	}

	sess.mu.Lock()
	position, count := sess.active, len(sess.tabs)
	sess.mu.Unlock()
	step := resolveOverflow(dir, cfg, position, count)
	if step.kind != overflowTabs {
		return err
	}
	candidate, ok := d.prepareTabOverflow(sess, target.tab, dir, span, step.delta)
	if !ok || !d.commitTabOverflow(sess, candidate) {
		return errNoNeighbor
	}
	d.activateTab(sess, candidate.target)
	if ac != nil {
		d.finishPaneFocusForClient(sess, ac, candidate.target, candidate.targetOldFocus, "pane_actions.go")
	}
	return nil
}

// finishPaneFocusForClient applies the attachment lifecycle shared by every
// successful directional focus move. Keep copy-mode exit before the optional
// stack title refresh, and both before render invalidation.
func (d *Daemon) finishPaneFocusForClient(sess *session, ac *attachedClient, tb *tab, oldFocus layout.PaneID, producer string) {
	tb.mu.Lock()
	newFocus := tb.tree.Focus
	pl, hasPlacement := focusedPlacementLocked(tb)
	tb.mu.Unlock()
	if newFocus != oldFocus {
		d.exitCopyMode(ac)
		if hasPlacement && pl.InStack {
			d.refreshPaneTitleOnFocus(sess, newFocus)
		}
	}
	d.invalidateRender(sess, ac, true, producer)
}

func (d *Daemon) focusDirAt(sess *session, tb *tab, target *pane, dir layout.Direction) (domain.Rect, error) {
	if tb == nil || target == nil {
		return domain.Rect{}, layout.ErrNotFound
	}
	tb.mu.Lock()
	if tb.tree == nil || tb.panes[target.id] != target || !layout.ContainsLeaf(tb.tree.Root, target.id) {
		tb.mu.Unlock()
		return domain.Rect{}, layout.ErrNotFound
	}
	candidate := tb.tree.Clone()
	candidate.Focus = target.id
	area := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	span, _ := candidate.FocusSpan(area)
	err := candidate.FocusDir(dir, area)
	committed := err == nil && candidate.Focus != tb.tree.Focus
	if committed {
		tb.tree = candidate
		tb.bumpLayoutGenerationLocked()
	}
	tb.mu.Unlock()
	if committed {
		d.applyTabLayout(sess, tb)
	}
	if errors.Is(err, layout.ErrNoPane) {
		return span, errNoNeighbor
	}
	return span, err
}
