package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

func (d *Daemon) splitPane(sess *session, ac *attachedClient, dir layout.Direction) error {
	target := resolveDaemonActionTarget(sess)
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
	if err := mutate(tb.tree, oldFocus, newID, area); err != nil {
		tb.mu.Unlock()
		if errors.Is(err, layout.ErrTooSmall) {
			return domain.UserWarn(domain.NoticeLayoutTooSmall, "not enough space to split", err)
		}
		return err
	}
	tb.nextPaneID++
	placements, ok := layout.Solve(tb.tree.Root, area)
	if !ok {
		_ = tb.tree.Close(newID)
		tb.tree.Focus = oldFocus
		tb.mu.Unlock()
		return domain.UserWarn(domain.NoticeLayoutTooSmall, "not enough space to split", layout.ErrTooSmall)
	}
	newRect := placementContent(placements, newID)
	tabStableID := tb.stableID
	tb.mu.Unlock()

	paneStableID, err := newStableID("p")
	if err != nil {
		tb.mu.Lock()
		_ = tb.tree.Close(newID)
		tb.tree.Focus = oldFocus
		tb.mu.Unlock()
		return fmt.Errorf("daemon: generating pane identity: %w", err)
	}
	command, args := d.ptyCommand(env)
	pty, err := d.ptys.Open(sess.ctx, command, args, childEnvFrom(env, name, tabStableID, paneStableID, term), cwd, rectSize(newRect))
	if err != nil {
		d.log.Warn("pty spawn failed", "err", err, "session", name, "pane", newID, "kind", "pane")
		tb.mu.Lock()
		_ = tb.tree.Close(newID)
		tb.tree.Focus = oldFocus
		tb.mu.Unlock()
		return domain.UserErr(domain.NoticePaneSpawn, "couldn't open pane: shell failed to start", err)
	}

	pctx, cancel := context.WithCancel(tb.ctx)
	p := newPaneWithStableID(newID, paneStableID, pty, rectSize(newRect))
	p.ctx, p.cancel = pctx, cancel
	p.rect = newRect

	tb.mu.Lock()
	if _, ok := tb.panes[newID]; ok || !layout.ContainsLeaf(tb.tree.Root, newID) {
		tb.mu.Unlock()
		cancel()
		_ = pty.Close()
		return layout.ErrNotFound
	}
	tb.panes[newID] = p
	tb.tree.Focus = newID
	d.applyLayoutLocked(tb)
	tb.mu.Unlock()

	d.startPaneGoroutines(sess, tb, p)
	markSnapshotDirty(sess)
	return nil
}

func (d *Daemon) applyLayoutLocked(tb *tab) {
	if tb == nil || tb.tree == nil || tb.tree.Root == nil || !tb.size.Valid() {
		return
	}
	placements, ok := layout.Solve(tb.tree.Root, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
	if !ok {
		for _, p := range tb.panes {
			d.applyPaneResize(p, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
		}
		return
	}
	for _, pl := range placements {
		if pl.Collapsed || pl.Content.Width <= 0 || pl.Content.Height <= 0 {
			continue
		}
		if p := tb.panes[pl.ID]; p != nil {
			d.applyPaneResize(p, pl.Content)
		}
	}
}

func (d *Daemon) applyPaneResize(p *pane, r domain.Rect) {
	if p == nil {
		return
	}
	p.mu.Lock()
	old := p.rect
	p.mu.Unlock()
	if old == r {
		return
	}
	sz := rectSize(r)
	if p.pty != nil {
		if err := p.pty.Resize(sz); err != nil {
			d.log.Warn("pty resize failed", "err", err)
		}
	}
	p.mu.Lock()
	p.screen.Resize(sz.Cols, sz.Rows)
	p.rect = r
	p.mu.Unlock()
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
	target := resolveDaemonActionTarget(sess)
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
	target := resolveDaemonActionTarget(sess)
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
	err := tb.tree.ToggleStack(target.id, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
	if err == nil {
		d.applyLayoutLocked(tb)
	}
	tb.mu.Unlock()
	if err == nil {
		markSnapshotDirty(sess)
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

func (d *Daemon) reapPane(sess *session, tb *tab, p *pane) {
	if p == nil {
		return
	}
	_ = d.closePane(sess, tb, p.id, nil, true)
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
	d.applyLayoutLocked(tb)
	tb.mu.Unlock()

	if ac != nil {
		ac.overlays.clearCopyModeForPane(p)
		ac.pruneCaptureFrames(p)
	}

	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteSyncPaneRemoved(p)
	}
	if p.cancel != nil {
		p.cancel()
	}
	if p.pty != nil {
		_ = p.pty.Close()
	}
	d.log.Info("pane closed", "session", sess.name, "pane", id)
	markSnapshotDirty(sess)
	if repaint {
		if ac != nil {
			d.invalidateRender(sess, ac, true, "pane_actions.go")
		}
	}
	return nil
}

func (d *Daemon) focusDir(sess *session, ac *attachedClient, dir layout.Direction) error {
	target := resolveDaemonActionTarget(sess)
	oldFocus := layout.PaneID("")
	if target.pane != nil {
		oldFocus = target.pane.id
	}
	_, err := d.focusDirAt(sess, target.tab, target.pane, dir)
	if err == nil && ac != nil {
		d.finishPaneFocusForClient(sess, ac, target.tab, oldFocus, "pane_actions.go")
	}
	return err
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
	oldFocus := target.id
	tb.tree.Focus = oldFocus
	area := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	span, _ := tb.tree.FocusSpan(area)
	err := tb.tree.FocusDir(dir, area)
	newFocus := tb.tree.Focus
	if err == nil {
		d.applyLayoutLocked(tb)
	}
	tb.mu.Unlock()
	if err == nil && newFocus != oldFocus {
		markSnapshotDirty(sess)
	}
	if errors.Is(err, layout.ErrNoPane) {
		return span, errNoNeighbor
	}
	return span, err
}
