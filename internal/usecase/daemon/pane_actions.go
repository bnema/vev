package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

func (d *Daemon) splitPane(sess *session, ac *attachedClient, dir layout.Direction) error {
	after := dir == layout.Right || dir == layout.Down
	return d.spawnPaneOp(sess, ac, func(tree *layout.Tree, oldFocus, newID layout.PaneID, area domain.Rect) error {
		return tree.Split(oldFocus, dir, after, newID, area)
	})
}

func (d *Daemon) spawnPaneOp(
	sess *session,
	ac *attachedClient,
	mutate func(tree *layout.Tree, oldFocus, newID layout.PaneID, area domain.Rect) error,
) error {
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

	sess.mu.Lock()
	name, cwd := sess.name, sess.cwd
	term := sess.terminal
	sess.mu.Unlock()

	tb.mu.Lock()
	if tb.tree == nil || tb.tree.Root == nil {
		tb.mu.Unlock()
		return layout.ErrNotFound
	}
	oldFocus := tb.tree.Focus
	newID := layout.PaneID(fmt.Sprintf("pane-%d", tb.nextPaneID))
	area := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	if err := mutate(tb.tree, oldFocus, newID, area); err != nil {
		tb.mu.Unlock()
		return err
	}
	tb.nextPaneID++
	placements, ok := layout.Solve(tb.tree.Root, area)
	if !ok {
		_ = tb.tree.Close(newID)
		tb.tree.Focus = oldFocus
		tb.mu.Unlock()
		return layout.ErrTooSmall
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
	pty, err := d.ptys.Open(sess.ctx, d.shell, d.shellArgs, d.childEnv(name, tabStableID, paneStableID, term), cwd, rectSize(newRect))
	if err != nil {
		d.log.Warn("pty spawn failed", "err", err, "session", name, "pane", newID, "kind", "pane")
		tb.mu.Lock()
		_ = tb.tree.Close(newID)
		tb.tree.Focus = oldFocus
		tb.mu.Unlock()
		return err
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
	if ac != nil {
		d.invalidateRender(sess, ac, true, "pane_actions.go")
	}
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
	return d.spawnPaneOp(sess, ac, func(tree *layout.Tree, oldFocus, newID layout.PaneID, area domain.Rect) error {
		return tree.StackNew(oldFocus, newID, area)
	})
}

func (d *Daemon) toggleStack(sess *session, ac *attachedClient) error {
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
	err := tb.tree.ToggleStack(tb.tree.Focus, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
	if err == nil {
		d.applyLayoutLocked(tb)
	}
	tb.mu.Unlock()
	if err == nil {
		markSnapshotDirty(sess)
		if ac != nil {
			d.invalidateRender(sess, ac, true, "pane_actions.go")
		}
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
		d.closeTab(sess, tb, repaint)
		return nil
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
	tb := sess.activeTab()
	if tb == nil {
		return layout.ErrNotFound
	}
	tb.mu.Lock()
	if tb.tree == nil {
		tb.mu.Unlock()
		return layout.ErrNotFound
	}
	oldFocus := tb.tree.Focus
	err := tb.tree.FocusDir(dir, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
	newFocus := tb.tree.Focus
	if err == nil {
		d.applyLayoutLocked(tb)
	}
	tb.mu.Unlock()
	if err == nil {
		if newFocus != oldFocus {
			markSnapshotDirty(sess)
		}
	}
	if err == nil && ac != nil {
		if newFocus != oldFocus {
			d.exitCopyMode(ac)
			tb.mu.Lock()
			pl, ok := focusedPlacementLocked(tb)
			tb.mu.Unlock()
			if ok && pl.TitleBar.Height > 0 {
				d.refreshPaneTitleOnFocus(sess, newFocus)
			}
		}
		d.invalidateRender(sess, ac, true, "pane_actions.go")
	}
	if errors.Is(err, layout.ErrNoPane) {
		return nil
	}
	return err
}
