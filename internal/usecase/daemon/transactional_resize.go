package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

// resizeMember is an unpublished layout decision.  Prepare only reads guarded
// state; apply performs the external PTY call; commit is the sole publisher.
type resizeMember struct {
	tab        *tab
	pane       *pane
	rect       domain.Rect
	floating   floatingGeometry
	isFloating bool
	ok         bool
}

type resizePlan struct {
	size    domain.Size
	tabs    []*tab
	members []resizeMember
}

func (d *Daemon) prepareResize(sess *session, size domain.Size) resizePlan {
	plan := resizePlan{size: size}
	sess.mu.Lock()
	plan.tabs = append(plan.tabs, sess.tabs...)
	active := sess.active
	sess.mu.Unlock()
	for i, tb := range plan.tabs {
		tb.mu.Lock()
		if tb.tree != nil && tb.tree.Root != nil {
			ts := tabSize(size)
			area := domain.Rect{Width: ts.Cols, Height: ts.Rows}
			if placements, ok := layout.Solve(tb.tree.Root, area); ok {
				for _, pl := range placements {
					if !pl.Collapsed && pl.Content.Width > 0 && pl.Content.Height > 0 {
						if p := tb.panes[pl.ID]; p != nil {
							plan.members = append(plan.members, resizeMember{tab: tb, pane: p, rect: pl.Content})
						}
					}
				}
			}
		}
		if i == active && tb.floating.state == floatingVisible && tb.floating.pane != nil {
			g := calculateContentFloatingGeometry(tabSize(size), d.currentFloatingConfig())
			if g.valid() {
				plan.members = append(plan.members, resizeMember{tab: tb, pane: tb.floating.pane, rect: g.Inner, floating: g, isFloating: true})
			}
		}
		tb.mu.Unlock()
	}
	return plan
}

// applyResize deliberately holds only a pane's resizeMu around its PTY call;
// no daemon, session, tab, or pane lock crosses the external boundary.
func (d *Daemon) applyResize(plan *resizePlan, current ...func() bool) bool {
	for i := range plan.members {
		if len(current) != 0 && !current[0]() {
			return false
		}
		m := &plan.members[i]
		m.pane.resizeMu.Lock()
		m.pane.mu.Lock()
		old, pty := m.pane.rect, m.pane.pty
		m.pane.mu.Unlock()
		if old.Width == m.rect.Width && old.Height == m.rect.Height {
			m.ok = true
		} else if pty == nil {
			m.ok = true
		} else if err := pty.Resize(rectSize(m.rect)); err != nil {
			d.log.Warn("pty resize failed", "err", err)
		} else {
			m.ok = true
		}
		m.pane.resizeMu.Unlock()
	}
	return true
}

func (d *Daemon) commitResize(sess *session, ac *attachedClient, plan resizePlan) {
	// Publish tab layout and every rectangle together only after the coordinator
	// has rejected obsolete work. A later request may have applied already, but
	// it can never publish through this epoch.
	for _, tb := range plan.tabs {
		tb.mu.Lock()
		tb.size = tabSize(plan.size)
		tb.mu.Unlock()
	}
	for _, m := range plan.members {
		m.pane.mu.Lock()
		m.pane.rect = m.rect
		if m.isFloating {
			m.pane.popupGeometry = m.floating
		}
		if m.ok {
			m.pane.screen.Resize(m.rect.Width, m.rect.Height)
		}
		m.pane.mu.Unlock()
	}
	if ac != nil {
		ac.sendMu.Lock()
		ac.size = plan.size
		ac.sendMu.Unlock()
	}
	markSnapshotDirty(sess)
}

func (d *Daemon) runResizeTransaction(sess *session, ac *attachedClient, epoch uint64) {
	rc := sess.renderCoordinator()
	if rc == nil {
		return
	}
	snap := rc.resizeSnapshot()
	if !rc.resizeCurrent(epoch, ac, false) {
		return
	}
	d.exitCopyMode(ac)
	plan := d.prepareResize(sess, snap.size)
	if !rc.resizeCurrent(epoch, ac, false) {
		return
	}
	if !d.applyResize(&plan, func() bool { return rc.resizeCurrent(epoch, ac, false) }) {
		return
	}
	if !rc.resizeCurrent(epoch, ac, true) {
		return
	}
	d.commitResize(sess, ac, plan)
	d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
	// A successful transaction publishes exactly one full S2 frame. The
	// coordinator is the only emission route and stale epochs never reach it.
	rc.invalidateForAttachment(ac, renderInvalidation{class: invalidateUrgent, reset: true, producer: "transactional_resize.go"})
}

func (d *Daemon) requestTransactionalResize(sess *session, ac *attachedClient, size domain.Size, immediate bool) {
	if sess == nil || !size.Valid() {
		return
	}
	if ac == nil {
		// Headless geometry has no coordinator/transport to coalesce, but keeps
		// the same prepare/apply/commit ordering.
		plan := d.prepareResize(sess, size)
		d.applyResize(&plan)
		d.commitResize(sess, nil, plan)
		return
	}
	rc := d.attachCoordinator(sess, nil, ac, true)
	if immediate {
		epoch := rc.recordResizeRequest(size, ac)
		if epoch != 0 {
			d.runResizeTransaction(sess, ac, epoch)
		}
		return
	}
	rc.scheduleResize(size, ac, func(epoch uint64) { d.runResizeTransaction(sess, ac, epoch) })
}
