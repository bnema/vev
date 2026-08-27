package daemon

import (
	"slices"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

// mutateTargetLayout preserves the publication semantics used by resize and
// equalize mutations, which always publish a successful candidate.
func (d *Daemon) mutateTargetLayout(target daemonActionTarget, requirePane bool, mutate func(*layout.Tree, domain.Rect) error) error {
	_, err := d.mutateTargetLayoutChanged(target, requirePane, func(candidate *layout.Tree, area domain.Rect) (bool, error) {
		if err := mutate(candidate, area); err != nil {
			return false, err
		}
		return true, nil
	})
	return err
}

// mutateTargetLayoutChanged validates an explicit target and publishes the
// candidate only when mutate reports a change. State locks are released before
// applying pane geometry so PTY.Resize never runs under daemon state locks.
func (d *Daemon) mutateTargetLayoutChanged(target daemonActionTarget, requirePane bool, mutate func(*layout.Tree, domain.Rect) (bool, error)) (bool, error) {
	if target.session == nil || target.tab == nil || (requirePane && target.pane == nil) {
		return false, layout.ErrNotFound
	}

	d.mu.Lock()
	if d.sessions[target.session.id] != target.session {
		d.mu.Unlock()
		return false, layout.ErrNotFound
	}
	target.session.mu.Lock()
	if !slices.Contains(target.session.tabs, target.tab) {
		target.session.mu.Unlock()
		d.mu.Unlock()
		return false, layout.ErrNotFound
	}
	target.tab.mu.Lock()
	if target.tab.tree == nil || (requirePane && target.tab.panes[target.pane.id] != target.pane) {
		target.tab.mu.Unlock()
		target.session.mu.Unlock()
		d.mu.Unlock()
		return false, layout.ErrNotFound
	}

	candidate := target.tab.tree.Clone()
	area := domain.Rect{Width: target.tab.size.Cols, Height: target.tab.size.Rows}
	changed, err := mutate(candidate, area)
	if err != nil {
		target.tab.mu.Unlock()
		target.session.mu.Unlock()
		d.mu.Unlock()
		return false, err
	}
	if !changed {
		target.tab.mu.Unlock()
		target.session.mu.Unlock()
		d.mu.Unlock()
		return false, nil
	}

	target.tab.tree = candidate
	target.tab.bumpLayoutGenerationLocked()
	target.tab.mu.Unlock()
	target.session.mu.Unlock()
	d.mu.Unlock()

	if !target.session.geometry.applyTabLayout(d, target.session, target.tab) {
		target.tab.mu.Lock()
		applied := target.tab.tree == candidate
		target.tab.mu.Unlock()
		if !applied {
			return false, layout.ErrNotFound
		}
	}
	return true, nil
}
