package daemon

import (
	"github.com/bnema/vev/internal/domain/layout"
)

// floatingSpare is one pre-started floating shell owned by a session rather
// than by a tab. The first tab that opens its floating pane claims it, and the
// session then starts a new spare in the background, so a session keeps at
// most one idle floating shell instead of one hidden shell per tab. A spare is
// never persisted or restored. Its reader starts only once a tab installs it;
// until then the kernel PTY buffer holds its first prompt. Its VEV identity
// names the tab that started it; floating panes are not --self targets.
//
// A claim takes the claiming tab's geometry (resize before the reader starts)
// and cwd: a spare started in another directory is discarded and the claim
// launches a fresh shell instead.
//
// The spare is guarded by session.floatingLaunchMu, which is never held across
// PTY operations or architecture locks.
type floatingSpare struct {
	pane    *pane
	cwd     string
	command string
}

// ensureFloatingSpare starts the session's spare when none exists or is
// starting. tb supplies the initial geometry and cwd; a claim from another tab
// resizes it or, when the cwd differs, replaces it.
func (d *Daemon) ensureFloatingSpare(sess *session, tb *tab) {
	if d == nil || sess == nil || tb == nil || d.ptys == nil {
		return
	}
	cfg := d.currentFloatingConfig()
	sess.floatingLaunchMu.Lock()
	if sess.floatingLaunchStopping || sess.floatingSpare != nil || sess.floatingSpareStarting {
		sess.floatingLaunchMu.Unlock()
		return
	}
	sess.floatingSpareStarting = true
	sess.floatingLaunchMu.Unlock()

	spec, err := d.newFloatingLaunchSpec(sess, tb, cfg, false)
	// The spare belongs to the session, so closing the tab that started it
	// must not cancel it.
	spec.parentCtx = sess.ctx
	var launch *floatingLaunch
	ok := err == nil && spec.parentCtx != nil
	if ok {
		launch, ok = sess.registerFloatingLaunch()
	}
	if !ok {
		sess.floatingLaunchMu.Lock()
		sess.floatingSpareStarting = false
		sess.floatingLaunchMu.Unlock()
		return
	}
	d.sessWg.Go(func() {
		defer sess.finishFloatingLaunch(launch)
		p := d.openFloatingPane(sess, spec)
		sess.floatingLaunchMu.Lock()
		sess.floatingSpareStarting = false
		if p != nil && !sess.floatingLaunchStopping {
			sess.floatingSpare = &floatingSpare{pane: p, cwd: spec.cwd, command: cfg.Command}
			p = nil
		}
		sess.floatingLaunchMu.Unlock()
		if p != nil {
			closeFloatingPane(p)
		}
	})
}

// takeFloatingSpare removes the session spare. It returns the pane when the
// spare was started for cwd and command; a mismatched spare is closed because
// it can never serve this directory or config. had reports whether a spare
// existed, so the caller knows to start its replacement.
func (sess *session) takeFloatingSpare(cwd, command string) (p *pane, had bool) {
	sess.floatingLaunchMu.Lock()
	spare := sess.floatingSpare
	sess.floatingSpare = nil
	sess.floatingLaunchMu.Unlock()
	if spare == nil {
		return nil, false
	}
	if spare.cwd != cwd || spare.command != command {
		closeFloatingPane(spare.pane)
		return nil, true
	}
	return spare.pane, true
}

// closeFloatingSpare releases the spare on session teardown. The caller has
// already set floatingLaunchStopping, so no launch can publish a new one.
func (sess *session) closeFloatingSpare() {
	sess.floatingLaunchMu.Lock()
	spare := sess.floatingSpare
	sess.floatingSpare = nil
	sess.floatingLaunchMu.Unlock()
	if spare != nil {
		closeFloatingPane(spare.pane)
	}
}

// openFloatingPane opens one floating PTY for spec and returns its initialized,
// unpublished pane, or nil when the launch failed or was cancelled.
func (d *Daemon) openFloatingPane(sess *session, spec floatingLaunchSpec) *pane {
	if err := spec.parentCtx.Err(); err != nil {
		return nil
	}
	lifetime := d.newPaneProcessLifetime(spec.parentCtx, sess.ctx)
	pty, err := d.ptys.Open(lifetime.ctx, spec.command, spec.args, spec.env, spec.cwd, spec.ptyGeometry)
	if err != nil {
		lifetime.abort()
		if pty != nil {
			_ = pty.Close()
		}
		d.log.Debug("floating spare spawn failed", "err", err, "session", spec.sessionName)
		return nil
	}
	p := newPaneWithStableIDAndTitle(layout.PaneID("floating"), spec.paneStableID, pty, spec.size, spec.fallback, d.currentHistoryConfig())
	p.geometry = spec.ptyGeometry
	setScreenGeometry(p.screen, spec.ptyGeometry)
	p.rect = spec.geometry.ptyRect()
	p.popupGeometry = spec.geometry
	if !lifetime.publish(p) {
		_ = pty.Close()
		return nil
	}
	return p
}

// adoptFloatingSpare fits a claimed spare to the claiming tab's launch spec
// and installs it in the current slot generation. It runs in the launch worker
// without any architecture lock, because PTY.Resize is external.
func (d *Daemon) adoptFloatingSpare(sess *session, tb *tab, p *pane, spec floatingLaunchSpec, generation uint64) {
	if p.geometry != spec.ptyGeometry {
		if err := p.pty.Resize(spec.ptyGeometry); err != nil {
			closeFloatingPane(p)
			d.failFloatingLaunch(sess, tb, generation, true, spec.sessionName, err)
			return
		}
	}
	p.mu.Lock()
	p.geometry = spec.ptyGeometry
	setScreenGeometry(p.screen, spec.ptyGeometry)
	p.rect = spec.geometry.ptyRect()
	p.popupGeometry = spec.geometry
	p.mu.Unlock()
	p.onExit = func() { d.reapInstalledFloating(p) }
	d.installFloating(sess, tb, p, generation)
}
