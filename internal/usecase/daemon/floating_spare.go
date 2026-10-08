package daemon

import (
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/layout"
)

// floatingSpare is one pre-started floating shell owned by a session rather
// than by a tab. The first tab that opens its floating pane claims it, and the
// session then starts a new spare in the background, so a session keeps at
// most one idle floating shell instead of one hidden shell per tab. A spare is
// never persisted or restored. Its reader starts with the shell, so startup
// terminal queries are answered before any claim; with no owner its screen
// changes publish nothing. Its VEV identity names the tab that started it;
// floating panes are not --self targets.
//
// A claim takes the claiming tab's geometry and requires the same cwd,
// floating.command, shell and environment: a mismatched spare is discarded and
// the claim launches a fresh shell instead.
//
// The spare is guarded by session.floatingLaunchMu, which is never held across
// PTY operations or architecture locks.
type floatingSpare struct {
	pane *pane
	key  floatingSpareKey
}

// floatingSpareKey is every launch input a claim must share with the spare.
// env excludes the per-launch VEV identity.
type floatingSpareKey struct {
	cwd, floatingCommand, command, args, env string
}

func spareKey(spec floatingLaunchSpec) floatingSpareKey {
	env := make([]string, 0, len(spec.env))
	for _, entry := range spec.env {
		if key, _, _ := environmentEntry(entry); key != "VEV" {
			env = append(env, entry)
		}
	}
	return floatingSpareKey{
		cwd: spec.cwd, floatingCommand: spec.floatingCommand, command: spec.command,
		args: strings.Join(spec.args, "\x00"), env: strings.Join(env, "\x00"),
	}
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
		if p != nil {
			// The reader answers color and scheme queries, so the spare
			// starts with the session theme, as a new tab does.
			d.applySessionThemeToPane(sess, p)
		}
		sess.floatingLaunchMu.Lock()
		sess.floatingSpareStarting = false
		if p != nil && !sess.floatingLaunchStopping {
			sess.floatingSpare = &floatingSpare{pane: p, key: spareKey(spec)}
			p.prestarted = true
			spare := p
			spare.onExit = func() {
				spare.spareExited.Store(true)
				// Still unclaimed: drop it. A claim takes the spare under the
				// same lock, then reaps after install when spareExited is set.
				if sess.dropFloatingSpare(spare) {
					return
				}
				d.reapInstalledFloating(spare)
			}
			d.sessWg.Add(1)
			go d.readPanePTY(p)
			p = nil
		}
		sess.floatingLaunchMu.Unlock()
		if p != nil {
			closeFloatingPane(p)
		}
	})
}

// takeFloatingSpare removes the session spare. It returns the pane when the
// spare was started with the same launch inputs as spec; a mismatched spare is
// closed because it can never serve this launch. had reports whether a spare
// existed, so the caller knows to start its replacement.
func (sess *session) takeFloatingSpare(spec floatingLaunchSpec) (p *pane, had bool) {
	sess.floatingLaunchMu.Lock()
	spare := sess.floatingSpare
	sess.floatingSpare = nil
	sess.floatingLaunchMu.Unlock()
	if spare == nil {
		return nil, false
	}
	if spare.key != spareKey(spec) {
		closeFloatingPane(spare.pane)
		return nil, true
	}
	return spare.pane, true
}

// dropFloatingSpare closes p when it is still the session's unclaimed spare
// and reports whether it did. A claimed spare is left to its tab.
func (sess *session) dropFloatingSpare(p *pane) bool {
	sess.floatingLaunchMu.Lock()
	unclaimed := sess.floatingSpare != nil && sess.floatingSpare.pane == p
	if unclaimed {
		sess.floatingSpare = nil
	}
	sess.floatingLaunchMu.Unlock()
	if unclaimed {
		closeFloatingPane(p)
	}
	return unclaimed
}

// applySessionThemeToPane gives a pane the theme of the session's first
// attachment, as createTab does for a new tab. A headless session leaves the
// pane unthemed until the next theme application.
func (d *Daemon) applySessionThemeToPane(sess *session, p *pane) {
	attachments := sess.snapshotAttachments()
	if len(attachments) == 0 {
		return
	}
	t := d.effectiveTheme(attachments[0].getClientTheme())
	p.mu.Lock()
	applyPaneThemeLocked(p, t, false)
	p.mu.Unlock()
}

// floatingSparePane returns the session's unclaimed spare pane, if any.
func (sess *session) floatingSparePane() *pane {
	sess.floatingLaunchMu.Lock()
	defer sess.floatingLaunchMu.Unlock()
	if sess.floatingSpare == nil {
		return nil
	}
	return sess.floatingSpare.pane
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
	p.resizeMu.Lock()
	p.mu.Lock()
	resize := p.geometry != spec.ptyGeometry
	if resize {
		// The spare's reader is running: buffer its output across the
		// resize so the redraw is parsed at the new size.
		p.resizeApplying = true
	}
	p.mu.Unlock()
	if resize {
		if err := resizePTYGeometry(p.pty, spec.ptyGeometry); err != nil {
			d.replayResizePending(sess, tb, p, false, domain.Rect{})
			p.resizeMu.Unlock()
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
	if resize {
		d.replayResizePending(sess, tb, p, false, domain.Rect{})
	}
	p.resizeMu.Unlock()
	d.installFloating(sess, tb, p, generation)
	if p.spareExited.Load() {
		// The shell exited before the owner was published, so its reader
		// could not reap the slot.
		d.reapInstalledFloating(p)
	}
}
