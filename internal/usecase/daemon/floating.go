package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

// floatingState describes the lifecycle of a tab's independent floating pane.
// A floating slot is always guarded by its owning tab.mu.
type floatingState uint8

const (
	floatingUninitialized floatingState = iota
	floatingWarming
	floatingHidden
	floatingVisible
)

// floatingSlot holds the runtime which is deliberately outside tab.tree and
// tab.panes. generation identifies an asynchronous launch and its installed
// pane, preventing stale launch completions and exits from changing new state.
type floatingSlot struct {
	state          floatingState
	pane           *pane
	desiredVisible bool
	generation     uint64
	launch         domain.FloatingConfig
}

// beginFloatingWarmLocked starts a single background launch. The caller must
// hold tb.mu and must only call it for an uninitialized slot.
func (tb *tab) beginFloatingWarmLocked(launch domain.FloatingConfig, desiredVisible bool) uint64 {
	if tb == nil || tb.floating.state != floatingUninitialized {
		return 0
	}
	tb.floating.generation++
	tb.floating.state = floatingWarming
	tb.floating.desiredVisible = desiredVisible
	tb.floating.launch = launch
	return tb.floating.generation
}

// toggleFloatingLocked applies the user action. Its result says whether a new
// PTY launch is required and returns the generation associated with that launch
// or current slot.
func (tb *tab) toggleFloatingLocked(launch domain.FloatingConfig) (bool, uint64) {
	if tb == nil {
		return false, 0
	}
	switch tb.floating.state {
	case floatingUninitialized:
		generation := tb.beginFloatingWarmLocked(launch, true)
		return generation != 0, generation
	case floatingWarming:
		tb.floating.desiredVisible = !tb.floating.desiredVisible
	case floatingHidden:
		tb.floating.state = floatingVisible
	case floatingVisible:
		tb.floating.state = floatingHidden
	}
	return false, tb.floating.generation
}

// installFloatingLocked installs a completed launch only when it is still the
// current warming generation. Stale completions must close their PTY elsewhere.
func (tb *tab) installFloatingLocked(p *pane, generation uint64) bool {
	if tb == nil || p == nil || tb.floating.state != floatingWarming || tb.floating.generation != generation {
		return false
	}
	tb.floating.pane = p
	if tb.floating.desiredVisible {
		tb.floating.state = floatingVisible
	} else {
		tb.floating.state = floatingHidden
	}
	return true
}

// failFloatingLocked clears only the current warming launch. It deliberately
// leaves generation intact; the next launch advances it.
func (tb *tab) failFloatingLocked(generation uint64) bool {
	if tb == nil || tb.floating.state != floatingWarming || tb.floating.generation != generation {
		return false
	}
	tb.floating.state = floatingUninitialized
	tb.floating.pane = nil
	tb.floating.desiredVisible = false
	tb.floating.launch = domain.FloatingConfig{}
	return true
}

// clearFloatingLocked clears a matching installed runtime. Matching both pane
// identity and generation prevents an old reader from reaping a newer pane.
func (tb *tab) clearFloatingLocked(p *pane, generation uint64) bool {
	if tb == nil || p == nil || tb.floating.pane != p || tb.floating.generation != generation ||
		(tb.floating.state != floatingHidden && tb.floating.state != floatingVisible) {
		return false
	}
	tb.floating.generation++
	tb.floating.state = floatingUninitialized
	tb.floating.pane = nil
	tb.floating.desiredVisible = false
	tb.floating.launch = domain.FloatingConfig{}
	return true
}

// terminalTargetLocked chooses the visible floating terminal ahead of the
// normal layout target. The caller must hold tb.mu.
func (tb *tab) terminalTargetLocked() *pane {
	if tb != nil && tb.floating.state == floatingVisible && tb.floating.pane != nil {
		return tb.floating.pane
	}
	return tb.focusedPane()
}

// floatingInnerSize returns the terminal area inside a percentage-sized popup.
// For each axis, a popup with at least three cells spends one cell on each
// border; smaller popups omit that axis's border. A valid tab always yields a
// valid PTY size.
func floatingInnerSize(tabSize domain.Size, cfg domain.FloatingConfig) domain.Size {
	if !tabSize.Valid() {
		return domain.Size{}
	}
	return domain.Size{
		Cols: floatingInnerAxis(tabSize.Cols, cfg.Width),
		Rows: floatingInnerAxis(tabSize.Rows, cfg.Height),
	}
}

func floatingInnerAxis(available, percent int) int {
	percent = min(max(percent, 1), 100)
	bounds := min(max(available*percent/100, 1), available)
	if bounds >= 3 {
		return bounds - 2
	}
	return bounds
}

// ensureFloatingWarm starts the background prewarm exactly once for a tab.
func (d *Daemon) ensureFloatingWarm(sess *session, tb *tab) {
	if d == nil || sess == nil || tb == nil || d.ptys == nil {
		return
	}
	d.startFloating(sess, tb, false)
}

// toggleFloating changes only this tab's slot. Opening an uninitialized slot
// launches asynchronously; installed slots are retained when hidden.
func (d *Daemon) toggleFloating(sess *session, ac *attachedClient) error {
	if d == nil || sess == nil {
		return fmt.Errorf("floating pane: session required")
	}
	tb := sess.activeTab()
	if tb == nil {
		return layout.ErrNotFound
	}
	cfg := d.currentFloatingConfig()
	tb.mu.Lock()
	start, generation := tb.toggleFloatingLocked(cfg)
	visible := tb.floating.desiredVisible
	tb.mu.Unlock()
	if start {
		d.launchFloating(sess, tb, cfg, generation, visible)
		return nil
	}
	if ac != nil {
		d.paint(sess, ac, true)
	}
	return nil
}

// startFloating starts a prewarm if its slot remains uninitialized.
func (d *Daemon) startFloating(sess *session, tb *tab, visible bool) {
	cfg := d.currentFloatingConfig()
	tb.mu.Lock()
	generation := tb.beginFloatingWarmLocked(cfg, visible)
	tb.mu.Unlock()
	if generation != 0 {
		d.launchFloating(sess, tb, cfg, generation, visible)
	}
}

// launchFloating snapshots all launch inputs before starting the potentially
// blocking factory call. In particular, no PTY operation occurs while tb.mu is
// held, and config reloads cannot affect this launch.
func (d *Daemon) launchFloating(sess *session, tb *tab, cfg domain.FloatingConfig, generation uint64, userOpen bool) {
	sess.mu.Lock()
	name, cwd, term := sess.name, sess.cwd, sess.terminal
	sess.mu.Unlock()
	tb.mu.Lock()
	sz := floatingInnerSize(tb.size, cfg)
	focused := tb.focusedPane()
	tabStableID := tb.stableID
	tabCtx := tb.ctx
	tb.mu.Unlock()
	if !sz.Valid() {
		sz = domain.Size{Cols: 1, Rows: 1}
	}
	if focused != nil && d.procCwd != nil && focused.pty != nil {
		if live, err := d.procCwd(focused.pty.Pid()); err == nil && live != "" {
			cwd = live
		}
	}
	if cwd == "" && d.dirOrHome != nil {
		cwd = d.dirOrHome("")
	}
	paneStableID, err := newStableID("p")
	if err != nil {
		d.failFloatingLaunch(sess, tb, generation, userOpen, err)
		return
	}
	env := d.childEnv(name, tabStableID, paneStableID, term)
	command, args := d.shell, append([]string(nil), d.shellArgs...)
	if cfg.Command != "" {
		args = []string{"-lc", cfg.Command}
	}
	fallback := floatingCommandFallback(cfg.Command, d.shell)
	go func() {
		pty, openErr := d.ptys.Open(command, args, env, cwd, sz)
		if openErr != nil {
			d.failFloatingLaunch(sess, tb, generation, userOpen, openErr)
			return
		}
		p := newPaneWithStableID(layout.PaneID("floating"), paneStableID, pty, sz)
		p.title.displayFallback = fallback
		if tabCtx == nil {
			tabCtx = sess.ctx
		}
		if tabCtx == nil {
			tabCtx = context.Background()
		}
		p.ctx, p.cancel = context.WithCancel(tabCtx)
		d.installFloating(sess, tb, p, generation)
	}()
}

func (d *Daemon) failFloatingLaunch(sess *session, tb *tab, generation uint64, userOpen bool, err error) {
	tb.mu.Lock()
	current := tb.failFloatingLocked(generation)
	tb.mu.Unlock()
	if current {
		kind := "background-prewarm"
		if userOpen {
			kind = "user-open"
		}
		d.log.Warn("floating pty spawn failed", "err", err, "session", sess.name, "kind", kind)
	}
}

// installFloating installs only the current generation. A stale successful
// launch is cancelled and closed outside the tab lock.
func (d *Daemon) installFloating(sess *session, tb *tab, p *pane, generation uint64) {
	tb.mu.Lock()
	installed := tb.installFloatingLocked(p, generation)
	visible := installed && tb.floating.state == floatingVisible
	tb.mu.Unlock()
	if !installed {
		if p.cancel != nil {
			p.cancel()
		}
		_ = p.pty.Close()
		return
	}
	p.onExit = func() { d.reapFloating(sess, tb, p, generation) }
	d.startPaneGoroutines(sess, tb, p)
	if visible {
		sess.mu.Lock()
		ac := sess.client
		sess.mu.Unlock()
		if ac != nil {
			d.paint(sess, ac, true)
		}
	}
}

// reapFloating ignores old readers and restores the background only when the
// exiting popup was visible.
func (d *Daemon) reapFloating(sess *session, tb *tab, p *pane, generation uint64) {
	tb.mu.Lock()
	visible := tb.floating.state == floatingVisible && tb.floating.pane == p && tb.floating.generation == generation
	cleared := tb.clearFloatingLocked(p, generation)
	tb.mu.Unlock()
	if !cleared {
		return
	}
	if p.cancel != nil {
		p.cancel()
	}
	if visible {
		sess.mu.Lock()
		ac := sess.client
		sess.mu.Unlock()
		if ac != nil {
			d.paint(sess, ac, true)
		}
	}
}

// teardownFloating invalidates first, then releases resources outside tb.mu.
func (d *Daemon) teardownFloating(tb *tab) {
	if tb == nil {
		return
	}
	tb.mu.Lock()
	p := tb.floating.pane
	tb.floating.generation++
	tb.floating.state = floatingUninitialized
	tb.floating.pane = nil
	tb.floating.desiredVisible = false
	tb.floating.launch = domain.FloatingConfig{}
	tb.mu.Unlock()
	if p != nil {
		if p.cancel != nil {
			p.cancel()
		}
		if p.pty != nil {
			_ = p.pty.Close()
		}
	}
}

func floatingCommandFallback(command, shell string) string {
	if fields := strings.Fields(command); len(fields) > 0 {
		return filepath.Base(fields[0])
	}
	return filepath.Base(shell)
}
