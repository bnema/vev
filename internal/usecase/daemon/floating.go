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
}

// beginFloatingWarmLocked starts a single background launch. The caller must
// hold tb.mu and must only call it for an uninitialized slot.
func (tb *tab) beginFloatingWarmLocked(desiredVisible bool) uint64 {
	if tb == nil || tb.floating.state != floatingUninitialized {
		return 0
	}
	tb.floating.generation++
	tb.floating.state = floatingWarming
	tb.floating.desiredVisible = desiredVisible
	return tb.floating.generation
}

// toggleFloatingLocked applies the user action. Its result says whether a new
// PTY launch is required and returns the generation associated with that launch
// or current slot.
func (tb *tab) toggleFloatingLocked() (bool, uint64) {
	if tb == nil {
		return false, 0
	}
	switch tb.floating.state {
	case floatingUninitialized:
		generation := tb.beginFloatingWarmLocked(true)
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
	return true
}

// clearFloatingLocked clears a matching installed runtime. Matching both pane
// identity and generation prevents an old reader from reaping a newer pane.
func (tb *tab) clearFloatingLocked(p *pane, generation uint64) bool {
	if tb == nil || p == nil || tb.floating.pane != p || tb.floating.generation != generation ||
		(tb.floating.state != floatingHidden && tb.floating.state != floatingVisible) {
		return false
	}
	tb.takeFloatingLocked()
	return true
}

// takeFloatingLocked invalidates the slot and detaches its installed pane. The
// caller must hold tb.mu and must close the returned pane after unlocking.
func (tb *tab) takeFloatingLocked() *pane {
	if tb == nil {
		return nil
	}
	p := tb.floating.pane
	tb.floating.generation++
	tb.floating.state = floatingUninitialized
	tb.floating.pane = nil
	tb.floating.desiredVisible = false
	return p
}

// terminalTargetLocked chooses the visible floating terminal ahead of the
// normal layout target. The caller must hold tb.mu.
func (tb *tab) terminalTargetLocked() *pane {
	if tb != nil && tb.floating.state == floatingVisible && tb.floating.pane != nil {
		return tb.floating.pane
	}
	return tb.focusedPane()
}

// visibleFloatingSnapshotLocked captures the visible popup and its current
// geometry under the tab lock, so input cannot mix a pane from one state with
// coordinates from another. The caller must hold tb.mu.
func (tb *tab) visibleFloatingSnapshotLocked(cfg domain.FloatingConfig) (*pane, floatingGeometry, bool) {
	if tb == nil || tb.floating.state != floatingVisible || tb.floating.pane == nil {
		return nil, floatingGeometry{}, false
	}
	p := tb.floating.pane
	desired := calculateContentFloatingGeometry(domain.Size{Cols: tb.size.Cols, Rows: tb.size.Rows}, cfg)
	p.mu.Lock()
	geometry := p.committedFloatingGeometryLocked(desired)
	p.mu.Unlock()
	if geometry.Inner.Width <= 0 || geometry.Inner.Height <= 0 {
		return nil, floatingGeometry{}, false
	}
	return p, geometry, true
}

// ensureFloatingWarm starts the background prewarm exactly once for a tab.
func (d *Daemon) ensureFloatingWarm(sess *session, tb *tab) {
	if d == nil || sess == nil || tb == nil || d.ptys == nil {
		return
	}
	d.startFloating(sess, tb, false)
}

// activateTab performs the work associated with making a tab the destination.
// It is deliberately idempotent: every tab transition can use this single
// hook, while inactive restored tabs remain cold until actually selected.
func (d *Daemon) activateTab(sess *session, tb *tab) {
	if d == nil || sess == nil || tb == nil {
		return
	}
	// A headless session has no actual terminal destination. Deferring warmup
	// until firstPaint keeps restored tabs cold and avoids launching children
	// merely because a tab was manipulated during teardown.
	sess.mu.Lock()
	ac := sess.client
	sess.mu.Unlock()
	d.exitCopyMode(ac)
	if ac == nil {
		return
	}
	d.ensureFloatingWarm(sess, tb)
	tb.mu.Lock()
	hasFloating := tb.floating.pane != nil
	size := domain.Size{Cols: tb.size.Cols, Rows: tb.size.Rows + 2}
	tb.mu.Unlock()
	if hasFloating {
		d.requestTransactionalResize(sess, ac, size, true)
	}
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
	start, generation := tb.toggleFloatingLocked()
	// desiredVisible is meaningful only while a launch is warming. Installed
	// slots derive their resize and paint work from the resulting slot state.
	visible := tb.floating.state == floatingVisible
	tb.mu.Unlock()
	if start {
		d.launchFloating(sess, tb, cfg, generation, true)
		if ac != nil {
			d.invalidateRender(sess, ac, true, "floating.go")
		}
		return nil
	}
	if visible {
		tb.mu.Lock()
		size := domain.Size{Cols: tb.size.Cols, Rows: tb.size.Rows + 2}
		tb.mu.Unlock()
		d.requestTransactionalResize(sess, ac, size, true)
	}
	return nil
}

// startFloating starts a prewarm if its slot remains uninitialized.
func (d *Daemon) startFloating(sess *session, tb *tab, visible bool) {
	cfg := d.currentFloatingConfig()
	tb.mu.Lock()
	generation := tb.beginFloatingWarmLocked(visible)
	tb.mu.Unlock()
	if generation != 0 {
		d.launchFloating(sess, tb, cfg, generation, visible)
	}
}

// floatingLaunchSpec is an immutable snapshot of one launch. It is built
// before the worker starts so config reloads cannot affect that launch.
type floatingLaunchSpec struct {
	sessionName  string
	cwd          string
	size         domain.Size
	geometry     floatingGeometry
	paneStableID string
	env          []string
	command      string
	args         []string
	fallback     string
	parentCtx    context.Context
	userOpen     bool
}

// launchFloating snapshots launch inputs, then accounts for a worker through
// Open, pane initialization, publication, and reader/coordinator startup.
func (d *Daemon) launchFloating(sess *session, tb *tab, cfg domain.FloatingConfig, generation uint64, userOpen bool) {
	spec, err := d.newFloatingLaunchSpec(sess, tb, cfg, userOpen)
	if err != nil {
		d.failFloatingLaunch(tb, generation, userOpen, spec.sessionName, err)
		return
	}
	// Count the launch before its goroutine starts. It remains counted through
	// install, including the reader/coordinator Adds, so shutdown cannot return
	// before a late Open completion has either been rejected or fully joined.
	d.sessWg.Go(func() { d.openAndInstallFloating(sess, tb, spec, generation) })
}

func (d *Daemon) newFloatingLaunchSpec(sess *session, tb *tab, cfg domain.FloatingConfig, userOpen bool) (floatingLaunchSpec, error) {
	sess.mu.Lock()
	name, cwd, term, sessCtx := sess.name, sess.cwd, sess.terminal, sess.ctx
	sess.mu.Unlock()
	tb.mu.Lock()
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: tb.size.Cols, Rows: tb.size.Rows}, cfg)
	size := rectSize(geometry.Inner)
	if !size.Valid() {
		geometry = floatingGeometry{Bounds: domain.Rect{Width: 1, Height: 1}, Inner: domain.Rect{Width: 1, Height: 1}}
		size = rectSize(geometry.Inner)
	}
	focused := tb.focusedPane()
	tabStableID, tabCtx := tb.stableID, tb.ctx
	tb.mu.Unlock()
	if focused != nil && d.procCwd != nil && focused.pty != nil {
		if live, err := d.procCwd(focused.pty.Pid()); err == nil && live != "" {
			cwd = live
		}
	}
	if d.dirOrHome != nil {
		cwd = d.dirOrHome(cwd)
	}
	paneStableID, err := newStableID("p")
	if err != nil {
		return floatingLaunchSpec{sessionName: name}, err
	}
	if tabCtx == nil {
		tabCtx = sessCtx
	}
	if tabCtx == nil {
		tabCtx = context.Background()
	}
	command, args := d.shell, append([]string(nil), d.shellArgs...)
	if cfg.Command != "" {
		args = []string{"-lc", cfg.Command}
	}
	return floatingLaunchSpec{
		sessionName:  name,
		cwd:          cwd,
		size:         size,
		geometry:     geometry,
		paneStableID: paneStableID,
		env:          d.childEnv(name, tabStableID, paneStableID, term),
		command:      command,
		args:         args,
		fallback:     floatingCommandFallback(cfg.Command, d.shell),
		parentCtx:    tabCtx,
		userOpen:     userOpen,
	}, nil
}

// openAndInstallFloating runs entirely in the launch worker. No PTY operation
// occurs under tb.mu; the pane is fully initialized before publication.
func (d *Daemon) openAndInstallFloating(sess *session, tb *tab, spec floatingLaunchSpec, generation uint64) {
	pty, err := d.ptys.Open(spec.command, spec.args, spec.env, spec.cwd, spec.size)
	if err != nil {
		d.failFloatingLaunch(tb, generation, spec.userOpen, spec.sessionName, err)
		return
	}
	p := newPaneWithStableID(layout.PaneID("floating"), spec.paneStableID, pty, spec.size)
	p.rect = spec.geometry.Inner
	p.popupGeometry = spec.geometry
	p.title.displayFallback = spec.fallback
	p.ctx, p.cancel = context.WithCancel(spec.parentCtx)
	// The reader may run as soon as install returns; make its exit policy
	// immutable before publishing the pane to the slot.
	p.onExit = func() { d.reapFloating(sess, tb, p, generation) }
	d.installFloating(sess, tb, p, generation)
}

func (d *Daemon) failFloatingLaunch(tb *tab, generation uint64, userOpen bool, sessionName string, err error) {
	tb.mu.Lock()
	current := tb.failFloatingLocked(generation)
	tb.mu.Unlock()
	if current {
		kind := "background-prewarm"
		if userOpen {
			kind = "user-open"
		}
		d.log.Warn("floating pty spawn failed", "err", err, "session", sessionName, "kind", kind)
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
		closeFloatingPane(p)
		return
	}
	d.reapplyThemeSession(sess)
	d.startPaneGoroutines(sess, tb, p)
	if visible {
		sess.mu.Lock()
		ac := sess.client
		sess.mu.Unlock()
		tb.mu.Lock()
		size := domain.Size{Cols: tb.size.Cols, Rows: tb.size.Rows + 2}
		tb.mu.Unlock()
		d.requestTransactionalResize(sess, ac, size, true)
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
	closeFloatingPane(p)
	sess.mu.Lock()
	ac := sess.client
	sess.mu.Unlock()
	copyCleared := ac != nil && ac.overlays.clearCopyModeForPane(p)
	if ac != nil && (visible || copyCleared) {
		d.invalidateRender(sess, ac, true, "floating.go")
	}
}

// teardownFloating invalidates first, then releases resources outside tb.mu.
func (d *Daemon) teardownFloating(tb *tab, ac *attachedClient) {
	if tb == nil {
		return
	}
	tb.mu.Lock()
	p := tb.takeFloatingLocked()
	tb.mu.Unlock()
	if ac != nil {
		ac.overlays.clearCopyModeForPane(p)
	}
	closeFloatingPane(p)
}

// closeFloatingPane releases a floating runtime at most once. It is called
// only after a generation/identity check has detached the pane from its slot.
func closeFloatingPane(p *pane) {
	if p == nil {
		return
	}
	p.floatingCloseOnce.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
		if p.pty != nil {
			_ = p.pty.Close()
		}
	})
}

func floatingCommandFallback(command, shell string) string {
	if fields := strings.Fields(command); len(fields) > 0 {
		return filepath.Base(fields[0])
	}
	return filepath.Base(shell)
}
