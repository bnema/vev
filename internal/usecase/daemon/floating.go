package daemon

import (
	"context"
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

// floatingTransferableLocked reports whether a tab's floating lifecycle can
// cross an ownership commit. A warming Open remains registered to its source
// session until installation or failure, so moves must reject only that state.
// The caller must hold tb.mu.
func (tb *tab) floatingTransferableLocked() bool {
	return tb != nil && tb.floating.state != floatingWarming
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
	if !geometry.committable() {
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
func (d *Daemon) activateTab(sess *session, tb *tab) bool {
	return d.activateTabAfterResize(sess, tb, false)
}

// activateTabAfterResize retains tab activation's warmup work while avoiding a
// second resize when a synchronous outer resize request was already accepted.
func (d *Daemon) activateTabAfterResize(sess *session, tb *tab, outerResizeAccepted bool) bool {
	return d.activateTabAfterResizeForLease(sess, tb, outerResizeAccepted, nil, nil)
}

// activateTabAfterResizeForLease keeps transition-owned resize effects bound to
// the exact coordinator incarnation. A nil lease preserves direct/headless
// activation behavior.
func (d *Daemon) activateTabAfterResizeForLease(sess *session, tb *tab, outerResizeAccepted bool, ac *attachedClient, lease *attachmentLease) bool {
	if d == nil || sess == nil || tb == nil {
		return false
	}
	// A headless session has no attachment-local copy mode to clear. Keep its
	// geometry work session-wide instead of choosing an arbitrary attachment.
	if rc := sess.renderCoordinator(); rc != nil && rc.opts.onActivateTabAfterResize != nil {
		rc.opts.onActivateTabAfterResize(lease != nil)
	}
	if lease != nil {
		rc := sess.renderCoordinator()
		if rc == nil || !rc.leaseCurrent(lease, true) {
			return false
		}
	}
	d.ensureFloatingWarm(sess, tb)
	if ac == nil {
		for _, attachment := range sess.snapshotAttachments() {
			d.exitCopyMode(attachment)
		}
		if outerResizeAccepted {
			return false
		}
		tb.mu.Lock()
		hasFloating := tb.floating.pane != nil
		size := domain.Size{Cols: tb.size.Cols, Rows: tb.size.Rows + 2}
		tb.mu.Unlock()
		if hasFloating {
			return d.requestTransactionalResize(sess, nil, size, true)
		}
		return false
	}
	d.exitCopyMode(ac)
	if outerResizeAccepted {
		return false
	}
	tb.mu.Lock()
	hasFloating := tb.floating.pane != nil
	size := domain.Size{Cols: tb.size.Cols, Rows: tb.size.Rows + 2}
	tb.mu.Unlock()
	if hasFloating {
		if lease != nil {
			return d.requestTransactionalResizeForLease(sess, ac, lease, size, true)
		}
		return d.requestTransactionalResize(sess, ac, size, true)
	}
	return false
}

// toggleFloating changes only this tab's slot. Opening an uninitialized slot
// launches asynchronously; installed slots are retained when hidden.
func (d *Daemon) toggleFloating(sess *session, ac *attachedClient) error {
	if d == nil || sess == nil {
		return domain.UserErr(domain.NoticeFloatingSpawn, "couldn't open floating pane: no active session", nil)
	}
	tb := sess.tabForAttachment(ac)
	if ac == nil {
		tb = sess.firstTab()
	}
	if tb == nil {
		return domain.UserErr(domain.NoticeFloatingSpawn, "couldn't open floating pane: no active tab", layout.ErrNotFound)
	}
	cfg := d.currentFloatingConfig()
	tb.mu.Lock()
	wasVisible := tb.floating.state == floatingVisible
	start, generation := tb.toggleFloatingLocked()
	// desiredVisible is meaningful only while a launch is warming. Installed
	// slots derive their resize and paint work from the resulting slot state.
	visible := tb.floating.state == floatingVisible
	hidden := wasVisible && tb.floating.state == floatingHidden
	tb.mu.Unlock()
	if start {
		d.launchFloating(sess, tb, cfg, generation, true)
		if ac != nil {
			d.invalidateRender(sess, ac, true, "floating.go")
		}
		return nil
	}
	if hidden {
		d.invalidateRender(sess, ac, true, "floating.go")
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
// floatingLaunch owns one worker from queueing through installation or stale
// result cleanup. Its done channel lets session teardown reap an in-flight
// Open before returning.
type floatingLaunch struct {
	done chan struct{}
}

func (sess *session) registerFloatingLaunch() (*floatingLaunch, bool) {
	launch := &floatingLaunch{done: make(chan struct{})}
	sess.floatingLaunchMu.Lock()
	defer sess.floatingLaunchMu.Unlock()
	if sess.floatingLaunchStopping {
		return nil, false
	}
	if sess.floatingLaunches == nil {
		sess.floatingLaunches = make(map[*floatingLaunch]struct{})
	}
	sess.floatingLaunches[launch] = struct{}{}
	return launch, true
}

func (sess *session) finishFloatingLaunch(launch *floatingLaunch) {
	sess.floatingLaunchMu.Lock()
	delete(sess.floatingLaunches, launch)
	close(launch.done)
	sess.floatingLaunchMu.Unlock()
}

func (sess *session) stopFloatingLaunches() {
	sess.floatingLaunchMu.Lock()
	sess.floatingLaunchStopping = true
	sess.floatingLaunchMu.Unlock()
}

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
		d.failFloatingLaunch(sess, tb, generation, userOpen, spec.sessionName, err)
		return
	}
	launch, ok := sess.registerFloatingLaunch()
	if !ok {
		d.failFloatingLaunch(sess, tb, generation, userOpen, spec.sessionName, context.Canceled)
		return
	}
	// Count the launch before its goroutine starts. It remains counted through
	// install, including the reader/coordinator Adds, so shutdown cannot return
	// before a late Open completion has either been rejected or fully joined.
	d.sessWg.Go(func() {
		defer sess.finishFloatingLaunch(launch)
		d.openAndInstallFloating(sess, tb, spec, generation)
	})
}

func (d *Daemon) newFloatingLaunchSpec(sess *session, tb *tab, cfg domain.FloatingConfig, userOpen bool) (floatingLaunchSpec, error) {
	sess.mu.Lock()
	name, cwd, term, sessCtx := sess.name, sess.cwd, sess.terminal, sess.ctx
	env := copyEnvironment(sess.env)
	sess.mu.Unlock()
	tb.mu.Lock()
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: tb.size.Cols, Rows: tb.size.Rows}, cfg)
	size := rectSize(geometry.ptyRect())
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
	command, args := d.ptyCommand(env)
	if cfg.Command != "" {
		args = []string{"-lc", cfg.Command}
	}
	return floatingLaunchSpec{
		sessionName:  name,
		cwd:          cwd,
		size:         size,
		geometry:     geometry,
		paneStableID: paneStableID,
		env:          childEnvFrom(env, name, tabStableID, paneStableID, term),
		command:      command,
		args:         args,
		fallback:     floatingCommandFallback(cfg.Command, command),
		parentCtx:    tabCtx,
		userOpen:     userOpen,
	}, nil
}

// openAndInstallFloating runs entirely in the launch worker. No PTY operation
// occurs under tb.mu; the pane is fully initialized before publication.
func (d *Daemon) openAndInstallFloating(sess *session, tb *tab, spec floatingLaunchSpec, generation uint64) {
	// Open is context-aware and is intentionally called without any daemon,
	// session, tab, or launch-ownership lock. Cancellation makes teardown
	// bounded even if the adapter is waiting to create the child.
	if err := spec.parentCtx.Err(); err != nil {
		return
	}
	lifetime := d.newPaneProcessLifetime(spec.parentCtx, sess.ctx)
	ownedByPane := false
	defer func() {
		if !ownedByPane {
			lifetime.abort()
		}
	}()
	pty, err := d.ptys.Open(lifetime.ctx, spec.command, spec.args, spec.env, spec.cwd, spec.size)
	if err != nil {
		// Open retains ownership of a nonnil PTY only on success. Some factory
		// implementations can return a partially opened PTY with an error, so
		// release both resources before publishing the launch failure.
		lifetime.abort()
		if pty != nil {
			_ = pty.Close()
		}
		d.failFloatingLaunch(sess, tb, generation, spec.userOpen, spec.sessionName, err)
		return
	}
	p := newPaneWithStableID(layout.PaneID("floating"), spec.paneStableID, pty, spec.size)
	p.rect = spec.geometry.ptyRect()
	p.popupGeometry = spec.geometry
	p.title.displayFallback = spec.fallback
	if !lifetime.publish(p) {
		_ = pty.Close()
		d.failFloatingLaunch(sess, tb, generation, spec.userOpen, spec.sessionName, lifetime.ctx.Err())
		return
	}
	ownedByPane = true
	// The reader may run as soon as install returns; make its exit policy
	// immutable before publishing the pane to the slot. Installed panes resolve
	// their owner dynamically so a transferred tab never reaps through source
	// session pointers captured by its launch.
	p.onExit = func() { d.reapInstalledFloating(p) }
	d.installFloating(sess, tb, p, generation)
}

func (d *Daemon) failFloatingLaunch(sess *session, tb *tab, generation uint64, userOpen bool, sessionName string, err error) {
	tb.mu.Lock()
	current := tb.failFloatingLocked(generation)
	tb.mu.Unlock()
	if current {
		kind := "background-prewarm"
		if userOpen {
			kind = "user-open"
		}
		d.log.Warn("floating pty spawn failed", "err", err, "session", sessionName, "kind", kind)
		// A background prewarm is speculative: the user never asked for it, so
		// a toast here would be confusing noise. Only a user-initiated open
		// (the user pressed the toggle-floating key) earns a toast; if a
		// prewarm failure matters, the user's later open attempt will fail
		// too and surface then.
		if userOpen {
			d.reportError(sess, domain.UserErr(domain.NoticeFloatingSpawn,
				"couldn't open floating pane: command failed to start", err))
		}
	}
}

// installFloating installs only the current generation. A stale successful
// launch is cancelled and closed outside the tab lock.
func (d *Daemon) installFloating(sess *session, tb *tab, p *pane, generation uint64) {
	if sess == nil || tb == nil || p == nil {
		closeFloatingPane(p)
		return
	}
	tb.mu.Lock()
	installable := tb.floating.state == floatingWarming && tb.floating.generation == generation
	if installable {
		// Publish under pane.mu before the slot becomes hidden or visible. The
		// tab-to-pane lock order also linearizes owner and slot publication.
		publishPaneOwner(p, sess, tb, generation)
	}
	installed := installable && tb.installFloatingLocked(p, generation)
	visible := installed && tb.floating.state == floatingVisible
	tb.mu.Unlock()
	if !installed {
		closeFloatingPane(p)
		return
	}
	d.reapplyThemeSession(sess)
	d.startPaneGoroutines(sess, tb, p)
	if visible {
		attachments := sess.snapshotAttachments()
		if len(attachments) == 0 {
			// A headless resize may commit while Open is warming. Reconcile the
			// visible floating geometry even without an attachment callback.
			d.applyVisibleFloatingLayout(sess, tb, nil)
			return
		}
		tb.mu.Lock()
		size := domain.Size{Cols: tb.size.Cols, Rows: tb.size.Rows + 2}
		tb.mu.Unlock()
		for _, ac := range attachments {
			d.requestTransactionalResize(sess, ac, size, true)
		}
	}
}

// reapInstalledFloating resolves the pane's current immutable owner on every
// attempt. If ownership changes before the tab lock is acquired, retrying the
// lookup routes exit through the destination. The owner pointer and floating
// slot generation are then checked together under tb.mu so an old exit can
// never clear a reused slot.
func (d *Daemon) reapInstalledFloating(p *pane) {
	if p == nil {
		return
	}
	for {
		owner := p.ownerSnapshot()
		if owner == nil || owner.session == nil || owner.tab == nil || owner.floatingSlotGeneration == 0 {
			return
		}
		sess, tb, generation := owner.session, owner.tab, owner.floatingSlotGeneration
		tb.mu.Lock()
		if p.ownerSnapshot() != owner {
			tb.mu.Unlock()
			continue
		}
		visible := tb.floating.state == floatingVisible && tb.floating.pane == p && tb.floating.generation == generation
		cleared := tb.clearFloatingLocked(p, generation)
		tb.mu.Unlock()
		if !cleared {
			return
		}
		closeFloatingPane(p)
		attachments := sess.snapshotAttachments()
		copyCleared := false
		for _, ac := range attachments {
			ac.pruneCaptureFrames(p)
			copyCleared = ac.overlays.clearCopyModeForPane(p) || copyCleared
			if visible || copyCleared {
				d.invalidateRender(sess, ac, true, "floating.go")
			}
		}
		return
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
		ac.pruneCaptureFrames(p)
	}
	closeFloatingPane(p)
}

// closeFloatingPane releases a floating runtime at most once. It is called
// only after a generation/identity check has detached the pane from its slot.
func closeFloatingPane(p *pane) {
	closePaneProcess(p)
}

func floatingCommandFallback(command, shell string) string {
	if fields := strings.Fields(command); len(fields) > 0 {
		return filepath.Base(fields[0])
	}
	return filepath.Base(shell)
}
