package daemon

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
)

type barScriptExecutor interface {
	run(ctx context.Context, command string, env []string, scriptCtx barScriptContext) (string, error)
}

type barScriptConfig struct {
	topRight    string
	bottomRight string
	interval    time.Duration
}

type barScriptOutputs struct {
	topRight    string
	bottomRight string
}

type barScriptFailureKey struct {
	id     domain.SessionID
	anchor string
}

type barScriptState struct {
	mu          sync.Mutex
	cfg         barScriptConfig
	runner      barScriptExecutor
	outputs     map[domain.SessionID]barScriptOutputs
	lastRefresh map[domain.SessionID]time.Time
	lastContext map[domain.SessionID]barScriptContext
	running     map[domain.SessionID]bool
	pending     map[domain.SessionID]bool
	lastFailure map[barScriptFailureKey]string
	version     uint64

	// reload wakes barScriptPoller so an interval change takes effect without
	// waiting out the timer armed under the previous interval.
	reload chan struct{}
}

func effectiveBarInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return 5 * time.Second
	}
	if d < domain.MinBarInterval {
		return domain.MinBarInterval
	}
	return d
}

func barConfigFromDomain(cfg domain.BarConfig) barScriptConfig {
	return barScriptConfig{topRight: cfg.TopRight, bottomRight: cfg.BottomRight, interval: effectiveBarInterval(cfg.Interval)}
}

func (s *barScriptState) initLocked() {
	if s.outputs == nil {
		s.outputs = make(map[domain.SessionID]barScriptOutputs)
	}
	if s.lastRefresh == nil {
		s.lastRefresh = make(map[domain.SessionID]time.Time)
	}
	if s.lastContext == nil {
		s.lastContext = make(map[domain.SessionID]barScriptContext)
	}
	if s.running == nil {
		s.running = make(map[domain.SessionID]bool)
	}
	if s.pending == nil {
		s.pending = make(map[domain.SessionID]bool)
	}
	if s.lastFailure == nil {
		s.lastFailure = make(map[barScriptFailureKey]string)
	}
}

func (d *Daemon) barScriptSnapshot(sess *session) (string, string) {
	if d == nil || d.barScripts == nil || sess == nil {
		return "", ""
	}
	d.barScripts.mu.Lock()
	defer d.barScripts.mu.Unlock()
	out := d.barScripts.outputs[sess.id]
	return out.topRight, out.bottomRight
}

func (d *Daemon) barScriptInterval() time.Duration {
	if d == nil || d.barScripts == nil {
		return 5 * time.Second
	}
	d.barScripts.mu.Lock()
	defer d.barScripts.mu.Unlock()
	return effectiveBarInterval(d.barScripts.cfg.interval)
}

func (d *Daemon) collectBarScriptContext(sess *session, anchor string) (barScriptContext, []string, bool) {
	ctx := barScriptContext{Anchor: anchor}
	if sess == nil {
		return ctx, nil, false
	}
	sess.mu.Lock()
	ctx.Session = sess.name
	// No copy needed: writers always replace sess.env with a whole fresh
	// slice under sess.mu (daemon.go, resume.go), and nothing mutates the
	// backing array in place, so capturing the header here is safe.
	env := sess.env
	active := sess.active
	var tb *tab
	if active >= 0 && active < len(sess.tabs) {
		tb = sess.tabs[active]
	}
	ac := sess.client
	ctx.PaneCWD = sess.cwd
	sess.mu.Unlock()
	if ac == nil || tb == nil {
		return ctx, env, false
	}
	tb.mu.Lock()
	ctx.Tab = tb.stableID
	ctx.Cols = tb.size.Cols
	pid := 0
	if p := tb.focusedPane(); p != nil {
		ctx.Pane = p.stableID
		if d != nil && d.procCwd != nil && p.pty != nil {
			pid = p.pty.Pid()
		}
	}
	tb.mu.Unlock()
	if pid != 0 && d != nil && d.procCwd != nil {
		if cwd, err := d.procCwd(pid); err == nil && cwd != "" {
			ctx.PaneCWD = cwd
		}
	}
	return ctx, env, true
}

// signalBarPollerReload wakes the poller without blocking. The channel is
// buffered to one, so a pending signal already conveys "config changed".
func (d *Daemon) signalBarPollerReload() {
	if d == nil || d.barScripts == nil || d.barScripts.reload == nil {
		return
	}
	select {
	case d.barScripts.reload <- struct{}{}:
	default:
	}
}

func (d *Daemon) barScriptPoller(ctx context.Context) {
	timer := d.clock.NewTimer(d.barScriptInterval())
	defer func() { timer.Stop() }()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.barScripts.reload:
			timer.Stop()
			timer = d.clock.NewTimer(d.barScriptInterval())
		case now := <-timer.C():
			for _, sess := range d.sessionsSnapshot() {
				d.refreshBarScriptsIfDue(sess, now, false)
			}
			timer.Stop()
			timer = d.clock.NewTimer(d.barScriptInterval())
		}
	}
}

// refreshBarScriptsAllSessions forces a bar-script run for every live session.
// Called after a config change so a new command takes effect immediately rather
// than leaving the anchor blank until the poller's next tick.
func (d *Daemon) refreshBarScriptsAllSessions() {
	if d == nil || d.barScripts == nil || d.clock == nil {
		return
	}
	now := d.clock.Now()
	for _, sess := range d.sessionsSnapshot() {
		d.refreshBarScriptsIfDue(sess, now, true)
	}
}

func (d *Daemon) sessionsSnapshot() []*session {
	d.mu.Lock()
	defer d.mu.Unlock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, sess := range d.sessions {
		sessions = append(sessions, sess)
	}
	return sessions
}

func (d *Daemon) clearBarScriptsForSession(id domain.SessionID) {
	if d == nil || d.barScripts == nil {
		return
	}
	d.barScripts.mu.Lock()
	defer d.barScripts.mu.Unlock()
	delete(d.barScripts.outputs, id)
	delete(d.barScripts.lastRefresh, id)
	delete(d.barScripts.lastContext, id)
	delete(d.barScripts.running, id)
	delete(d.barScripts.pending, id)
	delete(d.barScripts.lastFailure, barScriptFailureKey{id: id, anchor: "top-right"})
	delete(d.barScripts.lastFailure, barScriptFailureKey{id: id, anchor: "bottom-right"})
}

// shouldLogBarFailure reports whether this failure differs from the last one
// logged for the same session and anchor, so a persistently broken script
// warns once instead of every refresh interval.
func (d *Daemon) shouldLogBarFailure(id domain.SessionID, anchor, signature string) bool {
	d.barScripts.mu.Lock()
	defer d.barScripts.mu.Unlock()
	d.barScripts.initLocked()
	key := barScriptFailureKey{id: id, anchor: anchor}
	if d.barScripts.lastFailure[key] == signature {
		return false
	}
	d.barScripts.lastFailure[key] = signature
	return true
}

func (d *Daemon) clearBarFailure(id domain.SessionID, anchor string) {
	d.barScripts.mu.Lock()
	defer d.barScripts.mu.Unlock()
	d.barScripts.initLocked()
	delete(d.barScripts.lastFailure, barScriptFailureKey{id: id, anchor: anchor})
}

func (d *Daemon) refreshBarScriptsIfDue(sess *session, now time.Time, force bool) bool {
	if d == nil || d.barScripts == nil || sess == nil {
		return false
	}
	baseCtx, env, ok := d.collectBarScriptContext(sess, "")
	if !ok {
		return false
	}
	if len(env) == 0 {
		env = d.baseEnv
	}
	d.barScripts.mu.Lock()
	d.barScripts.initLocked()
	if d.barScripts.lastContext[sess.id] != baseCtx {
		force = true
	}
	lastRefresh := d.barScripts.lastRefresh[sess.id]
	if d.barScripts.running[sess.id] {
		if force {
			d.barScripts.pending[sess.id] = true
		}
		d.barScripts.mu.Unlock()
		return false
	}
	if !lastRefresh.IsZero() && now.Sub(lastRefresh) < domain.MinBarInterval {
		if force {
			d.scheduleBarScriptRefreshLocked(sess, lastRefresh.Add(domain.MinBarInterval).Sub(now))
		}
		d.barScripts.mu.Unlock()
		return false
	}
	if !force && !lastRefresh.IsZero() && now.Sub(lastRefresh) < d.barScripts.cfg.interval {
		d.barScripts.mu.Unlock()
		return false
	}
	cfg := d.barScripts.cfg
	if cfg.topRight == "" && cfg.bottomRight == "" {
		d.barScripts.lastRefresh[sess.id] = now
		d.barScripts.lastContext[sess.id] = baseCtx
		current := d.barScripts.outputs[sess.id]
		changed := current.topRight != "" || current.bottomRight != ""
		delete(d.barScripts.outputs, sess.id)
		d.barScripts.mu.Unlock()
		if changed {
			d.pokeSessionRender(sess)
		}
		return false
	}
	d.barScripts.running[sess.id] = true
	d.barScripts.lastRefresh[sess.id] = now
	d.barScripts.lastContext[sess.id] = baseCtx
	runner := d.barScripts.runner
	version := d.barScripts.version
	d.barScripts.mu.Unlock()
	go d.runBarScripts(sess, runner, cfg, baseCtx, env, version)
	return true
}

func (d *Daemon) scheduleBarScriptRefreshLocked(sess *session, delay time.Duration) {
	if d.barScripts.pending[sess.id] {
		return
	}
	if d.clock == nil {
		return
	}
	if delay < 0 {
		delay = 0
	}
	d.barScripts.pending[sess.id] = true
	go func() {
		ctx := sess.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		timer := d.clock.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
		}
		d.barScripts.mu.Lock()
		delete(d.barScripts.pending, sess.id)
		d.barScripts.mu.Unlock()
		d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
	}()
}

func (d *Daemon) runBarScripts(sess *session, runner barScriptExecutor, cfg barScriptConfig, base barScriptContext, env []string, version uint64) {
	if runner == nil {
		runner = barScriptRunner{}
	}
	ctx := sess.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	top, topOK := d.runOneBarScript(ctx, runner, cfg.topRight, env, base, "top-right", sess)
	bottom, bottomOK := d.runOneBarScript(ctx, runner, cfg.bottomRight, env, base, "bottom-right", sess)
	d.barScripts.mu.Lock()
	d.barScripts.initLocked()
	if !d.barScripts.running[sess.id] || d.barScripts.version != version {
		delete(d.barScripts.running, sess.id)
		d.barScripts.mu.Unlock()
		return
	}
	current := d.barScripts.outputs[sess.id]
	changed := false
	if topOK && current.topRight != top {
		current.topRight = top
		changed = true
	}
	if bottomOK && current.bottomRight != bottom {
		current.bottomRight = bottom
		changed = true
	}
	d.barScripts.outputs[sess.id] = current
	pending := d.barScripts.pending[sess.id]
	delete(d.barScripts.running, sess.id)
	delete(d.barScripts.pending, sess.id)
	d.barScripts.mu.Unlock()
	if changed {
		d.pokeSessionRender(sess)
	}
	if pending {
		d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
	}
}

func (d *Daemon) runOneBarScript(ctx context.Context, runner barScriptExecutor, command string, env []string, base barScriptContext, anchor string, sess *session) (string, bool) {
	if command == "" {
		return "", true
	}
	base.Anchor = anchor
	out, err := runner.run(ctx, command, env, base)
	if err != nil {
		d.logBarScriptFailure(sess, base.Session, anchor, command, env, err)
		return "", false
	}
	d.clearBarFailure(sess.id, anchor)
	return out, true
}

// logBarScriptFailure logs a bar script failure. sessionName must come from a
// synchronized read of sess.name (e.g. base.Session, captured under sess.mu
// in collectBarScriptContext) since sess.name is mutable via session rename.
func (d *Daemon) logBarScriptFailure(sess *session, sessionName, anchor, command string, env []string, err error) {
	if d.log == nil {
		return
	}
	if !d.shouldLogBarFailure(sess.id, anchor, err.Error()) {
		return
	}
	attrs := []any{"anchor", anchor, "session", sessionName, "command", command, "err", err}
	var scriptErr *barScriptError
	if errors.As(err, &scriptErr) {
		attrs = append(attrs, "exit_code", scriptErr.exitCode)
		if scriptErr.stderr != "" {
			attrs = append(attrs, "stderr", scriptErr.stderr)
		}
		if scriptErr.exitCode == 127 {
			attrs = append(attrs, "hint",
				"command not found on the daemon's PATH; use an absolute path or ensure it is on the attaching client's PATH",
				"PATH", pathFromEnv(env))
		}
	}
	d.log.Warn("bar script failed; keeping last good output", attrs...)
}

func (d *Daemon) pokeSessionRender(sess *session) {
	if sess == nil {
		return
	}
	sess.mu.Lock()
	ac := sess.client
	sess.mu.Unlock()
	// A headless session can still supply a picker preview to an attachment
	// owned by another session. Its coordinator fans this wake out to those
	// preview subscribers even though no primary attachment is bound.
	d.invalidateRender(sess, ac, false, "bar_refresh.go")
}
