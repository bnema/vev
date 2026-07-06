package daemon

import (
	"context"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
)

const minBarScriptInterval = time.Second

type barScriptExecutor interface {
	run(context.Context, string, barScriptContext) (string, error)
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

type barScriptState struct {
	mu          sync.Mutex
	cfg         barScriptConfig
	runner      barScriptExecutor
	outputs     map[domain.SessionID]barScriptOutputs
	lastRefresh map[domain.SessionID]time.Time
	lastContext map[domain.SessionID]barScriptContext
	running     map[domain.SessionID]bool
	pending     map[domain.SessionID]bool
	version     uint64
}

func effectiveBarInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return 5 * time.Second
	}
	if d < minBarScriptInterval {
		return minBarScriptInterval
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

func (d *Daemon) collectBarScriptContext(sess *session, anchor string) (barScriptContext, bool) {
	ctx := barScriptContext{Anchor: anchor}
	if sess == nil {
		return ctx, false
	}
	sess.mu.Lock()
	ctx.Session = sess.name
	active := sess.active
	var tb *tab
	if active >= 0 && active < len(sess.tabs) {
		tb = sess.tabs[active]
	}
	ac := sess.client
	ctx.PaneCWD = sess.cwd
	sess.mu.Unlock()
	if ac == nil || tb == nil {
		return ctx, false
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
	return ctx, true
}

func (d *Daemon) barScriptPoller(ctx context.Context) {
	timer := d.clock.NewTimer(d.barScriptInterval())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C():
			for _, sess := range d.sessionsSnapshot() {
				d.refreshBarScriptsIfDue(sess, now, false)
			}
			timer.Reset(d.barScriptInterval())
		}
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
}

func (d *Daemon) refreshBarScriptsIfDue(sess *session, now time.Time, force bool) bool {
	if d == nil || d.barScripts == nil || sess == nil {
		return false
	}
	baseCtx, ok := d.collectBarScriptContext(sess, "")
	if !ok {
		return false
	}
	d.barScripts.mu.Lock()
	d.barScripts.initLocked()
	if d.barScripts.lastContext[sess.id] != baseCtx {
		force = true
	}
	lastRefresh := d.barScripts.lastRefresh[sess.id]
	if d.barScripts.running[sess.id] || (!lastRefresh.IsZero() && now.Sub(lastRefresh) < minBarScriptInterval) {
		if force {
			d.scheduleBarScriptRefreshLocked(sess, lastRefresh.Add(minBarScriptInterval).Sub(now))
		}
		d.barScripts.mu.Unlock()
		return false
	}
	if !force && !lastRefresh.IsZero() && now.Sub(lastRefresh) < d.barScripts.cfg.interval {
		d.barScripts.mu.Unlock()
		return false
	}
	d.barScripts.running[sess.id] = true
	d.barScripts.lastRefresh[sess.id] = now
	d.barScripts.lastContext[sess.id] = baseCtx
	cfg := d.barScripts.cfg
	runner := d.barScripts.runner
	version := d.barScripts.version
	d.barScripts.mu.Unlock()
	go d.runBarScripts(sess, runner, cfg, baseCtx, version)
	return true
}

func (d *Daemon) scheduleBarScriptRefreshLocked(sess *session, delay time.Duration) {
	if d.barScripts.pending[sess.id] {
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
		if d.clock != nil {
			timer := d.clock.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
		d.barScripts.mu.Lock()
		delete(d.barScripts.pending, sess.id)
		d.barScripts.mu.Unlock()
		now := time.Now()
		if d.clock != nil {
			now = d.clock.Now()
		}
		d.refreshBarScriptsIfDue(sess, now, true)
	}()
}

func (d *Daemon) runBarScripts(sess *session, runner barScriptExecutor, cfg barScriptConfig, base barScriptContext, version uint64) {
	if runner == nil {
		runner = barScriptRunner{baseEnv: d.baseEnv}
	}
	ctx := sess.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	top, topOK := d.runOneBarScript(ctx, runner, cfg.topRight, base, "top-right", sess.name)
	bottom, bottomOK := d.runOneBarScript(ctx, runner, cfg.bottomRight, base, "bottom-right", sess.name)
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
	delete(d.barScripts.running, sess.id)
	d.barScripts.mu.Unlock()
	if changed {
		d.pokeSessionRender(sess)
	}
}

func (d *Daemon) runOneBarScript(ctx context.Context, runner barScriptExecutor, command string, base barScriptContext, anchor, sessionName string) (string, bool) {
	if command == "" {
		return "", true
	}
	base.Anchor = anchor
	out, err := runner.run(ctx, command, base)
	if err != nil {
		if d.log != nil {
			d.log.Warn("bar script failed; keeping last good output", "anchor", anchor, "session", sessionName, "err", err)
		}
		return "", false
	}
	return out, true
}

func (d *Daemon) pokeSessionRender(sess *session) {
	if sess == nil {
		return
	}
	if tb := sess.activeTab(); tb != nil {
		tb.mu.Lock()
		p := tb.focusedPane()
		tb.mu.Unlock()
		if p != nil {
			signal(p.dirty)
		}
	}
}
