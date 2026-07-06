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
	running     map[domain.SessionID]bool
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
	if s.running == nil {
		s.running = make(map[domain.SessionID]bool)
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
	if p := tb.focusedPane(); p != nil {
		ctx.Pane = p.stableID
	}
	tb.mu.Unlock()
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
	delete(d.barScripts.running, id)
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
	if d.barScripts.running[sess.id] || (!force && !d.barScripts.lastRefresh[sess.id].IsZero() && now.Sub(d.barScripts.lastRefresh[sess.id]) < d.barScripts.cfg.interval) {
		d.barScripts.mu.Unlock()
		return false
	}
	d.barScripts.running[sess.id] = true
	d.barScripts.lastRefresh[sess.id] = now
	cfg := d.barScripts.cfg
	runner := d.barScripts.runner
	d.barScripts.mu.Unlock()
	go d.runBarScripts(sess, runner, cfg, baseCtx)
	return true
}

func (d *Daemon) runBarScripts(sess *session, runner barScriptExecutor, cfg barScriptConfig, base barScriptContext) {
	if runner == nil {
		runner = barScriptRunner{baseEnv: d.baseEnv}
	}
	ctx := sess.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	top, topOK := runOneBarScript(ctx, runner, cfg.topRight, base, "top-right")
	bottom, bottomOK := runOneBarScript(ctx, runner, cfg.bottomRight, base, "bottom-right")
	d.barScripts.mu.Lock()
	d.barScripts.initLocked()
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

func runOneBarScript(ctx context.Context, runner barScriptExecutor, command string, base barScriptContext, anchor string) (string, bool) {
	if command == "" {
		return "", true
	}
	base.Anchor = anchor
	out, err := runner.run(ctx, command, base)
	return out, err == nil
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
