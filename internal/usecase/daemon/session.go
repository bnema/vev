// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-tab PTY reader
// and VT screen, and the per-client debounced render scheduler.
//
// Concurrency model (sessions own one or more PTY-backed tabs):
//
//   - Serve runs the accept loop. Each accepted connection is handled by its
//     own goroutine (handleConn): it reads the first frame and routes it to a
//     session create/attach, a list, or a kill.
//   - Per session there are exactly two long-lived goroutines: the PTY reader
//     (drains child output into the VT screen and pokes a cap-1 dirty channel)
//     and the render scheduler (debounces dirties and paints the attached
//     client). Both are tied to the session context and unwind when the
//     session is killed (pty.Close unblocks the reader; ctx cancel stops the
//     scheduler).
//   - The daemon exits (Serve returns) when the last session is removed, or
//     when the parent context is cancelled (graceful shutdown notifies any
//     attached clients with ReasonServerShutdown).
//
// Locking: a pane's screen/scrollback and per-client renderer shadow are
// guarded by pane.mu/tab.mu as appropriate; the attached-client pointer by
// session.mu; the registry by Daemon.mu. When more than one is held the order
// is always attachedClient.sendMu > Daemon.mu > session.mu > tab.mu > pane.mu.
// The PTY reader only ever takes pane.mu, so it never blocks on a slow client.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

type session struct {
	id        domain.SessionID
	name      string
	ephemeral bool

	ctx    context.Context
	cancel context.CancelFunc

	mu                     sync.Mutex // guards tabs, active, client, and clipboard queue state
	tabs                   []*tab
	active                 int
	client                 *attachedClient
	clipboardQueue         []clipboardForward
	clipboardWorkerRunning bool
	cwd                    string
	createdAt              int64
}

// tab is a pane layout container; pane owns PTY/screen/scrollback/render scheduling state.
type tab struct {
	mu sync.Mutex // guards tree, panes, nextPaneID, size, previewClient, and pane map membership

	tree       *layout.Tree
	panes      map[layout.PaneID]*pane
	nextPaneID int
	size       domain.Size
	ctx        context.Context
	cancel     context.CancelFunc

	// previewClient tracks the one client currently previewing this tab in the picker.
	// v1 is last-writer-wins: multiple clients previewing the same tab are not supported.
	previewClient *attachedClient
	// attention and attentionAt are guarded by the owning session.mu.
	attention   bool
	attentionAt time.Time
}

// attachedClient is a client currently attached to a session's tab. rend is
// its private renderer shadow (so each client gets output minimised against
// what it has actually seen). sendMu serialises the two senders — the render
// scheduler and the connection handler — so the transport's single-writer

func (d *Daemon) createSessionLocked(name string, ephemeral bool, cwd string, sz domain.Size) (*session, error) {
	tbSize := tabSize(sz)
	pty, err := d.ptys.Open(d.shell, d.shellArgs, d.childEnv(name), cwd, tbSize)
	if err != nil {
		d.log.Warn("pty spawn failed", "err", err, "session", name, "kind", "session")
		return nil, fmt.Errorf("daemon: spawning session %q: %w", name, err)
	}

	id := domain.SessionID(fmt.Sprintf("sess-%d", d.nextID))
	d.nextID++
	createdAt := time.Now().UnixNano()

	tb := newTab(pty, tbSize)
	sctx, cancel := context.WithCancel(d.serveCtx)
	tb.ctx, tb.cancel = context.WithCancel(sctx)
	sess := &session{
		id:        id,
		name:      name,
		ephemeral: ephemeral,
		ctx:       sctx,
		cancel:    cancel,
		tabs:      []*tab{tb},
		cwd:       cwd,
		createdAt: createdAt,
	}
	if !ephemeral {
		if err := d.persist.Save(persist.Record{Name: name, Cwd: cwd, CreatedAt: createdAt, UpdatedAt: createdAt}); err != nil {
			_ = pty.Close()
			cancel()
			return nil, err
		}
		delete(d.stopped, name)
	}
	d.sessions[id] = sess
	d.log.Info("session created", "session", name, "id", id, "ephemeral", ephemeral)
	d.log.Info("tab created", "session", name, "tab", 0)
	d.startTabGoroutines(sess, tb)
	return sess, nil
}

func (d *Daemon) createSessionAndSwitch(from *session, ac *attachedClient, name string) error {
	if name == "" {
		return errors.New("name required")
	}
	sz := ac.size
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		return errors.New("daemon is shutting down")
	}
	if d.nameLiveOrStoppedLocked(name) {
		d.mu.Unlock()
		return errors.New("name already in use")
	}
	from.mu.Lock()
	cwd := from.cwd
	if from.client != ac {
		from.mu.Unlock()
		d.mu.Unlock()
		return errors.New("client detached")
	}
	from.mu.Unlock()

	newSess, err := d.createSessionLocked(name, false, cwd, sz)
	if err != nil {
		d.mu.Unlock()
		return err
	}
	from.mu.Lock()
	if from.client != ac {
		from.mu.Unlock()
		d.mu.Unlock()
		_ = d.killSession(newSess, ports.ReasonSessionKilled, true)
		return errors.New("client detached")
	}
	from.client = nil
	ac.setSession(nil)
	from.mu.Unlock()

	newSess.mu.Lock()
	newSess.client = ac
	newSess.mu.Unlock()
	ac.setSession(newSess)
	d.log.Info("client attached", "session", newSess.name, "resume", ac.resumeCapable)
	d.mu.Unlock()

	d.firstPaint(newSess, ac, sz)
	return nil
}

func (d *Daemon) createTab(sess *session, sz domain.Size) error {
	tbSize := tabSize(sz)
	sess.mu.Lock()
	name := sess.name
	cwd := sess.cwd
	client := sess.client
	sess.mu.Unlock()
	pty, err := d.ptys.Open(d.shell, d.shellArgs, d.childEnv(name), cwd, tbSize)
	if err != nil {
		d.log.Warn("pty spawn failed", "err", err, "session", name, "kind", "tab")
		return fmt.Errorf("daemon: spawning tab for session %q: %w", name, err)
	}
	tb := newTab(pty, tbSize)
	if client != nil {
		t := client.getTheme()
		tb.mu.Lock()
		p := tb.focusedPane()
		tb.mu.Unlock()
		if p != nil {
			p.mu.Lock()
			p.screen.SetDefaultColors(t.Foreground, t.Background, t.HasFG && t.HasBG)
			p.mu.Unlock()
		}
	}
	d.mu.Lock()
	if d.closing || d.sessions[sess.id] != sess || sess.ctx.Err() != nil {
		d.mu.Unlock()
		_ = pty.Close()
		return errors.New("daemon: session closed")
	}
	tb.ctx, tb.cancel = context.WithCancel(sess.ctx)
	for _, p := range tb.panes {
		p.ctx, p.cancel = context.WithCancel(tb.ctx)
	}
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, tb)
	sess.active = len(sess.tabs) - 1
	tabIndex := sess.active
	sess.mu.Unlock()
	d.log.Info("tab created", "session", name, "tab", tabIndex)
	d.startTabGoroutines(sess, tb)
	d.mu.Unlock()
	return nil
}

func newTab(pty ports.PTY, sz domain.Size) *tab {
	id := layout.PaneID("pane-1")
	p := newPane(id, pty, sz)
	return &tab{
		tree:       layout.NewTree(id),
		panes:      map[layout.PaneID]*pane{id: p},
		nextPaneID: 2,
		size:       sz,
	}
}

func (tb *tab) focusedPane() *pane {
	if tb == nil || tb.tree == nil || tb.panes == nil {
		return nil
	}
	return tb.panes[tb.tree.Focus]
}

func (tb *tab) panesSnapshot() []*pane {
	if tb == nil {
		return nil
	}
	out := make([]*pane, 0, len(tb.panes))
	for _, p := range tb.panes {
		out = append(out, p)
	}
	return out
}

func (tb *tab) closeAllPanes() {
	tb.mu.Lock()
	panes := tb.panesSnapshot()
	tb.mu.Unlock()
	for _, p := range panes {
		if p.cancel != nil {
			p.cancel()
		}
		if p.pty != nil {
			_ = p.pty.Close()
		}
	}
}

func tabSize(clientSize domain.Size) domain.Size {
	if !clientSize.Valid() {
		clientSize = defaultSize
	}
	rows := max(clientSize.Rows-2, 1)
	return domain.Size{Cols: clientSize.Cols, Rows: rows}
}

func (d *Daemon) startTabGoroutines(sess *session, tb *tab) {
	tb.mu.Lock()
	panes := tb.panesSnapshot()
	tb.mu.Unlock()
	for _, p := range panes {
		d.startPaneGoroutines(sess, tb, p)
	}
}

func (d *Daemon) startPaneGoroutines(sess *session, tb *tab, p *pane) {
	if p != nil {
		d.log.Info("pane created", "session", sess.name, "pane", p.id)
	}
	d.sessWg.Add(2)
	go d.ptyReader(sess, tb, p)
	go d.scheduler(sess, tb, p)
}

// attachClient makes ac the session's current client, displacing any prior one
// (which is notified with ReasonDetach and disconnected — its own conn handler

func (s *session) detachIfCurrent(ac *attachedClient) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == ac {
		s.client = nil
		ac.setSession(nil)
		return true
	}
	return false
}

func (s *session) activeTab() *tab {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active < 0 || s.active >= len(s.tabs) {
		return nil
	}
	return s.tabs[s.active]
}

func (s *session) switchTab(idx int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx < 0 || idx >= len(s.tabs) || idx == s.active {
		return false
	}
	s.active = idx
	return true
}

func (s *session) switchRelative(delta int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tabs) < 2 {
		return false
	}
	s.active = (s.active + delta + len(s.tabs)) % len(s.tabs)
	return true
}

func (d *Daemon) renameSession(sess *session, name string) error {
	if name == "" {
		return errors.New("name required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if taken := d.findByNameLocked(name); taken != nil && taken != sess {
		return errors.New("name already in use")
	}
	if stopped, ok := d.stopped[name]; ok && stopped.name != sess.name {
		return errors.New("name already in use")
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	oldName := sess.name
	wasEphemeral := sess.ephemeral
	createdAt := sess.createdAt
	if createdAt == 0 {
		createdAt = time.Now().UnixNano()
		sess.createdAt = createdAt
	}
	if wasEphemeral || oldName != name {
		if err := d.persist.Save(persist.Record{Name: name, Cwd: sess.cwd, CreatedAt: createdAt, UpdatedAt: time.Now().UnixNano()}); err != nil {
			return err
		}
	}
	if !wasEphemeral && oldName != name {
		if err := d.persist.Delete(oldName); err != nil {
			if cleanupErr := d.persist.Delete(name); cleanupErr != nil {
				d.log.Warn("cleaning up renamed persisted session failed", "err", cleanupErr, "session", name)
			}
			return err
		}
	}
	delete(d.stopped, oldName)
	delete(d.stopped, name)
	sess.name = name
	sess.ephemeral = false
	return nil
}

func (d *Daemon) closeTab(sess *session, tb *tab, repaint bool) {
	sess.mu.Lock()
	idx := -1
	for i, w := range sess.tabs {
		if w == tb {
			idx = i
			break
		}
	}
	if idx == -1 {
		sess.mu.Unlock()
		return
	}
	if len(sess.tabs) == 1 {
		name := sess.name
		sess.mu.Unlock()
		d.log.Info("tab closed", "session", name, "last", true)
		_ = d.killSession(sess, ports.ReasonSessionKilled, false)
		return
	}
	ringing := tb.attention
	sess.tabs = append(sess.tabs[:idx], sess.tabs[idx+1:]...)
	if sess.active >= len(sess.tabs) {
		sess.active = len(sess.tabs) - 1
	} else if idx < sess.active {
		sess.active--
	}
	ac := sess.client
	name := sess.name
	sess.mu.Unlock()
	d.log.Info("tab closed", "session", name)

	d.clearDestroyedTabPreview(tb)
	if tb.cancel != nil {
		tb.cancel()
	}
	tb.closeAllPanes()
	if repaint && ac != nil {
		d.paint(sess, ac, true)
	}
	if ringing {
		d.repaintAllAttachedClients()
	}
}

// ptyReader drains child output into the VT screen and pokes the dirty channel
// (non-blocking: a full channel already means a render is pending). On any read

func (d *Daemon) killSession(sess *session, reason uint8, purge bool) error {
	ringing := sess.anyAttention()
	sess.mu.Lock()
	isEphemeral := sess.ephemeral
	sess.mu.Unlock()
	if !purge && !isEphemeral && d.persistEnabled {
		d.refreshSessionCwd(sess)
	}
	d.mu.Lock()
	if _, ok := d.sessions[sess.id]; !ok {
		d.mu.Unlock()
		return nil
	}
	delete(d.sessions, sess.id)
	d.purgeParkedForSessionLocked(sess)
	sess.mu.Lock()
	stoppedName := sess.name
	stoppedCwd := sess.cwd
	createdAt := sess.createdAt
	ephemeral := sess.ephemeral
	sess.mu.Unlock()
	if !ephemeral {
		stopped := stoppedSession{name: stoppedName, cwd: stoppedCwd, createdAt: createdAt, purging: purge}
		d.stopped[stoppedName] = stopped
	}
	empty := len(d.sessions) == 0
	if empty {
		// Shutdown is now inevitable and irreversible (doneOnce below): stop
		// route from inserting new sessions from this instant, while we still
		// hold the same lock route checks under.
		d.closing = true
	}
	d.mu.Unlock()
	d.log.Info("session closed", "session", stoppedName, "id", sess.id, "ephemeral", ephemeral, "purge", purge, "reason", reason)

	var purgeErr error
	if !ephemeral && purge {
		if err := d.persist.Delete(stoppedName); err != nil {
			purgeErr = err
			d.log.Warn("deleting persisted session failed", "err", err, "session", stoppedName)
			d.mu.Lock()
			if stopped, ok := d.stopped[stoppedName]; ok && stopped.purging {
				stopped.purging = false
				d.stopped[stoppedName] = stopped
			}
			d.mu.Unlock()
		} else {
			d.mu.Lock()
			if stopped, ok := d.stopped[stoppedName]; ok && stopped.purging {
				delete(d.stopped, stoppedName)
			}
			d.mu.Unlock()
		}
	}

	sess.mu.Lock()
	ac := sess.client
	sess.client = nil
	sess.mu.Unlock()
	if ac != nil {
		d.unregisterPreview(ac)
		ac.setSession(nil)
	}

	sess.cancel()
	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	for _, tb := range tabs {
		d.clearDestroyedTabPreview(tb)
		tb.closeAllPanes()
	}
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
	} else if ringing {
		d.repaintAllAttachedClients()
	}

	if ac != nil {
		d.notifyDetachedAsync(ac, reason)
	}
	return purgeErr
}

// allocEphemeralNameLocked returns the lowest free decimal name. Caller holds
// d.mu.
func (d *Daemon) allocEphemeralNameLocked() string {
	used := make(map[string]struct{}, len(d.sessions)+len(d.stopped))
	for _, s := range d.sessions {
		used[s.name] = struct{}{}
	}
	for name := range d.stopped {
		used[name] = struct{}{}
	}
	for i := 0; ; i++ {
		n := strconv.Itoa(i)
		if _, taken := used[n]; !taken {
			return n
		}
	}
}

func (d *Daemon) findByNameLocked(name string) *session {
	for _, s := range d.sessions {
		if s.name == name {
			return s
		}
	}
	return nil
}

func (d *Daemon) nameLiveOrStoppedLocked(name string) bool {
	if d.findByNameLocked(name) != nil {
		return true
	}
	_, ok := d.stopped[name]
	return ok
}

func (d *Daemon) cwdSampler(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.refreshNamedSessionCwds()
		}
	}
}

func (d *Daemon) refreshNamedSessionCwds() {
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	d.mu.Unlock()
	for _, sess := range sessions {
		sess.mu.Lock()
		ephemeral := sess.ephemeral
		sess.mu.Unlock()
		if !ephemeral {
			d.refreshSessionCwd(sess)
		}
	}
}

func (d *Daemon) refreshSessionCwd(sess *session) {
	if d.procCwd == nil {
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	p := tb.focusedPane()
	tb.mu.Unlock()
	if p == nil {
		return
	}
	cwd, err := d.procCwd(p.pty.Pid())
	if err != nil || cwd == "" {
		return
	}
	d.mu.Lock()
	if d.sessions[sess.id] != sess {
		d.mu.Unlock()
		return
	}
	sess.mu.Lock()
	if sess.cwd == cwd {
		sess.mu.Unlock()
		d.mu.Unlock()
		return
	}
	sess.cwd = cwd
	name := sess.name
	ephemeral := sess.ephemeral
	sess.mu.Unlock()
	if !ephemeral {
		if err := d.persist.Touch(name, cwd, time.Now().UnixNano()); err != nil {
			d.log.Warn("touching persisted session cwd failed", "err", err, "session", name)
		}
	}
	d.mu.Unlock()
}

// childEnv builds the session child's environment: the daemon's own, with TERM
// and VEV forced to well-known values.
func (d *Daemon) childEnv(name string) []string {
	out := make([]string, 0, len(d.baseEnv)+2)
	for _, e := range d.baseEnv {
		if strings.HasPrefix(e, "TERM=") || strings.HasPrefix(e, "VEV=") {
			continue
		}
		out = append(out, e)
	}
	return append(out, "TERM=xterm-256color", "VEV="+name)
}
