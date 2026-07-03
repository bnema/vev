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
// Locking: a session's screen and per-client renderer shadow are both guarded
// by tab.mu; the attached-client pointer by session.mu; the registry by
// Daemon.mu. When more than one is held the order is always
// Daemon.mu > session.mu, and (for the transport) attachedClient.sendMu >
// tab.mu — the PTY reader only ever takes tab.mu, so it never blocks on
// a slow client.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/pkg/vt"
)

type session struct {
	id        domain.SessionID
	name      string
	ephemeral bool

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex // guards tabs, active, and client
	tabs   []*tab
	active int
	client *attachedClient
	cwd    string
}

// tab is one PTY-backed screen. dirty is a cap-1 channel so bursty output
// applies back-pressure by collapsing (a full buffer means "a render is
// already pending"), never by blocking the reader.
type tab struct {
	pty        ports.PTY
	mu         sync.Mutex // guards screen, syncGen, and every attached renderer's shadow
	screen     *vt.Screen
	scrollback *scopy.Scrollback
	dirty      chan struct{}
	flush      chan struct{}
	syncGen    uint64
	// previewClient tracks the one client currently previewing this tab in the picker.
	// v1 is last-writer-wins: multiple clients previewing the same tab are not supported.
	previewClient *attachedClient
	size          domain.Size
	ctx           context.Context
	cancel        context.CancelFunc
}

// attachedClient is a client currently attached to a session's tab. rend is
// its private renderer shadow (so each client gets output minimised against
// what it has actually seen). sendMu serialises the two senders — the render
// scheduler and the connection handler — so the transport's single-writer

func (d *Daemon) createSessionLocked(name string, ephemeral bool, cwd string, sz domain.Size) (*session, error) {
	tbSize := tabSize(sz)
	pty, err := d.ptys.Open(d.shell, d.shellArgs, d.childEnv(name), cwd, tbSize)
	if err != nil {
		return nil, fmt.Errorf("daemon: spawning session %q: %w", name, err)
	}

	id := domain.SessionID(fmt.Sprintf("sess-%d", d.nextID))
	d.nextID++

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
	}
	d.sessions[id] = sess
	d.startTabGoroutines(sess, tb)
	return sess, nil
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
		return fmt.Errorf("daemon: spawning tab for session %q: %w", name, err)
	}
	tb := newTab(pty, tbSize)
	if client != nil {
		t := client.getTheme()
		tb.screen.SetDefaultColors(t.Foreground, t.Background, t.HasFG && t.HasBG)
	}
	d.mu.Lock()
	if d.closing || d.sessions[sess.id] != sess || sess.ctx.Err() != nil {
		d.mu.Unlock()
		_ = pty.Close()
		return errors.New("daemon: session closed")
	}
	tb.ctx, tb.cancel = context.WithCancel(sess.ctx)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, tb)
	sess.active = len(sess.tabs) - 1
	sess.mu.Unlock()
	d.startTabGoroutines(sess, tb)
	d.mu.Unlock()
	return nil
}

func newTab(pty ports.PTY, sz domain.Size) *tab {
	sb := scopy.NewScrollback(defaultScrollbackRows)
	screen := vt.NewScreen(sz.Cols, sz.Rows)
	screen.OnLineEvicted = sb.Append
	return &tab{
		pty:        pty,
		screen:     screen,
		scrollback: sb,
		dirty:      make(chan struct{}, 1),
		flush:      make(chan struct{}, 1),
		size:       sz,
	}
}

func tabSize(clientSize domain.Size) domain.Size {
	if !clientSize.Valid() {
		clientSize = defaultSize
	}
	rows := max(clientSize.Rows-1, 1)
	return domain.Size{Cols: clientSize.Cols, Rows: rows}
}

func (d *Daemon) startTabGoroutines(sess *session, tb *tab) {
	d.sessWg.Add(2)
	go d.ptyReader(sess, tb)
	go d.scheduler(sess, tb)
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
	sess.mu.Lock()
	defer sess.mu.Unlock()
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
		sess.mu.Unlock()
		d.killSession(sess, ports.ReasonSessionKilled)
		return
	}
	sess.tabs = append(sess.tabs[:idx], sess.tabs[idx+1:]...)
	if sess.active >= len(sess.tabs) {
		sess.active = len(sess.tabs) - 1
	} else if idx < sess.active {
		sess.active--
	}
	ac := sess.client
	sess.mu.Unlock()

	d.clearDestroyedTabPreview(tb)
	if tb.cancel != nil {
		tb.cancel()
	}
	_ = tb.pty.Close()
	if repaint && ac != nil {
		d.paint(sess, ac, true)
	}
}

// ptyReader drains child output into the VT screen and pokes the dirty channel
// (non-blocking: a full channel already means a render is pending). On any read

func (d *Daemon) killSession(sess *session, reason uint8) {
	d.mu.Lock()
	if _, ok := d.sessions[sess.id]; !ok {
		d.mu.Unlock()
		return
	}
	delete(d.sessions, sess.id)
	empty := len(d.sessions) == 0
	if empty {
		// Shutdown is now inevitable and irreversible (doneOnce below): stop
		// route from inserting new sessions from this instant, while we still
		// hold the same lock route checks under.
		d.closing = true
	}
	d.mu.Unlock()

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
		_ = tb.pty.Close()
	}
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
	}

	if ac != nil {
		d.notifyDetachedAsync(ac, reason)
	}
}

// allocEphemeralNameLocked returns the lowest free decimal name. Caller holds
// d.mu.
func (d *Daemon) allocEphemeralNameLocked() string {
	used := make(map[string]struct{}, len(d.sessions))
	for _, s := range d.sessions {
		used[s.name] = struct{}{}
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

func (d *Daemon) nameTakenLocked(name string) bool { return d.findByNameLocked(name) != nil }

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
