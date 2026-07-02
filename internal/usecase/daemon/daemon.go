// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-tab PTY reader
// and VT screen, and the per-client debounced render scheduler.
//
// Concurrency model (one tab per session for the MVP; multi-tab is M3):
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/mouse"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

// Scheduler debounce bounds. Idle updates use the minimum for low latency;
// sustained floods step toward the maximum to reduce frame/syscall pressure.
const (
	minDebounceInterval   = 2 * time.Millisecond
	maxDebounceInterval   = 16 * time.Millisecond
	debounceStep          = 2 * time.Millisecond
	maxSyncUpdateDuration = 500 * time.Millisecond
)

var pickerModal = ui.Modal{WidthPct: 80, HeightPct: 80, MinWidth: 24, MinHeight: 8, Title: " Sessions "}

// debounceInterval is kept as a test/back-compat alias for the initial delay.
const debounceInterval = minDebounceInterval

// ptyReadBufSize is the PTY reader's read buffer.
const ptyReadBufSize = 32 * 1024

// defaultScrollbackRows is the per-tab retained history size.
const defaultScrollbackRows = 10_000

// detachNotifyTimeout bounds the best-effort Detached notification on the
// detach/kill/shutdown paths: if a wedged client (full kernel send buffer)
// blocks the write for this long, its transport is force-closed, which fails
// the in-flight send. Teardown is never gated on a client draining its socket.
const detachNotifyTimeout = time.Second

// defaultSize is used when a client's Hello carries no valid dimensions.
var defaultSize = domain.Size{Cols: 80, Rows: 24}

// Daemon is vev's server-side multiplexer. Construct it with New and drive it
// with Serve.
type Daemon struct {
	mu       sync.Mutex
	sessions map[domain.SessionID]*session
	nextID   uint64
	// closing marks that shutdown has irreversibly begun. It is set under mu,
	// atomically with the event that makes shutdown inevitable (the registry
	// emptying in killSession, or shutdownAll starting), and checked by route
	// under the same mutex — so a Hello racing shutdown can never insert a new
	// session that nobody would tear down.
	closing bool
	// notifies holds one completion channel per in-flight async Detached
	// notification (guarded by mu, pruned on insert). Channels rather than a
	// WaitGroup: notifications are spawned from arbitrary goroutines while
	// Serve may be waiting, and WaitGroup forbids Add-from-zero concurrent
	// with Wait.
	notifies []chan struct{}

	ptys      ports.PTYFactory
	clock     ports.Clock
	log       *slog.Logger
	baseEnv   []string
	shell     string
	shellArgs []string

	serveCtx    context.Context
	serveCancel context.CancelFunc

	// hardCtx force-closes connection transports on shutdown, but only after
	// shutdownAll has delivered graceful Detached notices — keeping it separate
	// from serveCtx so a parent-context cancel never races the notice.
	hardCtx    context.Context
	hardCancel context.CancelFunc

	done     chan struct{}
	doneOnce sync.Once

	sessWg sync.WaitGroup // PTY reader + scheduler goroutines
	connWg sync.WaitGroup // per-connection handler goroutines
}

// session is a single multiplexed session. It owns one or more full-screen
// tabs and at most one attached client.
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
// contract holds and Draw→Send stays atomic (correct output ordering).
type attachedClient struct {
	tr            ports.Transport
	rend          *renderer.Renderer
	size          domain.Size
	keys          *keys.Router
	sessMu        sync.Mutex
	sess          *session
	copyMu        sync.Mutex
	copyMode      *scopy.Mode
	copyPending   []byte
	pickerMu      sync.Mutex
	picker        *picker.Model
	pickerPreview *tab
	pickerPending []byte
	mouseScan     mouse.Scanner
	lastCursor    cursorOut
	sendMu        sync.Mutex
}

type cursorOut struct {
	valid    bool
	hidden   bool
	row      int
	col      int
	style    int
	hasStyle bool
}

func (ac *attachedClient) currentSession() *session {
	ac.sessMu.Lock()
	defer ac.sessMu.Unlock()
	return ac.sess
}

func (ac *attachedClient) setSession(sess *session) {
	ac.sessMu.Lock()
	defer ac.sessMu.Unlock()
	ac.sess = sess
}

func (ac *attachedClient) copyModeActive() bool {
	ac.copyMu.Lock()
	defer ac.copyMu.Unlock()
	return ac.copyMode != nil
}

func (ac *attachedClient) pickerActive() bool {
	ac.pickerMu.Lock()
	defer ac.pickerMu.Unlock()
	return ac.picker != nil
}

// send serialises a frame onto the client's transport.
func (ac *attachedClient) send(f ports.Frame) error {
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	return ac.tr.Send(f)
}

// boundedSend sends f to ac with a deadline watchdog: if the send (including
// waiting on sendMu behind a wedged paint) does not complete within
// detachNotifyTimeout, the transport is force-closed, failing the in-flight
// write. Detach/kill/shutdown paths use this so they are never gated on a
// client that has stopped draining its socket.
func (d *Daemon) boundedSend(ac *attachedClient, f ports.Frame) {
	timer := d.clock.NewTimer(detachNotifyTimeout)
	sent := make(chan struct{})
	go func() {
		select {
		case <-timer.C():
			_ = ac.tr.Close()
		case <-sent:
		}
	}()
	_ = ac.send(f)
	timer.Stop()
	close(sent)
}

// notifyDetachedAsync delivers a best-effort Detached notice off the caller's
// goroutine and then closes the transport. Session teardown (killSession) must
// complete regardless of client state, so the notice is both asynchronous and
// deadline-bounded; Serve waits for pending notices before force-closing
// connections so a graceful notice is not raced by the hard close.
func (d *Daemon) notifyDetachedAsync(ac *attachedClient, reason uint8) {
	done := make(chan struct{})
	d.mu.Lock()
	// Prune completed entries so the slice stays bounded by the number of
	// notifications actually in flight.
	kept := d.notifies[:0]
	for _, c := range d.notifies {
		select {
		case <-c:
		default:
			kept = append(kept, c)
		}
	}
	d.notifies = append(kept, done)
	d.mu.Unlock()

	go func() {
		defer close(done)
		d.boundedSend(ac, frameDetached(reason))
		_ = ac.tr.Close()
	}()
}

// waitNotifies blocks until every Detached notification in flight at the time
// of the call has completed. Each is deadline-bounded (boundedSend), so this
// wait is bounded too.
func (d *Daemon) waitNotifies() {
	d.mu.Lock()
	snapshot := make([]chan struct{}, len(d.notifies))
	copy(snapshot, d.notifies)
	d.mu.Unlock()
	for _, c := range snapshot {
		<-c
	}
}

// Option customises a Daemon at construction.
type Option func(*Daemon)

// WithShell overrides the command (and its args) each session spawns. The
// default is $SHELL (or /bin/sh) with no arguments; tests use this to run a
// deterministic program.
func WithShell(cmd string, args []string) Option {
	return func(d *Daemon) {
		d.shell = cmd
		d.shellArgs = args
	}
}

// New constructs a Daemon. ptys spawns PTY-backed children, clock drives the
// render debounce, and log receives diagnostics (defaults to slog.Default).
func New(ptys ports.PTYFactory, clock ports.Clock, log *slog.Logger, opts ...Option) *Daemon {
	if log == nil {
		log = slog.Default()
	}
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}
	d := &Daemon{
		sessions: make(map[domain.SessionID]*session),
		ptys:     ptys,
		clock:    clock,
		log:      log,
		baseEnv:  os.Environ(),
		shell:    shell,
		done:     make(chan struct{}),
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Serve runs the accept loop over l, owning it for the loop's lifetime. It
// returns when the last session is removed or ctx is cancelled; on the latter
// path attached clients are detached with ReasonServerShutdown.
func (d *Daemon) Serve(ctx context.Context, l ports.Listener) error {
	d.serveCtx, d.serveCancel = context.WithCancel(ctx)
	defer d.serveCancel()
	d.hardCtx, d.hardCancel = context.WithCancel(context.Background())
	defer d.hardCancel()

	// Break the accept loop when either the parent context is cancelled or the
	// registry drains to empty: both close the listener, which fails Accept.
	go func() {
		select {
		case <-d.serveCtx.Done():
		case <-d.done:
		}
		_ = l.Close()
	}()

	for {
		tr, err := l.Accept()
		if err != nil {
			break
		}
		d.connWg.Go(func() {
			d.handleConn(tr)
		})
	}

	// Tear down. shutdownAll marks the daemon closing (route now rejects any
	// racing Hello) and kills every session: PTYs are closed and contexts
	// cancelled unconditionally, with the Detached notices sent asynchronously
	// under a deadline. Wait for those notices (bounded — a wedged client's
	// transport is force-closed by the deadline) before hard-closing the
	// remaining parked connections, so graceful notices are never raced by the
	// force-close. Finally drain the conn handlers, run one defensive sweep in
	// case any ordering ever leaves a session behind, and join the session
	// goroutines (readers unblock via pty.Close, schedulers via ctx cancel).
	d.shutdownAll(ports.ReasonServerShutdown)
	d.waitNotifies()
	d.hardCancel()
	d.serveCancel()
	d.connWg.Wait()
	d.shutdownAll(ports.ReasonServerShutdown)
	d.sessWg.Wait()
	d.waitNotifies()
	return nil
}

// shutdownAll marks the daemon closing and kills every live session. Setting
// closing under the same lock as the snapshot guarantees no session can be
// inserted after the snapshot: route rejects once closing is set, and both run
// under d.mu. killSession (which relocks) runs after the lock is released.
func (d *Daemon) shutdownAll(reason uint8) {
	d.mu.Lock()
	d.closing = true
	snapshot := make([]*session, 0, len(d.sessions))
	for _, s := range d.sessions {
		snapshot = append(snapshot, s)
	}
	d.mu.Unlock()
	for _, s := range snapshot {
		d.killSession(s, reason)
	}
}

// handleConn reads the first frame off a fresh connection and routes it. A
// context watcher closes the transport on serve-context cancel so a handler
// parked in Recv (mid-handshake or between input frames) unwinds on shutdown.
func (d *Daemon) handleConn(tr ports.Transport) {
	stop := context.AfterFunc(d.hardCtx, func() { _ = tr.Close() })
	defer stop()

	first, err := tr.Recv()
	if err != nil {
		_ = tr.Close()
		return
	}
	switch first.Type {
	case ports.MsgList:
		d.handleList(tr)
	case ports.MsgKill:
		d.handleKill(tr, first)
	case ports.MsgHello:
		d.handleHello(tr, first)
	default:
		_ = tr.Send(frameError(ports.ErrInternal, "expected hello"))
		_ = tr.Close()
	}
}

// handleList replies with the current session listing and closes the (one-shot
// control) connection.
func (d *Daemon) handleList(tr ports.Transport) {
	defer func() { _ = tr.Close() }()

	d.mu.Lock()
	infos := make([]ports.SessionInfo, 0, len(d.sessions))
	for _, s := range d.sessions {
		s.mu.Lock()
		info := ports.SessionInfo{
			SessionID: string(s.id),
			Name:      s.name,
			Ephemeral: s.ephemeral,
			Tabs:      uint16(len(s.tabs)),
			Attached:  s.client != nil,
		}
		s.mu.Unlock()
		infos = append(infos, info)
	}
	d.mu.Unlock()

	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	_ = tr.Send(frameSessions(infos))
}

// handleKill terminates the named session (if any) and closes the control
// connection; the resulting EOF is the client's success signal.
func (d *Daemon) handleKill(tr ports.Transport, f ports.Frame) {
	defer func() { _ = tr.Close() }()

	k, err := ports.UnmarshalKill(f.Payload)
	if err != nil {
		_ = tr.Send(frameError(ports.ErrInternal, "malformed kill request"))
		return
	}

	d.mu.Lock()
	target := d.findByNameLocked(k.Name)
	d.mu.Unlock()

	if target == nil {
		_ = tr.Send(frameError(ports.ErrNoSuchSession, "no such session: "+k.Name))
		return
	}
	d.killSession(target, ports.ReasonSessionKilled)
}

// handleHello runs the attach handshake: version check, intent routing,
// Welcome, guaranteed first paint, then the per-connection input loop.
func (d *Daemon) handleHello(tr ports.Transport, f ports.Frame) {
	h, err := ports.UnmarshalHello(f.Payload)
	if err != nil {
		_ = tr.Send(frameError(ports.ErrInternal, "malformed hello"))
		_ = tr.Close()
		return
	}
	if h.Version != ports.ProtocolVersion {
		_ = tr.Send(frameError(ports.ErrVersionMismatch, "protocol version mismatch"))
		_ = tr.Close()
		return
	}

	sess, ac, rerr := d.route(h, tr)
	if rerr != nil {
		if pe, ok := errors.AsType[*protoErr](rerr); ok {
			_ = tr.Send(frameError(pe.code, pe.text))
		} else {
			_ = tr.Send(frameError(ports.ErrInternal, rerr.Error()))
		}
		_ = tr.Close()
		return
	}

	if err := ac.send(frameWelcome(sess)); err != nil {
		d.clientGone(sess, ac, false)
		return
	}
	d.firstPaint(sess, ac, h.Size)
	d.runConnLoop(sess, ac)
	_ = tr.Close()
}

// protoErr is a session-level rejection carrying a wire ErrorMsg code.
type protoErr struct {
	code uint16
	text string
}

func (e *protoErr) Error() string { return e.text }

// route resolves a Hello to a session and a freshly attached client, creating
// the session for ephemeral/new intents.
func (d *Daemon) route(h ports.Hello, tr ports.Transport) (*session, *attachedClient, error) {
	sz := h.Size
	if !sz.Valid() {
		sz = defaultSize
	}

	d.mu.Lock()
	// Shutdown/create interlock: once shutdown has begun (last session removed,
	// or shutdownAll started) no new session may be created and no attach may
	// proceed — the teardown snapshot has already been (or is being) taken, so
	// anything inserted now would leak its PTY and hang Serve.
	if d.closing {
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
	}
	switch h.Intent {
	case ports.IntentEphemeral:
		name := d.allocEphemeralNameLocked()
		sess, err := d.createSessionLocked(name, true, sz)
		d.mu.Unlock()
		if err != nil {
			return nil, nil, err
		}
		return sess, d.attachClient(sess, tr, sz), nil

	case ports.IntentNew:
		if h.Name == "" {
			d.mu.Unlock()
			return nil, nil, &protoErr{ports.ErrInternal, "empty session name"}
		}
		if d.nameTakenLocked(h.Name) {
			d.mu.Unlock()
			return nil, nil, &protoErr{ports.ErrNameTaken, "session name already in use: " + h.Name}
		}
		sess, err := d.createSessionLocked(h.Name, false, sz)
		d.mu.Unlock()
		if err != nil {
			return nil, nil, err
		}
		return sess, d.attachClient(sess, tr, sz), nil

	case ports.IntentAttach:
		sess := d.findByNameLocked(h.Name)
		d.mu.Unlock()
		if sess == nil {
			return nil, nil, &protoErr{ports.ErrNoSuchSession, "no such session: " + h.Name}
		}
		return sess, d.attachClient(sess, tr, sz), nil

	default:
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrInternal, "unknown intent"}
	}
}

// createSessionLocked spawns a PTY-backed session and starts its reader and
// scheduler goroutines. Caller holds d.mu.
func (d *Daemon) createSessionLocked(name string, ephemeral bool, sz domain.Size) (*session, error) {
	tbSize := tabSize(sz)
	pty, err := d.ptys.Open(d.shell, d.shellArgs, d.childEnv(name), tbSize)
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
	}
	d.sessions[id] = sess
	d.startTabGoroutines(sess, tb)
	return sess, nil
}

func (d *Daemon) createTab(sess *session, sz domain.Size) error {
	tbSize := tabSize(sz)
	pty, err := d.ptys.Open(d.shell, d.shellArgs, d.childEnv(sess.name), tbSize)
	if err != nil {
		return fmt.Errorf("daemon: spawning tab for session %q: %w", sess.name, err)
	}
	tb := newTab(pty, tbSize)
	tb.ctx, tb.cancel = context.WithCancel(sess.ctx)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, tb)
	sess.active = len(sess.tabs) - 1
	sess.mu.Unlock()
	d.startTabGoroutines(sess, tb)
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
// then unwinds without killing the session, since it is no longer current).
func (d *Daemon) attachClient(sess *session, tr ports.Transport, sz domain.Size) *attachedClient {
	ac := &attachedClient{
		tr:   tr,
		rend: renderer.New(renderer.Capabilities{}),
		size: sz,
	}
	ac.setSession(sess)
	ac.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac})
	sess.mu.Lock()
	old := sess.client
	sess.client = ac
	sess.mu.Unlock()

	if old != nil {
		d.unregisterPreview(old)
		old.setSession(nil)
		// Async + bounded: a dead or wedged old client must not stall the new
		// client's handshake.
		d.notifyDetachedAsync(old, ports.ReasonDetach)
	}
	return ac
}

// firstPaint guarantees the freshly attached client sees the full screen: if
// the tab size differs from the client's it resizes first (which paints a
// full redraw), otherwise it forces a full paint against the fresh renderer.
func (d *Daemon) firstPaint(sess *session, ac *attachedClient, clientSize domain.Size) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	wsz := tb.size
	tb.mu.Unlock()

	if clientSize.Valid() && wsz != tabSize(clientSize) {
		d.resize(sess, ac, clientSize)
		return
	}
	d.paint(sess, ac, true)
}

// runConnLoop is the per-connection input router: it pumps client messages
// until detach, EOF, or a transport error.
func (d *Daemon) runConnLoop(sess *session, ac *attachedClient) {
	for {
		sess = ac.currentSession()
		if sess == nil {
			return
		}
		f, err := ac.tr.Recv()
		if err != nil {
			d.clientGone(sess, ac, false)
			return
		}
		sess = ac.currentSession()
		if sess == nil {
			return
		}
		switch f.Type {
		case ports.MsgInput:
			if in, derr := ports.UnmarshalInput(f.Payload); derr == nil {
				d.handleInput(sess, ac, in.Data)
			}
		case ports.MsgResize:
			if rz, derr := ports.UnmarshalResize(f.Payload); derr == nil {
				d.resize(sess, ac, rz.Size)
			}
		case ports.MsgDetach:
			d.clientGone(sess, ac, true)
			return
		case ports.MsgPing:
			_ = ac.send(framePong())
		default:
			// Unknown/out-of-band client messages are ignored so a newer
			// client can add message types without breaking an older daemon.
		}
	}
}

// clientGone detaches ac if it is still the session's current client. An
// ephemeral session dies with its client; a named one survives headless. When
// explicit (a MsgDetach), the client is acked with ReasonDetach.
func (d *Daemon) clientGone(sess *session, ac *attachedClient, explicit bool) {
	if !sess.detachIfCurrent(ac) {
		return // already displaced by a newer client; nothing to do
	}
	d.unregisterPreview(ac)
	if explicit {
		// Synchronous so the ack is delivered before the transport closes
		// (the client is actively awaiting it), but deadline-bounded so a
		// wedged client cannot pin this conn handler and hang Serve's
		// connWg.Wait.
		d.boundedSend(ac, frameDetached(ports.ReasonDetach))
	}
	_ = ac.tr.Close()
	if sess.ephemeral {
		d.killSession(sess, ports.ReasonSessionKilled)
	}
}

// detachIfCurrent clears the client iff ac is the current one, reporting
// whether it acted.
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

func (d *Daemon) handleInput(sess *session, ac *attachedClient, data []byte) {
	ac.mouseScan.Scan(data,
		func(ev mouse.Event) { d.handleMouse(sess, ac, ev) },
		func(b []byte) {
			if ac.pickerActive() {
				d.handlePickerInput(sess, ac, b)
				return
			}
			if ac.copyModeActive() {
				d.handleCopyInput(sess, ac, b)
				return
			}
			ac.keys.Route(b)
		},
	)
}

func (d *Daemon) handleMouse(sess *session, ac *attachedClient, ev mouse.Event) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	if ac.copyModeActive() {
		switch ev.Button {
		case mouse.WheelUp:
			d.copyWheel(sess, ac, -3)
		case mouse.WheelDown:
			d.copyWheel(sess, ac, 3)
		}
		return
	}

	tb.mu.Lock()
	childRows := tb.screen.Frame.Height
	mouseMode, mouseSGR := tb.screen.MouseMode()
	altScreen := tb.screen.AltScreenActive()
	tb.mu.Unlock()

	if mouseMode != 0 {
		if !mouseSGR || ev.Row >= childRows {
			return
		}
		daemonKeyHandler{d: d, ac: ac}.Forward(ev.Raw)
		return
	}

	switch ev.Button {
	case mouse.WheelUp:
		if altScreen {
			daemonKeyHandler{d: d, ac: ac}.Forward([]byte("\x1b[A\x1b[A\x1b[A"))
			return
		}
		d.enterCopyMode(sess, ac)
		d.copyWheel(sess, ac, -3)
	case mouse.WheelDown:
		if altScreen {
			daemonKeyHandler{d: d, ac: ac}.Forward([]byte("\x1b[B\x1b[B\x1b[B"))
		}
	}
}

func (d *Daemon) copyWheel(sess *session, ac *attachedClient, delta int) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	tb.mu.Lock()
	ac.copyMu.Lock()
	if ac.copyMode == nil {
		ac.copyMu.Unlock()
		tb.mu.Unlock()
		return
	}
	snap := scopy.NewSnapshot(tb.scrollback, tb.screen.Frame)
	if delta > 0 && ac.copyMode.AtBottom(snap) {
		ac.copyMode = nil
		ac.copyMu.Unlock()
		tb.mu.Unlock()
		d.paint(sess, ac, true)
		return
	}
	ac.copyMode.Move(snap, delta)
	exit := delta > 0 && ac.copyMode.AtBottom(snap)
	if exit {
		ac.copyMode = nil
	}
	ac.copyMu.Unlock()
	tb.mu.Unlock()

	d.paint(sess, ac, true)
}

func (d *Daemon) enterCopyMode(sess *session, ac *attachedClient) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	snap := scopy.NewSnapshot(tb.scrollback, tb.screen.Frame)
	tb.mu.Unlock()
	ac.copyMu.Lock()
	ac.copyMode = scopy.NewMode(snap)
	ac.copyMu.Unlock()
	d.paint(sess, ac, true)
}

func (d *Daemon) handleCopyInput(sess *session, ac *attachedClient, data []byte) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	tb.mu.Lock()
	ac.copyMu.Lock()
	if ac.copyMode == nil {
		ac.copyPending = nil
		ac.copyMu.Unlock()
		tb.mu.Unlock()
		return
	}
	if len(ac.copyPending) > 0 {
		combined := make([]byte, 0, len(ac.copyPending)+len(data))
		combined = append(combined, ac.copyPending...)
		combined = append(combined, data...)
		data = combined
		ac.copyPending = nil
	}
	snap := scopy.NewSnapshot(tb.scrollback, tb.screen.Frame)
	changed := false
	copyOut := false
	exit := false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'j':
			ac.copyMode.Move(snap, 1)
			changed = true
		case 'k':
			ac.copyMode.Move(snap, -1)
			changed = true
		case 'g':
			ac.copyMode.Top(snap)
			changed = true
		case 'G':
			ac.copyMode.Bottom(snap)
			changed = true
		case ' ':
			ac.copyMode.ToggleSelection()
			changed = true
		case '\r', '\n', 'y':
			copyOut = true
			exit = true
		case 'q', 0x03, 0x1b:
			if data[i] == 0x1b {
				tail := data[i:]
				consumed, ok := routeCopyEscape(ac.copyMode, snap, tail)
				if ok {
					i += consumed - 1
					changed = true
					continue
				}
				if isCopyEscapePrefix(tail) {
					ac.copyPending = append(ac.copyPending[:0], tail...)
					break
				}
			}
			exit = true
		}
	}
	text := ""
	if copyOut {
		text = ac.copyMode.SelectedText(snap)
	}
	if exit {
		ac.copyMode = nil
	}
	ac.copyMu.Unlock()
	tb.mu.Unlock()

	if copyOut && text != "" {
		for _, chunk := range scopy.OSC52(text) {
			if err := ac.send(frameOutput(chunk)); err != nil {
				d.detachOnSendError(sess, ac)
				return
			}
		}
	}
	if exit {
		d.paint(sess, ac, true)
		return
	}
	if changed {
		d.paint(sess, ac, true)
	}
}

func routeCopyEscape(m *scopy.Mode, snap scopy.Snapshot, data []byte) (int, bool) {
	if len(data) >= 3 && data[1] == '[' {
		switch data[2] {
		case 'A':
			m.Move(snap, -1)
			return 3, true
		case 'B':
			m.Move(snap, 1)
			return 3, true
		}
	}
	if len(data) >= 4 && data[1] == '[' && data[3] == '~' {
		switch data[2] {
		case '5':
			m.Page(snap, -1)
			return 4, true
		case '6':
			m.Page(snap, 1)
			return 4, true
		}
	}
	return 0, false
}

func isCopyEscapePrefix(data []byte) bool {
	return len(data) == 2 && data[0] == 0x1b && data[1] == '[' ||
		len(data) == 3 && data[0] == 0x1b && data[1] == '[' && (data[2] == '5' || data[2] == '6')
}

func (d *Daemon) enterPicker(sess *session, ac *attachedClient) {
	views, curTab := d.pickerViews(sess)
	ac.pickerMu.Lock()
	ac.picker = picker.New(views, sess.id, curTab)
	ac.pickerPending = nil
	ac.pickerPreview = nil
	ac.pickerMu.Unlock()
	d.registerPreviewForSelection(ac)
	d.paint(sess, ac, true)
}

func (d *Daemon) pickerViews(cur *session) ([]picker.SessionView, int) {
	d.mu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, s := range d.sessions {
		sessions = append(sessions, s)
	}
	d.mu.Unlock()
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].name < sessions[j].name })

	views := make([]picker.SessionView, 0, len(sessions))
	curTab := 0
	for _, s := range sessions {
		s.mu.Lock()
		view := picker.SessionView{ID: s.id, Name: s.name, Active: s.active, Tabs: make([]string, len(s.tabs))}
		for i := range s.tabs {
			view.Tabs[i] = strconv.Itoa(i + 1)
		}
		if s == cur {
			curTab = s.active
		}
		s.mu.Unlock()
		views = append(views, view)
	}
	return views, curTab
}

func (d *Daemon) handlePickerInput(sess *session, ac *attachedClient, data []byte) {
	ac.pickerMu.Lock()
	if ac.picker == nil {
		ac.pickerPending = nil
		ac.pickerMu.Unlock()
		return
	}
	if len(ac.pickerPending) > 0 {
		combined := make([]byte, 0, len(ac.pickerPending)+len(data))
		combined = append(combined, ac.pickerPending...)
		combined = append(combined, data...)
		data = combined
		ac.pickerPending = nil
	}
	changed := false
	exit := false
	switchTarget := false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'j':
			ac.picker.Down()
			changed = true
		case 'k':
			ac.picker.Up()
			changed = true
		case '\r', '\n':
			switchTarget = true
			exit = true
		case 'q', 0x03, 0x1b:
			if data[i] == 0x1b {
				tail := data[i:]
				consumed, ok := routePickerEscape(ac.picker, tail)
				if ok {
					i += consumed - 1
					changed = true
					continue
				}
				if isPickerEscapePrefix(tail) {
					ac.pickerPending = append(ac.pickerPending[:0], tail...)
					break
				}
			}
			exit = true
		}
	}
	var target picker.Target
	var ok bool
	if switchTarget {
		target, ok = ac.picker.Selected()
	}
	ac.pickerMu.Unlock()

	if changed {
		d.registerPreviewForSelection(ac)
	}
	if exit {
		d.closePicker(ac)
	}
	if switchTarget && ok {
		d.switchToTarget(sess, ac, target)
		return
	}
	if exit || changed {
		d.paint(sess, ac, true)
	}
}

func routePickerEscape(m *picker.Model, data []byte) (int, bool) {
	if len(data) >= 3 && data[1] == '[' {
		switch data[2] {
		case 'A':
			m.Up()
			return 3, true
		case 'B':
			m.Down()
			return 3, true
		}
	}
	return 0, false
}

func isPickerEscapePrefix(data []byte) bool {
	return len(data) == 2 && data[0] == 0x1b && data[1] == '['
}

func (d *Daemon) registerPreviewForSelection(ac *attachedClient) {
	ac.pickerMu.Lock()
	var target picker.Target
	var ok bool
	if ac.picker != nil {
		target, ok = ac.picker.Selected()
	}
	ac.pickerMu.Unlock()

	var next *tab
	if ok {
		next = d.tabByTarget(target)
	}

	ac.pickerMu.Lock()
	old := ac.pickerPreview
	if ac.picker == nil {
		next = nil
	}
	ac.pickerPreview = next
	ac.pickerMu.Unlock()

	if old != nil && old != next {
		old.mu.Lock()
		if old.previewClient == ac {
			old.previewClient = nil
		}
		old.mu.Unlock()
	}
	if next != nil {
		next.mu.Lock()
		next.previewClient = ac
		next.mu.Unlock()

		ac.pickerMu.Lock()
		keep := ac.pickerPreview == next
		ac.pickerMu.Unlock()
		if !keep {
			next.mu.Lock()
			if next.previewClient == ac {
				next.previewClient = nil
			}
			next.mu.Unlock()
		}
	}
}

func (d *Daemon) unregisterPreview(ac *attachedClient) {
	ac.pickerMu.Lock()
	old := ac.pickerPreview
	ac.pickerPreview = nil
	ac.pickerMu.Unlock()

	if old != nil {
		old.mu.Lock()
		if old.previewClient == ac {
			old.previewClient = nil
		}
		old.mu.Unlock()
	}
}

func (d *Daemon) closePicker(ac *attachedClient) {
	ac.pickerMu.Lock()
	ac.picker = nil
	ac.pickerPending = nil
	ac.pickerMu.Unlock()
	d.unregisterPreview(ac)
}

func (d *Daemon) tabByTarget(target picker.Target) *tab {
	d.mu.Lock()
	sess := d.sessions[target.Session]
	d.mu.Unlock()
	if sess == nil {
		return nil
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if target.TabIndex < 0 || target.TabIndex >= len(sess.tabs) {
		return nil
	}
	return sess.tabs[target.TabIndex]
}

func (d *Daemon) sessionByID(id domain.SessionID) *session {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions[id]
}

func (d *Daemon) switchToTarget(sess *session, ac *attachedClient, target picker.Target) {
	targetSess := d.sessionByID(target.Session)
	if targetSess == nil {
		d.paint(sess, ac, true)
		return
	}
	if targetSess == sess {
		if sess.switchTab(target.TabIndex) {
			d.paint(sess, ac, true)
		} else {
			d.paint(sess, ac, true)
		}
		return
	}
	if !sess.detachIfCurrent(ac) {
		return
	}
	targetSess.stealClient(d, ac, target.TabIndex)
	ac.setSession(targetSess)
	d.firstPaint(targetSess, ac, ac.size)
}

func (s *session) stealClient(d *Daemon, ac *attachedClient, idx int) {
	s.mu.Lock()
	old := s.client
	s.client = ac
	if idx >= 0 && idx < len(s.tabs) {
		s.active = idx
	}
	s.mu.Unlock()
	if old != nil && old != ac {
		d.unregisterPreview(old)
		old.setSession(nil)
		d.notifyDetachedAsync(old, ports.ReasonDetach)
	}
}

type daemonKeyHandler struct {
	d  *Daemon
	ac *attachedClient
}

func (h daemonKeyHandler) Forward(data []byte) {
	sess := h.ac.currentSession()
	if sess == nil {
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	if _, err := tb.pty.Write(data); err != nil {
		h.d.log.Error("pty write failed", "err", err, "session", sess.name)
	}
}

func (h daemonKeyHandler) Action(action keys.Action) {
	sess := h.ac.currentSession()
	if sess == nil {
		return
	}
	switch action {
	case keys.ActionCreateTab:
		if err := h.d.createTab(sess, h.ac.size); err != nil {
			h.d.log.Error("create tab failed", "err", err, "session", sess.name)
			return
		}
		h.d.paint(sess, h.ac, true)
	case keys.ActionNextTab:
		if sess.switchRelative(1) {
			h.d.paint(sess, h.ac, true)
		}
	case keys.ActionPreviousTab:
		if sess.switchRelative(-1) {
			h.d.paint(sess, h.ac, true)
		}
	case keys.ActionDetach:
		h.d.clientGone(sess, h.ac, true)
	case keys.ActionCloseTab:
		tb := sess.activeTab()
		if tb != nil {
			h.d.closeTab(sess, tb, true)
		}
	case keys.ActionCopyMode:
		h.d.enterCopyMode(sess, h.ac)
	case keys.ActionRenameSession:
		sess.promoteEphemeral()
		h.d.paint(sess, h.ac, true)
	case keys.ActionOpenPicker:
		h.d.enterPicker(sess, h.ac)
	case keys.ActionSwitchTab1, keys.ActionSwitchTab2, keys.ActionSwitchTab3,
		keys.ActionSwitchTab4, keys.ActionSwitchTab5, keys.ActionSwitchTab6,
		keys.ActionSwitchTab7, keys.ActionSwitchTab8, keys.ActionSwitchTab9:
		idx := int(action - keys.ActionSwitchTab1)
		if sess.switchTab(idx) {
			h.d.paint(sess, h.ac, true)
		}
	}
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

// promoteEphemeral is M3's promptless Alt+r behavior: without a naming UI yet,
// the current ephemeral numeric name is kept and the session is made persistent.
func (s *session) promoteEphemeral() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ephemeral = false
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
// error (EOF when the child exits) it kills the session.
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (d *Daemon) ptyReader(sess *session, tb *tab) {
	defer d.sessWg.Done()
	buf := make([]byte, ptyReadBufSize)
	var resp []byte
	tb.mu.Lock()
	tb.screen.OnResponse = func(b []byte) { resp = append(resp, b...) }
	tb.mu.Unlock()
	for {
		n, err := tb.pty.Read(buf)
		if n > 0 {
			data := buf[:n]
			tb.mu.Lock()
			wasSyncing := tb.screen.SyncUpdateActive()
			tb.screen.Write(data)
			isSyncing := tb.screen.SyncUpdateActive()
			tb.mu.Unlock()
			if len(resp) > 0 {
				if _, writeErr := tb.pty.Write(resp); writeErr != nil {
					d.log.Warn("pty response write failed", "err", writeErr, "session", sess.name)
				}
				resp = resp[:0]
			}
			if wasSyncing != isSyncing {
				tb.mu.Lock()
				tb.syncGen++
				gen := tb.syncGen
				tb.mu.Unlock()
				if isSyncing {
					go d.syncWatchdog(tb, gen)
				}
			}
			if (wasSyncing && !isSyncing) || (!isSyncing && syncUpdateEndIn(data)) {
				signal(tb.flush)
				continue
			}
			if isSyncing {
				continue
			}
			signal(tb.dirty)
		}
		if err != nil {
			d.closeTab(sess, tb, true)
			return
		}
	}
}

// scheduler debounces dirty signals. The first dirty opens a short tab;
// sustained floods progressively widen that tab, while isolated updates
// return to the minimum delay for interactive latency.
func (d *Daemon) scheduler(sess *session, tb *tab) {
	defer d.sessWg.Done()
	delay := minDebounceInterval
	lastRender := d.clock.Now()
outer:
	for {
		select {
		case <-sess.ctx.Done():
			return
		case <-tabDone(tb):
			return
		case <-tb.flush:
			d.render(sess, tb)
			lastRender = d.clock.Now()
			continue
		case <-tb.dirty:
			if d.clock.Now().Sub(lastRender) >= maxDebounceInterval {
				delay = minDebounceInterval
			}
		}

		coalesced := 0
		timer := d.clock.NewTimer(delay)
	absorb:
		for {
			select {
			case <-sess.ctx.Done():
				timer.Stop()
				return
			case <-tabDone(tb):
				timer.Stop()
				return
			case <-tb.flush:
				if !timer.Stop() {
					select {
					case <-timer.C():
					default:
					}
				}
				d.render(sess, tb)
				lastRender = d.clock.Now()
				continue outer
			case <-tb.dirty:
				coalesced++
			case <-timer.C():
				break absorb
			}
		}
		delay = nextDebounceDelay(delay, coalesced)
		d.render(sess, tb)
		lastRender = d.clock.Now()
	}
}

func nextDebounceDelay(delay time.Duration, coalesced int) time.Duration {
	if coalesced == 0 {
		return minDebounceInterval
	}
	if delay >= maxDebounceInterval {
		return maxDebounceInterval
	}
	delay += debounceStep
	if delay > maxDebounceInterval {
		return maxDebounceInterval
	}
	return delay
}

func (d *Daemon) syncWatchdog(tb *tab, gen uint64) {
	timer := d.clock.NewTimer(maxSyncUpdateDuration)
	select {
	case <-tabDone(tb):
		timer.Stop()
		return
	case <-timer.C():
	}

	tb.mu.Lock()
	if tb.syncGen != gen || !tb.screen.SyncUpdateActive() {
		tb.mu.Unlock()
		return
	}
	tb.screen.ForceSyncEnd()
	tb.mu.Unlock()
	signal(tb.flush)
}

func tabDone(tb *tab) <-chan struct{} {
	if tb.ctx == nil {
		return nil
	}
	return tb.ctx.Done()
}

func syncUpdateEndIn(data []byte) bool {
	return bytes.Contains(data, []byte("\x1b[?2026l"))
}

// render paints the current client, or (when detached) just clears accumulated
// damage so it never grows unbounded while headless.
func (d *Daemon) render(sess *session, tb *tab) {
	tb.mu.Lock()
	if tb.screen.SyncUpdateActive() {
		tb.mu.Unlock()
		return
	}
	tb.mu.Unlock()

	tb.mu.Lock()
	previewer := tb.previewClient
	tb.mu.Unlock()

	sess.mu.Lock()
	ac := sess.client
	active := sess.active >= 0 && sess.active < len(sess.tabs) && sess.tabs[sess.active] == tb
	sess.mu.Unlock()

	if previewer != nil {
		if previewSess := previewer.currentSession(); previewSess != nil {
			d.paint(previewSess, previewer, false)
		}
		if ac != nil && active && ac != previewer {
			d.paint(sess, ac, false)
			return
		}
		tb.mu.Lock()
		tb.screen.ClearDamage()
		tb.mu.Unlock()
		return
	}

	if ac == nil || !active {
		tb.mu.Lock()
		tb.screen.ClearDamage()
		tb.mu.Unlock()
		return
	}
	d.paint(sess, ac, false)
}

// paint draws the composed client frame (active tab plus status bar) and
// sends the resulting bytes. The renderer shadow is reset on explicit invalidations
// such as switch/create/close/rename/resize so the repaint is complete.
func (d *Daemon) paint(sess *session, ac *attachedClient, reset bool) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	ac.sendMu.Lock()
	ac.copyMu.Lock()
	copyActive := ac.copyMode != nil
	copyMode := ac.copyMode
	ac.copyMu.Unlock()
	ac.pickerMu.Lock()
	pickerActive := ac.picker != nil
	pickerModel := ac.picker
	previewTab := ac.pickerPreview
	preview := snapshotPickerPreview(previewTab)

	tb.mu.Lock()
	if reset || copyActive || pickerActive {
		ac.rend.Reset()
	}
	if reset || pickerActive {
		ac.lastCursor.valid = false
	}
	frame, damage := composeClientFrame(sess, tb, reset)
	if copyActive {
		frame, damage = composeCopyClientFrame(copyMode, tb)
	}
	if pickerActive {
		if previewTab == tb {
			preview = pickerPreviewFromLockedTab(tb)
		}
		frame, damage = composePickerClientFrame(pickerModel, preview, frame)
	}
	ac.pickerMu.Unlock()
	desiredCursor := desiredCursorOut(tb.screen, copyActive || pickerActive)
	data, err := ac.rend.Draw(frame, damage)
	var cursorTail []byte
	if err == nil {
		cursorTail = ac.encodeCursorTail(desiredCursor, len(data) > 0)
	}
	tb.screen.ClearDamage()
	tb.mu.Unlock()

	var serr error
	if err == nil {
		data = append(data, cursorTail...)
		if len(data) > 0 {
			serr = ac.tr.Send(frameOutput(data))
		}
	}
	ac.sendMu.Unlock()

	if err != nil {
		d.log.Error("render draw failed", "err", err, "session", sess.name)
		return
	}
	if serr != nil {
		d.detachOnSendError(sess, ac)
	}
}

// resize applies a client size change to every tab using rows-1 for the PTY
// and VT screen, then resets the renderer shadow and repaints the composed
// client-sized frame (including the status row).
func desiredCursorOut(s *vt.Screen, copyActive bool) cursorOut {
	if copyActive || !s.CursorVisible() {
		return cursorOut{hidden: true}
	}
	style, ok := s.CursorStyle()
	if !ok {
		style = 5
	}
	return cursorOut{row: s.CursorRow(), col: s.CursorCol(), style: style, hasStyle: true}
}

func (ac *attachedClient) encodeCursorTail(desired cursorOut, force bool) []byte {
	changed := force || !ac.lastCursor.valid || ac.lastCursor.hidden != desired.hidden || ac.lastCursor.row != desired.row || ac.lastCursor.col != desired.col || ac.lastCursor.style != desired.style || ac.lastCursor.hasStyle != desired.hasStyle
	if !changed {
		return nil
	}
	prev := ac.lastCursor
	ac.lastCursor = desired
	ac.lastCursor.valid = true
	if desired.hidden {
		return []byte("\x1b[?25l")
	}
	var b []byte
	b = append(b, "\x1b["...)
	b = strconv.AppendInt(b, int64(desired.row+1), 10)
	b = append(b, ';')
	b = strconv.AppendInt(b, int64(desired.col+1), 10)
	b = append(b, 'H')
	if !prev.valid || prev.hidden || prev.style != desired.style || prev.hasStyle != desired.hasStyle {
		b = append(b, "\x1b["...)
		b = strconv.AppendInt(b, int64(desired.style), 10)
		b = append(b, " q"...)
	}
	b = append(b, "\x1b[?25h"...)
	return b
}

func composeClientFrame(sess *session, tb *tab, full bool) (renderer.Frame, []renderer.Damage) {
	width, screenRows := tb.screen.Frame.Width, tb.screen.Frame.Height
	frame := renderer.NewFrame(width, screenRows+1)
	for y := range screenRows {
		copy(frame.Row(y), tb.screen.Frame.Row(y))
	}
	drawStatus(frame.Row(screenRows), sess)
	if full {
		return frame, []renderer.Damage{renderer.FullRedraw()}
	}
	damage := append([]renderer.Damage(nil), tb.screen.Damage()...)
	damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: screenRows, Width: width, Height: 1})
	return frame, damage
}

func composeCopyClientFrame(mode *scopy.Mode, tb *tab) (renderer.Frame, []renderer.Damage) {
	snap := scopy.NewSnapshot(tb.scrollback, tb.screen.Frame)
	frame := mode.Render(snap)
	return frame, []renderer.Damage{renderer.FullRedraw()}
}

func composePickerClientFrame(model *picker.Model, preview picker.Preview, base renderer.Frame) (renderer.Frame, []renderer.Damage) {
	inner := pickerModal.Composite(base, renderer.DefaultStyle())
	modalFrame := model.Render(domain.Size{Cols: inner.Width, Rows: inner.Height}, preview)
	for y := range min(inner.Height, modalFrame.Height) {
		for x := range min(inner.Width, modalFrame.Width) {
			base.Set(inner.X+x, inner.Y+y, modalFrame.At(x, y))
		}
	}
	return base, []renderer.Damage{renderer.FullRedraw()}
}

func snapshotPickerPreview(tb *tab) picker.Preview {
	if tb == nil {
		return picker.Preview{}
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return pickerPreviewFromLockedTab(tb)
}

func pickerPreviewFromLockedTab(tb *tab) picker.Preview {
	rows := make([][]renderer.Cell, tb.screen.Frame.Height)
	for y := range rows {
		rows[y] = append([]renderer.Cell(nil), tb.screen.Frame.Row(y)...)
	}
	return picker.Preview{Rows: rows, Width: tb.screen.Frame.Width, Height: tb.screen.Frame.Height}
}

func drawStatus(row []renderer.Cell, sess *session) {
	for i := range row {
		row[i] = renderer.BlankCell()
	}
	status := sess.statusSegments()
	x := 0
	writeStatusText(row, &x, " "+status.session+" ", renderer.DefaultStyle())
	for _, w := range status.tabs {
		style := renderer.DefaultStyle()
		if w.active {
			style.Inverse = true
		}
		writeStatusText(row, &x, " "+w.name+" ", style)
	}
}

type statusSnapshot struct {
	session string
	tabs    []statusTab
}

type statusTab struct {
	name   string
	active bool
}

func (s *session) statusSegments() statusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := statusSnapshot{session: s.name, tabs: make([]statusTab, len(s.tabs))}
	for i := range s.tabs {
		name := strconv.Itoa(i + 1)
		snap.tabs[i] = statusTab{name: name, active: i == s.active}
	}
	return snap
}

func writeStatusText(row []renderer.Cell, x *int, text string, style renderer.Style) {
	for _, r := range text {
		if *x >= len(row) {
			return
		}
		row[*x] = renderer.Cell{Rune: r, Style: style}
		(*x)++
	}
}

func (d *Daemon) resize(sess *session, ac *attachedClient, sz domain.Size) {
	if !sz.Valid() {
		return
	}
	tbSize := tabSize(sz)
	ac.size = sz

	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	if len(tabs) == 0 {
		return
	}

	for _, tb := range tabs {
		if err := tb.pty.Resize(tbSize); err != nil {
			d.log.Warn("pty resize failed", "err", err, "session", sess.name)
		}
		tb.mu.Lock()
		tb.screen.Resize(tbSize.Cols, tbSize.Rows)
		tb.size = tbSize
		tb.mu.Unlock()
	}
	d.paint(sess, ac, true)
}

// detachOnSendError drops a client whose transport failed. Like every other
// detach path, losing the client kills an ephemeral session (its lifetime is
// its client's); a named session keeps running headless.
func (d *Daemon) detachOnSendError(sess *session, ac *attachedClient) {
	if sess.detachIfCurrent(ac) {
		d.unregisterPreview(ac)
		_ = ac.tr.Close()
		d.log.Warn("detached client after send error", "session", sess.name)
		if sess.ephemeral {
			d.killSession(sess, ports.ReasonSessionKilled)
		}
	}
}

// killSession removes a session and tears down its resources. It is
// idempotent: only the caller that wins the registry delete acts. When the
// registry empties it marks the daemon closing (atomically with the
// empty-check, under d.mu) and signals shutdown.
//
// Teardown ordering matters: context cancel, pty.Close, and the done signal
// run first and unconditionally — never gated behind a client send. The
// Detached notice is best-effort, asynchronous, and deadline-bounded.
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

func frameWelcome(s *session) ports.Frame {
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{
		SessionID:   string(s.id),
		SessionName: s.name,
		Ephemeral:   s.ephemeral,
	})}
}

func frameError(code uint16, text string) ports.Frame {
	return ports.Frame{Type: ports.MsgError, Payload: ports.MarshalErrorMsg(ports.ErrorMsg{Code: code, Text: text})}
}

func frameOutput(b []byte) ports.Frame {
	return ports.Frame{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{Data: b})}
}

func frameDetached(reason uint8) ports.Frame {
	return ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: reason})}
}

func framePong() ports.Frame {
	return ports.Frame{Type: ports.MsgPong, Payload: ports.MarshalPong(ports.Pong{})}
}

func frameSessions(infos []ports.SessionInfo) ports.Frame {
	return ports.Frame{Type: ports.MsgSessions, Payload: ports.MarshalSessions(ports.Sessions{Sessions: infos})}
}
