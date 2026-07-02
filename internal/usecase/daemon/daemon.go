// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-window PTY reader
// and VT screen, and the per-client debounced render scheduler.
//
// Concurrency model (one window per session for the MVP; multi-window is M3):
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
// by window.mu; the attached-client pointer by session.mu; the registry by
// Daemon.mu. When more than one is held the order is always
// Daemon.mu > session.mu, and (for the transport) attachedClient.sendMu >
// window.mu — the PTY reader only ever takes window.mu, so it never blocks on
// a slow client.
package daemon

import (
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
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

// debounceInterval is the fixed window opened on the first dirty signal: all
// further dirties arriving within it are absorbed and coalesced into a single
// render. It bounds render frequency to ~100Hz per session.
const debounceInterval = 10 * time.Millisecond

// ptyReadBufSize is the PTY reader's read buffer.
const ptyReadBufSize = 32 * 1024

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
// windows and at most one attached client.
type session struct {
	id        domain.SessionID
	name      string
	ephemeral bool

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex // guards windows, active, and client
	windows []*window
	active  int
	client  *attachedClient
}

// window is one PTY-backed screen. dirty is a cap-1 channel so bursty output
// applies back-pressure by collapsing (a full buffer means "a render is
// already pending"), never by blocking the reader.
type window struct {
	pty    ports.PTY
	mu     sync.Mutex // guards screen and every attached renderer's shadow
	screen *vt.Screen
	dirty  chan struct{}
	size   domain.Size
}

// attachedClient is a client currently attached to a session's window. rend is
// its private renderer shadow (so each client gets output minimised against
// what it has actually seen). sendMu serialises the two senders — the render
// scheduler and the connection handler — so the transport's single-writer
// contract holds and Draw→Send stays atomic (correct output ordering).
type attachedClient struct {
	tr     ports.Transport
	rend   *renderer.Renderer
	size   domain.Size
	sendMu sync.Mutex
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
	defer tr.Close()

	d.mu.Lock()
	infos := make([]ports.SessionInfo, 0, len(d.sessions))
	for _, s := range d.sessions {
		s.mu.Lock()
		attached := s.client != nil
		s.mu.Unlock()
		windows := len(s.windows)
		infos = append(infos, ports.SessionInfo{
			SessionID: string(s.id),
			Name:      s.name,
			Ephemeral: s.ephemeral,
			Windows:   uint16(windows),
			Attached:  attached,
		})
	}
	d.mu.Unlock()

	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	_ = tr.Send(frameSessions(infos))
}

// handleKill terminates the named session (if any) and closes the control
// connection; the resulting EOF is the client's success signal.
func (d *Daemon) handleKill(tr ports.Transport, f ports.Frame) {
	defer tr.Close()

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
	pty, err := d.ptys.Open(d.shell, d.shellArgs, d.childEnv(name), sz)
	if err != nil {
		return nil, fmt.Errorf("daemon: spawning session %q: %w", name, err)
	}

	id := domain.SessionID(fmt.Sprintf("sess-%d", d.nextID))
	d.nextID++

	win := &window{
		pty:    pty,
		screen: vt.NewScreen(sz.Cols, sz.Rows),
		dirty:  make(chan struct{}, 1),
		size:   sz,
	}
	sctx, cancel := context.WithCancel(d.serveCtx)
	sess := &session{
		id:        id,
		name:      name,
		ephemeral: ephemeral,
		ctx:       sctx,
		cancel:    cancel,
		windows:   []*window{win},
	}
	d.sessions[id] = sess
	d.startWindowGoroutines(sess, win)
	return sess, nil
}

func (d *Daemon) createWindow(sess *session, sz domain.Size) error {
	pty, err := d.ptys.Open(d.shell, d.shellArgs, d.childEnv(sess.name), sz)
	if err != nil {
		return fmt.Errorf("daemon: spawning window for session %q: %w", sess.name, err)
	}
	win := &window{
		pty:    pty,
		screen: vt.NewScreen(sz.Cols, sz.Rows),
		dirty:  make(chan struct{}, 1),
		size:   sz,
	}
	sess.mu.Lock()
	sess.windows = append(sess.windows, win)
	sess.active = len(sess.windows) - 1
	sess.mu.Unlock()
	d.startWindowGoroutines(sess, win)
	return nil
}

func (d *Daemon) startWindowGoroutines(sess *session, win *window) {
	d.sessWg.Add(2)
	go d.ptyReader(sess, win)
	go d.scheduler(sess, win)
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
	sess.mu.Lock()
	old := sess.client
	sess.client = ac
	sess.mu.Unlock()

	if old != nil {
		// Async + bounded: a dead or wedged old client must not stall the new
		// client's handshake.
		d.notifyDetachedAsync(old, ports.ReasonDetach)
	}
	return ac
}

// firstPaint guarantees the freshly attached client sees the full screen: if
// the window size differs from the client's it resizes first (which paints a
// full redraw), otherwise it forces a full paint against the fresh renderer.
func (d *Daemon) firstPaint(sess *session, ac *attachedClient, clientSize domain.Size) {
	win := sess.activeWindow()
	if win == nil {
		return
	}
	win.mu.Lock()
	wsz := win.size
	win.mu.Unlock()

	if clientSize.Valid() && wsz != clientSize {
		d.resize(sess, ac, clientSize)
		return
	}
	d.paint(sess, ac, true)
}

// runConnLoop is the per-connection input router: it pumps client messages
// until detach, EOF, or a transport error.
func (d *Daemon) runConnLoop(sess *session, ac *attachedClient) {
	for {
		f, err := ac.tr.Recv()
		if err != nil {
			d.clientGone(sess, ac, false)
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
		return true
	}
	return false
}

func (s *session) activeWindow() *window {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active < 0 || s.active >= len(s.windows) {
		return nil
	}
	return s.windows[s.active]
}

func (d *Daemon) handleInput(sess *session, ac *attachedClient, data []byte) {
	switch string(data) {
	case "\x1bc":
		if err := d.createWindow(sess, ac.size); err != nil {
			d.log.Error("create window failed", "err", err, "session", sess.name)
			return
		}
		d.paint(sess, ac, true)
		return
	case "\x1b1", "\x1b2", "\x1b3", "\x1b4", "\x1b5", "\x1b6", "\x1b7", "\x1b8", "\x1b9":
		idx := int(data[1] - '1')
		if sess.switchWindow(idx) {
			d.paint(sess, ac, true)
		}
		return
	case "\x1bn":
		if sess.switchRelative(1) {
			d.paint(sess, ac, true)
		}
		return
	case "\x1bp":
		if sess.switchRelative(-1) {
			d.paint(sess, ac, true)
		}
		return
	case "\x1bx":
		win := sess.activeWindow()
		if win != nil {
			d.closeWindow(sess, win, true)
		}
		return
	}
	win := sess.activeWindow()
	if win == nil {
		return
	}
	_, _ = win.pty.Write(data)
}

func (s *session) switchWindow(idx int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx < 0 || idx >= len(s.windows) || idx == s.active {
		return false
	}
	s.active = idx
	return true
}

func (s *session) switchRelative(delta int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.windows) < 2 {
		return false
	}
	s.active = (s.active + delta + len(s.windows)) % len(s.windows)
	return true
}

func (d *Daemon) closeWindow(sess *session, win *window, repaint bool) {
	sess.mu.Lock()
	idx := -1
	for i, w := range sess.windows {
		if w == win {
			idx = i
			break
		}
	}
	if idx == -1 {
		sess.mu.Unlock()
		return
	}
	if len(sess.windows) == 1 {
		sess.mu.Unlock()
		d.killSession(sess, ports.ReasonSessionKilled)
		return
	}
	sess.windows = append(sess.windows[:idx], sess.windows[idx+1:]...)
	if sess.active >= len(sess.windows) {
		sess.active = len(sess.windows) - 1
	} else if idx < sess.active {
		sess.active--
	}
	ac := sess.client
	sess.mu.Unlock()

	_ = win.pty.Close()
	if repaint && ac != nil {
		d.paint(sess, ac, true)
	}
}

// ptyReader drains child output into the VT screen and pokes the dirty channel
// (non-blocking: a full channel already means a render is pending). On any read
// error (EOF when the child exits) it kills the session.
func (d *Daemon) ptyReader(sess *session, win *window) {
	defer d.sessWg.Done()
	buf := make([]byte, ptyReadBufSize)
	for {
		n, err := win.pty.Read(buf)
		if n > 0 {
			win.mu.Lock()
			win.screen.Write(buf[:n])
			win.mu.Unlock()
			select {
			case win.dirty <- struct{}{}:
			default:
			}
		}
		if err != nil {
			d.closeWindow(sess, win, false)
			return
		}
	}
}

// scheduler debounces dirty signals: the first opens a fixed window during
// which further dirties are absorbed, then it renders exactly once.
func (d *Daemon) scheduler(sess *session, win *window) {
	defer d.sessWg.Done()
	for {
		select {
		case <-sess.ctx.Done():
			return
		case <-win.dirty:
		}

		timer := d.clock.NewTimer(debounceInterval)
	absorb:
		for {
			select {
			case <-sess.ctx.Done():
				timer.Stop()
				return
			case <-win.dirty:
				// Coalesced into the pending render.
			case <-timer.C():
				break absorb
			}
		}
		d.render(sess, win)
	}
}

// render paints the current client, or (when detached) just clears accumulated
// damage so it never grows unbounded while headless.
func (d *Daemon) render(sess *session, win *window) {
	sess.mu.Lock()
	ac := sess.client
	active := sess.active >= 0 && sess.active < len(sess.windows) && sess.windows[sess.active] == win
	sess.mu.Unlock()

	if ac == nil || !active {
		win.mu.Lock()
		win.screen.ClearDamage()
		win.mu.Unlock()
		return
	}
	d.paint(sess, ac, false)
}

// paint draws the screen for ac and sends the resulting bytes. The renderer
// shadow is read/written under win.mu; sendMu makes Draw→Send atomic per client
// so the scheduler and a concurrent resize never interleave their output.
func (d *Daemon) paint(sess *session, ac *attachedClient, reset bool) {
	win := sess.activeWindow()
	if win == nil {
		return
	}

	ac.sendMu.Lock()
	win.mu.Lock()
	if reset {
		ac.rend.Reset()
	}
	data, err := ac.rend.Draw(win.screen.Frame, win.screen.Damage())
	win.screen.ClearDamage()
	win.mu.Unlock()

	var serr error
	if err == nil && len(data) > 0 {
		serr = ac.tr.Send(frameOutput(data))
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

// resize applies a client size change in the plan's strict order: pty.Resize,
// then screen.Resize under the window lock, then a renderer reset + full
// redraw, then send.
func (d *Daemon) resize(sess *session, ac *attachedClient, sz domain.Size) {
	if !sz.Valid() {
		return
	}
	win := sess.activeWindow()
	if win == nil {
		return
	}

	if err := win.pty.Resize(sz); err != nil {
		d.log.Warn("pty resize failed", "err", err, "session", sess.name)
	}

	ac.sendMu.Lock()
	win.mu.Lock()
	win.screen.Resize(sz.Cols, sz.Rows)
	win.size = sz
	ac.rend.Reset()
	data, err := ac.rend.Draw(win.screen.Frame, win.screen.Damage())
	win.screen.ClearDamage()
	win.mu.Unlock()

	var serr error
	if err == nil && len(data) > 0 {
		serr = ac.tr.Send(frameOutput(data))
	}
	ac.sendMu.Unlock()

	if err != nil {
		d.log.Error("resize draw failed", "err", err, "session", sess.name)
		return
	}
	if serr != nil {
		d.detachOnSendError(sess, ac)
	}
}

// detachOnSendError drops a client whose transport failed. Like every other
// detach path, losing the client kills an ephemeral session (its lifetime is
// its client's); a named session keeps running headless.
func (d *Daemon) detachOnSendError(sess *session, ac *attachedClient) {
	if sess.detachIfCurrent(ac) {
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

	sess.cancel()
	sess.mu.Lock()
	windows := append([]*window(nil), sess.windows...)
	sess.mu.Unlock()
	for _, win := range windows {
		_ = win.pty.Close()
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
