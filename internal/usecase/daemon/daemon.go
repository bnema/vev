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

// defaultSize is used when a client's Hello carries no valid dimensions.
var defaultSize = domain.Size{Cols: 80, Rows: 24}

// Daemon is vev's server-side multiplexer. Construct it with New and drive it
// with Serve.
type Daemon struct {
	mu       sync.Mutex
	sessions map[domain.SessionID]*session
	nextID   uint64

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

// session is a single multiplexed session. For the MVP it owns exactly one
// window and at most one attached client.
type session struct {
	id        domain.SessionID
	name      string
	ephemeral bool
	win       *window

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex // guards client
	client *attachedClient
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
		d.connWg.Add(1)
		go func() {
			defer d.connWg.Done()
			d.handleConn(tr)
		}()
	}

	// Tear down: first notify any attached clients and close their PTYs and
	// transports (killSession), THEN force-close whatever connections are still
	// parked (control conns, in-flight handshakes), then cancel the serve
	// context and wait for every goroutine.
	d.shutdownAll(ports.ReasonServerShutdown)
	d.hardCancel()
	d.serveCancel()
	d.sessWg.Wait()
	d.connWg.Wait()
	return nil
}

// shutdownAll kills every live session, snapshotting under the registry lock so
// killSession (which relocks) never runs while the lock is held.
func (d *Daemon) shutdownAll(reason uint8) {
	d.mu.Lock()
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
		infos = append(infos, ports.SessionInfo{
			SessionID: string(s.id),
			Name:      s.name,
			Ephemeral: s.ephemeral,
			Windows:   1,
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
		var pe *protoErr
		if errors.As(rerr, &pe) {
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
		win:       win,
		ctx:       sctx,
		cancel:    cancel,
	}
	d.sessions[id] = sess

	d.sessWg.Add(2)
	go d.ptyReader(sess)
	go d.scheduler(sess)
	return sess, nil
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
		_ = old.send(frameDetached(ports.ReasonDetach))
		_ = old.tr.Close()
	}
	return ac
}

// firstPaint guarantees the freshly attached client sees the full screen: if
// the window size differs from the client's it resizes first (which paints a
// full redraw), otherwise it forces a full paint against the fresh renderer.
func (d *Daemon) firstPaint(sess *session, ac *attachedClient, clientSize domain.Size) {
	win := sess.win
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
	win := sess.win
	for {
		f, err := ac.tr.Recv()
		if err != nil {
			d.clientGone(sess, ac, false)
			return
		}
		switch f.Type {
		case ports.MsgInput:
			if in, derr := ports.UnmarshalInput(f.Payload); derr == nil {
				_, _ = win.pty.Write(in.Data)
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
		_ = ac.send(frameDetached(ports.ReasonDetach))
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

// ptyReader drains child output into the VT screen and pokes the dirty channel
// (non-blocking: a full channel already means a render is pending). On any read
// error (EOF when the child exits) it kills the session.
func (d *Daemon) ptyReader(sess *session) {
	defer d.sessWg.Done()
	win := sess.win
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
			d.killSession(sess, ports.ReasonSessionKilled)
			return
		}
	}
}

// scheduler debounces dirty signals: the first opens a fixed window during
// which further dirties are absorbed, then it renders exactly once.
func (d *Daemon) scheduler(sess *session) {
	defer d.sessWg.Done()
	win := sess.win
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
		d.render(sess)
	}
}

// render paints the current client, or (when detached) just clears accumulated
// damage so it never grows unbounded while headless.
func (d *Daemon) render(sess *session) {
	win := sess.win
	sess.mu.Lock()
	ac := sess.client
	sess.mu.Unlock()

	if ac == nil {
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
	win := sess.win

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
	win := sess.win

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

// detachOnSendError drops a client whose transport failed but keeps the session
// running headless (per plan: output errors never kill the session).
func (d *Daemon) detachOnSendError(sess *session, ac *attachedClient) {
	if sess.detachIfCurrent(ac) {
		_ = ac.tr.Close()
		d.log.Warn("detached client after send error", "session", sess.name)
	}
}

// killSession removes a session and tears down its resources. It is
// idempotent: only the caller that wins the registry delete acts. When the
// registry empties it signals daemon shutdown.
func (d *Daemon) killSession(sess *session, reason uint8) {
	d.mu.Lock()
	if _, ok := d.sessions[sess.id]; !ok {
		d.mu.Unlock()
		return
	}
	delete(d.sessions, sess.id)
	empty := len(d.sessions) == 0
	d.mu.Unlock()

	sess.mu.Lock()
	ac := sess.client
	sess.client = nil
	sess.mu.Unlock()
	if ac != nil {
		_ = ac.send(frameDetached(reason))
		_ = ac.tr.Close()
	}

	sess.cancel()
	_ = sess.win.pty.Close()

	if empty {
		d.doneOnce.Do(func() { close(d.done) })
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
