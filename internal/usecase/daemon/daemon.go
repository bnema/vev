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
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Scheduler debounce bounds. Idle updates use the minimum for low latency;
// sustained floods step toward the maximum to reduce frame/syscall pressure.
const (
	minDebounceInterval   = 2 * time.Millisecond
	maxDebounceInterval   = 16 * time.Millisecond
	debounceStep          = 2 * time.Millisecond
	maxSyncUpdateDuration = 500 * time.Millisecond
)

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
	empty := len(snapshot) == 0
	d.mu.Unlock()
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
		return
	}
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

// handleKill terminates the named session (if any), or all sessions, and
// closes the control connection; the resulting EOF is the client's success
// signal.
func (d *Daemon) handleKill(tr ports.Transport, f ports.Frame) {
	defer func() { _ = tr.Close() }()

	k, err := ports.UnmarshalKill(f.Payload)
	if err != nil {
		_ = tr.Send(frameError(ports.ErrInternal, "malformed kill request"))
		return
	}
	if k.All {
		d.shutdownAll(ports.ReasonServerShutdown)
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
		if version, ok := ports.PeekHelloVersion(f.Payload); ok && version != ports.ProtocolVersion {
			_ = tr.Send(frameError(ports.ErrVersionMismatch, "protocol version mismatch"))
		} else {
			_ = tr.Send(frameError(ports.ErrInternal, "malformed hello"))
		}
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
	d.runConnLoop(ac)
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
		sess, err := d.createSessionLocked(name, true, h.Cwd, sz)
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
		sess, err := d.createSessionLocked(h.Name, false, h.Cwd, sz)
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
