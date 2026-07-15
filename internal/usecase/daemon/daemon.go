// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

// Scheduler debounce bounds. Idle updates use the minimum for low latency;
// sustained floods step toward the maximum to reduce frame/syscall pressure.
const (
	minDebounceInterval   = 2 * time.Millisecond
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

// maxUnackedOutputStates caps how many output states may be in flight (sent but
// not yet acked by the client) before paint defers rather than composing
// another diff. It bounds the daemon's paint rate to the client's ack rate, so
// heavy output degrades to lower fps on a slow link instead of overflowing the
// transport. It must stay well under the UDP proxy's 32-frame client window so
// the proxy's reliable queue never fills from painting alone.
const maxUnackedOutputStates = 8

// normalizeOutputWindow bounds the untrusted Hello value. Zero deliberately
// means the legacy/default window, so malformed or absent values remain safe.
func normalizeOutputWindow(window uint8) uint8 {
	if window == 0 || window > maxUnackedOutputStates {
		return maxUnackedOutputStates
	}
	return window
}

const defaultResumeParkGrace = 15 * time.Minute

// defaultSize is used when a client's Hello carries no valid dimensions.
var defaultSize = domain.Size{Cols: 80, Rows: 24}

type Daemon struct {
	mu       sync.Mutex
	sessions map[domain.SessionID]*session
	stopped  map[string]stoppedSession
	nextID   uint64
	mruSeq   atomic.Uint64
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
	parked   map[uint64]*parkedAttachment

	attnMu    sync.Mutex
	animFrame int
	animWake  chan struct{}

	paletteRecentMu sync.Mutex
	paletteRecent   []string
	// beforeRecentSessionHandoff is a deterministic test seam for the narrow
	// interval between JRS validation and its committed hand-off.
	beforeRecentSessionHandoff func()
	ptys                       ports.PTYFactory
	clock                      ports.Clock
	log                        *slog.Logger
	runtimeObserver            ports.RuntimeObserver
	baseEnv                    []string
	shell                      string
	shellArgs                  []string
	shellOverride              bool
	persist                    *persist.Persister
	persistEnabled             bool
	snaps                      ports.SnapshotStore
	snapsEnabled               bool
	snapshotMarshal            func(snapcodec.Session) ([]byte, error)
	snapshotJobs               chan *snapshotCapture
	snapshotWorkerMu           sync.Mutex
	snapshotWorkerID           uint64
	snapshotWorkerCtx          context.Context
	snapshotWorkerCancel       context.CancelFunc
	snapshotWorkerDone         chan struct{}
	snapshotWorkerFlush        chan struct{}
	snapshotWorkerFinalWake    chan struct{}
	// snapshotFinalJobs coalesces terminal captures by session when the bounded
	// regular queue is full. It retains at most snapshotFinalQueueCapacity named
	// sessions, each with only its newest terminal state while the worker blocks.
	snapshotFinalJobs       map[*session]*snapshotCapture
	snapshotFinalOrder      []*session
	snapshotWorkerClosing   bool
	snapshotWorkerInFlight  *snapshotCapture
	restoreDone             chan struct{}
	restoreOnce             sync.Once
	procCwd                 func(int) (string, error)
	procComm                func(int) (string, error)
	procArgv                func(int) ([]string, error)
	procGroupArgv           func(int, int) ([]string, error)
	dirOrHome               func(string) string
	bindings                atomic.Pointer[keys.Bindings]
	codeOverrides           atomic.Pointer[map[string]string]
	restoreProcessAllowlist atomic.Pointer[map[string]struct{}]
	floatingConfig          atomic.Pointer[domain.FloatingConfig]
	copyConfig              atomic.Pointer[domain.CopyConfig]
	paletteConfig           atomic.Pointer[domain.PaletteConfig]
	tabsConfig              atomic.Pointer[domain.TabsConfig]
	themeMode               atomic.Uint32
	barScripts              *barScriptState
	resumeParkGrace         time.Duration
	// tempDir overrides os.TempDir() for clipboard-image-transfer writes
	// (see clipboard.go); empty means use os.TempDir().
	tempDir string

	serveCtx    context.Context
	serveCancel context.CancelFunc

	// hardCtx force-closes connection transports on shutdown, but only after
	// shutdownAll has delivered graceful Detached notices — keeping it separate
	// from serveCtx so a parent-context cancel never races the notice.
	hardCtx    context.Context
	hardCancel context.CancelFunc

	done     chan struct{}
	doneOnce sync.Once

	// sessWg owns attention animation, bar-script polling, CWD sampling,
	// snapshot save/restore, and floating-session launch goroutines.
	sessWg sync.WaitGroup
	connWg sync.WaitGroup // per-connection handler goroutines
	// attachmentCleanupWg owns replacement transport closes and retired render
	// worker joins. Only connection handlers add work; Serve waits for those
	// handlers before joining this group, so no Add races its terminal Wait.
	attachmentCleanupWg sync.WaitGroup
}

type parkedAttachment struct {
	sess     *session
	ac       *attachedClient
	timer    ports.Timer
	done     chan struct{}
	doneOnce sync.Once
}

// session is a single multiplexed session. It owns one or more full-screen

type stoppedSession struct {
	name        string
	cwd         string
	createdAt   int64
	lastUsedSeq uint64
	tabNames    []string
	purging     bool
}

func (s stoppedSession) same(other stoppedSession) bool {
	return s.name == other.name &&
		s.cwd == other.cwd &&
		s.createdAt == other.createdAt &&
		s.lastUsedSeq == other.lastUsedSeq &&
		s.purging == other.purging &&
		slices.Equal(s.tabNames, other.tabNames)
}

type Option func(*Daemon)

// WithRuntimeObserver accepts only a composition-root serialized observer.
// The application owns its lifecycle; the daemon never creates or closes a
// second reporting worker around it.
func WithRuntimeObserver(observer ports.SerializedRuntimeObserver) Option {
	return func(d *Daemon) { d.runtimeObserver = observer }
}

// WithShell overrides the command (and its args) each session spawns. The
// default is $SHELL (or /bin/sh) with no arguments; tests use this to run a
// deterministic program.
func WithShell(cmd string, args []string) Option {
	return func(d *Daemon) {
		d.shell = cmd
		d.shellArgs = args
		d.shellOverride = true
	}
}

// WithStore enables persisted named session metadata. A nil store keeps the
// daemon in no-op persistence mode.
func WithStore(store ports.Store) Option {
	return func(d *Daemon) {
		d.persist = persist.New(store)
		d.persistEnabled = store != nil
	}
}

// WithSnapshotStore enables durable named session snapshots. A nil store keeps
// the daemon in no-op snapshot mode.
func WithSnapshotStore(store ports.SnapshotStore) Option {
	return func(d *Daemon) {
		d.snaps = store
		d.snapsEnabled = store != nil
	}
}

// WithCwdReader overrides the process cwd reader used for persistence tests.
func WithCwdReader(fn func(int) (string, error)) Option {
	return func(d *Daemon) {
		if fn != nil {
			d.procCwd = fn
		}
	}
}

// WithProcessInspector installs the platform process-inspection implementation.
func WithProcessInspector(ins ports.ProcessInspector) Option {
	return func(d *Daemon) {
		if ins == nil {
			return
		}
		d.procCwd = ins.Cwd
		d.procComm = ins.Comm
		d.procArgv = ins.Argv
		d.procGroupArgv = ins.GroupArgv
	}
}

// WithDirOrHome installs path fallback behavior from the application layer.
func WithDirOrHome(fn func(string) string) Option {
	return func(d *Daemon) {
		if fn != nil {
			d.dirOrHome = fn
		}
	}
}

// WithTempDir overrides the directory clipboard-image-transfer writes temp
// files into (production default: os.TempDir()); tests use this with
// t.TempDir() so writes are isolated and auto-cleaned.
func WithTempDir(dir string) Option {
	return func(d *Daemon) {
		d.tempDir = dir
	}
}

// WithBarScriptCommandRunner installs the shell command runner used by bar scripts.
func WithBarScriptCommandRunner(runner ports.ShellCommandRunner) Option {
	return func(d *Daemon) {
		if runner != nil {
			d.barScripts.runner = barScriptRunner{runner: runner, baseEnv: d.baseEnv}
		}
	}
}

// WithResumeParkGrace overrides how long detached resume-capable clients stay
// parked for reconnection. Non-positive durations keep the default.
func WithResumeParkGrace(grace time.Duration) Option {
	return func(d *Daemon) {
		if grace > 0 {
			d.resumeParkGrace = grace
		}
	}
}

// WithConfig applies the initial user configuration.
func WithConfig(cfg domain.Config) Option {
	return func(d *Daemon) {
		d.ApplyConfig(cfg)
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
		sessions:        make(map[domain.SessionID]*session),
		stopped:         make(map[string]stoppedSession),
		parked:          make(map[uint64]*parkedAttachment),
		ptys:            ptys,
		clock:           clock,
		log:             log,
		baseEnv:         os.Environ(),
		shell:           shell,
		persist:         persist.New(nil),
		dirOrHome:       dirOrHome,
		done:            make(chan struct{}),
		restoreDone:     make(chan struct{}),
		animWake:        make(chan struct{}, 1),
		snapshotMarshal: snapcodec.Marshal,
		snapshotJobs:    make(chan *snapshotCapture, snapshotQueueCapacity),
		resumeParkGrace: defaultResumeParkGrace,
		barScripts: &barScriptState{
			cfg:         barConfigFromDomain(domain.Defaults().Bar),
			outputs:     make(map[domain.SessionID]barScriptOutputs),
			lastRefresh: make(map[domain.SessionID]time.Time),
			lastContext: make(map[domain.SessionID]barScriptContext),
			running:     make(map[domain.SessionID]bool),
		},
	}
	defaults := domain.Defaults()
	defaultFloating := defaults.Floating
	d.floatingConfig.Store(&defaultFloating)
	defaultCopy := defaults.Copy
	d.copyConfig.Store(&defaultCopy)
	defaultPalette := defaults.Palette
	d.paletteConfig.Store(&defaultPalette)
	for _, o := range opts {
		o(d)
	}
	if d.persist == nil {
		d.persist = persist.New(nil)
	}
	if d.dirOrHome == nil {
		d.dirOrHome = dirOrHome
	}
	if d.bindings.Load() == nil {
		d.bindings.Store(keys.DefaultBindings())
	}
	if d.codeOverrides.Load() == nil {
		empty := map[string]string{}
		d.codeOverrides.Store(&empty)
	}
	if d.restoreProcessAllowlist.Load() == nil {
		allow := buildRestoreProcessAllowlist(domain.DefaultSnapshotRestoreProcesses())
		d.restoreProcessAllowlist.Store(&allow)
	}
	if records, err := d.persist.LoadAll(); err != nil {
		d.log.Warn("loading persisted sessions failed", "err", err)
	} else {
		var maxSeq uint64
		for _, r := range records {
			d.stopped[r.Name] = stoppedSession{name: r.Name, cwd: r.Cwd, createdAt: r.CreatedAt, lastUsedSeq: r.LastUsedSeq, tabNames: r.TabNames}
			if r.LastUsedSeq > maxSeq {
				maxSeq = r.LastUsedSeq
			}
		}
		d.mruSeq.Store(maxSeq)
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

	d.sessWg.Go(func() {
		d.attentionAnimator(d.serveCtx)
	})
	d.sessWg.Go(func() {
		d.barScriptPoller(d.serveCtx)
	})
	if d.persistEnabled && d.procCwd != nil {
		d.sessWg.Go(func() {
			d.cwdSampler(d.serveCtx)
		})
	}
	if d.snapsEnabled {
		d.startSnapshotEncodeWorker()
		d.sessWg.Go(func() {
			d.snapshotSaver(d.serveCtx)
		})
		d.sessWg.Go(func() {
			d.restoreSnapshots(d.serveCtx)
		})
	} else {
		d.closeRestoreDone()
	}

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
			if d.serveCtx.Err() != nil {
				d.log.Info("accept loop exiting", "err", err, "reason", "context canceled")
			} else {
				d.log.Warn("accept loop exiting", "err", err)
			}
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
	// goroutines (readers unblock via pty.Close, coordinators via ctx cancel).
	d.shutdownAll(ports.ReasonServerShutdown)
	d.waitNotifies()
	d.hardCancel()
	d.serveCancel()
	// Wait for handlers before flushing snapshots: a handler that already
	// entered killSession may still submit its terminal capture after the
	// registry snapshot has been removed.
	d.connWg.Wait()
	d.attachmentCleanupWg.Wait()
	d.stopSnapshotEncodeWorker()
	d.shutdownAll(ports.ReasonServerShutdown)
	d.sessWg.Wait()
	d.waitNotifies()
	if err := d.persist.Close(); err != nil {
		d.log.Warn("closing session persister failed", "err", err)
	}
	return nil
}

// shutdownAll marks the daemon closing and kills every live session. Setting
// closing under the same lock as the snapshot guarantees no session can be
// inserted after the snapshot: route rejects once closing is set, and both run
// under d.mu. killSession (which relocks) runs after the lock is released.
func (d *Daemon) shutdownAll(reason uint8) {
	d.mu.Lock()
	d.closing = true
	for token, parked := range d.parked {
		d.removeParkedLocked(token, parked)
	}
	snapshot := d.sessionsSnapshotLocked()
	empty := len(snapshot) == 0
	d.mu.Unlock()
	d.log.Info("graceful shutdown begin", "reason", reason, "sessions", len(snapshot))
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
		return
	}
	for _, s := range snapshot {
		if err := d.killSession(s, reason, false); err != nil {
			d.log.Error("closing session with unpersisted terminal state", "err", err)
		}
	}
}

func (d *Daemon) sessionsSnapshotLocked() []*session {
	snapshot := make([]*session, 0, len(d.sessions))
	for _, s := range d.sessions {
		snapshot = append(snapshot, s)
	}
	return snapshot
}

// handleConn reads the first frame off a fresh connection and routes it. A
// context watcher closes the transport on serve-context cancel so a handler
// parked in Recv (mid-handshake or between input frames) unwinds on shutdown.
func (d *Daemon) handleConn(tr ports.Transport) {
	stop := context.AfterFunc(d.hardCtx, func() { _ = tr.Close() })
	defer stop()

	first, err := tr.Recv()
	if err != nil {
		d.log.Warn("connection closed before hello", "err", err)
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
		d.log.Warn("hello rejected", "err", "expected hello", "type", first.Type)
		_ = tr.Send(frameError(ports.ErrInternal, "expected hello"))
		_ = tr.Close()
	}
}

// handleList replies with the current session listing and closes the (one-shot
// control) connection.
func (d *Daemon) handleList(tr ports.Transport) {
	defer func() { _ = tr.Close() }()

	d.mu.Lock()
	infos := make([]ports.SessionInfo, 0, len(d.sessions)+len(d.stopped))
	liveNames := make(map[string]struct{}, len(d.sessions))
	for _, s := range d.sessions {
		s.mu.Lock()
		info := ports.SessionInfo{
			SessionID: string(s.id),
			Name:      s.name,
			Ephemeral: s.ephemeral,
			Tabs:      uint16(len(s.tabs)),
			Attached:  s.client != nil,
		}
		liveNames[s.name] = struct{}{}
		s.mu.Unlock()
		infos = append(infos, info)
	}
	for name, stopped := range d.stopped {
		if stopped.purging {
			continue
		}
		if _, live := liveNames[name]; live {
			continue
		}
		infos = append(infos, ports.SessionInfo{Name: name, Stopped: true})
	}
	d.mu.Unlock()

	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	_ = tr.Send(frameSessions(infos))
}

// handleKill terminates the requested live session or stopped named session,
// or all sessions, and closes the control connection; the resulting EOF is the
// client's success signal.
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
	if target == nil {
		if stopped, ok := d.stopped[k.Name]; ok {
			d.mu.Unlock()
			if d.snapsEnabled && d.snaps != nil {
				if err := d.snaps.Delete(k.Name); err != nil {
					d.log.Warn("deleting stopped session snapshot failed", "err", err, "session", k.Name)
					_ = tr.Send(frameError(ports.ErrInternal, "deleting stopped session snapshot failed"))
					return
				}
			}
			if err := d.persist.Delete(k.Name); err != nil {
				d.log.Warn("deleting persisted stopped session failed", "err", err, "session", k.Name)
				_ = tr.Send(frameError(ports.ErrInternal, "deleting persisted stopped session failed"))
				return
			}
			d.mu.Lock()
			if cur, ok := d.stopped[k.Name]; ok && cur.same(stopped) {
				delete(d.stopped, k.Name)
			}
			d.mu.Unlock()
			return
		}
	}
	d.mu.Unlock()

	if target == nil {
		_ = tr.Send(frameError(ports.ErrNoSuchSession, "no such session: "+k.Name))
		return
	}
	if err := d.killSession(target, ports.ReasonSessionKilled, true); err != nil {
		_ = tr.Send(frameError(ports.ErrInternal, "deleting persisted session failed"))
	}
}

// handleHello runs the attach handshake: version check, intent routing,
// Welcome, guaranteed first paint, then the per-connection input loop.
func (d *Daemon) handleHello(tr ports.Transport, f ports.Frame) {
	h, err := ports.UnmarshalHello(f.Payload)
	if err != nil {
		if version, ok := ports.PeekHelloVersion(f.Payload); ok && version != ports.ProtocolVersion {
			d.log.Warn("hello rejected", "err", "protocol version mismatch", "version", version, "expected", ports.ProtocolVersion)
			_ = tr.Send(frameError(ports.ErrVersionMismatch, "protocol version mismatch"))
		} else {
			d.log.Warn("hello rejected", "err", err)
			_ = tr.Send(frameError(ports.ErrInternal, "malformed hello"))
		}
		_ = tr.Close()
		return
	}
	if h.Version != ports.ProtocolVersion {
		d.log.Warn("hello rejected", "err", "protocol version mismatch", "version", h.Version, "expected", ports.ProtocolVersion, "intent", h.Intent, "session", h.Name)
		_ = tr.Send(frameError(ports.ErrVersionMismatch, "protocol version mismatch"))
		_ = tr.Close()
		return
	}

	sess, ac, rerr := d.route(h, tr)
	if rerr != nil {
		d.log.Warn("hello rejected", "err", rerr, "intent", h.Intent, "session", h.Name)
		if pe, ok := errors.AsType[*protoErr](rerr); ok {
			_ = tr.Send(frameError(pe.code, pe.text))
		} else {
			_ = tr.Send(frameError(ports.ErrInternal, rerr.Error()))
		}
		_ = tr.Close()
		return
	}

	rc := sess.renderCoordinator()
	lease := (*attachmentLease)(nil)
	if rc != nil {
		lease = rc.attachmentLease(ac)
	}
	expected := ac.transportSnapshot()
	if lease == nil || expected.transport != tr {
		d.clientGone(sess, ac, tr, false)
		return
	}
	if err := ac.sendExpectedTransport(expected, frameWelcome(sess, ac)); err != nil {
		d.clientGone(sess, ac, tr, false)
		return
	}
	if !rc.markAttachmentReady(lease) {
		// The attachment was displaced or detached while Welcome was in flight;
		// never let this stale handshake emit an Output frame.
		d.clientGone(sess, ac, tr, false)
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

// finishAttach completes an attachment prepared while d.mu is held. It
// publishes the terminal state before releasing d.mu, then queues replacement
// teardown so obsolete worker joins never delay the new handshake.
func (d *Daemon) finishAttach(sess *session, tr ports.Transport, sz domain.Size, term terminalEnv, h ports.Hello) *attachedClient {
	// Session state is the sole source for future PTY children. Update it before
	// publishing the attachment; existing PTYs keep their original environment.
	sess.mu.Lock()
	sess.env = copyEnvironment(h.Env)
	sess.terminal = term
	sess.mu.Unlock()
	ac, old, cleanup := d.attachClientDeferred(sess, tr, sz, attachClientOptions{
		clientID:          h.ClientID,
		resumeCapable:     true,
		maxOutputInFlight: normalizeOutputWindow(h.MaxOutputInFlight),
	})
	d.mu.Unlock()
	d.retireReplacedClient(old, cleanup)
	return ac
}

// route resolves a Hello to a session and a freshly attached client, creating
// the session for ephemeral/new intents.
func (d *Daemon) route(h ports.Hello, tr ports.Transport) (*session, *attachedClient, error) {
	sz := h.Size
	if !sz.Valid() {
		sz = defaultSize
	}
	if d.snapsEnabled && (h.Intent == ports.IntentResume || h.Intent == ports.IntentAttach || h.Intent == ports.IntentNew) {
		select {
		case <-d.restoreDone:
		case <-d.serveCtx.Done():
			return nil, nil, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
		}
	}
	term := terminalEnv{TrueColor: h.TrueColor}

	if h.Intent == ports.IntentResume {
		if sess, ac, ok, err := d.resumeParked(h, tr, sz); ok || err != nil {
			return sess, ac, err
		}
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
	case ports.IntentResume:
		// Resume miss/expiry falls back to normal attach semantics.
		sess := d.findByNameLocked(h.Name)
		if sess == nil {
			stopped, ok := d.stopped[h.Name]
			if !ok || stopped.purging {
				d.mu.Unlock()
				return nil, nil, &protoErr{ports.ErrNoSuchSession, "no such session: " + h.Name}
			}
			cwd := d.dirOrHome(stopped.cwd)
			var err error
			sess, err = d.createSessionLocked(h.Name, false, cwd, sz, term, h.Env, stopped.tabNames)
			if err != nil {
				d.mu.Unlock()
				return nil, nil, err
			}
		}
		d.purgeParkedForSessionLocked(sess)
		return sess, d.finishAttach(sess, tr, sz, term, h), nil

	case ports.IntentEphemeral:
		name := d.allocEphemeralNameLocked()
		sess, err := d.createSessionLocked(name, true, h.Cwd, sz, term, h.Env)
		if err != nil {
			d.mu.Unlock()
			return nil, nil, err
		}
		d.purgeParkedForSessionLocked(sess)
		return sess, d.finishAttach(sess, tr, sz, term, h), nil

	case ports.IntentNew:
		if h.Name == "" {
			d.mu.Unlock()
			return nil, nil, &protoErr{ports.ErrInvalidSessionName, "empty session name"}
		}
		if err := domain.ValidateSessionName(h.Name); err != nil {
			d.mu.Unlock()
			return nil, nil, &protoErr{ports.ErrInvalidSessionName, err.Error()}
		}
		if d.nameLiveOrStoppedLocked(h.Name) {
			d.mu.Unlock()
			return nil, nil, &protoErr{ports.ErrNameTaken, "session name already in use: " + h.Name}
		}
		sess, err := d.createSessionLocked(h.Name, false, h.Cwd, sz, term, h.Env)
		if err != nil {
			d.mu.Unlock()
			return nil, nil, err
		}
		d.purgeParkedForSessionLocked(sess)
		return sess, d.finishAttach(sess, tr, sz, term, h), nil

	case ports.IntentAttach:
		sess := d.findByNameLocked(h.Name)
		if sess == nil {
			stopped, ok := d.stopped[h.Name]
			if !ok || stopped.purging {
				d.mu.Unlock()
				return nil, nil, &protoErr{ports.ErrNoSuchSession, "no such session: " + h.Name}
			}
			cwd := d.dirOrHome(stopped.cwd)
			var err error
			sess, err = d.createSessionLocked(h.Name, false, cwd, sz, term, h.Env, stopped.tabNames)
			if err != nil {
				d.mu.Unlock()
				return nil, nil, err
			}
		}
		d.purgeParkedForSessionLocked(sess)
		return sess, d.finishAttach(sess, tr, sz, term, h), nil

	default:
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrInternal, "unknown intent"}
	}
}
