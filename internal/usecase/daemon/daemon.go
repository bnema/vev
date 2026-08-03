// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
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

// New and restored panes are bounded by both history rows and cells. The
// cell budget keeps a very wide terminal from retaining disproportionately
// more scrollback than the normal 160-column case.
const (
	defaultScrollbackRows  = 10_000
	defaultScrollbackCells = 12_000 * 160
)

// detachNotifyTimeout bounds the best-effort Detached notification on the
// detach/kill/shutdown paths: if a wedged client (full kernel send buffer)
// blocks the write for this long, its transport is force-closed, which fails
// the in-flight send. Teardown is never gated on a client draining its socket.
const detachNotifyTimeout = time.Second

// maxUnackedOutputStates caps how many output states may be in flight (sent but
// not yet acked by the client) before paint defers rather than composing
// another diff. It bounds the daemon's paint rate to the client's ack rate, so
// heavy output degrades to lower fps on a slow link instead of overflowing the
// transport. It must stay well under the datagram carriage's 32-frame client
// window so its reliable queue never fills from painting alone.
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

// defaultSize is retained for headless layout helpers that have no client
// window; Hello routing rejects invalid dimensions instead of using it.
var defaultSize = domain.Size{Cols: 80, Rows: 24}

type Daemon struct {
	mu       sync.Mutex
	sessions map[domain.SessionID]attachmentSession
	stopped  map[string]stoppedSession
	// creating reserves names while durable creation I/O runs without mu.
	creating map[string]struct{}
	nextID   uint64
	// lastAllocatedCreatedAt is the named-session lifecycle timestamp high-water
	// mark. It is guarded by mu and prevents a wall-clock regression from
	// reusing a lifecycle identity.
	lastAllocatedCreatedAt int64
	mruSeq                 atomic.Uint64
	// pickerSort is the picker ordering mode for this daemon's lifetime
	// (pickerSortMode); not persisted across restarts.
	pickerSort atomic.Uint32
	// closing marks that shutdown has irreversibly begun. It is set under mu,
	// atomically with the event that makes shutdown inevitable (the registry
	// emptying in killSession, or shutdownAll starting), and checked by route
	// under the same mutex — so a Hello racing shutdown can never insert a new
	// session that nobody would tear down.
	closing bool

	// moveLifecycleMu is the daemon-level admission gate for transferable
	// ownership changes. It is acquired before any session teardownMu, and is
	// never held while a move, teardown, or external operation runs.
	moveLifecycleMu      sync.Mutex
	moveLifecycleClosing bool
	moveLifecycleActive  uint
	moveLifecycleChanged chan struct{}
	// paneProcessCtx roots transferable pane processes outside any one session.
	// Shutdown cancels it only after the global move gate drains.
	paneProcessCtx    context.Context
	paneProcessCancel context.CancelFunc

	// notifies holds one completion channel per in-flight async Detached
	// notification (guarded by mu, pruned on insert). Channels rather than a
	// WaitGroup: notifications are spawned from arbitrary goroutines while
	// Serve may be waiting, and WaitGroup forbids Add-from-zero concurrent
	// with Wait.
	notifies []chan struct{}
	parked   map[uint64]*parkedAttachment
	// parking tracks resume-capable attachments from before detach clears the
	// live seat until parkAttachment publishes the token into parked.
	// IntentResume waits on the matching entry instead of treating the live
	// credential as unknown across that gap.
	parking map[uint64]*parkingAttachment

	attnMu    sync.Mutex
	animFrame int
	animWake  chan struct{}

	paletteRecentMu sync.Mutex
	paletteRecent   []string
	// beforeRecentSessionHandoff is a deterministic test seam for the narrow
	// interval between JRS validation and its committed hand-off.
	beforeRecentSessionHandoff func()
	// beforeCopyModeRevalidate is a deterministic test seam between staging a
	// copy-mode candidate and revalidating its pane membership.
	beforeCopyModeRevalidate func()
	// beforeCopyMouseMap is a deterministic seam after an immutable copy-input
	// snapshot is captured and before its mapped position is applied.
	beforeCopyMouseMap func()
	// beforeSessionResizePublication runs after final external PTY validation
	// and before coordinator epoch admission. It is a deterministic regression
	// seam for stale resize publication.
	beforeSessionResizePublication func()
	// beforeResizeOwnerPostEffect pauses after a resize's optimistic owner check
	// and immediately before a post-commit effect is published. Tests use it to
	// move a pane through the ordered resize fences at the former TOCTOU window.
	beforeResizeOwnerPostEffect func(resizeOwnerPostEffect)
	// afterResizeCommitSendLocked is a deterministic lock-order seam. It runs
	// after publishResizeCommit acquires attachment sendMu and before it acquires
	// owner fences. Tests must not perform external work from this callback.
	afterResizeCommitSendLocked func()
	// afterConnectionSessionSnapshot is a deterministic test seam after the
	// connection loop reads its current session and before it captures that role.
	afterConnectionSessionSnapshot func(attachmentSession)
	// afterAttachmentFrameDispatch is a deterministic test seam after the
	// connection loop snapshots a token and before a decoded frame takes effect.
	afterAttachmentFrameDispatch func(attachmentConnectionToken)
	// afterAttachmentEffectParticipantsSnapshotted observes immutable role-gate
	// participants after architecture preflight locks are released and before
	// the globally ordered freeze begins.
	afterAttachmentEffectParticipantsSnapshotted func(string, []*attachedClient)
	// afterAttachmentEffectGateFrozen observes each participant after it is frozen in
	// immutable identity order and before drain/publication continues.
	afterAttachmentEffectGateFrozen func(string, *attachedClient)
	// afterAttachmentEffectsFrozen observes the lock-free boundary after all affected
	// attachment gates are frozen and drained, before architecture publication.
	afterAttachmentEffectsFrozen func()
	// afterAttachmentTransitionCoordinatorsLocked is a deterministic lock-order
	// seam used by transition validation tests.
	afterAttachmentTransitionCoordinatorsLocked func()
	// beforeMoveDispatch, afterMoveLifecycleReserved, and
	// afterMovePaneSourceSnapshot are test-only seams. They run before dispatch
	// admission, after lifecycle reservation, and after the pre-fence source
	// attachment snapshot respectively. beforeMovePaneCommit runs inside the
	// non-failing publication section. None is set in production, and none may
	// perform external work while locks are held.
	beforeMoveDispatch                        func()
	afterMoveLifecycleReserved                func()
	afterMoveLifecycleGateBeforeTeardownLocks func()
	afterMovePaneSourceSnapshot               func()
	afterMoveTabSourceSnapshot                func()
	beforeMovePaneCommit                      func()
	beforeMoveTabCommit                       func()
	// afterDetachAttachmentEffectsFrozen observes terminal detach after it wins the
	// attachment gate but before it checks session ownership.
	afterDetachAttachmentEffectsFrozen func()
	// beforeClientGoneDetach pauses clientGone after the stale-transport
	// precheck and before exact transport/incarnation detach validation.
	beforeClientGoneDetach func()
	// beforeMarkParkingInFlight pauses resumeLiveAttachment after the live
	// credential matches and before markParkingInFlight, so tests can win
	// explicit detach or session removal before late marker publication.
	beforeMarkParkingInFlight func()
	// afterClientGoneDetach pauses finishClientGone after detach won and before
	// parkAttachment. The parking-in-flight marker must already be published.
	afterClientGoneDetach func()
	// afterResumeLiveDetach pauses resumeLiveAttachment after detach won and
	// before parkAttachment. The parking-in-flight marker must already be
	// published so a concurrent same-token resume can wait the gap.
	afterResumeLiveDetach func()
	// afterParkingWaitArmed observes waitParkingInFlight after it has resolved a
	// matching in-flight entry and before it blocks on done.
	afterParkingWaitArmed func()
	// beforeResumeParkedSendMu pauses resumeParked after the initial parked
	// lookup and before the attachment send lock, so tests can consume or
	// replace the credential while the handshake waits.
	beforeResumeParkedSendMu func()
	// afterAttachmentEffectAdmitted is a deterministic test seam after a frame/paint
	// reserves its exact capability and before its first observable mutation.
	afterAttachmentEffectAdmitted func(attachmentConnectionToken)
	// beforeFirstPaintSendWait is a test-only seam immediately before a
	// transition first paint waits for the attachment send lock.
	beforeFirstPaintSendWait func(attachmentConnectionToken)
	// afterDelayedKeyEffectAttempt observes whether a timer callback acquired a
	// fresh exact capability before producing PTY, action, or overlay effects.
	afterDelayedKeyEffectAttempt func(bool)
	// afterActionAttachmentEffectEnded observes action-specific admission release. It
	// is a deterministic seam for proving no role-bound mutation follows release.
	afterActionAttachmentEffectEnded func(string)
	// beforeAttachmentSendErrorCleanup pauses asynchronous render-failure retirement
	// after the render ticket ends and before exact lifecycle validation.
	beforeAttachmentSendErrorCleanup func(attachmentConnectionToken)
	afterAttachmentSendErrorCleanup  func()
	ptys                             ports.PTYFactory
	clock                            ports.Clock
	log                              *slog.Logger
	runtimeObserver                  ports.RuntimeObserver
	baseEnv                          []string
	shell                            string
	shellArgs                        []string
	shellOverride                    bool
	persistEnabled                   bool
	catalogue                        ports.Catalogue
	catalogueRecords                 []domain.CatalogueRecord
	catalogueRecordsProvided         bool
	// snapshotRepository is the sole checkpoint storage contract.
	snapshotRepository      ports.SnapshotRepository
	recovery                *recoveryusecase.Coordinator
	maintenanceWorkerCancel context.CancelFunc
	maintenanceWorkerDone   chan struct{}
	// restoreWorkerDone is the restoration goroutine's ownership signal. Startup
	// restoration reconciles durable checkpoints, so it is a durable writer and
	// is guarded by snapshotWorkerMu with the other two.
	restoreWorkerDone chan struct{}
	snapsEnabled      bool
	noticeStore       ports.NoticeStore
	snapshotJobs      chan *snapshotCapture
	// snapshotAdmitted contains every capture accepted by either worker queue,
	// including captures buffered in snapshotJobs. Guarded by snapshotWorkerMu.
	snapshotAdmitted map[*snapshotCapture]struct{}
	// snapshotWake wakes the repository scheduler when a session becomes dirty
	// or an attempt completes. It is never closed and producers only send
	// non-blockingly.
	snapshotWake            chan struct{}
	snapshotWorkerMu        sync.Mutex
	snapshotWorkerID        uint64
	snapshotWorkerCtx       context.Context
	snapshotWorkerCancel    context.CancelFunc
	snapshotWorkerDone      chan struct{}
	snapshotWorkerFlush     chan struct{}
	snapshotWorkerFinalWake chan struct{}
	// snapshotFinalJobs coalesces terminal captures by session when the bounded
	// regular queue is full. It retains at most snapshotFinalQueueCapacity named
	// sessions, each with only its newest terminal state while the worker blocks.
	snapshotFinalJobs      map[*session]*snapshotCapture
	snapshotFinalOrder     []*session
	snapshotWorkerClosing  bool
	snapshotWorkerInFlight *snapshotCapture
	// snapshotNoticeMu guards the active global persistence failure signature.
	// It is separate from snapshotWorkerMu so notice routing cannot block a
	// producer or a repository worker.
	snapshotNoticeMu               sync.Mutex
	snapshotActiveFailureSignature string
	shutdownNoticeMu               sync.Mutex
	shutdownNoticedSessions        map[string]struct{}
	restoreDone                    chan struct{}
	restoreOnce                    sync.Once
	procCwd                        func(int) (string, error)
	procComm                       func(int) (string, error)
	procArgv                       func(int) ([]string, error)
	procGroupArgv                  func(int, int) ([]string, error)
	dirOrHome                      func(string) string
	bindings                       atomic.Pointer[keys.Bindings]
	codeOverrides                  atomic.Pointer[map[string]string]
	restoreProcessAllowlist        atomic.Pointer[map[string]struct{}]
	floatingConfig                 atomic.Pointer[domain.FloatingConfig]
	copyConfig                     atomic.Pointer[domain.CopyConfig]
	paletteConfig                  atomic.Pointer[domain.PaletteConfig]
	navConfig                      atomic.Pointer[domain.NavConfig]
	tabsConfig                     atomic.Pointer[domain.TabsConfig]
	themeConfig                    atomic.Pointer[themeConfigSnapshot]
	barScripts                     *barScriptState
	notices                        *noticeCenter
	resumeParkGrace                time.Duration
	// remoteCatalog owns cache-derived discovery state independently of the
	// live attachment registry. Its cache reads and writes never hold d.mu.
	remoteCatalog       remoteCatalogState
	remoteHostStore     ports.RemoteHostStore
	remoteCatalogClient ports.RemoteCatalogClient
	remoteCatalogCache  ports.RemoteCatalogCache
	remoteDialerFactory ports.RemoteDialerFactory
	remoteTransportMode ports.RemoteTransportMode
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
	sess             *session
	ac               *attachedClient
	pickerGeneration uint64
	timer            ports.Timer
	claimed          bool
	done             chan struct{}
	doneOnce         sync.Once
}

// parkingAttachment is the observable detach→park lifecycle for one resume
// credential. done closes when the token is published into parked or parking
// is abandoned.
type parkingAttachment struct {
	sess     *session
	ac       *attachedClient
	done     chan struct{}
	doneOnce sync.Once
}

func (p *parkingAttachment) closeDone() {
	if p == nil || p.done == nil {
		return
	}
	p.doneOnce.Do(func() { close(p.done) })
}

// session is a single multiplexed session. It owns one or more full-screen

type stoppedSession struct {
	name        string
	cwd         string
	createdAt   int64
	incarnation domain.IncarnationID
	lastUsedSeq uint64
	tabNames    []string
	purging     bool
	record      domain.CatalogueRecord
	state       ports.SessionState
	restoreDone chan struct{}
}

type Option func(*Daemon)

// WithRuntimeObserver accepts only a composition-root serialized observer.
// The application owns its lifecycle; the daemon never creates or closes a
// second reporting worker around it.
func WithRuntimeObserver(observer ports.SerializedRuntimeObserver) Option {
	return func(d *Daemon) { d.runtimeObserver = observer }
}

// WithRemoteDiscovery installs the remote discovery ports used by the daemon.
// The composition root validates mode before constructing the daemon.
func WithRemoteDiscovery(store ports.RemoteHostStore, catalog ports.RemoteCatalogClient, cache ports.RemoteCatalogCache, dialer ports.RemoteDialerFactory, mode ports.RemoteTransportMode) Option {
	return func(d *Daemon) {
		d.remoteHostStore = store
		d.remoteCatalogClient = catalog
		d.remoteCatalogCache = cache
		d.remoteDialerFactory = dialer
		d.remoteTransportMode = mode
	}
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

// WithCatalogue installs the singular catalogue and the strictly opened
// records that define the daemon's expected-session registry at startup.
func WithCatalogue(catalogue ports.Catalogue, records []domain.CatalogueRecord) Option {
	return func(d *Daemon) {
		d.catalogue = catalogue
		d.persistEnabled = catalogue != nil
		d.catalogueRecords = append([]domain.CatalogueRecord(nil), records...)
		d.catalogueRecordsProvided = true
	}
}

func (d *Daemon) catalogueRecord(name string) (domain.CatalogueRecord, bool, error) {
	if d == nil || d.catalogue == nil {
		return domain.CatalogueRecord{}, false, nil
	}
	return d.catalogue.Record(name)
}

// markCatalogueDirty buffers metadata. The periodic catalogue timer, the next
// identity write, or shutdown provides the durability barrier.
func (d *Daemon) markCatalogueDirty(update domain.CatalogueMetadataUpdate) {
	if d == nil || d.catalogue == nil {
		return
	}
	if err := d.catalogue.UpdateMetadata(update); err != nil {
		d.log.Warn("buffering session metadata failed", "err", err, "session", update.Name)
	}
}

func (d *Daemon) flushCatalogue() error {
	if d == nil || d.catalogue == nil {
		return nil
	}
	return d.catalogue.Sync()
}

// WithRecoveryCoordinator installs the durable recovery coordinator.
func WithRecoveryCoordinator(coordinator *recoveryusecase.Coordinator) Option {
	return func(d *Daemon) {
		d.recovery = coordinator
	}
}

// WithSnapshotRepository enables content-addressed incremental snapshots.
func WithSnapshotRepository(repository ports.SnapshotRepository) Option {
	return func(d *Daemon) {
		if isNilSnapshotRepository(repository) {
			return
		}
		d.snapshotRepository = repository
		d.snapsEnabled = true
	}
}

func isNilSnapshotRepository(repository ports.SnapshotRepository) bool {
	if repository == nil {
		return true
	}
	value := reflect.ValueOf(repository)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// WithNoticeStore enables persisting undeliverable notices across daemon
// restarts. A nil store keeps the daemon in no-op notice-persistence mode.
func WithNoticeStore(store ports.NoticeStore) Option {
	return func(d *Daemon) { d.noticeStore = store }
}

// WithDurableMaintenance retains the application wiring for the one
// pre-publication GC pass. Standalone users without an explicitly supplied
// coordinator receive the same canonical coordinator path.
func WithDurableMaintenance(catalogue ports.Catalogue, repository ports.SnapshotRepository) Option {
	return func(d *Daemon) {
		if d.recovery == nil {
			d.recovery = recoveryusecase.NewCoordinator(catalogue, repository, nil)
		}
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
			d.barScripts.runner = barScriptRunner{runner: runner}
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

// allocateLifecycleCreatedAtLocked returns a unique lifecycle timestamp. Caller
// holds d.mu. The persisted high-water mark is loaded by New before any named
// session can be resumed or created.
func (d *Daemon) allocateLifecycleCreatedAtLocked() (int64, error) {
	now := d.nowUnixNano()
	if now <= d.lastAllocatedCreatedAt {
		if d.lastAllocatedCreatedAt == math.MaxInt64 {
			return 0, errors.New("daemon: lifecycle identities exhausted")
		}
		now = d.lastAllocatedCreatedAt + 1
	}
	d.lastAllocatedCreatedAt = now
	return now, nil
}

func (d *Daemon) nowUnixNano() int64 {
	return d.clock.Now().UnixNano()
}

type systemClock struct{}
type systemTimer struct{ *time.Timer }

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTimer(delay time.Duration) ports.Timer {
	return systemTimer{Timer: time.NewTimer(delay)}
}
func (t systemTimer) C() <-chan time.Time { return t.Timer.C }

// New constructs a Daemon. ptys spawns PTY-backed children, clock drives the
// render debounce, and log receives diagnostics (defaults to slog.Default).
func New(ptys ports.PTYFactory, clock ports.Clock, log *slog.Logger, opts ...Option) *Daemon {
	if log == nil {
		log = slog.Default()
	}
	if clock == nil {
		clock = systemClock{}
	}
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}
	paneProcessCtx, paneProcessCancel := context.WithCancel(context.Background())
	d := &Daemon{
		sessions:          make(map[domain.SessionID]attachmentSession),
		stopped:           make(map[string]stoppedSession),
		creating:          make(map[string]struct{}),
		parked:            make(map[uint64]*parkedAttachment),
		parking:           make(map[uint64]*parkingAttachment),
		paneProcessCtx:    paneProcessCtx,
		paneProcessCancel: paneProcessCancel,
		ptys:              ptys,
		clock:             clock,
		log:               log,
		baseEnv:           os.Environ(),
		shell:             shell,
		dirOrHome:         dirOrHome,
		done:              make(chan struct{}),
		restoreDone:       make(chan struct{}),
		animWake:          make(chan struct{}, 1),
		snapshotJobs:      make(chan *snapshotCapture, snapshotQueueCapacity),
		snapshotAdmitted:  make(map[*snapshotCapture]struct{}),
		snapshotWake:      make(chan struct{}, 1),
		notices:           newNoticeCenter(),
		remoteCatalog:     newRemoteCatalogState(),
		resumeParkGrace:   defaultResumeParkGrace,
		barScripts: &barScriptState{
			cfg:         barConfigFromDomain(domain.Defaults().Bar),
			outputs:     make(map[domain.SessionID]barScriptOutputs),
			lastRefresh: make(map[domain.SessionID]time.Time),
			lastContext: make(map[domain.SessionID]barScriptContext),
			running:     make(map[domain.SessionID]bool),
			reload:      make(chan struct{}, 1),
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
	records := d.catalogueRecords
	var maxSeq uint64
	var maxCreatedAt int64
	hasCreatedAt := false
	for _, r := range records {
		if d.catalogueRecordsProvided {
			state, done := initialSessionState(r)
			d.stopped[r.Name] = stoppedSessionFromRecord(r, state, done)
		} else {
			d.stopped[r.Name] = stoppedSession{name: r.Name, cwd: r.Cwd, createdAt: r.CreatedAt, incarnation: r.IncarnationID, lastUsedSeq: r.LastUsedSeq, tabNames: append([]string(nil), r.TabNames...)}
		}
		if !hasCreatedAt || r.CreatedAt > maxCreatedAt {
			maxCreatedAt = r.CreatedAt
			hasCreatedAt = true
		}
		if r.LastUsedSeq > maxSeq {
			maxSeq = r.LastUsedSeq
		}
	}
	if hasCreatedAt {
		d.lastAllocatedCreatedAt = maxCreatedAt
	}
	d.mruSeq.Store(maxSeq)
	d.loadRemoteCatalogCache()
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
	if d.persistEnabled {
		d.sessWg.Go(func() {
			d.cwdSampler(d.serveCtx)
		})
	}
	if d.snapsEnabled {
		d.startSnapshotEncodeWorker()
		d.sessWg.Go(func() {
			d.snapshotRepositorySaver(d.serveCtx)
		})
		d.startSnapshotRestoration()
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
	// One deadline owns every terminal checkpoint wait and the final worker
	// join. A repository is allowed to ignore cancellation, so spending a fresh
	// interval at either stage would make shutdown exceed its documented bound.
	var snapshotDeadline *snapshotShutdownDeadline
	if d.snapsEnabled {
		snapshotDeadline = newSnapshotShutdownDeadline(d.clock)
		defer snapshotDeadline.stop()
	}
	d.shutdownAllWithSnapshotDeadline(ports.ReasonServerShutdown, snapshotDeadline)
	d.waitNotifies()
	d.hardCancel()
	d.serveCancel()
	// Wait for handlers before flushing snapshots: a handler that already
	// entered killSession may still submit its terminal capture after the
	// registry snapshot has been removed.
	d.connWg.Wait()
	d.attachmentCleanupWg.Wait()
	// Forced terminal checkpoints run before shutdown stops producers or the
	// worker. StopDurableWriters reports every session whose final capture still
	// owns work when the shared checkpoint budget expires. Persist one notice per
	// affected session, then retain ownership until every writer actually exits.
	stopCtx, stopCancel := snapshotStopContext(snapshotDeadline)
	timedOutSessions := d.StopDurableWriters(stopCtx)
	stopCancel()
	for _, name := range timedOutSessions {
		d.persistShutdownSnapshotFailure(name, context.DeadlineExceeded)
	}
	d.WaitDurableWriters()
	d.shutdownAllWithSnapshotDeadline(ports.ReasonServerShutdown, snapshotDeadline)
	d.waitSessionWorkersWithSnapshotDeadline(snapshotDeadline)
	d.waitNotifies()
	if err := d.flushCatalogue(); err != nil {
		d.log.Warn("flushing session catalogue at shutdown failed", "err", err)
	}
	if d.catalogue != nil {
		if err := d.catalogue.Close(); err != nil {
			d.log.Warn("closing session catalogue failed", "err", err)
		}
	}
	return nil
}

// shutdownAll marks the daemon closing and kills every live session. Setting
// closing under the same lock as the snapshot guarantees no session can be
// inserted after the snapshot: route rejects once closing is set, and both run
// under d.mu. killSession (which relocks) runs after the lock is released.
func (d *Daemon) shutdownAll(reason uint8) (checkpointIncomplete bool) {
	return d.shutdownAllWithSnapshotDeadline(reason, nil)
}

func (d *Daemon) shutdownAllWithSnapshotDeadline(reason uint8, deadline *snapshotShutdownDeadline) (checkpointIncomplete bool) {
	d.cancelRemotePickerRefresh()
	d.closeMoveLifecycles()
	d.mu.Lock()
	d.closing = true
	d.purgeAllParkingLocked()
	parkedRetirements := d.purgeAllParkedLocked()
	snapshot := d.sessionsSnapshotLocked()
	empty := len(snapshot) == 0
	d.mu.Unlock()
	d.finishParkedAttachmentRetirements(parkedRetirements)
	d.log.Info("graceful shutdown begin", "reason", reason, "sessions", len(snapshot))
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
		return false
	}
	for _, s := range snapshot {
		// Cancellation and PTY closure must not wait behind a teardown owner that
		// is blocked in snapshot publication or purge work.
		s.mu.Lock()
		name := s.name
		ephemeral := s.ephemeral
		s.mu.Unlock()
		s.stopInMemoryLifecycle()
		if err := d.killSessionWithSnapshotDeadline(s, reason, false, deadline, nil); err != nil {
			checkpointIncomplete = true
			d.log.Error("closing session with unpersisted terminal state", "err", err)
		}
		if !ephemeral && deadline != nil {
			select {
			case <-deadline.Done():
				checkpointIncomplete = true
				d.persistShutdownSnapshotFailure(name, context.DeadlineExceeded)
			default:
			}
		}
	}
	// Signal Serve even when a concurrent role transition made this initial
	// pass abort; Serve owns the bounded defensive teardown passes.
	d.doneOnce.Do(func() { close(d.done) })
	return checkpointIncomplete
}

// waitSessionWorkersWithSnapshotDeadline joins in-memory workers when possible,
// but never extends Serve beyond the shared repository shutdown budget. By this
// point connection handlers are joined and every session has been cancelled, so
// no new session worker can be registered while the waiter is active.
func (d *Daemon) waitSessionWorkersWithSnapshotDeadline(deadline *snapshotShutdownDeadline) {
	done := make(chan struct{})
	go func() {
		d.sessWg.Wait()
		close(done)
	}()
	if deadline == nil {
		<-done
		return
	}
	select {
	case <-done:
	case <-deadline.Done():
	}
}

// snapshotShutdownDeadline is a single-use shutdown budget shared by forced
// checkpoints and the final snapshot worker join. Its done channel is never
// closed by callers, so a detached worker can safely retain session state until
// an uncooperative repository call returns.
type snapshotShutdownDeadline struct {
	done     chan struct{}
	stopCh   chan struct{}
	finished chan struct{}
	stopOnce sync.Once
}

func newSnapshotShutdownDeadline(clock ports.Clock) *snapshotShutdownDeadline {
	deadline := &snapshotShutdownDeadline{done: make(chan struct{}), stopCh: make(chan struct{}), finished: make(chan struct{})}
	timer := clock.NewTimer(snapshotFinalFlushTimeout)
	go func() {
		defer close(deadline.finished)
		select {
		case <-timer.C():
			close(deadline.done)
		case <-deadline.stopCh:
			timer.Stop()
		}
	}()
	return deadline
}

func (d *snapshotShutdownDeadline) stop() {
	if d != nil {
		d.stopOnce.Do(func() {
			close(d.stopCh)
			<-d.finished
		})
	}
}

func (d *snapshotShutdownDeadline) Done() <-chan struct{} {
	if d == nil {
		return nil
	}
	return d.done
}

func snapshotStopContext(deadline *snapshotShutdownDeadline) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	if deadline == nil {
		return ctx, cancel
	}
	select {
	case <-deadline.Done():
		cancel()
		return ctx, cancel
	default:
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-deadline.Done():
			cancel()
		case <-stop:
		}
	}()
	var stopOnce sync.Once
	return ctx, func() {
		stopOnce.Do(func() { close(stop) })
		cancel()
	}
}

// registerSessionLocked publishes an exact attachment-session identity. Caller
// holds d.mu.
func (d *Daemon) registerSessionLocked(entry attachmentSession) bool {
	if entry == nil {
		return false
	}
	core := entry.core()
	if core == nil || core.id == "" || d.sessions[core.id] != nil {
		return false
	}
	d.sessions[core.id] = entry
	return true
}

// unregisterSessionLocked removes only the exact registered identity. Caller
// holds d.mu.
func (d *Daemon) unregisterSessionLocked(entry attachmentSession) bool {
	if entry == nil {
		return false
	}
	core := entry.core()
	if core == nil || d.sessions[core.id] != entry {
		return false
	}
	delete(d.sessions, core.id)
	return true
}

func (d *Daemon) sessionsSnapshotLocked() []*session {
	return localSessionsSnapshot(d.sessions)
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
	case ports.MsgCommand:
		if err := d.handleCommand(tr, first); err != nil {
			d.log.Warn("command handler failed", "err", err)
		}
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
	for _, entry := range d.sessions {
		s, ok := localSession(entry)
		if !ok {
			continue
		}
		s.mu.Lock()
		info := ports.SessionInfo{
			SessionID: string(s.id),
			Name:      s.name,
			State:     ports.SessionRunning,
			Ephemeral: s.ephemeral,
			Tabs:      uint16(len(s.tabs)),
			Attached:  len(s.attachments) != 0,
		}
		liveNames[s.name] = struct{}{}
		s.mu.Unlock()
		infos = append(infos, info)
	}
	for name, stopped := range d.stopped {
		if _, live := liveNames[name]; live {
			continue
		}
		state := ports.SessionStopped
		if stopped.purging || stopped.state == ports.SessionBroken {
			// Purge is the dominant externally visible state: restoration must
			// never make a deletion-reserved record appear attachable.
			state = ports.SessionBroken
		}
		infos = append(infos, ports.SessionInfo{Name: name, State: state})
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
		if _, ok := d.stopped[k.Name]; ok {
			d.mu.Unlock()
			// Stopped sessions use the same catalogue-first, incarnation-second
			// deletion order as live and offline purges.
			if err := d.retryStoppedPurge(k.Name); err != nil {
				d.log.Warn("deleting stopped session failed", "err", err, "session", k.Name)
				_ = tr.Send(frameError(ports.ErrInternal, "deleting stopped session failed"))
			}
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
	welcomeToken := sess.attachmentToken(ac, tr)
	welcomeToken.lease = lease
	welcomeTicket, admitted := ac.beginAttachmentEffect(welcomeToken)
	if expected.transport != tr || !admitted || welcomeToken.ac == nil {
		if admitted {
			welcomeTicket.End()
		}
		if !d.abortResumeClaim(ac) {
			d.clientGone(sess, ac, tr, false)
		}
		return
	}
	if err := ac.sendExpectedTransportForAttachment(expected, frameWelcome(sess, ac), welcomeTicket); err != nil {
		welcomeTicket.End()
		if !d.abortResumeClaim(ac) {
			d.clientGone(sess, ac, tr, false)
		}
		return
	}
	if !d.commitResumeClaim(ac) {
		welcomeTicket.End()
		if !d.abortResumeClaim(ac) {
			d.clientGone(sess, ac, tr, false)
		}
		return
	}
	// Release Welcome's effect before discovering post-handshake authority so a
	// replacement blocked behind the send can publish its generation and lease.
	welcomeTicket.End()
	postWelcomeToken, postWelcomeTicket, admitted := ac.beginCurrentAttachmentEffect(sess, tr)
	if !admitted {
		d.clientGone(sess, ac, tr, false)
		return
	}
	postWelcomeLease := postWelcomeToken.lease
	if postWelcomeToken.ac == nil {
		postWelcomeTicket.End()
		d.clientGone(sess, ac, tr, false)
		return
	}
	if postWelcomeLease != nil && (rc == nil || !rc.markAttachmentReady(postWelcomeLease)) {
		postWelcomeTicket.End()
		// The attachment was detached while Welcome was in flight; never let
		// this stale handshake emit an Output frame.
		d.clientGone(sess, ac, tr, false)
		return
	}
	postWelcomeTicket.End()
	paintToken := sess.attachmentToken(ac, tr)
	if !d.firstPaintForTransition(paintToken) {
		d.clientGone(sess, ac, tr, false)
		return
	}
	d.runConnLoop(ac)
	_ = tr.Close()
}

// protoErr is a session-level rejection carrying a wire ErrorMsg code.
type protoErr struct {
	code uint16
	text string
}

func (e *protoErr) Error() string { return e.text }

// finishAttach completes an attachment prepared while d.mu is held. Every
// caller must hold d.mu on entry; finishAttach transfers ownership by
// unlocking d.mu before returning, including every error path. It publishes
// terminal and role state before releasing d.mu, then defers coordinator
// cleanup so obsolete workers never delay the new handshake.
func (d *Daemon) finishAttach(sess *session, tr ports.Transport, sz domain.Size, term terminalEnv, h ports.Hello) (*attachedClient, error) {
	// Session state is the sole source for future PTY children. Update it before
	// publishing the attachment; existing PTYs keep their original environment.
	sess.mu.Lock()
	sess.env = copyEnvironment(h.Env)
	sess.terminal = term
	sess.mu.Unlock()
	opts := attachClientOptions{
		clientID:          h.ClientID,
		resumeCapable:     true,
		maxOutputInFlight: normalizeOutputWindow(h.MaxOutputInFlight),
	}
	ac := d.prepareAttachedClientLocked(tr, sz, opts)
	d.mu.Unlock()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess,
		next:   ac,

		expectedTransport: ac.transportSnapshot(),
		ready:             false,
	})
	if err != nil {
		if tr != nil {
			_ = tr.Close()
		}
		return nil, err
	}
	d.finishAttachedClient(sess, ac, opts)
	d.deferAttachmentTransitionCleanups(result)
	return ac, nil
}

func (d *Daemon) waitForTargetRestore(ctx context.Context, name string) error {
	d.mu.Lock()
	var (
		done    chan struct{}
		stopped stoppedSession
		ok      bool
	)
	if sess := d.findByNameLocked(name); sess != nil {
		sess.mu.Lock()
		done = sess.restoreDone
		sess.mu.Unlock()
	} else {
		stopped, ok = d.stopped[name]
		if !ok || stopped.purging {
			d.mu.Unlock()
			return nil
		}
		done = stopped.restoreDone
	}
	d.mu.Unlock()

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.findByNameLocked(name) != nil {
		return nil
	}
	stopped, ok = d.stopped[name]
	if !ok || stopped.record.Name == "" {
		return nil
	}
	if stopped.state == ports.SessionBroken {
		return &protoErr{ports.ErrInternal, "session durable state is broken: " + name}
	}
	if stopped.record.Committed == nil {
		return nil
	}
	return &protoErr{ports.ErrInternal, "session was not restored into this daemon: " + name}
}

// route resolves a Hello to a session and a freshly attached client, creating
// the session for ephemeral/new intents.
func (d *Daemon) route(h ports.Hello, tr ports.Transport) (*session, *attachedClient, error) {
	sz := h.Size
	if !sz.Valid() {
		return nil, nil, &protoErr{ports.ErrInternal, "invalid terminal size"}
	}
	term := terminalEnv{TrueColor: h.TrueColor}

	// A non-zero token is an authoritative resume credential. If it is unknown,
	// expired, or raced with lifecycle teardown, fail closed instead of routing
	// the Hello as an ordinary attach that could create or replace ownership.
	// The one legitimate pre-park race is a still-active same-client credential
	// for the requested session; hand that into the park/resume lifecycle.
	if h.ResumeToken != 0 {
		d.mu.Lock()
		parkedAtStart := d.parked[h.ResumeToken]
		d.mu.Unlock()
		if parkedAtStart == nil {
			if sess, ac, ok, err := d.resumeLiveAttachment(h, tr, sz); err != nil {
				if errors.Is(err, errResumeTokenLifecycleRace) {
					// Live recovery parked then lost a competing resumeParked
					// race; keep the fail-closed wire response instead of
					// leaking the internal sentinel to handleHello as ErrInternal.
					return nil, nil, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
				}
				return nil, nil, err
			} else if ok {
				return sess, ac, nil
			}
			return nil, nil, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
		}
		if sess, ac, ok, err := d.resumeParked(h, tr, sz); err == nil {
			if ok {
				return sess, ac, nil
			}
			return nil, nil, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
		} else if errors.Is(err, errResumeTokenLifecycleRace) {
			// The parked entry was replaced while this handshake waited for
			// its send lock. Fail closed on the original credential rather
			// than falling through to ordinary attach/create routing.
			return nil, nil, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
		} else {
			return nil, nil, err
		}
	}
	if h.Intent == ports.IntentResume || h.Intent == ports.IntentAttach {
		ctx := d.serveCtx
		if ctx == nil {
			ctx = context.Background()
		}
		if err := d.waitForTargetRestore(ctx, h.Name); err != nil {
			return nil, nil, err
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
			sess, err = d.createSessionLockedWithMode(h.Name, false, cwd, sz, term, h.Env, stopped.tabNames)
			if err != nil {
				d.mu.Unlock()
				return nil, nil, err
			}
		}
		ac, err := d.finishAttach(sess, tr, sz, term, h)
		return sess, ac, err

	case ports.IntentEphemeral:
		name := d.allocEphemeralNameLocked()
		sess, err := d.createSessionLockedWithMode(name, true, h.Cwd, sz, term, h.Env)
		if err != nil {
			d.mu.Unlock()
			return nil, nil, err
		}
		ac, err := d.finishAttach(sess, tr, sz, term, h)
		return sess, ac, err

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
		sess, err := d.createSessionLockedWithMode(h.Name, false, h.Cwd, sz, term, h.Env)
		if err != nil {
			d.mu.Unlock()
			return nil, nil, err
		}
		ac, err := d.finishAttach(sess, tr, sz, term, h)
		return sess, ac, err

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
			sess, err = d.createSessionLockedWithMode(h.Name, false, cwd, sz, term, h.Env, stopped.tabNames)
			if err != nil {
				d.mu.Unlock()
				return nil, nil, err
			}
		}
		ac, err := d.finishAttach(sess, tr, sz, term, h)
		return sess, ac, err

	default:
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrInternal, "unknown intent"}
	}
}
