// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
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
	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
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

// New and restored panes have independent line and uncompressed logical-byte
// ceilings. Visible primary and alternate screens are outside this budget.
const (
	defaultScrollbackRows  = domain.DefaultScrollbackLines
	defaultScrollbackBytes = domain.DefaultScrollbackMegabytes * 1_000_000
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
const maxUnackedOutputStates = protocol.MaxOutputWindow

// normalizeOutputWindow bounds the untrusted Hello value. Zero deliberately
// means the legacy/default window, so malformed or absent values remain safe.
func normalizeOutputWindow(window uint8) uint8 {
	if window == 0 || window > maxUnackedOutputStates {
		return maxUnackedOutputStates
	}
	return window
}

const defaultResumeParkGrace = 15 * time.Minute

// defaultSuspendedSafetyExpiry bounds how long the daemon retains a suspended
// attachment. Suspension keeps transport ownership and session membership but
// removes interactive authority, so a client that never returns must not keep
// the underlying process alive indefinitely. Expiry is a terminal, no-notice
// eviction that never mints a resume credential.
const defaultSuspendedSafetyExpiry = 24 * time.Hour

// defaultSize is retained for headless layout helpers that have no client
// window; Hello routing rejects invalid dimensions instead of using it.
var defaultSize = domain.Size{Cols: 80, Rows: 24}

type Daemon struct {
	mu       sync.Mutex
	sessions map[domain.SessionID]*session
	inactive map[string]inactiveSession
	// creating reserves names while durable creation I/O runs without mu.
	creating map[string]struct{}
	nextID   uint64
	// lastAllocatedCreatedAt is the named-session lifecycle timestamp high-water
	// mark. It is guarded by mu and prevents a wall-clock regression from
	// reusing a lifecycle identity.
	lastAllocatedCreatedAt int64
	mruSeq                 atomic.Uint64
	creationRequestSeq     atomic.Uint64
	// closing marks that explicit daemon shutdown (KillDaemon/process
	// cancellation) has irreversibly begun. It is set under mu when
	// shutdownAll starts, and checked by route under the same mutex — so a Hello
	// racing shutdown can never insert a new session that nobody would tear
	// down. Neither an empty registry (final session removal) nor a KillAll
	// purge sets it: the daemon survives empty→occupied→empty and survives a
	// purge, and only an explicit stop/process cancellation ends it.
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

	// purgeAllMu grants one daemon-wide purge (KillAll) owner at a time so two
	// concurrent kill-all requests never interleave their exact lifecycle sets.
	// It is not an admission gate; see purgeAdmissionClosing below.
	purgeAllMu sync.Mutex
	// purgeAdmissionClosing is the transient KillAll admission gate. Unlike
	// closing and moveLifecycleClosing it is re-openable: purgeAllSessions sets
	// it under mu, removes exactly the live and stopped/broken/durable records
	// captured at that instant, then clears it so the daemon keeps serving.
	// Creation and restoration wait on the changed channel (a blocked writer
	// keeps new admissions out), while moves and internal creations reject, so
	// no transition can be inserted into the purge's exact lifecycle set.
	// purgeAdmissionActive counts admitted transitions the purge must drain.
	// purgeEpoch increments once at each purge that acquires the gate (after the
	// drain) so a restoration worker blocked on admission can detect a purge
	// that completed while it waited and must not resurrect the records captured
	// before that purge.
	// All three fields are guarded by mu.
	purgeAdmissionClosing bool
	purgeAdmissionActive  uint
	purgeAdmissionChanged chan struct{}
	purgeEpoch            uint64

	// notifies holds one completion channel per in-flight async Detached
	// notification (guarded by mu, pruned on insert). Channels rather than a
	// WaitGroup: notifications are spawned from arbitrary goroutines while
	// Serve may be waiting, and WaitGroup forbids Add-from-zero concurrent
	// with Wait.
	notifies []chan struct{}
	parked   map[uint64]*parkedAttachment
	// suspended retains one daemon-owned safety expiry per suspended
	// attachment. Expiry is exact: the record carries the suspension's
	// lifecycle generation and transport incarnation, so a later activation,
	// re-suspension, detach, or session teardown can never be retired by a
	// stale timer. Guarded by mu.
	suspended map[*attachedClient]*suspendedAttachmentRetention
	// graphicsNamespaces reserves deterministic, attachment/session-scoped Kitty
	// ID blocks. Once a block may have reached an outer terminal it remains in
	// this bounded table for the daemon lifetime: side-effect Output frames have
	// no terminal ACK. Pool exhaustion disables graphics for new attachments and
	// leaves their ordinary text output intact.
	graphicsNamespaces           map[uint64]struct{}
	graphicsNamespaceFences      map[uint64]uint64
	graphicsNamespaceQuarantines map[uint64]*graphicsNamespaceQuarantine
	// graphicsNamespaceSalt separates daemon lifetimes before attachment keys
	// are hashed. Kitty IDs are terminal-global, so a restarted or neighboring
	// daemon must not deterministically reopen the previous daemon's block.
	graphicsNamespaceSalt uint64
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
	// afterConnectionSessionSnapshot is a deterministic test seam after the
	// connection loop reads its current session and before it captures that role.
	afterConnectionSessionSnapshot func(*session)
	// afterAttachmentFrameDispatch is a deterministic test seam after the
	// connection loop snapshots a token and before a decoded frame takes effect.
	afterAttachmentFrameDispatch func(attachmentCapability)
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
	// afterPurgeAdmission is a deterministic test seam that runs after a KillAll
	// purge has closed purge admission and taken the closing fence, while it
	// still owns purgeAllMu. It lets tests observe purge serialization.
	afterPurgeAdmission func()
	// beforePurgeLiveSessionKill is a deterministic test seam that runs after a
	// KillAll purge's per-unit shutdown precheck and before that live unit
	// attempts teardown ownership. Tests use it to let an explicit stop win that
	// exact window, proving a stop-owned unit is never reported as a success.
	beforePurgeLiveSessionKill func(*session)
	// afterSessionKillQuarantine is a deterministic test seam that runs while a
	// teardown owns the session and its snapshot coordinator is quarantined, but
	// before the pre-publication revalidation. Tests use it to change the
	// participant set in that window and prove a post-quarantine abort rolls the
	// quarantine back so a surviving session keeps checkpointing.
	afterSessionKillQuarantine func(*session)
	// beforeAttachmentTransitionIdentityAdmission is a deterministic test seam
	// after a ready transition published its capability and before the committed
	// route identity is admitted, so tests can supersede that exact capability in
	// the window before the transition's first paint and completion.
	beforeAttachmentTransitionIdentityAdmission func(attachmentCapability)
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
	// beforeMoveFollowCompletion is a deterministic test seam after the composite
	// follow's first paint and before the postcommit admits the capability that
	// reports completion, so tests can supersede the published capability.
	beforeMoveFollowCompletion func(attachmentCapability)
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
	afterAttachmentEffectAdmitted func(attachmentCapability)
	// beforeFirstPaintSendWait is a test-only seam immediately before a
	// transition first paint waits for the attachment send lock.
	beforeFirstPaintSendWait func(attachmentCapability)
	// afterDelayedKeyEffectAttempt observes whether a timer callback acquired a
	// fresh exact capability before producing PTY, action, or overlay effects.
	afterDelayedKeyEffectAttempt func(bool)
	// afterActionAttachmentEffectEnded observes action-specific admission release. It
	// is a deterministic seam for proving no role-bound mutation follows release.
	afterActionAttachmentEffectEnded func(string)
	// beforeAttachmentSendErrorCleanup pauses asynchronous render-failure retirement
	// after the render ticket ends and before exact lifecycle validation.
	beforeAttachmentSendErrorCleanup func(attachmentCapability)
	afterAttachmentSendErrorCleanup  func()
	// beforeSuspendedExpiryFreeze pauses the safety expiry after its timer fired
	// and before it freezes the attachment effect gate, so tests can win
	// activation, disconnect, or replacement first. It is a test-only seam.
	beforeSuspendedExpiryFreeze func(*attachedClient)
	ptys                        ports.PTYFactory
	clock                       ports.Clock
	log                         *slog.Logger
	runtimeObserver             ports.RuntimeObserver
	baseEnv                     []string
	shell                       string
	shellArgs                   []string
	shellOverride               bool
	persistEnabled              bool
	catalogue                   ports.Catalogue
	catalogueRecords            []domain.CatalogueRecord
	catalogueRecordsProvided    bool
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
	scrollbackConfig               atomic.Pointer[domain.ScrollbackConfig]
	themeConfig                    atomic.Pointer[themeConfigSnapshot]
	barScripts                     *barScriptState
	notices                        *noticeCenter
	resumeParkGrace                time.Duration
	suspendedSafetyExpiry          time.Duration
	remotePreview                  remotePreviewState
	remotePreviewClient            ports.RemotePreviewClient
	// remoteDirectory is the snapshot-only projection served by the remote
	// monitor. Presentation reads it without I/O; nil disables remote
	// directory projections without affecting local behavior.
	remoteDirectory ports.RemoteDirectory
	// remoteFailureNoticed tracks active observation failure episodes so
	// bounded retries do not produce repeated global toasts.
	remoteFailureNoticeMu sync.Mutex
	remoteFailureNoticed  map[string]uint64
	// remoteMonitorRun starts the monitor policy loop. Serve owns its
	// lifetime; nil disables background monitoring.
	remoteMonitorRun func(context.Context) error
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

	// done is closed exactly once by an explicit daemon stop (shutdownAll) to
	// break Serve's accept loop. It is deliberately not closed by final session
	// removal or by a KillAll purge: an empty registry and a purged registry
	// both keep the daemon serving.
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
	sess              *session
	ac                *attachedClient
	pickerGeneration  uint64
	paletteGeneration uint64
	timer             ports.Timer
	claimed           bool
	done              chan struct{}
	doneOnce          sync.Once
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

// inactiveSession is the durable authority for a named session without a live
// runtime. It remains present while healthy-down, restoring, broken, degraded,
// or purge-fenced; callers must use its predicates instead of inferring those
// states from map membership.
type inactiveSession struct {
	name        string
	cwd         string
	createdAt   int64
	incarnation domain.IncarnationID
	lastUsedSeq uint64
	tabNames    []string
	tabRecords  []domain.CatalogueTabRecord
	purging     bool
	record      domain.CatalogueRecord
	state       protocol.SessionState
	restoreDone chan struct{}
}

func (s inactiveSession) restorePending() bool {
	if s.restoreDone == nil {
		return false
	}
	select {
	case <-s.restoreDone:
		return false
	default:
		return true
	}
}

func (s inactiveSession) broken() bool {
	return s.state == protocol.SessionBroken || s.record.DegradedReason != ""
}

func (s inactiveSession) visible() bool {
	return !s.purging
}

func (s inactiveSession) canResume() bool {
	return s.visible() && s.state == protocol.SessionDown && !s.broken()
}

func (s inactiveSession) sameLifecycle(other inactiveSession) bool {
	return s.name == other.name && s.createdAt == other.createdAt && s.incarnation == other.incarnation
}

type Option func(*Daemon)

// WithRuntimeObserver accepts only a composition-root serialized observer.
// The application owns its lifecycle; the daemon never creates or closes a
// second reporting worker around it.
func WithRuntimeObserver(observer ports.SerializedRuntimeObserver) Option {
	return func(d *Daemon) { d.runtimeObserver = observer }
}

// WithRemoteMonitor installs the snapshot-only remote directory and the
// monitor runner. Serve starts the runner without waiting for registry,
// cache or runtime readiness and stops it with its own context.
//
// The daemon-side monitor stays intentionally active in this state: it is part
// of the coordinated P7 removal set, so no polling gate, feature switch, or
// early shutdown path is added here (GO-001, deferred to P7).
func WithRemoteMonitor(directory ports.RemoteDirectory, run func(context.Context) error) Option {
	return func(d *Daemon) {
		d.remoteDirectory = directory
		d.remoteMonitorRun = run
	}
}

// WithRemotePreview installs the optional non-attaching remote viewport client.
func WithRemotePreview(client ports.RemotePreviewClient) Option {
	return func(d *Daemon) { d.remotePreviewClient = client }
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

// WithSuspendedSafetyExpiry overrides how long the daemon retains a suspended
// attachment before terminal eviction. Non-positive durations keep the default.
// Fake-clock tests set a short duration so the real timer path stays under test.
func WithSuspendedSafetyExpiry(expiry time.Duration) Option {
	return func(d *Daemon) {
		if expiry > 0 {
			d.suspendedSafetyExpiry = expiry
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
	return d.daemonNow().UnixNano()
}

// daemonNow reports the daemon clock. New defaults a nil clock to the
// system clock, so the fallback below only serves hand-built fixtures
// and stays explicit about its provenance instead of hiding a bare
// wall-clock read.
func (d *Daemon) daemonNow() time.Time {
	if d == nil || d.clock == nil {
		return systemClock{}.Now()
	}
	return d.clock.Now()
}

type systemClock struct{}
type systemTimer struct{ *time.Timer }

var graphicsNamespaceFallbackSalt atomic.Uint64

func newGraphicsNamespaceSalt() uint64 {
	var bytes [8]byte
	if _, err := cryptorand.Read(bytes[:]); err == nil {
		if salt := binary.BigEndian.Uint64(bytes[:]); salt != 0 {
			return salt
		}
	}
	for {
		salt := graphicsNamespaceFallbackSalt.Add(1)
		if salt != 0 {
			return salt
		}
	}
}

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
	paneProcessCtx, paneProcessCancel := context.WithCancel(context.Background())
	d := &Daemon{
		sessions:                     make(map[domain.SessionID]*session),
		inactive:                     make(map[string]inactiveSession),
		creating:                     make(map[string]struct{}),
		parked:                       make(map[uint64]*parkedAttachment),
		suspended:                    make(map[*attachedClient]*suspendedAttachmentRetention),
		graphicsNamespaces:           make(map[uint64]struct{}),
		graphicsNamespaceFences:      make(map[uint64]uint64),
		graphicsNamespaceQuarantines: make(map[uint64]*graphicsNamespaceQuarantine),
		graphicsNamespaceSalt:        newGraphicsNamespaceSalt(),
		parking:                      make(map[uint64]*parkingAttachment),
		paneProcessCtx:               paneProcessCtx,
		paneProcessCancel:            paneProcessCancel,
		ptys:                         ptys,
		clock:                        clock,
		log:                          log,
		baseEnv:                      os.Environ(),
		shell:                        defaultShellCommand,
		dirOrHome:                    dirOrHome,
		done:                         make(chan struct{}),
		restoreDone:                  make(chan struct{}),
		animWake:                     make(chan struct{}, 1),
		snapshotJobs:                 make(chan *snapshotCapture, snapshotQueueCapacity),
		snapshotAdmitted:             make(map[*snapshotCapture]struct{}),
		snapshotWake:                 make(chan struct{}, 1),
		notices:                      newNoticeCenter(),
		resumeParkGrace:              defaultResumeParkGrace,
		suspendedSafetyExpiry:        defaultSuspendedSafetyExpiry,
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
	defaultTabs := defaults.Tabs
	d.tabsConfig.Store(&defaultTabs)
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
			d.inactive[r.Name] = inactiveSessionFromRecord(r, state, done)
		} else {
			d.inactive[r.Name] = inactiveSessionFromRecord(r, protocol.SessionDown, nil)
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
	return d
}

// remoteMonitorShutdownJoin bounds the daemon's wait for monitor teardown.
// The monitor bounds its own runtime cleanup to two seconds; this join only
// covers scheduling slack and never enters the session worker group.
const remoteMonitorShutdownJoin = 3 * time.Second

// Serve runs the accept loop over l, owning it for the loop's lifetime. It
// returns only on ctx cancellation or an explicit daemon shutdown request
// (KillDaemon), never when the session registry drains to empty: final
// session removal is independent of daemon shutdown so the daemon survives
// empty→occupied→empty and can accept fresh sessions again. A KillAll purge
// leaves Serve running too. On the ctx path
// attached clients are detached with ReasonServerShutdown.
func (d *Daemon) Serve(ctx context.Context, l ports.ServerListener) error {
	d.serveCtx, d.serveCancel = context.WithCancel(ctx)
	defer d.serveCancel()
	d.hardCtx, d.hardCancel = context.WithCancel(context.Background())
	defer d.hardCancel()

	// Remote directory projections relay without I/O: the watcher rebuilds
	// only open views on publication and never requests reconciliation.
	// The watcher and the monitor runner share one bounded shutdown budget
	// below, outside the session worker group.
	watcherDone := make(chan struct{})
	if d.remoteDirectory != nil {
		go func() {
			defer close(watcherDone)
			d.watchRemoteDirectory(d.serveCtx)
		}()
	} else {
		close(watcherDone)
	}
	var monitorDone chan error
	if d.remoteMonitorRun != nil {
		monitorDone = make(chan error, 1)
		go func() { monitorDone <- d.remoteMonitorRun(d.serveCtx) }()
	}
	// Bounded join outside the session worker group: remote teardown never
	// extends daemon shutdown past this single budget. The monitor runner
	// owns its runtime's two-second cleanup join, so joining the runner
	// transitively joins the workers; the watcher owns no I/O or workers.
	// This join is a wall-time operational bound on purpose: it uses the
	// package wall clock (like the handshake timeout) instead of the
	// injected daemon clock, whose timers may never fire in tests.
	defer func() {
		join := systemClock{}.NewTimer(remoteMonitorShutdownJoin)
		defer join.Stop()
		if monitorDone != nil {
			select {
			case <-monitorDone:
			case <-join.C():
				d.log.Warn("remote monitor shutdown join timed out")
				return
			}
		}
		select {
		case <-watcherDone:
		case <-join.C():
			d.log.Warn("remote directory watcher shutdown join timed out")
		}
	}()

	d.sessWg.Go(func() {
		d.attentionAnimator(d.serveCtx)
	})
	d.sessWg.Go(func() {
		d.barScriptPoller(d.serveCtx)
	})
	d.sessWg.Go(func() {
		d.historyMaintenance(d.serveCtx)
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
	// Break the accept loop only on an explicit shutdown: the parent context
	// being cancelled or an explicit daemon stop request (d.done). An empty
	// registry never closes the listener, so the daemon keeps accepting.
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
	d.terminateAllForShutdown(protocol.ReasonServerShutdown, snapshotDeadline)
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
	d.terminateAllForShutdown(protocol.ReasonServerShutdown, snapshotDeadline)
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

// shutdownAll is the explicit daemon-stop operation (wire KillDaemon). It is
// the only caller that irreversibly ends the daemon: it closes move admission,
// marks closing, cancels the daemon-wide pane process context, preserves every
// live session as stopped durable authority, and closes done so Serve returns.
// KillAll never reaches this path. Setting closing under the same lock as the
// snapshot guarantees no session can be inserted after the snapshot: route
// rejects once closing is set, and both run under d.mu. killSession (which
// relocks) runs after the lock is released.
func (d *Daemon) shutdownAll(reason uint8) (checkpointIncomplete bool) {
	return d.terminateAllForShutdown(reason, nil)
}

// terminateAllForShutdown performs the irreversible daemon teardown shared by
// shutdownAll and Serve's exit path. It is separate from purgeAllSessions,
// which leaves every daemon-wide shutdown fact untouched.
func (d *Daemon) terminateAllForShutdown(reason uint8, deadline *snapshotShutdownDeadline) (checkpointIncomplete bool) {
	d.closeMoveLifecycles()
	d.mu.Lock()
	d.closing = true
	// Wake any KillAll waiting on purge admission so it defers to this stop
	// instead of holding the gate until its bounded drain deadline expires.
	d.signalPurgeAdmissionChangedLocked()
	d.purgeAllParkingLocked()
	parkedRetirements := d.purgeAllParkedLocked()
	d.purgeAllSuspendedLocked()
	snapshot := d.sessionsSnapshotLocked()
	d.mu.Unlock()
	d.finishParkedAttachmentRetirements(parkedRetirements)
	// Publish preservation policy for the complete registry snapshot before
	// cancelling any PTY. Cancellation-driven EOF is allowed to own teardown,
	// but its disposition must remain daemon shutdown rather than session purge.
	for _, s := range snapshot {
		s.reserveShutdownTeardown()
	}
	d.log.Info("session termination begin", "reason", reason, "live_sessions", len(snapshot))
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
func (d *Daemon) registerSessionLocked(entry *session) bool {
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
func (d *Daemon) unregisterSessionLocked(entry *session) bool {
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
	return sessionsSnapshot(d.sessions)
}

// handleConn reads the first typed message off a fresh connection and routes it.
// A context watcher closes the connection on cancellation or handshake timeout.
func (d *Daemon) handleConn(tr ports.ServerConnection) {
	handshakeCtx, timedOut, finishHandshake := d.newHandshakeContext(d.hardCtx, acceptedHandshakeDeadline(tr))
	stopTransport := watchHandshakeTransport(handshakeCtx, tr)
	defer finishHandshake()
	defer stopTransport()

	var first protocol.ClientMessage
	err := boundedHandshakeOperation(handshakeCtx, tr, func() error {
		var receiveErr error
		first, receiveErr = tr.ReceiveClient()
		return receiveErr
	})
	if err != nil {
		var failure *protocol.DecodeFailure
		if errors.As(err, &failure) {
			d.handleInitialDecodeFailure(handshakeCtx, tr, failure)
		} else {
			err = handshakeContextError(d.hardCtx, timedOut, err)
			d.log.Warn("connection closed before hello", "err", err)
		}
		_ = tr.Close()
		return
	}
	switch message := first.(type) {
	case protocol.List:
		stopTransport()
		finishHandshake()
		d.handleList(tr)
	case protocol.CommandRequest:
		stopTransport()
		finishHandshake()
		if err := d.handleCommand(tr, message); err != nil {
			d.log.Warn("command handler failed", "err", err)
		}
	case protocol.RemotePreviewRequest:
		stopTransport()
		finishHandshake()
		if err := d.handleRemotePreview(tr, message); err != nil {
			d.log.Warn("remote preview handler failed", "err", err)
		}
	case protocol.NavigationInventoryRequest:
		stopTransport()
		finishHandshake()
		d.handleNavigationInventory(tr, message)
	case protocol.PickerControlRequest:
		stopTransport()
		finishHandshake()
		d.handlePickerControl(tr, message)
	case protocol.Kill:
		stopTransport()
		finishHandshake()
		d.handleKill(tr, message)
	case protocol.Hello:
		d.handleHelloWithContext(handshakeCtx, timedOut, stopTransport, finishHandshake, tr, message)
	default:
		d.log.Warn("hello rejected", "err", "expected hello")
		_ = boundedHandshakeOperation(handshakeCtx, tr, func() error {
			return tr.SendServer(serverError(protocol.ErrInternal, "expected hello"))
		})
		stopTransport()
		finishHandshake()
		_ = tr.Close()
	}
}

func (d *Daemon) handleInitialDecodeFailure(ctx context.Context, tr ports.ServerConnection, failure *protocol.DecodeFailure) {
	send := func(message protocol.ServerMessage) {
		_ = boundedHandshakeOperation(ctx, tr, func() error { return tr.SendServer(message) })
		_ = tr.Close()
	}
	switch failure.Kind {
	case protocol.DecodeMessageHello:
		if failure.Version != 0 && failure.Version != protocol.Version {
			send(serverError(protocol.ErrVersionMismatch, "protocol version mismatch"))
			return
		}
		send(serverError(protocol.ErrInternal, "malformed hello"))
	case protocol.DecodeMessageCommand:
		if !failure.HasRequestID {
			return
		}
		code, text := protocol.ErrInternal, "malformed command request"
		if failure.Version != 0 && failure.Version != protocol.Version {
			code, text = protocol.ErrVersionMismatch, "protocol version mismatch"
		}
		send(protocol.CommandResult{RequestID: failure.RequestID, Outcome: protocol.CommandFailed, Code: code, Text: text})
	case protocol.DecodeMessageKill:
		send(serverError(protocol.ErrInternal, "malformed kill request"))
	case protocol.DecodeMessageRemotePreview:
		send(protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewMalformed})
	case protocol.DecodeMessageNavigationInventory:
		if !failure.HasRequestID {
			return
		}
		status := protocol.NavigationInventoryInvalid
		if failure.Version != 0 && failure.Version != protocol.Version {
			status = protocol.NavigationInventoryVersionMismatch
		}
		send(protocol.NavigationInventoryResponse{RequestID: failure.RequestID, Operation: protocol.NavigationInventorySnapshot, Status: status})
	default:
		send(serverError(protocol.ErrInternal, "expected hello"))
	}
}

// handleList replies with the current session listing and closes the (one-shot
// control) connection.
func (d *Daemon) handleList(tr ports.ServerConnection) {
	defer func() { _ = tr.Close() }()

	d.mu.Lock()
	infos := make([]protocol.SessionInfo, 0, len(d.sessions)+len(d.inactive))
	liveNames := make(map[string]struct{}, len(d.sessions))
	for _, s := range d.sessions {
		if s == nil {
			continue
		}
		s.mu.Lock()
		info := protocol.SessionInfo{
			SessionID: string(s.id),
			Name:      s.name,
			State:     protocol.SessionUp,
			Ephemeral: s.ephemeral,
			Tabs:      uint16(len(s.tabs)),
			// Attached means registered interactive presence, not mere transport
			// membership: a suspended-only session is headless to observers.
			Attached: sessionInteractivelyAttachedLocked(s),
		}
		liveNames[s.name] = struct{}{}
		s.mu.Unlock()
		infos = append(infos, info)
	}
	for name, inactive := range d.inactive {
		if _, live := liveNames[name]; live || !inactive.visible() {
			continue
		}
		state := protocol.SessionDown
		if inactive.broken() {
			state = protocol.SessionBroken
		}
		infos = append(infos, protocol.SessionInfo{Name: name, State: state})
	}
	d.mu.Unlock()

	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	_ = d.boundedControlSend(tr, serverSessions(infos))
}

// handleKill terminates the requested live session or stopped named session,
// purges every session (KillAll), or accepts daemon shutdown (KillDaemon).
// Every normally decoded request receives exactly one correlated result before
// the control connection closes. KillAll leaves the daemon active.
func (d *Daemon) handleKill(tr ports.ServerConnection, request protocol.Kill) {
	defer func() { _ = tr.Close() }()
	send := func(result protocol.KillResult) {
		result.RequestID = request.RequestID
		d.logKillResultSendFailure(result, d.boundedControlSend(tr, result))
	}

	switch request.Scope {
	case protocol.KillAll:
		result := d.purgeAllSessions(protocol.ReasonSessionKilled)
		if result.Superseded() {
			send(protocol.KillResult{Outcome: protocol.KillOutcomeUnknown, Code: protocol.ErrServerShutdown, Text: "daemon is shutting down"})
			return
		}
		if result.Failed() {
			failures := make([]protocol.KillFailure, 0, min(len(result.Failures), purgeSummaryFailureLimit))
			for _, failure := range result.Failures[:min(len(result.Failures), purgeSummaryFailureLimit)] {
				failures = append(failures, protocol.KillFailure{Class: failure.Class.String(), Name: failure.Name, Text: failure.Err.Error()})
			}
			send(protocol.KillResult{Outcome: protocol.KillFailed, Code: protocol.ErrInternal, Text: result.summary(), Failures: failures})
			return
		}
		send(protocol.KillResult{Outcome: protocol.KillSucceeded})
		return
	case protocol.KillDaemon:
		send(protocol.KillResult{Outcome: protocol.KillSucceeded})
		d.shutdownAll(protocol.ReasonServerShutdown)
		return
	case protocol.KillSession:
	default:
		send(protocol.KillResult{Outcome: protocol.KillFailed, Code: protocol.ErrInternal, Text: "invalid kill scope"})
		return
	}

	d.mu.Lock()
	target := d.findByNameLocked(request.Name)
	if target == nil {
		if _, ok := d.inactive[request.Name]; ok {
			d.mu.Unlock()
			// Stopped sessions use the same catalogue-first, incarnation-second
			// deletion order as live and offline purges.
			if err := d.retryStoppedPurge(request.Name); err != nil {
				d.log.Warn("deleting stopped session failed", "err", err, "session", request.Name)
				send(protocol.KillResult{Outcome: protocol.KillFailed, Code: protocol.ErrInternal, Text: "deleting stopped session failed"})
				return
			}
			send(protocol.KillResult{Outcome: protocol.KillSucceeded})
			return
		}
	}
	d.mu.Unlock()

	if target == nil {
		send(protocol.KillResult{Outcome: protocol.KillFailed, Code: protocol.ErrNoSuchSession, Text: "no such session: " + request.Name})
		return
	}
	if err := d.killSession(target, protocol.ReasonSessionKilled, true); err != nil {
		if errors.Is(err, errSessionKillParticipantsChanged) {
			// The exact session was not removed; a concurrent detach or token
			// resume invalidated the teardown snapshot. Report the retryable
			// outcome instead of a permanent delete failure.
			send(protocol.KillResult{Outcome: protocol.KillFailed, Code: protocol.ErrInternal, Text: sessionKillRetryMessage})
			return
		}
		send(protocol.KillResult{Outcome: protocol.KillFailed, Code: protocol.ErrInternal, Text: "deleting persisted session failed"})
		return
	}
	send(protocol.KillResult{Outcome: protocol.KillSucceeded})
}

// handleHello runs the attach handshake for direct package callers. Accepted
// connections use handleConn, which resolves the optional accepted deadline so
// one budget also covers the first frame read; handleHello adopts the same seam
// when the connection exposes it. A connection without the seam keeps the
// ordinary fresh protocol.HandshakeTimeout budget.
func (d *Daemon) handleHello(tr ports.ServerConnection, hello protocol.Hello) {
	handshakeCtx, timedOut, finishHandshake := d.newHandshakeContext(context.Background(), acceptedHandshakeDeadline(tr))
	stopTransport := watchHandshakeTransport(handshakeCtx, tr)
	defer finishHandshake()
	defer stopTransport()
	d.handleHelloWithContext(handshakeCtx, timedOut, stopTransport, finishHandshake, tr, hello)
}

// handleHelloWithContext runs the attach handshake: version check, intent
// routing, Welcome, guaranteed first paint, then the per-connection input loop.
func (d *Daemon) handleHelloWithContext(handshakeCtx context.Context, timedOut <-chan struct{}, stopHandshakeTransport, finishHandshake func(), tr ports.ServerConnection, h protocol.Hello) {
	if err := handshakeContextError(d.hardCtx, timedOut, handshakeCtx.Err()); err != nil {
		_ = tr.Close()
		return
	}
	sendHandshake := func(message protocol.ServerMessage) error {
		err := boundedHandshakeOperation(handshakeCtx, tr, func() error { return tr.SendServer(message) })
		if err != nil {
			return handshakeContextError(d.hardCtx, timedOut, err)
		}
		return nil
	}
	if h.Version != protocol.Version {
		d.log.Warn("hello rejected", "err", "protocol version mismatch", "version", h.Version, "expected", protocol.Version, "intent", h.Intent, "session", h.Name)
		_ = sendHandshake(serverError(protocol.ErrVersionMismatch, "protocol version mismatch"))
		_ = tr.Close()
		return
	}

	sess, ac, rerr := d.routeWithContext(handshakeCtx, h, tr)
	if rerr != nil {
		d.log.Warn("hello rejected", "err", rerr, "intent", h.Intent, "session", h.Name)
		if pe, ok := errors.AsType[*protoErr](rerr); ok {
			_ = sendHandshake(serverError(pe.code, pe.text))
		} else {
			_ = sendHandshake(serverError(protocol.ErrInternal, rerr.Error()))
		}
		_ = tr.Close()
		return
	}

	welcomeSent := false
	//nolint:contextcheck // handshakeCtx is intentionally not propagated: cancellation teardown must complete its geometry and ownership cleanup.
	failAttachment := func() {
		d.failHandshakeAttachment(sess, ac, tr, welcomeSent)
	}
	if err := handshakeCtx.Err(); err != nil {
		failAttachment()
		return
	}
	expected := ac.transportSnapshot()
	welcomeToken := sess.captureAttachmentCapability(ac, tr)
	welcomeTicket, admitted := ac.beginAttachmentEffect(welcomeToken)
	if expected.transport != tr || !admitted || welcomeToken.ac == nil {
		if admitted {
			welcomeTicket.End()
		}
		failAttachment()
		return
	}
	if err := handshakeCtx.Err(); err != nil {
		welcomeTicket.End()
		failAttachment()
		return
	}
	welcomeDone, welcomeErr := boundedHandshakeOperationTracked(handshakeCtx, tr, func() error {
		frame, err := serverWelcome(sess, d.resumeTokenSnapshot(ac))
		if err != nil {
			return err
		}
		return ac.sendExpectedTransportForAttachment(expected, frame, welcomeTicket)
	})
	if welcomeErr != nil {
		<-welcomeDone
		welcomeTicket.End()
		failAttachment()
		return
	}
	welcomeSent = true
	// Release Welcome's effect before discovering post-handshake authority so a
	// replacement blocked behind the send can publish its generation and lease.
	welcomeTicket.End()
	postWelcomeToken, postWelcomeTicket, admitted := ac.beginCurrentAttachmentEffectContext(handshakeCtx, sess, tr)
	if !admitted {
		failAttachment()
		return
	}
	postWelcomeLease := postWelcomeToken.lease
	if postWelcomeToken.ac == nil {
		postWelcomeTicket.End()
		failAttachment()
		return
	}
	postWelcomeRC := sess.renderCoordinator()
	if postWelcomeLease != nil && (postWelcomeRC == nil || !postWelcomeRC.markAttachmentReady(postWelcomeLease)) {
		postWelcomeTicket.End()
		// The attachment was detached while Welcome was in flight; never let
		// this stale handshake emit an Output frame.
		failAttachment()
		return
	}
	paintToken := sess.captureAttachmentCapability(ac, tr)
	painted := make(chan bool, 1)
	paintDone, paintErr := boundedHandshakeOperationTracked(handshakeCtx, tr, func() error {
		//nolint:contextcheck // firstPaintForTransition is capability-bounded; the surrounding handshake operation owns cancellation.
		painted <- d.firstPaintForTransition(paintToken)
		return nil
	})
	if paintErr != nil {
		<-paintDone
		postWelcomeTicket.End()
		failAttachment()
		return
	}
	if !<-painted || handshakeCtx.Err() != nil {
		postWelcomeTicket.End()
		failAttachment()
		return
	}
	if !d.commitResumeClaim(ac) {
		postWelcomeTicket.End()
		failAttachment()
		return
	}
	postWelcomeTicket.End()
	stopHandshakeTransport()
	finishHandshake()
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
func (d *Daemon) finishAttach(sess *session, tr ports.ServerConnection, sz domain.Size, h protocol.Hello) (*attachedClient, error) {
	initialTabIndex := -1
	if h.SessionTarget != nil {
		var ok bool
		initialTabIndex, ok = remoteTargetTabIndexLocked(sess, *h.SessionTarget)
		if !ok {
			if tr != nil {
				_ = tr.Close()
			}
			d.mu.Unlock()
			return nil, &protoErr{protocol.ErrNoSuchTarget, "remote tab no longer exists"}
		}
	}
	if h.PreferredTabID != "" {
		// Route memory is a best-effort attachment cursor, not attach authority.
		// It overrides point-in-time picker tab metadata when still present and
		// otherwise falls back to the session's normal first-tab repair.
		initialTabIndex = preferredTabIndex(sess, h.PreferredTabID)
	}
	// Session state is the sole source for future PTY children. Update it before
	// publishing the attachment; existing PTYs keep their original environment.
	// A picker handoff deliberately leaves the daemon-owned environment and CWD
	// untouched, even though Hello retains those fields for direct CLI clients.
	sess.mu.Lock()
	if h.EnvironmentPolicy != protocol.EnvironmentPolicyDaemonOwned {
		sess.env = copyEnvironment(h.Env)
	}
	sess.mu.Unlock()
	terminalCapabilities := terminalcap.Detect(h.Env)
	// Kitty graphics are enabled only by the explicit direct-terminal
	// declaration in Hello. Environment values remain useful for color and
	// diagnostics, but cannot authorize terminal-global graphics side effects.
	terminalCapabilities.KittyGraphics = h.KittyDirectGraphics
	if h.TrueColor && !terminalCapabilities.TrueColor() {
		terminalCapabilities.ColorMode = terminalcap.TrueColor
		terminalCapabilities.ColorSource = terminalcap.SourceDeclared
	}
	opts := attachClientOptions{
		clientID:               h.ClientID,
		resumeCapable:          true,
		maxOutputInFlight:      normalizeOutputWindow(h.MaxOutputInFlight),
		navigationCapabilities: h.NavigationCapabilities,
		terminalCapabilities:   terminalCapabilities,
		capabilitiesSet:        true,
	}
	geometry := h.Geometry()
	if geometry.Size != sz {
		geometry = domain.Geometry{Size: sz}
	}
	ac := d.prepareAttachedClientLocked(sess, tr, geometry, opts)
	d.mu.Unlock()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              ac,
		expectedTransport: ac.transportSnapshot(),
		activateTargetTab: initialTabIndex >= 0,
		targetTabIndex:    initialTabIndex,
		ready:             false,
	})
	if err != nil {
		if ac.output != nil && ac.output.graphicsOutput != nil {
			d.retireGraphicsOutput(ac, ac.output.graphicsOutput)
		}
		if tr != nil {
			_ = tr.Close()
		}
		return nil, err
	}
	d.finishAttachedClient(sess, ac, opts)
	d.deferAttachmentTransitionCleanups(result)
	return ac, nil
}

func preferredTabIndex(sess *session, preferred domain.TabStableID) int {
	if sess == nil || preferred == "" {
		return -1
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for i, candidate := range sess.tabs {
		if candidate != nil && domain.TabStableID(candidate.stableID) == preferred {
			return i
		}
	}
	return -1
}

func (d *Daemon) waitForTargetRestore(ctx context.Context, name string) error {
	d.mu.Lock()
	var (
		done    chan struct{}
		stopped inactiveSession
		ok      bool
	)
	if sess := d.findByNameLocked(name); sess != nil {
		sess.mu.Lock()
		done = sess.restoreDone
		sess.mu.Unlock()
	} else {
		stopped, ok = d.inactive[name]
		if !ok || !stopped.visible() {
			d.mu.Unlock()
			return nil
		}
		if stopped.restorePending() {
			done = stopped.restoreDone
		}
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
	stopped, ok = d.inactive[name]
	if !ok || stopped.record.Name == "" {
		return nil
	}
	if stopped.broken() {
		return &protoErr{protocol.ErrInternal, "session durable state is broken: " + name}
	}
	if stopped.record.Committed == nil {
		return nil
	}
	return &protoErr{protocol.ErrInternal, "session was not restored into this daemon: " + name}
}

// route resolves a Hello to a session and a freshly attached client, creating
// the session for ephemeral/new intents. Direct package callers retain the
// daemon lifetime context; inbound handshakes use routeWithContext.
func (d *Daemon) validateExactSessionTargetLocked(target protocol.ExactSessionTarget) error {
	if sess := d.findByNameLocked(target.SessionName); sess != nil {
		if sess.incarnation != target.LifecycleID {
			return &protoErr{protocol.ErrNoSuchSession, "session lifecycle has changed: " + target.SessionName}
		}
		return nil
	}
	stopped, ok := d.inactive[target.SessionName]
	if !ok || !stopped.visible() || stopped.incarnation != target.LifecycleID {
		return &protoErr{protocol.ErrNoSuchSession, "no such session lifecycle: " + target.SessionName}
	}
	return nil
}

func (d *Daemon) validateExactSessionTarget(ctx context.Context, target protocol.ExactSessionTarget) error {
	if err := target.Validate(); err != nil {
		return &protoErr{protocol.ErrNoSuchSession, "invalid exact session target"}
	}
	if err := d.waitForTargetRestore(ctx, target.SessionName); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.validateExactSessionTargetLocked(target)
}

func (d *Daemon) route(h protocol.Hello, tr ports.ServerConnection) (*session, *attachedClient, error) {
	ctx := d.serveCtx
	if ctx == nil {
		ctx = context.Background()
	}
	return d.routeWithContext(ctx, h, tr)
}

func (d *Daemon) routeWithContext(ctx context.Context, h protocol.Hello, tr ports.ServerConnection) (*session, *attachedClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	sz := h.Size
	if !sz.Valid() {
		return nil, nil, &protoErr{protocol.ErrInternal, "invalid terminal size"}
	}
	// The accepting side's provisioned admission, when present, is the authority
	// this Hello is validated against before any restore, creation, resume
	// claim, ownership change, or environment mutation. A connection without
	// that seam keeps its legacy validation unchanged and gains no admission
	// exception, so a forged Hello can never widen what it is allowed to do.
	admission, admitted := sessionHelloAdmission(tr)
	if admitted {
		if err := validateSessionHelloAdmission(h, admission); err != nil {
			return nil, nil, err
		}
		if admission.Admission == ports.BrokerAdmissionExact && h.ResumeToken != 0 {
			if err := d.validateResumeAdmittedTarget(h.ResumeToken, admission.Target); err != nil {
				return nil, nil, err
			}
		}
	}
	if h.Intent == protocol.IntentResume && h.ResumeToken == 0 {
		return nil, nil, &protoErr{protocol.ErrNoSuchSession, "resume token is required"}
	}
	if h.ExactTarget != nil && h.ResumeToken == 0 && h.Name != h.ExactTarget.SessionName {
		return nil, nil, &protoErr{protocol.ErrNoSuchSession, "exact session target name mismatch"}
	}
	// A remote exact-target handoff is an admitted transition like a local
	// create/restore: it reserves purge admission before dispatch, so a Hello
	// arriving during a KillAll waits on the gate exactly as a local route does
	// instead of being rejected outright.
	if h.SessionTarget != nil && h.ResumeToken == 0 {
		if err := d.acquirePurgeAdmission(ctx); err != nil {
			return nil, nil, err
		}
		defer d.releasePurgeAdmission()
		return d.routeRemoteTargetWithContext(ctx, h, tr)
	}
	if h.ExactTarget != nil && h.ResumeToken == 0 {
		if err := d.validateExactSessionTarget(ctx, *h.ExactTarget); err != nil {
			return nil, nil, err
		}
	}

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
					return nil, nil, &protoErr{protocol.ErrNoSuchSession, "resume token is no longer valid"}
				}
				return nil, nil, err
			} else if ok {
				return sess, ac, nil
			}
			return nil, nil, &protoErr{protocol.ErrNoSuchSession, "resume token is no longer valid"}
		}
		if sess, ac, ok, err := d.resumeParked(h, tr, sz); err == nil {
			if ok {
				return sess, ac, nil
			}
			return nil, nil, &protoErr{protocol.ErrNoSuchSession, "resume token is no longer valid"}
		} else if errors.Is(err, errResumeTokenLifecycleRace) {
			// The parked entry was replaced while this handshake waited for
			// its send lock. Fail closed on the original credential rather
			// than falling through to ordinary attach/create routing.
			return nil, nil, &protoErr{protocol.ErrNoSuchSession, "resume token is no longer valid"}
		} else {
			return nil, nil, err
		}
	}
	if h.Intent == protocol.IntentResume || h.Intent == protocol.IntentAttach {
		if err := d.waitForTargetRestore(ctx, h.Name); err != nil {
			return nil, nil, err
		}
	}
	// A daemon-owned ephemeral Hello without an exact remote target is a legacy
	// shape the daemon refuses. An authenticated remote admission that names the
	// creation is the one exception, and it is granted only under a validated
	// remote admission; a connection without one never gains it.
	if !admitted && h.EnvironmentPolicy == protocol.EnvironmentPolicyDaemonOwned && h.SessionTarget == nil &&
		h.Intent == protocol.IntentEphemeral {
		return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "daemon-owned environment requires an exact remote target"}
	}
	// KillAll admission gate: a purge owns the exact lifecycle set it captured,
	// so a Hello arriving during it waits here and is then admitted as a create
	// or restore after the purge, never inserted into the set being removed.
	// This is transient and re-opened by the purge; it is not daemon closing.
	if err := d.acquirePurgeAdmission(ctx); err != nil {
		return nil, nil, err
	}
	defer d.releasePurgeAdmission()
	d.mu.Lock()
	// Shutdown/create interlock: once an explicit shutdown has begun
	// (shutdownAll started) no new session may be created and no attach may
	// proceed — the teardown snapshot has already been (or is being) taken, so
	// anything inserted now would leak its PTY and hang Serve. An empty registry
	// alone is not shutdown, and a KillAll purge is gated by admission above.
	if d.closing {
		d.mu.Unlock()
		return nil, nil, &protoErr{protocol.ErrServerShutdown, "daemon is shutting down"}
	}
	// The first exact-target check happens before restore I/O. Recheck while
	// holding d.mu so a same-name lifecycle replacement cannot slip between
	// validation and attachment creation.
	if h.ExactTarget != nil && h.ResumeToken == 0 {
		if err := d.validateExactSessionTargetLocked(*h.ExactTarget); err != nil {
			d.mu.Unlock()
			return nil, nil, err
		}
	}
	switch h.Intent {
	case protocol.IntentEphemeral:
		name := d.allocEphemeralNameLocked()
		// A daemon-owned creation (an authenticated remote admission) uses the
		// daemon's own environment and home rather than any client value; a
		// client-owned creation (local) uses the admitted client environment and
		// working directory. The Hello fields are already validated to match the
		// admission, so this mirrors the named-creation ownership rule below.
		cwd, env := h.Cwd, h.Env
		if h.EnvironmentPolicy == protocol.EnvironmentPolicyDaemonOwned {
			cwd, env = d.dirOrHome(""), copyEnvironment(d.baseEnv)
		}
		sess, err := d.createSessionLockedWithMode(name, true, cwd, h.Geometry(), env)
		if err != nil {
			d.mu.Unlock()
			return nil, nil, err
		}
		ac, err := d.finishRouteAttach(sess, tr, sz, h, true, true)
		return sess, ac, err

	case protocol.IntentNew:
		if h.Name == "" {
			d.mu.Unlock()
			return nil, nil, &protoErr{protocol.ErrInvalidSessionName, "empty session name"}
		}
		if err := domain.ValidateSessionName(h.Name); err != nil {
			d.mu.Unlock()
			return nil, nil, &protoErr{protocol.ErrInvalidSessionName, err.Error()}
		}
		if d.nameLiveOrStoppedLocked(h.Name) {
			d.mu.Unlock()
			return nil, nil, &protoErr{protocol.ErrNameTaken, "session name already in use: " + h.Name}
		}
		cwd, env := h.Cwd, h.Env
		if h.EnvironmentPolicy == protocol.EnvironmentPolicyDaemonOwned {
			// A remote CNS destination owns shell startup just like a picker
			// attach. Client paths may exist here but belong to another user.
			cwd, env = d.dirOrHome(""), copyEnvironment(d.baseEnv)
		}
		sess, err := d.createSessionLockedWithMode(h.Name, false, cwd, h.Geometry(), env)
		if err != nil {
			d.mu.Unlock()
			return nil, nil, err
		}
		ac, err := d.finishRouteAttach(sess, tr, sz, h, true, true)
		return sess, ac, err

	case protocol.IntentAttach:
		sess := d.findByNameLocked(h.Name)
		created := false
		if sess == nil {
			stopped, ok := d.inactive[h.Name]
			if !ok || !stopped.canResume() {
				d.mu.Unlock()
				return nil, nil, &protoErr{protocol.ErrNoSuchSession, "no such resumable session: " + h.Name}
			}
			cwd := d.dirOrHome(stopped.cwd)
			env := h.Env
			if h.EnvironmentPolicy == protocol.EnvironmentPolicyDaemonOwned {
				env = copyEnvironment(d.baseEnv)
			}
			var err error
			sess, err = d.resumeInactiveSessionLocked(h.Name, cwd, h.Geometry(), env, stopped, stopped.tabNames)
			if err != nil {
				d.mu.Unlock()
				return nil, nil, err
			}
			created = true
		}
		ac, err := d.finishRouteAttach(sess, tr, sz, h, created, false)
		return sess, ac, err

	default:
		d.mu.Unlock()
		return nil, nil, &protoErr{protocol.ErrInternal, "unknown intent"}
	}
}
