// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

var (
	errSessionNameRequired = errors.New("name required")
	errSessionNameInUse    = errors.New("name already in use")
)

type terminalEnv struct {
	TrueColor bool
}

type session struct {
	sessionCore

	ctx    context.Context
	cancel context.CancelFunc

	// dispatchMu serializes user-initiated mutating command dispatch (palette
	// and vev-cmd control) for this session, so a one-shot command cannot
	// resolve focus against one state and mutate a later one. Lock order:
	// dispatchMu strictly before d.mu, sess.mu, or any tab mu.
	dispatchMu sync.Mutex
	// teardownMu guards teardown ownership and is the outermost per-session
	// lifecycle lock: never acquire it while holding daemon, session, snapshot,
	// tab, or pane locks. The owner performs teardown without holding teardownMu;
	// duplicate daemon-shutdown callers can therefore bound their ownership wait
	// with Serve's shared snapshot deadline.
	teardownMu        sync.Mutex
	teardownActive    bool
	teardownDone      chan struct{}
	moveReservations  uint
	teardownWaiters   uint
	teardownChanged   chan struct{}
	lifecycleStopOnce sync.Once
	// sessionCore.mu guards tabs, active, attachment membership, restoreDone,
	// clipFiles, and clipboard queue state.
	// restoreDone remains attached to a restored session after it is published in
	// the live registry, so racing attaches cannot bypass restoration completion.
	restoreDone            chan struct{}
	themeMu                sync.Mutex
	tabs                   []*tab
	active                 int
	clipboardQueue         []clipboardForward
	clipboardWorkerRunning bool
	cwd                    string
	terminal               terminalEnv
	// env is the authoritative immutable environment snapshot for future PTY children.
	// It is guarded by mu and is always copied on ingress and egress.
	env          []string
	snapDirty    atomic.Bool
	snapEligible atomic.Bool
	// snapshotMu serializes mutation revisions and repository publication
	// generations with worker completion. It is intentionally independent from
	// mu: persistence never holds session state locks while encoding or writing.
	snapshotMu                        sync.Mutex
	snapshotGeneration                uint64 // newest mutation revision
	snapshotPublishedGeneration       uint64 // newest repository generation
	snapshotPublishedCheckpoint       *domain.CheckpointRef
	snapshotPublishedMutationRevision uint64
	snapshotNextEligibleAt            time.Time
	// The coordinator state below is guarded by snapshotMu. A capture can be
	// queued globally or in flight, never more than one of each per session.
	// Quarantine cancels the session publication context before destructive
	// repository operations and publicationDone joins a started publication.
	snapshotPending         bool
	snapshotPendingCaptures uint
	// snapshotForcedGeneration is the newest mutation revision a forced
	// checkpoint must publish. It survives an older routine capture so worker
	// completion can enqueue exactly one forced successor.
	snapshotForcedGeneration   uint64
	snapshotQueuedCapture      *snapshotCapture
	snapshotInFlightCapture    *snapshotCapture
	snapshotPublicationDone    chan struct{}
	snapshotPublicationContext context.Context
	snapshotPublicationCancel  context.CancelFunc
	snapshotQuarantined        bool
	// snapshotChunkCache retains encoded sealed history only for this named
	// session. snapshotMu owns it so cache touches never contend with pane
	// state or mutate bytes retained by queued/in-flight publications.
	snapshotChunkCache *snapshotChunkCache
	snapshotChanged    chan struct{}
	snapshotWake       chan struct{}
	// syncGen makes synchronized-output watchdog generations unique across all
	// panes in this session.
	syncGen atomic.Uint64
	// layoutApplyMu serializes whole-session resize prepare/apply/admit/publish
	// transactions. It is deliberately not an architecture lock and may span
	// external PTY.Resize calls.
	layoutApplyMu sync.Mutex
	// clipFiles records clipboard-image-transfer temp file paths (see
	// clipboard.go) written for this session, removed best-effort in
	// killSession.
	clipFiles []string

	// floatingLaunchMu is ownership synchronization for external floating
	// launches. It is separate from mu so PTYFactory.Open never holds a
	// session, tab, or daemon architecture lock.
	floatingLaunchMu       sync.Mutex
	floatingLaunchStopping bool
	floatingLaunches       map[*floatingLaunch]struct{}
}

// tab is a pane layout container; pane owns PTY/screen/scrollback/render scheduling state.
type tab struct {
	mu sync.Mutex // guards tree, panes, floating, nextPaneID, size, and pane map membership

	// layoutGeneration invalidates prepared geometry whenever live tab layout
	// state changes. layoutApplyMu serializes the lock-free PTY apply boundary.
	layoutGeneration uint64
	layoutApplyMu    sync.Mutex
	// layoutRetryMu owns the one bounded delayed retry worker for accepted
	// tiled-layout degradation. Its context is derived from ctx, so tab/session
	// teardown cancels a waiting retry before it can touch PTY state.
	layoutRetryMu      sync.Mutex
	layoutRetryCancel  context.CancelFunc
	layoutRetryRunning bool

	stableID   string
	name       string
	tree       *layout.Tree
	panes      map[layout.PaneID]*pane
	nextPaneID int
	size       domain.Size
	ctx        context.Context
	cancel     context.CancelFunc
	// floating is independent from the normal layout tree and pane map.
	floating floatingSlot

	// attention fields are guarded by the owning session.mu.
	attention             bool
	attentionAt           time.Time
	attentionVisiblePaint bool
}

// attachedClient is a client currently attached to a session's tab. rend is
// its private renderer shadow (so each client gets output minimised against
// what it has actually seen). sendMu serialises the two senders — the render
// coordinator and the connection handler — so the transport's single-writer

func (d *Daemon) touchMRU(entry attachmentSession) {
	if d == nil || entry == nil || entry.core() == nil {
		return
	}
	core := entry.core()
	seq := d.mruSeq.Add(1)
	for {
		old := core.mruAt.Load()
		if old >= seq {
			return
		}
		if core.mruAt.CompareAndSwap(old, seq) {
			break
		}
	}
	// Proxy recency is live derived state only. Local named sessions retain the
	// existing catalogue metadata publication semantics.
	sess, ok := localSession(entry)
	if !ok {
		return
	}
	sess.mu.Lock()
	if !sess.ephemeral {
		d.markCatalogueDirty(sess.persistRecordLocked(max(d.nowUnixNano(), sess.createdAt, int64(1))).MetadataUpdate())
	}
	sess.mu.Unlock()
}

func (d *Daemon) createSessionLocked(name string, ephemeral bool, cwd string, sz domain.Size, term terminalEnv, env []string, restoredTabNames ...[]string) (*session, error) {
	return d.createSessionLockedWithMode(name, ephemeral, cwd, sz, term, env, false, restoredTabNames...)
}

func (d *Daemon) createSessionLockedWithMode(name string, ephemeral bool, cwd string, sz domain.Size, term terminalEnv, env []string, proxied bool, restoredTabNames ...[]string) (*session, error) {
	env = copyEnvironment(env)
	if _, reserved := d.creating[name]; reserved {
		return nil, errSessionNameInUse
	}
	d.creating[name] = struct{}{}
	defer delete(d.creating, name)
	stopped, resuming := d.stopped[name]
	var authoritative domain.CatalogueRecord
	var authoritativeExists bool
	if !ephemeral && resuming && d.persistEnabled {
		d.mu.Unlock()
		var err error
		authoritative, authoritativeExists, err = d.catalogueRecord(name)
		d.mu.Lock()
		if err != nil {
			return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't read session catalogue", err)
		}
		if d.closing {
			return nil, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
		}
		stopped, resuming = d.stopped[name]
	}
	var createdAt int64
	var incarnation domain.IncarnationID
	if !ephemeral {
		if resuming {
			// A stopped session is the same lifecycle when resumed; it must retain
			// the persisted identity rather than receive a fresh timestamp.
			createdAt = stopped.createdAt
			incarnation = stopped.incarnation
			if authoritativeExists {
				createdAt = authoritative.CreatedAt
				incarnation = authoritative.IncarnationID
			}
			if createdAt > d.lastAllocatedCreatedAt {
				d.lastAllocatedCreatedAt = createdAt
			}
		} else {
			var err error
			createdAt, err = d.allocateLifecycleCreatedAtLocked()
			if err != nil {
				return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session", err)
			}
		}
		if incarnation == (domain.IncarnationID{}) {
			var err error
			incarnation, err = domain.NewIncarnationID(rand.Reader)
			if err != nil {
				return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session", fmt.Errorf("generate durable identity: %w", err))
			}
		}
	}

	tbSize := contentSize(sz, proxied)
	var names []string
	if len(restoredTabNames) > 0 {
		names = append([]string(nil), restoredTabNames[0]...)
	}
	tabs := make([]*tab, 0, max(1, len(names)))
	for i := range max(1, len(names)) {
		tabStableID, paneStableID, err := d.newTabPaneStableIDs()
		if err != nil {
			closeTabs(tabs)
			return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session", err)
		}
		command, args := d.ptyCommand(env)
		lifetime := d.newPaneProcessLifetime(d.serveCtx)
		pty, err := d.ptys.Open(lifetime.ctx, command, args, childEnvFrom(env, name, tabStableID, paneStableID, term), cwd, tbSize)
		if err != nil {
			lifetime.abort()
			if pty != nil {
				_ = pty.Close()
			}
			closeTabs(tabs)
			d.log.Warn("pty spawn failed", "err", err, "session", name, "kind", "session")
			return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session: shell failed to start", err)
		}
		tb := newTabWithStableID(tabStableID, paneStableID, pty, tbSize)
		if !lifetime.publish(tb.focusedPane()) {
			lifetime.abort()
			_ = pty.Close()
			closeTabs(tabs)
			return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session: shell failed to start", lifetime.ctx.Err())
		}
		if i < len(names) {
			tb.name = names[i]
		}
		tabs = append(tabs, tb)
	}

	id := domain.SessionID(fmt.Sprintf("sess-%d", d.nextID))
	d.nextID++

	sctx, cancel := context.WithCancel(d.serveCtx)
	for _, tb := range tabs {
		tb.ctx, tb.cancel = context.WithCancel(sctx)
	}
	lastUsedSeq := uint64(0)
	if !ephemeral && resuming {
		lastUsedSeq = stopped.lastUsedSeq
	}
	if lastUsedSeq == 0 {
		lastUsedSeq = d.mruSeq.Add(1)
	}
	sess := &session{
		sessionCore: sessionCore{
			id:          id,
			name:        name,
			ephemeral:   ephemeral,
			createdAt:   createdAt,
			incarnation: incarnation,
		},
		ctx:          sctx,
		cancel:       cancel,
		tabs:         tabs,
		cwd:          cwd,
		terminal:     term,
		env:          env,
		snapshotWake: d.snapshotWake,
	}
	if !ephemeral && name != "" {
		sess.snapshotChunkCache = newSnapshotChunkCache(snapshotChunkCacheLimit)
	}
	sess.mruAt.Store(lastUsedSeq)
	sess.snapEligible.Store(!ephemeral && name != "")
	// These tabs are still private construction state. Publish every tiled
	// owner before the session enters d.sessions or any PTY reader starts.
	for _, tb := range tabs {
		publishTiledPaneOwners(sess, tb)
	}
	var rollbackDurable func() error
	if !ephemeral && d.persistEnabled {
		record := domain.CatalogueRecord{Name: name, IncarnationID: incarnation, Cwd: cwd, CreatedAt: createdAt, UpdatedAt: createdAt, LastUsedSeq: lastUsedSeq, TabNames: names}
		if d.recovery == nil {
			closeTabs(tabs)
			cancel()
			return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session", errors.New("durable session authority is not configured"))
		}
		var err error
		if resuming && authoritativeExists {
			err = d.catalogue.UpdateMetadata(record.MetadataUpdate())
			if err == nil {
				rollbackDurable = func() error { return d.catalogue.Replace(name, authoritative) }
				err = d.catalogue.Sync()
			}
		} else {
			record, err = d.recovery.Create(d.serveCtx, record)
			if err == nil {
				rollbackDurable = func() error {
					current, ok, recordErr := d.catalogue.Record(name)
					if recordErr != nil {
						return recordErr
					}
					if !ok {
						return nil
					}
					if current.IncarnationID != record.IncarnationID {
						return errors.New("daemon: created session authority changed before rollback")
					}
					return d.catalogue.Delete(name)
				}
			}
		}
		if err != nil {
			if rollbackDurable != nil {
				err = errors.Join(err, rollbackDurable())
			}
			closeTabs(tabs)
			cancel()
			return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session", err)
		}
		sess.incarnation = record.IncarnationID
		if resuming && authoritativeExists && authoritative.Committed != nil && authoritative.IncarnationID == sess.incarnation {
			checkpoint := *authoritative.Committed
			sess.snapshotPublishedGeneration = checkpoint.Generation
			sess.snapshotPublishedCheckpoint = &checkpoint
		}
	}
	if !d.registerSessionLocked(sess) {
		err := errors.New("daemon: session registry rejected local session")
		if rollbackDurable != nil {
			err = errors.Join(err, rollbackDurable())
		}
		closeTabs(tabs)
		cancel()
		return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session", err)
	}
	if !ephemeral {
		// Keep stopped lifecycle authority intact until runtime publication and
		// its durable side effects have both succeeded.
		delete(d.stopped, name)
	}
	d.log.Info("session created", "session", name, "id", id, "ephemeral", ephemeral)
	for i, tb := range tabs {
		d.log.Info("tab created", "session", name, "tab", i)
		d.startTabGoroutines(sess, tb)
	}
	return sess, nil
}

func closeTabs(tabs []*tab) {
	for _, tb := range tabs {
		tb.closeAllPanes()
	}
}

func (d *Daemon) createSessionAndSwitch(from *session, ac *attachedClient, name string) error {
	if name == "" {
		return errSessionNameRequired
	}
	if err := domain.ValidateSessionName(name); err != nil {
		return err
	}
	sz := ac.size
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		return errors.New("daemon is shutting down")
	}
	if d.nameLiveOrStoppedLocked(name) {
		d.mu.Unlock()
		return errSessionNameInUse
	}
	from.mu.Lock()
	cwd := from.cwd
	term := from.terminal
	env := copyEnvironment(from.env)
	if from.client != ac {
		from.mu.Unlock()
		d.mu.Unlock()
		return errors.New("client detached")
	}
	from.mu.Unlock()

	newSess, err := d.createSessionLockedWithMode(name, false, cwd, sz, term, env, ac.proxied)
	d.mu.Unlock()
	if err != nil {
		return err
	}
	// The newly created target is registered before the role transition, but
	// remains headless. Freeze/drain must occur with no daemon/routing/session
	// lock held; the transition revalidates both lifecycles before publication.
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source:            from,
		target:            newSess,
		next:              ac,
		expectedRole:      attachmentActive,
		targetRole:        attachmentActive,
		expectedTransport: ac.transportSnapshot(),
		ready:             true,
	})
	if err != nil {
		_ = d.killSession(newSess, ports.ReasonSessionKilled, true)
		return err
	}

	d.touchMRU(newSess)
	ac.recordPreviousSession(from)
	d.log.Info("client attached", "session", newSess.name, "resume", ac.resumeCapable)
	d.deferAttachmentTransitionCleanups(transition)
	d.firstPaintForTransition(transition.published)
	return nil
}

// createSessionAndSwitchForRole creates no target until the initiating role
// ticket has ended and the exact source capability is frozen and revalidated.
func (d *Daemon) createSessionAndSwitchForRole(token attachmentRoleToken, name string) error {
	if name == "" {
		return errSessionNameRequired
	}
	if err := domain.ValidateSessionName(name); err != nil {
		return err
	}
	if token.sess == nil || token.ac == nil || token.effect == nil || token.role != attachmentActive {
		return errAttachmentTransition
	}
	token.effect.bindActionEnd(d, "create-session")
	token.effect.End()

	var created *session
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: token.sess, next: token.ac,
		expectedRole: attachmentActive, targetRole: attachmentActive,
		expectedTransport: token.transport, sourceToken: &token, action: "create-session",
		copySourceEnvironment: true, ready: true,
		createTargetLocked: func() (*session, error) {
			source, ok := localSession(token.sess)
			if !ok {
				return nil, errAttachmentTransition
			}
			if d.closing {
				return nil, errors.New("daemon is shutting down")
			}
			if d.nameLiveOrStoppedLocked(name) {
				return nil, errSessionNameInUse
			}
			source.mu.Lock()
			cwd, term, env := source.cwd, source.terminal, copyEnvironment(source.env)
			source.mu.Unlock()
			var createErr error
			created, createErr = d.createSessionLockedWithMode(name, false, cwd, token.ac.size, term, env, token.ac.proxied)
			return created, createErr
		},
	})
	if err != nil {
		if created != nil {
			_ = d.killSession(created, ports.ReasonSessionKilled, true)
		}
		return err
	}

	d.touchMRU(created)
	if source, ok := localSession(token.sess); ok {
		token.ac.recordPreviousSession(source)
	}
	d.log.Info("client attached", "session", created.name, "resume", token.ac.resumeCapable)
	d.deferAttachmentTransitionCleanups(transition)
	d.firstPaintForTransition(transition.published)
	return nil
}

func (d *Daemon) createTab(sess *session, sz domain.Size) error {
	sess.mu.Lock()
	name := sess.name
	cwd := sess.cwd
	client := sess.client
	term := sess.terminal
	env := copyEnvironment(sess.env)
	sess.mu.Unlock()
	tbSize := contentSize(sz, client != nil && client.proxied)
	tabStableID, paneStableID, err := d.newTabPaneStableIDs()
	if err != nil {
		return err
	}
	command, args := d.ptyCommand(env)
	lifetime := d.newPaneProcessLifetime(sess.ctx)
	pty, err := d.ptys.Open(lifetime.ctx, command, args, childEnvFrom(env, name, tabStableID, paneStableID, term), cwd, tbSize)
	if err != nil {
		lifetime.abort()
		if pty != nil {
			_ = pty.Close()
		}
		d.log.Warn("pty spawn failed", "err", err, "session", name, "kind", "tab")
		return domain.UserErr(domain.NoticeTabSpawn, "couldn't open tab: shell failed to start", err)
	}
	tb := newTabWithStableID(tabStableID, paneStableID, pty, tbSize)
	if client != nil {
		t := d.effectiveTheme(client.getClientTheme())
		tb.mu.Lock()
		p := tb.focusedPane()
		tb.mu.Unlock()
		if p != nil {
			p.mu.Lock()
			applyPaneThemeLocked(p, t, false)
			p.mu.Unlock()
		}
	}
	d.mu.Lock()
	if d.closing || d.sessions[sess.id] != sess || sess.ctx.Err() != nil {
		d.mu.Unlock()
		lifetime.abort()
		_ = pty.Close()
		return errors.New("daemon: session closed")
	}
	tb.ctx, tb.cancel = context.WithCancel(sess.ctx)
	sess.mu.Lock()
	p := tb.focusedPane()
	if !lifetime.publish(p) {
		sess.mu.Unlock()
		d.mu.Unlock()
		lifetime.abort()
		_ = pty.Close()
		return errors.New("daemon: session closed")
	}
	// Publish ownership before appending the tab to the visible session.
	publishPaneOwner(p, sess, tb, 0)
	sess.tabs = append(sess.tabs, tb)
	sess.active = len(sess.tabs) - 1
	tabIndex := sess.active
	if !sess.ephemeral {
		d.markCatalogueDirty(sess.persistRecordLocked(max(d.nowUnixNano(), sess.createdAt, int64(1))).MetadataUpdate())
	}
	sess.mu.Unlock()
	d.mu.Unlock()
	d.log.Info("tab created", "session", name, "tab", tabIndex)
	d.startTabGoroutines(sess, tb)
	d.activateTab(sess, tb)
	markSnapshotDirty(sess)
	return nil
}

func newTab(pty ports.PTY, sz domain.Size) *tab {
	return newTabWithStableID(fallbackStableID("t"), fallbackStableID("p"), pty, sz)
}

func newTabWithStableID(tabStableID, paneStableID string, pty ports.PTY, sz domain.Size) *tab {
	id := layout.PaneID("pane-1")
	p := newPaneWithStableID(id, paneStableID, pty, sz)
	return &tab{
		stableID:   tabStableID,
		tree:       layout.NewTree(id),
		panes:      map[layout.PaneID]*pane{id: p},
		nextPaneID: 2,
		size:       sz,
	}
}

func (tb *tab) bumpLayoutGenerationLocked() {
	tb.layoutGeneration++
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
	// Invalidate before cancellation so a concurrently unblocked floating
	// reader cannot reap a subsequently reused slot.
	floating := tb.takeFloatingLocked()
	tb.mu.Unlock()
	if floating != nil {
		panes = append(panes, floating)
	}
	for _, p := range panes {
		closePaneProcess(p)
	}
}

// tabChromeRows is the vertical chrome (tab bar + status bar) between a
// client viewport and a tab's content area.
const tabChromeRows = 2

func tabSize(clientSize domain.Size) domain.Size {
	return contentSize(clientSize, false)
}

// contentSize maps the negotiated viewport to PTY geometry. A proxied client
// has already removed its local chrome rows, so the remote daemon must consume
// the received size verbatim instead of subtracting chrome a second time.
func contentSize(clientSize domain.Size, proxied bool) domain.Size {
	if !clientSize.Valid() {
		clientSize = defaultSize
	}
	if proxied {
		return clientSize
	}
	rows := max(clientSize.Rows-tabChromeRows, 1)
	return domain.Size{Cols: clientSize.Cols, Rows: rows}
}

// fullViewportSize derives a full client-equivalent viewport from the active
// tab's retained content size, for actions that need a viewport while no
// client is attached. It falls back to defaultSize when no valid size exists.
func (s *session) fullViewportSize() domain.Size {
	s.mu.Lock()
	var tb *tab
	if s.active >= 0 && s.active < len(s.tabs) {
		tb = s.tabs[s.active]
	}
	s.mu.Unlock()

	var content domain.Size
	if tb != nil {
		tb.mu.Lock()
		content = tb.size
		tb.mu.Unlock()
	}
	if !content.Valid() {
		return defaultSize
	}
	return domain.Size{Cols: content.Cols, Rows: content.Rows + tabChromeRows}
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
	if p == nil {
		return
	}
	owner := p.ownerSnapshot()
	if owner == nil {
		// Teardown may win after publication but before reader startup. In that
		// case the process is already closed and no ownerless reader may start.
		return
	}
	if owner.session != sess || owner.tab != tb {
		panic("daemon: starting pane reader for a different published owner")
	}
	sess.mu.Lock()
	name := sess.name
	sess.mu.Unlock()
	d.log.Info("pane created", "session", name, "pane", p.id)
	// Scheduler ownership was removed; this launch creates exactly one reader.
	d.sessWg.Add(1)
	go d.readPanePTY(p)
}

// attachClient makes ac the session's current client, displacing any prior one
// (which is notified with ReasonDetach and disconnected — its own conn handler

func (s *session) detachIfCurrent(ac *attachedClient) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == ac {
		s.client = nil
		s.unregisterAttachmentLocked(ac)
		ac.setSession(nil)
		return true
	}
	return false
}

// detachIfCurrent publishes terminal role invalidation through the attachment
// gate. Freeze/drain happens before routing or session locks are acquired.
func (d *Daemon) detachIfCurrent(sess *session, ac *attachedClient) bool {
	return d.detachIfCurrentTransport(sess, ac, transportSnapshot{})
}

// detachIfCurrentTransport is detachIfCurrent with an optional exact-link
// fence. When expected.transport is non-nil, a rebound incarnation of the same
// attachment object cannot be detached by a stale connection loop.
func (d *Daemon) detachIfCurrentTransport(sess *session, ac *attachedClient, expected transportSnapshot) bool {
	if sess == nil || ac == nil {
		return false
	}
	frozen := freezeRoleEffectGates(ac)
	defer frozen.unfreeze()
	if d.afterDetachRoleEffectsFrozen != nil {
		d.afterDetachRoleEffectsFrozen()
	}
	d.notices.routingMu.Lock()
	sess.mu.Lock()
	current := sess.client == ac && ac.currentSession() == sess
	if current && expected.transport != nil && !ac.transportSnapshotCurrent(expected) {
		current = false
	}
	if current {
		sess.client = nil
		sess.unregisterAttachmentLocked(ac)
		ac.roleGeneration.Add(1)
		ac.setSession(nil)
		ac.invalidateFrozenRoleCapability()
	}
	sess.mu.Unlock()
	d.notices.routingMu.Unlock()
	return current
}

// detachProxyIfCurrentTransport is detachIfCurrentTransport for a proxy
// attachment. It fences on the same published identity — exact session pointer,
// exact attachment, exact transport incarnation — but reads the attachment
// through the shared sessionCore rather than a local session.
func (d *Daemon) detachProxyIfCurrentTransport(p *proxySession, ac *attachedClient, expected transportSnapshot) bool {
	if p == nil || ac == nil {
		return false
	}
	frozen := freezeRoleEffectGates(ac)
	defer frozen.unfreeze()
	if d.afterDetachRoleEffectsFrozen != nil {
		d.afterDetachRoleEffectsFrozen()
	}
	d.notices.routingMu.Lock()
	p.sessionCore.mu.Lock()
	current := p.client == ac && ac.currentAttachmentSession() == attachmentSession(p)
	if current && expected.transport != nil && !ac.transportSnapshotCurrent(expected) {
		current = false
	}
	if current {
		p.client = nil
		unregisterAttachmentSessionLocked(attachmentSession(p), ac)
		ac.roleGeneration.Add(1)
		ac.setSession(nil)
		ac.invalidateFrozenRoleCapability()
	}
	p.sessionCore.mu.Unlock()
	d.notices.routingMu.Unlock()
	if current {
		d.armProxyWarm(p)
	}
	return current
}

// detachIfRoleCurrent invalidates only the exact role generation and transport
// whose admitted send failed. A queued stale error cannot detach a later role
// publication or a rebound transport on the same attachment object.
func (d *Daemon) detachIfRoleCurrent(token attachmentRoleToken) bool {
	return d.detachIfRoleCurrentUntil(token, nil)
}

func (d *Daemon) detachIfRoleCurrentUntil(token attachmentRoleToken, done func() <-chan struct{}) bool {
	if token.sess == nil || token.ac == nil || token.role != attachmentActive || token.transport.transport == nil {
		return false
	}
	frozen := freezeRoleEffectGatesWith(roleEffectFreezeOptions{done: done}, token.ac)
	defer frozen.unfreeze()
	if !frozen.acquired || !frozen.drained {
		return false
	}
	if d.afterDetachRoleEffectsFrozen != nil {
		d.afterDetachRoleEffectsFrozen()
	}
	d.mu.Lock()
	core := token.sess.core()
	if core == nil || d.sessions[core.id] != token.sess {
		d.mu.Unlock()
		return false
	}
	d.notices.routingMu.Lock()
	core.mu.Lock()
	coordinator := core.coordinator.Load()
	if coordinator != nil {
		coordinator.mu.Lock()
	}
	req := attachmentTransitionRequest{
		source: token.sess, next: token.ac, expectedRole: token.role,
		expectedTransport: token.transport,
	}
	current := coordinator != nil && transitionSourceTokenCurrentLocked(token, token.sess, coordinator, req)
	if current {
		core.client = nil
		unregisterAttachmentSessionLocked(token.sess, token.ac)
		token.ac.roleGeneration.Add(1)
		token.ac.setSession(nil)
		token.ac.invalidateFrozenRoleCapability()
	}
	if coordinator != nil {
		coordinator.mu.Unlock()
	}
	core.mu.Unlock()
	d.notices.routingMu.Unlock()
	d.mu.Unlock()
	return current
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
	if idx < 0 || idx >= len(s.tabs) || idx == s.active {
		s.mu.Unlock()
		return false
	}
	s.active = idx
	s.mu.Unlock()
	markSnapshotDirty(s)
	return true
}

func (s *session) switchRelative(delta int) bool {
	s.mu.Lock()
	if len(s.tabs) < 2 {
		s.mu.Unlock()
		return false
	}
	s.active = (s.active + delta + len(s.tabs)) % len(s.tabs)
	s.mu.Unlock()
	markSnapshotDirty(s)
	return true
}

func (d *Daemon) renameSession(sess *session, name string) error {
	if name == "" {
		return errSessionNameRequired
	}
	if err := domain.ValidateSessionName(name); err != nil {
		return err
	}

	// Hold daemon then session state across the durable identity write. The
	// incarnation-keyed snapshot namespace is never relocated.
	d.mu.Lock()
	if taken := d.findByNameLocked(name); taken != nil && taken != sess {
		d.mu.Unlock()
		return errSessionNameInUse
	}
	sess.mu.Lock()
	if stopped, ok := d.stopped[name]; ok && stopped.name != sess.name {
		sess.mu.Unlock()
		d.mu.Unlock()
		return errSessionNameInUse
	}
	oldName := sess.name
	wasEphemeral := sess.ephemeral
	createdAt := sess.createdAt
	priorIncarnation := sess.incarnation
	if wasEphemeral {
		var err error
		createdAt, err = d.allocateLifecycleCreatedAtLocked()
		if err != nil {
			sess.mu.Unlock()
			d.mu.Unlock()
			return err
		}
		sess.incarnation, err = domain.NewIncarnationID(rand.Reader)
		if err != nil {
			sess.mu.Unlock()
			d.mu.Unlock()
			return fmt.Errorf("generate durable identity: %w", err)
		}
	}
	lastUsedSeq := sess.mruAt.Load()
	record := sess.persistRecordLocked(max(d.nowUnixNano(), sess.createdAt, int64(1)))
	record.Name = name
	record.CreatedAt = createdAt
	record.LastUsedSeq = lastUsedSeq
	rollback := func(err error) error {
		sess.incarnation = priorIncarnation
		sess.mu.Unlock()
		d.mu.Unlock()
		return err
	}
	// Durable named renames are one atomic catalogue batch. The incarnation-keyed
	// snapshot namespace is intentionally untouched.
	if (wasEphemeral || oldName != name) && d.persistEnabled {
		if d.recovery == nil {
			return rollback(errors.New("durable session authority is not configured"))
		}
		{
			var committed domain.CatalogueRecord
			var err error
			if wasEphemeral {
				committed, err = d.recovery.Create(d.serveCtx, record)
			} else {
				committed, err = d.recovery.Rename(d.serveCtx, oldName, name)
			}
			if err != nil {
				return rollback(err)
			}
			record = committed
		}
	}

	delete(d.stopped, oldName)
	delete(d.stopped, name)
	sess.name = name
	sess.createdAt = createdAt
	sess.incarnation = record.IncarnationID
	sess.ephemeral = false
	sess.snapEligible.Store(name != "")
	sess.mu.Unlock()
	d.mu.Unlock()
	if wasEphemeral {
		sess.snapshotMu.Lock()
		sess.snapshotChunkCache = newSnapshotChunkCache(snapshotChunkCacheLimit)
		sess.snapshotMu.Unlock()
	}
	markSnapshotDirty(sess)
	return nil
}

func (d *Daemon) renameTab(sess *session, tb *tab, name string) error {
	if tb == nil {
		return errors.New("tab required")
	}
	d.mu.Lock()
	if d.sessions[sess.id] != sess {
		d.mu.Unlock()
		return errors.New("daemon: session closed")
	}
	sess.mu.Lock()
	if !slices.Contains(sess.tabs, tb) {
		sess.mu.Unlock()
		d.mu.Unlock()
		return errors.New("tab not found")
	}
	tb.name = name
	if !sess.ephemeral {
		d.markCatalogueDirty(sess.persistRecordLocked(max(d.nowUnixNano(), sess.createdAt, int64(1))).MetadataUpdate())
	}
	sess.mu.Unlock()
	d.mu.Unlock()
	return nil
}

func (s *session) persistRecordLocked(updatedAt int64) domain.CatalogueRecord {
	createdAt := s.createdAt
	tabNames := make([]string, len(s.tabs))
	lastCustom := -1
	for i, tb := range s.tabs {
		tabNames[i] = tb.name
		if tb.name != "" {
			lastCustom = i
		}
	}
	if lastCustom == -1 {
		tabNames = nil
	} else {
		tabNames = tabNames[:lastCustom+1]
	}
	return domain.CatalogueRecord{Name: s.name, IncarnationID: s.incarnation, Cwd: s.cwd, CreatedAt: createdAt, UpdatedAt: updatedAt, LastUsedSeq: s.mruAt.Load(), TabNames: tabNames}
}

func (d *Daemon) closeTab(sess *session, tb *tab, repaint bool) error {
	if sess == nil || tb == nil {
		return layout.ErrNotFound
	}
	d.mu.Lock()
	if d.sessions[sess.id] != sess {
		d.mu.Unlock()
		return errors.New("daemon: session closed")
	}
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
		d.mu.Unlock()
		return errors.New("tab not found")
	}
	if len(sess.tabs) == 1 {
		name := sess.name
		sess.mu.Unlock()
		d.mu.Unlock()
		if err := d.killSession(sess, ports.ReasonSessionKilled, false); err != nil {
			d.log.Warn("closing last tab failed", "session", name, "err", err)
			d.reportError(sess, domain.UserErr(domain.NoticeSnapshotSaturated,
				"couldn't close tab: session state not yet saved; try again", err))
			return err
		}
		d.log.Info("tab closed", "session", name, "last", true)
		return nil
	}
	ringing := tb.attention
	wasActive := idx == sess.active
	sess.tabs = append(sess.tabs[:idx], sess.tabs[idx+1:]...)
	if sess.active >= len(sess.tabs) {
		sess.active = len(sess.tabs) - 1
	} else if idx < sess.active {
		sess.active--
	}
	destination := sess.tabs[sess.active]
	ac := sess.client
	name := sess.name
	if !sess.ephemeral {
		d.markCatalogueDirty(sess.persistRecordLocked(max(d.nowUnixNano(), sess.createdAt, int64(1))).MetadataUpdate())
	}
	sess.mu.Unlock()
	d.mu.Unlock()
	d.log.Info("tab closed", "session", name)
	markSnapshotDirty(sess)

	d.clearDestroyedTabPreview(tb)
	tb.mu.Lock()
	panes := tb.panesSnapshot()
	tb.mu.Unlock()
	if ac != nil {
		ac.pruneCaptureFrames(panes...)
	}
	if tb.cancel != nil {
		tb.cancel()
	}
	d.teardownFloating(tb, ac)
	if rc := sess.renderCoordinator(); rc != nil {
		for _, p := range panes {
			rc.noteSyncPaneRemoved(p)
		}
	}
	tb.closeAllPanes()
	if wasActive {
		d.activateTab(sess, destination)
	}
	if repaint && ac != nil {
		d.invalidateRender(sess, ac, true, "session.go")
	}
	if ringing {
		d.repaintAllAttachedClients()
	}
	return nil
}

// ptyReader drains child output into the VT screen and pokes the dirty channel
// (non-blocking: a full channel already means a render is pending). On any read

// beginSnapshotPurge commits a logical named-session kill before its live
// identity or persisted metadata is removed. It never runs under daemon or
// session locks.
func (d *Daemon) beginSnapshotPurge(_ string, _ domain.IncarnationID) error {
	if d.persistEnabled && d.recovery == nil {
		return errors.New("durable session authority is not configured")
	}
	// The lifecycle coordinator performs the complete ordered protocol after
	// snapshot writers have joined.
	return nil
}

// finishSnapshotPurge removes catalogue metadata first, then deletes the
// incarnation directory. Startup garbage collection removes the directory if
// the second step is interrupted.
func (d *Daemon) finishSnapshotPurge(ctx context.Context, name string, _ domain.IncarnationID) error {
	if !d.persistEnabled {
		return nil
	}
	if d.recovery == nil {
		return errors.New("durable session authority is not configured")
	}
	if ctx == nil {
		ctx = d.serveCtx
	}
	purgeCtx, cancel := context.WithTimeout(ctx, snapshotFinalFlushTimeout)
	defer cancel()
	return d.recovery.Delete(purgeCtx, name)
}

// retryStoppedPurge completes a previously closed session's durable purge.
// It keeps the stopped record hidden until all sources and metadata cross their
// deletion boundaries.
func (d *Daemon) retryStoppedPurge(name string) error {
	return d.retryStoppedPurgeContext(d.serveCtx, name)
}

func (d *Daemon) retryStoppedPurgeContext(ctx context.Context, name string) error {
	d.mu.Lock()
	stopped, ok := d.stopped[name]
	if !ok {
		d.mu.Unlock()
		return nil
	}
	stopped.purging = true
	d.stopped[name] = stopped

	if err := d.beginSnapshotPurge(name, stopped.incarnation); err != nil {
		d.mu.Unlock()
		return err
	}
	if err := d.finishSnapshotPurge(ctx, name, stopped.incarnation); err != nil {
		d.mu.Unlock()
		return err
	}
	if current, ok := d.stopped[name]; ok && current.purging {
		delete(d.stopped, name)
	}
	d.mu.Unlock()
	return nil
}

func (d *Daemon) killSession(sess *session, reason uint8, purge bool) error {
	return d.killSessionWithSnapshotDeadline(sess, reason, purge, nil, nil)
}

type sessionKillAdmission struct {
	token  attachmentRoleToken
	action string
}

type sessionKillParticipants struct {
	active      *attachedClient
	snatched    map[*attachedClient]struct{}
	transports  map[*attachedClient]transportSnapshot
	roleGates   []*attachedClient
	interrupts  []roleTransportInterrupt
	source      *session
	sourceToken *attachmentRoleToken
	target      *session
}

func (d *Daemon) killSessionForRole(sess *session, reason uint8, purge bool, token attachmentRoleToken, action string) error {
	return d.killSessionWithSnapshotDeadline(sess, reason, purge, nil, &sessionKillAdmission{token: token, action: action})
}

// snapshotSessionKillParticipants captures role membership under the canonical
// architecture locks, but releases every lock before any role gate is frozen or
// drained. The initiator is part of the same deduplicated global gate set even
// when it belongs to a different source session.
func (d *Daemon) snapshotSessionKillParticipants(target *session, admission *sessionKillAdmission) (sessionKillParticipants, bool) {
	snapshot := sessionKillParticipants{target: target}
	if admission != nil {
		token := admission.token
		if token.sess == nil || token.ac == nil || token.role != attachmentActive || token.transport.transport == nil {
			return sessionKillParticipants{}, false
		}
		var ok bool
		snapshot.source, ok = localSession(token.sess)
		if !ok {
			return sessionKillParticipants{}, false
		}
		snapshot.sourceToken = &admission.token
	}

	d.mu.Lock()
	if d.sessions[target.id] != target || snapshot.source != nil && d.sessions[snapshot.source.id] != snapshot.source {
		d.mu.Unlock()
		return sessionKillParticipants{}, false
	}
	d.notices.routingMu.Lock()
	source := snapshot.source
	if source == nil {
		source = target
	}
	unlockSessions := lockAttachmentSessions(source, target)
	snapshot.active = target.client
	snapshot.snatched = make(map[*attachedClient]struct{}, len(target.snatched))
	snapshot.transports = make(map[*attachedClient]transportSnapshot, 1+len(target.snatched))
	if snapshot.active != nil {
		snapshot.transports[snapshot.active] = snapshot.active.transportSnapshot()
	}
	for ac := range target.snatched {
		snapshot.snatched[ac] = struct{}{}
		snapshot.transports[ac] = ac.transportSnapshot()
	}
	unlockSessions()
	d.notices.routingMu.Unlock()
	d.mu.Unlock()

	seen := make(map[*attachedClient]struct{}, 2+len(snapshot.snatched))
	add := func(ac *attachedClient) {
		if ac == nil {
			return
		}
		if _, duplicate := seen[ac]; duplicate {
			return
		}
		seen[ac] = struct{}{}
		snapshot.roleGates = append(snapshot.roleGates, ac)
	}
	if admission != nil {
		add(admission.token.ac)
	}
	add(snapshot.active)
	for ac := range snapshot.snatched {
		add(ac)
	}
	for ac, transport := range snapshot.transports {
		if transport.transport != nil {
			snapshot.interrupts = append(snapshot.interrupts, roleTransportInterrupt{ac: ac, transport: transport})
		}
	}
	return snapshot, true
}

// sessionKillParticipantsCurrent repeats lifecycle, source-token, and exact
// target-membership validation while every discovered role gate is frozen.
func (d *Daemon) sessionKillParticipantsCurrent(snapshot sessionKillParticipants) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sessions[snapshot.target.id] != snapshot.target ||
		snapshot.source != nil && d.sessions[snapshot.source.id] != snapshot.source {
		return false
	}
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	source := snapshot.source
	if source == nil {
		source = snapshot.target
	}
	sourceCoordinator := source.renderCoordinator()
	targetCoordinator := snapshot.target.renderCoordinator()
	unlockSessions := lockAttachmentSessions(source, snapshot.target)
	defer unlockSessions()
	unlockCoordinators := lockAttachmentCoordinators(source, sourceCoordinator, snapshot.target, targetCoordinator)
	defer unlockCoordinators()
	return sessionKillParticipantsCurrentLocked(snapshot, sourceCoordinator)
}

// sessionKillParticipantsCurrentLocked requires the ordered source/target
// session and coordinator locks.
func sessionKillParticipantsCurrentLocked(snapshot sessionKillParticipants, sourceCoordinator *renderCoordinator) bool {
	target := snapshot.target
	if target.client != snapshot.active || len(target.snatched) != len(snapshot.snatched) {
		return false
	}
	for ac := range snapshot.snatched {
		if _, current := target.snatched[ac]; !current {
			return false
		}
	}
	for ac, transport := range snapshot.transports {
		if !ac.transportSnapshotCurrent(transport) {
			return false
		}
	}
	if snapshot.sourceToken == nil {
		return true
	}
	token := *snapshot.sourceToken
	req := attachmentTransitionRequest{
		source: token.sess, next: token.ac, expectedRole: token.role,
		expectedTransport: token.transport,
	}
	return transitionSourceTokenCurrentLocked(token, token.sess, sourceCoordinator, req)
}

// stopInMemoryLifecycle cancels every session producer and closes every PTY
// independently of teardown ownership. Repository work may remain blocked, but
// repeated shutdown passes cannot leave pane readers or launches running.
func (s *session) stopInMemoryLifecycle() {
	if s == nil {
		return
	}
	s.lifecycleStopOnce.Do(func() {
		s.stopFloatingLaunches()
		if s.cancel != nil {
			s.cancel()
		}
		s.mu.Lock()
		tabs := append([]*tab(nil), s.tabs...)
		s.mu.Unlock()
		for _, tb := range tabs {
			tb.closeAllPanes()
		}
	})
}

func (s *session) teardownChangeLocked() chan struct{} {
	if s.teardownChanged == nil {
		s.teardownChanged = make(chan struct{})
	}
	return s.teardownChanged
}

func (s *session) signalTeardownChangedLocked() {
	close(s.teardownChangeLocked())
	s.teardownChanged = make(chan struct{})
}

// beginTeardown waits for ownership without holding teardownMu across teardown.
// A false result means Serve's shared shutdown budget expired while another
// lifecycle path owned the session. Ordinary callers wait and retry ownership,
// preserving failed-teardown and stopped-purge retry semantics.
func (s *session) beginTeardown(deadline *snapshotShutdownDeadline) bool {
	for {
		if deadline != nil {
			select {
			case <-deadline.Done():
				return false
			default:
			}
		}

		s.teardownMu.Lock()
		if !s.teardownActive && s.moveReservations == 0 {
			if deadline != nil {
				select {
				case <-deadline.Done():
					s.teardownMu.Unlock()
					return false
				default:
				}
			}
			s.teardownActive = true
			s.teardownDone = make(chan struct{})
			s.signalTeardownChangedLocked()
			s.teardownMu.Unlock()
			return true
		}
		s.teardownWaiters++
		s.signalTeardownChangedLocked()
		changed := s.teardownChangeLocked()
		s.teardownMu.Unlock()

		timedOut := false
		if deadline == nil {
			<-changed
		} else {
			select {
			case <-changed:
			case <-deadline.Done():
				timedOut = true
			}
		}
		s.teardownMu.Lock()
		s.teardownWaiters--
		s.signalTeardownChangedLocked()
		s.teardownMu.Unlock()
		if timedOut {
			return false
		}
	}
}

func (s *session) finishTeardown() {
	s.teardownMu.Lock()
	s.teardownActive = false
	close(s.teardownDone)
	s.teardownDone = nil
	s.signalTeardownChangedLocked()
	s.teardownMu.Unlock()
}

// killSessionWithSnapshotDeadline shares Serve's shutdown budget with its
// terminal checkpoint and coordinator join. A timed-out repository call keeps
// only immutable capture state; it never observes closed worker channels or
// session-owned cache that teardown has released.
func (d *Daemon) killSessionWithSnapshotDeadline(sess *session, reason uint8, purge bool, deadline *snapshotShutdownDeadline, admission *sessionKillAdmission) error {
	if sess == nil {
		return nil
	}
	// Discover every role whose membership can be invalidated before freezing
	// any one of them. The common gate helper deduplicates and freezes the whole
	// immutable identity set in the same global order used by transitions.
	participants, ok := d.snapshotSessionKillParticipants(sess, admission)
	if !ok {
		return nil
	}
	if admission != nil {
		d.endActionRoleEffect(admission.token.effect, admission.action)
	}
	action := ""
	if admission != nil {
		action = admission.action
	}
	if d.afterRoleEffectParticipantsSnapshotted != nil {
		d.afterRoleEffectParticipantsSnapshotted(action, participants.roleGates)
	}
	var drainDone func() <-chan struct{}
	var drainDeadline *roleEffectDrainDeadline
	if deadline != nil {
		drainDone = deadline.Done
	} else {
		drainDeadline = newRoleEffectDrainDeadline(d.clock)
		drainDone = drainDeadline.Done
		defer drainDeadline.stop()
	}
	frozen := freezeRoleEffectGatesWith(roleEffectFreezeOptions{interrupts: participants.interrupts, done: drainDone, afterFrozen: func(ac *attachedClient) {
		if d.afterRoleEffectGateFrozen != nil {
			d.afterRoleEffectGateFrozen(action, ac)
		}
	}}, participants.roleGates...)
	defer func() { frozen.unfreeze() }()
	if !frozen.acquired {
		// The acquisition helper has already rolled back exactly the gates it
		// owned. Do not enter architecture locks or publish terminal ownership
		// changes unless the complete canonical participant set is frozen.
		return nil
	}
	if !d.sessionKillParticipantsCurrent(participants) {
		return nil
	}

	// Pane EOF, explicit close, and daemon shutdown can all converge on the same
	// session. Only one owner may create the terminal mutation and checkpoint;
	// later callers observe the completed registry transition instead of
	// manufacturing a newer generation that the owner will quarantine. A
	// shutdown duplicate abandons this wait when the shared budget expires.
	if !sess.beginTeardown(deadline) {
		return nil
	}
	defer sess.finishTeardown()

	d.mu.Lock()
	current := d.sessions[sess.id]
	d.mu.Unlock()
	if current != sess {
		if purge {
			sess.mu.Lock()
			name := sess.name
			sess.mu.Unlock()
			return d.retryStoppedPurge(name)
		}
		return nil
	}

	ringing := sess.anyAttention()
	sess.mu.Lock()
	isEphemeral := sess.ephemeral
	name := sess.name
	incarnation := sess.incarnation
	sess.mu.Unlock()
	// Join snapshot publication before changing live identity or deleting the
	// incarnation directory, so an older publication cannot recreate it.
	if purge && !isEphemeral {
		if err := d.beginSnapshotPurge(name, incarnation); err != nil {
			return err
		}
		if d.snapshotRepository != nil {
			<-quarantineSnapshotCoordinator(sess)
		}
	}
	if !purge && !isEphemeral && d.persistEnabled {
		d.refreshSessionCwd(sess)
	}
	var terminalSnapshotErr error
	retainSnapshotRetry := false
	if d.snapsEnabled && !isEphemeral && !purge {
		markSnapshotDirty(sess)
		sess.snapshotMu.Lock()
		terminalGeneration := sess.snapshotGeneration
		sess.snapshotMu.Unlock()
		if !d.scheduleFinalSnapshot(sess) || !d.waitForSnapshotGenerationWithDeadline(sess, terminalGeneration, deadline) {
			sess.mu.Lock()
			name := sess.name
			sess.mu.Unlock()
			terminalSnapshotErr = fmt.Errorf("retain final snapshot for session %q: snapshot worker unavailable, saturated, or timed out", name)
			if reason != ports.ReasonServerShutdown {
				return terminalSnapshotErr
			}
			// Preserve any already-queued routine capture until the blocked
			// worker can safely discard it. This retains the dirty generation
			// and forced retry intent without adding another queue entry.
			retainSnapshotRetry = true
			d.persistShutdownSnapshotFailure(name, terminalSnapshotErr)
		}
	}
	if !isEphemeral {
		// Joining first preserves byte ownership for a publication that is still
		// encoding while this session is being torn down. Once Serve's shared
		// budget expires, the worker is cancelled by its one final join instead;
		// keep the cache reachable because an uncooperative writer may still be
		// encoding immutable state.
		var quarantineDone <-chan struct{}
		if retainSnapshotRetry {
			quarantineDone = quarantineSnapshotCoordinatorRetainingQueuedCapture(sess)
		} else {
			quarantineDone = quarantineSnapshotCoordinator(sess)
		}
		joined := true
		if deadline != nil {
			select {
			case <-quarantineDone:
			case <-deadline.Done():
				joined = false
			}
		} else {
			<-quarantineDone
		}
		if joined {
			sess.snapshotMu.Lock()
			sess.snapshotChunkCache = nil
			sess.snapshotMu.Unlock()
		}
	}
	// Revalidate the exact source capability, target lifecycle, and active plus
	// snatched memberships under canonical architecture locks immediately before
	// publication. A changed preflight aborts without mutating either lifecycle.
	d.mu.Lock()
	if d.sessions[sess.id] != sess || participants.source != nil && d.sessions[participants.source.id] != participants.source {
		d.mu.Unlock()
		return nil
	}
	d.notices.routingMu.Lock()
	source := participants.source
	if source == nil {
		source = sess
	}
	sourceCoordinator := source.renderCoordinator()
	targetCoordinator := sess.renderCoordinator()
	unlockSessions := lockAttachmentSessions(source, sess)
	unlockCoordinators := lockAttachmentCoordinators(source, sourceCoordinator, sess, targetCoordinator)
	if !sessionKillParticipantsCurrentLocked(participants, sourceCoordinator) {
		unlockCoordinators()
		unlockSessions()
		d.notices.routingMu.Unlock()
		d.mu.Unlock()
		return nil
	}

	// d.mu -> routingMu -> ordered sessions -> ordered coordinators are held
	// here. Registry removal and every role invalidation are one publication
	// while the complete immutable gate set remains frozen.
	if !d.unregisterSessionLocked(sess) {
		unlockCoordinators()
		unlockSessions()
		d.notices.routingMu.Unlock()
		d.mu.Unlock()
		return errAttachmentTransition
	}
	d.clearBarScriptsForSession(sess.id)
	d.purgeParkingForSessionLocked(sess)
	parkedRetirements := d.purgeParkedForSessionLocked(sess)
	detachedActive := sess.client
	attachments := make([]detachedAttachmentSnapshot, 0, len(participants.roleGates))
	capture := func(attached *attachedClient) {
		if attached == nil {
			return
		}
		attachments = append(attachments, detachedAttachmentSnapshot{ac: attached, transport: participants.transports[attached]})
		attached.roleGeneration.Add(1)
		attached.setSession(nil)
		attached.invalidateFrozenRoleCapability()
	}
	capture(sess.client)
	for snatched := range sess.snatched {
		capture(snatched)
	}
	sess.client = nil
	clear(sess.snatched)
	stoppedName := sess.name
	stoppedCwd := sess.cwd
	createdAt := sess.createdAt
	tabNames := sess.persistRecordLocked(max(d.nowUnixNano(), sess.createdAt, int64(1))).TabNames
	ephemeral := sess.ephemeral
	unlockCoordinators()
	unlockSessions()
	d.notices.routingMu.Unlock()
	if !ephemeral {
		stopped := stoppedSession{name: stoppedName, cwd: stoppedCwd, createdAt: createdAt, incarnation: incarnation, lastUsedSeq: sess.mruAt.Load(), tabNames: tabNames, purging: purge}
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
	d.finishParkedAttachmentRetirements(parkedRetirements)
	frozen.unfreeze()
	frozen = frozenRoleEffectGates{}
	d.log.Info("session closed", "session", stoppedName, "id", sess.id, "ephemeral", ephemeral, "purge", purge, "reason", reason)

	var purgeErr error
	if rc := sess.renderCoordinator(); rc != nil {
		// Terminal teardown has two phases: this session owner first prevents any
		// new worker registration and detaches all tokens, then stops and waits
		// outside coordinator callbacks and session locks.
		rc.beginSessionTeardown().finish()
		rc.waitForTimerWorkers()
	}
	for _, attachment := range attachments {
		d.unregisterPreview(attachment.ac)
		attachment.ac.clearPreviousSession()
		attachment.ac.clearCaptureFrames()
	}

	// Prevent queued launches from entering Open, cancel the parent context, and
	// close PTYs. Serve may already have performed this phase while repository
	// work owned by this teardown was blocked.
	sess.stopInMemoryLifecycle()
	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	clipFiles := sess.clipFiles
	sess.clipFiles = nil
	sess.mu.Unlock()
	for _, tb := range tabs {
		d.clearDestroyedTabPreview(tb)
		d.teardownFloating(tb, detachedActive)
	}
	for _, path := range clipFiles {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			d.log.Warn("removing clipboard temp file failed", "err", err, "path", path)
		}
	}
	// The coordinator and all panes are now stopped, so no producer can publish
	// another generation after this destructive source deletion. Keep the hidden
	// stopped record when deletion fails so a repeated live kill can retry.
	if !ephemeral && purge {
		d.mu.Lock()
		sess.mu.Lock()
		err := d.finishSnapshotPurge(d.serveCtx, stoppedName, incarnation)
		sess.mu.Unlock()
		if err != nil {
			purgeErr = errors.Join(purgeErr, err)
			d.log.Warn("finishing session snapshot purge failed", "err", err, "session", stoppedName)
		} else if stopped, ok := d.stopped[stoppedName]; ok && stopped.purging {
			delete(d.stopped, stoppedName)
		}
		d.mu.Unlock()
	}
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
	} else if ringing {
		d.repaintAllAttachedClients()
	}

	for _, attachment := range attachments {
		d.notifyDetachedSnapshotAsync(attachment, reason)
	}
	return errors.Join(purgeErr, terminalSnapshotErr)
}

// allocEphemeralNameLocked returns the lowest free decimal name. Caller holds
// d.mu.
func (d *Daemon) allocEphemeralNameLocked() string {
	used := make(map[string]struct{}, len(d.sessions)+len(d.stopped))
	for _, entry := range d.sessions {
		if s, ok := localSession(entry); ok {
			used[s.name] = struct{}{}
		}
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
	for _, entry := range d.sessions {
		s, ok := localSession(entry)
		if ok && s.name == name {
			return s
		}
	}
	return nil
}

func (d *Daemon) nameLiveOrStoppedLocked(name string) bool {
	if d.findByNameLocked(name) != nil {
		return true
	}
	if _, ok := d.stopped[name]; ok {
		return true
	}
	_, reserved := d.creating[name]
	return reserved
}

func (d *Daemon) cwdSampler(ctx context.Context) {
	t := d.clock.NewTimer(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		if d.procCwd != nil {
			d.refreshNamedSessionCwds()
		}
		if err := d.flushCatalogue(); err != nil {
			d.log.Warn("flushing session catalogue failed", "err", err)
		}
		t.Reset(5 * time.Second)
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
	if !sess.ephemeral {
		d.markCatalogueDirty(sess.persistRecordLocked(max(d.nowUnixNano(), sess.createdAt, int64(1))).MetadataUpdate())
	}
	sess.mu.Unlock()
	d.mu.Unlock()
	markSnapshotDirty(sess)
}

// childEnv retains the daemon-environment helper for daemon-local legacy callers.
// Interactive PTY launch paths use childEnvFrom with their session snapshot.
func (d *Daemon) childEnv(name, tabStableID, paneStableID string, term terminalEnv) []string {
	return childEnvFrom(d.baseEnv, name, tabStableID, paneStableID, term)
}

func copyEnvironment(env []string) []string {
	return append([]string(nil), env...)
}

func environmentEntry(entry string) (name, value string, ok bool) {
	name, value, ok = strings.Cut(entry, "=")
	return name, value, ok
}

func terminalEnvFromEnvironment(env []string) terminalEnv {
	var term, colorTerm string
	for _, entry := range env {
		name, value, ok := environmentEntry(entry)
		if !ok {
			continue
		}
		switch name {
		case "TERM":
			term = value
		case "COLORTERM":
			colorTerm = value
		}
	}
	return terminalEnv{TrueColor: ports.DetectTrueColor(term, colorTerm)}
}

func (d *Daemon) ptyCommand(env []string) (string, []string) {
	if d.shellOverride {
		return d.shell, append([]string(nil), d.shellArgs...)
	}
	return shellFromEnvironment(env), nil
}

func shellFromEnvironment(env []string) string {
	shell := ""
	for _, entry := range env {
		if name, value, ok := environmentEntry(entry); ok && name == "SHELL" {
			shell = value
		}
	}
	if shell == "" {
		return "/bin/sh"
	}
	return shell
}

// childEnvFrom preserves the supplied environment byte-for-byte and in order,
// except for exact reserved variable names which vev owns.
func childEnvFrom(env []string, name, tabStableID, paneStableID string, term terminalEnv) []string {
	out := make([]string, 0, len(env)+4)
	for _, entry := range env {
		key, _, ok := environmentEntry(entry)
		if ok && (key == "TERM" || key == "COLORTERM" || key == "TERM_PROGRAM" || key == "VEV") {
			continue
		}
		out = append(out, entry)
	}
	if term.TrueColor {
		out = append(out, "TERM=xterm-direct", "COLORTERM=truecolor")
	} else {
		out = append(out, "TERM=xterm-256color")
	}
	return append(out,
		"TERM_PROGRAM=vev",
		"VEV=session="+escapeVEVComponent(name)+",tab="+tabStableID+",pane="+paneStableID,
	)
}
