// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"context"
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
	"github.com/bnema/vev/internal/persist"
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
	id        domain.SessionID
	name      string
	ephemeral bool

	ctx    context.Context
	cancel context.CancelFunc

	mu                     sync.Mutex // guards tabs, active, client, clipFiles, and clipboard queue state
	themeMu                sync.Mutex
	tabs                   []*tab
	active                 int
	client                 *attachedClient
	clipboardQueue         []clipboardForward
	clipboardWorkerRunning bool
	renameInProgress       bool
	cwd                    string
	terminal               terminalEnv
	// env is the authoritative immutable environment snapshot for future PTY children.
	// It is guarded by mu and is always copied on ingress and egress.
	env          []string
	createdAt    int64
	mruAt        atomic.Uint64
	snapDirty    atomic.Bool
	snapEligible atomic.Bool
	// snapshotMu serializes the dirty generation with worker completion. It is
	// intentionally independent from mu: persistence never holds session state
	// locks while encoding or writing.
	snapshotMu                  sync.Mutex
	snapshotGeneration          uint64
	snapshotCapturedGeneration  uint64
	snapshotPublishedGeneration uint64
	snapshotNextEligibleAt      time.Time
	snapshotAttempted           bool
	snapshotAttemptKind         snapshotAttemptKind
	// The coordinator state below is guarded by snapshotMu. A capture can be
	// queued globally or in flight, never more than one of each per session.
	// Quarantine cancels the session publication context before destructive
	// repository operations and publicationDone joins a started publication.
	snapshotPending         bool
	snapshotPendingCaptures uint
	// snapshotForcedGeneration is the newest dirty generation a forced
	// checkpoint must publish. It survives an older routine capture so worker
	// completion can enqueue exactly one forced successor.
	snapshotForcedGeneration   uint64
	snapshotQueuedCapture      *snapshotCapture
	snapshotInFlightCapture    *snapshotCapture
	snapshotPublicationDone    chan struct{}
	snapshotPublicationContext context.Context
	snapshotPublicationCancel  context.CancelFunc
	snapshotQuarantined        bool
	snapshotQuarantineEpoch    uint64
	// snapshotFailureSig is the stable class of the active failed publication.
	// It is guarded by snapshotMu and deliberately never contains error text.
	snapshotFailureSig string
	// snapshotChunkCache retains encoded sealed history only for this named
	// session. snapshotMu owns it so cache touches never contend with pane
	// state or mutate bytes retained by queued/in-flight publications.
	snapshotChunkCache *snapshotChunkCache
	snapshotChanged    chan struct{}
	snapshotWake       chan struct{}
	// syncGen makes synchronized-output watchdog generations unique across all
	// panes in this session.
	syncGen atomic.Uint64
	// coordinator fans in this session's producer render invalidations.
	coordinator atomic.Pointer[renderCoordinator]
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

func (d *Daemon) touchMRU(sess *session) {
	if d == nil || sess == nil {
		return
	}
	seq := d.mruSeq.Add(1)
	updated := false
	for {
		old := sess.mruAt.Load()
		if old >= seq {
			return
		}
		if sess.mruAt.CompareAndSwap(old, seq) {
			updated = true
			break
		}
	}
	if !updated {
		return
	}
	sess.mu.Lock()
	name := sess.name
	ephemeral := sess.ephemeral
	sess.mu.Unlock()
	if !ephemeral && d.persist != nil {
		if err := d.persist.TouchMRU(name, seq); err != nil {
			d.log.Warn("touching persisted session recency failed", "err", err, "session", name)
		}
	}
}

func (d *Daemon) createSessionLocked(name string, ephemeral bool, cwd string, sz domain.Size, term terminalEnv, env []string, restoredTabNames ...[]string) (*session, error) {
	env = copyEnvironment(env)
	stopped, resuming := d.stopped[name]
	var createdAt int64
	if !ephemeral {
		if resuming {
			// A stopped session is the same lifecycle when resumed; it must retain
			// the persisted identity rather than receive a fresh timestamp.
			createdAt = stopped.createdAt
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
	}

	tbSize := tabSize(sz)
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
		pty, err := d.ptys.Open(d.serveCtx, command, args, childEnvFrom(env, name, tabStableID, paneStableID, term), cwd, tbSize)
		if err != nil {
			closeTabs(tabs)
			d.log.Warn("pty spawn failed", "err", err, "session", name, "kind", "session")
			return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session: shell failed to start", err)
		}
		tb := newTabWithStableID(tabStableID, paneStableID, pty, tbSize)
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
	sess := &session{
		id:           id,
		name:         name,
		ephemeral:    ephemeral,
		ctx:          sctx,
		cancel:       cancel,
		tabs:         tabs,
		cwd:          cwd,
		terminal:     term,
		env:          env,
		createdAt:    createdAt,
		snapshotWake: d.snapshotWake,
	}
	if !ephemeral && name != "" {
		sess.snapshotChunkCache = newSnapshotChunkCache(snapshotChunkCacheLimit)
	}
	if lastUsedSeq > 0 {
		sess.mruAt.Store(lastUsedSeq)
	}
	sess.snapEligible.Store(!ephemeral && name != "")
	if !ephemeral {
		if err := d.persist.Save(persist.Record{Name: name, Cwd: cwd, CreatedAt: createdAt, UpdatedAt: createdAt, LastUsedSeq: lastUsedSeq, TabNames: names}); err != nil {
			closeTabs(tabs)
			cancel()
			return nil, domain.UserErr(domain.NoticeSessionSpawn, "couldn't create session", err)
		}
		delete(d.stopped, name)
	}
	d.sessions[id] = sess
	if lastUsedSeq == 0 {
		d.touchMRU(sess)
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
		for _, p := range tb.panes {
			_ = p.pty.Close()
		}
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
	// The caller owns d.mu. Make the entire ownership handoff atomic with
	// global routing so it cannot select a client between source removal and
	// destination publication.
	d.notices.routingMu.Lock()
	from.mu.Lock()
	cwd := from.cwd
	term := from.terminal
	env := copyEnvironment(from.env)
	if from.client != ac {
		from.mu.Unlock()
		d.notices.routingMu.Unlock()
		d.mu.Unlock()
		return errors.New("client detached")
	}
	from.mu.Unlock()

	newSess, err := d.createSessionLocked(name, false, cwd, sz, term, env)
	if err != nil {
		d.notices.routingMu.Unlock()
		d.mu.Unlock()
		return err
	}
	from.mu.Lock()
	if from.client != ac {
		from.mu.Unlock()
		d.notices.routingMu.Unlock()
		d.mu.Unlock()
		_ = d.killSession(newSess, ports.ReasonSessionKilled, true)
		return errors.New("client detached")
	}
	from.client = nil
	ac.setSession(nil)
	from.mu.Unlock()

	// Prepare source invalidation, output rebase, and destination coordinator
	// identity before publishing the destination attachment.
	cleanups := d.handoffCoordinator(from, newSess, nil, ac)
	ac.setSession(newSess)
	newSess.mu.Lock()
	newSess.client = ac
	newSess.mu.Unlock()

	d.touchMRU(newSess)
	ac.recordPreviousSession(from)
	d.log.Info("client attached", "session", newSess.name, "resume", ac.resumeCapable)
	d.notices.routingMu.Unlock()
	d.mu.Unlock()
	finishRenderLifecycleCleanups(cleanups)
	d.firstPaint(newSess, ac, sz)
	return nil
}

func (d *Daemon) createTab(sess *session, sz domain.Size) error {
	tbSize := tabSize(sz)
	sess.mu.Lock()
	name := sess.name
	cwd := sess.cwd
	client := sess.client
	term := sess.terminal
	env := copyEnvironment(sess.env)
	sess.mu.Unlock()
	tabStableID, paneStableID, err := d.newTabPaneStableIDs()
	if err != nil {
		return err
	}
	command, args := d.ptyCommand(env)
	pty, err := d.ptys.Open(sess.ctx, command, args, childEnvFrom(env, name, tabStableID, paneStableID, term), cwd, tbSize)
	if err != nil {
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
		_ = pty.Close()
		return errors.New("daemon: session closed")
	}
	tb.ctx, tb.cancel = context.WithCancel(sess.ctx)
	for _, p := range tb.panes {
		p.ctx, p.cancel = context.WithCancel(tb.ctx)
	}
	sess.mu.Lock()
	oldActive := sess.active
	sess.tabs = append(sess.tabs, tb)
	sess.active = len(sess.tabs) - 1
	tabIndex := sess.active
	record := sess.persistRecordLocked(time.Now().UnixNano())
	ephemeral := sess.ephemeral
	if !ephemeral {
		if err := d.persist.Save(record); err != nil {
			sess.tabs = sess.tabs[:len(sess.tabs)-1]
			sess.active = oldActive
			if tb.cancel != nil {
				tb.cancel()
			}
			sess.mu.Unlock()
			d.mu.Unlock()
			_ = pty.Close()
			return err
		}
	}
	sess.mu.Unlock()
	d.log.Info("tab created", "session", name, "tab", tabIndex)
	d.startTabGoroutines(sess, tb)
	d.mu.Unlock()
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
		sess.mu.Lock()
		name := sess.name
		sess.mu.Unlock()
		d.log.Info("pane created", "session", name, "pane", p.id)
	}
	// Scheduler ownership was removed; this launch creates exactly one reader.
	d.sessWg.Add(1)
	go d.ptyReader(sess, tb, p)
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

// detachIfCurrent serializes client removal with global notice routing. The
// routing lock is released before the caller performs teardown, so it covers
// only attachment ownership and is never held across transport work.
func (d *Daemon) detachIfCurrent(sess *session, ac *attachedClient) bool {
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	return sess.detachIfCurrent(ac)
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

	// Reserve the rename while state is inspected, then perform persistence and
	// repository I/O without daemon/session locks. The old identity is first
	// quarantined and durably purged; committing the new record before that
	// transaction completes could leave a restorable old legacy source.
	d.mu.Lock()
	if taken := d.findByNameLocked(name); taken != nil && taken != sess {
		d.mu.Unlock()
		return errSessionNameInUse
	}
	sess.mu.Lock()
	if sess.renameInProgress {
		sess.mu.Unlock()
		d.mu.Unlock()
		return errors.New("session rename already in progress")
	}
	if stopped, ok := d.stopped[name]; ok && stopped.name != sess.name {
		sess.mu.Unlock()
		d.mu.Unlock()
		return errSessionNameInUse
	}
	oldName := sess.name
	wasEphemeral := sess.ephemeral
	createdAt := sess.createdAt
	if wasEphemeral {
		var err error
		createdAt, err = d.allocateLifecycleCreatedAtLocked()
		if err != nil {
			sess.mu.Unlock()
			d.mu.Unlock()
			return err
		}
	}
	lastUsedSeq := sess.mruAt.Load()
	record := sess.persistRecordLocked(d.nowUnixNano())
	record.Name = name
	record.CreatedAt = createdAt
	record.LastUsedSeq = lastUsedSeq
	sess.renameInProgress = true
	sess.mu.Unlock()
	d.mu.Unlock()

	rollback := func(err error) error {
		sess.mu.Lock()
		sess.renameInProgress = false
		sess.mu.Unlock()
		return err
	}
	var quarantine snapshotCoordinatorQuarantine
	if !wasEphemeral && oldName != name {
		quarantine = quarantineSnapshotCoordinatorWithEpoch(sess)
		<-quarantine.done
		if err := d.beginSnapshotPurge(oldName); err != nil {
			return rollback(err)
		}
		// finishSnapshotPurge preserves the tombstone through both source
		// deletions and the old metadata delete, including a retained legacy
		// import whose verified source delete is still pending.
		if err := d.finishSnapshotPurge(oldName); err != nil {
			return rollback(err)
		}
	}
	// For a named rename this is the durable commit point, deliberately after
	// the old tombstone transaction. Ephemeral promotion has no old durable
	// identity and can create its first record directly.
	if wasEphemeral || oldName != name {
		if err := d.persist.Save(record); err != nil {
			return rollback(err)
		}
	}

	d.mu.Lock()
	sess.mu.Lock()
	// The reservation prevents another rename. A closing session is allowed to
	// finish this short commit; killSession subsequently owns its teardown.
	delete(d.stopped, oldName)
	delete(d.stopped, name)
	sess.name = name
	sess.createdAt = createdAt
	sess.ephemeral = false
	sess.renameInProgress = false
	// Keep the coordinator in quarantine until the new in-memory identity is
	// fully committed. The fresh coordinator below makes the new repository
	// start at generation one.
	if !wasEphemeral && oldName != name {
		sess.snapEligible.Store(false)
	} else {
		sess.snapEligible.Store(name != "")
	}
	sess.mu.Unlock()
	d.mu.Unlock()
	if !wasEphemeral && oldName != name {
		// A concurrent teardown may have quarantined after this rename did. In
		// that case its newer epoch owns the stopped coordinator.
		resumeSnapshotCoordinatorForNewIdentity(sess, quarantine)
		return nil
	}
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
	oldName := tb.name
	tb.name = name
	record := sess.persistRecordLocked(time.Now().UnixNano())
	ephemeral := sess.ephemeral
	if !ephemeral {
		if err := d.persist.Save(record); err != nil {
			tb.name = oldName
			sess.mu.Unlock()
			d.mu.Unlock()
			return err
		}
	}
	sess.mu.Unlock()
	d.mu.Unlock()
	return nil
}

func (s *session) persistRecordLocked(updatedAt int64) persist.Record {
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
	return persist.Record{Name: s.name, Cwd: s.cwd, CreatedAt: createdAt, UpdatedAt: updatedAt, LastUsedSeq: s.mruAt.Load(), TabNames: tabNames}
}

func (d *Daemon) closeTab(sess *session, tb *tab, repaint bool) {
	d.mu.Lock()
	if d.sessions[sess.id] != sess {
		d.mu.Unlock()
		return
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
		return
	}
	if len(sess.tabs) == 1 {
		name := sess.name
		sess.mu.Unlock()
		d.mu.Unlock()
		if err := d.killSession(sess, ports.ReasonSessionKilled, false); err != nil {
			d.log.Warn("closing last tab failed", "session", name, "err", err)
			d.reportError(sess, domain.UserErr(domain.NoticeSnapshotSaturated,
				"couldn't close tab: session state not yet saved; try again", err))
			return
		}
		d.log.Info("tab closed", "session", name, "last", true)
		return
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
	record := sess.persistRecordLocked(time.Now().UnixNano())
	ephemeral := sess.ephemeral
	if !ephemeral {
		if err := d.persist.Save(record); err != nil {
			d.log.Warn("persisting closed tab failed", "err", err, "session", name)
		}
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
}

// ptyReader drains child output into the VT screen and pokes the dirty channel
// (non-blocking: a full channel already means a render is pending). On any read

// beginSnapshotPurge commits a logical named-session kill before its live
// identity or persisted metadata is removed. It never runs under daemon or
// session locks.
func (d *Daemon) beginSnapshotPurge(name string) error {
	if d.snapshotDeletion == nil || name == "" {
		return nil
	}
	return d.snapshotDeletion.Tombstone(context.Background(), name)
}

// finishSnapshotPurge deletes both independently durable snapshot sources,
// then metadata, and only then clears the tombstone. Any failure deliberately
// leaves the marker in place so startup cannot restore or import the name and
// a later live/offline kill can retry the idempotent source deletes.
func (d *Daemon) finishSnapshotPurge(name string) error {
	var incrementalErr, legacyErr error
	if d.snapshotRepository != nil {
		incrementalErr = d.snapshotRepository.Delete(context.Background(), name)
	}
	if d.legacySnapshots != nil {
		legacyErr = d.legacySnapshots.DeleteLegacy(context.Background(), name)
	}
	if incrementalErr != nil || legacyErr != nil {
		return errors.Join(incrementalErr, legacyErr)
	}
	if err := d.persist.Delete(name); err != nil {
		return err
	}
	if d.snapshotDeletion != nil {
		return d.snapshotDeletion.DeleteTombstone(context.Background(), name)
	}
	return nil
}

// retryStoppedPurge completes a previously closed session's durable purge.
// It keeps the stopped record hidden until all sources and metadata cross their
// deletion boundaries.
func (d *Daemon) retryStoppedPurge(name string) error {
	d.mu.Lock()
	stopped, ok := d.stopped[name]
	if !ok {
		d.mu.Unlock()
		return nil
	}
	// A stopped session without a durable snapshot source retains the existing
	// metadata-only delete behavior. There is no restore/import source to fence.
	if d.snapshotDeletion == nil {
		d.mu.Unlock()
		if err := d.persist.Delete(name); err != nil {
			return err
		}
		d.mu.Lock()
		delete(d.stopped, name)
		d.mu.Unlock()
		return nil
	}
	stopped.purging = true
	d.stopped[name] = stopped
	d.mu.Unlock()

	if err := d.beginSnapshotPurge(name); err != nil {
		return err
	}
	if err := d.finishSnapshotPurge(name); err != nil {
		return err
	}
	d.mu.Lock()
	if current, ok := d.stopped[name]; ok && current.purging {
		delete(d.stopped, name)
	}
	d.mu.Unlock()
	return nil
}

func (d *Daemon) killSession(sess *session, reason uint8, purge bool) error {
	return d.killSessionWithSnapshotDeadline(sess, reason, purge, nil)
}

// killSessionWithSnapshotDeadline shares Serve's shutdown budget with its
// terminal checkpoint and coordinator join. A timed-out repository call keeps
// only immutable capture state; it never observes closed worker channels or
// session-owned cache that teardown has released.
func (d *Daemon) killSessionWithSnapshotDeadline(sess *session, reason uint8, purge bool, deadline *snapshotShutdownDeadline) error {
	ringing := sess.anyAttention()
	sess.mu.Lock()
	isEphemeral := sess.ephemeral
	name := sess.name
	sess.mu.Unlock()
	// Commit the tombstone before changing live identity or metadata. A
	// repository delete must never race an older publication that could recreate
	// the just-quarantined name. Do not hold daemon or session locks while
	// tombstoning or joining: a repository implementation may block on I/O.
	if purge && !isEphemeral {
		if err := d.beginSnapshotPurge(name); err != nil {
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
	if d.snapsEnabled && !isEphemeral {
		if purge {
			if d.snapshotRepository != nil {
				// Repository deletion is intentionally deferred until the render
				// coordinator is cancelled and joined below. See client.go's lock
				// ordering notes: no coordinator callback may race a purge.
			}
		} else {
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
	d.mu.Lock()
	if _, ok := d.sessions[sess.id]; !ok {
		d.mu.Unlock()
		return nil
	}
	delete(d.sessions, sess.id)
	d.clearBarScriptsForSession(sess.id)
	d.purgeParkedForSessionLocked(sess)
	sess.mu.Lock()
	stoppedName := sess.name
	stoppedCwd := sess.cwd
	createdAt := sess.createdAt
	tabNames := sess.persistRecordLocked(time.Now().UnixNano()).TabNames
	ephemeral := sess.ephemeral
	sess.mu.Unlock()
	if !ephemeral {
		stopped := stoppedSession{name: stoppedName, cwd: stoppedCwd, createdAt: createdAt, lastUsedSeq: sess.mruAt.Load(), tabNames: tabNames, purging: purge}
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

	// A global route that selected this attachment must publish before terminal
	// teardown makes it stale.
	d.notices.routingMu.Lock()
	sess.mu.Lock()
	ac := sess.client
	sess.client = nil
	sess.mu.Unlock()
	d.notices.routingMu.Unlock()
	if rc := sess.renderCoordinator(); rc != nil {
		// Terminal teardown has two phases: this session owner first prevents any
		// new worker registration and detaches all tokens, then stops and waits
		// outside coordinator callbacks and session locks.
		rc.beginSessionTeardown().finish()
		rc.waitForTimerWorkers()
	}
	if ac != nil {
		d.unregisterPreview(ac)
		ac.clearPreviousSession()
		ac.setSession(nil)
		ac.clearCaptureFrames()
	}

	// Prevent queued launches from entering Open, then cancel the parent
	// context. The launch worker owns any late PTY result and closes it rather
	// than publishing it into the torn-down tab.
	sess.stopFloatingLaunches()
	sess.cancel()
	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	clipFiles := sess.clipFiles
	sess.clipFiles = nil
	sess.mu.Unlock()
	for _, tb := range tabs {
		d.clearDestroyedTabPreview(tb)
		d.teardownFloating(tb, ac)
		tb.closeAllPanes()
	}
	for _, path := range clipFiles {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			d.log.Warn("removing clipboard temp file failed", "err", err, "path", path)
		}
	}
	// The coordinator and all panes are now stopped, so no producer can publish
	// another generation after this destructive source deletion. Keep the
	// tombstone and hidden stopped record when any source or metadata operation
	// fails; retryStoppedPurge (including a repeated live kill) resumes safely.
	if !ephemeral && purge {
		if err := d.finishSnapshotPurge(stoppedName); err != nil {
			purgeErr = errors.Join(purgeErr, err)
			d.log.Warn("finishing session snapshot purge failed", "err", err, "session", stoppedName)
		} else {
			d.mu.Lock()
			if stopped, ok := d.stopped[stoppedName]; ok && stopped.purging {
				delete(d.stopped, stoppedName)
			}
			d.mu.Unlock()
		}
	}
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
	} else if ringing {
		d.repaintAllAttachedClients()
	}

	if ac != nil {
		d.notifyDetachedAsync(ac, reason)
	}
	return errors.Join(purgeErr, terminalSnapshotErr)
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
	t := d.clock.NewTimer(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		d.refreshNamedSessionCwds()
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
	name := sess.name
	ephemeral := sess.ephemeral
	sess.mu.Unlock()
	if !ephemeral {
		if err := d.persist.Touch(name, cwd, time.Now().UnixNano()); err != nil {
			d.log.Warn("touching persisted session cwd failed", "err", err, "session", name)
		}
		markSnapshotDirty(sess)
	}
	d.mu.Unlock()
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
	i := strings.IndexByte(entry, '=')
	if i < 0 {
		return "", "", false
	}
	return entry[:i], entry[i+1:], true
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
