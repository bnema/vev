package daemon

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/vt"
)

func (d *Daemon) closeRestoreDone() {
	d.mu.Lock()
	for name, entry := range d.stopped {
		if entry.restoreDone == nil {
			continue
		}
		select {
		case <-entry.restoreDone:
			continue
		default:
		}
		entry.state = ports.SessionBroken
		if entry.record.DegradedReason == "" {
			entry.record.DegradedReason = "restore unavailable"
		}
		d.stopped[name] = entry
		closeRuntimeRestoreDoneLocked(entry.restoreDone)
	}
	d.mu.Unlock()
	d.restoreOnce.Do(func() { close(d.restoreDone) })
}

// startSnapshotRestoration launches catalogue-driven restoration as a durable
// writer. Restoration repairs HEADs, promotes fallbacks, and replaces catalogue
// records, so lifecycle ownership must outlive it exactly as it outlives the
// snapshot and maintenance workers. The completion channel is registered before
// the goroutine starts, so shutdown can never begin waiting while this writer
// is still unregistered.
func (d *Daemon) startSnapshotRestoration() {
	if d == nil {
		return
	}
	d.snapshotWorkerMu.Lock()
	if d.restoreWorkerDone != nil {
		d.snapshotWorkerMu.Unlock()
		return
	}
	done := make(chan struct{})
	d.restoreWorkerDone = done
	d.snapshotWorkerMu.Unlock()
	d.sessWg.Go(func() {
		defer close(done)
		d.restoreSnapshots(d.serveCtx)
	})
}

func (d *Daemon) restoreSnapshots(ctx context.Context) {
	if d == nil {
		return
	}
	defer d.logStartupRecoveryCounts(0)
	defer d.closeRestoreDone()
	if ns := d.noticeStore; ns != nil {
		claimed, err := ns.Claim()
		if err != nil {
			d.log.Warn("claiming pending notices failed", "err", err)
		} else {
			for _, n := range claimed {
				d.notices.record(n)
				d.notices.queueGlobal(n)
			}
			// Do not discard the claim until every notice has been recorded and
			// queued. An Ack failure is safe: the claim is replayed at startup.
			if err := ns.Ack(); err != nil {
				d.log.Warn("acknowledging pending notices failed", "err", err)
			}
		}
	}
	if !d.snapsEnabled {
		return
	}
	if d.snapshotRepository != nil {
		d.restoreIncrementalSnapshots(ctx)
	}

}

func validateRestoreSessionSnapshot(snap snapcodec.Session) error {
	if len(snap.Tabs) == 0 {
		if snap.Active != 0 {
			return fmt.Errorf("snapshot: active tab out of range")
		}
	} else if int(snap.Active) >= len(snap.Tabs) {
		return fmt.Errorf("snapshot: active tab out of range")
	}
	if snap.Name == "" {
		return fmt.Errorf("snapshot: empty session name")
	}
	if snap.CreatedAt > math.MaxInt64 {
		return fmt.Errorf("snapshot: created_at overflows int64")
	}
	return nil
}

// restoreSession owns the restored tabs until registration succeeds. Every
// unsuccessful path closes their PTYs and cancels their shared session context.
func (d *Daemon) restoreSession(ctx context.Context, snap snapcodec.Session, repositoryGeneration uint64, checkpoint domain.CheckpointRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRestoreSessionSnapshot(snap); err != nil {
		return err
	}
	if d.restoredSessionAlreadyExists(snap.Name) {
		return nil
	}

	parent := d.serveCtx
	if parent == nil {
		parent = context.Background()
	}
	sctx, cancel := context.WithCancel(parent)
	opened, err := d.restoreSnapshotTabs(ctx, sctx, snap)
	if err != nil {
		cancel()
		return err
	}
	if len(opened) == 0 {
		cancel()
		return nil
	}
	ownsOpened := true
	defer func() {
		if ownsOpened {
			closeRestoredTabs(opened)
			cancel()
		}
	}()

	sess := d.newRestoredSession(snap, sctx, cancel, opened)
	// The loaded manifest is the repository head for this name. Future dirty
	// checkpoints must continue from it rather than reuse generation one.
	sess.snapshotPublishedGeneration = repositoryGeneration
	sess.snapshotPublishedCheckpoint = &checkpoint
	registered, err := d.persistAndRegisterRestoredSession(ctx, sess)
	if err != nil || !registered {
		return err
	}
	ownsOpened = false
	for _, tb := range opened {
		d.startTabGoroutines(sess, tb)
	}
	return nil
}

func (d *Daemon) restoredSessionAlreadyExists(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closing || d.findByNameLocked(name) != nil
}

func (d *Daemon) restoreSnapshotTabs(ctx, sctx context.Context, snap snapcodec.Session) ([]*tab, error) {
	// Restore runs before client Hello. Future panes therefore inherit the
	// daemon's startup environment until an attach supplies terminal capability.
	restoreTerm := terminalEnv{}
	restoreEnv := copyEnvironment(d.baseEnv)
	allowlist := d.restoreProcessAllowlistSnapshot()
	stoppedTabNames := d.restoredSessionTabNames(snap.Name)
	opened := make([]*tab, 0, len(snap.Tabs))
	for index, tabSnap := range snap.Tabs {
		if err := ctx.Err(); err != nil {
			closeRestoredTabs(opened)
			return nil, err
		}
		tabName := ""
		if index < len(stoppedTabNames) {
			tabName = stoppedTabNames[index]
		}
		tb, err := d.restoreSnapshotTab(ctx, sctx, snap.Name, tabName, tabSnap, restoreEnv, restoreTerm, allowlist)
		if err != nil {
			closeRestoredTabs(opened)
			return nil, err
		}
		opened = append(opened, tb)
	}
	return opened, nil
}

// closeRestoredTabs releases every PTY and pane context still owned by a
// failed restore. Registered sessions transfer this ownership to the daemon.
func closeRestoredTabs(tabs []*tab) {
	for _, tb := range tabs {
		tb.closeAllPanes()
	}
}

func (d *Daemon) restoredSessionTabNames(name string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.stopped[name].tabNames...)
}

func (d *Daemon) restoreSnapshotTab(ctx, sctx context.Context, sessionName, tabName string, tabSnap snapcodec.Tab, restoreEnv []string, restoreTerm terminalEnv, allowlist map[string]struct{}) (*tab, error) {
	tbSize := domain.Size{Cols: int(tabSnap.Cols), Rows: int(tabSnap.Rows)}
	if !tbSize.Valid() {
		return nil, fmt.Errorf("snapshot: invalid tab size")
	}
	placements, ok := layout.Solve(tabSnap.Tree.Root, domain.Rect{Width: tbSize.Cols, Height: tbSize.Rows})
	if !ok {
		return nil, fmt.Errorf("snapshot: unsolvable layout")
	}
	placementByPane := make(map[layout.PaneID]domain.Rect, len(placements))
	for _, placement := range placements {
		placementByPane[placement.ID] = placement.Content
	}
	tabStableID := tabSnap.StableID
	if tabStableID == "" {
		var err error
		tabStableID, err = newStableID("t")
		if err != nil {
			return nil, fmt.Errorf("snapshot: generating tab identity: %w", err)
		}
	}
	tb := &tab{stableID: tabStableID, name: tabName, tree: tabSnap.Tree.Clone(), panes: make(map[layout.PaneID]*pane, len(tabSnap.Panes)), nextPaneID: int(tabSnap.NextPaneID), size: tbSize}
	if tb.nextPaneID <= 0 {
		tb.nextPaneID = 1
	}
	tb.ctx, tb.cancel = context.WithCancel(sctx)
	for _, paneSnap := range tabSnap.Panes {
		if err := ctx.Err(); err != nil {
			tb.closeAllPanes()
			return nil, err
		}
		contentRect, ok := placementByPane[paneSnap.ID]
		if !ok {
			tb.closeAllPanes()
			return nil, fmt.Errorf("snapshot: missing pane placement")
		}
		p, err := d.restoreSnapshotPane(ctx, sessionName, tabStableID, paneSnap, contentRect, tbSize, restoreEnv, restoreTerm, allowlist, tb.ctx)
		if err != nil {
			tb.closeAllPanes()
			return nil, err
		}
		tb.panes[paneSnap.ID] = p
	}
	return tb, nil
}

func (d *Daemon) restoreSnapshotPane(ctx context.Context, sessionName, tabStableID string, paneSnap snapcodec.Pane, contentRect domain.Rect, tabSize domain.Size, restoreEnv []string, restoreTerm terminalEnv, allowlist map[string]struct{}, tabCtx context.Context) (*pane, error) {
	paneStableID := paneSnap.StableID
	if paneStableID == "" {
		var err error
		paneStableID, err = newStableID("p")
		if err != nil {
			return nil, fmt.Errorf("snapshot: generating pane identity: %w", err)
		}
	}
	command, args := d.ptyCommand(restoreEnv)
	pty, err := d.ptys.Open(ctx, command, args, childEnvFrom(restoreEnv, sessionName, tabStableID, paneStableID, restoreTerm), paneSnap.Cwd, restorePTYSize(contentRect, tabSize))
	if err != nil {
		return nil, err
	}
	p := newPaneWithStableID(paneSnap.ID, paneStableID, pty, restorePTYSize(contentRect, tabSize))
	p.ctx, p.cancel = context.WithCancel(tabCtx)
	p.rect = contentRect
	if err := restorePaneTerminal(p, paneSnap); err != nil {
		p.cancel()
		_ = pty.Close()
		return nil, err
	}
	if decision := planProcessRestore(paneSnap.Process, allowlist); decision.Restore {
		if _, err := pty.Write([]byte(decision.Command + "\n")); err != nil {
			d.log.Warn("writing snapshot restore command failed", "err", err, "session", sessionName, "pane", paneSnap.ID)
			d.NotifyGlobal(domain.NoticeWarn, domain.NoticeAutoResume, "couldn't restore the running program in session "+sessionName, err)
		}
	}
	return p, nil
}

func (d *Daemon) newRestoredSession(snap snapcodec.Session, sctx context.Context, cancel context.CancelFunc, tabs []*tab) *session {
	sess := &session{name: snap.Name, ctx: sctx, cancel: cancel, tabs: tabs, active: int(snap.Active), terminal: terminalEnv{}, env: copyEnvironment(d.baseEnv), createdAt: int64(snap.CreatedAt), snapshotWake: d.snapshotWake, snapshotChunkCache: newSnapshotChunkCache(snapshotChunkCacheLimit)}
	sess.snapEligible.Store(true)
	if len(snap.Tabs) > 0 && len(snap.Tabs[0].Panes) > 0 {
		sess.cwd = snap.Tabs[0].Panes[0].Cwd
	}
	d.mu.Lock()
	if stopped, ok := d.stopped[snap.Name]; ok {
		sess.mruAt.Store(stopped.lastUsedSeq)
	}
	d.mu.Unlock()
	return sess
}

// persistAndRegisterRestoredSession atomically transfers a successfully
// persisted session to the daemon. The caller retains cleanup on false/error.
func (d *Daemon) persistAndRegisterRestoredSession(ctx context.Context, sess *session) (bool, error) {
	if sess.incarnation == (domain.IncarnationID{}) {
		d.mu.Lock()
		stopped := d.stopped[sess.name]
		d.mu.Unlock()
		sess.incarnation = stopped.incarnation
		if sess.incarnation == (domain.IncarnationID{}) {
			var err error
			sess.incarnation, err = domain.NewIncarnationID(rand.Reader)
			if err != nil {
				return false, fmt.Errorf("snapshot: generate durable identity: %w", err)
			}
		}
	}
	// Restoration is catalogue-authorized. Missing or unreadable authority is a
	// hard failure; migration creates catalogue records before daemon startup.
	if d.persistEnabled {
		record, ok, err := d.catalogueRecord(sess.name)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, errors.New("snapshot: restored session is absent from catalogue")
		}
		if record.IncarnationID != sess.incarnation {
			return false, errors.New("snapshot: restored session incarnation differs from catalogue")
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || d.findByNameLocked(sess.name) != nil || ctx.Err() != nil {
		return false, nil
	}
	sess.id = domain.SessionID(fmt.Sprintf("sess-%d", d.nextID))
	d.nextID++
	delete(d.stopped, sess.name)
	d.sessions[sess.id] = sess
	sess.snapDirty.Store(false)
	return true, nil
}

func restorePaneTerminal(p *pane, snap snapcodec.Pane) error {
	if len(snap.Tail) == 0 {
		return fmt.Errorf("snapshot history: missing tail blob")
	}
	if len(snap.Visible) == 0 {
		return fmt.Errorf("snapshot visible: missing visible blob")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	screen, err := vt.NewScreenWithRestoredHistory(p.screen.Frame.Width, p.screen.Frame.Height, vt.HistoryConfig{MaxRows: defaultScrollbackRows, MaxCells: defaultScrollbackCells}, snap.SealedChunks, snap.Tail)
	if err != nil {
		return fmt.Errorf("snapshot history: %w", err)
	}
	if err := screen.RestorePrimaryVisible(snap.Visible); err != nil {
		return fmt.Errorf("snapshot visible: %w", err)
	}
	p.screen = screen
	p.history = screen.History()
	return nil
}

func restorePTYSize(contentRect domain.Rect, tabSize domain.Size) domain.Size {
	cols := contentRect.Width
	if cols <= 0 {
		cols = tabSize.Cols
	}
	if cols < layout.MinPaneCols {
		cols = layout.MinPaneCols
	}
	rows := contentRect.Height
	if rows <= 0 {
		rows = min(max(tabSize.Rows, layout.MinPaneRows), 24)
	}
	if rows < layout.MinPaneRows {
		rows = layout.MinPaneRows
	}
	return domain.Size{Cols: cols, Rows: rows}
}

// scheduleSnapshot captures a session only once while an immutable capture is
// queued or being written. It never waits for the bounded worker queue.
