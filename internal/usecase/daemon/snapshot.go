package daemon

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

// snapshotQueueCapacity bounds retained immutable captures. A full queue never
// stalls session producers; the session remains dirty for the next interval.
const snapshotQueueCapacity = 1

type snapshotCapture struct {
	session    *session
	generation uint64
	name       string
	createdAt  uint64
	active     uint16
	tabs       []snapshotCaptureTab
}

type snapshotCaptureTab struct {
	stableID   string
	cols       uint16
	rows       uint16
	nextPaneID uint64
	focus      layout.PaneID
	tree       *layout.Tree
	panes      []snapshotCapturePane
}

type snapshotCapturePane struct {
	id       layout.PaneID
	stableID string
	cwd      string
	history  vt.HistoryView
	visible  renderer.Frame
	process  *snapcodec.Process
}

const snapshotInterval = 30 * time.Second

func (d *Daemon) closeRestoreDone() {
	d.restoreOnce.Do(func() { close(d.restoreDone) })
}

func (d *Daemon) restoreSnapshots(ctx context.Context) {
	defer d.closeRestoreDone()
	if d == nil || !d.snapsEnabled || d.snaps == nil {
		return
	}
	blobs, err := d.snaps.Load()
	if err != nil {
		d.log.Warn("loading session snapshots failed", "err", err)
		return
	}
	for _, blob := range blobs {
		select {
		case <-ctx.Done():
			return
		default:
		}
		snap, err := snapcodec.Unmarshal(blob.Data)
		if err != nil {
			d.log.Warn("decoding session snapshot failed", "err", err, "session", blob.Name)
			continue
		}
		if err := d.restoreSession(ctx, snap); err != nil {
			d.log.Warn("restoring session snapshot failed", "err", err, "session", snap.Name)
		}
	}
}

func (d *Daemon) restoreSession(ctx context.Context, snap snapcodec.Session) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snap.Name == "" {
		return fmt.Errorf("snapshot: empty session name")
	}
	if snap.CreatedAt > math.MaxInt64 {
		return fmt.Errorf("snapshot: created_at overflows int64")
	}
	d.mu.Lock()
	if d.closing || d.findByNameLocked(snap.Name) != nil {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	sctx, cancel := context.WithCancel(d.serveCtx)
	opened := make([]*tab, 0, len(snap.Tabs))
	// Snapshot restore runs before any client Hello is available, so restored
	// panes start with the conservative terminal environment. The next attach or
	// resume updates sess.terminal for future panes from the client's capability.
	restoreTerm := terminalEnv{}
	closeOpened := func() {
		for _, tb := range opened {
			tb.closeAllPanes()
		}
		cancel()
	}
	allowlist := d.restoreProcessAllowlistSnapshot()
	d.mu.Lock()
	persisted := d.stopped[snap.Name]
	d.mu.Unlock()
	for tabIndex, tabSnap := range snap.Tabs {
		if err := ctx.Err(); err != nil {
			closeOpened()
			return err
		}
		tbSize := domain.Size{Cols: int(tabSnap.Cols), Rows: int(tabSnap.Rows)}
		if !tbSize.Valid() {
			closeOpened()
			return fmt.Errorf("snapshot: invalid tab size")
		}
		placements, ok := layout.Solve(tabSnap.Tree.Root, domain.Rect{Width: tbSize.Cols, Height: tbSize.Rows})
		if !ok {
			closeOpened()
			return fmt.Errorf("snapshot: unsolvable layout")
		}
		placementByPane := make(map[layout.PaneID]domain.Rect, len(placements))
		for _, pl := range placements {
			placementByPane[pl.ID] = pl.Content
		}
		tabStableID := tabSnap.StableID
		if tabStableID == "" {
			var err error
			tabStableID, err = newStableID("t")
			if err != nil {
				closeOpened()
				return fmt.Errorf("snapshot: generating tab identity: %w", err)
			}
		}
		tb := &tab{stableID: tabStableID, tree: tabSnap.Tree.Clone(), panes: make(map[layout.PaneID]*pane, len(tabSnap.Panes)), nextPaneID: int(tabSnap.NextPaneID), size: tbSize}
		if tabIndex < len(persisted.tabNames) {
			tb.name = persisted.tabNames[tabIndex]
		}
		if tb.nextPaneID <= 0 {
			tb.nextPaneID = 1
		}
		tb.ctx, tb.cancel = context.WithCancel(sctx)
		opened = append(opened, tb)
		for _, paneSnap := range tabSnap.Panes {
			restoreCommand := ""
			if decision := planProcessRestore(paneSnap.Process, allowlist); decision.Restore {
				restoreCommand = decision.Command
			}
			if err := ctx.Err(); err != nil {
				closeOpened()
				return err
			}
			contentRect, ok := placementByPane[paneSnap.ID]
			if !ok {
				closeOpened()
				return fmt.Errorf("snapshot: missing pane placement")
			}
			contentSize := restorePTYSize(contentRect, tbSize)
			paneStableID := paneSnap.StableID
			if paneStableID == "" {
				var err error
				paneStableID, err = newStableID("p")
				if err != nil {
					closeOpened()
					return fmt.Errorf("snapshot: generating pane identity: %w", err)
				}
			}
			pty, err := d.ptys.Open(ctx, d.shell, d.shellArgs, d.childEnv(snap.Name, tabStableID, paneStableID, restoreTerm), paneSnap.Cwd, contentSize)
			if err != nil {
				closeOpened()
				return err
			}
			p := newPaneWithStableID(paneSnap.ID, paneStableID, pty, contentSize)
			p.ctx, p.cancel = context.WithCancel(tb.ctx)
			p.rect = contentRect
			if err := restorePaneTerminal(p, paneSnap); err != nil {
				closeOpened()
				return err
			}
			if restoreCommand != "" {
				if _, err := pty.Write([]byte(restoreCommand + "\n")); err != nil {
					d.log.Warn("writing snapshot restore command failed", "err", err, "session", snap.Name, "pane", paneSnap.ID)
				}
			}
			tb.panes[paneSnap.ID] = p
		}
	}
	if len(opened) == 0 {
		cancel()
		return nil
	}
	createdAt := int64(snap.CreatedAt)
	sess := &session{name: snap.Name, ctx: sctx, cancel: cancel, tabs: opened, active: int(snap.Active), terminal: restoreTerm, createdAt: createdAt}
	if sess.active < 0 || sess.active >= len(sess.tabs) {
		sess.active = 0
	}
	sess.snapEligible.Store(true)
	if len(snap.Tabs) > 0 && len(snap.Tabs[0].Panes) > 0 {
		sess.cwd = snap.Tabs[0].Panes[0].Cwd
	}
	d.mu.Lock()
	if stopped, ok := d.stopped[snap.Name]; ok {
		sess.mruAt.Store(stopped.lastUsedSeq)
	}
	d.mu.Unlock()
	record := sess.persistRecordLocked(createdAt)
	if err := d.persist.Save(record); err != nil {
		closeOpened()
		return err
	}
	d.mu.Lock()
	if d.closing || d.findByNameLocked(snap.Name) != nil || ctx.Err() != nil {
		d.mu.Unlock()
		closeOpened()
		return nil
	}
	id := domain.SessionID(fmt.Sprintf("sess-%d", d.nextID))
	d.nextID++
	sess.id = id
	delete(d.stopped, snap.Name)
	d.sessions[id] = sess
	d.mu.Unlock()
	sess.snapDirty.Store(false)
	for _, tb := range opened {
		d.startTabGoroutines(sess, tb)
	}
	return nil
}

func markSnapshotDirty(sess *session) {
	if sess == nil || !sess.snapEligible.Load() {
		return
	}
	sess.snapshotMu.Lock()
	sess.snapshotGeneration++
	sess.snapDirty.Store(true)
	sess.snapshotMu.Unlock()
}

func restorePaneTerminal(p *pane, snap snapcodec.Pane) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Unmarshal requires both blobs. The empty fallback only supports direct
	// in-process construction used by restore callers; it is unreachable for a
	// durable snapshot because the v3 manifest rejects missing blobs.
	tail := snap.Tail
	if len(tail) == 0 {
		tail, _ = vt.MarshalEmptyHistoryTail()
	}
	visible := snap.Visible
	if len(visible) == 0 {
		visible, _ = vt.MarshalVisible(p.screen.PrimaryVisibleFrame())
	}
	history, err := vt.HistoryFromBlobs(vt.HistoryConfig{MaxRows: defaultScrollbackRows}, snap.SealedChunks, tail)
	if err != nil {
		return fmt.Errorf("snapshot history: %w", err)
	}
	if err := p.screen.RestorePrimaryVisible(visible); err != nil {
		return fmt.Errorf("snapshot visible: %w", err)
	}
	p.history = history
	p.screen.OnLineEvicted = history.Append
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
func (d *Daemon) scheduleSnapshot(sess *session) bool {
	if d == nil || sess == nil || !d.snapsEnabled || d.snaps == nil || !sess.snapEligible.Load() {
		return true
	}
	sess.snapshotMu.Lock()
	if !sess.snapDirty.Load() || sess.snapshotPending {
		sess.snapshotMu.Unlock()
		return true
	}
	generation := sess.snapshotGeneration
	sess.snapshotPending = true
	sess.snapshotMu.Unlock()

	capture, ok := d.captureSnapshotState(sess, generation)
	if !ok {
		d.finishSnapshotCapture(sess, generation, false)
		return false
	}
	if d.enqueueSnapshotCapture(capture) {
		return true
	}
	// Coalesce under saturation or shutdown: the latest state stays dirty and
	// will be captured on a later tick once an active worker has room.
	d.finishSnapshotCapture(sess, generation, false)
	return false
}

// enqueueSnapshotCapture serializes worker shutdown with the non-blocking
// queue send. The queue is deliberately never closed: producers can race
// shutdown without risking a send-on-closed panic.
func (d *Daemon) enqueueSnapshotCapture(capture snapshotCapture) bool {
	d.snapshotWorkerMu.Lock()
	defer d.snapshotWorkerMu.Unlock()
	if d.snapshotWorkerCancel == nil || d.snapshotWorkerCtx == nil || d.snapshotWorkerCtx.Err() != nil {
		return false
	}
	select {
	case d.snapshotJobs <- capture:
		return true
	default:
		return false
	}
}

// captureSession rotates history tails and clones visible frames while holding
// each pane lock. The returned capture contains only immutable state; encoding
// and persistence are deliberately deferred to snapshotEncodeWorker.
func (d *Daemon) captureSnapshotState(sess *session, generation uint64) (snapshotCapture, bool) {
	sess.mu.Lock()
	capture := snapshotCapture{
		session:    sess,
		generation: generation,
		name:       sess.name,
		createdAt:  uint64(sess.createdAt),
		active:     uint16(max(sess.active, 0)),
	}
	ephemeral := sess.ephemeral
	fallbackCwd := sess.cwd
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	if ephemeral || capture.name == "" {
		return snapshotCapture{}, false
	}

	capture.tabs = make([]snapshotCaptureTab, 0, len(tabs))
	for _, tb := range tabs {
		tb.mu.Lock()
		tabCapture := snapshotCaptureTab{
			stableID:   tb.stableID,
			cols:       uint16(max(tb.size.Cols, 0)),
			rows:       uint16(max(tb.size.Rows, 0)),
			nextPaneID: uint64(max(tb.nextPaneID, 0)),
			tree:       tb.tree.Clone(),
		}
		if tb.tree != nil {
			tabCapture.focus = tb.tree.Focus
		}
		panes := make([]*pane, 0, len(tb.panes))
		for _, p := range tb.panes {
			panes = append(panes, p)
		}
		tb.mu.Unlock()

		tabCapture.panes = make([]snapshotCapturePane, 0, len(panes))
		for _, p := range panes {
			p.mu.Lock()
			pty := p.pty
			pid := 0
			if pty != nil {
				pid = pty.Pid()
			}
			paneCapture := snapshotCapturePane{
				id:       p.id,
				stableID: p.stableID,
				history:  p.history.SealAndView(),
				visible:  p.screen.PrimaryVisibleFrame(),
			}
			p.mu.Unlock()
			paneCapture.cwd = fallbackCwd
			if d.procCwd != nil && pid > 0 {
				if cwd, err := d.procCwd(pid); err == nil && cwd != "" {
					paneCapture.cwd = cwd
				}
			}
			paneCapture.process = d.capturePaneProcess(pty, pid)
			tabCapture.panes = append(tabCapture.panes, paneCapture)
		}
		capture.tabs = append(capture.tabs, tabCapture)
	}
	return capture, true
}

// captureSession remains the synchronous producer-facing trigger for callers
// such as teardown and benchmarks; the actual encoding and Write stay async.
func (d *Daemon) captureSession(sess *session) bool {
	markSnapshotDirty(sess)
	return d.scheduleSnapshot(sess)
}

func (d *Daemon) encodeSnapshotCapture(capture snapshotCapture) ([]byte, error) {
	snap := snapcodec.Session{Name: capture.name, CreatedAt: capture.createdAt, Active: capture.active, Tabs: make([]snapcodec.Tab, 0, len(capture.tabs))}
	for _, tabCapture := range capture.tabs {
		tabSnap := snapcodec.Tab{
			StableID: tabCapture.stableID, Cols: tabCapture.cols, Rows: tabCapture.rows,
			NextPaneID: tabCapture.nextPaneID, Focus: tabCapture.focus, Tree: tabCapture.tree,
			Panes: make([]snapcodec.Pane, 0, len(tabCapture.panes)),
		}
		paneIDs := make(map[layout.PaneID]struct{}, len(tabCapture.panes))
		for _, paneCapture := range tabCapture.panes {
			sealed, tail, historyErr := vt.MarshalSealedHistory(paneCapture.history)
			visible, visibleErr := vt.MarshalVisible(paneCapture.visible)
			if historyErr != nil || visibleErr != nil {
				return nil, fmt.Errorf("terminal snapshot pane %s: history=%w visible=%w", paneCapture.id, historyErr, visibleErr)
			}
			tabSnap.Panes = append(tabSnap.Panes, snapcodec.Pane{ID: paneCapture.id, StableID: paneCapture.stableID, Cwd: paneCapture.cwd, SealedChunks: sealed, Tail: tail, Visible: visible, Process: paneCapture.process})
			paneIDs[paneCapture.id] = struct{}{}
		}
		if tabSnap.Tree != nil {
			pruneTreeToPanes(tabSnap.Tree.Root, paneIDs)
		}
		snap.Tabs = append(snap.Tabs, tabSnap)
	}
	return d.snapshotMarshal(snap)
}

func (d *Daemon) finishSnapshotCapture(sess *session, generation uint64, succeeded bool) {
	sess.snapshotMu.Lock()
	sess.snapshotPending = false
	if succeeded && sess.snapshotGeneration == generation {
		sess.snapDirty.Store(false)
	} else if !succeeded {
		sess.snapDirty.Store(true)
	}
	sess.snapshotMu.Unlock()
}

func (d *Daemon) startSnapshotEncodeWorker(ctx context.Context) {
	if d == nil || ctx.Err() != nil {
		return
	}
	d.snapshotWorkerMu.Lock()
	if d.snapshotWorkerCancel != nil {
		d.snapshotWorkerMu.Unlock()
		return
	}
	workerCtx, cancel := context.WithCancel(ctx)
	d.snapshotWorkerCtx = workerCtx
	d.snapshotWorkerCancel = cancel
	d.snapshotWorkerDone = make(chan struct{})
	done := d.snapshotWorkerDone
	d.snapshotWorkerMu.Unlock()
	go func() {
		defer close(done)
		for {
			select {
			case <-workerCtx.Done():
				return
			case capture := <-d.snapshotJobs:
				// A queued capture must not begin persistence after shutdown.
				if workerCtx.Err() != nil {
					d.finishSnapshotCapture(capture.session, capture.generation, false)
					return
				}
				data, err := d.encodeSnapshotCapture(capture)
				if err == nil {
					err = d.snaps.Write(capture.name, data)
				}
				if err != nil {
					d.log.Warn("writing session snapshot failed", "err", err, "session", capture.name)
				}
				d.finishSnapshotCapture(capture.session, capture.generation, err == nil)
			}
		}
	}()
}

func (d *Daemon) stopSnapshotEncodeWorker() {
	if d == nil {
		return
	}
	d.snapshotWorkerMu.Lock()
	defer d.snapshotWorkerMu.Unlock()
	cancel, done := d.snapshotWorkerCancel, d.snapshotWorkerDone
	if cancel == nil {
		return
	}
	// Keep workerMu held until the old worker has exited and its queue has been
	// drained, so a concurrent restart cannot consume a stale capture.
	d.snapshotWorkerCtx = nil
	d.snapshotWorkerCancel = nil
	d.snapshotWorkerDone = nil
	cancel()
	<-done
	for {
		select {
		case capture := <-d.snapshotJobs:
			d.finishSnapshotCapture(capture.session, capture.generation, false)
		default:
			return
		}
	}
}

func (d *Daemon) capturePaneProcess(pty interface{ ForegroundPgid() (int, error) }, shellPid int) *snapcodec.Process {
	if d == nil || pty == nil || shellPid <= 0 || d.procGroupArgv == nil {
		return nil
	}
	pgid, err := pty.ForegroundPgid()
	if err != nil || pgid <= 0 || pgid == shellPid {
		return nil
	}
	argv, err := d.procGroupArgv(pgid, shellPid)
	if err != nil || len(argv) == 0 || argv[0] == "" {
		return nil
	}
	strategy := detectProcessStrategy(argv)
	return &snapcodec.Process{
		Argv:     append([]string(nil), argv...),
		Strategy: strategy,
		Opts: snapcodec.ProcessOpts{
			AgentSessionID: extractAgentSessionID(strategy, argv),
		},
	}
}

func (d *Daemon) snapshotSaver(ctx context.Context) {
	t := d.clock.NewTimer(snapshotInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}

		d.mu.Lock()
		sessions := d.sessionsSnapshotLocked()
		d.mu.Unlock()
		for _, sess := range sessions {
			if sess.snapEligible.Load() && sess.snapDirty.Load() {
				d.scheduleSnapshot(sess)
			}
		}
		t.Reset(snapshotInterval)
	}
}

func pruneTreeToPanes(n *layout.Node, panes map[layout.PaneID]struct{}) bool {
	if n == nil {
		return false
	}
	if n.Kind == layout.Leaf {
		_, ok := panes[n.Leaf]
		return ok
	}
	kept := n.Children[:0]
	for _, child := range n.Children {
		if pruneTreeToPanes(child, panes) {
			kept = append(kept, child)
		}
	}
	n.Children = kept
	return len(n.Children) > 0
}
