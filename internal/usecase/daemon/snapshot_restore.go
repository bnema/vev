package daemon

import (
	"context"
	"fmt"
	"math"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/vt"
)

func (d *Daemon) closeRestoreDone() {
	d.restoreOnce.Do(func() { close(d.restoreDone) })
}

func (d *Daemon) restoreSnapshots(ctx context.Context) {
	if d == nil {
		return
	}
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

func (d *Daemon) restoreSession(ctx context.Context, snap snapcodec.Session) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	d.mu.Lock()
	if d.closing || d.findByNameLocked(snap.Name) != nil {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	parent := d.serveCtx
	if parent == nil {
		parent = context.Background()
	}
	sctx, cancel := context.WithCancel(parent)
	opened := make([]*tab, 0, len(snap.Tabs))
	// Snapshot restore runs before any client Hello is available. Start panes
	// from the daemon environment captured at startup; the next attach replaces
	// this snapshot for future panes from the client's capability.
	restoreTerm := terminalEnv{}
	restoreEnv := copyEnvironment(d.baseEnv)
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
			command, args := d.ptyCommand(restoreEnv)
			pty, err := d.ptys.Open(ctx, command, args, childEnvFrom(restoreEnv, snap.Name, tabStableID, paneStableID, restoreTerm), paneSnap.Cwd, contentSize)
			if err != nil {
				closeOpened()
				return err
			}
			p := newPaneWithStableID(paneSnap.ID, paneStableID, pty, contentSize)
			p.ctx, p.cancel = context.WithCancel(tb.ctx)
			p.rect = contentRect
			if err := restorePaneTerminal(p, paneSnap); err != nil {
				// p is not in tb.panes yet, so closeOpened cannot reach it.
				p.cancel()
				_ = pty.Close()
				closeOpened()
				return err
			}
			if restoreCommand != "" {
				if _, err := pty.Write([]byte(restoreCommand + "\n")); err != nil {
					d.log.Warn("writing snapshot restore command failed", "err", err, "session", snap.Name, "pane", paneSnap.ID)
					d.NotifyGlobal(domain.NoticeWarn, domain.NoticeAutoResume,
						"couldn't restore the running program in session "+snap.Name, err)
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
	sess := &session{name: snap.Name, ctx: sctx, cancel: cancel, tabs: opened, active: int(snap.Active), terminal: restoreTerm, env: restoreEnv, createdAt: createdAt, snapshotWake: d.snapshotWake, snapshotChunkCache: newSnapshotChunkCache(snapshotChunkCacheLimit)}
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

// snapshotCoordinatorContext returns the cancellation context shared by this
// session's queued and in-flight captures. snapshotMu serializes creation with

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
