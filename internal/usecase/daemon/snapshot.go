package daemon

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

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
			pty, err := d.ptys.Open(d.serveCtx, d.shell, d.shellArgs, d.childEnv(snap.Name, tabStableID, paneStableID, restoreTerm), paneSnap.Cwd, contentSize)
			if err != nil {
				closeOpened()
				return err
			}
			p := newPaneWithStableID(paneSnap.ID, paneStableID, pty, contentSize)
			p.ctx, p.cancel = context.WithCancel(tb.ctx)
			p.rect = contentRect
			seedPaneRows(p, paneSnap.Scrollback, paneSnap.Visible)
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
	if sess == nil {
		return
	}
	if sess.snapEligible.Load() {
		sess.snapDirty.Store(true)
	}
}

func seedPaneRows(p *pane, scrollback, visible [][]renderer.Cell) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scrollback = scopy.NewScrollback(defaultScrollbackRows)
	for _, row := range scrollback {
		p.scrollback.Append(row)
	}
	if len(visible) > 0 {
		p.screen = vt.NewScreen(p.screen.Frame.Width, p.screen.Frame.Height)
		for y, row := range visible {
			if y >= p.screen.Frame.Height {
				break
			}
			for x, cell := range row {
				if x >= p.screen.Frame.Width {
					break
				}
				p.screen.Frame.Set(x, y, cell)
			}
		}
	}
	p.screen.OnLineEvicted = p.scrollback.Append
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

func (d *Daemon) captureSession(sess *session) bool {
	if d == nil || sess == nil || !d.snapsEnabled || d.snaps == nil {
		return true
	}

	sess.mu.Lock()
	name := sess.name
	ephemeral := sess.ephemeral
	createdAt := sess.createdAt
	active := sess.active
	fallbackCwd := sess.cwd
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	if ephemeral || name == "" {
		return true
	}

	snap := snapcodec.Session{Name: name, CreatedAt: uint64(createdAt), Active: uint16(max(active, 0))}
	for _, tb := range tabs {
		tb.mu.Lock()
		tabSnap := snapcodec.Tab{
			StableID:   tb.stableID,
			Cols:       uint16(max(tb.size.Cols, 0)),
			Rows:       uint16(max(tb.size.Rows, 0)),
			NextPaneID: uint64(max(tb.nextPaneID, 0)),
			Tree:       tb.tree.Clone(),
		}
		if tb.tree != nil {
			tabSnap.Focus = tb.tree.Focus
		}
		panes := make([]*pane, 0, len(tb.panes))
		for _, p := range tb.panes {
			panes = append(panes, p)
		}
		tb.mu.Unlock()

		paneIDs := make(map[layout.PaneID]struct{}, len(panes))
		paneSnaps := make([]snapcodec.Pane, 0, len(panes))
		for _, p := range panes {
			p.mu.Lock()
			pid := 0
			pty := p.pty
			if pty != nil {
				pid = pty.Pid()
			}
			paneSnap := snapcodec.Pane{
				ID:         p.id,
				StableID:   p.stableID,
				Scrollback: cloneRows(p.scrollback.Snapshot()),
				Visible:    cloneRows(p.screen.PrimaryVisibleRows()),
			}
			p.mu.Unlock()
			paneSnap.Cwd = fallbackCwd
			if d.procCwd != nil && pid > 0 {
				if cwd, err := d.procCwd(pid); err == nil && cwd != "" {
					paneSnap.Cwd = cwd
				}
			}
			paneSnap.Process = d.capturePaneProcess(pty, pid)
			paneIDs[paneSnap.ID] = struct{}{}
			paneSnaps = append(paneSnaps, paneSnap)
		}
		if tabSnap.Tree != nil {
			pruneTreeToPanes(tabSnap.Tree.Root, paneIDs)
		}
		tabSnap.Panes = paneSnaps
		snap.Tabs = append(snap.Tabs, tabSnap)
	}

	data, err := snapcodec.Marshal(snap)
	if err != nil {
		d.log.Warn("marshaling session snapshot failed", "err", err, "session", name)
		return false
	}
	if err := d.snaps.Write(name, data); err != nil {
		d.log.Warn("writing session snapshot failed", "err", err, "session", name)
		return false
	}
	return true
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
			if sess.snapEligible.Load() && sess.snapDirty.Swap(false) {
				if !d.captureSession(sess) {
					sess.snapDirty.Store(true)
				}
			}
		}
		t.Reset(snapshotInterval)
	}
}

func cloneRows(rows [][]renderer.Cell) [][]renderer.Cell {
	if len(rows) == 0 {
		return nil
	}
	out := make([][]renderer.Cell, len(rows))
	for i, row := range rows {
		out[i] = append([]renderer.Cell(nil), row...)
	}
	return out
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
