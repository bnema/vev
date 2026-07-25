package daemon

import snapcodec "github.com/bnema/vev/internal/usecase/snapshot"

// captureSession rotates history tails and clones visible frames while holding
// each pane lock. The returned capture contains only immutable state; encoding
// and persistence are deliberately deferred to snapshotEncodeWorker.
func (d *Daemon) captureSnapshotState(sess *session, generation uint64) (*snapshotCapture, bool) {
	sess.mu.Lock()
	capture := &snapshotCapture{
		session:     sess,
		generation:  generation,
		name:        sess.name,
		incarnation: sess.incarnation,
		createdAt:   uint64(sess.createdAt),
		active:      uint16(max(sess.active, 0)),
	}
	ephemeral := sess.ephemeral
	fallbackCwd := sess.cwd
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	if ephemeral || capture.name == "" {
		return capture, false
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
				visible:  p.screen.PrimaryVisibleSnapshot(),
			}
			paneCapture.sealed = p.history.SnapshotView()
			paneCapture.tail = paneCapture.sealed.Tail()
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
