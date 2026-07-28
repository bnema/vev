package daemon

// migratePaneSyncOwnerLocked transfers synchronized-output coordination after
// an ownership commit. The caller must hold p.mu and pass the exact leases
// returned by publishOwnerLocked. Holding the pane parsing fence keeps PTY
// writes from observing a coordinator between owners.
//
// The returned cleanup must be finished after releasing pane and coordinator
// locks. It stops detached timers but never joins their workers.
func (d *Daemon) migratePaneSyncOwnerLocked(p *pane, oldOwner, newOwner paneEffectLease) syncTimerCleanup {
	var cleanup syncTimerCleanup
	if p == nil || oldOwner.pane != p || newOwner.pane != p || newOwner.owner == nil || p.owner.Load() != newOwner.owner {
		return cleanup
	}

	oldGeneration := p.syncGen
	if oldOwner.owner != nil && oldOwner.owner.session != nil {
		if coordinator := oldOwner.owner.session.renderCoordinator(); coordinator != nil {
			cleanup.append(coordinator.detachSyncBatchGeneration(p, oldGeneration))
		}
	}
	p.syncGen = 0
	if p.screen == nil || !p.screen.SyncUpdateActive() || newOwner.owner.session == nil || newOwner.owner.tab == nil {
		return cleanup
	}

	generation := newOwner.owner.session.syncGen.Add(1)
	p.syncGen = generation
	coordinator := newOwner.owner.session.renderCoordinator()
	if coordinator == nil {
		return cleanup
	}

	lease := newOwner
	renderable := func() bool {
		return lease.Current() && d.paneRenderable(lease.owner.session, lease.owner.tab, p)
	}
	forceEnd := func() {
		p.mu.Lock()
		if p.owner.Load() == lease.owner && p.syncGen == generation && p.screen.SyncUpdateActive() {
			p.screen.ForceSyncEnd()
		}
		p.mu.Unlock()
	}
	cleanup.append(coordinator.beginSyncBatchWithRenderability(p, generation, renderable, forceEnd))
	return cleanup
}
