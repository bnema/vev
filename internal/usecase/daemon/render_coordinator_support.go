package daemon

// Small coordinator accessors stay separate from scheduling and lifecycle
// transitions, keeping the coordinator state file focused on ownership.

// burstMetricsSnapshot returns a consistent read-only metric view for
// benchmark reporting without exposing coordinator policy outside this package.
func (c *renderCoordinator) burstMetricsSnapshot() renderCoordinatorBurstMetricsSnapshot {
	return renderCoordinatorBurstMetricsSnapshot{
		invalidations: c.metrics.invalidations.Load(),
		wakes:         c.metrics.wakes.Load(),
		coalesced:     c.metrics.coalesced.Load(),
	}
}

// attachmentRenderCoordinator returns the coordinator installed for the exact
// attachment-session identity.
func attachmentRenderCoordinator(entry attachmentSession) *renderCoordinator {
	if entry == nil || entry.core() == nil {
		return nil
	}
	return entry.core().coordinator.Load()
}

// installAttachmentRenderCoordinator publishes rc for the exact
// attachment-session identity.
func installAttachmentRenderCoordinator(entry attachmentSession, rc *renderCoordinator) {
	if entry == nil || entry.core() == nil {
		return
	}
	entry.core().coordinator.Store(rc)
}

// renderCoordinator retains the local-only method used by PTY and tab owners.
func (s *session) renderCoordinator() *renderCoordinator {
	return attachmentRenderCoordinator(s)
}

// installRenderCoordinator retains the local-only setup seam used by tests.
func (s *session) installRenderCoordinator(rc *renderCoordinator) {
	installAttachmentRenderCoordinator(s, rc)
}

func (c *renderCoordinator) fireCurrent(watchdog bool) {
	c.mu.Lock()
	gen := c.normalLane.generation
	c.mu.Unlock()
	c.fire(gen, watchdog, watchdog)
}
