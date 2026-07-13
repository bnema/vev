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

// renderCoordinator returns the coordinator installed for this session.
func (s *session) renderCoordinator() *renderCoordinator { return s.coordinator.Load() }

// installRenderCoordinator publishes rc as the session's coordinator.
func (s *session) installRenderCoordinator(rc *renderCoordinator) { s.coordinator.Store(rc) }

func (c *renderCoordinator) fireCurrent(watchdog bool) {
	c.mu.Lock()
	gen := c.generation
	c.mu.Unlock()
	c.fire(gen, watchdog, watchdog)
}
