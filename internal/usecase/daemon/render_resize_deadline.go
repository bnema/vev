package daemon

import "github.com/bnema/vev/internal/domain"

// Resize deadlines and retries own only scheduling mechanics. Attachment and
// epoch validity stay with renderCoordinator, alongside the state they guard.

// recordResizeRequest records the latest requested geometry for the attachment
// currently owning source. Production callbacks use recordResizeRequestForLease
// so a captured lease, rather than a reused attachedClient pointer, is checked.
func (c *renderCoordinator) recordResizeRequest(size domain.Size, source *attachedClient) uint64 {
	return c.recordResizeRequestForLease(size, source, c.attachmentLease(source))
}

// recordResizeRequestForLease records a latest-wins request only for this exact
// attachment incarnation. A same-object resume installs a different lease.
func (c *renderCoordinator) recordResizeRequestForLease(size domain.Size, source *attachedClient, lease *attachmentLease) uint64 {
	c.mu.Lock()
	if !c.leaseCurrentLocked(lease, false) || lease.attachment != source {
		c.mu.Unlock()
		return 0
	}
	// An intervening request supersedes any failed-pane retry, even if a fake
	// timer delivers a stopped callback afterwards.
	_, retryTimer := c.retryTimerOwner().replaceLocked()
	c.resize.epoch++
	c.resize.size = size
	c.resize.source = source
	c.resize.lease = lease
	epoch := c.resize.epoch
	c.mu.Unlock()
	stopAndJoinTimerWorker(retryTimer, nil)
	return epoch
}

// scheduleResizeForLease runs apply after the bounded bulk window. The timer
// callback validates the precise lease captured at request dispatch.
func (c *renderCoordinator) scheduleResizeForLease(size domain.Size, source *attachedClient, lease *attachmentLease, run func(uint64)) uint64 {
	epoch := c.recordResizeRequestForLease(size, source, lease)
	if epoch == 0 {
		return 0
	}
	c.mu.Lock()
	_, old := c.resizeTimerOwner().replaceLocked()
	gen := c.resizeGen
	clock := c.opts.clock
	c.mu.Unlock()
	stopAndJoinTimerWorker(old, nil)
	if clock == nil {
		if c.resizeCurrentForLease(epoch, source, lease, false) {
			run(epoch)
		}
		c.completeResizeTimer(gen)
		return epoch
	}
	timer := clock.NewTimer(minOutputRenderDeadline)
	timerC := timer.C()
	if timerC == nil {
		stopTimer(timer)
		if c.resizeCurrentForLease(epoch, source, lease, false) {
			run(epoch)
		}
		c.completeResizeTimer(gen)
		return epoch
	}
	c.mu.Lock()
	valid := !c.torndown && c.resizeGen == gen && c.leaseCurrentLocked(lease, false) && c.resize.lease == lease && c.resize.epoch == epoch && c.resize.source == source
	var cancel, done chan struct{}
	if valid {
		cancel, done, valid = c.resizeTimerOwner().publishLocked(gen, timer)
	}
	c.mu.Unlock()
	if !valid {
		stopTimer(timer)
		c.completeResizeTimer(gen)
		return epoch
	}
	runTimerWorker(timerC, cancel, done, func() {
		c.mu.Lock()
		valid := !c.torndown && c.resizeGen == gen && c.leaseCurrentLocked(lease, false) && c.resize.lease == lease && c.resize.epoch == epoch && c.resize.source == source
		c.mu.Unlock()
		c.clearResizeTimer(gen)
		if valid {
			run(epoch)
		}
	})
	return epoch
}

// clearResizeTimer releases timer ownership only for the matching resize generation.
// A stale callback must not clear a newer request's timer or cancellation channel.
func (c *renderCoordinator) clearResizeTimer(gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resizeTimerOwner().clearLocked(gen)
}

func (c *renderCoordinator) completeResizeTimer(gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resizeTimerOwner().completeLocked(gen)
}

func (c *renderCoordinator) resizeCurrentForLease(epoch uint64, source *attachedClient, lease *attachmentLease, commit bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.leaseCurrentLocked(lease, false) || lease.attachment != source || c.resize.lease != lease || c.resize.source != source || c.resize.epoch != epoch {
		return false
	}
	if commit {
		if c.resize.committed >= epoch {
			return false
		}
		c.resize.committed = epoch
	}
	return true
}

func (c *renderCoordinator) scheduleResizeRetryForLease(epoch uint64, source *attachedClient, lease *attachmentLease, run func()) {
	c.mu.Lock()
	if !c.leaseCurrentLocked(lease, false) || lease.attachment != source || c.resize.lease != lease || c.resize.source != source || c.resize.epoch != epoch || c.resize.committed != epoch {
		c.mu.Unlock()
		return
	}
	_, old := c.retryTimerOwner().replaceLocked()
	gen := c.retryGen
	clock := c.opts.clock
	c.mu.Unlock()
	stopAndJoinTimerWorker(old, nil)
	if clock == nil {
		if c.retryCurrentForLease(epoch, source, lease) {
			run()
		}
		return
	}
	timer := clock.NewTimer(minOutputRenderDeadline)
	timerC := timer.C()
	if timerC == nil {
		// A nil timer channel is the deterministic disabled-clock contract used
		// by headless tests; do not spin retries synchronously.
		stopTimer(timer)
		return
	}
	c.mu.Lock()
	valid := !c.torndown && c.retryGen == gen && c.leaseCurrentLocked(lease, false) && c.resize.lease == lease && c.resize.epoch == epoch && c.resize.committed == epoch && c.resize.source == source
	var cancel, done chan struct{}
	if valid {
		cancel, done, valid = c.retryTimerOwner().publishLocked(gen, timer)
	}
	c.mu.Unlock()
	if !valid {
		stopTimer(timer)
		return
	}
	runTimerWorker(timerC, cancel, done, func() {
		c.mu.Lock()
		valid := !c.torndown && c.retryGen == gen && c.leaseCurrentLocked(lease, false) && c.resize.lease == lease && c.resize.epoch == epoch && c.resize.committed == epoch && c.resize.source == source
		if valid {
			c.retryTimerOwner().clearLocked(gen)
		}
		c.mu.Unlock()
		if valid {
			run()
		}
	})
}

func (c *renderCoordinator) retryCurrentForLease(epoch uint64, source *attachedClient, lease *attachmentLease) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaseCurrentLocked(lease, false) && lease.attachment == source && c.resize.lease == lease && c.resize.source == source && c.resize.epoch == epoch && c.resize.committed == epoch
}

// resizeCallbackDone returns the completion edge for the latest scheduled
// resize callback. A nil result means no resize deadline is pending.
func (c *renderCoordinator) resizeCallbackDone() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resizeDone
}

// resizeSnapshot returns a locked copy of the latest resize metadata.
func (c *renderCoordinator) resizeSnapshot() resizeRequestMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resize
}
