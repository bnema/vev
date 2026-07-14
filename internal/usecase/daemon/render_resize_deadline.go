package daemon

import "github.com/bnema/vev/internal/domain"

// Resize deadlines and retries retain coordinator-owned attachment, epoch, and
// token validation at every callback effect boundary.
func (c *renderCoordinator) recordResizeRequest(size domain.Size, source *attachedClient) uint64 {
	return c.recordResizeRequestForLease(size, source, c.attachmentLease(source))
}

func (c *renderCoordinator) recordResizeRequestForLease(size domain.Size, source *attachedClient, lease *attachmentLease) uint64 {
	c.mu.Lock()
	if !c.leaseCurrentLocked(lease, false) || lease.attachment != source {
		c.mu.Unlock()
		return 0
	}
	_, retry := c.retryLane.replaceLocked()
	c.resize.epoch++
	c.resize.size, c.resize.source, c.resize.lease = size, source, lease
	epoch := c.resize.epoch
	c.mu.Unlock()
	stopDetachedTimer(retry)
	return epoch
}

func (c *renderCoordinator) scheduleResizeForLease(size domain.Size, source *attachedClient, lease *attachmentLease, run func(uint64)) uint64 {
	epoch := c.recordResizeRequestForLease(size, source, lease)
	if epoch == 0 {
		return 0
	}
	c.mu.Lock()
	gen, old := c.resizeLane.replaceLocked()
	clock := c.opts.clock
	c.mu.Unlock()
	stopDetachedTimer(old)
	if clock == nil {
		if c.resizeCurrentForLease(epoch, source, lease, false) {
			run(epoch)
		}
		return epoch
	}
	timer := clock.NewTimer(minOutputRenderDeadline)
	timerC := timer.C()
	if timerC == nil {
		timer.Stop()
		if c.resizeCurrentForLease(epoch, source, lease, false) {
			run(epoch)
		}
		return epoch
	}
	c.mu.Lock()
	valid := !c.torndown && c.resizeLane.generation == gen && c.leaseCurrentLocked(lease, false) && c.resize.lease == lease && c.resize.epoch == epoch && c.resize.source == source
	var token *timerToken
	if valid {
		token = c.resizeLane.publishLocked(gen, timer)
		valid = token != nil
		if valid {
			c.supervisor.startLocked(token, timerC, func() {
				c.mu.Lock()
				valid := !c.torndown && c.resizeLane.token == token && c.resizeLane.generation == token.generation && c.leaseCurrentLocked(lease, false) && c.resize.lease == lease && c.resize.epoch == epoch && c.resize.source == source
				if valid {
					c.resizeLane.clearLocked(token)
				}
				c.mu.Unlock()
				if valid {
					run(epoch)
				}
			})
		}
	}
	c.mu.Unlock()
	if !valid {
		timer.Stop()
	}
	return epoch
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
	gen, old := c.retryLane.replaceLocked()
	clock := c.opts.clock
	c.mu.Unlock()
	stopDetachedTimer(old)
	if clock == nil {
		return
	}
	timer := clock.NewTimer(minOutputRenderDeadline)
	timerC := timer.C()
	if timerC == nil {
		timer.Stop()
		return
	}
	c.mu.Lock()
	valid := !c.torndown && c.retryLane.generation == gen && c.leaseCurrentLocked(lease, false) && c.resize.lease == lease && c.resize.epoch == epoch && c.resize.committed == epoch && c.resize.source == source
	if valid {
		token := c.retryLane.publishLocked(gen, timer)
		valid = token != nil
		if valid {
			c.supervisor.startLocked(token, timerC, func() {
				c.mu.Lock()
				valid := !c.torndown && c.retryLane.token == token && c.retryLane.generation == token.generation && c.leaseCurrentLocked(lease, false) && c.resize.lease == lease && c.resize.epoch == epoch && c.resize.committed == epoch && c.resize.source == source
				if valid {
					c.retryLane.clearLocked(token)
				}
				c.mu.Unlock()
				if valid {
					run()
				}
			})
		}
	}
	c.mu.Unlock()
	if !valid {
		timer.Stop()
	}
}

func (c *renderCoordinator) retryCurrentForLease(epoch uint64, source *attachedClient, lease *attachmentLease) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaseCurrentLocked(lease, false) && lease.attachment == source && c.resize.lease == lease && c.resize.source == source && c.resize.epoch == epoch && c.resize.committed == epoch
}

func (c *renderCoordinator) resizeSnapshot() resizeRequestMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resize
}
