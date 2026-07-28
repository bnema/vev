package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const maxResizeRetryAttempts = 3

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
	c.resize.retryAttempts = 0
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

type resizeRetryReservation struct {
	coordinator *renderCoordinator
	epoch       uint64
	source      *attachedClient
	lease       *attachmentLease
	run         func()
	generation  uint64
	old         *timerToken
	clock       ports.Clock
}

// reserveResizeRetryForLease publishes the retry-lane generation only. Timer
// calls are deferred to finish so owner fences protect no external operation.
func (c *renderCoordinator) reserveResizeRetryForLease(epoch uint64, source *attachedClient, lease *attachmentLease, run func()) (*resizeRetryReservation, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.torndown || !c.leaseCurrentLocked(lease, false) || lease.attachment != source || c.resize.lease != lease || c.resize.source != source || c.resize.epoch != epoch || c.resize.committed != epoch || c.resize.retryAttempts >= maxResizeRetryAttempts {
		return nil, false
	}
	c.resize.retryAttempts++
	gen, old := c.retryLane.replaceLocked()
	return &resizeRetryReservation{coordinator: c, epoch: epoch, source: source, lease: lease, run: run, generation: gen, old: old, clock: c.opts.clock}, true
}

func (r *resizeRetryReservation) finish() {
	if r == nil || r.coordinator == nil {
		return
	}
	c := r.coordinator
	stopDetachedTimer(r.old)
	if r.clock == nil {
		r.run()
		return
	}
	timer := r.clock.NewTimer(minOutputRenderDeadline)
	timerC := timer.C()
	if timerC == nil {
		timer.Stop()
		return
	}
	c.mu.Lock()
	valid := !c.torndown && c.retryLane.generation == r.generation && c.leaseCurrentLocked(r.lease, false) && c.resize.lease == r.lease && c.resize.epoch == r.epoch && c.resize.committed == r.epoch && c.resize.source == r.source
	if valid {
		token := c.retryLane.publishLocked(r.generation, timer)
		valid = token != nil
		if valid {
			c.supervisor.startLocked(token, timerC, func() {
				c.mu.Lock()
				valid := !c.torndown && c.retryLane.token == token && c.retryLane.generation == token.generation && c.leaseCurrentLocked(r.lease, false) && c.resize.lease == r.lease && c.resize.epoch == r.epoch && c.resize.committed == r.epoch && c.resize.source == r.source
				if valid {
					c.retryLane.clearLocked(token)
				}
				c.mu.Unlock()
				if valid {
					r.run()
				}
			})
		}
	}
	c.mu.Unlock()
	if !valid {
		timer.Stop()
	}
}

func (c *renderCoordinator) scheduleResizeRetryForLease(epoch uint64, source *attachedClient, lease *attachmentLease, run func()) {
	reservation, ok := c.reserveResizeRetryForLease(epoch, source, lease, run)
	if !ok {
		return
	}
	reservation.finish()
}

func (c *renderCoordinator) resizeRetryAvailableForLease(epoch uint64, source *attachedClient, lease *attachmentLease) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.torndown && c.leaseCurrentLocked(lease, false) && lease.attachment == source && c.resize.lease == lease && c.resize.source == source && c.resize.epoch == epoch && c.resize.committed == epoch && c.resize.retryAttempts < maxResizeRetryAttempts
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
