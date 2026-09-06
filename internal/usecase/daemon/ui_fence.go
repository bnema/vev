package daemon

import "time"

const uiFenceTimeout = 30 * time.Second

// pendingUIFence is owned by coordinator.mu. Its cancellation channel releases
// its one lifecycle-owned timer without invoking clock methods under that lock.
type pendingUIFence struct {
	actionID  uint64
	threshold uint64
	retiring  bool
	cancel    chan struct{}
	canceled  bool
}

func (p *pendingUIFence) cancelLocked() {
	if !p.canceled {
		p.canceled = true
		close(p.cancel)
	}
}

func (lease *attachmentLease) cancelUIFenceLocked() {
	if p := lease.pendingUIFence; p != nil {
		p.cancelLocked()
		lease.pendingUIFence = nil
	}
}

// registerUIFence reserves the capture threshold and urgent invalidation in one
// transaction. Dispatch must not inherit timer fallbacks that synchronously
// render: an ACK later on the same receive loop may be needed to make progress.
func (c *renderCoordinator) registerUIFence(lease *attachmentLease, actionID uint64, expired func(uint64)) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.leaseCurrentLocked(lease, true) || actionID == 0 || lease.pendingUIFence != nil || lease.requestedUIFence == ^uint64(0) {
		return false
	}
	reservation, ok := c.reserveInvalidationForLeaseAtResizeEpochLocked(lease.attachment, lease, 0, renderInvalidation{
		class: invalidateUrgent, producer: "ui-fence",
	})
	if !ok {
		return false
	}
	lease.requestedUIFence++
	pending := &pendingUIFence{actionID: actionID, threshold: lease.requestedUIFence, cancel: make(chan struct{})}
	lease.pendingUIFence = pending
	c.supervisor.startTaskLocked(reservation.finish)
	if clock := c.opts.clock; clock != nil {
		c.supervisor.startTaskLocked(func() {
			timer := clock.NewTimer(uiFenceTimeout)
			defer timer.Stop()
			select {
			case <-pending.cancel:
				return
			case <-timer.C():
			}
			c.mu.Lock()
			current := c.leaseCurrentLocked(lease, true) && lease.pendingUIFence == pending && !pending.retiring
			if current {
				lease.cancelUIFenceLocked()
			}
			c.mu.Unlock()
			if current && expired != nil {
				expired(actionID)
			}
		})
	}
	return true
}

// captureUIFence is sampled after send/capacity admission and before any render
// capture. It must never be called while holding pane ownership.
func (c *renderCoordinator) captureUIFence(lease *attachmentLease) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.leaseCurrentLocked(lease, true) {
		return 0
	}
	return lease.requestedUIFence
}

func (c *renderCoordinator) needsUIFence(lease *attachmentLease, captured uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.uiFenceEligibleLocked(lease, captured)
}

func (c *renderCoordinator) uiFenceEligibleLocked(lease *attachmentLease, captured uint64) bool {
	if !c.leaseCurrentLocked(lease, true) {
		return false
	}
	p := lease.pendingUIFence
	return p != nil && !p.retiring && p.threshold <= captured
}

// retireUIFence selects a single sender only after a successful publication.
// The slot stays occupied until that sender's ordered send succeeds.
func (c *renderCoordinator) retireUIFence(lease *attachmentLease, captured uint64) *pendingUIFence {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.uiFenceEligibleLocked(lease, captured) {
		return nil
	}
	p := lease.pendingUIFence
	p.retiring = true
	p.cancelLocked()
	return p
}

func (c *renderCoordinator) finishUIFence(lease *attachmentLease, pending *pendingUIFence) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaseCurrentLocked(lease, true) && lease.pendingUIFence == pending && pending.retiring {
		lease.cancelUIFenceLocked()
	}
}
