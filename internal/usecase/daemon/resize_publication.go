package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type resizeCommitPublication struct {
	members     []resizeMember
	session     *session
	attachment  *attachedClient
	lease       *attachmentLease
	epoch       uint64
	role        attachmentRoleToken
	coordinator *renderCoordinator
	observer    ports.RuntimeObserver
	mark        ports.RuntimeMark
}

func (p resizeCommitPublication) current() bool {
	if !resizeMembersOwnerCurrent(p.members) {
		return false
	}
	if p.attachment == nil {
		return true
	}
	if p.role.effect == nil || p.role.effect.ended.Load() || p.role.sess != p.session ||
		p.role.ac != p.attachment || p.role.role != attachmentActive || p.role.lease != p.lease ||
		p.role.generation != p.attachment.roleGeneration.Load() ||
		!p.attachment.transportSnapshotCurrent(p.role.transport) ||
		p.attachment.currentSession() != p.session || p.coordinator == nil {
		return false
	}
	p.session.mu.Lock()
	active := p.session.attachmentRoleLocked(p.attachment) == attachmentActive
	p.session.mu.Unlock()
	return active && p.coordinator.resizeCurrentForLease(p.epoch, p.attachment, p.lease, false)
}

// emit performs observer work only after every resize-owner fence and
// attachment sendMu are released. The generation-bound publication is checked
// once more so a move that wins after geometry publication but before observer
// admission suppresses stale source telemetry.
func (p resizeCommitPublication) emit() bool {
	if !p.current() {
		return false
	}
	if p.observer != nil {
		p.observer.ObserveRuntime(p.mark)
	}
	return true
}

// publishResizeCommit linearizes attachment geometry and an immutable telemetry
// reservation with pane ownership. sendMu is acquired before resize fences, so
// the canonical rule never acquires an attachment lock while a pane fence is
// held. Observer callbacks run only after both lock families are released.
func (d *Daemon) publishResizeCommit(members []resizeMember, sess *session, ac *attachedClient, lease *attachmentLease, epoch uint64, ticket *roleEffectTicket, size domain.Size) bool {
	d.observeBeforeResizeOwnerPostEffect(resizeOwnerPostCommitPublication)
	if ac != nil {
		ac.sendMu.Lock()
		if d.afterResizeCommitSendLocked != nil {
			d.afterResizeCommitSendLocked()
		}
	}
	fences := acquireResizeOwnerPostEffectFences(members)
	if fences == nil {
		if ac != nil {
			ac.sendMu.Unlock()
		}
		return false
	}
	publication := resizeCommitPublication{
		members:    append([]resizeMember(nil), members...),
		session:    sess,
		attachment: ac,
		lease:      lease,
		epoch:      epoch,
		observer:   d.runtimeObserver,
		mark:       ports.NewRuntimeMark("daemon", ports.RuntimeResizeCommitted, 0, true),
	}
	if ticket != nil {
		publication.role = ticket.roleToken()
		publication.coordinator = sess.renderCoordinator()
	}
	if !publication.current() {
		fences.Release()
		if ac != nil {
			ac.sendMu.Unlock()
		}
		return false
	}
	if ac != nil {
		ac.size = size
	}
	fences.Release()
	if ac != nil {
		ac.sendMu.Unlock()
	}
	return publication.emit()
}

func (d *Daemon) runResizeTransaction(sess *session, ac *attachedClient, lease *attachmentLease, epoch uint64) bool {
	ticket, admitted := beginActiveLeaseEffect(sess, ac, lease)
	if !admitted {
		return false
	}
	defer ticket.End()
	rc := sess.renderCoordinator()
	if rc == nil {
		return false
	}
	snap := rc.resizeSnapshot()
	current := func() bool { return rc.resizeCurrentForLease(epoch, ac, lease, false) }
	if !current() {
		return false
	}
	d.exitCopyMode(ac)
	result, ok := d.applySessionLayout(sess, snap.size, current, func() bool {
		return rc.resizeCurrentForLease(epoch, ac, lease, true)
	})
	if !ok {
		return false
	}
	if !d.publishResizeOwnerPostEffect(result.members, resizeOwnerPostSnapshotDirty, func() {
		markSnapshotDirty(sess)
	}) {
		return false
	}
	if !d.publishResizeCommit(result.members, sess, ac, lease, epoch, ticket, snap.size) {
		return false
	}
	if len(result.failed) != 0 {
		var retry *resizeRetryReservation
		if !d.publishResizeOwnerPostEffect(result.failed, resizeOwnerPostRetrySchedule, func() {
			retry, _ = rc.reserveResizeRetryForLease(epoch, ac, lease, func() {
				d.retryResizeMembers(sess, ac, lease, epoch, result.failed)
			})
		}) || retry == nil {
			return false
		}
		retry.finish()
	}
	d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
	if !d.publishResizeOwnerInvalidation(result.members, sess, ac, lease, epoch,
		renderInvalidation{class: invalidateUrgent, reset: true, producer: "transactional_resize.go"}) {
		return false
	}
	// The resize debounce has already elapsed. Consume this sticky reset now;
	// fire preserves ACK and synchronized-output gates rather than scheduling a
	// second urgent deadline.
	rc.fireCurrent(false)
	return true
}

// retryResizeMembers retries failed committed members through a freshly
// prepared tab transaction. The captured members identify retry candidates
// only: their rectangles must never cross this delayed boundary, because any
// later layout mutation may have changed their geometry or removed them.
func (d *Daemon) retryResizeMembers(sess *session, ac *attachedClient, lease *attachmentLease, epoch uint64, members []resizeMember) {
	ticket, admitted := beginActiveLeaseEffect(sess, ac, lease)
	if !admitted {
		return
	}
	defer ticket.End()
	rc := sess.renderCoordinator()
	if rc == nil || !rc.retryCurrentForLease(epoch, ac, lease) {
		return
	}

	// Retain tab order for deterministic external PTY ordering while collapsing
	// several failed members from the same tab into one canonical transaction.
	tabs := make([]*tab, 0, len(members))
	seen := make(map[*tab]struct{}, len(members))
	for _, member := range members {
		if member.tab == nil || member.isFloating || !resizeMemberOwnerCurrentLocked(&member) {
			continue
		}
		if _, ok := seen[member.tab]; ok {
			continue
		}
		seen[member.tab] = struct{}{}
		tabs = append(tabs, member.tab)
	}

	failed := make([]resizeMember, 0, len(members))
	succeeded := false
	for _, tb := range tabs {
		if !rc.retryCurrentForLease(epoch, ac, lease) {
			return
		}
		// A later accepted layout transaction clears resizeRetry. In that case
		// this delayed callback has no remaining work and, importantly, must not
		// replay the obsolete rectangle it captured before the mutation.
		retryPending := false
		tb.mu.Lock()
		for _, member := range members {
			if member.tab != tb || member.pane == nil || tb.panes[member.pane.id] != member.pane {
				continue
			}
			member.pane.mu.Lock()
			retryPending = retryPending || member.pane.resizeRetry
			member.pane.mu.Unlock()
		}
		tb.mu.Unlock()
		if !retryPending {
			continue
		}

		// applyTabLayoutTransaction captures the current generation, pane
		// pointers, and solved rectangles, and validates them again before any
		// VT/rectangle publication. It also preserves the resize gate and
		// degradation notice behavior for another failed external attempt.
		result, ok := d.applyTabLayoutTransaction(sess, tb, func() bool {
			return rc.retryCurrentForLease(epoch, ac, lease)
		})
		if !ok || !rc.retryCurrentForLease(epoch, ac, lease) {
			return
		}
		failed = append(failed, result.failed...)
		// A retry success is a formerly failed, still-current target whose
		// canonical apply cleared its retry bit. A collapsed or removed target
		// is neither a retry completion nor a reason to publish a reset.
		tb.mu.Lock()
		for _, member := range members {
			if member.tab != tb || member.pane == nil || tb.panes[member.pane.id] != member.pane {
				continue
			}
			member.pane.mu.Lock()
			succeeded = succeeded || !member.pane.resizeRetry
			member.pane.mu.Unlock()
		}
		tb.mu.Unlock()
	}
	for _, member := range members {
		if !member.isFloating || member.tab == nil || member.pane == nil || !resizeMemberOwnerCurrentLocked(&member) {
			continue
		}
		if !rc.retryCurrentForLease(epoch, ac, lease) {
			return
		}
		// Floating panes are outside tb.panes. Validate the exact accepted slot
		// before preparing a fresh geometry; applyVisibleFloatingLayout repeats
		// the same pointer/generation check around external PTY.Resize.
		member.tab.mu.Lock()
		currentSlot := member.tab.floating.state == floatingVisible &&
			member.tab.floating.generation == member.floatingGeneration &&
			member.tab.floating.pane == member.pane &&
			resizeMemberOwnerCurrentLocked(&member)
		retryPending := false
		if currentSlot {
			member.pane.mu.Lock()
			retryPending = member.pane.resizeRetry
			member.pane.mu.Unlock()
		}
		member.tab.mu.Unlock()
		if !currentSlot || !retryPending {
			continue
		}
		result, ok := d.applyVisibleFloatingLayoutForMember(sess, member.tab, func() bool {
			return rc.retryCurrentForLease(epoch, ac, lease)
		}, &member)
		if !ok || !rc.retryCurrentForLease(epoch, ac, lease) {
			return
		}
		failed = append(failed, result.failed...)
		member.tab.mu.Lock()
		stillCurrent := member.tab.floating.state == floatingVisible &&
			member.tab.floating.generation == member.floatingGeneration &&
			member.tab.floating.pane == member.pane &&
			resizeMemberOwnerCurrentLocked(&member)
		member.tab.mu.Unlock()
		if stillCurrent && len(result.failed) == 0 {
			member.pane.mu.Lock()
			succeeded = succeeded || !member.pane.resizeRetry
			member.pane.mu.Unlock()
		}
	}
	if len(failed) != 0 {
		var retry *resizeRetryReservation
		if !d.publishResizeOwnerPostEffect(failed, resizeOwnerPostRetrySchedule, func() {
			retry, _ = rc.reserveResizeRetryForLease(epoch, ac, lease, func() {
				d.retryResizeMembers(sess, ac, lease, epoch, failed)
			})
		}) || retry == nil {
			return
		}
		retry.finish()
	}
	if succeeded {
		// Retry completion changes VT state after the original layout commit.
		// Keep a named session's eventual snapshot generation aligned with it.
		if !d.publishResizeOwnerPostEffect(members, resizeOwnerPostSnapshotDirty, func() {
			markSnapshotDirty(sess)
		}) {
			return
		}
		d.publishResizeOwnerInvalidation(members, sess, ac, lease, 0,
			renderInvalidation{class: invalidateUrgent, reset: true, producer: "transactional_resize.go"})
	}
}
