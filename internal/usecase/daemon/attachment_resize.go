package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// requestResize reports reset invalidation completion for immediate
// attached requests, and coordinator schedule acceptance for async requests.
// Headless requests have no reset invalidation and report geometry completion.
func (g *sharedPTYGeometry) requestResize(d *Daemon, sess *session, ac *attachedClient, size domain.Size, immediate bool) bool {
	if ac == nil {
		return g.requestResizeForLease(d, sess, nil, nil, size, immediate)
	}
	rc := d.attachCoordinator(sess, nil, ac, true)
	return g.requestResizeForLease(d, sess, ac, rc.attachmentLease(ac), size, immediate)
}

// requestResizeForLease carries the Connection's captured lease
// through the full resize transaction and every delayed retry callback.
func (g *sharedPTYGeometry) requestResizeForLease(d *Daemon, sess *session, ac *attachedClient, lease *attachmentLease, size domain.Size, immediate bool) bool {
	valid := sess != nil && size.Valid()
	d.observeRuntime(ports.RuntimeResizeRequested, 0, valid)
	if !valid {
		return false
	}
	if ac == nil {
		// Headless geometry has no coordinator/transport to coalesce, but keeps
		// the same prepare/apply/commit ordering.
		_, ok := g.applySessionLayout(d, sess, size, nil, nil)
		return ok
	}
	rc := sess.renderCoordinator()
	if rc == nil || !rc.leaseCurrent(lease, true) {
		return false
	}
	geometryClaim, claimed := g.claimSize(sess, ac, size)
	if !claimed {
		return false
	}
	resize := attachmentResize{d: d, sess: sess, ac: ac, lease: lease}
	if immediate {
		resize.epoch = rc.recordResizeRequestForLeaseWithClaim(size, ac, lease, geometryClaim)
		if resize.epoch == 0 {
			g.release(sess, geometryClaim)
			return false
		}
		return resize.run()
	}
	epoch := rc.scheduleResizeForLeaseWithClaim(size, ac, lease, geometryClaim, func(epoch uint64) {
		accepted := resize
		accepted.epoch = epoch
		accepted.run()
	})
	if epoch == 0 {
		g.release(sess, geometryClaim)
	}
	return epoch != 0
}

// attachmentResize is one accepted resize of an attachment's session
// geometry. It owns the complete lifecycle after the coordinator admits an
// epoch: the session layout transaction (applySessionLayout), attachment size
// publication, bounded background retries for members whose PTY resize
// failed, and the single reset frame. The coordinator keeps scheduling and
// epoch fencing; this type keeps what an accepted epoch does.
type attachmentResize struct {
	d     *Daemon
	sess  *session
	ac    *attachedClient
	lease *attachmentLease
	epoch uint64
}

func (r attachmentResize) retryCurrent(rc *renderCoordinator) bool {
	return rc.retryCurrentForLease(r.epoch, r.ac, r.lease)
}

func (r attachmentResize) scheduleRetry(rc *renderCoordinator, failed []resizeMember) {
	rc.scheduleResizeRetryForLease(r.epoch, r.ac, r.lease, func() { r.retry(failed) })
}

// run is the accepted attachment resize: claim shared geometry, apply the
// session layout transaction, publish the attachment size, schedule retries
// for failed members, and emit exactly one reset frame. Every step is fenced
// by the attachment effect, the geometry claim, and the resize epoch.
func (r attachmentResize) run() bool {
	d, sess, ac, lease, epoch := r.d, r.sess, r.ac, r.lease, r.epoch
	g := &sess.geometry
	rc := sess.renderCoordinator()
	if rc == nil {
		return false
	}
	snap := rc.resizeSnapshot()
	geometryClaim := snap.geometryClaim
	if geometryClaim == nil {
		var claimed bool
		geometryClaim, claimed = g.claimSize(sess, ac, snap.size)
		if !claimed {
			return false
		}
	}
	effect, admitted := beginAttachmentLeaseEffect(sess, ac, lease)
	if !admitted {
		return false
	}
	defer effect.End()
	current := func() bool {
		return effect.current() && sess.geometry.current(geometryClaim) &&
			rc.resizeCurrentForLease(epoch, ac, lease, false)
	}
	if !current() {
		return false
	}
	d.exitCopyMode(ac)
	failed, ok := g.applySessionLayout(d, sess, snap.size, current, func() bool {
		return rc.resizeCurrentForLease(epoch, ac, lease, true)
	})
	if !ok {
		return false
	}
	// Revalidate while holding the attachment output boundary. A lease can be
	// retired after the layout commit but before this attachment-local size is
	// published; stale generations must not publish any remaining effects.
	d.observeBeforeResizeOwnerPostEffect(resizeOwnerPostCommitPublication)
	ac.sendMu.Lock()
	if !current() {
		ac.sendMu.Unlock()
		return false
	}
	ac.setSize(snap.size)
	ac.sendMu.Unlock()
	if !current() {
		return false
	}
	if len(failed) != 0 && rc.resizeRetryAvailableForLease(r.epoch, r.ac, r.lease) {
		r.scheduleRetry(rc, failed)
	}
	if !current() {
		return false
	}
	d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
	// A successful transaction publishes exactly one full S2 frame. The
	// coordinator is the only emission route and stale epochs never reach it.
	if !current() || !rc.invalidateForLeaseAtResizeEpoch(ac, lease, epoch, renderInvalidation{class: invalidateUrgent, reset: true, producer: "transactional_resize.go"}) {
		return false
	}
	// The resize debounce has already elapsed. Consume this sticky reset now;
	// fire preserves ACK and synchronized-output gates rather than scheduling a
	// second urgent deadline.
	if !current() {
		return false
	}
	rc.fireCurrent(false)
	return true
}

// retryResizeMembers retries failed committed members through a freshly
// prepared tab transaction. The captured members identify retry candidates
// only: their rectangles must never cross this delayed boundary, because any
// later layout mutation may have changed their geometry or removed them.
func (r attachmentResize) retry(members []resizeMember) {
	d, sess := r.d, r.sess
	g := &sess.geometry
	rc := sess.renderCoordinator()
	if rc == nil || !r.retryCurrent(rc) {
		return
	}

	// Retain tab order for deterministic external PTY ordering while collapsing
	// several failed members from the same tab into one canonical transaction.
	tabs := make([]*tab, 0, len(members))
	seen := make(map[*tab]struct{}, len(members))
	for _, member := range members {
		if member.tab == nil || member.isFloating || !resizeMemberOwnerCurrent(&member) {
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
		if !r.retryCurrent(rc) {
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
		freshFailed, ok := g.applyTabLayoutTransaction(d, sess, tb, func() bool {
			return r.retryCurrent(rc)
		})
		if !ok || !r.retryCurrent(rc) {
			return
		}
		failed = append(failed, freshFailed...)
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
		if !member.isFloating || member.tab == nil || member.pane == nil {
			continue
		}
		if !r.retryCurrent(rc) {
			return
		}
		// Floating panes are outside tb.panes. Validate the exact accepted slot
		// before preparing a fresh geometry; applyVisibleFloatingLayout repeats
		// the same pointer/generation check around external PTY.Resize.
		member.tab.mu.Lock()
		currentSlot := member.tab.floating.state == floatingVisible &&
			member.tab.floating.generation == member.floatingGeneration &&
			member.tab.floating.pane == member.pane && resizeMemberOwnerCurrent(&member)
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
		freshFailed, ok := g.applyVisibleFloatingLayoutForMember(d, sess, member.tab, func() bool {
			return r.retryCurrent(rc)
		}, &member)
		if !ok || !r.retryCurrent(rc) {
			return
		}
		failed = append(failed, freshFailed...)
		member.tab.mu.Lock()
		stillCurrent := member.tab.floating.state == floatingVisible &&
			member.tab.floating.generation == member.floatingGeneration &&
			member.tab.floating.pane == member.pane && resizeMemberOwnerCurrent(&member)
		member.tab.mu.Unlock()
		if stillCurrent && len(freshFailed) == 0 {
			member.pane.mu.Lock()
			succeeded = succeeded || !member.pane.resizeRetry
			member.pane.mu.Unlock()
		}
	}
	if len(failed) != 0 && rc.resizeRetryAvailableForLease(r.epoch, r.ac, r.lease) {
		r.scheduleRetry(rc, failed)
	}
	if succeeded {
		// Retry completion changes VT state after the original layout commit.
		// Keep a named session's eventual snapshot generation aligned with it.
		markSnapshotDirty(sess)
		rc.invalidateForLease(r.ac, r.lease, renderInvalidation{class: invalidateUrgent, reset: true, producer: "transactional_resize.go"})
	}
}
