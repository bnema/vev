package daemon

import (
	"errors"
	"os"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// movePanePostcommitPlan is captured while the move's architecture and owner
// fences still protect publication. It contains only immutable pointers,
// generations, and exact attachment tokens needed by fallible follow-up work.
type movePanePostcommitPlan struct {
	source             *session
	destination        *session
	sourceTab          *tab
	destinationTab     *tab
	movedPane          *pane
	destinationClient  *attachedClient
	sourceCleanupToken attachmentRoleToken
	handoffResult      attachmentTransitionResult
	syncCleanup        syncTimerCleanup
	frozenRoles        frozenRoleEffectGates
	rolesFrozen        bool
	unlockDispatch     func()
	reservation        *moveLifecycleReservation

	sourceTabWasActive bool
	sourceTabRemoved   bool
	sourceEmpty        bool

	retiredParked            []parkedAttachmentRetirement
	retiredAttachments       []detachedAttachmentSnapshot
	sourceMetadata           domain.CatalogueMetadataUpdate
	sourceMetadataValid      bool
	destinationMetadata      domain.CatalogueMetadataUpdate
	destinationMetadataValid bool
}

// execute runs only after every resize-owner fence and architecture lock is
// released. Generation-bound tokens make stale effects drop rather than route
// back to the retired source.
func (p movePanePostcommitPlan) execute(d *Daemon) {
	if p.sourceTabRemoved && p.sourceTab.cancel != nil {
		p.sourceTab.cancel()
	}
	if p.sourceCleanupToken.current() {
		cleanupClient := p.sourceCleanupToken.ac
		if cleanupClient.overlays != nil {
			cleanupClient.overlays.clearCopyModeForPane(p.movedPane)
		}
		cleanupClient.pruneCaptureFrames(p.movedPane)
	}

	// Snapshot dirtiness is admitted while lifecycle reservations still pin
	// both sessions. Repository publication remains outside every lock below.
	markSnapshotDirty(p.destination)
	if !p.sourceEmpty {
		markSnapshotDirty(p.source)
	}
	p.syncCleanup.finish()
	if p.rolesFrozen {
		p.frozenRoles.unfreeze()
	}
	p.unlockDispatch()
	p.reservation.Release()

	if p.sourceTabWasActive && !moveSessionStillEmpty(d, p.source) {
		d.activateTab(p.source, p.source.activeTab())
	}
	if !moveSessionStillEmpty(d, p.source) {
		sourceLayoutTab := p.sourceTab
		if p.sourceTabRemoved {
			sourceLayoutTab = p.source.activeTab()
		}
		if sourceLayoutTab != nil && sourceLayoutTab != p.destinationTab {
			d.applyTabLayout(p.source, sourceLayoutTab)
		}
	}
	d.applyTabLayout(p.destination, p.destinationTab)
	if p.sourceMetadataValid && !p.sourceEmpty {
		d.markCatalogueDirty(p.sourceMetadata)
	}
	if p.destinationMetadataValid {
		d.markCatalogueDirty(p.destinationMetadata)
	}
	if p.destinationClient != nil && sessionClientIs(p.destination, p.destinationClient) && destinationTabIsActive(p.destination, p.destinationTab) {
		d.invalidateRender(p.destination, p.destinationClient, true, "move.go")
	}
	if p.sourceCleanupToken.ac != nil && !p.sourceEmpty && p.sourceTabWasActive && sessionClientIs(p.source, p.sourceCleanupToken.ac) {
		d.invalidateRender(p.source, p.sourceCleanupToken.ac, true, "move.go")
	}
	if p.handoffResult.published.ac != nil {
		follower := p.handoffResult.published.ac
		d.applyHostTheme(p.destination, follower, follower.getClientTheme(), false)
		follower.recordPreviousSession(p.source)
		d.deferAttachmentTransitionCleanups(p.handoffResult)
		d.firstPaintForTransition(p.handoffResult.published)
	}
	if p.sourceEmpty {
		retireEmptySessionAfterMove(d, p.source)
	}
	for _, attachment := range p.retiredAttachments {
		d.unregisterPreview(attachment.ac)
		attachment.ac.clearPreviousSession()
		attachment.ac.clearCaptureFrames()
		d.notifyDetachedSnapshotAsync(attachment, ports.ReasonSessionKilled)
	}
	for _, retirement := range p.retiredParked {
		if retirement.parked == nil || retirement.parked.ac == nil {
			continue
		}
		d.unregisterPreview(retirement.parked.ac)
		retirement.parked.ac.clearCaptureFrames()
	}
	finishParkedAttachmentRetirements(p.retiredParked)
	if p.sourceEmpty && p.sourceMetadataValid && d.persistEnabled {
		if err := d.beginSnapshotPurge(p.source.name, p.source.incarnation); err == nil {
			if err := d.finishSnapshotPurge(d.serveCtx, p.source.name, p.source.incarnation); err != nil {
				d.log.Warn("moving final pane source purge failed", "err", err, "session", p.source.name)
			}
		}
	}
}

// retireEmptyMoveSessionLocked removes all source-owned attachment roles that
// were not transferred. It runs inside the move commit, after the moved pane
// has already acquired its destination owner, and performs no external work.
func retireEmptyMoveSessionLocked(sess *session) []detachedAttachmentSnapshot {
	if sess == nil || sess.client != nil {
		return nil
	}
	retired := make([]detachedAttachmentSnapshot, 0, len(sess.snatched))
	for ac := range sess.snatched {
		retired = append(retired, detachedAttachmentSnapshot{ac: ac, transport: ac.transportSnapshot()})
		ac.roleGeneration.Add(1)
		ac.setSession(nil)
		ac.invalidateFrozenRoleCapability()
	}
	clear(sess.snatched)
	sess.tabs = nil
	sess.active = -1
	return retired
}

func moveSessionStillEmpty(d *Daemon, sess *session) bool {
	if d == nil || sess == nil {
		return true
	}
	d.mu.Lock()
	_, live := d.sessions[sess.id]
	d.mu.Unlock()
	return !live
}

func destinationTabIsActive(sess *session, target *tab) bool {
	if sess == nil || target == nil {
		return false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.active >= 0 && sess.active < len(sess.tabs) && sess.tabs[sess.active] == target
}

func sessionClientIs(sess *session, client *attachedClient) bool {
	if sess == nil || client == nil {
		return false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.client == client
}

// retireEmptySessionAfterMove completes only source-owned lifecycle work. The
// moved pane has already been removed from source.tabs, so stopInMemoryLifecycle
// cannot cancel or close the transferred PTY.
func retireEmptySessionAfterMove(d *Daemon, sess *session) {
	if d == nil || sess == nil {
		return
	}
	sess.stopInMemoryLifecycle()
	if coordinator := sess.renderCoordinator(); coordinator != nil {
		coordinator.beginSessionTeardown().finish()
		coordinator.waitForTimerWorkers()
	}
	sess.mu.Lock()
	clipFiles := append([]string(nil), sess.clipFiles...)
	sess.clipFiles = nil
	sess.mu.Unlock()
	for _, path := range clipFiles {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			d.log.Warn("removing clipboard temp file after move failed", "err", err, "path", path)
		}
	}
}
