package daemon

import (
	"context"
	"errors"
	"os"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// movePanePostcommitPlan is captured while the move's architecture and owner
// fences still protect publication. It contains only immutable pointers,
// generations, and exact attachment tokens needed by fallible follow-up work.
func firstMovePane(panes []*pane) *pane {
	if len(panes) == 0 {
		return nil
	}
	return panes[0]
}

type movePanePostcommitPlan struct {
	source             *session
	destination        *session
	sourceTab          *tab
	destinationTab     *tab
	movedPane          *pane
	movedPanes         []*pane
	destinationClient  *attachedClient
	sourceCleanupToken attachmentConnectionToken
	handoffResult      attachmentTransitionResult
	syncCleanup        syncTimerCleanup
	frozenEffects      frozenAttachmentEffectGates
	effectsFrozen      bool
	unlockDispatch     func()
	reservation        *moveLifecycleReservation
	oldTabCancel       context.CancelFunc

	sourceTabRemoved bool
	sourceEmpty      bool

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
	if p.oldTabCancel != nil {
		p.oldTabCancel()
	} else if p.sourceTabRemoved && p.sourceTab != nil && p.sourceTab.cancel != nil {
		p.sourceTab.cancel()
	}
	if p.sourceCleanupToken.current() {
		cleanupClient := p.sourceCleanupToken.ac
		panes := p.movedPanes
		if len(panes) == 0 && p.movedPane != nil {
			panes = []*pane{p.movedPane}
		}
		for _, movedPane := range panes {
			if cleanupClient.overlays != nil {
				cleanupClient.overlays.clearCopyModeForPane(movedPane)
			}
			cleanupClient.pruneCaptureFrames(movedPane)
		}
	}

	// Snapshot dirtiness is admitted while lifecycle reservations still pin
	// both sessions. Repository publication remains outside every lock below.
	markSnapshotDirty(p.destination)
	if !p.sourceEmpty {
		markSnapshotDirty(p.source)
	}
	p.syncCleanup.finish()
	if p.effectsFrozen {
		p.frozenEffects.unfreeze()
	}
	p.unlockDispatch()
	p.reservation.Release()

	// Repair attachment-local stable targets after shared topology publication
	// and after the move dispatch locks have been released.
	p.source.repairAttachmentViews()
	if p.destination != p.source {
		p.destination.repairAttachmentViews()
	}

	if !moveSessionRetired(d, p.source) {
		sourceLayoutTab := p.sourceTab
		if p.sourceTabRemoved {
			sourceLayoutTab = p.source.firstTab()
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
	for _, ac := range p.destination.snapshotAttachments() {
		d.invalidateRender(p.destination, ac, true, "move.go")
	}
	if p.sourceCleanupToken.ac != nil && !p.sourceEmpty && sessionClientIs(p.source, p.sourceCleanupToken.ac) {
		d.invalidateRender(p.source, p.sourceCleanupToken.ac, true, "move.go")
	}
	if p.handoffResult.published.ac != nil {
		follower := p.handoffResult.published.ac
		p.destination.selectAttachmentTab(follower, domain.TabStableID(p.destinationTab.stableID))
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
	d.finishParkedAttachmentRetirements(p.retiredParked)
	if p.sourceEmpty && p.sourceMetadataValid && d.persistEnabled {
		if err := d.beginSnapshotPurge(p.source.name, p.source.incarnation); err == nil {
			if err := d.finishSnapshotPurge(d.serveCtx, p.source.name, p.source.incarnation); err != nil {
				d.log.Warn("moving final pane source purge failed", "err", err, "session", p.source.name)
			}
		}
	}
}

// retireEmptyMoveSessionLocked clears the empty source session after the exact
// attachment transfer has committed. It performs no external work.
func retireEmptyMoveSessionLocked(sess *session, retirement frozenMoveAttachmentRetirement) []detachedAttachmentSnapshot {
	if sess == nil || len(sess.attachments) != 0 {
		return nil
	}
	_ = retirement
	sess.tabs = nil
	return nil
}

func moveSessionRetired(d *Daemon, sess *session) bool {
	if d == nil || sess == nil {
		return true
	}
	d.mu.Lock()
	_, live := d.sessions[sess.id]
	d.mu.Unlock()
	return !live
}

func sessionClientIs(sess *session, client *attachedClient) bool {
	if sess == nil || client == nil {
		return false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	_, ok := sess.attachments[client]
	return ok
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
