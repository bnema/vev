package daemon

import (
	"reflect"

	"github.com/bnema/vev/internal/domain"
)

// movePaneRequest identifies both lifecycles and both topology objects by
// immutable identity. Session names are advisory; the ID and incarnation are
// the commit-time authority.
type movePaneRequest struct {
	Attachment       *attachedClient
	AttachmentToken  attachmentConnectionToken
	Source           moveSessionLocator
	SourceTabID      domain.TabStableID
	SourcePaneID     domain.PaneStableID
	Destination      moveSessionLocator
	DestinationTabID domain.TabStableID
}

type moveTabRequest struct {
	Attachment      *attachedClient
	AttachmentToken attachmentConnectionToken
	Source          moveSessionLocator
	SourceTabID     domain.TabStableID
	Destination     moveSessionLocator
}

// movePane resolves, reserves, validates, and commits one pane relocation. All
// fallible work happens before the membership publication section. Once the
// section starts, the caller holds the ordered architecture locks and only
// non-failing in-memory writes are permitted.
func (d *Daemon) movePane(req movePaneRequest) (result error) {
	defer func() { result = normalizeMoveRejection(result) }()
	if d == nil || req.Source.ID == "" || req.Destination.ID == "" ||
		req.SourceTabID == "" || req.SourcePaneID == "" || req.DestinationTabID == "" {
		return errMovePaneInvalid
	}

	d.mu.Lock()
	source := moveSessionForLocatorLocked(d, req.Source)
	destination := moveSessionForLocatorLocked(d, req.Destination)
	d.mu.Unlock()
	if source == nil || destination == nil {
		return errMoveStaleTarget
	}

	reservation, err := d.reserveMoveLifecycles(source, destination)
	if err != nil {
		return errMoveStaleTarget
	}
	reservationHeld := true
	defer func() {
		if reservationHeld {
			reservation.Release()
		}
	}()
	if d.afterMoveLifecycleReserved != nil {
		d.afterMoveLifecycleReserved()
	}

	unlockDispatch := lockMoveDispatch(source, destination)
	dispatchHeld := true
	defer func() {
		if dispatchHeld {
			unlockDispatch()
		}
	}()

	// Snapshot the exact live objects and generations before waiting on resize
	// fences. The locked commit repeats every authority check before publication.
	admission, err := d.snapshotMovePaneAdmission(req, source, destination)
	if err != nil {
		return err
	}
	sourceTab := admission.sourceTab
	destinationTab := admission.destinationTab
	movedPane := admission.movedPane
	sourceTabInitialGeneration := admission.sourceGeneration
	destinationTabInitialGeneration := admission.destinationGeneration
	finalSourceTab := admission.finalSourceTab
	if d.afterMovePaneSourceSnapshot != nil {
		d.afterMovePaneSourceSnapshot()
	}

	// A final source tab is retired without transferring any attachment to the
	// destination. Freeze every affected source connection before publication so
	// teardown invalidates exact generations rather than racing input or render.
	var frozen frozenAttachmentEffectGates
	var sourceEffectsFrozen bool
	if finalSourceTab && source != destination && len(admission.sourceAttachments) != 0 {
		interrupts := make([]attachmentTransportInterrupt, 0, len(admission.sourceAttachments))
		for _, ac := range admission.sourceAttachments {
			if transport := admission.sourceTransports[ac]; transport.transport != nil {
				interrupts = append(interrupts, attachmentTransportInterrupt{ac: ac, transport: transport})
			}
		}
		frozen = freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{interrupts: interrupts, nonblocking: true, afterFrozen: func(ac *attachedClient) {
			if d.afterAttachmentEffectGateFrozen != nil {
				d.afterAttachmentEffectGateFrozen("move-pane", ac)
			}
		}}, admission.sourceAttachments...)
		if !frozen.acquired || !frozen.drained {
			frozen.unfreeze()
			return errMovePaneInvalid
		}
		sourceEffectsFrozen = true
	}
	if sourceEffectsFrozen {
		defer func() {
			if sourceEffectsFrozen {
				frozen.unfreeze()
			}
		}()
	}

	commit := movePaneCommit{
		req:                   req,
		source:                source,
		destination:           destination,
		sourceTab:             sourceTab,
		destinationTab:        destinationTab,
		movedPane:             movedPane,
		sourceAttachments:     admission.sourceAttachments,
		sourceTransports:      admission.sourceTransports,
		sourceGeneration:      sourceTabInitialGeneration,
		destinationGeneration: destinationTabInitialGeneration,
		frozenEffects:         frozen,
	}
	fences := newMovePaneResizeFences(source, destination, sourceTab, destinationTab, movedPane)
	if !fences.acquire(func() bool { return commit.publishLocked(d) }) {
		if commit.err != nil {
			return commit.err
		}
		return errMoveStaleTarget
	}
	// Owner/layout fences end at the atomic publication boundary. The immutable
	// postcommit plan owns every later cleanup and effect.
	fences.Release()
	postcommit := movePanePostcommitPlan{
		source:                   source,
		destination:              destination,
		sourceTab:                sourceTab,
		destinationTab:           destinationTab,
		movedPane:                movedPane,
		sourceAttachments:        admission.sourceAttachments,
		syncCleanup:              commit.syncCleanup,
		frozenEffects:            frozen,
		effectsFrozen:            sourceEffectsFrozen,
		unlockDispatch:           unlockDispatch,
		reservation:              reservation,
		sourceTabRemoved:         commit.sourceTabRemoved,
		sourceEmpty:              commit.sourceEmpty,
		retiredParked:            commit.retiredParked,
		retiredAttachments:       commit.retiredAttachments,
		sourceMetadata:           commit.sourceMetadata,
		sourceMetadataValid:      commit.sourceMetadataValid,
		destinationMetadata:      commit.destinationMetadata,
		destinationMetadataValid: commit.destinationMetadataValid,
	}
	// Transfer cleanup ownership away from the admission defers only after the
	// complete postcommit plan exists.
	sourceEffectsFrozen = false
	dispatchHeld = false
	reservationHeld = false
	postcommit.execute(d)
	return nil
}

// moveSessionForLocatorLocked resolves only live sessions. The name is
// intentionally ignored after the stable ID/incarnation pair is checked.
func moveSessionForLocatorLocked(d *Daemon, locator moveSessionLocator) *session {
	if d == nil {
		return nil
	}
	sess, ok := localSession(d.sessions[locator.ID])
	if !ok || sess.incarnation != locator.Incarnation {
		return nil
	}
	return sess
}

func moveSessionLocatorCurrentLocked(sess *session, locator moveSessionLocator) bool {
	return sess != nil && sess.id == locator.ID && sess.incarnation == locator.Incarnation
}

func findMoveTabLocked(sess *session, stableID domain.TabStableID) *tab {
	if sess == nil {
		return nil
	}
	for _, tb := range sess.tabs {
		if tb != nil && tb.stableID == string(stableID) {
			return tb
		}
	}
	return nil
}

func indexMoveTabLocked(sess *session, target *tab) int {
	for i, tb := range sess.tabs {
		if tb == target {
			return i
		}
	}
	return -1
}

func moveTabMemberLocked(sess *session, target *tab) bool {
	return indexMoveTabLocked(sess, target) >= 0
}

func lockMoveTabs(a, b *tab) func() {
	if a == nil && b == nil {
		return func() {}
	}
	if a == nil {
		b.mu.Lock()
		return b.mu.Unlock
	}
	if b == nil {
		a.mu.Lock()
		return a.mu.Unlock
	}
	if a == b {
		a.mu.Lock()
		return a.mu.Unlock
	}
	first, second := a, b
	if first.stableID > second.stableID ||
		(first.stableID == second.stableID && reflect.ValueOf(first).Pointer() > reflect.ValueOf(second).Pointer()) {
		first, second = second, first
	}
	first.mu.Lock()
	second.mu.Lock()
	return func() {
		second.mu.Unlock()
		first.mu.Unlock()
	}
}

func lockMoveDispatch(a, b *session) func() {
	if a == b {
		a.dispatchMu.Lock()
		return a.dispatchMu.Unlock
	}
	first, second := a, b
	if first.id > second.id {
		first, second = second, first
	}
	first.dispatchMu.Lock()
	second.dispatchMu.Lock()
	return func() {
		second.dispatchMu.Unlock()
		first.dispatchMu.Unlock()
	}
}
