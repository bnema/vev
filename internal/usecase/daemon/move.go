package daemon

import (
	"reflect"

	"github.com/bnema/vev/internal/domain"
)

// movePaneRequest identifies both lifecycles and both topology objects by
// immutable identity. Session names are advisory; the ID and incarnation are
// the commit-time authority.
type movePaneRequest struct {
	Source           moveSessionLocator
	SourceTabID      domain.TabStableID
	SourcePaneID     domain.PaneStableID
	Destination      moveSessionLocator
	DestinationTabID domain.TabStableID
}

type moveTabRequest struct {
	Source      moveSessionLocator
	SourceTabID domain.TabStableID
	Destination moveSessionLocator
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
	sourceClient := admission.sourceClient
	destinationClient := admission.destinationClient
	sourceSnatched := admission.sourceSnatched
	sourceTabWasActive := admission.sourceTabWasActive
	sourceTabInitialGeneration := admission.sourceGeneration
	destinationTabInitialGeneration := admission.destinationGeneration
	finalSourceTab := admission.finalSourceTab
	if d.afterMovePaneSourceSnapshot != nil {
		d.afterMovePaneSourceSnapshot()
	}

	// A final source tab with an active client needs the centralized attachment
	// transition. Freeze that exact role set before entering the fence callback;
	// the callback then validates the handoff before changing any topology.
	var handoffReq attachmentTransitionRequest
	var handoffParticipants attachmentTransitionParticipants
	var frozen frozenRoleEffectGates
	var sourceRolesFrozen bool
	var handoffFrozen bool

	if finalSourceTab && source != destination && sourceClient != nil {
		transport := sourceClient.transportSnapshot()
		sourceToken := source.attachmentToken(sourceClient, transport.transport)
		if sourceToken.role != attachmentActive || sourceToken.transport.transport == nil {
			return errMovePaneInvalid
		}
		destinationTabIndex := moveTabIndex(destination, destinationTab)
		if destinationTabIndex < 0 {
			return errMovePaneInvalid
		}
		handoffReq = attachmentTransitionRequest{
			source: source, target: destination, next: sourceClient,
			expectedRole: attachmentActive, targetRole: attachmentActive,
			expectedTransport: sourceToken.transport, sourceToken: &sourceToken,
			expectedSourceTab: sourceTab, activateTargetTab: true,
			targetTabIndex: destinationTabIndex, ready: true,
		}
		var err error
		handoffReq, handoffParticipants, err = d.snapshotAttachmentTransition(handoffReq)
		if err != nil {
			return err
		}
		// Retiring source snatched clients is part of the same role gate set as
		// the follower. Their exact membership is checked again in the commit.
		d.mu.Lock()
		source.mu.Lock()
		for _, ac := range sourceSnatched {
			handoffParticipants.clients = append(handoffParticipants.clients, ac)
			tr := ac.transportSnapshot()
			if tr.transport != nil {
				handoffParticipants.interrupts = append(handoffParticipants.interrupts, roleTransportInterrupt{ac: ac, transport: tr})
			}
		}
		source.mu.Unlock()
		d.mu.Unlock()
		frozen = freezeRoleEffectGatesWith(roleEffectFreezeOptions{interrupts: handoffParticipants.interrupts, nonblocking: true, afterFrozen: func(ac *attachedClient) {
			if d.afterRoleEffectGateFrozen != nil {
				d.afterRoleEffectGateFrozen("move-pane", ac)
			}
		}}, handoffParticipants.clients...)
		if !frozen.acquired || !frozen.drained {
			frozen.unfreeze()
			return errMovePaneInvalid
		}
		handoffFrozen = true
		sourceRolesFrozen = true
		handoffReq.roleEffectsFrozen = true
	} else if finalSourceTab && source != destination && len(sourceSnatched) > 0 {
		// A headless source may still have persistent snatched clients. Freeze
		// their exact role gates before retiring the source registry.
		d.mu.Lock()
		source.mu.Lock()
		participants := append([]*attachedClient(nil), sourceSnatched...)
		interrupts := make([]roleTransportInterrupt, 0, len(sourceSnatched))
		for _, ac := range sourceSnatched {
			if transport := ac.transportSnapshot(); transport.transport != nil {
				interrupts = append(interrupts, roleTransportInterrupt{ac: ac, transport: transport})
			}
		}
		source.mu.Unlock()
		d.mu.Unlock()
		frozen = freezeRoleEffectGatesWith(roleEffectFreezeOptions{interrupts: interrupts, nonblocking: true, afterFrozen: func(ac *attachedClient) {
			if d.afterRoleEffectGateFrozen != nil {
				d.afterRoleEffectGateFrozen("move-pane", ac)
			}
		}}, participants...)
		if !frozen.acquired || !frozen.drained {
			frozen.unfreeze()
			return errMovePaneInvalid
		}
		sourceRolesFrozen = true
	}
	if sourceRolesFrozen {
		defer func() {
			if sourceRolesFrozen {
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
		sourceClient:          sourceClient,
		sourceSnatched:        sourceSnatched,
		sourceGeneration:      sourceTabInitialGeneration,
		destinationGeneration: destinationTabInitialGeneration,
		handoffFrozen:         handoffFrozen,
		frozenRoles:           frozen,
		handoffReq:            handoffReq,
	}
	fences := newMovePaneResizeFences(source, destination, sourceTab, destinationTab, movedPane)
	if !fences.acquire(func() bool { return commit.publishLocked(d) }) {
		commit.releasePublication()
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
		destinationClient:        destinationClient,
		sourceCleanupToken:       commit.sourceCleanupToken,
		handoffResult:            commit.handoffResult,
		syncCleanup:              commit.syncCleanup,
		frozenRoles:              frozen,
		rolesFrozen:              sourceRolesFrozen,
		unlockDispatch:           unlockDispatch,
		reservation:              reservation,
		sourceTabWasActive:       sourceTabWasActive,
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
	sourceRolesFrozen = false
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
	sess := d.sessions[locator.ID]
	if sess == nil || sess.incarnation != locator.Incarnation {
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

func moveTabIndex(sess *session, target *tab) int {
	if sess == nil || target == nil {
		return -1
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return indexMoveTabLocked(sess, target)
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
