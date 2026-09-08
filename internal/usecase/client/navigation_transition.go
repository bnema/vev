package client

// navigationOperation identifies the kind of pending cross-route navigation.
// Creation and recent-route callers share this representation; inventory
// selections reuse it once the relay lands. Home-picker overlay navigation
// stays distinct where its overlay semantics require it.
type navigationOperation uint8

const (
	navigationOperationNone navigationOperation = iota
	navigationOperationCreation
	navigationOperationRecent
	navigationOperationInventory
)

// navigationStage tracks where a pending transition stands. Recovery
// information is preserved until the validated destination
// publication/commit milestone, not merely after resolve or first Welcome.
type navigationStage uint8

const (
	navigationStagePrepare navigationStage = iota + 1
	navigationStageDestinationPending
	navigationStageRestoring
	navigationStageSettled
)

// navigationTransition is the single private pending-navigation value for
// creation, recent-route, and inventory operations. It keeps two distinct
// routes captured before activation:
//
//   - targetAuthority: the proven source route, used for endpoint-empty
//     target resolution and target environment policy. It never overwrites
//     the recovery destination.
//   - returnRoute: the currently committed active route, including exact
//     identity, concrete dialer/request, and resume information. It is the
//     recovery destination.
//
// The transition settles once; history and visible route commit only at the
// existing validated publication milestone. Restoration settles as failed
// navigation even if the source displays successfully. There is no fallback
// to a same-name replacement or an unrelated session.
type navigationTransition struct {
	operation        navigationOperation
	targetAuthority  attachRoute
	returnRoute      attachRoute
	requestID        uint64
	actionKey        uint64
	actionGeneration uint64
	stage            navigationStage
	settled          bool
	historyCommitted bool
}

func (t *navigationTransition) active() bool {
	return t != nil && t.operation != navigationOperationNone && !t.settled && t.stage != navigationStageSettled
}

// pendingCreation reports an unsettled creation handoff awaiting its
// destination outcome. It replaces the former creation pending boolean.
func (t *navigationTransition) pendingCreation() bool {
	return t != nil && t.operation == navigationOperationCreation && t.active()
}

// pendingRecent reports an unsettled recent-route handoff awaiting its
// destination outcome. It replaces the former recent pending boolean.
func (t *navigationTransition) pendingRecent() bool {
	return t != nil && t.operation == navigationOperationRecent && t.active()
}

// beginCreation captures the prior committed route before a creation handoff.
// The authority route proves the source; the return route is the recovery
// destination and is never overwritten by the authority.
func (t *navigationTransition) beginCreation(authority, returnRoute attachRoute, requestID uint64) {
	*t = navigationTransition{
		operation:       navigationOperationCreation,
		targetAuthority: authority,
		returnRoute:     returnRoute,
		requestID:       requestID,
		stage:           navigationStageDestinationPending,
	}
}

// beginRecent captures the prior committed route before a recent-route
// navigation handoff.
func (t *navigationTransition) beginRecent(authority, returnRoute attachRoute, key, generation uint64) {
	*t = navigationTransition{
		operation:        navigationOperationRecent,
		targetAuthority:  authority,
		returnRoute:      returnRoute,
		actionKey:        key,
		actionGeneration: generation,
		stage:            navigationStageDestinationPending,
	}
}

// beginInventory captures the proven local source route as authority and the
// currently committed remote route as the recovery destination.
func (t *navigationTransition) beginInventory(authority, returnRoute attachRoute, causeActionID uint64) {
	*t = navigationTransition{
		operation:       navigationOperationInventory,
		targetAuthority: authority,
		returnRoute:     returnRoute,
		requestID:       causeActionID,
		stage:           navigationStagePrepare,
	}
}

// admitDestination moves a prepared inventory transition to destination
// pending after source resolve. Creation and recent transitions start
// pending; calling admit on them is a no-op.
func (t *navigationTransition) admitDestination() {
	if t == nil || t.settled {
		return
	}
	if t.operation == navigationOperationInventory && t.stage == navigationStagePrepare {
		t.stage = navigationStageDestinationPending
	}
}

// restore returns the exact prior committed route for recovery. The first
// call moves the transition to restoring; later calls repeat the same route
// without changing state. Restoring settles as failed navigation even if the
// source displays successfully.
func (t *navigationTransition) restore() (attachRoute, bool) {
	if t == nil || !t.active() {
		return attachRoute{}, false
	}
	if t.stage == navigationStageDestinationPending || t.stage == navigationStagePrepare {
		t.stage = navigationStageRestoring
	}
	return t.returnRoute, true
}

// settleSuccess settles the transition after destination publication commits.
// It reports whether history may be recorded: exactly once per transition.
func (t *navigationTransition) settleSuccess() bool {
	if t == nil || t.settled {
		return false
	}
	t.settled = true
	t.stage = navigationStageSettled
	if t.historyCommitted {
		return false
	}
	t.historyCommitted = true
	return true
}

// settleFailure settles the transition after failed navigation, including
// failed source restoration. It reports false when already settled so
// callers terminate with existing unavailable/navigation-failed semantics
// instead of retrying indefinitely.
func (t *navigationTransition) settleFailure() bool {
	if t == nil || t.settled {
		return false
	}
	t.settled = true
	t.stage = navigationStageSettled
	return true
}

// clear abandons the transition without settling, for paths where another
// owner (session kill, explicit detach) takes over navigation.
func (t *navigationTransition) clear() {
	if t == nil {
		return
	}
	*t = navigationTransition{}
}
