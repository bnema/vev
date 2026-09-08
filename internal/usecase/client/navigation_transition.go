package client

// Home-picker overlays retain their separate navigation lifecycle.
type navigationOperation uint8

const (
	navigationOperationNone navigationOperation = iota
	navigationOperationCreation
	navigationOperationRecent
	navigationOperationInventory
)

// navigationTransition retains recovery until destination publication. Target
// transport authority belongs to attachHandoff.source; history belongs to the
// route ledger. Neither is duplicated here.
type navigationTransition struct {
	operation   navigationOperation
	returnRoute attachRoute
	requestID   uint64
}

func (t *navigationTransition) active() bool {
	return t != nil && t.operation != navigationOperationNone
}

func (t *navigationTransition) pendingCreation() bool {
	return t != nil && t.operation == navigationOperationCreation
}

func (t *navigationTransition) pendingRecent() bool {
	return t != nil && t.operation == navigationOperationRecent
}

func (t *navigationTransition) pendingInventory() bool {
	return t != nil && t.operation == navigationOperationInventory
}

func (t *navigationTransition) beginCreation(returnRoute attachRoute, requestID uint64) {
	*t = navigationTransition{operation: navigationOperationCreation, returnRoute: returnRoute, requestID: requestID}
}

func (t *navigationTransition) beginRecent(returnRoute attachRoute) {
	*t = navigationTransition{operation: navigationOperationRecent, returnRoute: returnRoute}
}

func (t *navigationTransition) beginInventory(returnRoute attachRoute) {
	*t = navigationTransition{operation: navigationOperationInventory, returnRoute: returnRoute}
}

func (t *navigationTransition) restore() (attachRoute, bool) {
	if !t.active() {
		return attachRoute{}, false
	}
	return t.returnRoute, true
}

// Both outcomes retire the same pending recovery record. The route ledger
// independently commits successful destinations.
func (t *navigationTransition) settleSuccess() { t.clear() }
func (t *navigationTransition) settleFailure() { t.clear() }

func (t *navigationTransition) clear() {
	if t != nil {
		*t = navigationTransition{}
	}
}
