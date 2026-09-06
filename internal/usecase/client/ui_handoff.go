package client

import (
	"context"

	"github.com/bnema/vev/internal/ports"
)

// follow joins an immutable navigation cause to the Runner's existing handoff.
// It selects no route and never retries or opens a connection itself.
func (u *UI) follow(generation, actionID uint64) bool {
	var dispatchContext context.Context
	startDispatch := false
	u.mu.Lock()
	record, exists := u.records[actionID]
	if actionID == 0 || !exists || generation != u.generation || record.Context.Generation != generation || record.Status != ports.UIActionPending && record.Status != ports.UIActionProcessed {
		u.mu.Unlock()
		return false
	}
	if u.pending != 0 && u.pending != actionID {
		u.finishLocked(u.pending, ports.UIActionOutcomeUnknown, ports.UIActionResult{})
	}
	if record.Status == ports.UIActionProcessed {
		dispatchContext = u.foreground
		if dispatchContext == nil {
			dispatchContext = context.Background()
		}
		startDispatch = true
	}
	record.Status = ports.UIActionPending
	u.records[actionID] = record
	u.pending = actionID
	u.handoff = &uiActionHandoff{actionID: actionID, sourceGeneration: generation}
	u.signalLocked()
	u.mu.Unlock()
	if startDispatch {
		go u.trackDispatch(dispatchContext, actionID, nil)
	}
	return true
}

// completeLocal runs after the foreground's FIFO callback barrier, without
// inventing a daemon fence for an action consumed entirely by the client.
func (u *UI) completeLocal(generation, actionID uint64) {
	snapshot, err := u.state.Snapshot()
	if err != nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	record, exists := u.records[actionID]
	if !exists || record.Status != ports.UIActionPending || generation != u.generation || snapshot.Context.Generation != generation || u.handoff != nil {
		return
	}
	u.finishLocked(actionID, ports.UIActionProcessed, ports.UIActionResult{Revision: snapshot.Revision, Context: snapshot.Context})
}

func (u *UI) failHandoff() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.handoff != nil {
		u.finishLocked(u.handoff.actionID, ports.UIActionNavigationFailed, ports.UIActionResult{})
	}
}

// destinationFull is called only after the existing handoff owner validates,
// writes and publishes its destination full Output. Identity alone is not proof.
func (u *UI) destinationFull(generation uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.handoff == nil || u.handoff.destinationGeneration != generation || generation == u.handoff.sourceGeneration || u.boundary.Context.Generation != generation || u.boundary.Context.Status != ports.UIStatusAttached {
		return
	}
	u.handoff.boundary = u.boundary
	u.completeHandoffLocked()
}

func (u *UI) completeHandoffLocked() {
	if u.handoff != nil && u.handoff.boundary.Revision != 0 && u.dispatched[u.handoff.actionID] {
		u.finishLocked(u.handoff.actionID, ports.UIActionProcessed, u.handoff.boundary)
	}
}
