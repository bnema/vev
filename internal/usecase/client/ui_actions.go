package client

import (
	"context"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// bindForeground runs at the existing foreground ownership boundary.
func (u *UI) bindForeground(ctx context.Context, input *terminalInputPump, consumer uint64) uint64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.generation++
	u.input = input
	u.consumer = consumer
	u.foreground = ctx
	u.boundary = ports.UIActionResult{}
	if u.handoff != nil && u.handoff.destinationGeneration == 0 {
		u.handoff.destinationGeneration = u.generation
	} else if u.pending != 0 {
		u.finishLocked(u.pending, ports.UIActionOutcomeUnknown, ports.UIActionResult{})
		u.pending = 0
	}
	u.signalLocked()
	return u.generation
}

func (u *UI) accept(id, generation uint64) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.pending != id || u.generation != generation {
		return false
	}
	if len(u.order) == uiActionHistory {
		delete(u.records, u.order[0])
		delete(u.dispatched, u.order[0])
		u.order = u.order[1:]
	}
	u.order = append(u.order, id)
	u.records[id] = ports.UIActionResult{ActionID: id, Accepted: true, Status: ports.UIActionPending, Context: u.reservedContext}
	u.signalLocked()
	return true
}

func (u *UI) finishLocked(id uint64, status string, boundary ports.UIActionResult) {
	record, ok := u.records[id]
	if !ok {
		return
	}
	record.Status = status
	if status == ports.UIActionProcessed {
		u.dispatched[id] = true
		record.Revision = boundary.Revision
		record.Context = boundary.Context
	}
	u.records[id] = record
	if u.handoff != nil && u.handoff.actionID == id {
		u.handoff = nil
	}
	if u.pending == id {
		u.pending = 0
	}
	u.signalLocked()
}

// published remembers only a committed output boundary, not receipt receive
// time or an unrelated status-only revision.
func (u *UI) published(generation uint64) {
	snapshot, err := u.state.Snapshot()
	if err != nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if generation != u.generation || snapshot.Context.Generation != generation {
		return
	}
	u.boundary = ports.UIActionResult{Revision: snapshot.Revision, Context: snapshot.Context}
	u.signalLocked()
}

func (u *UI) receipt(generation uint64, receipt protocol.UIReceipt) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.handoff != nil && u.handoff.actionID == receipt.ActionID {
		return
	}
	record, ok := u.records[receipt.ActionID]
	if !ok || record.Status != ports.UIActionPending || record.Context.Generation != generation || generation != u.generation || receipt.Validate() != nil {
		return
	}
	if receipt.Outcome == protocol.UIReceiptUnavailable {
		u.finishLocked(receipt.ActionID, ports.UIActionUnavailable, ports.UIActionResult{})
		return
	}
	boundary := u.boundary
	if boundary.Context.OutputEpoch != receipt.Epoch || boundary.Context.OutputState != receipt.State || boundary.Context.ViewPublication != receipt.ViewPublication || boundary.Revision == 0 {
		return
	}
	u.finishLocked(receipt.ActionID, ports.UIActionProcessed, boundary)
}

func (u *UI) Action(ctx context.Context, request ports.UIActionRequest) (ports.UIActionResult, error) {
	timeout, err := uiTimeout(request.Timeout)
	if err != nil {
		return ports.UIActionResult{}, err
	}
	if request.Attachment != u.handle {
		return ports.UIActionResult{}, &ports.UIError{Code: ports.UIErrStaleAttachment}
	}
	snapshot, err := u.state.Snapshot()
	if err != nil {
		return ports.UIActionResult{}, err
	}
	if request.Generation == 0 || snapshot.Context.Generation != request.Generation {
		return ports.UIActionResult{}, &ports.UIError{Code: ports.UIErrStaleAttachment}
	}
	var data []byte
	switch {
	case len(request.Keys) > 0 && request.Text == "":
		data, err = encodeUIKeys(request.Keys, snapshot.ApplicationCursor)
	case len(request.Keys) == 0 && request.Text != "":
		data, err = encodeUIText(request.Text)
	default:
		err = errUIInvalidInput
	}
	if err != nil {
		return ports.UIActionResult{}, &ports.UIError{Code: ports.UIErrInvalidRequest}
	}
	u.mu.Lock()
	if request.Generation != u.generation {
		u.mu.Unlock()
		return ports.UIActionResult{}, &ports.UIError{Code: ports.UIErrStaleAttachment}
	}
	if u.pending != 0 {
		u.mu.Unlock()
		return ports.UIActionResult{}, &ports.UIError{Code: ports.UIErrBusy}
	}
	if u.foreground == nil || u.foreground.Err() != nil || snapshot.Context.Status != ports.UIStatusAttached {
		u.mu.Unlock()
		return ports.UIActionResult{}, &ports.UIError{Code: ports.UIErrUnavailable}
	}
	u.nextAction++
	id := u.nextAction
	u.pending = id
	u.reservedContext = snapshot.Context
	input, consumer, foreground := u.input, u.consumer, u.foreground
	u.mu.Unlock()
	timer := u.clock.NewTimer(timeout)
	defer timer.Stop()
	admissionCtx, cancelAdmission := context.WithCancel(ctx)
	defer cancelAdmission()
	batch := terminalAutomationRequest{ctx: admissionCtx, owner: u, consumer: consumer, record: terminalReadResult{data: data, source: terminalInputAutomation, generation: request.Generation, actionID: id, endBatch: true}, admitted: make(chan bool, 1), dispatched: make(chan bool, 1)}
	reject := func(code string) (ports.UIActionResult, error) {
		u.mu.Lock()
		delete(u.records, id)
		if u.pending == id {
			u.pending = 0
		}
		for i, entry := range u.order {
			if entry == id {
				u.order = append(u.order[:i], u.order[i+1:]...)
				break
			}
		}
		u.signalLocked()
		u.mu.Unlock()
		return ports.UIActionResult{}, &ports.UIError{Code: code}
	}
	select {
	case input.automation <- batch:
	case <-ctx.Done():
		return reject(ports.UIErrTimeout)
	case <-foreground.Done():
		return reject(ports.UIErrUnavailable)
	case <-timer.C():
		return reject(ports.UIErrTimeout)
	}
	// Once the owner receives the request it returns admission without I/O. Learn
	// that decision even if cancellation races it; never claim rollback of input.
	admitted := <-batch.admitted
	if !admitted {
		return reject(ports.UIErrInputBusy)
	}
	go u.trackDispatch(foreground, id, batch.dispatched)
	for {
		u.mu.Lock()
		record, exists := u.records[id]
		changed := u.changed
		u.mu.Unlock()
		if !exists {
			return ports.UIActionResult{}, &ports.UIError{Code: ports.UIErrActionExpired, Accepted: true, ActionID: id}
		}
		if record.Status == ports.UIActionProcessed {
			return record, nil
		}
		if record.Status != ports.UIActionPending {
			return record, &ports.UIError{Code: record.Status, Accepted: true, ActionID: id}
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return record, &ports.UIError{Code: ports.UIErrTimeout, Accepted: true, ActionID: id}
		case <-timer.C():
			return record, &ports.UIError{Code: ports.UIErrTimeout, Accepted: true, ActionID: id}
		}
	}
}

func (u *UI) trackDispatch(ctx context.Context, id uint64, dispatched <-chan bool) {
	timer := u.clock.NewTimer(30 * time.Second)
	defer timer.Stop()
	foregroundDone := ctx.Done()
	for {
		u.mu.Lock()
		record, exists := u.records[id]
		changed := u.changed
		u.mu.Unlock()
		if !exists || record.Status != ports.UIActionPending {
			return
		}
		status := ""
		select {
		case ok := <-dispatched:
			if !ok {
				status = ports.UIActionOutcomeUnknown
			} else {
				u.mu.Lock()
				if _, exists := u.records[id]; exists {
					u.dispatched[id] = true
					u.completeHandoffLocked()
				}
				u.mu.Unlock()
			}
			dispatched = nil
		case <-foregroundDone:
			u.mu.Lock()
			following := u.handoff != nil && u.handoff.actionID == id
			u.mu.Unlock()
			if following {
				foregroundDone = nil
			} else {
				status = ports.UIActionOutcomeUnknown
			}
		case <-timer.C():
			status = ports.UIActionUnavailable
		case <-changed:
		}
		if status != "" {
			u.mu.Lock()
			if current := u.records[id]; current.Status == ports.UIActionPending {
				u.finishLocked(id, status, ports.UIActionResult{})
			}
			u.mu.Unlock()
			return
		}
	}
}

var _ ports.UIService = (*UI)(nil)
