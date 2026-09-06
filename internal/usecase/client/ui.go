package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const uiActionHistory = 64

// UI owns attachment-local action and waiter bookkeeping. It never holds its
// mutex across terminal access, transport I/O, or predicate evaluation.
type UI struct {
	state           ports.UIState
	clock           ports.Clock
	handle          string
	mu              sync.Mutex
	changed         chan struct{}
	generation      uint64
	input           *terminalInputPump
	consumer        uint64
	foreground      context.Context
	records         map[uint64]ports.UIActionResult
	order           []uint64
	nextAction      uint64
	pending         uint64
	reservedContext ports.UIContext
	waits           int
	boundary        ports.UIActionResult
	dispatched      map[uint64]bool
	completion      map[uint64]chan struct{}
	handoff         *uiActionHandoff
}

type uiActionHandoff struct {
	actionID              uint64
	sourceGeneration      uint64
	destinationGeneration uint64
	boundary              ports.UIActionResult
}

func NewUI(state ports.UIState, clock ports.Clock) *UI {
	if clock == nil {
		clock = systemClock{}
	}
	return &UI{state: state, clock: clock, handle: fmt.Sprintf("%x", newClientID()), changed: make(chan struct{}), records: make(map[uint64]ports.UIActionResult), dispatched: make(map[uint64]bool), completion: make(map[uint64]chan struct{})}
}

func (u *UI) Handle() string { return u.handle }

// WaitForSnapshot waits on the UI owner's broadcast signal rather than
// consuming the terminal's single coalesced state channel. The predicate is
// evaluated against the latest owned snapshot and never under the UI mutex.
func (u *UI) WaitForSnapshot(ctx context.Context, match func(ports.UISnapshot) bool) (ports.UISnapshot, error) {
	if match == nil {
		return ports.UISnapshot{}, ports.ErrUIUnavailable
	}
	var lastSnapshotErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastSnapshotErr != nil {
				return ports.UISnapshot{}, errors.Join(err, lastSnapshotErr)
			}
			return ports.UISnapshot{}, err
		}
		snapshot, err := u.state.Snapshot()
		if err == nil {
			lastSnapshotErr = nil
			if match(snapshot) {
				return snapshot, nil
			}
		} else {
			lastSnapshotErr = err
		}
		u.mu.Lock()
		changed := u.changed
		u.mu.Unlock()
		select {
		case <-ctx.Done():
			if lastSnapshotErr != nil {
				return ports.UISnapshot{}, errors.Join(ctx.Err(), lastSnapshotErr)
			}
			return ports.UISnapshot{}, ctx.Err()
		case <-changed:
		}
	}
}

// ActionComplete returns a closed channel once an accepted action reaches a
// terminal status. It is an optional lifecycle seam for adapters enforcing
// attachment-wide action admission after a request timeout.
func (u *UI) ActionComplete(actionID uint64) <-chan struct{} {
	if actionID == 0 {
		return nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if done, ok := u.completion[actionID]; ok {
		return done
	}
	if record, ok := u.records[actionID]; ok && record.Status != ports.UIActionPending {
		done := make(chan struct{})
		close(done)
		return done
	}
	return nil
}

func (u *UI) status(status ports.UIPresentationStatus) {
	publication, ok := u.state.(ports.UIOutputTransaction)
	if !ok {
		return
	}
	snapshot, err := u.state.Snapshot()
	if err != nil {
		return
	}
	u.mu.Lock()
	generation := u.generation
	u.mu.Unlock()
	snapshot.Context.AttachmentHandle = u.handle
	snapshot.Context.Generation = generation
	snapshot.Context.Status = status
	_ = publication.PublishContext(snapshot.Context) // Unavailable capture remains unavailable.
}

// Observe relays the sink's single coalesced signal to bounded concurrent
// waiters. The Runner owns this worker's lifetime.
func (u *UI) Observe(ctx context.Context) {
	defer func() {
		u.mu.Lock()
		if u.pending != 0 {
			u.finishLocked(u.pending, ports.UIActionOutcomeUnknown, ports.UIActionResult{})
		}
		u.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-u.state.Changes():
			u.mu.Lock()
			u.signalLocked()
			u.mu.Unlock()
		}
	}
}

func (u *UI) signalLocked() { close(u.changed); u.changed = make(chan struct{}) }

func (u *UI) Capture(attachment string) (ports.UISnapshot, error) {
	if attachment != u.handle {
		return ports.UISnapshot{}, &ports.UIError{Code: ports.UIErrStaleAttachment}
	}
	return u.state.Snapshot()
}

func uiTimeout(timeout time.Duration) (time.Duration, error) {
	if timeout == 0 {
		return 5 * time.Second, nil
	}
	if timeout < time.Millisecond || timeout > 30*time.Second {
		return 0, &ports.UIError{Code: ports.UIErrInvalidRequest}
	}
	return timeout, nil
}

func validateUIExpect(expect ports.UIExpect) bool {
	if expect.TextContains == nil && expect.Session == nil && expect.Focus == nil && expect.Status == nil {
		return false
	}
	if expect.TextContains != nil && (len(*expect.TextContains) > 4096 || !utf8.ValidString(*expect.TextContains)) {
		return false
	}
	if expect.Session != nil && expect.Session.Validate() != nil {
		return false
	}
	if expect.Focus != nil && (domain.ValidateTabStableID(expect.Focus.TabID) != nil || domain.ValidatePaneStableID(expect.Focus.PaneID) != nil) {
		return false
	}
	if expect.Status != nil {
		switch *expect.Status {
		case ports.UIStatusAttached, ports.UIStatusTransitioning, ports.UIStatusReconnecting, ports.UIStatusDetached:
		default:
			return false
		}
	}
	return true
}

func uiMatches(snapshot ports.UISnapshot, expect ports.UIExpect) bool {
	if expect.Session != nil && snapshot.Context.Route.Target != *expect.Session {
		return false
	}
	if expect.Focus != nil && (snapshot.Context.TabID != expect.Focus.TabID || snapshot.Context.FocusedPaneID != expect.Focus.PaneID) {
		return false
	}
	if expect.Status != nil && snapshot.Context.Status != *expect.Status {
		return false
	}
	if expect.TextContains != nil && !strings.Contains(uiSnapshotText(snapshot), *expect.TextContains) {
		return false
	}
	return true
}

func uiSnapshotText(snapshot ports.UISnapshot) string {
	var text strings.Builder
	for row := 0; row < snapshot.Rows; row++ {
		if row > 0 {
			text.WriteByte('\n')
		}
		for col := 0; col < snapshot.Columns; col++ {
			index := row*snapshot.Columns + col
			if index >= len(snapshot.Cells) {
				text.WriteByte(' ')
				continue
			}
			cell := snapshot.Cells[index]
			if cell.Continuation {
				continue
			}
			if cell.Text == "" {
				text.WriteByte(' ')
			} else {
				text.WriteString(cell.Text)
			}
		}
	}
	return text.String()
}

func (u *UI) Wait(ctx context.Context, request ports.UIWaitRequest) (ports.UIWaitResult, error) {
	timeout, err := uiTimeout(request.Timeout)
	if err != nil || !validateUIExpect(request.Expect) {
		return ports.UIWaitResult{}, &ports.UIError{Code: ports.UIErrInvalidRequest}
	}
	if request.Attachment != u.handle {
		return ports.UIWaitResult{}, &ports.UIError{Code: ports.UIErrStaleAttachment}
	}
	u.mu.Lock()
	if u.waits >= 4 {
		u.mu.Unlock()
		return ports.UIWaitResult{}, &ports.UIError{Code: ports.UIErrBusy}
	}
	u.waits++
	u.mu.Unlock()
	defer func() { u.mu.Lock(); u.waits--; u.mu.Unlock() }()
	timer := u.clock.NewTimer(timeout)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return ports.UIWaitResult{}, err
		}
		u.mu.Lock()
		changed := u.changed
		action, exists := u.records[request.AfterAction]
		generation := u.generation
		u.mu.Unlock()
		if request.AfterAction != 0 {
			if !exists {
				return ports.UIWaitResult{}, &ports.UIError{Code: ports.UIErrActionExpired, ActionID: request.AfterAction}
			}
			if action.Status != ports.UIActionPending && action.Status != ports.UIActionProcessed {
				return ports.UIWaitResult{}, &ports.UIError{Code: ports.UIErrorCode(action.Status), Accepted: action.Accepted, ActionID: action.ActionID}
			}
			if action.Status == ports.UIActionProcessed && action.Context.Generation != generation {
				return ports.UIWaitResult{}, &ports.UIError{Code: ports.UIErrOutcomeUnknown, Accepted: true, ActionID: action.ActionID}
			}
		}
		snapshot, snapshotErr := u.Capture(request.Attachment)
		eligible := request.AfterAction == 0 || action.Status == ports.UIActionProcessed && snapshot.Revision >= action.Revision && snapshot.Context.Generation == action.Context.Generation
		if snapshotErr == nil && eligible && uiMatches(snapshot, request.Expect) {
			if request.AfterAction != 0 {
				u.mu.Lock()
				current := u.records[request.AfterAction]
				unchanged := current == action && u.generation == generation
				u.mu.Unlock()
				if !unchanged {
					continue
				}
			}
			return ports.UIWaitResult{ActionID: request.AfterAction, ActionStatus: action.Status, Revision: snapshot.Revision, Context: snapshot.Context}, nil
		}
		select {
		case <-ctx.Done():
			return ports.UIWaitResult{}, ctx.Err()
		case <-timer.C():
			return ports.UIWaitResult{}, &ports.UIError{Code: ports.UIErrTimeout, Accepted: action.Accepted, ActionID: request.AfterAction}
		case <-changed:
		}
	}
}
