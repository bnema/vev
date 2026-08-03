package daemon

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const attachedCommandTimeout = 10 * time.Second

var (
	errAttachedCommandTimeout     = errors.New("attached command timed out")
	errAttachedCommandUnavailable = errors.New("attached command connection is unavailable")
)

type attachedCommandOutcome struct {
	result ports.CommandResult
	err    error
}

type attachedCommandPending struct {
	generation uint64
	outcome    chan attachedCommandOutcome
}

// attachedCommandTracker owns command correlation for one attachment
// connection. Its lock protects only request publication state; command waits
// happen after the attachment sender lock is released.
type attachedCommandTracker struct {
	commandMu sync.Mutex
	mu        sync.Mutex
	next      uint64
	pending   map[uint64]attachedCommandPending
}

// sendCommand publishes one attached command and waits for its exact result.
// commandMu keeps interactive palette commands ordered for one attachment;
// other attachment traffic uses only sendMu during request publication.
func (ac *attachedClient) sendCommand(ctx context.Context, clock ports.Clock, slug string, args []string) (ports.CommandResult, error) {
	if ac == nil || clock == nil {
		return ports.CommandResult{}, errAttachedCommandUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ports.CommandResult{}, err
	}

	ac.commands.commandMu.Lock()
	defer ac.commands.commandMu.Unlock()
	if err := ctx.Err(); err != nil {
		return ports.CommandResult{}, err
	}

	ac.sendMu.Lock()
	expected := ac.transportSnapshot()
	generation := ac.connectionGeneration.Load()
	if expected.transport == nil {
		ac.sendMu.Unlock()
		return ports.CommandResult{}, errAttachedCommandUnavailable
	}
	ac.commands.mu.Lock()
	if ac.commands.pending == nil {
		ac.commands.pending = make(map[uint64]attachedCommandPending)
	}
	ac.commands.next++
	if ac.commands.next == 0 {
		ac.commands.next++
	}
	requestID := ac.commands.next
	outcome := make(chan attachedCommandOutcome, 1)
	ac.commands.pending[requestID] = attachedCommandPending{generation: generation, outcome: outcome}
	ac.commands.mu.Unlock()
	if !ac.transportSnapshotCurrent(expected) || ac.connectionGeneration.Load() != generation {
		ac.removeAttachedCommand(requestID, generation)
		ac.sendMu.Unlock()
		return ports.CommandResult{}, errAttachedCommandUnavailable
	}

	payload, err := ports.MarshalCommandRequest(ports.CommandRequest{
		Version: ports.ProtocolVersion, RequestID: requestID, Attached: true,
		Slug: slug, Args: append([]string(nil), args...),
	})
	if err != nil {
		ac.removeAttachedCommand(requestID, generation)
		ac.sendMu.Unlock()
		return ports.CommandResult{}, err
	}
	if err := expected.transport.Send(ports.Frame{Type: ports.MsgCommand, Payload: payload}); err != nil {
		ac.removeAttachedCommand(requestID, generation)
		ac.sendMu.Unlock()
		return ports.CommandResult{}, err
	}
	ac.sendMu.Unlock()

	timer := clock.NewTimer(attachedCommandTimeout)
	if timer == nil {
		ac.removeAttachedCommand(requestID, generation)
		return ports.CommandResult{}, errAttachedCommandTimeout
	}
	defer timer.Stop()
	select {
	case completed := <-outcome:
		return completed.result, completed.err
	case <-ctx.Done():
		ac.removeAttachedCommand(requestID, generation)
		return ports.CommandResult{}, ctx.Err()
	case <-timer.C():
		ac.removeAttachedCommand(requestID, generation)
		return ports.CommandResult{}, errAttachedCommandTimeout
	}
}

func (ac *attachedClient) removeAttachedCommand(requestID, generation uint64) {
	if ac == nil {
		return
	}
	ac.commands.mu.Lock()
	pending, ok := ac.commands.pending[requestID]
	if ok && pending.generation == generation {
		delete(ac.commands.pending, requestID)
	}
	ac.commands.mu.Unlock()
}

// completeCommandResult accepts only a result for the exact request and
// connection generation. Unknown, late, and old-generation results are safe
// no-ops.
func (ac *attachedClient) completeCommandResult(generation uint64, result ports.CommandResult) {
	if ac == nil || result.RequestID == 0 || ac.connectionGeneration.Load() != generation {
		return
	}
	ac.commands.mu.Lock()
	pending, ok := ac.commands.pending[result.RequestID]
	if !ok || pending.generation != generation {
		ac.commands.mu.Unlock()
		return
	}
	delete(ac.commands.pending, result.RequestID)
	ac.commands.mu.Unlock()
	pending.outcome <- attachedCommandOutcome{result: result}
}

func (ac *attachedClient) pendingAttachedCommandCount() int {
	if ac == nil {
		return 0
	}
	ac.commands.mu.Lock()
	defer ac.commands.mu.Unlock()
	return len(ac.commands.pending)
}
