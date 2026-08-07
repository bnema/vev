package daemon

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const CommandRequestTimeout = 10 * time.Second

var (
	ErrCommandRequestTimeout     = errors.New("command request timed out")
	ErrCommandRequestUnavailable = errors.New("command request connection is unavailable")
)

// CommandRequestOutcome is the result delivered for one tracked request.
type CommandRequestOutcome struct {
	Result ports.CommandResult
	Err    error
}

// CommandRequestTracker correlates request results for one connection. The
// generation is part of every pending entry so a reused request ID from an
// older connection cannot complete a newer request.
type CommandRequestTracker struct {
	mu      sync.Mutex
	next    uint64
	pending map[uint64]commandRequestPending
}

type commandRequestPending struct {
	generation uint64
	outcome    chan CommandRequestOutcome
}

// NewCommandRequestTracker constructs an empty per-connection tracker.
func NewCommandRequestTracker() *CommandRequestTracker {
	return &CommandRequestTracker{}
}

// Publish allocates and records a request before its bytes are sent.
func (t *CommandRequestTracker) Publish(generation uint64) (uint64, <-chan CommandRequestOutcome) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending == nil {
		t.pending = make(map[uint64]commandRequestPending)
	}
	t.next++
	if t.next == 0 {
		t.next++
	}
	outcome := t.trackLocked(t.next, generation)
	return t.next, outcome
}

// Track records an existing wire request ID, as used by an inbound one-shot
// command connection. It rejects duplicate IDs on that connection.
func (t *CommandRequestTracker) Track(requestID, generation uint64) (<-chan CommandRequestOutcome, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending == nil {
		t.pending = make(map[uint64]commandRequestPending)
	}
	if _, exists := t.pending[requestID]; exists {
		return nil, false
	}
	if requestID > t.next {
		t.next = requestID
	}
	outcome := t.trackLocked(requestID, generation)
	return outcome, true
}

func (t *CommandRequestTracker) trackLocked(requestID, generation uint64) <-chan CommandRequestOutcome {
	outcome := make(chan CommandRequestOutcome, 1)
	t.pending[requestID] = commandRequestPending{generation: generation, outcome: outcome}
	return outcome
}

// Wait waits without holding the tracker or connection sender lock. Timeout
// and cancellation remove only the matching request.
func (t *CommandRequestTracker) Wait(ctx context.Context, clock ports.Clock, requestID, generation uint64, outcome <-chan CommandRequestOutcome) (ports.CommandResult, error) {
	if t == nil || clock == nil {
		return ports.CommandResult{}, ErrCommandRequestUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		t.Remove(requestID, generation)
		return ports.CommandResult{}, err
	}
	timer := clock.NewTimer(CommandRequestTimeout)
	if timer == nil {
		t.Remove(requestID, generation)
		return ports.CommandResult{}, ErrCommandRequestTimeout
	}
	defer timer.Stop()
	select {
	case completed := <-outcome:
		return completed.Result, completed.Err
	case <-ctx.Done():
		t.Remove(requestID, generation)
		return ports.CommandResult{}, ctx.Err()
	case <-timer.C():
		t.Remove(requestID, generation)
		return ports.CommandResult{}, ErrCommandRequestTimeout
	}
}

// Remove abandons only the exact request generation.
func (t *CommandRequestTracker) Remove(requestID, generation uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	pending, ok := t.pending[requestID]
	if ok && pending.generation == generation {
		delete(t.pending, requestID)
	}
	t.mu.Unlock()
}

// Complete accepts only the exact request generation. Unknown, late, and old
// generation results are safe no-ops.
func (t *CommandRequestTracker) Complete(generation uint64, result ports.CommandResult) {
	t.finish(result.RequestID, generation, CommandRequestOutcome{Result: result})
}

// Fail completes the exact request with a transport or decode failure.
func (t *CommandRequestTracker) Fail(requestID, generation uint64, err error) {
	t.finish(requestID, generation, CommandRequestOutcome{Err: err})
}

// FailGeneration completes every pending request for one retired transport
// generation. It releases the tracker lock before publishing outcomes so a
// caller can immediately wait or begin work on a replacement generation.
func (t *CommandRequestTracker) FailGeneration(generation uint64, err error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	pending := make([]commandRequestPending, 0)
	for requestID, request := range t.pending {
		if request.generation != generation {
			continue
		}
		delete(t.pending, requestID)
		pending = append(pending, request)
	}
	t.mu.Unlock()
	for _, request := range pending {
		request.outcome <- CommandRequestOutcome{Err: err}
	}
}

func (t *CommandRequestTracker) finish(requestID, generation uint64, outcome CommandRequestOutcome) {
	if t == nil {
		return
	}
	t.mu.Lock()
	pending, ok := t.pending[requestID]
	if !ok || pending.generation != generation {
		t.mu.Unlock()
		return
	}
	delete(t.pending, requestID)
	t.mu.Unlock()
	pending.outcome <- outcome
}

// PendingCount is intended for deterministic lifecycle tests.
func (t *CommandRequestTracker) PendingCount() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}
