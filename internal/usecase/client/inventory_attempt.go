package client

import (
	"sync"

	"github.com/bnema/vev/internal/protocol"
)

// inventoryPollOutcome is one finished control-source snapshot query. The
// interaction and query identity bind the completion to its originating
// namespace: stale results from a closed interaction drop.
type inventoryPollOutcome struct {
	interaction uint64
	query       uint64
	response    protocol.NavigationInventoryResponse
	err         error
}

// inventoryPollBox holds the latest finished poll outcome while the attempt
// loop is busy. Queries never block on a slow loop: the newest outcome wins
// and older ones drop, matching the single in-flight plus one coalesced
// update budget.
type inventoryPollBox struct {
	mu      sync.Mutex
	outcome inventoryPollOutcome
	has     bool
	wake    chan struct{}
}

func newInventoryPollBox() *inventoryPollBox {
	return &inventoryPollBox{wake: make(chan struct{}, 1)}
}

func (b *inventoryPollBox) offer(outcome inventoryPollOutcome) {
	b.mu.Lock()
	// Keep the newest completion: a late outcome from an older query must
	// not overwrite a newer one, or the newer query's completion is lost
	// while its slot stays busy and polling stalls.
	if b.has && outcome.query < b.outcome.query {
		b.mu.Unlock()
		return
	}
	b.outcome = outcome
	b.has = true
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *inventoryPollBox) take() (inventoryPollOutcome, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	outcome, has := b.outcome, b.has
	b.outcome = inventoryPollOutcome{}
	b.has = false
	return outcome, has
}

// inventoryResolveOutcome is one finished selection resolve: either the
// non-mutating attach target bound to its validated selection or the
// resolve error. Stale outcomes drop on receipt when the interaction moved.
type inventoryResolveOutcome struct {
	selection protocol.NavigationInventorySelection
	target    protocol.AttachTarget
	err       error
}
