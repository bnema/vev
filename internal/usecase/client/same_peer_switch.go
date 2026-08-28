package client

import (
	"context"
	"sync"

	"github.com/bnema/vev/internal/protocol"
)

// samePeerInputGate holds raw input behind a requested in-band switch. It does
// not drop or reparse bytes: once the daemon commits, queued input follows the
// attachment to the target; a typed pre-commit failure releases it to source.
type samePeerInputGate struct {
	mu      sync.Mutex
	paused  bool
	changed chan struct{}
	// afterInputHeld is a deterministic test synchronization seam.
	afterInputHeld func()
}

type samePeerSwitchPending struct {
	requestID uint64
	target    protocol.ExactSessionTarget
}

func newSamePeerInputGate() *samePeerInputGate {
	return &samePeerInputGate{changed: make(chan struct{})}
}

func (g *samePeerInputGate) setPaused(paused bool) {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.paused != paused {
		g.paused = paused
		close(g.changed)
		g.changed = make(chan struct{})
	}
	g.mu.Unlock()
}

func (g *samePeerInputGate) snapshot() (bool, <-chan struct{}) {
	if g == nil {
		return false, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.paused, g.changed
}

func (g *samePeerInputGate) wait(ctx context.Context) bool {
	for {
		paused, changed := g.snapshot()
		if !paused {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
	}
}
