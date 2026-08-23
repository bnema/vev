package client

import (
	"context"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// samePeerInputGate holds raw input behind a requested in-band switch. It does
// not drop or reparse bytes: once the daemon commits, queued input follows the
// attachment to the target; a typed pre-commit failure releases it to source.
type samePeerInputGate struct {
	mu      sync.Mutex
	paused  bool
	changed chan struct{}
}

type samePeerSwitchPending struct {
	requestID uint64
	target    ports.ExactSessionTarget
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

func (g *samePeerInputGate) wait(ctx context.Context) bool {
	if g == nil {
		return true
	}
	for {
		g.mu.Lock()
		paused, changed := g.paused, g.changed
		g.mu.Unlock()
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
