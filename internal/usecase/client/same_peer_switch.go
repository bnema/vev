package client

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// samePeerInputGate holds raw input behind a requested in-band switch. It does
// not drop or reparse bytes: once the daemon commits, queued input follows the
// attachment to the target; a typed pre-commit failure releases it to source.
type samePeerInputGate struct {
	ui        *UI
	retiredUI atomic.Uint64
	mu        sync.Mutex
	paused    bool
	changed   chan struct{}
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

// discardRetiredUI prevents already queued automation from following human
// input across an in-band route switch. Dropping a fence alone is harmless;
// dropping normal input makes its accepted action's delivery ambiguous.
func (g *samePeerInputGate) discardRetiredUI(message protocol.ClientMessage) bool {
	if g == nil || g.retiredUI.Load() == 0 {
		return false
	}
	var actionID uint64
	input := false
	switch message := message.(type) {
	case protocol.Input:
		actionID, input = message.ActionID, true
	case protocol.UIFence:
		actionID = message.ActionID
	}
	if actionID == 0 || actionID > g.retiredUI.Load() {
		return false
	}
	if input && g.ui != nil {
		g.ui.mu.Lock()
		g.ui.finishLocked(actionID, ports.UIActionOutcomeUnknown, ports.UIActionResult{})
		g.ui.mu.Unlock()
	}
	return true
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
