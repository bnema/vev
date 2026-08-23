package client

import (
	"context"
	"testing"
)

func TestSamePeerInputGateBlocksUntilReleased(t *testing.T) {
	gate := newSamePeerInputGate()
	gate.setPaused(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan bool, 1)
	go func() { ready <- gate.wait(ctx) }()
	gate.setPaused(false)
	if ok := <-ready; !ok {
		t.Fatal("input gate did not release")
	}
}

func TestSamePeerInputGateCancels(t *testing.T) {
	gate := newSamePeerInputGate()
	gate.setPaused(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if gate.wait(ctx) {
		t.Fatal("cancelled input gate wait succeeded")
	}
}
