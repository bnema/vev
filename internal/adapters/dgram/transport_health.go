package dgram

import (
	"time"

	"github.com/bnema/vev/internal/ports"
)

// healthLoop owns link-health timing so bulk retransmission cannot delay state
// transitions or socket hopping.
func (t *Transport) healthLoop() {
	health := t.clock.NewTimer(t.resendAfter)
	defer health.Stop()
	for {
		select {
		case <-health.C():
			t.checkSilence()
			health.Reset(t.resendAfter)
			t.notifyTimerHook(func() func() { return t.afterHealthTimer })
		case <-t.done:
			return
		}
	}
}

func (t *Transport) heartbeatLoop() {
	heartbeat := t.clock.NewTimer(t.heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-heartbeat.C():
			t.queueControl(recProbe, 0)
			heartbeat.Reset(t.heartbeat)
			t.notifyTimerHook(func() func() { return t.afterHeartbeatTimer })
		case <-t.done:
			return
		}
	}
}

func (t *Transport) notifyTimerHook(get func() func()) {
	t.mu.Lock()
	hook := get()
	t.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (t *Transport) hopPacketConnOnce(generation uint64) {
	t.mu.Lock()
	if t.rebind == nil || t.hoppedOffline || t.closed || t.linkState != ports.LinkStateProbing || t.health.generation != generation {
		t.mu.Unlock()
		return
	}
	old := t.pc
	rebind := t.rebind
	t.hoppedOffline = true
	t.hopGeneration = generation
	t.mu.Unlock()
	pc, err := rebind(old)

	// Serialize the swap with writes. writePacket takes writeMu before mu, so
	// preserve that order here while retiring old.
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	t.mu.Lock()
	if err != nil {
		t.rollbackHopLocked(generation)
		t.mu.Unlock()
		return
	}
	if t.closed || t.pc != old || t.health.generation != generation || t.linkState != ports.LinkStateProbing {
		t.rollbackHopLocked(generation)
		t.mu.Unlock()
		_ = pc.Close()
		return
	}
	t.pc = pc
	t.writeDeadlines = newWriteDeadlineState(pc)
	t.mu.Unlock()
	go t.readLoop(pc)
	_ = old.Close()
}

func (t *Transport) rollbackHopLocked(generation uint64) {
	if t.hoppedOffline && t.hopGeneration == generation {
		t.hoppedOffline = false
		t.hopGeneration = 0
	}
}

func (t *Transport) checkSilence() {
	runHook := true
	for {
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return
		}
		now := t.clock.Now()
		generation := t.health.generation
		state, hop, dead := t.health.decide(now, len(t.pending) > 0, t.degradedAfter, t.probeAfter, t.offlineAfter, t.deadAfter)
		afterDecision := t.afterHealthDecision
		t.mu.Unlock()

		if runHook {
			runHook = false
			if afterDecision != nil {
				afterDecision()
			}
		}

		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return
		}
		if t.health.generation != generation {
			t.mu.Unlock()
			continue
		}
		changed, stateGeneration := t.setLinkStateLocked(state)
		afterStateCommit := t.afterLinkStateCommit
		closed := false
		if dead {
			closed = t.closeWithErrorLocked(ErrLinkDead)
		}
		t.mu.Unlock()

		if changed {
			if afterStateCommit != nil {
				afterStateCommit(state)
			}
			var linkErr error
			if dead {
				linkErr = ErrLinkDead
			}
			t.publishLinkState(state, now, linkErr, stateGeneration)
		}
		if closed {
			t.broadcastClosed()
			return
		}
		if hop {
			t.hopPacketConnOnce(generation)
		}
		return
	}
}

func (t *Transport) setLinkState(state ports.LinkState, err error) {
	now := t.clock.Now()
	t.mu.Lock()
	changed, stateGeneration := t.setLinkStateLocked(state)
	afterStateCommit := t.afterLinkStateCommit
	t.mu.Unlock()
	if changed {
		if afterStateCommit != nil {
			afterStateCommit(state)
		}
		t.publishLinkState(state, now, err, stateGeneration)
	}
}

func (t *Transport) setLinkStateLocked(state ports.LinkState) (bool, uint64) {
	if t.linkState == state {
		return false, t.linkStateGeneration
	}
	t.linkState = state
	t.linkStateGeneration++
	if state == ports.LinkStateConnected {
		t.hoppedOffline = false
		t.hopGeneration = 0
		for _, p := range t.pending {
			if p != nil {
				p.attempts = 0
			}
		}
	}
	t.notifySendWaitersLocked()
	return true, t.linkStateGeneration
}

func (t *Transport) publishLinkState(state ports.LinkState, now time.Time, err error, generation uint64) {
	t.linkEventMu.Lock()
	defer t.linkEventMu.Unlock()
	t.mu.Lock()
	if t.linkState != state || t.linkStateGeneration != generation {
		t.mu.Unlock()
		return
	}
	event := ports.LinkEvent{State: state, At: now, Err: err}
	select {
	case t.linkEvents <- event:
	default:
	}
	t.mu.Unlock()
	t.emitDiagnostic()
}
