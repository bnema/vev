package dgram

import (
	"time"

	"github.com/bnema/vev/internal/ports"
)

// healthTracker separates authenticated path contact from useful stream
// progress. Transport.mu protects every field.
type healthTracker struct {
	lastPacket   time.Time
	lastRecord   time.Time
	lastProgress time.Time
	pendingSince time.Time
	generation   uint64
}

func newHealthTracker(now time.Time) healthTracker {
	return healthTracker{lastPacket: now, lastRecord: now, lastProgress: now}
}

func (h *healthTracker) authenticatedPacket(now time.Time) {
	h.lastPacket = now
	h.generation++
}

func (h *healthTracker) completeRecord(now time.Time) {
	h.lastRecord = now
	h.generation++
}

func (h *healthTracker) ackProgress(now time.Time) {
	h.lastProgress = now
	h.generation++
}

func (h *healthTracker) pendingStarted(now time.Time) {
	h.pendingSince = now
	h.generation++
}

func (h *healthTracker) pendingCleared() {
	h.pendingSince = time.Time{}
	h.generation++
}

func (h healthTracker) decide(now time.Time, hasPending bool, degradedAfter, probeAfter, offlineAfter, deadAfter time.Duration) (ports.LinkState, bool, bool) {
	packetAge := now.Sub(h.lastPacket)
	switch {
	case deadAfter > 0 && packetAge >= deadAfter:
		return ports.LinkStateDead, false, true
	case offlineAfter > 0 && packetAge >= offlineAfter:
		return ports.LinkStateOffline, false, false
	case probeAfter > 0 && packetAge >= probeAfter:
		return ports.LinkStateProbing, true, false
	}

	qualityAt := h.lastRecord
	if h.lastProgress.After(qualityAt) {
		qualityAt = h.lastProgress
	}
	qualityAge := now.Sub(qualityAt)
	if hasPending && probeAfter > 0 {
		pendingBaseline := h.pendingSince
		if pendingBaseline.IsZero() || h.lastProgress.After(pendingBaseline) {
			pendingBaseline = h.lastProgress
		}
		if now.Sub(pendingBaseline) >= probeAfter {
			return ports.LinkStateProbing, true, false
		}
	}
	if degradedAfter > 0 && qualityAge >= degradedAfter {
		return ports.LinkStateDegraded, false, false
	}
	return ports.LinkStateConnected, false, false
}
