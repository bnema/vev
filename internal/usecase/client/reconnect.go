package client

import (
	"errors"
	"time"

	"github.com/bnema/vev/internal/ports"
)

type reconnectBackoff struct {
	initial time.Duration
	max     time.Duration
}

var defaultReconnectBackoff = reconnectBackoff{initial: 100 * time.Millisecond, max: 2 * time.Second}

// errLinkOffline is returned by attachOnce when the transport link reaches the
// Offline state. It is retryable so Run re-dials over ssh with the resume token
// at ~30s of silence instead of waiting the full Dead timeout at 60s.
var errLinkOffline = errors.New("vev: link offline; reconnecting")

type reconnectStage int

const (
	reconnectStageDegraded reconnectStage = iota
	reconnectStageProbingUDP
	reconnectStageSSH
	reconnectStageOfflineRetrying
)

func stageForLinkState(state ports.LinkState) reconnectStage {
	switch state {
	case ports.LinkStateDegraded:
		return reconnectStageDegraded
	case ports.LinkStateProbing:
		return reconnectStageProbingUDP
	case ports.LinkStateOffline, ports.LinkStateDead:
		return reconnectStageOfflineRetrying
	default:
		return reconnectStageSSH
	}
}

func nextReconnectBackoff(cur, limit time.Duration) time.Duration {
	cur *= 2
	if cur > limit {
		return limit
	}
	return cur
}

func shouldReconnect(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := errors.AsType[*ProtocolError](err); ok {
		return false
	}
	_, ok := errors.AsType[*DetachedError](err)
	return !ok
}
