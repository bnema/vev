package daemonmux

// Physical-connection heartbeat.
//
// Ping and Pong are stream-less control frames. They never pass through the
// per-stream Scheduler (which can be full or fair-queued behind bulk data);
// they ride Pump.control, a single slot the writer drains before any scheduler
// frame. A pump only ever produces one kind (the daemon side pongs, the broker
// side pings) and a newer unsent frame replaces an older one, so the slot is
// bounded by construction. Control frames are not part of Flush.
//
// The daemon-side pump answers the latest pending Ping with a Pong echoing its
// nonce: a newer unsent Pong replaces an older one, so a burst of pings may be
// answered once. The broker treats any inbound frame as liveness without
// matching nonces; the nonce is for diagnostics only. The
// broker-side pump of a remote connection runs heartbeatLoop: every
// heartbeatInterval it queues a Ping, and it settles the connection as
// RemoteFailureTimeout once heartbeatMisses consecutive ticks passed with no
// inbound frame since the first unanswered ping.
//
// Liveness is inbound frames only, of any kind. Outbound progress is never
// liveness: QUIC and SSH pipes buffer writes, so a carrier Send completes
// instantly on a dead link. A silent link is therefore detected between
// heartbeatInterval*heartbeatMisses and heartbeatInterval*(heartbeatMisses+1)
// after it died.
//
// Time comes only from the injected ports.Clock timer, and the loop counts
// ticks instead of reading wall time. Suspend needs no special case: timers
// run on the monotonic clock, which pauses while the machine sleeps, so after
// resume the next tick fires within one interval, queues a ping, and a dead
// link is declared after at most heartbeatMisses further intervals (~15s).

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	// heartbeatInterval is how often the broker pings the daemon.
	heartbeatInterval = 5 * time.Second
	// heartbeatMisses is how many consecutive ticks may pass without any
	// inbound frame after a ping before the connection is declared dead.
	heartbeatMisses = 2
)

// ErrHeartbeatTimeout is the terminal cause of a physical connection whose
// peer stopped answering the heartbeat.
var ErrHeartbeatTimeout = errors.New("daemonmux: heartbeat timeout")

// queueControl stores one encoded Ping or Pong in the priority slot, replacing
// any unsent one, and wakes the writer. An encode failure drops the frame.
func (p *Pump) queueControl(raw []byte, err error) {
	if err != nil {
		slog.Warn("daemonmux_heartbeat_encode", "side", p.local.String(), "cause", err)
		return
	}
	p.flushMu.Lock()
	p.control = raw
	p.flushMu.Unlock()
	p.signal()
}

// noteLive records an inbound frame. It never blocks: one pending token is
// enough to tell the heartbeat loop the link delivered.
func (p *Pump) noteLive() {
	select {
	case p.live <- struct{}{}:
	default:
	}
}

// heartbeatLoop is the broker-side heartbeat goroutine. It exits when the run
// context ends (every terminal path cancels it) or when it declares the
// connection dead.
func (p *Pump) heartbeatLoop(ctx context.Context, tick ports.Timer) {
	defer close(p.hbDone)
	defer tick.Stop()
	var nonce uint64
	missed := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
		}
		select {
		case <-p.live:
			missed = 0
		default:
			if nonce > 0 { // a ping is outstanding
				missed++
			}
		}
		if missed >= heartbeatMisses {
			p.settleFailure(domain.RemoteFailureTimeout, ErrHeartbeatTimeout)
			return
		}
		nonce++
		p.queueControl(EncodeClient(Ping{Nonce: nonce}, p.ceilings.MaxReceiveEnvelopeBytes, p.ceilings.StreamChunkLimit))
		tick.Reset(heartbeatInterval)
	}
}
