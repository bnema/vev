package daemonmux

// Physical-connection heartbeat.
//
// Ping and Pong are stream-less control frames. They never pass through the
// per-stream Scheduler (which admits only frames of an admitted stream and can
// be full or fair-queued behind bulk data); they ride a dedicated control slot
// the writer drains before any scheduler frame. The slot holds at most one
// pending ping and one pending pong, and a newer value replaces an older
// unsent one, so it is bounded by construction.
//
// The daemon-side pump (inbound DirectionClient) answers every Ping with a
// Pong echoing the nonce. The broker-side pump (inbound DirectionServer) runs
// the heartbeat loop below when enabled with WithPumpHeartbeat: it pings every
// Interval and settles the connection as RemoteFailureTimeout when no inbound
// frame arrived for Timeout after a ping.
//
// Liveness is inbound frames only, of any kind. Outbound progress is never
// liveness: QUIC and SSH pipes buffer writes, so a carrier Send completes
// instantly on a dead link. To keep a slow-but-alive uplink (a ping queued
// behind a large write) from timing out early, the reply deadline starts when
// the writer actually handed the ping to the carrier. Until then the same
// Timeout bounds the wait for the write itself, so a Send that blocks forever
// is dead too; the worst case before giving up is therefore two Timeouts. A
// ping or pong is a control frame and is not part of Flush.
//
// All time comes from the injected ports.Clock/ports.Timer. Timer deadlines
// are durations on the timer's own monotonic axis, never absolute wall
// instants. A suspend is detected when a timer fires: Go timers use a monotonic
// clock that pauses while the machine sleeps, whereas the wall clock keeps
// running, so (wall delta since arming - the timer's planned period) greater
// than suspendSlack means the machine likely slept and the link may be dead.
// The loop then pings at once and allows only ResumeTimeout for a reply. The
// planned period stands in for the monotonic delta, which keeps the check
// independent of the clock's monotonic readings (and so deterministic under a
// mock clock). Consequently a forward NTP step, or a goroutine stall longer
// than suspendSlack, is also taken for a suspend; that only costs one early
// probe with the short deadline.

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Heartbeat defaults.
const (
	// DefaultHeartbeatInterval is how often the broker pings the daemon.
	DefaultHeartbeatInterval = 5 * time.Second
	// DefaultHeartbeatTimeout is how long the connection may stay silent
	// (no inbound frame of any kind) after a ping before it is declared dead.
	DefaultHeartbeatTimeout = 10 * time.Second
	// DefaultHeartbeatResumeTimeout is the reply deadline after a detected
	// suspend/resume.
	DefaultHeartbeatResumeTimeout = 3 * time.Second

	// suspendSlack is how far the wall-clock delta may exceed the monotonic
	// delta between two readings before the gap is treated as a suspend.
	suspendSlack = 5 * time.Second
)

// ErrHeartbeatTimeout is the terminal cause of a physical connection whose
// peer stopped answering the heartbeat.
var ErrHeartbeatTimeout = errors.New("daemonmux: heartbeat timeout")

// HeartbeatConfig tunes the broker-side heartbeat. A zero field selects its
// default; a negative field is invalid.
type HeartbeatConfig struct {
	Interval      time.Duration
	Timeout       time.Duration
	ResumeTimeout time.Duration
}

// DefaultHeartbeatConfig returns the default heartbeat tuning.
func DefaultHeartbeatConfig() HeartbeatConfig {
	return HeartbeatConfig{
		Interval:      DefaultHeartbeatInterval,
		Timeout:       DefaultHeartbeatTimeout,
		ResumeTimeout: DefaultHeartbeatResumeTimeout,
	}
}

// resolve fills zero fields with defaults and rejects negative ones.
func (c HeartbeatConfig) resolve() (HeartbeatConfig, error) {
	if c.Interval < 0 || c.Timeout < 0 || c.ResumeTimeout < 0 {
		return HeartbeatConfig{}, ErrPumpConfig
	}
	if c.Interval == 0 {
		c.Interval = DefaultHeartbeatInterval
	}
	if c.Timeout == 0 {
		c.Timeout = DefaultHeartbeatTimeout
	}
	if c.ResumeTimeout == 0 {
		c.ResumeTimeout = DefaultHeartbeatResumeTimeout
	}
	return c, nil
}

// heartbeat is the resolved broker-side heartbeat configuration of one pump.
type heartbeat struct {
	clock ports.Clock
	cfg   HeartbeatConfig
}

// PumpOption configures optional pump behavior at construction.
type PumpOption func(*pumpOptions) error

type pumpOptions struct {
	heartbeat *heartbeat
}

// WithPumpHeartbeat enables the broker-side heartbeat driven by clock. It is
// valid only on a pump whose inbound direction is DirectionServer (the broker
// side): the daemon side only answers pings. A nil clock or a negative
// configuration value is refused with ErrPumpConfig.
func WithPumpHeartbeat(clock ports.Clock, cfg HeartbeatConfig) PumpOption {
	return func(o *pumpOptions) error {
		if clock == nil {
			return ErrPumpConfig
		}
		resolved, err := cfg.resolve()
		if err != nil {
			return err
		}
		o.heartbeat = &heartbeat{clock: clock, cfg: resolved}
		return nil
	}
}

// controlSlot is the bounded priority queue for stream-less control frames:
// at most one pending encoded ping and one pending encoded pong.
type controlSlot struct {
	mu   sync.Mutex
	ping []byte
	pong []byte
}

// putPing stores an encoded ping, replacing any unsent one.
func (s *controlSlot) putPing(raw []byte) {
	s.mu.Lock()
	s.ping = raw
	s.mu.Unlock()
}

// putPong stores an encoded pong, replacing any unsent one.
func (s *controlSlot) putPong(raw []byte) {
	s.mu.Lock()
	s.pong = raw
	s.mu.Unlock()
}

// take removes the next pending control frame, pong first so a reply is never
// delayed behind the local probe, and reports whether it is the ping.
func (s *controlSlot) take() (raw []byte, isPing, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pong != nil {
		raw, s.pong = s.pong, nil
		return raw, false, true
	}
	if s.ping != nil {
		raw, s.ping = s.ping, nil
		return raw, true, true
	}
	return nil, false, false
}

// queuePong schedules the reply to one inbound Ping.
func (p *Pump) queuePong(nonce uint64) {
	if p.isTerminal() {
		return
	}
	raw, err := EncodeServer(Pong{Nonce: nonce}, p.ceilings.MaxReceiveEnvelopeBytes, p.ceilings.StreamChunkLimit)
	if err != nil {
		slog.Warn("daemonmux_heartbeat_encode", "side", p.local.String(), "frame", "pong", "cause", err)
		return
	}
	p.control.putPong(raw)
	p.signal()
}

// queuePing schedules the next outbound probe.
func (p *Pump) queuePing(nonce uint64) {
	if p.isTerminal() {
		return
	}
	raw, err := EncodeClient(Ping{Nonce: nonce}, p.ceilings.MaxReceiveEnvelopeBytes, p.ceilings.StreamChunkLimit)
	if err != nil {
		slog.Warn("daemonmux_heartbeat_encode", "side", p.local.String(), "frame", "ping", "cause", err)
		return
	}
	p.control.putPing(raw)
	p.signal()
}

// heartbeatState is the broker-side heartbeat's timers and bookkeeping. The
// timers are created in Start, before the goroutine runs, so a caller that has
// started the pump can rely on them being armed.
type heartbeatState struct {
	tick      ports.Timer
	dead      ports.Timer
	tickArmed time.Time // clock reading when tick was last armed or fired
	deadArmed time.Time // clock reading when dead was armed
	deadFor   time.Duration
	awaiting  bool // a ping is outstanding and dead is armed
	nonce     uint64
}

// newState arms the first tick; the deadline timer starts stopped.
func (h *heartbeat) newState() *heartbeatState {
	st := &heartbeatState{
		tick:      h.clock.NewTimer(h.cfg.Interval),
		dead:      h.clock.NewTimer(h.cfg.Timeout),
		tickArmed: h.clock.Now(),
	}
	st.dead.Stop()
	return st
}

// slept returns how much longer the wall clock advanced than a timer's
// monotonic period: the time the machine spent suspended. The monotonic side is
// the planned period itself, because the timer fires after exactly that much
// monotonic time; the wall side compares two readings stripped of their
// monotonic part with Round(0).
func slept(from, to time.Time, planned time.Duration) time.Duration {
	return to.Round(0).Sub(from.Round(0)) - planned
}

// noteLive records a sign of life: an inbound frame. It never blocks: one
// pending token is enough to tell the heartbeat goroutine the link delivered.
func (p *Pump) noteLive() {
	if p.hb == nil {
		return
	}
	select {
	case p.live <- struct{}{}:
	default:
	}
}

// notePingWritten tells the heartbeat goroutine that a ping was handed to the
// carrier. It never blocks.
func (p *Pump) notePingWritten() {
	if p.hb == nil {
		return
	}
	select {
	case p.pinged <- struct{}{}:
	default:
	}
}

// heartbeatLoop is the broker-side heartbeat goroutine. It exits when the run
// context ends (every terminal path cancels it) or when it declares the
// connection dead.
func (p *Pump) heartbeatLoop(ctx context.Context, st *heartbeatState) {
	defer close(p.hbDone)
	defer st.tick.Stop()
	defer st.dead.Stop()
	cfg := p.hb.cfg

	// alive clears the outstanding ping and its reply deadline.
	alive := func() {
		st.awaiting = false
		st.dead.Stop()
	}

	// arm (re)starts the deadline timer from now.
	arm := func(now time.Time, deadline time.Duration) {
		st.dead.Reset(deadline)
		st.deadArmed = now
		st.deadFor = deadline
	}

	// probe queues one ping. The deadline first bounds the wait for the ping
	// to be written at all (a blocked carrier Send is a dead link too) and is
	// armed again, for the reply, once the writer handed the ping to the
	// carrier. A token left over from before the ping proves nothing about
	// the reply, so it is dropped.
	probe := func(now time.Time, deadline time.Duration) {
		select {
		case <-p.live:
		default:
		}
		select {
		case <-p.pinged:
		default:
		}
		arm(now, deadline)
		st.awaiting = true
		st.nonce++
		p.queuePing(st.nonce)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.live:
			alive()
		case <-p.pinged:
			if st.awaiting {
				arm(p.hb.clock.Now(), st.deadFor)
			}
		case <-st.tick.C():
			now := p.hb.clock.Now()
			asleep := slept(st.tickArmed, now, cfg.Interval)
			st.tickArmed = now
			switch {
			case asleep > suspendSlack:
				// Probe now and allow only the short resume deadline, even
				// if a longer one was already running.
				slog.Debug("daemonmux_heartbeat_resume", "side", p.local.String(), "slept", asleep)
				probe(now, cfg.ResumeTimeout)
			case !st.awaiting:
				probe(now, cfg.Timeout)
			}
			// Re-arm last: once armed again, any probe for this tick is queued.
			st.tick.Reset(cfg.Interval)
		case <-st.dead.C():
			if !st.awaiting {
				// Already cleared by a sign of life.
				continue
			}
			select {
			case <-p.live:
				// A reply raced the deadline.
				alive()
				continue
			default:
			}
			now := p.hb.clock.Now()
			if slept(st.deadArmed, now, st.deadFor) > suspendSlack {
				// The deadline straddled a suspend: the link had no real
				// chance to answer yet, so probe again with the short
				// resume deadline instead of failing outright.
				probe(now, cfg.ResumeTimeout)
				continue
			}
			p.settleFailure(domain.RemoteFailureTimeout, ErrHeartbeatTimeout)
			return
		}
	}
}
