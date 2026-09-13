// Package quicnettest provides a deterministic, unprivileged UDP carriage
// simulator for adverse-network tests of QUIC-like traffic.
//
// The package is test support: it is stdlib-only, runs entirely in user space,
// and never needs netem, root, or another privileged facility. A [Proxy] sits
// between a UDP client and a UDP server and impairs both directions
// independently:
//
//   - deterministic packet loss,
//   - deterministic packet duplication,
//   - delay-based reordering,
//   - constant latency plus bounded uniform jitter,
//   - scheduled or manually toggled blackout windows,
//   - NAT-style rebinding that changes the proxy's upstream source address.
//
// # Determinism
//
// Every random decision uses either the per-direction source seeded from
// [Config.Seed] or the caller-supplied [Config.RNG], so a given seed and packet
// sequence always produce the same decisions, in the documented order. Random
// draws are consumed only for impairments that are enabled, so a zeroed
// [LinkConfig] never touches the RNG. [Config.Clock] defaults to wall time; pass
// a [ManualClock] to advance the schedule without sleeping.
//
// # Bounds
//
// The proxy runs exactly three goroutines (one reader per socket direction plus
// one scheduler) regardless of traffic, and never spawns per-packet work. The
// packet budget is [Config.QueueCapacity]: queued and deadline-pending packets
// together may not exceed it. When the budget is exhausted the arriving packet
// is dropped and counted in [LinkStats.OverflowDrops] instead of blocking the
// reader. Packet buffers are bounded by [Config.MaxPacketSize], and larger
// datagrams are dropped and counted in [LinkStats.OversizeDrops].
//
// # Shutdown
//
// [Proxy.Close] is idempotent, wakes every goroutine, and waits for them to
// exit. Errors observed while shutting down are not counted as delivery errors,
// so [Proxy.Stats] stays meaningful after Close.
package quicnettest

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	// DefaultQueueCapacity bounds in-flight packets per proxy.
	DefaultQueueCapacity = 256
	// DefaultMaxPacketSize bounds a single datagram. QUIC-like traffic stays
	// well below this; larger datagrams are dropped rather than buffered.
	DefaultMaxPacketSize = 1500
	seedStride           = int64(2654435761)
)

// Direction identifies one carriage direction of the proxy.
type Direction int

const (
	// ToServer is the client -> proxy -> server direction.
	ToServer Direction = iota
	// ToClient is the server -> proxy -> client direction.
	ToClient
)

// String implements fmt.Stringer.
func (d Direction) String() string {
	switch d {
	case ToServer:
		return "to-server"
	case ToClient:
		return "to-client"
	default:
		return "direction(" + strconv.Itoa(int(d)) + ")"
	}
}

// RNG is the deterministic randomness seam for impairment decisions. The
// standard library's *rand.Rand satisfies it. The default source is seeded per
// direction and only ever touched by that direction's reader goroutine, so the
// simulation is race-free and reproducible. An injected RNG is shared by both
// directions and must be safe for concurrent use; it is intended for scripted
// single-direction tests.
type RNG interface {
	Intn(n int) int
}

// Blackout is a window during which every packet on a link is dropped. Start and
// End are relative to proxy creation, so the schedule never depends on wall
// time.
type Blackout struct {
	Start time.Duration
	End   time.Duration
}

// LinkConfig describes the impairment of one carriage direction. Percentages
// are in [0,100]; 100 always triggers and 0 never draws from the RNG.
type LinkConfig struct {
	// BaseLatency is added to every packet before it is scheduled.
	BaseLatency time.Duration
	// Jitter adds a uniform random delay in [0, Jitter] on top of BaseLatency.
	Jitter time.Duration
	// LossPercent drops packets with uniform probability.
	LossPercent int
	// DuplicatePercent schedules one extra copy of a packet.
	DuplicatePercent int
	// DuplicateDelay separates the extra copy from the original deadline.
	DuplicateDelay time.Duration
	// ReorderPercent delays a packet by ReorderDelay, which moves it behind
	// packets scheduled later in the same direction.
	ReorderPercent int
	// ReorderDelay is the reordering window.
	ReorderDelay time.Duration
	// Blackouts lists drop windows relative to proxy creation.
	Blackouts []Blackout
}

func (l LinkConfig) validate(direction Direction) error {
	if l.BaseLatency < 0 || l.Jitter < 0 || l.DuplicateDelay < 0 || l.ReorderDelay < 0 {
		return fmt.Errorf("quicnettest: %s link durations must not be negative", direction)
	}
	for name, percent := range map[string]int{
		"loss":      l.LossPercent,
		"duplicate": l.DuplicatePercent,
		"reorder":   l.ReorderPercent,
	} {
		if percent < 0 || percent > 100 {
			return fmt.Errorf("quicnettest: %s link %s percent %d is outside [0,100]", direction, name, percent)
		}
	}
	for i, window := range l.Blackouts {
		if window.Start < 0 || window.End <= window.Start {
			return fmt.Errorf("quicnettest: %s link blackout %d must satisfy 0 <= start < end", direction, i)
		}
	}
	return nil
}

// Config configures a [Proxy].
type Config struct {
	// ServerAddr is the UDP server the proxy forwards to. Required.
	ServerAddr *net.UDPAddr
	// Clock drives all scheduling. Defaults to [SystemClock].
	Clock Clock
	// Seed deterministically seeds the default per-direction RNG.
	Seed int64
	// RNG replaces the seeded source. Tests use it to script decisions.
	RNG RNG
	// QueueCapacity bounds queued and deadline-pending packets.
	// Defaults to [DefaultQueueCapacity].
	QueueCapacity int
	// MaxPacketSize bounds a single datagram. Defaults to
	// [DefaultMaxPacketSize].
	MaxPacketSize int
	// ToServer impairs client -> server traffic.
	ToServer LinkConfig
	// ToClient impairs server -> client traffic.
	ToClient LinkConfig
	// NATRebindAfterPackets changes the proxy's upstream source address after
	// every N client -> server packets have been sent. Zero disables automatic
	// rebinding; [Proxy.Rebind] is always available.
	NATRebindAfterPackets int
	// OnPacket, when set, observes every ingested packet. It is called from a
	// reader goroutine and must not block.
	OnPacket func(Event)
}

func (c Config) withDefaults() Config {
	if c.Clock == nil {
		c.Clock = SystemClock{}
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = DefaultQueueCapacity
	}
	if c.MaxPacketSize <= 0 {
		c.MaxPacketSize = DefaultMaxPacketSize
	}
	return c
}

func (c Config) validate() error {
	if c.ServerAddr == nil {
		return errors.New("quicnettest: ServerAddr is required")
	}
	if c.ServerAddr.Port < 1 || c.ServerAddr.Port > 65535 {
		return fmt.Errorf("quicnettest: ServerAddr port %d is out of range", c.ServerAddr.Port)
	}
	if c.NATRebindAfterPackets < 0 {
		return errors.New("quicnettest: NATRebindAfterPackets must not be negative")
	}
	if err := c.ToServer.validate(ToServer); err != nil {
		return err
	}
	return c.ToClient.validate(ToClient)
}

// Decision is the deterministic impairment outcome for one ingested packet.
type Decision struct {
	// Drop reports that the packet was discarded.
	Drop bool
	// Blackout reports that a blackout window caused the drop.
	Blackout bool
	// Oversize reports that the datagram exceeded MaxPacketSize.
	Oversize bool
	// Duplicates is the number of extra copies scheduled (0 or 1).
	Duplicates int
	// Reordered reports that the reordering window applied.
	Reordered bool
	// ExtraDelay is the random component added on top of BaseLatency.
	ExtraDelay time.Duration
}

// Event describes one ingested packet and its outcome.
type Event struct {
	Direction Direction
	Sequence  uint64
	Bytes     int
	// At is the clock time at which the packet was ingested.
	At time.Time
	// Due is the first copy's delivery deadline; zero for drops.
	Due      time.Time
	Decision Decision
}

// LinkStats audits one direction.
type LinkStats struct {
	// Received counts in-budget packets accepted from the source socket.
	// Oversize datagrams are counted only in OversizeDrops.
	Received uint64
	// Sent counts packets handed to the destination socket. It is incremented
	// before the write, so it is stable once the peer has observed a packet.
	Sent uint64
	// LossDrops counts seeded loss drops.
	LossDrops uint64
	// BlackoutDrops counts drops inside a blackout window or manual blackout.
	BlackoutDrops uint64
	// OversizeDrops counts datagrams larger than MaxPacketSize.
	OversizeDrops uint64
	// OverflowDrops counts packets dropped because QueueCapacity was full.
	OverflowDrops uint64
	// Duplicates counts extra copies scheduled.
	Duplicates uint64
	// Reordered counts packets delayed by the reordering window.
	Reordered uint64
	// DeliveryErrors counts socket write failures outside shutdown.
	DeliveryErrors uint64
}

// String implements fmt.Stringer.
func (l LinkStats) String() string {
	return fmt.Sprintf("received:%d sent:%d loss:%d blackout:%d oversize:%d overflow:%d duplicates:%d reordered:%d delivery-errors:%d",
		l.Received, l.Sent, l.LossDrops, l.BlackoutDrops, l.OversizeDrops, l.OverflowDrops,
		l.Duplicates, l.Reordered, l.DeliveryErrors)
}

// Stats is a snapshot of proxy counters.
type Stats struct {
	ToServer LinkStats
	ToClient LinkStats
	// StrayPackets counts upstream datagrams that did not come from ServerAddr
	// or arrived before any client address was known.
	StrayPackets uint64
	// Rebinds counts successful upstream source-address changes.
	Rebinds uint64
	// RebindErrors counts automatic rebinds that failed to open a socket.
	RebindErrors uint64
}

// String implements fmt.Stringer.
func (s Stats) String() string {
	return fmt.Sprintf("to-server{%s} to-client{%s} stray:%d rebinds:%d rebind-errors:%d",
		s.ToServer, s.ToClient, s.StrayPackets, s.Rebinds, s.RebindErrors)
}

// impairer applies one LinkConfig to one direction.
type impairer struct {
	link  LinkConfig
	rng   RNG
	start time.Time
}

// decide applies impairments in a fixed order so a given seed and packet
// sequence always produces the same decisions:
//
//  1. link blackout window,
//  2. loss,
//  3. duplication,
//  4. reordering (extra ReorderDelay),
//  5. jitter (uniform in [0, Jitter]).
func (im *impairer) decide(now time.Time) Decision {
	var decision Decision
	if im.blackout(now) {
		decision.Drop = true
		decision.Blackout = true
		return decision
	}
	if im.link.LossPercent > 0 && im.rng.Intn(100) < im.link.LossPercent {
		decision.Drop = true
		return decision
	}
	if im.link.DuplicatePercent > 0 && im.rng.Intn(100) < im.link.DuplicatePercent {
		decision.Duplicates = 1
	}
	if im.link.ReorderPercent > 0 && im.rng.Intn(100) < im.link.ReorderPercent {
		decision.Reordered = true
		decision.ExtraDelay += im.link.ReorderDelay
	}
	if im.link.Jitter > 0 {
		decision.ExtraDelay += time.Duration(im.rng.Intn(int(im.link.Jitter) + 1))
	}
	return decision
}

func (im *impairer) blackout(now time.Time) bool {
	elapsed := now.Sub(im.start)
	for _, window := range im.link.Blackouts {
		if elapsed >= window.Start && elapsed < window.End {
			return true
		}
	}
	return false
}

// lockedRNG serializes an injected RNG shared by both directions.
type lockedRNG struct {
	mu  sync.Mutex
	rng RNG
}

func (l *lockedRNG) Intn(n int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rng.Intn(n)
}

func newRNG(seed int64, direction Direction) RNG {
	return rand.New(rand.NewSource(seed + int64(direction)*seedStride))
}
