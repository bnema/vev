package daemonmux

// Stateless bound refusals.
//
// One physical daemon connection negotiates one immutable set of ceilings
// exactly once, in the daemonmux physical preamble: a receive envelope
// ceiling (1 MiB..16 MiB), a stream chunk ceiling (1..64 KiB), a stream
// count ceiling (1..128), and an aggregate buffered-bytes ceiling
// (MinMuxAggregateBytes..64 MiB, one maximum chunk plus envelope overhead at
// the floor). Every application envelope and every opaque data frame is
// bounded statelessly before it allocates; the strictly-increasing stream
// ID rule and the aggregate accounting are state-layer concerns owned by
// the later engine slice, so this codec only enforces nonzero stream IDs.
//
// The daemonmux conversation carries no bulk category: every application
// envelope is control-class and never exceeds 1 MiB, so one envelope ceiling
// and one chunk ceiling cover the whole conversation. The negotiated receive
// ceiling still bounds the physical carriage's own envelope reads, which may
// sit above what a mux application envelope can ever need.

import (
	"errors"

	"github.com/bnema/vev/internal/protocol/wire"
)

const (
	// MuxRoleClient is the daemonmux preamble client role: the broker side
	// that owns the physical connection. It is disjoint from the session
	// roles 1/2 and the broker roles 3/4.
	MuxRoleClient uint32 = 5
	// MuxRoleServer is the daemonmux preamble server role: the daemon that
	// accepts the multiplexed streams.
	MuxRoleServer uint32 = 6

	// MuxPreambleLimit bounds the serialized daemonmux preamble. It is the
	// same 4 KiB bound the session and broker preambles use.
	MuxPreambleLimit = 4 << 10

	// MinMuxEnvelopeBytes is the advertised-envelope floor: a daemonmux peer
	// must accept at least 1 MiB per application envelope
	// (wire.ControlEnvelopeLimit, the control-class ceiling).
	MinMuxEnvelopeBytes uint64 = wire.ControlEnvelopeLimit
	// MaxMuxEnvelopeBytes is the advertised-envelope ceiling: a daemonmux
	// peer must never advertise above 16 MiB (wire.AbsoluteEnvelopeLimit).
	MaxMuxEnvelopeBytes uint64 = wire.AbsoluteEnvelopeLimit

	// MaxMuxApplicationBytes is the absolute bound on one daemonmux
	// application envelope. Every mux envelope is control-class metadata, so
	// no application envelope ever exceeds 1 MiB (wire.ControlEnvelopeLimit)
	// even when the negotiated receive ceiling admits a larger carriage
	// envelope. The codec therefore enforces both bounds at once.
	MaxMuxApplicationBytes uint64 = wire.ControlEnvelopeLimit

	// MinMuxChunkBytes is the smallest legal stream chunk ceiling.
	MinMuxChunkBytes uint64 = 1
	// MaxMuxChunkBytes is the largest legal stream chunk ceiling: 64 KiB per
	// opaque data frame.
	MaxMuxChunkBytes uint64 = 64 << 10

	// MinMuxStreams is the smallest legal concurrent-stream ceiling.
	MinMuxStreams uint64 = 1
	// MaxMuxStreams is the largest legal concurrent-stream ceiling.
	MaxMuxStreams uint64 = 128

	// MuxEnvelopeOverheadBytes is the largest carriage overhead one daemonmux
	// data frame adds around its opaque payload: the outer directional oneof
	// tag and length plus the MuxData field tags, the varint physical stream
	// ID, and the varint payload length. The scheduler charges a frame's full
	// serialized envelope against the aggregate byte budget, so an aggregate
	// ceiling below one maximum chunk plus this overhead could refuse even a
	// single legal frame. 64 bytes is a deliberate upper bound on that
	// overhead (the worst case is under 32 bytes).
	MuxEnvelopeOverheadBytes uint64 = 64

	// MinMuxAggregateBytes is the smallest legal aggregate buffered-bytes
	// ceiling: one maximum stream chunk plus the envelope overhead, so a peer
	// that advertises the floor can always buffer at least one full-size
	// frame in the direction it accepts.
	MinMuxAggregateBytes uint64 = MaxMuxChunkBytes + MuxEnvelopeOverheadBytes
	// MaxMuxAggregateBytes is the largest legal aggregate buffered-bytes
	// ceiling: 64 MiB across every live stream of one physical connection.
	MaxMuxAggregateBytes uint64 = 64 << 20

	// MuxChunkCreditOverhead is the fixed credit one Data frame costs on top of
	// its payload bytes. Charging it keeps a burst of tiny frames bounded in
	// count as well as in bytes.
	MuxChunkCreditOverhead uint64 = MuxEnvelopeOverheadBytes
	// MaxMuxStreamWindowBytes caps one stream's receive window and every
	// WindowUpdate grant.
	MaxMuxStreamWindowBytes uint64 = 4 << 20
)

// chunkCredit is the flow-control cost of one Data frame of n payload bytes.
func chunkCredit(n int) uint64 { return uint64(n) + MuxChunkCreditOverhead }

// StreamWindow is the per-stream receive window both peers derive from the
// same negotiated ceilings: the aggregate budget shared evenly across the
// stream ceiling, so every live stream's window fits the aggregate at once,
// never below one maximum chunk (a full chunk must always be sendable) and
// never above MaxMuxStreamWindowBytes. With the default ceilings it is 512 KiB.
func (c MuxCeilings) StreamWindow() uint64 {
	window := c.MaxAggregateBytes / max(c.MaxStreams, 1)
	window = max(window, chunkCredit(int(c.StreamChunkLimit)))
	return min(window, MaxMuxStreamWindowBytes)
}

// creditReturnThreshold is the consumed credit a receiver batches before it
// grants a WindowUpdate: half the window, lowered when needed so a sender
// blocked on a maximum chunk is always unblocked. A sender waits only once it
// spent more than window-chunkCredit(limit), so granting at or below that
// point can never deadlock.
func (c MuxCeilings) creditReturnThreshold() uint64 {
	window := c.StreamWindow()
	cost := chunkCredit(int(c.StreamChunkLimit))
	return max(min(window/2, window-cost+1), 1)
}

// ErrInvalidCeilings reports an advertisement outside its window: a zero,
// below-floor, or above-ceiling envelope, chunk, stream-count, or aggregate
// bound. The preamble maps it to its own ErrPreambleRejected sentinel; the
// engine and scheduler constructors return it directly.
var ErrInvalidCeilings = errors.New("daemonmux: invalid ceilings")

// MuxCeilings is the validated, immutable set of per-connection ceilings
// negotiated exactly once by the daemonmux physical preamble: the receive
// envelope ceiling (1 MiB..16 MiB), the stream chunk ceiling (1..64 KiB), the
// concurrent stream ceiling (1..128), and the aggregate buffered-bytes
// ceiling (MinMuxAggregateBytes..64 MiB). The preamble adopts the
// element-wise minima of both peers' advertisements as its effective value.
//
// The engine and scheduler constructors take a MuxCeilings value and copy it,
// so the ceilings an instance enforces are fixed for its lifetime: a caller
// that mutates its own value after construction never changes live
// enforcement. The zero value is invalid and is refused by Validate.
type MuxCeilings struct {
	// MaxReceiveEnvelopeBytes is the largest serialized application envelope
	// either peer accepts, 1 MiB..16 MiB.
	MaxReceiveEnvelopeBytes uint64
	// StreamChunkLimit is the largest opaque stream chunk, 1..64 KiB.
	StreamChunkLimit uint64
	// MaxStreams is the largest number of pending plus live streams, 1..128:
	// opening, open, or closing (a closing stream keeps its slot until its
	// preserved inbound data is drained).
	MaxStreams uint64
	// MaxAggregateBytes is the largest number of buffered bytes across every
	// live stream, MinMuxAggregateBytes..64 MiB. It is a per-direction budget:
	// the inbound engine and the outbound scheduler each enforce it
	// independently, so the two buffered directions never share one
	// accounting. The floor admits one maximum chunk plus envelope overhead.
	MaxAggregateBytes uint64
}

// Validate refuses a zero, below-floor, or above-ceiling advertisement.
// Envelope bounds are the 1 MiB..16 MiB window, chunk bounds are 1..64 KiB,
// stream counts are 1..128, and aggregate bounds are
// MinMuxAggregateBytes..64 MiB (one maximum chunk plus envelope overhead at
// the floor).
func (c MuxCeilings) Validate() error {
	if c.MaxReceiveEnvelopeBytes < MinMuxEnvelopeBytes ||
		c.MaxReceiveEnvelopeBytes > MaxMuxEnvelopeBytes {
		return ErrInvalidCeilings
	}
	if c.StreamChunkLimit < MinMuxChunkBytes ||
		c.StreamChunkLimit > MaxMuxChunkBytes {
		return ErrInvalidCeilings
	}
	if c.MaxStreams < MinMuxStreams || c.MaxStreams > MaxMuxStreams {
		return ErrInvalidCeilings
	}
	if c.MaxAggregateBytes < MinMuxAggregateBytes ||
		c.MaxAggregateBytes > MaxMuxAggregateBytes {
		return ErrInvalidCeilings
	}
	return nil
}

// DefaultMuxCeilings advertises the local daemonmux receive policy: the full
// absolute envelope ceiling, the maximum chunk, the maximum stream count,
// and the maximum aggregate buffered bytes.
func DefaultMuxCeilings() MuxCeilings {
	return MuxCeilings{
		MaxReceiveEnvelopeBytes: MaxMuxEnvelopeBytes,
		StreamChunkLimit:        MaxMuxChunkBytes,
		MaxStreams:              MaxMuxStreams,
		MaxAggregateBytes:       MaxMuxAggregateBytes,
	}
}

// EffectiveMuxCeilings takes the element-wise minima of both sides'
// advertisements: the value the preamble adopts when a peer offers less than
// the local policy. Each field is independent, so a peer that offers less in
// one dimension lowers only that dimension. Both inputs are assumed
// individually valid; the minima of two valid advertisements is valid.
func EffectiveMuxCeilings(local, remote MuxCeilings) MuxCeilings {
	return MuxCeilings{
		MaxReceiveEnvelopeBytes: min(local.MaxReceiveEnvelopeBytes, remote.MaxReceiveEnvelopeBytes),
		StreamChunkLimit:        min(local.StreamChunkLimit, remote.StreamChunkLimit),
		MaxStreams:              min(local.MaxStreams, remote.MaxStreams),
		MaxAggregateBytes:       min(local.MaxAggregateBytes, remote.MaxAggregateBytes),
	}
}

// checkMuxCeilings refuses an invalid advertisement as a preamble refusal so
// the preamble keeps its single ErrPreambleRejected sentinel.
func checkMuxCeilings(ceilings MuxCeilings) error {
	if err := ceilings.Validate(); err != nil {
		return ErrPreambleRejected
	}
	return nil
}

// checkMuxEnvelopeCeiling rejects one serialized daemonmux application
// envelope above either bound: the negotiated receive ceiling or the fixed
// 1 MiB application ceiling. Every daemonmux envelope is control-class, so
// the application ceiling always applies in full.
func checkMuxEnvelopeCeiling(payload []byte, ceiling uint64) error {
	if uint64(len(payload)) > min(ceiling, MaxMuxApplicationBytes) {
		return wire.ErrScanLength
	}
	return nil
}

// CheckPreambleSize refuses one serialized daemonmux preamble above the
// 4 KiB bound before scanning or allocation. Callers apply it to an encoded
// preamble before sending and to a received payload before decode; the
// session and broker contracts apply the identical wire.PreambleLimit.
func CheckPreambleSize(payload []byte) error {
	if len(payload) > MuxPreambleLimit {
		return ErrPreambleRejected
	}
	return nil
}
