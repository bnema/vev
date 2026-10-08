package brokerwire

// Stateless bound refusals.
//
// The broker preamble negotiates one immutable pair of ceilings per
// connection: MaxReceiveEnvelopeBytes bounds every envelope this side
// accepts, and StreamChunkLimit bounds one opaque ClientStreamData or
// ServerStreamData frame. Envelope ceilings are fixed: ordinary envelopes
// never exceed the advertised floor, and the preamble advertisement itself
// rides inside BrokerPreambleLimit.
//
// The broker has no Output/image/picker bulk category: every application
// envelope is control-class and shares one ceiling. Stream chunk payloads
// are bounded separately so hostile lengths never allocate.

import (
	"github.com/bnema/vev/internal/protocol/wire"
)

const (
	// BrokerRoleClient is the broker preamble client role. It is disjoint
	// from the session preamble roles 1/2.
	BrokerRoleClient uint32 = 3
	// BrokerRoleServer is the broker preamble server role.
	BrokerRoleServer uint32 = 4

	// BrokerPreambleLimit bounds the serialized broker preamble envelope.
	BrokerPreambleLimit = 4 << 10

	// MinBrokerEnvelopeBytes is the advertised-envelope floor: a broker
	// peer must accept at least 1 MiB per envelope.
	MinBrokerEnvelopeBytes uint64 = 1 << 20
	// MaxBrokerEnvelopeBytes is the advertised-envelope ceiling: a broker
	// peer must never advertise above 16 MiB (wire.AbsoluteEnvelopeLimit).
	MaxBrokerEnvelopeBytes uint64 = wire.AbsoluteEnvelopeLimit

	// MinStreamChunkBytes is the smallest legal stream chunk ceiling.
	MinStreamChunkBytes uint64 = 1
	// MaxStreamChunkBytes is the largest legal stream chunk ceiling:
	// 64 KiB per opaque stream frame.
	MaxStreamChunkBytes uint64 = 64 << 10

	// StreamWindowBytes is the stream-data credit each side of one logical
	// stream grants its peer per direction. A sender spends StreamChunkCredit
	// per data frame and waits when it has too little, so the receiver never
	// queues more than this per stream; StreamWindowUpdate returns the credit
	// of consumed frames.
	StreamWindowBytes uint64 = 4 << 20
	// StreamChunkCreditOverhead is the fixed credit one data frame costs on top
	// of its payload, so a burst of tiny frames is bounded in count as well.
	StreamChunkCreditOverhead uint64 = 64
)

// StreamChunkCredit is the flow-control cost of one stream data frame of n
// payload bytes.
func StreamChunkCredit(n int) uint64 { return uint64(n) + StreamChunkCreditOverhead }

// Ceilings are the immutable per-connection ceilings negotiated once by the
// broker preamble. MaxReceiveEnvelopeBytes bounds every envelope this side
// accepts; StreamChunkLimit bounds one opaque stream data frame in either
// direction.
//
// The preamble helpers and the connection dispatcher share this value: a
// caller that completed the preamble reads the negotiated limits from
// BrokerPreambleRequest or BrokerPreambleResponse and passes them to
// EncodeClient/DecodeClient and EncodeServer/DecodeServer.
type Ceilings struct {
	MaxReceiveEnvelopeBytes uint64
	StreamChunkLimit        uint64
}

// brokerCeilings is the in-package alias for Ceilings. It exists only so
// The codec tests keep their short spelling; production code uses Ceilings.
type brokerCeilings = Ceilings

// defaultBrokerCeilings advertises the local broker receive policy: the
// full absolute envelope ceiling and the maximum stream chunk.
func defaultBrokerCeilings() Ceilings {
	return Ceilings{
		MaxReceiveEnvelopeBytes: wire.AbsoluteEnvelopeLimit,
		StreamChunkLimit:        MaxStreamChunkBytes,
	}
}

// DefaultCeilings returns the local broker receive policy: the full absolute
// envelope ceiling and the maximum stream chunk. It is the offer a broker IPC
// client or server advertises before negotiation.
func DefaultCeilings() Ceilings { return defaultBrokerCeilings() }

// effectiveBrokerCeilings takes the minima of both sides' advertisements.
func effectiveBrokerCeilings(local, remote Ceilings) Ceilings {
	return Ceilings{
		MaxReceiveEnvelopeBytes: min(local.MaxReceiveEnvelopeBytes, remote.MaxReceiveEnvelopeBytes),
		StreamChunkLimit:        min(local.StreamChunkLimit, remote.StreamChunkLimit),
	}
}

// EffectiveCeilings returns the element-wise minima of the two advertised
// ceilings: one byte stream carries exactly the limits both peers accepted.
func EffectiveCeilings(local, remote Ceilings) Ceilings {
	return effectiveBrokerCeilings(local, remote)
}

// checkBrokerCeilings refuses a zero, below-floor, or above-ceiling
// advertisement. Envelope bounds are the 1 MiB..16 MiB advertised window;
// chunk bounds are 1..64 KiB.
func checkBrokerCeilings(remote Ceilings) error {
	if remote.MaxReceiveEnvelopeBytes < MinBrokerEnvelopeBytes ||
		remote.MaxReceiveEnvelopeBytes > MaxBrokerEnvelopeBytes {
		return ErrPreambleRejected
	}
	if remote.StreamChunkLimit < MinStreamChunkBytes ||
		remote.StreamChunkLimit > MaxStreamChunkBytes {
		return ErrPreambleRejected
	}
	return nil
}

// Validate reports whether one advertised ceiling is negotiable: an envelope
// window inside 1 MiB..16 MiB and a stream chunk inside 1..64 KiB.
func (c Ceilings) Validate() error { return checkBrokerCeilings(c) }

// checkBrokerEnvelopeCeiling rejects one serialized broker envelope above
// the negotiated ceiling. All broker envelopes are control-class: the
// negotiated ceiling always applies in full.
func checkBrokerEnvelopeCeiling(payload []byte, ceiling uint64) error {
	if uint64(len(payload)) > ceiling {
		return wire.ErrScanLength
	}
	return nil
}

// CheckPreambleSize refuses one serialized broker preamble above the 4 KiB
// bound before scanning or allocation. Callers apply it to an encoded
// preamble before sending and to a received payload before decode; the
// session contract applies the identical wire.PreambleLimit.
func CheckPreambleSize(payload []byte) error {
	if len(payload) > BrokerPreambleLimit {
		return ErrPreambleRejected
	}
	return nil
}
