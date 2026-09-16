package brokerwire

// Stateless bound refusals (P3.1).
//
// The broker preamble negotiates one immutable pair of ceilings per
// connection: maxReceiveEnvelopeBytes bounds every envelope this side
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
)

// brokerCeilings are the immutable per-connection ceilings negotiated once
// by the broker preamble. StreamChunkLimit bounds one opaque stream data
// frame in either direction.
type brokerCeilings struct {
	maxReceiveEnvelopeBytes uint64
	streamChunkLimit        uint64
}

// defaultBrokerCeilings advertises the local broker receive policy: the
// full absolute envelope ceiling and the maximum stream chunk.
func defaultBrokerCeilings() brokerCeilings {
	return brokerCeilings{
		maxReceiveEnvelopeBytes: wire.AbsoluteEnvelopeLimit,
		streamChunkLimit:        MaxStreamChunkBytes,
	}
}

// effectiveBrokerCeilings takes the minima of both sides' advertisements.
func effectiveBrokerCeilings(local, remote brokerCeilings) brokerCeilings {
	return brokerCeilings{
		maxReceiveEnvelopeBytes: min(local.maxReceiveEnvelopeBytes, remote.maxReceiveEnvelopeBytes),
		streamChunkLimit:        min(local.streamChunkLimit, remote.streamChunkLimit),
	}
}

// checkBrokerCeilings refuses a zero, below-floor, or above-ceiling
// advertisement. Envelope bounds are the 1 MiB..16 MiB advertised window;
// chunk bounds are 1..64 KiB.
func checkBrokerCeilings(remote brokerCeilings) error {
	if remote.maxReceiveEnvelopeBytes < MinBrokerEnvelopeBytes ||
		remote.maxReceiveEnvelopeBytes > MaxBrokerEnvelopeBytes {
		return ErrPreambleRejected
	}
	if remote.streamChunkLimit < MinStreamChunkBytes ||
		remote.streamChunkLimit > MaxStreamChunkBytes {
		return ErrPreambleRejected
	}
	return nil
}

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
