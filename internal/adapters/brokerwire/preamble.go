package brokerwire

// Broker preamble (P3.1).
//
// The first client frame on a broker connection is always PreambleRequest;
// the first server frame is always PreambleResponse. The broker reuses the
// session preamble shapes with broker-specific roles: 3 names the broker
// client, 4 names the broker server. Magic must equal wire.PreambleMagic,
// epoch must equal wire.ProtocolEpoch, and version must exactly equal
// protocol.Version. Capability bits must be zero: the broker negotiates no
// optional capabilities in P3.1.
//
// The preamble negotiates immutable per-connection ceilings once: the
// advertised envelope ceiling (1 MiB..16 MiB) rides in
// max_receive_envelope_bytes, and the stream chunk ceiling (1..64 KiB)
// rides in output_data_limit. Exact version equality is mandatory.
//
// Refusals carry the precise PreambleRejectionCode taxonomy: 1 bad magic,
// 2 epoch mismatch, 3 version mismatch, 4 wrong role, 5 duplicate
// preamble, 7 limit refused. Code 6 is reserved for the connection
// dispatcher: out-of-order application frames are a framing-order failure
// that this stateless codec never observes. Nonzero capability bits have no
// dedicated code in P3.1 and are refused as code 7 (limit refused), since
// the broker negotiates no optional capabilities.

import (
	"errors"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// ErrPreambleRejected reports a broker preamble that fails validation:
// bad magic, epoch, version, role, capability bits, or limit
// advertisement. Callers map it to a precise rejection code with
// RejectionCodeFor.
var ErrPreambleRejected = errors.New("brokerwire: preamble rejected")

// Preamble rejection codes. They mirror the session preamble refusal
// taxonomy carried on wire.PreambleRejectionCode.
const (
	// RejectionBadMagic reports a preamble magic mismatch.
	RejectionBadMagic uint32 = 1
	// RejectionEpochMismatch reports a protocol epoch mismatch.
	RejectionEpochMismatch uint32 = 2
	// RejectionVersionMismatch reports a session protocol version mismatch.
	RejectionVersionMismatch uint32 = 3
	// RejectionWrongRole reports a preamble role mismatch.
	RejectionWrongRole uint32 = 4
	// RejectionDuplicate reports a second preamble on one connection.
	RejectionDuplicate uint32 = 5
	// RejectionOutOfOrder is reserved for the connection dispatcher, which
	// owns framing-order detection. The stateless codec never emits it.
	RejectionOutOfOrder uint32 = 6
	// RejectionLimitRefused reports an unnegotiable limit advertisement. It
	// also covers nonzero capability bits: P3.1 negotiates no optional
	// capabilities, so any capability bit is an unnegotiable advertisement.
	RejectionLimitRefused uint32 = 7
)

// BrokerPreambleRequest is the validated broker client preamble: the
// negotiated ceilings plus the raw wire message for stateless re-encode.
type BrokerPreambleRequest struct {
	Ceilings brokerCeilings
}

// BrokerPreambleResponse is the validated broker server preamble.
type BrokerPreambleResponse struct {
	Ceilings brokerCeilings
	Accepted bool
	Code     uint32
}

// EncodePreambleRequest builds the local broker client preamble offer.
func EncodePreambleRequest(ceilings brokerCeilings) *wire.PreambleRequest {
	return &wire.PreambleRequest{
		Magic:                   wire.PreambleMagic,
		Epoch:                   wire.ProtocolEpoch,
		Version:                 uint32(protocol.Version),
		Role:                    &wire.PreambleRole{Role: BrokerRoleClient},
		MaxReceiveEnvelopeBytes: ceilings.maxReceiveEnvelopeBytes,
		OutputDataLimit:         ceilings.streamChunkLimit,
	}
}

// EncodePreambleResponse builds the broker server preamble answer:
// acceptance with the effective ceilings, or a typed refusal carrying the
// precise rejection code.
func EncodePreambleResponse(accepted bool, ceilings brokerCeilings, code uint32) *wire.PreambleResponse {
	response := &wire.PreambleResponse{
		Magic:                   wire.PreambleMagic,
		Epoch:                   wire.ProtocolEpoch,
		Version:                 uint32(protocol.Version),
		Role:                    &wire.PreambleRole{Role: BrokerRoleServer},
		MaxReceiveEnvelopeBytes: ceilings.maxReceiveEnvelopeBytes,
		OutputDataLimit:         ceilings.streamChunkLimit,
		Accepted:                accepted,
	}
	if !accepted {
		response.Rejection = &wire.PreambleRejectionCode{Code: code}
	}
	return response
}

// DecodePreambleRequest validates one raw broker client preamble. Magic,
// epoch, version, role, and zero capability bits are checked before the
// limit advertisement; the refusal code is precise.
func DecodePreambleRequest(message *wire.PreambleRequest) (BrokerPreambleRequest, error) {
	if message == nil {
		return BrokerPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetMagic() != wire.PreambleMagic {
		return BrokerPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetEpoch() != wire.ProtocolEpoch {
		return BrokerPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetVersion() != uint32(protocol.Version) {
		return BrokerPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetRole().GetRole() != BrokerRoleClient {
		return BrokerPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetCapabilityBits() != 0 {
		return BrokerPreambleRequest{}, ErrPreambleRejected
	}
	remote := brokerCeilings{
		maxReceiveEnvelopeBytes: message.GetMaxReceiveEnvelopeBytes(),
		streamChunkLimit:        message.GetOutputDataLimit(),
	}
	if err := checkBrokerCeilings(remote); err != nil {
		return BrokerPreambleRequest{}, err
	}
	return BrokerPreambleRequest{Ceilings: remote}, nil
}

// DecodePreambleResponse validates one raw broker server preamble.
// Refusals fail with ErrPreambleRejected; acceptance additionally
// validates the server role and its limit advertisement.
func DecodePreambleResponse(message *wire.PreambleResponse) (BrokerPreambleResponse, error) {
	if message == nil {
		return BrokerPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetMagic() != wire.PreambleMagic {
		return BrokerPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetEpoch() != wire.ProtocolEpoch {
		return BrokerPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetVersion() != uint32(protocol.Version) {
		return BrokerPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetRole().GetRole() != BrokerRoleServer {
		return BrokerPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetCapabilityBits() != 0 {
		return BrokerPreambleResponse{}, ErrPreambleRejected
	}
	if !message.GetAccepted() {
		return BrokerPreambleResponse{Code: message.GetRejection().GetCode()}, ErrPreambleRejected
	}
	remote := brokerCeilings{
		maxReceiveEnvelopeBytes: message.GetMaxReceiveEnvelopeBytes(),
		streamChunkLimit:        message.GetOutputDataLimit(),
	}
	if err := checkBrokerCeilings(remote); err != nil {
		return BrokerPreambleResponse{}, err
	}
	return BrokerPreambleResponse{Ceilings: remote, Accepted: true}, nil
}

// RejectionCodeFor maps a preamble validation failure to its precise
// refusal code. Limit, magic, epoch, version, and role failures each carry
// their own code; nonzero capability bits are refused as
// RejectionLimitRefused because P3.1 negotiates no capabilities; unknown
// scan failures are limit refusals. Code 6 is never returned here: the
// dispatcher owns out-of-order framing.
func RejectionCodeFor(request *wire.PreambleRequest, err error) uint32 {
	if err == nil {
		return 0
	}
	if request != nil {
		if request.GetMagic() != wire.PreambleMagic {
			return RejectionBadMagic
		}
		if request.GetEpoch() != wire.ProtocolEpoch {
			return RejectionEpochMismatch
		}
		if request.GetVersion() != uint32(protocol.Version) {
			return RejectionVersionMismatch
		}
		if request.GetRole().GetRole() != BrokerRoleClient {
			return RejectionWrongRole
		}
		if request.GetCapabilityBits() != 0 {
			// No capability is negotiable in P3.1, so any bit is a refused
			// limit advertisement (code 7), documented explicitly.
			return RejectionLimitRefused
		}
		remote := brokerCeilings{
			maxReceiveEnvelopeBytes: request.GetMaxReceiveEnvelopeBytes(),
			streamChunkLimit:        request.GetOutputDataLimit(),
		}
		if checkBrokerCeilings(remote) != nil {
			return RejectionLimitRefused
		}
	}
	if errors.Is(err, wire.ErrScanDuplicate) {
		return RejectionDuplicate
	}
	return RejectionLimitRefused
}
