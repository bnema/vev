package daemonmux

// Daemonmux physical preamble (P3.2).
//
// The first daemonmux client frame on a physical connection is always
// MuxPreambleRequest; the first daemon frame is always MuxPreambleResponse.
// The preamble is an adapter message pair, not an envelope variant. It
// reuses the shared session preamble magic, epoch, and exact protocol
// version, and the shared refusal taxonomy, with daemonmux-specific roles: 5
// names the broker client, 6 names the daemon server.
//
// The preamble negotiates immutable per-connection ceilings once: the
// receive envelope ceiling (1 MiB..16 MiB), the stream chunk ceiling
// (1..64 KiB), the concurrent stream ceiling (1..128), and the aggregate
// buffered-bytes ceiling (1..64 MiB). The response carries the effective
// minimum of both advertisements, the authenticated daemon identity, its
// 16-byte incarnation, and the accepted connection policy. The effective
// minima are the exported MuxCeilings value (limits.go) that
// NewStreamEngineWithCeilings and NewSchedulerWithCeilings consume. Capability
// bits must be zero: daemonmux negotiates no optional capabilities.
//
// Refusals carry the precise shared PreambleRejectionCode taxonomy: 1 bad
// magic, 2 epoch mismatch, 3 version mismatch, 4 wrong role, 5 duplicate, 6
// out of order, 7 limit refused. Code 5 means a duplicate observed inside one
// frame: the strict scanner found a repeated singular field or oneof member
// (wire.ErrScanDuplicate) in the serialized preamble this codec decodes. The
// connection dispatcher separately refuses a second preamble frame on one
// connection, also with code 5, but that stateful, cross-frame duplicate is
// never observed here. Code 6 is reserved for the dispatcher too: out-of-order
// application frames are a framing-order failure this stateless codec never
// sees. A refused policy is also a code 7 refusal, because daemonmux negotiates
// exact policy equality rather than a capability set.
//
// A response is either an acceptance or a refusal and never a blend of the
// two: an acceptance carries no rejection field, and a refusal carries none of
// the accepted-only authority fields (accepted policy, daemon identity,
// incarnation) and a rejection code in 1..7. Both directions are enforced on
// encode and decode so a peer can never smuggle accepted authority into a
// refusal or a stale refusal code into an acceptance.

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// ErrPreambleRejected reports a daemonmux preamble that fails validation:
// bad magic, epoch, version, role, capability bits, limit advertisement,
// policy, identity, or incarnation; a refusal code outside 1..7; an
// acceptance that also carries a rejection field; or a refusal that also
// carries accepted-only authority. Callers map it to a precise rejection code
// with RejectionCodeFor.
var ErrPreambleRejected = errors.New("daemonmux: preamble rejected")

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
	// RejectionDuplicate reports a duplicate within one frame: the strict
	// scanner found a repeated singular field or oneof member
	// (wire.ErrScanDuplicate) in the serialized preamble. The connection
	// dispatcher separately refuses a second preamble frame on one connection
	// with this same code; that stateful, cross-frame duplicate is never
	// observed by this stateless codec.
	RejectionDuplicate uint32 = 5
	// RejectionOutOfOrder is reserved for the connection dispatcher, which
	// owns framing-order detection. The stateless codec never emits it, but it
	// accepts it as a well-formed refusal code on the wire.
	RejectionOutOfOrder uint32 = 6
	// RejectionLimitRefused reports an unnegotiable advertisement. It also
	// covers nonzero capability bits and a policy the daemon does not
	// accept: P3.2 negotiates neither capabilities nor a merged policy
	// server, so either is an unnegotiable advertisement.
	RejectionLimitRefused uint32 = 7
)

// validRejectionCode reports whether code is one of the shared preamble
// refusal codes 1..7. The stateless codec accepts code 6 (out of order) as a
// well-formed code on the wire even though it never emits it, because the
// dispatcher owns framing-order detection.
func validRejectionCode(code uint32) bool {
	return code >= RejectionBadMagic && code <= RejectionLimitRefused
}

// MuxPreambleRequest is the validated daemonmux client preamble: the
// requested ceilings plus the broker policy the client asks the daemon to
// accept.
type MuxPreambleRequest struct {
	Ceilings MuxCeilings
	Policy   ports.BrokerPolicy
}

// MuxPreambleResponse is the validated daemonmux server preamble: the
// effective ceilings, the authenticated daemon identity and incarnation, and
// the accepted policy on acceptance; a precise refusal code otherwise.
type MuxPreambleResponse struct {
	Ceilings    MuxCeilings
	Policy      ports.BrokerPolicy
	Identity    ports.BrokerDaemonIdentity
	Incarnation ports.BrokerDaemonIncarnation
	Accepted    bool
	Code        uint32
}

// EncodePreambleRequest builds the local daemonmux client preamble offer. It
// fails closed on a ceiling advertisement outside its window or a policy that
// could not be accepted.
func EncodePreambleRequest(ceilings MuxCeilings, policy ports.BrokerPolicy) (*wire.MuxPreambleRequest, error) {
	if err := checkMuxCeilings(ceilings); err != nil {
		return nil, err
	}
	if err := policy.Validate(); err != nil {
		return nil, ErrPreambleRejected
	}
	return &wire.MuxPreambleRequest{
		Magic:                   wire.PreambleMagic,
		Epoch:                   wire.ProtocolEpoch,
		Version:                 uint32(protocol.Version),
		Role:                    &wire.PreambleRole{Role: MuxRoleClient},
		MaxReceiveEnvelopeBytes: ceilings.MaxReceiveEnvelopeBytes,
		StreamChunkLimit:        ceilings.StreamChunkLimit,
		MaxStreams:              ceilings.MaxStreams,
		MaxAggregateBytes:       ceilings.MaxAggregateBytes,
		Policy:                  policyToWire(policy),
	}, nil
}

// EncodePreambleResponse builds the daemonmux server preamble answer:
// acceptance with the effective ceilings, the authenticated daemon identity
// and incarnation, and the accepted policy, or a typed refusal carrying the
// precise rejection code. The answer is either an acceptance or a refusal and
// never a blend: a refusal demands a code in 1..7 and carries no accepted
// authority, while an acceptance demands a zero code and never carries a
// rejection field.
func EncodePreambleResponse(accepted bool, ceilings MuxCeilings, policy ports.BrokerPolicy, identity ports.BrokerDaemonIdentity, incarnation ports.BrokerDaemonIncarnation, code uint32) (*wire.MuxPreambleResponse, error) {
	response := &wire.MuxPreambleResponse{
		Magic:                   wire.PreambleMagic,
		Epoch:                   wire.ProtocolEpoch,
		Version:                 uint32(protocol.Version),
		Role:                    &wire.PreambleRole{Role: MuxRoleServer},
		MaxReceiveEnvelopeBytes: ceilings.MaxReceiveEnvelopeBytes,
		StreamChunkLimit:        ceilings.StreamChunkLimit,
		MaxStreams:              ceilings.MaxStreams,
		MaxAggregateBytes:       ceilings.MaxAggregateBytes,
		Accepted:                accepted,
	}
	if !accepted {
		if !validRejectionCode(code) {
			return nil, ErrPreambleRejected
		}
		response.Rejection = &wire.PreambleRejectionCode{Code: code}
		return response, nil
	}
	if code != 0 {
		// An acceptance carries no rejection field; a nonzero code is a
		// caller bug that must fail closed rather than be dropped silently.
		return nil, ErrPreambleRejected
	}
	if err := checkMuxCeilings(ceilings); err != nil {
		return nil, err
	}
	if err := policy.Validate(); err != nil {
		return nil, ErrPreambleRejected
	}
	if err := identity.Validate(); err != nil {
		return nil, ErrPreambleRejected
	}
	if err := incarnation.Validate(); err != nil {
		return nil, ErrPreambleRejected
	}
	response.AcceptedPolicy = policyToWire(policy)
	response.DaemonIdentity = string(identity)
	response.Incarnation = append([]byte(nil), incarnation[:]...)
	return response, nil
}

// DecodePreambleRequest validates one raw daemonmux client preamble. Magic,
// epoch, version, role, and zero capability bits are checked before the
// ceiling advertisement and the requested policy; the refusal code is
// precise.
func DecodePreambleRequest(message *wire.MuxPreambleRequest) (MuxPreambleRequest, error) {
	if message == nil {
		return MuxPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetMagic() != wire.PreambleMagic {
		return MuxPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetEpoch() != wire.ProtocolEpoch {
		return MuxPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetVersion() != uint32(protocol.Version) {
		return MuxPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetRole().GetRole() != MuxRoleClient {
		return MuxPreambleRequest{}, ErrPreambleRejected
	}
	if message.GetCapabilityBits() != 0 {
		return MuxPreambleRequest{}, ErrPreambleRejected
	}
	ceilings := MuxCeilings{
		MaxReceiveEnvelopeBytes: message.GetMaxReceiveEnvelopeBytes(),
		StreamChunkLimit:        message.GetStreamChunkLimit(),
		MaxStreams:              message.GetMaxStreams(),
		MaxAggregateBytes:       message.GetMaxAggregateBytes(),
	}
	if err := checkMuxCeilings(ceilings); err != nil {
		return MuxPreambleRequest{}, err
	}
	policy, err := policyFromWire(message.GetPolicy())
	if err != nil {
		return MuxPreambleRequest{}, ErrPreambleRejected
	}
	return MuxPreambleRequest{Ceilings: ceilings, Policy: policy}, nil
}

// DecodePreambleResponse validates one raw daemonmux server preamble.
// Refusals fail with ErrPreambleRejected and carry their code; acceptance
// additionally validates the server role, the effective ceilings, the
// accepted policy, the authenticated daemon identity, and the 16-byte
// incarnation. An acceptance carrying a rejection field and a refusal carrying
// accepted-only authority are both rejected, as is any refusal code outside
// 1..7.
func DecodePreambleResponse(message *wire.MuxPreambleResponse) (MuxPreambleResponse, error) {
	if message == nil {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetMagic() != wire.PreambleMagic {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetEpoch() != wire.ProtocolEpoch {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetVersion() != uint32(protocol.Version) {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetRole().GetRole() != MuxRoleServer {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	if message.GetCapabilityBits() != 0 {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	if !message.GetAccepted() {
		code := message.GetRejection().GetCode()
		if !validRejectionCode(code) {
			return MuxPreambleResponse{}, ErrPreambleRejected
		}
		if message.GetAcceptedPolicy() != nil || message.GetDaemonIdentity() != "" || len(message.GetIncarnation()) != 0 {
			// A refusal carries no accepted-only authority: accepted policy,
			// daemon identity, and incarnation belong to an acceptance alone.
			return MuxPreambleResponse{}, ErrPreambleRejected
		}
		return MuxPreambleResponse{Code: code}, ErrPreambleRejected
	}
	if message.GetRejection() != nil {
		// An acceptance carries no rejection field.
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	ceilings := MuxCeilings{
		MaxReceiveEnvelopeBytes: message.GetMaxReceiveEnvelopeBytes(),
		StreamChunkLimit:        message.GetStreamChunkLimit(),
		MaxStreams:              message.GetMaxStreams(),
		MaxAggregateBytes:       message.GetMaxAggregateBytes(),
	}
	if err := checkMuxCeilings(ceilings); err != nil {
		return MuxPreambleResponse{}, err
	}
	policy, err := policyFromWire(message.GetAcceptedPolicy())
	if err != nil {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	identity := ports.BrokerDaemonIdentity(message.GetDaemonIdentity())
	if err := identity.Validate(); err != nil {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	var incarnation ports.BrokerDaemonIncarnation
	raw := message.GetIncarnation()
	if len(raw) != len(incarnation) {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	copy(incarnation[:], raw)
	if err := incarnation.Validate(); err != nil {
		return MuxPreambleResponse{}, ErrPreambleRejected
	}
	return MuxPreambleResponse{
		Ceilings:    ceilings,
		Policy:      policy,
		Identity:    identity,
		Incarnation: incarnation,
		Accepted:    true,
	}, nil
}

// ValidatePreambleResponseAgainstOffer refuses an accepted daemonmux response
// whose effective ceilings exceed the ceilings the local side offered. The
// effective value is the element-wise minimum of both advertisements, so a
// conforming daemon never grants more than the requester offered; a response
// above the offer is a protocol violation. A refusal (which carries no
// accepted ceilings), or an invalid advertisement on either side, is refused
// with ErrPreambleRejected. A response strictly below the offer is accepted:
// the daemon may lower any ceiling by its own policy.
func ValidatePreambleResponseAgainstOffer(response MuxPreambleResponse, offered MuxCeilings) error {
	if err := checkMuxCeilings(offered); err != nil {
		return err
	}
	if !response.Accepted {
		return ErrPreambleRejected
	}
	if err := checkMuxCeilings(response.Ceilings); err != nil {
		return err
	}
	if response.Ceilings.MaxReceiveEnvelopeBytes > offered.MaxReceiveEnvelopeBytes ||
		response.Ceilings.StreamChunkLimit > offered.StreamChunkLimit ||
		response.Ceilings.MaxStreams > offered.MaxStreams ||
		response.Ceilings.MaxAggregateBytes > offered.MaxAggregateBytes {
		return ErrPreambleRejected
	}
	return nil
}

// DecodePreambleResponseBytes strictly decodes one serialized daemonmux server
// preamble under the same 4 KiB bound, strict scan, generated unmarshal, and
// semantic decode as DecodePreambleRequestBytes. Carriage failures return the
// precise wire sentinel; oversize payloads and semantic failures return
// ErrPreambleRejected.
func DecodePreambleResponseBytes(payload []byte) (MuxPreambleResponse, error) {
	if err := CheckPreambleSize(payload); err != nil {
		return MuxPreambleResponse{}, err
	}
	message := &wire.MuxPreambleResponse{}
	if err := wire.ScanEnvelope(message, payload); err != nil {
		return MuxPreambleResponse{}, err
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, message)); err != nil {
		return MuxPreambleResponse{}, err
	}
	return DecodePreambleResponse(message)
}

// RejectionCodeFor maps a preamble validation failure to its precise refusal
// code. Limit, magic, epoch, version, role, and policy failures each carry
// their own code; nonzero capability bits are refused as
// RejectionLimitRefused because P3.2 negotiates no capabilities; unknown scan
// failures are limit refusals. A duplicate inside one frame
// (wire.ErrScanDuplicate) maps to RejectionDuplicate with the in-frame meaning
// described above, never the dispatcher's cross-frame duplicate. Code 6 is
// never returned here: the dispatcher owns out-of-order framing.
func RejectionCodeFor(request *wire.MuxPreambleRequest, err error) uint32 {
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
		if request.GetRole().GetRole() != MuxRoleClient {
			return RejectionWrongRole
		}
		if request.GetCapabilityBits() != 0 {
			// No capability is negotiable in P3.2, so any bit is a refused
			// limit advertisement (code 7), documented explicitly.
			return RejectionLimitRefused
		}
		remote := MuxCeilings{
			MaxReceiveEnvelopeBytes: request.GetMaxReceiveEnvelopeBytes(),
			StreamChunkLimit:        request.GetStreamChunkLimit(),
			MaxStreams:              request.GetMaxStreams(),
			MaxAggregateBytes:       request.GetMaxAggregateBytes(),
		}
		if checkMuxCeilings(remote) != nil {
			return RejectionLimitRefused
		}
		if _, err := policyFromWire(request.GetPolicy()); err != nil {
			// A malformed or unnegotiable requested policy is a refused
			// advertisement, not a transport failure.
			return RejectionLimitRefused
		}
	}
	if errors.Is(err, wire.ErrScanDuplicate) {
		return RejectionDuplicate
	}
	return RejectionLimitRefused
}
