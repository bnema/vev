package daemonmux

// Physical preamble handshake (P3.2a).
//
// The daemonmux connection begins with exactly one physical preamble exchange
// over the private FramedCarrier, before any application envelope. The client
// (broker) sends one MuxPreambleRequest carrying its advertised ceilings and
// the connection policy it needs; the server (daemon) answers with exactly one
// MuxPreambleResponse carrying either the effective (minimum) ceilings, the
// authoritative daemon identity and incarnation, and the accepted policy, or a
// typed refusal. The message pair is the existing stateless preamble codec
// (preamble.go): this file is only the stateful exchange and the authority
// checks on top of it. Magic, epoch, exact protocol.Version, roles 5/6, zero
// capability bits, the 4 KiB preamble bound, and valid/refusal exclusivity are
// enforced by that codec.
//
// The client verifies the accepted response against the independently resolved
// endpoint it dialed: the response identity must exactly equal
// ports.BrokerDialTarget.ExpectedIdentity.Identity, the accepted policy must exactly equal
// the endpoint policy, the incarnation must be nonzero, and the effective
// ceilings must never exceed what the client offered. The response is the
// server's authority, never a value the client supplied and got echoed back.
//
// The server verifies the request policy against its immutable ServerBinding
// (binding.go) instead of echoing the requested policy: an accepted answer
// carries the binding's identity, incarnation, and policy, and any request
// whose policy differs in a single field is refused. The server negotiates the
// element-wise minima of its local advertisement and the request, so a peer
// can never grant or demand more than either side offered.
//
// One caller-supplied context (or its deadline) bounds the whole exchange: the
// same value goes to the carrier's Send and Receive, no timer is created here,
// and there is no internal retry or grace period. Every failed handshake -
// local validation failure, carrier error, peer refusal, identity or policy
// mismatch, or an inadmissible advertisement - closes the carrier before
// returning, so no half-open physical connection survives a refusal. A
// successful handshake leaves the carrier open and untouched for its owner:
// the handshake never starts a Pump, never admits a stream, and never wraps
// the connection in ports values. The caller starts the Pump only after this
// function returns nil.
//
// Authentication premise: the handshake does not authenticate the carrier and
// does not prove an identity in band. The carriage must already be
// authenticated, and the expected endpoint binding is authoritative. The
// identity, incarnation, and policy here are authorization and pooling
// metadata on top of that premise, not a substitute for it.

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

var (
	// ErrHandshakeRejected reports a physical preamble handshake that was
	// refused by the peer or failed a local authority check on an otherwise
	// well-formed response. A peer refusal is reported as a *HandshakeRefusal
	// carrying its precise code and also matches errors.Is(ErrHandshakeRejected).
	ErrHandshakeRejected = errors.New("daemonmux: physical handshake rejected")
	// ErrHandshakeConfig reports a handshake called without a carrier, with an
	// invalid endpoint or binding, or with an invalid local ceiling
	// advertisement. It is a caller bug, not a peer refusal.
	ErrHandshakeConfig = errors.New("daemonmux: invalid handshake configuration")
)

// HandshakeRefusal is the typed refusal a physical preamble handshake returns:
// the precise PreambleRejectionCode the peer sent or the server sent.
type HandshakeRefusal struct {
	Code uint32
}

func (r *HandshakeRefusal) Error() string {
	return fmt.Sprintf("daemonmux: physical handshake refused with code %d", r.Code)
}

// Is classifies every typed refusal as ErrHandshakeRejected so a caller can
// test with errors.Is without losing the precise code carried on the value.
func (r *HandshakeRefusal) Is(target error) bool { return target == ErrHandshakeRejected }

// ClientHandshakeResult is the broker's validated physical handshake outcome:
// the negotiated (effective) ceilings, and the daemon binding the server
// proved against the independently resolved endpoint.
type ClientHandshakeResult struct {
	Ceilings    MuxCeilings
	Identity    ports.BrokerDaemonIdentity
	Incarnation ports.BrokerDaemonIncarnation
	Policy      ports.BrokerPolicy
}

// ServerHandshakeResult is the daemon's validated physical handshake outcome:
// the negotiated (effective) ceilings and the binding the server enforced.
type ServerHandshakeResult struct {
	Ceilings MuxCeilings
	Binding  ServerBinding
	// Policy is the exact immutable authority member admitted for this peer.
	Policy ports.BrokerPolicy
	// Origin is the provisioned locality of the admitted member, carried from
	// the same provisioned entry Policy was resolved from. A downstream listener
	// stamps it on every admitted session connection so a daemon use case can
	// enforce the local/remote locality bound without re-deriving it.
	Origin ports.SessionConnectionOrigin
}

// RunClientHandshake performs the broker side of the daemonmux physical
// preamble over carrier. It offers offered and the resolved endpoint's policy,
// then accepts exactly one response and verifies it against the endpoint:
// identity and policy must match exactly, the incarnation must be nonzero, and
// the effective ceilings must not exceed the offer. It returns a typed
// *HandshakeRefusal for a peer refusal and ErrHandshakeRejected for any local
// authority mismatch. The caller's ctx (or its deadline) bounds both the send
// and the receive. Every failure closes carrier; success leaves it open and
// does not start a pump.
func RunClientHandshake(ctx context.Context, carrier FramedCarrier, endpoint ports.BrokerDialTarget, offered MuxCeilings) (ClientHandshakeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if carrier == nil {
		return ClientHandshakeResult{}, ErrHandshakeConfig
	}
	if err := endpoint.Validate(); err != nil {
		closeFailedHandshake(carrier)
		return ClientHandshakeResult{}, ErrHandshakeConfig
	}
	if err := offered.Validate(); err != nil {
		closeFailedHandshake(carrier)
		return ClientHandshakeResult{}, ErrHandshakeConfig
	}

	request, err := EncodePreambleRequest(offered, endpoint.Policy)
	if err != nil {
		closeFailedHandshake(carrier)
		return ClientHandshakeResult{}, ErrHandshakeConfig
	}
	offer, err := proto.Marshal(request)
	if err != nil {
		closeFailedHandshake(carrier)
		return ClientHandshakeResult{}, errors.Join(ErrHandshakeConfig, err)
	}
	if err := carrier.Send(ctx, offer); err != nil {
		closeFailedHandshake(carrier)
		return ClientHandshakeResult{}, err
	}
	payload, err := carrier.Receive(ctx)
	if err != nil {
		closeFailedHandshake(carrier)
		return ClientHandshakeResult{}, err
	}
	response, err := DecodePreambleResponseBytes(payload)
	if err != nil {
		closeFailedHandshake(carrier)
		if !response.Accepted && response.Code != 0 {
			return ClientHandshakeResult{}, &HandshakeRefusal{Code: response.Code}
		}
		return ClientHandshakeResult{}, ErrHandshakeRejected
	}
	if err := validateClientResponse(response, endpoint, offered); err != nil {
		closeFailedHandshake(carrier)
		return ClientHandshakeResult{}, err
	}
	return ClientHandshakeResult{
		Ceilings:    EffectiveMuxCeilings(offered, response.Ceilings),
		Identity:    response.Identity,
		Incarnation: response.Incarnation,
		Policy:      response.Policy,
	}, nil
}

// validateClientResponse is the broker's authority check on one accepted
// response: identity and policy exactly equal the resolved endpoint, the
// incarnation is nonzero, and the effective ceilings never exceed the offer.
// A non-acceptance is refused too, so a caller can never treat a refusal as an
// accepted empty response.
func validateClientResponse(response MuxPreambleResponse, endpoint ports.BrokerDialTarget, offered MuxCeilings) error {
	if !response.Accepted {
		return ErrHandshakeRejected
	}
	if endpoint.ExpectedIdentity.Bound && response.Identity != endpoint.ExpectedIdentity.Identity {
		return ErrHandshakeRejected
	}
	if err := response.Identity.Validate(); err != nil {
		return ErrHandshakeRejected
	}
	if !response.Policy.Compatible(endpoint.Policy) {
		return ErrHandshakeRejected
	}
	if err := response.Incarnation.Validate(); err != nil {
		return ErrHandshakeRejected
	}
	if err := ValidatePreambleResponseAgainstOffer(response, offered); err != nil {
		return ErrHandshakeRejected
	}
	return nil
}

// RunServerHandshake performs the daemon side of the daemonmux physical
// preamble over carrier. It accepts exactly one request, refuses it with the
// precise rejection code when it is malformed or its policy differs from the
// immutable binding, and otherwise answers with the binding's identity,
// incarnation, and policy over the effective ceilings. The caller's ctx (or
// its deadline) bounds both the receive and the send. Every failure sends the
// typed refusal where possible, closes carrier, and returns a typed
// *HandshakeRefusal; success leaves carrier open and does not start a pump.
func RunServerHandshake(ctx context.Context, carrier FramedCarrier, binding ServerBinding, offered MuxCeilings) (ServerHandshakeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if carrier == nil {
		return ServerHandshakeResult{}, ErrHandshakeConfig
	}
	if err := binding.Validate(); err != nil {
		closeFailedHandshake(carrier)
		return ServerHandshakeResult{}, ErrHandshakeConfig
	}
	if err := offered.Validate(); err != nil {
		closeFailedHandshake(carrier)
		return ServerHandshakeResult{}, ErrHandshakeConfig
	}

	payload, err := carrier.Receive(ctx)
	if err != nil {
		closeFailedHandshake(carrier)
		return ServerHandshakeResult{}, err
	}
	request, code, err := decodePreambleRequestWithCode(payload)
	if err != nil {
		_ = sendHandshakeRefusal(ctx, carrier, offered, code)
		closeFailedHandshake(carrier)
		return ServerHandshakeResult{}, &HandshakeRefusal{Code: code}
	}
	accepted, ok := binding.Accepted(request.Policy)
	if !ok {
		code := uint32(RejectionLimitRefused)
		_ = sendHandshakeRefusal(ctx, carrier, offered, code)
		closeFailedHandshake(carrier)
		return ServerHandshakeResult{}, &HandshakeRefusal{Code: code}
	}

	effective := EffectiveMuxCeilings(offered, request.Ceilings)
	response, err := EncodePreambleResponse(true, effective, accepted.Policy, binding.Identity(), binding.Incarnation(), 0)
	if err != nil {
		closeFailedHandshake(carrier)
		return ServerHandshakeResult{}, ErrHandshakeConfig
	}
	answer, err := proto.Marshal(response)
	if err != nil {
		closeFailedHandshake(carrier)
		return ServerHandshakeResult{}, errors.Join(ErrHandshakeConfig, err)
	}
	if err := carrier.Send(ctx, answer); err != nil {
		closeFailedHandshake(carrier)
		return ServerHandshakeResult{}, err
	}
	return ServerHandshakeResult{Ceilings: effective, Binding: binding, Policy: accepted.Policy, Origin: accepted.Origin}, nil
}

// decodePreambleRequestWithCode is the server's strict request decode paired
// with the precise refusal code for its failure, so a refusal always carries
// the shared PreambleRejectionCode taxonomy rather than a generic limit code.
func decodePreambleRequestWithCode(payload []byte) (MuxPreambleRequest, uint32, error) {
	if err := CheckPreambleSize(payload); err != nil {
		return MuxPreambleRequest{}, RejectionLimitRefused, err
	}
	message := &wire.MuxPreambleRequest{}
	if err := wire.ScanEnvelope(message, payload); err != nil {
		return MuxPreambleRequest{}, RejectionCodeFor(nil, err), err
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, message)); err != nil {
		return MuxPreambleRequest{}, RejectionCodeFor(message, err), err
	}
	request, err := DecodePreambleRequest(message)
	if err != nil {
		return MuxPreambleRequest{}, RejectionCodeFor(message, err), err
	}
	return request, 0, nil
}

// sendHandshakeRefusal best-effort sends one typed refusal. The caller closes
// the carrier immediately afterwards regardless of the send result, so a
// failed refusal write never leaves the connection half-open.
func sendHandshakeRefusal(ctx context.Context, carrier FramedCarrier, offered MuxCeilings, code uint32) error {
	response, err := EncodePreambleResponse(false, offered, ports.BrokerPolicy{}, "", ports.BrokerDaemonIncarnation{}, code)
	if err != nil {
		return err
	}
	answer, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	return carrier.Send(ctx, answer)
}

// closeFailedHandshake closes the carrier on a failed handshake. The close
// error is deliberately dropped: the handshake result is the caller's signal,
// and a carrier that refuses to close must not replace or mask it.
func closeFailedHandshake(carrier FramedCarrier) {
	_ = carrier.Close()
}
