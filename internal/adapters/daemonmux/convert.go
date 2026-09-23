package daemonmux

// Shared semantic converters.
//
// Conversion mirrors the port, domain, and protocol types losslessly and
// validates every narrowing numeric cast before it happens. Wire uint32
// taxonomy fields never truncate into a smaller semantic enum: values whose
// high bits would alias a legitimate code are refused with errConvertRange
// before any cast. Identity lengths are exact (16-byte connection,
// incarnation, and lifecycle values); text fields enforce UTF-8,
// control/bidi, and byte bounds.
//
// The daemonmux codec deliberately imports no brokerwire symbol: it reuses
// the generated shared wire messages and the ports/domain semantic values
// directly, so the two conversations stay independently evolvable.

import (
	"github.com/bnema/vev/internal/adapters/protoconv"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// muxEnum8 maps shared narrowing failures to the local range sentinel.
func muxEnum8[T ~uint8](value uint32) (T, error) {
	v, err := protoconv.Uint8[T](value)
	if err != nil {
		return 0, errConvertRange
	}
	return v, nil
}

func startModeToWire(mode ports.BrokerDaemonStartMode) (uint32, error) {
	v, err := protoconv.BrokerStartModeToWire(mode)
	if err != nil {
		return 0, errConvertRange
	}
	return v, nil
}

func startModeFromWire(value uint32) (ports.BrokerDaemonStartMode, error) {
	v, err := protoconv.BrokerStartModeFromWire(value)
	if err != nil {
		return 0, errConvertRange
	}
	return v, nil
}

func physicalToWire(id PhysicalStreamID) uint64 { return uint64(id) }

func physicalFromWire(value uint64) (PhysicalStreamID, error) {
	id := PhysicalStreamID(value)
	if err := id.Validate(); err != nil {
		return 0, errConvertRange
	}
	return id, nil
}

func refToWire(ref StreamRef) *wire.MuxStreamRef {
	return &wire.MuxStreamRef{
		PhysicalStreamId: physicalToWire(ref.Physical),
		BrokerEpoch:      uint64(ref.Epoch),
		ConnectionId:     append([]byte(nil), ref.Connection[:]...),
		ClientStreamId:   uint64(ref.Client),
	}
}

func refFromWire(message *wire.MuxStreamRef) (StreamRef, error) {
	var ref StreamRef
	if message == nil {
		return ref, errConvertRange
	}
	physical, err := physicalFromWire(message.GetPhysicalStreamId())
	if err != nil {
		return StreamRef{}, err
	}
	if message.GetBrokerEpoch() == 0 {
		return StreamRef{}, errConvertRange
	}
	raw := message.GetConnectionId()
	if len(raw) != len(ref.Connection) {
		return StreamRef{}, errConvertRange
	}
	copy(ref.Connection[:], raw)
	if err := ref.Connection.Validate(); err != nil {
		return StreamRef{}, errConvertRange
	}
	client := ports.BrokerStreamID(message.GetClientStreamId())
	if err := client.Validate(); err != nil {
		return StreamRef{}, errConvertRange
	}
	return StreamRef{
		Physical:   physical,
		Epoch:      ports.BrokerEpoch(message.GetBrokerEpoch()),
		Connection: ref.Connection,
		Client:     client,
	}, nil
}

func registrationToWire(v domain.RemoteRegistration) *wire.RemoteRegistration {
	return protoconv.BrokerRegistrationToWire(v)
}
func registrationFromWire(v *wire.RemoteRegistration) (domain.RemoteRegistration, error) {
	out, err := protoconv.BrokerRegistrationFromWire(v)
	if err != nil {
		return domain.RemoteRegistration{}, errConvertRange
	}
	return out, nil
}

func exactTargetToWire(v protocol.ExactSessionTarget) *wire.ExactTarget {
	return protoconv.BrokerExactTargetToWire(v)
}

func exactTargetFromWire(v *wire.ExactTarget) (protocol.ExactSessionTarget, error) {
	out, err := protoconv.BrokerExactTargetFromWire(v)
	if err != nil {
		return protocol.ExactSessionTarget{}, errConvertRange
	}
	return out, nil
}

func policyToWire(v ports.BrokerPolicy) *wire.BrokerWirePolicy {
	return protoconv.BrokerPolicyToWire(v)
}

func policyFromWire(v *wire.BrokerWirePolicy) (ports.BrokerPolicy, error) {
	out, err := protoconv.BrokerPolicyFromWire(v)
	if err != nil {
		return ports.BrokerPolicy{}, mapBrokerConversionError(err)
	}
	return out, nil
}

func mapBrokerConversionError(err error) error {
	switch err {
	case protoconv.ErrOutOfRange:
		return errConvertRange
	case protoconv.ErrTooLarge:
		return ErrTooLarge
	default:
		return ErrInvalidMessage
	}
}

func validateDisplayText(v string, max int) error {
	return mapOptionalBrokerError(protoconv.BrokerDisplayText(v, max))
}

func mapOptionalBrokerError(err error) error {
	if err == nil {
		return nil
	}
	return mapBrokerConversionError(err)
}

func errorDetailToWire(detail ErrorDetail) *wire.BrokerErrorDetail {
	return &wire.BrokerErrorDetail{
		Code:          uint32(detail.Code),
		Text:          detail.Text,
		AdmissionCode: detail.AdmissionCode,
		FailureKind:   uint32(detail.FailureKind),
	}
}

func errorDetailFromWire(message *wire.BrokerErrorDetail) (ErrorDetail, error) {
	var detail ErrorDetail
	if message == nil {
		return detail, errConvertRange
	}
	code, err := muxEnum8[ports.BrokerErrorCode](message.GetCode())
	if err != nil {
		return ErrorDetail{}, err
	}
	if err := code.Validate(); err != nil {
		return ErrorDetail{}, ErrInvalidMessage
	}
	detail.Code = code
	if err := validateDisplayText(message.GetText(), ports.BrokerMaxErrorBytes); err != nil {
		return ErrorDetail{}, err
	}
	detail.Text = message.GetText()
	admission := message.GetAdmissionCode()
	if admission > MaxAdmissionCode {
		return ErrorDetail{}, errConvertRange
	}
	detail.AdmissionCode = admission
	kind, err := muxEnum8[domain.RemoteFailureKind](message.GetFailureKind())
	if err != nil {
		return ErrorDetail{}, err
	}
	if kind > MaxFailureKind {
		return ErrorDetail{}, ErrInvalidMessage
	}
	detail.FailureKind = kind
	failure := ports.BrokerError{Code: code, Text: detail.Text}
	if err := failure.Validate(); err != nil {
		return ErrorDetail{}, ErrInvalidMessage
	}
	return detail, nil
}

// checkEnvEntries keeps the local error taxonomy at the adapter boundary.
func checkEnvEntries(env []string) error {
	return mapOptionalBrokerError(protoconv.BrokerEnvEntries(env))
}
