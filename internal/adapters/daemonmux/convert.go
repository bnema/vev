package daemonmux

// Shared semantic converters (P3.2).
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
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// muxEnum8 narrows one wire uint32 into an 8-bit semantic enum. Values whose
// high bits would otherwise truncate into a valid enum range are refused
// before any cast.
func muxEnum8[T ~uint8](value uint32) (T, error) {
	if value > math.MaxUint8 {
		return 0, errConvertRange
	}
	return T(value), nil
}

// muxEnum16 narrows one wire uint32 into a 16-bit semantic value.
func muxEnum16[T ~uint16](value uint32) (T, error) {
	if value > math.MaxUint16 {
		return 0, errConvertRange
	}
	return T(value), nil
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

// registrationToWire mirrors domain.RemoteRegistration: an endpoint, an exact
// 16-byte incarnation, and a generation.
func registrationToWire(registration domain.RemoteRegistration) *wire.RemoteRegistration {
	incarnation := append([]byte(nil), registration.Incarnation[:]...)
	return &wire.RemoteRegistration{
		Endpoint:    registration.Endpoint,
		Incarnation: incarnation,
		Generation:  uint64(registration.Generation),
	}
}

func registrationFromWire(message *wire.RemoteRegistration) (domain.RemoteRegistration, error) {
	var registration domain.RemoteRegistration
	if message == nil {
		return registration, errConvertRange
	}
	registration.Endpoint = message.GetEndpoint()
	if err := domain.ValidateRemoteHostTarget(registration.Endpoint); err != nil {
		return domain.RemoteRegistration{}, errConvertRange
	}
	raw := message.GetIncarnation()
	if len(raw) != len(registration.Incarnation) {
		return domain.RemoteRegistration{}, errConvertRange
	}
	copy(registration.Incarnation[:], raw)
	registration.Generation = domain.RemoteGeneration(message.GetGeneration())
	if err := registration.Validate(); err != nil {
		return domain.RemoteRegistration{}, errConvertRange
	}
	return registration, nil
}

func exactTargetToWire(target protocol.ExactSessionTarget) *wire.ExactTarget {
	lifecycle := append([]byte(nil), target.LifecycleID[:]...)
	return &wire.ExactTarget{
		LifecycleId: &wire.LifecycleID{Value: lifecycle},
		SessionName: target.SessionName,
	}
}

func exactTargetFromWire(message *wire.ExactTarget) (protocol.ExactSessionTarget, error) {
	var target protocol.ExactSessionTarget
	if message == nil {
		return target, errConvertRange
	}
	lifecycle := message.GetLifecycleId()
	if lifecycle == nil || len(lifecycle.GetValue()) != len(target.LifecycleID) {
		return protocol.ExactSessionTarget{}, errConvertRange
	}
	copy(target.LifecycleID[:], lifecycle.GetValue())
	target.SessionName = message.GetSessionName()
	if err := target.Validate(); err != nil {
		return protocol.ExactSessionTarget{}, errConvertRange
	}
	return target, nil
}

func policyToWire(policy ports.BrokerPolicy) *wire.BrokerWirePolicy {
	return &wire.BrokerWirePolicy{
		ProtocolVersion:   uint32(policy.ProtocolVersion),
		CatalogueVersion:  uint32(policy.CatalogSchemaVersion),
		EnvironmentPolicy: uint32(policy.EnvironmentPolicy),
		Transport:         policy.Transport,
		Trust:             policy.Trust,
		Launch:            policy.Launch,
		Isolation:         policy.Isolation,
	}
}

func policyFromWire(message *wire.BrokerWirePolicy) (ports.BrokerPolicy, error) {
	var policy ports.BrokerPolicy
	if message == nil {
		return policy, errConvertRange
	}
	protocolVersion, err := muxEnum16[uint16](message.GetProtocolVersion())
	if err != nil {
		return ports.BrokerPolicy{}, err
	}
	catalogueVersion, err := muxEnum16[uint16](message.GetCatalogueVersion())
	if err != nil {
		return ports.BrokerPolicy{}, err
	}
	env, err := muxEnum8[protocol.EnvironmentPolicy](message.GetEnvironmentPolicy())
	if err != nil {
		return ports.BrokerPolicy{}, err
	}
	policy = ports.BrokerPolicy{
		ProtocolVersion:      protocolVersion,
		CatalogSchemaVersion: catalogueVersion,
		EnvironmentPolicy:    env,
		Transport:            message.GetTransport(),
		Trust:                message.GetTrust(),
		Launch:               message.GetLaunch(),
		Isolation:            message.GetIsolation(),
	}
	if err := policy.Validate(); err != nil {
		return ports.BrokerPolicy{}, ErrInvalidMessage
	}
	return policy, nil
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

// validateDisplayText enforces bounded, presentation-safe text: valid UTF-8,
// no control/bidi/line-separator characters, within maxBytes.
func validateDisplayText(value string, maxBytes int) error {
	if len(value) > maxBytes {
		return ErrTooLarge
	}
	if !utf8.ValidString(value) {
		return ErrInvalidMessage
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) || r == '\u2028' || r == '\u2029' {
			return ErrInvalidMessage
		}
	}
	return nil
}

// validateEnvEntry enforces the per-request environment contract: valid
// UTF-8, no control/bidi characters, bounded bytes. The daemonmux client
// never merges these into the broker or daemon process environment.
func validateEnvEntry(entry string) error {
	if len(entry) > ports.BrokerMaxEnvEntryBytes {
		return ErrTooLarge
	}
	if !utf8.ValidString(entry) {
		return ErrInvalidMessage
	}
	for _, r := range entry {
		if unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) || r == '\u2028' || r == '\u2029' {
			return ErrInvalidMessage
		}
	}
	return nil
}

// checkEnvEntries enforces the per-request environment bound: at most
// BrokerMaxEnvEntries entries, each valid, bounded, and an explicit
// KEY=VALUE assignment.
func checkEnvEntries(env []string) error {
	if len(env) > ports.BrokerMaxEnvEntries {
		return ErrTooLarge
	}
	for _, entry := range env {
		if err := validateEnvEntry(entry); err != nil {
			return err
		}
		if !strings.Contains(entry, "=") {
			return ErrInvalidMessage
		}
	}
	return nil
}
