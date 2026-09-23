package protoconv

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// ErrInvalid and ErrTooLarge distinguish semantic and byte-bound failures
// from ErrOutOfRange; adapters translate them to their local sentinels.
var (
	ErrInvalid  = errors.New("protoconv: invalid broker value")
	ErrTooLarge = errors.New("protoconv: broker value exceeds bound")
)

func BrokerStartModeToWire(mode ports.BrokerDaemonStartMode) (uint32, error) {
	switch mode {
	case ports.BrokerDaemonExistingOnly:
		return 1, nil
	case ports.BrokerDaemonStartIfNeeded:
		return 2, nil
	default:
		return 0, ErrOutOfRange
	}
}

func BrokerStartModeFromWire(value uint32) (ports.BrokerDaemonStartMode, error) {
	switch value {
	case 1:
		return ports.BrokerDaemonExistingOnly, nil
	case 2:
		return ports.BrokerDaemonStartIfNeeded, nil
	default:
		return 0, ErrOutOfRange
	}
}

func BrokerRegistrationToWire(registration domain.RemoteRegistration) *wire.RemoteRegistration {
	return &wire.RemoteRegistration{Endpoint: registration.Endpoint, Incarnation: append([]byte(nil), registration.Incarnation[:]...), Generation: uint64(registration.Generation)}
}

func BrokerRegistrationFromWire(message *wire.RemoteRegistration) (domain.RemoteRegistration, error) {
	var registration domain.RemoteRegistration
	if message == nil {
		return registration, ErrOutOfRange
	}
	registration.Endpoint = message.GetEndpoint()
	if err := domain.ValidateRemoteHostTarget(registration.Endpoint); err != nil {
		return domain.RemoteRegistration{}, ErrOutOfRange
	}
	raw := message.GetIncarnation()
	if len(raw) != len(registration.Incarnation) {
		return domain.RemoteRegistration{}, ErrOutOfRange
	}
	copy(registration.Incarnation[:], raw)
	registration.Generation = domain.RemoteGeneration(message.GetGeneration())
	if err := registration.Validate(); err != nil {
		return domain.RemoteRegistration{}, ErrOutOfRange
	}
	return registration, nil
}

func BrokerExactTargetToWire(target protocol.ExactSessionTarget) *wire.ExactTarget {
	return &wire.ExactTarget{LifecycleId: &wire.LifecycleID{Value: append([]byte(nil), target.LifecycleID[:]...)}, SessionName: target.SessionName}
}

func BrokerExactTargetFromWire(message *wire.ExactTarget) (protocol.ExactSessionTarget, error) {
	var target protocol.ExactSessionTarget
	if message == nil {
		return target, ErrOutOfRange
	}
	lifecycle := message.GetLifecycleId()
	if lifecycle == nil || len(lifecycle.GetValue()) != len(target.LifecycleID) {
		return protocol.ExactSessionTarget{}, ErrOutOfRange
	}
	copy(target.LifecycleID[:], lifecycle.GetValue())
	target.SessionName = message.GetSessionName()
	if err := target.Validate(); err != nil {
		return protocol.ExactSessionTarget{}, ErrOutOfRange
	}
	return target, nil
}

func BrokerPolicyToWire(policy ports.BrokerPolicy) *wire.BrokerWirePolicy {
	return &wire.BrokerWirePolicy{ProtocolVersion: uint32(policy.ProtocolVersion), CatalogueVersion: uint32(policy.CatalogSchemaVersion), EnvironmentPolicy: uint32(policy.EnvironmentPolicy), Transport: policy.Transport, Trust: policy.Trust, Launch: policy.Launch, Isolation: policy.Isolation}
}

func BrokerPolicyFromWire(message *wire.BrokerWirePolicy) (ports.BrokerPolicy, error) {
	if message == nil {
		return ports.BrokerPolicy{}, ErrOutOfRange
	}
	version, err := Uint16[uint16](message.GetProtocolVersion())
	if err != nil {
		return ports.BrokerPolicy{}, err
	}
	catalogue, err := Uint16[uint16](message.GetCatalogueVersion())
	if err != nil {
		return ports.BrokerPolicy{}, err
	}
	env, err := Uint8[protocol.EnvironmentPolicy](message.GetEnvironmentPolicy())
	if err != nil {
		return ports.BrokerPolicy{}, err
	}
	policy := ports.BrokerPolicy{ProtocolVersion: version, CatalogSchemaVersion: catalogue, EnvironmentPolicy: env, Transport: message.GetTransport(), Trust: message.GetTrust(), Launch: message.GetLaunch(), Isolation: message.GetIsolation()}
	if err := policy.Validate(); err != nil {
		return ports.BrokerPolicy{}, ErrInvalid
	}
	return policy, nil
}

func BrokerDisplayText(value string, maxBytes int) error {
	if len(value) > maxBytes {
		return ErrTooLarge
	}
	if !utf8.ValidString(value) {
		return ErrInvalid
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) || r == '\u2028' || r == '\u2029' {
			return ErrInvalid
		}
	}
	return nil
}

func BrokerEnvEntry(entry string) error {
	return BrokerDisplayText(entry, ports.BrokerMaxEnvEntryBytes)
}

func BrokerEnvEntries(env []string) error {
	if len(env) > ports.BrokerMaxEnvEntries {
		return ErrTooLarge
	}
	for _, entry := range env {
		if err := BrokerEnvEntry(entry); err != nil {
			return err
		}
		if !strings.Contains(entry, "=") {
			return ErrInvalid
		}
	}
	return nil
}
