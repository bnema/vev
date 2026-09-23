// Protobuf semantic converters.
//
// This file owns every semantic protocol value <-> generated wire envelope
// conversion used by client.go/server.go dispatch. The actual field-by-field
// conversion logic lives in internal/adapters/protoconv, the single
// canonical implementation shared with brokerwire; the wrappers below adapt
// protoconv's shared ErrOutOfRange (and, for the RemotePreview family,
// protocol's own validation sentinels) to this package's local
// errProtoConvertRange, and add the pointer/nil-tolerance policy specific to
// sessionwire's call sites. Use cases always see uncompressed terminal data.
package sessionwire

import (
	"errors"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/adapters/protoconv"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

var errProtoConvertRange = errors.New("sessionwire: wire value out of semantic range")

func mustUint16(v uint32) (uint16, error) {
	out, err := protoconv.Uint16[uint16](v)
	if err != nil {
		return 0, errProtoConvertRange
	}
	return out, nil
}

func mustUint8(v uint32) (uint8, error) {
	out, err := protoconv.Uint8[uint8](v)
	if err != nil {
		return 0, errProtoConvertRange
	}
	return out, nil
}

// enum8 narrows one wire uint32 into an 8-bit semantic enum. Values whose
// high bits would otherwise truncate into a valid enum range are refused
// before any cast, so hostile wire input cannot alias a legitimate value.
func enum8[T ~uint8](value uint32) (T, error) {
	out, err := protoconv.Uint8[T](value)
	if err != nil {
		return 0, errProtoConvertRange
	}
	return out, nil
}

func mustTabIDString(value string) domain.TabStableID { return domain.TabStableID(value) }

func lifecycleToWire(id domain.SessionLifecycleID) *wire.LifecycleID {
	return protoconv.LifecycleToWire(id)
}

func lifecycleFromWire(message *wire.LifecycleID) (domain.SessionLifecycleID, error) {
	id, err := protoconv.LifecycleFromWire(message)
	if err != nil {
		return domain.SessionLifecycleID{}, errProtoConvertRange
	}
	return id, nil
}

func exactTargetToWire(target *protocol.ExactSessionTarget) *wire.ExactTarget {
	if target == nil {
		return nil
	}
	return &wire.ExactTarget{LifecycleId: lifecycleToWire(target.LifecycleID), SessionName: target.SessionName}
}

func exactTargetFromWire(message *wire.ExactTarget) (*protocol.ExactSessionTarget, error) {
	if message == nil {
		return nil, nil
	}
	lifecycle, err := lifecycleFromWire(message.GetLifecycleId())
	if err != nil {
		return nil, err
	}
	return &protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: message.GetSessionName()}, nil
}

func rgbToWire(color renderer.RGB) *wire.RGB {
	return protoconv.RGBToWire(color)
}

func rgbFromWire(message *wire.RGB) (renderer.RGB, error) {
	color, err := protoconv.RGBFromWire(message)
	if err != nil {
		return renderer.RGB{}, errProtoConvertRange
	}
	return color, nil
}

func registrationToWire(registration domain.RemoteRegistration) *wire.RemoteRegistration {
	incarnation := append([]byte(nil), registration.Incarnation[:]...)
	return &wire.RemoteRegistration{Endpoint: registration.Endpoint, Incarnation: incarnation, Generation: uint64(registration.Generation)}
}

func registrationFromWire(message *wire.RemoteRegistration) (domain.RemoteRegistration, error) {
	var registration domain.RemoteRegistration
	if message == nil {
		return registration, nil
	}
	registration.Endpoint = message.GetEndpoint()
	if len(message.GetIncarnation()) != len(registration.Incarnation) {
		return domain.RemoteRegistration{}, errProtoConvertRange
	}
	copy(registration.Incarnation[:], message.GetIncarnation())
	registration.Generation = domain.RemoteGeneration(message.GetGeneration())
	return registration, nil
}
