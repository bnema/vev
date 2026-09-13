// Strict protowire inspection for generated envelope payloads.
//
// scanEnvelope runs before generated unmarshal: it enforces maximum depth,
// field count, repeated-element counts and lengths, recursively rejects
// unknown fields and wire types, and requires exactly one top-level
// envelope variant occurrence. Repeated occurrences of singular fields and
// concatenated envelope payloads are rejected; declared repeated fields
// remain legal. Generated unmarshal and semantic validation run after.
package wire

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Scanner bounds. They cap decode work before any allocation a hostile
// envelope could inflate; semantic validators apply tighter per-message
// limits afterwards.
const (
	maxScanDepth       = 16
	maxScanFields      = 65536
	maxScanRepeated    = 8192
	maxScanBytesField  = AbsoluteEnvelopeLimit
	maxScanStringField = 4 << 20
)

var (
	ErrScanDepth       = errors.New("wire: envelope exceeds maximum nesting depth")
	ErrScanFields      = errors.New("wire: envelope exceeds maximum field count")
	ErrScanRepeated    = errors.New("wire: envelope exceeds maximum repeated elements")
	ErrScanLength      = errors.New("wire: envelope field exceeds maximum length")
	ErrScanUnknown     = errors.New("wire: envelope has unknown field")
	ErrScanWireType    = errors.New("wire: envelope has invalid wire type")
	ErrScanDuplicate   = errors.New("wire: envelope repeats singular field")
	ErrScanEmpty       = errors.New("wire: envelope has no payload variant")
	ErrScanTrailing    = errors.New("wire: envelope has trailing bytes")
	ErrScanTruncated   = errors.New("wire: envelope is truncated")
	ErrScanNotMessage  = errors.New("wire: envelope field is not a message")
	ErrScanVarintRange = errors.New("wire: envelope varint exceeds declared field range")
)

// scanSpec describes the expected shape of one message level: known fields
// by number with their wire type and repetition, resolved from the
// descriptor at scan time.
type scanSpec struct {
	fields map[protoreflect.FieldNumber]scanField
	// oneofs marks every field number that belongs to a oneof: its
	// members are exclusive, so any second occurrence is a duplicate.
	oneofs map[protoreflect.FieldNumber]bool
}

// scanField records one known field's expectations.
type scanField struct {
	kind      protoreflect.Kind
	isList    bool
	isMessage bool
}

// specCache memoizes scan specs by descriptor: descriptors are process
// singletons, so each message shape is reflected exactly once instead
// of on every received envelope (the receive hot path).
var specCache sync.Map // map[protoreflect.MessageDescriptor]scanSpec

// specForMessage returns the cached scan spec for one message descriptor.
func specForMessage(descriptor protoreflect.MessageDescriptor) scanSpec {
	if cached, ok := specCache.Load(descriptor); ok {
		return cached.(scanSpec)
	}
	spec := scanSpec{
		fields: make(map[protoreflect.FieldNumber]scanField),
		oneofs: make(map[protoreflect.FieldNumber]bool),
	}
	fields := descriptor.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		spec.fields[field.Number()] = scanField{
			kind:      field.Kind(),
			isList:    field.IsList(),
			isMessage: field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind,
		}
	}
	oneofs := descriptor.Oneofs()
	for i := range oneofs.Len() {
		oneof := oneofs.Get(i)
		members := oneof.Fields()
		for j := range members.Len() {
			spec.oneofs[members.Get(j).Number()] = true
		}
	}
	actual, _ := specCache.LoadOrStore(descriptor, spec)
	return actual.(scanSpec)
}

// ScanEnvelope strictly inspects one serialized envelope of the given type
// before generated unmarshal. It returns nil only when the payload is
// well-formed, known, singular at the top level, and fully consumed.
func ScanEnvelope(message proto.Message, payload []byte) error {
	if message == nil {
		return ErrScanNotMessage
	}
	if len(payload) == 0 {
		return ErrScanEmpty
	}
	if len(payload) > AbsoluteEnvelopeLimit {
		return ErrScanLength
	}
	scanner := &envelopeScanner{fields: 0}
	consumed, err := scanner.scanMessage(message.ProtoReflect().Descriptor(), payload, 0)
	if err != nil {
		return err
	}
	if consumed != len(payload) {
		return ErrScanTrailing
	}
	return nil
}

type envelopeScanner struct {
	fields int
}

// scanMessage scans one message level, returning bytes consumed.
func (s *envelopeScanner) scanMessage(descriptor protoreflect.MessageDescriptor, payload []byte, depth int) (int, error) {
	if depth > maxScanDepth {
		return 0, ErrScanDepth
	}
	spec := specForMessage(descriptor)
	consumed := 0
	variantSeen := false
	repeated := 0
	seen := map[protoreflect.FieldNumber]bool{}
	for len(payload) > 0 {
		number, wireType, length := protowire.ConsumeTag(payload)
		if length < 0 {
			return 0, ErrScanTruncated
		}
		field, ok := spec.fields[number]
		if !ok {
			return 0, fmt.Errorf("%w: field %d", ErrScanUnknown, number)
		}
		if err := checkWireType(field, wireType); err != nil {
			return 0, err
		}
		s.fields++
		if s.fields > maxScanFields {
			return 0, ErrScanFields
		}
		if spec.oneofs[number] {
			// Oneof members are exclusive: any second occurrence —
			// same variant or another — is a duplicate, and the
			// top level requires exactly one variant.
			if variantSeen {
				return 0, fmt.Errorf("%w: oneof field %d", ErrScanDuplicate, number)
			}
			variantSeen = true
		} else if !field.isList {
			if seen[number] {
				return 0, fmt.Errorf("%w: field %d", ErrScanDuplicate, number)
			}
			seen[number] = true
		} else {
			repeated++
			if repeated > maxScanRepeated {
				return 0, ErrScanRepeated
			}
		}
		fieldDescriptor := descriptor.Fields().ByNumber(number)
		n, err := s.scanField(fieldDescriptor, field, wireType, payload[length:], depth)
		if err != nil {
			return 0, err
		}
		consumed += length + n
		payload = payload[length+n:]
	}
	if depth == 0 && isEnvelopeDescriptor(descriptor) && !variantSeen {
		return 0, ErrScanEmpty
	}
	return consumed, nil
}

// isEnvelopeDescriptor reports whether the descriptor is a top-level
// directional envelope requiring exactly one variant.
func isEnvelopeDescriptor(descriptor protoreflect.MessageDescriptor) bool {
	name := string(descriptor.FullName())
	return name == "vev.wire.v1.ClientEnvelope" || name == "vev.wire.v1.ServerEnvelope"
}

// checkVarintRange rejects a raw varint that cannot be represented by the
// declared scalar kind. Generated unmarshal truncates the high bits of 32-bit
// fields, so a hostile 64-bit varint could otherwise alias a legitimate value
// (for example an in-range enum code, a bool, or a negative int32).
func checkVarintRange(kind protoreflect.Kind, value uint64) error {
	switch kind {
	case protoreflect.BoolKind:
		// Protobuf treats any nonzero varint as true, so a strict scanner
		// rejects the non-canonical values above one.
		if value > 1 {
			return fmt.Errorf("%w: kind %v", ErrScanVarintRange, kind)
		}
	case protoreflect.Uint32Kind, protoreflect.Sint32Kind, protoreflect.EnumKind:
		if value > math.MaxUint32 {
			return fmt.Errorf("%w: kind %v", ErrScanVarintRange, kind)
		}
	case protoreflect.Int32Kind:
		// A canonical negative int32 sign-extends into ten bytes, so the
		// valid encodings are 0..MaxInt32 and the sign-extended range
		// [MaxUint64-MaxInt32, MaxUint64]. Anything between the two
		// truncates to a different int32 and is rejected.
		if value > math.MaxInt32 && value < math.MaxUint64-uint64(math.MaxInt32) {
			return fmt.Errorf("%w: kind %v", ErrScanVarintRange, kind)
		}
	}
	return nil
}

func checkWireType(field scanField, wireType protowire.Type) error {
	switch field.kind {
	case protoreflect.BoolKind, protoreflect.EnumKind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind:
		if wireType != protowire.VarintType {
			return fmt.Errorf("%w: want varint", ErrScanWireType)
		}
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
		if wireType != protowire.Fixed32Type {
			return fmt.Errorf("%w: want fixed32", ErrScanWireType)
		}
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
		if wireType != protowire.Fixed64Type {
			return fmt.Errorf("%w: want fixed64", ErrScanWireType)
		}
	case protoreflect.StringKind, protoreflect.BytesKind, protoreflect.MessageKind, protoreflect.GroupKind:
		if wireType != protowire.BytesType {
			return fmt.Errorf("%w: want length-delimited", ErrScanWireType)
		}
	default:
		return fmt.Errorf("%w: kind %v", ErrScanWireType, field.kind)
	}
	return nil
}

func (s *envelopeScanner) scanField(descriptor protoreflect.FieldDescriptor, field scanField, wireType protowire.Type, payload []byte, depth int) (int, error) {
	switch wireType {
	case protowire.VarintType:
		value, length := protowire.ConsumeVarint(payload)
		if length < 0 {
			return 0, ErrScanTruncated
		}
		if err := checkVarintRange(field.kind, value); err != nil {
			return 0, err
		}
		return length, nil
	case protowire.Fixed32Type:
		if len(payload) < 4 {
			return 0, ErrScanTruncated
		}
		return 4, nil
	case protowire.Fixed64Type:
		if len(payload) < 8 {
			return 0, ErrScanTruncated
		}
		return 8, nil
	case protowire.BytesType:
		value, length := protowire.ConsumeBytes(payload)
		if length < 0 {
			return 0, ErrScanTruncated
		}
		switch {
		case field.isMessage:
			if descriptor == nil || descriptor.Message() == nil {
				return 0, ErrScanNotMessage
			}
			if len(value) > maxScanBytesField {
				return 0, ErrScanLength
			}
			if _, err := s.scanMessage(descriptor.Message(), value, depth+1); err != nil {
				return 0, err
			}
			return length, nil
		case field.kind == protoreflect.StringKind:
			if len(value) > maxScanStringField {
				return 0, ErrScanLength
			}
			return length, nil
		default:
			if len(value) > maxScanBytesField {
				return 0, ErrScanLength
			}
			return length, nil
		}
	default:
		return 0, fmt.Errorf("%w: %d", ErrScanWireType, wireType)
	}
}

// validatedUnmarshal scans payload, unmarshals into message, and reports a
// trailing-garbage-safe result. Callers run semantic validation afterwards.
func validatedUnmarshal(message proto.Message, payload []byte) error {
	if err := ScanEnvelope(message, payload); err != nil {
		return err
	}
	options := proto.UnmarshalOptions{DiscardUnknown: false}
	if err := options.Unmarshal(payload, message); err != nil {
		return err
	}
	return nil
}
