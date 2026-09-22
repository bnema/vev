package daemonmux

// Directional envelope adaptation (P3.2).
//
// EncodeClient wraps one adapter-local client value in its directional
// MuxClientEnvelope; DecodeClient unwraps and validates. EncodeServer and
// DecodeServer do the same for the daemon direction. Every convert path uses
// an explicit taxonomy switch: unknown variants and wrong-direction payloads
// fail with ErrWrongDirection before any mutation. Decode paths run strict
// wire.ScanEnvelope before generated unmarshal, then validate stream
// identity, UTF-8/control/bidi text, bounds, environment/policy, purpose,
// chunks, and error details.
//
// The codecs are stateless: no stream table, admission decision, scheduler,
// queue, pump, socket, or framing engine lives here. Envelope sizes ride
// under the caller's negotiated ceiling, every application envelope is
// additionally capped at the fixed 1 MiB control-class bound
// (MaxMuxApplicationBytes), and opaque stream frames ride under the
// negotiated chunk ceiling. Data, Close, and Reset are shared payloads: the
// same committed value converts into either direction, and direction is fixed
// by the closed envelope oneof.

import (
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// EncodeClient wraps one broker-to-daemon message in its serialized
// directional envelope. The payload must fit maxEnvelopeBytes and the fixed
// 1 MiB application ceiling; stream chunks must fit maxChunkBytes.
func EncodeClient(message ClientMessage, maxEnvelopeBytes, maxChunkBytes uint64) ([]byte, error) {
	envelope, err := encodeClientEnvelope(message, maxChunkBytes)
	if err != nil {
		return nil, err
	}
	raw, err := proto.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if err := checkMuxEnvelopeCeiling(raw, maxEnvelopeBytes); err != nil {
		return nil, err
	}
	return raw, nil
}

// DecodeClient unwraps one serialized broker-to-daemon envelope into its
// adapter-local value. Strict scanning runs before generated unmarshal;
// wrong-direction payloads fail with ErrWrongDirection.
func DecodeClient(payload []byte, maxEnvelopeBytes, maxChunkBytes uint64) (ClientMessage, error) {
	if err := checkMuxEnvelopeCeiling(payload, maxEnvelopeBytes); err != nil {
		return nil, err
	}
	envelope := &wire.MuxClientEnvelope{}
	if err := wire.ScanEnvelope(envelope, payload); err != nil {
		return nil, err
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, envelope)); err != nil {
		return nil, err
	}
	return decodeClientEnvelope(envelope, maxChunkBytes)
}

// EncodeServer wraps one daemon-to-broker message in its serialized
// directional envelope under the same envelope and chunk bounds.
func EncodeServer(message ServerMessage, maxEnvelopeBytes, maxChunkBytes uint64) ([]byte, error) {
	envelope, err := encodeServerEnvelope(message, maxChunkBytes)
	if err != nil {
		return nil, err
	}
	raw, err := proto.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if err := checkMuxEnvelopeCeiling(raw, maxEnvelopeBytes); err != nil {
		return nil, err
	}
	return raw, nil
}

// DecodeServer unwraps one serialized daemon-to-broker envelope into its
// adapter-local value.
func DecodeServer(payload []byte, maxEnvelopeBytes, maxChunkBytes uint64) (ServerMessage, error) {
	if err := checkMuxEnvelopeCeiling(payload, maxEnvelopeBytes); err != nil {
		return nil, err
	}
	envelope := &wire.MuxServerEnvelope{}
	if err := wire.ScanEnvelope(envelope, payload); err != nil {
		return nil, err
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, envelope)); err != nil {
		return nil, err
	}
	return decodeServerEnvelope(envelope, maxChunkBytes)
}

func encodeClientEnvelope(message ClientMessage, maxChunkBytes uint64) (*wire.MuxClientEnvelope, error) {
	if message == nil {
		return nil, ErrInvalidMessage
	}
	switch m := message.(type) {
	case Open:
		converted, err := openToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.MuxClientEnvelope{Payload: &wire.MuxClientEnvelope_Open{Open: converted}}, nil
	case *Open:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case Data:
		converted, err := dataToWire(m, maxChunkBytes)
		if err != nil {
			return nil, err
		}
		return &wire.MuxClientEnvelope{Payload: &wire.MuxClientEnvelope_Data{Data: converted}}, nil
	case *Data:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case Close:
		converted, err := closeToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.MuxClientEnvelope{Payload: &wire.MuxClientEnvelope_Close{Close: converted}}, nil
	case *Close:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case Reset:
		converted, err := resetToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.MuxClientEnvelope{Payload: &wire.MuxClientEnvelope_Reset_{Reset_: converted}}, nil
	case *Reset:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	default:
		return nil, ErrWrongDirection
	}
}

func decodeClientEnvelope(envelope *wire.MuxClientEnvelope, maxChunkBytes uint64) (ClientMessage, error) {
	if envelope == nil {
		return nil, ErrInvalidMessage
	}
	switch payload := envelope.Payload.(type) {
	case *wire.MuxClientEnvelope_Open:
		if payload.Open == nil {
			return nil, ErrInvalidMessage
		}
		return openFromWire(payload.Open)
	case *wire.MuxClientEnvelope_Data:
		return dataFromWire(payload.Data, maxChunkBytes)
	case *wire.MuxClientEnvelope_Close:
		return closeFromWire(payload.Close)
	case *wire.MuxClientEnvelope_Reset_:
		return resetFromWire(payload.Reset_)
	default:
		return nil, ErrWrongDirection
	}
}

func encodeServerEnvelope(message ServerMessage, maxChunkBytes uint64) (*wire.MuxServerEnvelope, error) {
	if message == nil {
		return nil, ErrInvalidMessage
	}
	switch m := message.(type) {
	case Opened:
		if err := m.Ref.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		return &wire.MuxServerEnvelope{Payload: &wire.MuxServerEnvelope_Opened{Opened: &wire.MuxOpened{
			Ref: refToWire(m.Ref),
		}}}, nil
	case *Opened:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case Refused:
		converted, err := refusedToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.MuxServerEnvelope{Payload: &wire.MuxServerEnvelope_Refused{Refused: converted}}, nil
	case *Refused:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case Data:
		converted, err := dataToWire(m, maxChunkBytes)
		if err != nil {
			return nil, err
		}
		return &wire.MuxServerEnvelope{Payload: &wire.MuxServerEnvelope_Data{Data: converted}}, nil
	case *Data:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case Close:
		converted, err := closeToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.MuxServerEnvelope{Payload: &wire.MuxServerEnvelope_Close{Close: converted}}, nil
	case *Close:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case Reset:
		converted, err := resetToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.MuxServerEnvelope{Payload: &wire.MuxServerEnvelope_Reset_{Reset_: converted}}, nil
	case *Reset:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	default:
		return nil, ErrWrongDirection
	}
}

func decodeServerEnvelope(envelope *wire.MuxServerEnvelope, maxChunkBytes uint64) (ServerMessage, error) {
	if envelope == nil {
		return nil, ErrInvalidMessage
	}
	switch payload := envelope.Payload.(type) {
	case *wire.MuxServerEnvelope_Opened:
		if payload.Opened == nil {
			return nil, ErrInvalidMessage
		}
		ref, err := refFromWire(payload.Opened.GetRef())
		if err != nil {
			return nil, ErrInvalidMessage
		}
		return Opened{Ref: ref}, nil
	case *wire.MuxServerEnvelope_Refused:
		return refusedFromWire(payload.Refused)
	case *wire.MuxServerEnvelope_Data:
		return dataFromWire(payload.Data, maxChunkBytes)
	case *wire.MuxServerEnvelope_Close:
		return closeFromWire(payload.Close)
	case *wire.MuxServerEnvelope_Reset_:
		return resetFromWire(payload.Reset_)
	default:
		return nil, ErrWrongDirection
	}
}

func openToWire(m Open) (*wire.MuxOpen, error) {
	if err := m.Ref.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	request := ports.BrokerOpenStreamRequest{
		Epoch: m.Ref.Epoch, Purpose: m.Purpose, Admission: m.Admission, Name: m.Name, Local: m.Local,
		Connection: m.Ref.Connection, Stream: m.Ref.Client,
		Endpoint: m.Endpoint, Registration: m.Registration,
		Target: m.Target, Env: m.Env, Policy: m.Policy, StartMode: m.StartMode,
	}
	if err := request.Validate(); err != nil {
		// Distinguish bound refusals from semantic refusals.
		if len(m.Env) > ports.BrokerMaxEnvEntries {
			return nil, ErrTooLarge
		}
		for _, entry := range m.Env {
			if len(entry) > ports.BrokerMaxEnvEntryBytes {
				return nil, ErrTooLarge
			}
		}
		return nil, ErrInvalidMessage
	}
	if err := checkEnvEntries(m.Env); err != nil {
		return nil, err
	}
	purpose, err := purposeToWire(m.Purpose)
	if err != nil {
		return nil, err
	}
	startMode, err := startModeToWire(m.StartMode)
	if err != nil {
		return nil, err
	}
	out := &wire.MuxOpen{
		Ref:     refToWire(m.Ref),
		Purpose: purpose, Local: m.Local, Endpoint: m.Endpoint,
		Env:    append([]string(nil), m.Env...),
		Policy: policyToWire(m.Policy),
		// The daemon-start authorization always travels, including for
		// observation and control streams, so the daemon-side transport can
		// never infer a spawn from an absent field.
		StartMode: startMode,
	}
	if !m.Local {
		out.Registration = registrationToWire(m.Registration)
	}
	if m.Purpose == ports.BrokerStreamAttachment {
		out.Admission = uint32(m.Admission)
		out.Name = m.Name
		if m.Admission == ports.BrokerAdmissionExact {
			out.Target = exactTargetToWire(m.Target)
		}
	}
	return out, nil
}

func openFromWire(message *wire.MuxOpen) (Open, error) {
	var out Open
	if message == nil {
		return out, ErrInvalidMessage
	}
	ref, err := refFromWire(message.GetRef())
	if err != nil {
		return Open{}, ErrInvalidMessage
	}
	purpose, err := purposeFromWire(message.GetPurpose())
	if err != nil {
		return Open{}, err
	}
	// Narrow the wire uint32 before the cast: a value whose high bits
	// truncate onto a valid admission code would otherwise alias silently.
	admission, err := muxEnum8[ports.BrokerStreamAdmission](message.GetAdmission())
	if err != nil {
		return Open{}, ErrInvalidMessage
	}
	name := message.GetName()
	if purpose != ports.BrokerStreamAttachment && (admission != 0 || name != "") {
		return Open{}, ErrInvalidMessage
	}
	startMode, err := startModeFromWire(message.GetStartMode())
	if err != nil {
		return Open{}, ErrInvalidMessage
	}
	local := message.GetLocal()
	var registration domain.RemoteRegistration
	if !local {
		registration, err = registrationFromWire(message.GetRegistration())
		if err != nil {
			return Open{}, ErrInvalidMessage
		}
	} else if message.GetRegistration() != nil {
		return Open{}, ErrInvalidMessage
	}
	var target protocol.ExactSessionTarget
	if purpose == ports.BrokerStreamAttachment {
		if admission == ports.BrokerAdmissionExact {
			target, err = exactTargetFromWire(message.GetTarget())
			if err != nil {
				return Open{}, ErrInvalidMessage
			}
		} else if message.GetTarget() != nil {
			return Open{}, ErrInvalidMessage
		}
	} else if message.GetTarget() != nil {
		return Open{}, ErrInvalidMessage
	}
	env := append([]string(nil), message.GetEnv()...)
	if err := checkEnvEntries(env); err != nil {
		return Open{}, err
	}
	if purpose != ports.BrokerStreamAttachment && len(env) != 0 {
		return Open{}, ErrInvalidMessage
	}
	policy, err := policyFromWire(message.GetPolicy())
	if err != nil {
		return Open{}, ErrInvalidMessage
	}
	endpoint := message.GetEndpoint()
	candidate := Open{
		Ref: ref, Purpose: purpose, Admission: admission, Name: name, Local: local, Endpoint: endpoint,
		Registration: registration, Target: target, Env: env, Policy: policy,
		StartMode: startMode,
	}
	request := ports.BrokerOpenStreamRequest{
		Epoch: candidate.Ref.Epoch, Purpose: candidate.Purpose, Admission: candidate.Admission, Name: candidate.Name, Local: candidate.Local,
		Connection: candidate.Ref.Connection, Stream: candidate.Ref.Client,
		Endpoint: candidate.Endpoint, Registration: candidate.Registration,
		Target: candidate.Target, Env: candidate.Env, Policy: candidate.Policy,
		StartMode: candidate.StartMode,
	}
	if err := request.Validate(); err != nil {
		return Open{}, ErrInvalidMessage
	}
	return candidate, nil
}

func refusedToWire(m Refused) (*wire.MuxRefused, error) {
	if err := m.Ref.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Error.validate(); err != nil {
		return nil, err
	}
	return &wire.MuxRefused{Ref: refToWire(m.Ref), Error: errorDetailToWire(m.Error)}, nil
}

func refusedFromWire(message *wire.MuxRefused) (Refused, error) {
	var out Refused
	if message == nil {
		return out, ErrInvalidMessage
	}
	ref, err := refFromWire(message.GetRef())
	if err != nil {
		return Refused{}, ErrInvalidMessage
	}
	detail, err := errorDetailFromWire(message.GetError())
	if err != nil {
		return Refused{}, ErrInvalidMessage
	}
	return Refused{Ref: ref, Error: detail}, nil
}

func dataToWire(m Data, maxChunkBytes uint64) (*wire.MuxData, error) {
	if err := m.Physical.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if uint64(len(m.Data)) > maxChunkBytes {
		return nil, ErrTooLarge
	}
	if uint64(len(m.Data)) > MaxMuxChunkBytes {
		return nil, ErrTooLarge
	}
	if len(m.Data) == 0 {
		return nil, ErrInvalidMessage
	}
	return &wire.MuxData{
		PhysicalStreamId: physicalToWire(m.Physical),
		Data:             append([]byte(nil), m.Data...),
	}, nil
}

func dataFromWire(message *wire.MuxData, maxChunkBytes uint64) (Data, error) {
	var out Data
	if message == nil {
		return out, ErrInvalidMessage
	}
	physical, err := physicalFromWire(message.GetPhysicalStreamId())
	if err != nil {
		return Data{}, ErrInvalidMessage
	}
	data := message.GetData()
	if uint64(len(data)) > maxChunkBytes {
		return Data{}, ErrTooLarge
	}
	if uint64(len(data)) > MaxMuxChunkBytes {
		return Data{}, ErrTooLarge
	}
	if len(data) == 0 {
		return Data{}, ErrInvalidMessage
	}
	return Data{Physical: physical, Data: append([]byte(nil), data...)}, nil
}

func closeToWire(m Close) (*wire.MuxClose, error) {
	if err := m.Physical.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	return &wire.MuxClose{PhysicalStreamId: physicalToWire(m.Physical)}, nil
}

func closeFromWire(message *wire.MuxClose) (Close, error) {
	var out Close
	if message == nil {
		return out, ErrInvalidMessage
	}
	physical, err := physicalFromWire(message.GetPhysicalStreamId())
	if err != nil {
		return Close{}, ErrInvalidMessage
	}
	return Close{Physical: physical}, nil
}

func resetToWire(m Reset) (*wire.MuxReset, error) {
	if err := m.Physical.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if m.HasError {
		if err := m.Error.validate(); err != nil {
			return nil, err
		}
	} else if m.Error != (ErrorDetail{}) {
		// A reset without a declared detail carries no detail at all; a
		// nonzero one under HasError=false is a caller bug that must fail
		// closed rather than be silently dropped.
		return nil, ErrInvalidMessage
	}
	out := &wire.MuxReset{PhysicalStreamId: physicalToWire(m.Physical)}
	if m.HasError {
		out.Error = errorDetailToWire(m.Error)
	}
	return out, nil
}

func resetFromWire(message *wire.MuxReset) (Reset, error) {
	var out Reset
	if message == nil {
		return out, ErrInvalidMessage
	}
	physical, err := physicalFromWire(message.GetPhysicalStreamId())
	if err != nil {
		return Reset{}, ErrInvalidMessage
	}
	result := Reset{Physical: physical}
	if message.GetError() != nil {
		detail, err := errorDetailFromWire(message.GetError())
		if err != nil {
			return Reset{}, ErrInvalidMessage
		}
		result.Error = detail
		result.HasError = true
	}
	return result, nil
}

// purposeToWire maps the closed ports stream-purpose taxonomy onto the wire
// taxonomy. The switch is explicit: an unknown purpose never defaults to a
// wire value.
func purposeToWire(purpose ports.BrokerStreamPurpose) (uint32, error) {
	switch purpose {
	case ports.BrokerStreamAttachment:
		return 1, nil
	case ports.BrokerStreamControl:
		return 2, nil
	case ports.BrokerStreamObservation:
		return 3, nil
	default:
		return 0, ErrInvalidMessage
	}
}

func purposeFromWire(value uint32) (ports.BrokerStreamPurpose, error) {
	switch value {
	case 1:
		return ports.BrokerStreamAttachment, nil
	case 2:
		return ports.BrokerStreamControl, nil
	case 3:
		return ports.BrokerStreamObservation, nil
	default:
		return 0, ErrInvalidMessage
	}
}
