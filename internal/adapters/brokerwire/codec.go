package brokerwire

// Directional envelope adaptation (P3.1).
//
// EncodeClient wraps one adapter-local client value in its directional
// BrokerClientEnvelope; DecodeClient unwraps and validates. EncodeServer
// and DecodeServer do the same for the server direction. Every convert
// path uses an explicit taxonomy switch: unknown variants and
// wrong-direction payloads fail with ErrWrongDirection before any
// mutation. Decode paths run strict wire.ScanEnvelope before generated
// unmarshal, then validate IDs, UTF-8/control/bidi text, bounds, times,
// environment/policy, chunks, and snapshot parts.
//
// The codecs are stateless: no connection state, multipart assembler,
// operation tracker, stream lifecycle, socket, framing pump, or P3.2
// transport lives here. Envelope sizes ride under the caller's negotiated
// ceiling; stream chunks ride under the negotiated chunk ceiling.

import (
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
)

// EncodeClient wraps one client message in its serialized directional
// envelope. The payload must fit maxEnvelopeBytes; stream chunks must fit
// maxChunkBytes.
func EncodeClient(message ClientMessage, maxEnvelopeBytes, maxChunkBytes uint64) ([]byte, error) {
	envelope, err := encodeClientEnvelope(message, maxChunkBytes)
	if err != nil {
		return nil, err
	}
	raw, err := proto.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if err := checkBrokerEnvelopeCeiling(raw, maxEnvelopeBytes); err != nil {
		return nil, err
	}
	return raw, nil
}

// DecodeClient unwraps one serialized client envelope into its
// adapter-local value. Strict scanning runs before generated unmarshal;
// wrong-direction payloads fail with ErrWrongDirection.
func DecodeClient(payload []byte, maxEnvelopeBytes, maxChunkBytes uint64) (ClientMessage, error) {
	if err := checkBrokerEnvelopeCeiling(payload, maxEnvelopeBytes); err != nil {
		return nil, err
	}
	envelope := &wire.BrokerClientEnvelope{}
	if err := wire.ScanEnvelope(envelope, payload); err != nil {
		return nil, err
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, envelope)); err != nil {
		return nil, err
	}
	return decodeClientEnvelope(envelope, maxChunkBytes)
}

// EncodeServer wraps one server message in its serialized directional
// envelope.
func EncodeServer(message ServerMessage, maxEnvelopeBytes, maxChunkBytes uint64) ([]byte, error) {
	envelope, err := encodeServerEnvelope(message, maxChunkBytes)
	if err != nil {
		return nil, err
	}
	raw, err := proto.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if err := checkBrokerEnvelopeCeiling(raw, maxEnvelopeBytes); err != nil {
		return nil, err
	}
	return raw, nil
}

// DecodeServer unwraps one serialized server envelope into its
// adapter-local value.
func DecodeServer(payload []byte, maxEnvelopeBytes, maxChunkBytes uint64) (ServerMessage, error) {
	if err := checkBrokerEnvelopeCeiling(payload, maxEnvelopeBytes); err != nil {
		return nil, err
	}
	envelope := &wire.BrokerServerEnvelope{}
	if err := wire.ScanEnvelope(envelope, payload); err != nil {
		return nil, err
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, envelope)); err != nil {
		return nil, err
	}
	return decodeServerEnvelope(envelope, maxChunkBytes)
}

func encodeClientEnvelope(message ClientMessage, maxChunkBytes uint64) (*wire.BrokerClientEnvelope, error) {
	if message == nil {
		return nil, ErrInvalidMessage
	}
	switch m := message.(type) {
	case Register:
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_Register{Register: &wire.Register{}}}, nil
	case *Register:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case Subscribe:
		if m.Epoch == 0 {
			return nil, ErrInvalidMessage
		}
		if err := m.Connection.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_Subscribe{Subscribe: &wire.Subscribe{
			Scope: scopeToWire(m.Epoch, m.Connection), Generation: m.Generation, Passive: m.Passive,
		}}}, nil
	case *Subscribe:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case Resync:
		if m.Epoch == 0 {
			return nil, ErrInvalidMessage
		}
		if err := m.Connection.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_Resync{Resync: &wire.Resync{
			Scope: scopeToWire(m.Epoch, m.Connection), Generation: m.Generation,
		}}}, nil
	case *Resync:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case Unsubscribe:
		if m.Epoch == 0 {
			return nil, ErrInvalidMessage
		}
		if err := m.Connection.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_Unsubscribe{Unsubscribe: &wire.Unsubscribe{
			Scope: scopeToWire(m.Epoch, m.Connection), Generation: m.Generation,
		}}}, nil
	case *Unsubscribe:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case AddHost:
		converted, err := addHostToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_AddHost{AddHost: converted}}, nil
	case *AddHost:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case RemoveHost:
		converted, err := removeHostToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_RemoveHost{RemoveHost: converted}}, nil
	case *RemoveHost:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case UpdateHostPolicy:
		converted, err := updateHostPolicyToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_UpdateHostPolicy{UpdateHostPolicy: converted}}, nil
	case *UpdateHostPolicy:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case Reconcile:
		converted, err := reconcileToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_Reconcile{Reconcile: converted}}, nil
	case *Reconcile:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case OpenStream:
		converted, err := openStreamToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_OpenStream{OpenStream: converted}}, nil
	case *OpenStream:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case ClientStreamData:
		converted, err := clientStreamDataToWire(m, maxChunkBytes)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_ClientStreamData{ClientStreamData: converted}}, nil
	case *ClientStreamData:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case CloseStream:
		if m.Epoch == 0 {
			return nil, ErrInvalidMessage
		}
		if err := m.Connection.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		if err := m.Stream.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_CloseStream{CloseStream: &wire.CloseStream{
			Ref: refToWire(m.Epoch, m.Connection, m.Stream),
		}}}, nil
	case *CloseStream:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case StartPreview:
		converted, err := startPreviewToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_StartPreview{StartPreview: converted}}, nil
	case *StartPreview:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	case CancelPreview:
		converted, err := cancelPreviewToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerClientEnvelope{Payload: &wire.BrokerClientEnvelope_CancelPreview{CancelPreview: converted}}, nil
	case *CancelPreview:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeClientEnvelope(*m, maxChunkBytes)
	default:
		return nil, ErrWrongDirection
	}
}

func decodeClientEnvelope(envelope *wire.BrokerClientEnvelope, maxChunkBytes uint64) (ClientMessage, error) {
	if envelope == nil {
		return nil, ErrInvalidMessage
	}
	switch payload := envelope.Payload.(type) {
	case *wire.BrokerClientEnvelope_Register:
		if payload.Register == nil {
			return nil, ErrInvalidMessage
		}
		return Register{}, nil
	case *wire.BrokerClientEnvelope_Subscribe:
		return subscribeFromWire(payload.Subscribe)
	case *wire.BrokerClientEnvelope_Resync:
		return resyncFromWire(payload.Resync)
	case *wire.BrokerClientEnvelope_Unsubscribe:
		return unsubscribeFromWire(payload.Unsubscribe)
	case *wire.BrokerClientEnvelope_AddHost:
		return addHostFromWire(payload.AddHost)
	case *wire.BrokerClientEnvelope_RemoveHost:
		return removeHostFromWire(payload.RemoveHost)
	case *wire.BrokerClientEnvelope_UpdateHostPolicy:
		return updateHostPolicyFromWire(payload.UpdateHostPolicy)
	case *wire.BrokerClientEnvelope_Reconcile:
		return reconcileFromWire(payload.Reconcile)
	case *wire.BrokerClientEnvelope_OpenStream:
		return openStreamFromWire(payload.OpenStream)
	case *wire.BrokerClientEnvelope_ClientStreamData:
		return clientStreamDataFromWire(payload.ClientStreamData, maxChunkBytes)
	case *wire.BrokerClientEnvelope_CloseStream:
		if payload.CloseStream == nil {
			return nil, ErrInvalidMessage
		}
		epoch, connection, stream, err := refFromWire(payload.CloseStream.GetRef())
		if err != nil {
			return nil, ErrInvalidMessage
		}
		return CloseStream{Epoch: epoch, Connection: connection, Stream: stream}, nil
	case *wire.BrokerClientEnvelope_StartPreview:
		return startPreviewFromWire(payload.StartPreview)
	case *wire.BrokerClientEnvelope_CancelPreview:
		return cancelPreviewFromWire(payload.CancelPreview)
	default:
		return nil, ErrWrongDirection
	}
}

func addHostToWire(m AddHost) (*wire.AddHost, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Operation.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := validateBrokerEndpoint(m.Endpoint); err != nil {
		return nil, err
	}
	// Policy is required authority: a zero or invalid policy is refused
	// here rather than travelling as an absent message the peer must
	// interpret.
	if err := m.Policy.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	return &wire.AddHost{
		Scope:       scopeToWire(m.Epoch, m.Connection),
		OperationId: operationToWire(m.Operation),
		Endpoint:    m.Endpoint,
		Policy:      policyToWire(m.Policy),
	}, nil
}

func addHostFromWire(message *wire.AddHost) (AddHost, error) {
	var out AddHost
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return AddHost{}, ErrInvalidMessage
	}
	operation, err := operationFromWire(message.GetOperationId())
	if err != nil {
		return AddHost{}, ErrInvalidMessage
	}
	if err := validateBrokerEndpoint(message.GetEndpoint()); err != nil {
		return AddHost{}, err
	}
	policy, err := policyFromWire(message.GetPolicy())
	if err != nil {
		return AddHost{}, ErrInvalidMessage
	}
	return AddHost{Epoch: epoch, Connection: connection, Operation: operation, Endpoint: message.GetEndpoint(), Policy: policy}, nil
}

func removeHostToWire(m RemoveHost) (*wire.RemoveHost, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Operation.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	// Removal carries exact authority, never a bare endpoint: a zero or
	// partial registration could not fence a remove/re-add race.
	if err := m.Registration.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	return &wire.RemoveHost{
		Scope:        scopeToWire(m.Epoch, m.Connection),
		OperationId:  operationToWire(m.Operation),
		Registration: registrationToWire(m.Registration),
	}, nil
}

func removeHostFromWire(message *wire.RemoveHost) (RemoveHost, error) {
	var out RemoveHost
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return RemoveHost{}, ErrInvalidMessage
	}
	operation, err := operationFromWire(message.GetOperationId())
	if err != nil {
		return RemoveHost{}, ErrInvalidMessage
	}
	registration, err := registrationFromWire(message.GetRegistration())
	if err != nil {
		return RemoveHost{}, ErrInvalidMessage
	}
	return RemoveHost{Epoch: epoch, Connection: connection, Operation: operation, Registration: registration}, nil
}

func updateHostPolicyToWire(m UpdateHostPolicy) (*wire.UpdateHostPolicy, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Operation.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Registration.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Policy.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	return &wire.UpdateHostPolicy{
		Scope:        scopeToWire(m.Epoch, m.Connection),
		OperationId:  operationToWire(m.Operation),
		Registration: registrationToWire(m.Registration),
		Policy:       policyToWire(m.Policy),
	}, nil
}

func updateHostPolicyFromWire(message *wire.UpdateHostPolicy) (UpdateHostPolicy, error) {
	var out UpdateHostPolicy
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return UpdateHostPolicy{}, ErrInvalidMessage
	}
	operation, err := operationFromWire(message.GetOperationId())
	if err != nil {
		return UpdateHostPolicy{}, ErrInvalidMessage
	}
	registration, err := registrationFromWire(message.GetRegistration())
	if err != nil {
		return UpdateHostPolicy{}, ErrInvalidMessage
	}
	policy, err := policyFromWire(message.GetPolicy())
	if err != nil {
		return UpdateHostPolicy{}, ErrInvalidMessage
	}
	return UpdateHostPolicy{Epoch: epoch, Connection: connection, Operation: operation, Registration: registration, Policy: policy}, nil
}

func reconcileToWire(m Reconcile) (*wire.Reconcile, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Registration.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	return &wire.Reconcile{
		Scope:        scopeToWire(m.Epoch, m.Connection),
		Registration: registrationToWire(m.Registration),
	}, nil
}

func reconcileFromWire(message *wire.Reconcile) (Reconcile, error) {
	var out Reconcile
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return Reconcile{}, ErrInvalidMessage
	}
	registration, err := registrationFromWire(message.GetRegistration())
	if err != nil {
		return Reconcile{}, ErrInvalidMessage
	}
	return Reconcile{Epoch: epoch, Connection: connection, Registration: registration}, nil
}

func subscribeFromWire(message *wire.Subscribe) (Subscribe, error) {
	var out Subscribe
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return Subscribe{}, ErrInvalidMessage
	}
	return Subscribe{Epoch: epoch, Connection: connection, Generation: message.GetGeneration(), Passive: message.GetPassive()}, nil
}

func resyncFromWire(message *wire.Resync) (Resync, error) {
	var out Resync
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return Resync{}, ErrInvalidMessage
	}
	return Resync{Epoch: epoch, Connection: connection, Generation: message.GetGeneration()}, nil
}

func unsubscribeFromWire(message *wire.Unsubscribe) (Unsubscribe, error) {
	var out Unsubscribe
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return Unsubscribe{}, ErrInvalidMessage
	}
	return Unsubscribe{Epoch: epoch, Connection: connection, Generation: message.GetGeneration()}, nil
}

func openStreamToWire(m OpenStream) (*wire.OpenStream, error) {
	request := ports.BrokerOpenStreamRequest{
		Epoch: m.Epoch, Purpose: m.Purpose, Admission: m.Admission, Name: m.Name,
		Local:      m.Local,
		Connection: m.Connection, Stream: m.Stream,
		Endpoint: m.Endpoint, Registration: m.Registration,
		Target: m.Target, Env: m.Env, Policy: m.Policy,
		StartMode: m.StartMode,
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
	var purpose uint32
	switch m.Purpose {
	case ports.BrokerStreamAttachment:
		purpose = 1
	case ports.BrokerStreamControl:
		purpose = 2
	case ports.BrokerStreamObservation:
		purpose = 3
	default:
		return nil, ErrInvalidMessage
	}
	admission, err := admissionToWire(m.Admission)
	if err != nil {
		return nil, err
	}
	startMode, err := startModeToWire(m.StartMode)
	if err != nil {
		return nil, err
	}
	out := &wire.OpenStream{
		Ref:     refToWire(m.Epoch, m.Connection, m.Stream),
		Purpose: purpose, Local: m.Local, Endpoint: m.Endpoint,
		Env:    append([]string(nil), m.Env...),
		Policy: policyToWire(m.Policy),
		// The admission taxonomy mirrors the port values (0 none, 1 exact,
		// 2 create named, 3 create ephemeral); the name travels only for the
		// create-named variant.
		Admission: admission, Name: m.Name, StartMode: startMode,
	}
	if !m.Local {
		out.Registration = registrationToWire(m.Registration)
	}
	// Only exact attach/resume carries a session target; a creation
	// admission carries a zero target that must never be encoded as a
	// present message.
	if m.Purpose == ports.BrokerStreamAttachment && m.Admission == ports.BrokerAdmissionExact {
		out.Target = exactTargetToWire(m.Target)
	}
	return out, nil
}

// admissionToWire maps the closed admission taxonomy onto its wire code.
func admissionToWire(admission ports.BrokerStreamAdmission) (uint32, error) {
	switch admission {
	case 0:
		return 0, nil
	case ports.BrokerAdmissionExact:
		return 1, nil
	case ports.BrokerAdmissionCreateNamed:
		return 2, nil
	case ports.BrokerAdmissionCreateEphemeral:
		return 3, nil
	default:
		return 0, errConvertRange
	}
}

// admissionFromWire maps a wire admission code onto the closed taxonomy.
func admissionFromWire(value uint32) (ports.BrokerStreamAdmission, error) {
	switch value {
	case 0:
		return 0, nil
	case 1:
		return ports.BrokerAdmissionExact, nil
	case 2:
		return ports.BrokerAdmissionCreateNamed, nil
	case 3:
		return ports.BrokerAdmissionCreateEphemeral, nil
	default:
		return 0, errConvertRange
	}
}

// startModeToWire maps the closed daemon-start taxonomy onto its wire code.
// The zero value is refused rather than encoded, so a peer never has to guess
// whether an absent mode authorized a spawn.
func startModeToWire(mode ports.BrokerDaemonStartMode) (uint32, error) {
	switch mode {
	case ports.BrokerDaemonExistingOnly:
		return 1, nil
	case ports.BrokerDaemonStartIfNeeded:
		return 2, nil
	default:
		return 0, errConvertRange
	}
}

// startModeFromWire maps a wire daemon-start code onto the closed taxonomy.
func startModeFromWire(value uint32) (ports.BrokerDaemonStartMode, error) {
	switch value {
	case 1:
		return ports.BrokerDaemonExistingOnly, nil
	case 2:
		return ports.BrokerDaemonStartIfNeeded, nil
	default:
		return 0, errConvertRange
	}
}

func openStreamFromWire(message *wire.OpenStream) (OpenStream, error) {
	var out OpenStream
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, stream, err := refFromWire(message.GetRef())
	if err != nil {
		return OpenStream{}, ErrInvalidMessage
	}
	var purpose ports.BrokerStreamPurpose
	switch message.GetPurpose() {
	case 1:
		purpose = ports.BrokerStreamAttachment
	case 2:
		purpose = ports.BrokerStreamControl
	case 3:
		purpose = ports.BrokerStreamObservation
	default:
		return OpenStream{}, ErrInvalidMessage
	}
	admission, err := admissionFromWire(message.GetAdmission())
	if err != nil {
		return OpenStream{}, ErrInvalidMessage
	}
	startMode, err := startModeFromWire(message.GetStartMode())
	if err != nil {
		return OpenStream{}, ErrInvalidMessage
	}
	local := message.GetLocal()
	var registration domain.RemoteRegistration
	if !local {
		registration, err = registrationFromWire(message.GetRegistration())
		if err != nil {
			return OpenStream{}, ErrInvalidMessage
		}
	} else if message.GetRegistration() != nil {
		return OpenStream{}, ErrInvalidMessage
	}
	// Only exact attach/resume carries a session target. A present target on
	// any other admission or purpose is malformed.
	var target protocol.ExactSessionTarget
	if purpose == ports.BrokerStreamAttachment && admission == ports.BrokerAdmissionExact {
		target, err = exactTargetFromWire(message.GetTarget())
		if err != nil {
			return OpenStream{}, ErrInvalidMessage
		}
	} else if message.GetTarget() != nil {
		return OpenStream{}, ErrInvalidMessage
	}
	env := append([]string(nil), message.GetEnv()...)
	if err := checkEnvEntries(env); err != nil {
		return OpenStream{}, err
	}
	if purpose != ports.BrokerStreamAttachment && len(env) != 0 {
		return OpenStream{}, ErrInvalidMessage
	}
	policy, err := policyFromWire(message.GetPolicy())
	if err != nil {
		return OpenStream{}, ErrInvalidMessage
	}
	endpoint := message.GetEndpoint()
	candidate := OpenStream{
		Epoch: epoch, Connection: connection, Stream: stream,
		Purpose: purpose, Admission: admission, Name: message.GetName(),
		Local:        local,
		Endpoint:     endpoint,
		Registration: registration, Target: target, Env: env, Policy: policy,
		StartMode: startMode,
	}
	request := ports.BrokerOpenStreamRequest{
		Epoch: candidate.Epoch, Purpose: candidate.Purpose,
		Admission: candidate.Admission, Name: candidate.Name, Local: candidate.Local,
		Connection: candidate.Connection, Stream: candidate.Stream,
		Endpoint: candidate.Endpoint, Registration: candidate.Registration,
		Target: candidate.Target, Env: candidate.Env, Policy: candidate.Policy,
		StartMode: candidate.StartMode,
	}
	if err := request.Validate(); err != nil {
		return OpenStream{}, ErrInvalidMessage
	}
	return candidate, nil
}

func clientStreamDataToWire(m ClientStreamData, maxChunkBytes uint64) (*wire.ClientStreamData, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Stream.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if uint64(len(m.Data)) > maxChunkBytes {
		return nil, ErrTooLarge
	}
	if uint64(len(m.Data)) > MaxStreamChunkBytes {
		return nil, ErrTooLarge
	}
	if len(m.Data) == 0 {
		return nil, ErrInvalidMessage
	}
	return &wire.ClientStreamData{
		Ref:  refToWire(m.Epoch, m.Connection, m.Stream),
		Data: append([]byte(nil), m.Data...),
	}, nil
}

func clientStreamDataFromWire(message *wire.ClientStreamData, maxChunkBytes uint64) (ClientStreamData, error) {
	var out ClientStreamData
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, stream, err := refFromWire(message.GetRef())
	if err != nil {
		return ClientStreamData{}, ErrInvalidMessage
	}
	data := message.GetData()
	if uint64(len(data)) > maxChunkBytes {
		return ClientStreamData{}, ErrTooLarge
	}
	if uint64(len(data)) > MaxStreamChunkBytes {
		return ClientStreamData{}, ErrTooLarge
	}
	if len(data) == 0 {
		return ClientStreamData{}, ErrInvalidMessage
	}
	return ClientStreamData{Epoch: epoch, Connection: connection, Stream: stream, Data: append([]byte(nil), data...)}, nil
}

func encodeServerEnvelope(message ServerMessage, maxChunkBytes uint64) (*wire.BrokerServerEnvelope, error) {
	if message == nil {
		return nil, ErrInvalidMessage
	}
	switch m := message.(type) {
	case Registered:
		if m.Epoch == 0 {
			return nil, ErrInvalidMessage
		}
		if err := m.Connection.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_Registered{Registered: &wire.Registered{
			Scope: scopeToWire(m.Epoch, m.Connection),
		}}}, nil
	case *Registered:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case SnapshotPart:
		converted, err := snapshotPartToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_SnapshotPart{SnapshotPart: converted}}, nil
	case *SnapshotPart:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case OperationResult:
		converted, err := operationResultToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_OperationResult{OperationResult: converted}}, nil
	case *OperationResult:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case StreamOpened:
		if m.Epoch == 0 {
			return nil, ErrInvalidMessage
		}
		if err := m.Connection.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		if err := m.Stream.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_StreamOpened{StreamOpened: &wire.StreamOpened{
			Ref: refToWire(m.Epoch, m.Connection, m.Stream),
		}}}, nil
	case *StreamOpened:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case ServerStreamData:
		converted, err := serverStreamDataToWire(m, maxChunkBytes)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_ServerStreamData{ServerStreamData: converted}}, nil
	case *ServerStreamData:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case StreamClosed:
		converted, err := streamClosedToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_StreamClosed{StreamClosed: converted}}, nil
	case *StreamClosed:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case Progress:
		converted, err := progressToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_Progress{Progress: converted}}, nil
	case *Progress:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case BrokerErrorMessage:
		if m.Epoch == 0 {
			return nil, ErrInvalidMessage
		}
		if err := m.Connection.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		if err := m.Error.validate(); err != nil {
			return nil, err
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_BrokerErrorMessage{BrokerErrorMessage: &wire.BrokerErrorMessage{
			Scope: scopeToWire(m.Epoch, m.Connection), Error: errorDetailToWire(m.Error),
		}}}, nil
	case *BrokerErrorMessage:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case Shutdown:
		converted, err := shutdownToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_Shutdown{Shutdown: converted}}, nil
	case *Shutdown:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	case PreviewPublication:
		converted, err := previewPublicationToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.BrokerServerEnvelope{Payload: &wire.BrokerServerEnvelope_PreviewPublication{PreviewPublication: converted}}, nil
	case *PreviewPublication:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeServerEnvelope(*m, maxChunkBytes)
	default:
		return nil, ErrWrongDirection
	}
}

func decodeServerEnvelope(envelope *wire.BrokerServerEnvelope, maxChunkBytes uint64) (ServerMessage, error) {
	if envelope == nil {
		return nil, ErrInvalidMessage
	}
	switch payload := envelope.Payload.(type) {
	case *wire.BrokerServerEnvelope_Registered:
		if payload.Registered == nil {
			return nil, ErrInvalidMessage
		}
		epoch, connection, err := scopeFromWire(payload.Registered.GetScope())
		if err != nil {
			return nil, ErrInvalidMessage
		}
		return Registered{Epoch: epoch, Connection: connection}, nil
	case *wire.BrokerServerEnvelope_SnapshotPart:
		return snapshotPartFromWire(payload.SnapshotPart)
	case *wire.BrokerServerEnvelope_OperationResult:
		return operationResultFromWire(payload.OperationResult)
	case *wire.BrokerServerEnvelope_StreamOpened:
		if payload.StreamOpened == nil {
			return nil, ErrInvalidMessage
		}
		epoch, connection, stream, err := refFromWire(payload.StreamOpened.GetRef())
		if err != nil {
			return nil, ErrInvalidMessage
		}
		return StreamOpened{Epoch: epoch, Connection: connection, Stream: stream}, nil
	case *wire.BrokerServerEnvelope_ServerStreamData:
		return serverStreamDataFromWire(payload.ServerStreamData, maxChunkBytes)
	case *wire.BrokerServerEnvelope_StreamClosed:
		return streamClosedFromWire(payload.StreamClosed)
	case *wire.BrokerServerEnvelope_Progress:
		return progressFromWire(payload.Progress)
	case *wire.BrokerServerEnvelope_BrokerErrorMessage:
		if payload.BrokerErrorMessage == nil {
			return nil, ErrInvalidMessage
		}
		epoch, connection, err := scopeFromWire(payload.BrokerErrorMessage.GetScope())
		if err != nil {
			return nil, ErrInvalidMessage
		}
		detail, err := errorDetailFromWire(payload.BrokerErrorMessage.GetError())
		if err != nil {
			return nil, ErrInvalidMessage
		}
		return BrokerErrorMessage{Epoch: epoch, Connection: connection, Error: detail}, nil
	case *wire.BrokerServerEnvelope_Shutdown:
		return shutdownFromWire(payload.Shutdown)
	case *wire.BrokerServerEnvelope_PreviewPublication:
		return previewPublicationFromWire(payload.PreviewPublication)
	default:
		return nil, ErrWrongDirection
	}
}

func snapshotPartToWire(m SnapshotPart) (*wire.SnapshotPart, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if m.Revision == 0 {
		return nil, ErrInvalidMessage
	}
	if m.Part == nil {
		return nil, ErrInvalidMessage
	}
	out := &wire.SnapshotPart{
		Scope:      scopeToWire(m.Epoch, m.Connection),
		Generation: m.Generation, Revision: uint64(m.Revision), Index: m.Index,
	}
	switch part := m.Part.(type) {
	case SnapshotBegin:
		if part.HostCount > ports.BrokerMaxDaemonsPerSnapshot ||
			part.SessionCount > ports.BrokerMaxDaemonsPerSnapshot*ports.BrokerMaxSessionsPerHost ||
			part.TombstoneCount > ports.BrokerMaxTombstones {
			return nil, ErrTooLarge
		}
		if part.LocalPresent && part.HostCount == 0 {
			return nil, ErrInvalidMessage
		}
		out.Part = &wire.SnapshotPart_Begin{Begin: &wire.SnapshotBegin{
			HostCount: part.HostCount, SessionCount: part.SessionCount, TombstoneCount: part.TombstoneCount,
			LocalPresent: part.LocalPresent,
		}}
	case *SnapshotBegin:
		if part == nil {
			return nil, ErrInvalidMessage
		}
		return snapshotPartToWire(SnapshotPart{Epoch: m.Epoch, Connection: m.Connection, Generation: m.Generation, Revision: m.Revision, Index: m.Index, Part: *part})
	case SnapshotDaemonPart:
		converted, err := snapshotDaemonToWire(part)
		if err != nil {
			return nil, err
		}
		out.Part = &wire.SnapshotPart_Daemon{Daemon: converted}
	case *SnapshotDaemonPart:
		if part == nil {
			return nil, ErrInvalidMessage
		}
		return snapshotPartToWire(SnapshotPart{Epoch: m.Epoch, Connection: m.Connection, Generation: m.Generation, Revision: m.Revision, Index: m.Index, Part: *part})
	case SnapshotSessionPart:
		converted, err := snapshotSessionToWire(part)
		if err != nil {
			return nil, err
		}
		out.Part = &wire.SnapshotPart_Session{Session: converted}
	case *SnapshotSessionPart:
		if part == nil {
			return nil, ErrInvalidMessage
		}
		return snapshotPartToWire(SnapshotPart{Epoch: m.Epoch, Connection: m.Connection, Generation: m.Generation, Revision: m.Revision, Index: m.Index, Part: *part})
	case SnapshotTombstonePart:
		if part.RetiredRevision == 0 {
			return nil, ErrInvalidMessage
		}
		if err := part.Registration.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		out.Part = &wire.SnapshotPart_Tombstone{Tombstone: &wire.SnapshotTombstone{
			TombstoneIndex:  part.TombstoneIndex,
			Registration:    registrationToWire(part.Registration),
			RetiredRevision: uint64(part.RetiredRevision),
		}}
	case *SnapshotTombstonePart:
		if part == nil {
			return nil, ErrInvalidMessage
		}
		return snapshotPartToWire(SnapshotPart{Epoch: m.Epoch, Connection: m.Connection, Generation: m.Generation, Revision: m.Revision, Index: m.Index, Part: *part})
	case SnapshotEnd:
		out.Part = &wire.SnapshotPart_End{End: &wire.SnapshotEnd{}}
	case *SnapshotEnd:
		if part == nil {
			return nil, ErrInvalidMessage
		}
		return snapshotPartToWire(SnapshotPart{Epoch: m.Epoch, Connection: m.Connection, Generation: m.Generation, Revision: m.Revision, Index: m.Index, Part: *part})
	default:
		return nil, ErrWrongDirection
	}
	return out, nil
}

func snapshotPartFromWire(message *wire.SnapshotPart) (SnapshotPart, error) {
	var out SnapshotPart
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return SnapshotPart{}, ErrInvalidMessage
	}
	if message.GetRevision() == 0 {
		return SnapshotPart{}, ErrInvalidMessage
	}
	part := SnapshotPart{
		Epoch: epoch, Connection: connection,
		Generation: message.GetGeneration(), Revision: ports.BrokerRevision(message.GetRevision()),
		Index: message.GetIndex(),
	}
	switch payload := message.GetPart().(type) {
	case *wire.SnapshotPart_Begin:
		if payload.Begin == nil {
			return SnapshotPart{}, ErrInvalidMessage
		}
		if payload.Begin.GetHostCount() > ports.BrokerMaxDaemonsPerSnapshot ||
			payload.Begin.GetSessionCount() > ports.BrokerMaxDaemonsPerSnapshot*ports.BrokerMaxSessionsPerHost ||
			payload.Begin.GetTombstoneCount() > ports.BrokerMaxTombstones {
			return SnapshotPart{}, ErrTooLarge
		}
		if payload.Begin.GetLocalPresent() && payload.Begin.GetHostCount() == 0 {
			return SnapshotPart{}, ErrInvalidMessage
		}
		part.Part = SnapshotBegin{
			HostCount:      payload.Begin.GetHostCount(),
			SessionCount:   payload.Begin.GetSessionCount(),
			TombstoneCount: payload.Begin.GetTombstoneCount(),
			LocalPresent:   payload.Begin.GetLocalPresent(),
		}
	case *wire.SnapshotPart_Daemon:
		if payload.Daemon == nil {
			return SnapshotPart{}, ErrInvalidMessage
		}
		daemon, index, sessionCount, err := snapshotDaemonFromWire(payload.Daemon)
		if err != nil {
			return SnapshotPart{}, err
		}
		part.Part = SnapshotDaemonPart{HostIndex: index, Daemon: daemon, SessionCount: sessionCount}
	case *wire.SnapshotPart_Session:
		if payload.Session == nil {
			return SnapshotPart{}, ErrInvalidMessage
		}
		session, hostIndex, sessionIndex, local, err := snapshotSessionFromWire(payload.Session)
		if err != nil {
			return SnapshotPart{}, err
		}
		part.Part = SnapshotSessionPart{HostIndex: hostIndex, SessionIndex: sessionIndex, Local: local, Session: session}
	case *wire.SnapshotPart_Tombstone:
		if payload.Tombstone == nil {
			return SnapshotPart{}, ErrInvalidMessage
		}
		registration, err := registrationFromWire(payload.Tombstone.GetRegistration())
		if err != nil {
			return SnapshotPart{}, ErrInvalidMessage
		}
		if payload.Tombstone.GetRetiredRevision() == 0 {
			return SnapshotPart{}, ErrInvalidMessage
		}
		part.Part = SnapshotTombstonePart{
			TombstoneIndex:  payload.Tombstone.GetTombstoneIndex(),
			Registration:    registration,
			RetiredRevision: ports.BrokerRevision(payload.Tombstone.GetRetiredRevision()),
		}
	case *wire.SnapshotPart_End:
		if payload.End == nil {
			return SnapshotPart{}, ErrInvalidMessage
		}
		part.Part = SnapshotEnd{}
	default:
		return SnapshotPart{}, ErrWrongDirection
	}
	return part, nil
}

// snapshotDaemonToWire converts one daemon part losslessly. It refuses a
// malformed local/remote authority pairing, a partial identity, an
// out-of-range enum or rank, a session inventory that could not be durable,
// inline sessions (they travel separately), and any index or count above
// the snapshot bounds.
func snapshotDaemonToWire(part SnapshotDaemonPart) (*wire.SnapshotDaemon, error) {
	if part.HostIndex >= ports.BrokerMaxDaemonsPerSnapshot {
		return nil, ErrTooLarge
	}
	daemon := part.Daemon
	if err := validateSnapshotDaemonFields(daemon); err != nil {
		return nil, err
	}
	if err := daemon.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if !daemon.Local {
		if err := ports.ValidateDurableHostProjection(daemon); err != nil {
			return nil, ErrInvalidMessage
		}
	}
	if len(daemon.Sessions) != 0 {
		return nil, ErrInvalidMessage
	}
	if part.SessionCount > ports.BrokerMaxSessionsPerHost {
		return nil, ErrTooLarge
	}
	lastAttemptSeconds, lastAttemptNanos := brokerTimeToWire(daemon.LastAttempt)
	lastSuccessSeconds, lastSuccessNanos := brokerTimeToWire(daemon.LastSuccess)
	nextDueSeconds, nextDueNanos := brokerTimeToWire(daemon.NextDue)
	var registration *wire.RemoteRegistration
	if !daemon.Local {
		registration = registrationToWire(daemon.Registration)
	}
	var incarnation []byte
	if !daemon.Incarnation.IsZero() {
		incarnation = append([]byte(nil), daemon.Incarnation[:]...)
	}
	return &wire.SnapshotDaemon{
		HostIndex: part.HostIndex, Local: daemon.Local,
		Endpoint: daemon.Endpoint, DisplayOrigin: daemon.DisplayOrigin,
		Rank: uint32(daemon.Rank), Registration: registration,
		Policy:         policyToWire(daemon.Policy),
		DaemonIdentity: string(daemon.Identity), DaemonIncarnation: incarnation,
		ProtocolVersion: uint32(daemon.ProtocolVersion), Capabilities: daemon.Capabilities,
		Availability: uint32(daemon.Availability), Checking: daemon.Checking,
		LastAttempt:         &wire.BrokerTimestamp{Seconds: lastAttemptSeconds, Nanos: lastAttemptNanos},
		LastSuccess:         &wire.BrokerTimestamp{Seconds: lastSuccessSeconds, Nanos: lastSuccessNanos},
		NextDue:             &wire.BrokerTimestamp{Seconds: nextDueSeconds, Nanos: nextDueNanos},
		ConsecutiveFailures: uint64(daemon.ConsecutiveFailures), FailureEpisode: daemon.FailureEpisode,
		FailureKind:    uint32(daemon.LastFailure.Kind),
		InventoryKnown: daemon.InventoryKnown, SessionCount: part.SessionCount,
	}, nil
}

func snapshotDaemonFromWire(message *wire.SnapshotDaemon) (ports.BrokerDaemonObservation, uint32, uint32, error) {
	var daemon ports.BrokerDaemonObservation
	if message == nil {
		return daemon, 0, 0, ErrInvalidMessage
	}
	if message.GetHostIndex() >= ports.BrokerMaxDaemonsPerSnapshot {
		return daemon, 0, 0, ErrTooLarge
	}
	local := message.GetLocal()
	endpoint := message.GetEndpoint()
	origin := message.GetDisplayOrigin()
	if err := validateBrokerOrigin(origin); err != nil {
		return daemon, 0, 0, err
	}
	var registration domain.RemoteRegistration
	if local {
		if message.GetRegistration() != nil || endpoint != "" {
			return daemon, 0, 0, ErrInvalidMessage
		}
	} else {
		if err := validateBrokerEndpoint(endpoint); err != nil {
			return daemon, 0, 0, err
		}
		converted, err := registrationFromWire(message.GetRegistration())
		if err != nil {
			return daemon, 0, 0, ErrInvalidMessage
		}
		if endpoint != converted.Endpoint {
			return daemon, 0, 0, ErrInvalidMessage
		}
		registration = converted
	}
	policy, err := policyFromWire(message.GetPolicy())
	if err != nil {
		return daemon, 0, 0, ErrInvalidMessage
	}
	raw := message.GetDaemonIncarnation()
	if len(raw) != 0 && len(raw) != len(daemon.Incarnation) {
		return daemon, 0, 0, errConvertRange
	}
	if len(raw) == len(daemon.Incarnation) {
		copy(daemon.Incarnation[:], raw)
	}
	protocolVersion, err := brokerEnum16[uint16](message.GetProtocolVersion())
	if err != nil {
		return daemon, 0, 0, ErrInvalidMessage
	}
	availability, err := brokerEnum8[domain.RemoteAvailability](message.GetAvailability())
	if err != nil {
		return daemon, 0, 0, ErrInvalidMessage
	}
	if availability < domain.RemoteAvailabilityUnknown || availability > domain.RemoteAvailabilityNoDaemon {
		return daemon, 0, 0, ErrInvalidMessage
	}
	failureKind, err := brokerEnum8[domain.RemoteFailureKind](message.GetFailureKind())
	if err != nil {
		return daemon, 0, 0, ErrInvalidMessage
	}
	if failureKind > domain.RemoteFailureInvalidResponse {
		return daemon, 0, 0, ErrInvalidMessage
	}
	lastAttempt, err := brokerTimeFromWire(message.GetLastAttempt().GetSeconds(), message.GetLastAttempt().GetNanos())
	if err != nil {
		return daemon, 0, 0, ErrInvalidMessage
	}
	lastSuccess, err := brokerTimeFromWire(message.GetLastSuccess().GetSeconds(), message.GetLastSuccess().GetNanos())
	if err != nil {
		return daemon, 0, 0, ErrInvalidMessage
	}
	nextDue, err := brokerTimeFromWire(message.GetNextDue().GetSeconds(), message.GetNextDue().GetNanos())
	if err != nil {
		return daemon, 0, 0, ErrInvalidMessage
	}
	if message.GetConsecutiveFailures() > math.MaxUint32 {
		return daemon, 0, 0, errConvertRange
	}
	if message.GetSessionCount() > ports.BrokerMaxSessionsPerHost {
		return daemon, 0, 0, ErrTooLarge
	}
	daemon = ports.BrokerDaemonObservation{
		Local: local, Endpoint: endpoint, DisplayOrigin: origin, Rank: int(message.GetRank()),
		Registration: registration, Policy: policy,
		Identity: ports.BrokerDaemonIdentity(message.GetDaemonIdentity()), Incarnation: daemon.Incarnation,
		ProtocolVersion: protocolVersion, Capabilities: message.GetCapabilities(),
		Availability: availability, Checking: message.GetChecking(),
		LastAttempt: lastAttempt, LastSuccess: lastSuccess, NextDue: nextDue,
		ConsecutiveFailures: uint(message.GetConsecutiveFailures()), FailureEpisode: message.GetFailureEpisode(),
		LastFailure:    domain.RemoteFailure{Kind: failureKind},
		InventoryKnown: message.GetInventoryKnown(),
		Sessions:       []catalogue.RemoteCatalogSession{},
	}
	if err := validateSnapshotDaemonFields(daemon); err != nil {
		return daemon, 0, 0, err
	}
	if err := daemon.Validate(); err != nil {
		return daemon, 0, 0, ErrInvalidMessage
	}
	return daemon, message.GetHostIndex(), message.GetSessionCount(), nil
}

func snapshotSessionToWire(part SnapshotSessionPart) (*wire.SnapshotSession, error) {
	if part.Local {
		if part.HostIndex != 0 {
			return nil, ErrInvalidMessage
		}
	} else if part.HostIndex >= ports.BrokerMaxDaemonsPerSnapshot {
		return nil, ErrTooLarge
	}
	if part.SessionIndex >= ports.BrokerMaxSessionsPerHost {
		return nil, ErrTooLarge
	}
	if err := validateCatalogSession(part.Session); err != nil {
		return nil, err
	}
	return &wire.SnapshotSession{
		HostIndex: part.HostIndex, SessionIndex: part.SessionIndex,
		Local:   part.Local,
		Session: catalogSessionToWire(part.Session),
	}, nil
}

func snapshotSessionFromWire(message *wire.SnapshotSession) (catalogue.RemoteCatalogSession, uint32, uint32, bool, error) {
	var session catalogue.RemoteCatalogSession
	if message == nil {
		return session, 0, 0, false, ErrInvalidMessage
	}
	local := message.GetLocal()
	if local {
		if message.GetHostIndex() != 0 {
			return session, 0, 0, false, ErrInvalidMessage
		}
	} else if message.GetHostIndex() >= ports.BrokerMaxDaemonsPerSnapshot {
		return session, 0, 0, false, ErrTooLarge
	}
	if message.GetSessionIndex() >= ports.BrokerMaxSessionsPerHost {
		return session, 0, 0, false, ErrTooLarge
	}
	converted, err := catalogSessionFromWire(message.GetSession())
	if err != nil {
		return session, 0, 0, false, err
	}
	return converted, message.GetHostIndex(), message.GetSessionIndex(), local, nil
}

func operationResultToWire(m OperationResult) (*wire.OperationResult, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Operation.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Outcome.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	var outcome uint32
	switch m.Outcome {
	case ports.BrokerOutcomeOK:
		outcome = 1
	case ports.BrokerOutcomeFailed:
		outcome = 2
	case ports.BrokerOutcomeUnknown:
		outcome = 3
	default:
		return nil, ErrInvalidMessage
	}
	result := ports.BrokerOperationResult{Operation: m.Operation, Outcome: m.Outcome}
	if m.HasError {
		if err := m.Error.validate(); err != nil {
			return nil, err
		}
		result.Text = m.Error.Text
	} else if m.Error != (ErrorDetail{}) {
		// HasError=false is exactly the absent detail: a nonzero
		// ErrorDetail would otherwise be silently dropped instead of
		// travelling, so the whole result fails closed.
		return nil, ErrInvalidMessage
	}
	if err := result.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	hasRegistration := m.Registration != (domain.RemoteRegistration{})
	// The presence rules that hold without knowing the request kind: removal
	// and registration never travel together, a failed or outcome-unknown
	// result carries neither authority and must carry its typed error, and a
	// successful result carries no error. The per-kind rule (an
	// AddHost/UpdateHostPolicy success needs a registration, a RemoveHost
	// success must not) is enforced where the pending request is known; the
	// stateless codec has no such state.
	if err := validateResultAuthority(m.Outcome, m.Removed, hasRegistration, m.HasError); err != nil {
		return nil, err
	}
	var registration *wire.RemoteRegistration
	if hasRegistration {
		if err := m.Registration.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		registration = registrationToWire(m.Registration)
	}
	out := &wire.OperationResult{
		Scope:       scopeToWire(m.Epoch, m.Connection),
		OperationId: operationToWire(m.Operation),
		Outcome:     outcome, Removed: m.Removed,
		Registration: registration,
	}
	if m.HasError {
		out.Error = errorDetailToWire(m.Error)
	}
	return out, nil
}

func operationResultFromWire(message *wire.OperationResult) (OperationResult, error) {
	var out OperationResult
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return OperationResult{}, ErrInvalidMessage
	}
	operation, err := operationFromWire(message.GetOperationId())
	if err != nil {
		return OperationResult{}, ErrInvalidMessage
	}
	var outcome ports.BrokerMutationOutcome
	switch message.GetOutcome() {
	case 1:
		outcome = ports.BrokerOutcomeOK
	case 2:
		outcome = ports.BrokerOutcomeFailed
	case 3:
		outcome = ports.BrokerOutcomeUnknown
	default:
		return OperationResult{}, ErrInvalidMessage
	}
	var registration domain.RemoteRegistration
	if message.GetRegistration() != nil {
		converted, err := registrationFromWire(message.GetRegistration())
		if err != nil {
			return OperationResult{}, ErrInvalidMessage
		}
		registration = converted
	}
	result := OperationResult{
		Epoch: epoch, Connection: connection, Operation: operation,
		Outcome: outcome, Removed: message.GetRemoved(),
		Registration: registration,
	}
	if message.GetError() != nil {
		detail, err := errorDetailFromWire(message.GetError())
		if err != nil {
			return OperationResult{}, ErrInvalidMessage
		}
		result.Error = detail
		result.HasError = true
	}
	if err := validateResultAuthority(result.Outcome, result.Removed, registration != (domain.RemoteRegistration{}), result.HasError); err != nil {
		return OperationResult{}, err
	}
	semantic := ports.BrokerOperationResult{Operation: result.Operation, Outcome: result.Outcome}
	if result.HasError {
		semantic.Text = result.Error.Text
	}
	if err := semantic.Validate(); err != nil {
		return OperationResult{}, ErrInvalidMessage
	}
	return result, nil
}

func serverStreamDataToWire(m ServerStreamData, maxChunkBytes uint64) (*wire.ServerStreamData, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Stream.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if uint64(len(m.Data)) > maxChunkBytes {
		return nil, ErrTooLarge
	}
	if uint64(len(m.Data)) > MaxStreamChunkBytes {
		return nil, ErrTooLarge
	}
	if len(m.Data) == 0 {
		return nil, ErrInvalidMessage
	}
	return &wire.ServerStreamData{
		Ref:  refToWire(m.Epoch, m.Connection, m.Stream),
		Data: append([]byte(nil), m.Data...),
	}, nil
}

func serverStreamDataFromWire(message *wire.ServerStreamData, maxChunkBytes uint64) (ServerStreamData, error) {
	var out ServerStreamData
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, stream, err := refFromWire(message.GetRef())
	if err != nil {
		return ServerStreamData{}, ErrInvalidMessage
	}
	data := message.GetData()
	if uint64(len(data)) > maxChunkBytes {
		return ServerStreamData{}, ErrTooLarge
	}
	if uint64(len(data)) > MaxStreamChunkBytes {
		return ServerStreamData{}, ErrTooLarge
	}
	if len(data) == 0 {
		return ServerStreamData{}, ErrInvalidMessage
	}
	return ServerStreamData{Epoch: epoch, Connection: connection, Stream: stream, Data: append([]byte(nil), data...)}, nil
}

func streamClosedToWire(m StreamClosed) (*wire.StreamClosed, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Stream.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if m.HasError {
		if err := m.Error.validate(); err != nil {
			return nil, err
		}
	} else if m.Error != (ErrorDetail{}) {
		// An orderly close carries no detail at all; a nonzero one under
		// HasError=false is a caller bug that must fail closed.
		return nil, ErrInvalidMessage
	}
	out := &wire.StreamClosed{Ref: refToWire(m.Epoch, m.Connection, m.Stream)}
	if m.HasError {
		out.Error = errorDetailToWire(m.Error)
	}
	return out, nil
}

func streamClosedFromWire(message *wire.StreamClosed) (StreamClosed, error) {
	var out StreamClosed
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, stream, err := refFromWire(message.GetRef())
	if err != nil {
		return StreamClosed{}, ErrInvalidMessage
	}
	result := StreamClosed{Epoch: epoch, Connection: connection, Stream: stream}
	if message.GetError() != nil {
		detail, err := errorDetailFromWire(message.GetError())
		if err != nil {
			return StreamClosed{}, ErrInvalidMessage
		}
		result.Error = detail
		result.HasError = true
	}
	return result, nil
}

func progressToWire(m Progress) (*wire.Progress, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Stream.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Phase.validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := validateBrokerDisplayText(m.Text, ports.BrokerMaxErrorBytes); err != nil {
		return nil, err
	}
	return &wire.Progress{
		Ref:   refToWire(m.Epoch, m.Connection, m.Stream),
		Phase: uint32(m.Phase), Text: m.Text,
	}, nil
}

func progressFromWire(message *wire.Progress) (Progress, error) {
	var out Progress
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, stream, err := refFromWire(message.GetRef())
	if err != nil {
		return Progress{}, ErrInvalidMessage
	}
	phase := BrokerProgressPhase(message.GetPhase())
	if err := phase.validate(); err != nil {
		return Progress{}, ErrInvalidMessage
	}
	if err := validateBrokerDisplayText(message.GetText(), ports.BrokerMaxErrorBytes); err != nil {
		return Progress{}, err
	}
	return Progress{Epoch: epoch, Connection: connection, Stream: stream, Phase: phase, Text: message.GetText()}, nil
}

func shutdownToWire(m Shutdown) (*wire.Shutdown, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Reason.validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := validateBrokerDisplayText(m.Text, ports.BrokerMaxErrorBytes); err != nil {
		return nil, err
	}
	var reason uint32
	switch m.Reason {
	case ShutdownIdleExit:
		reason = 1
	case ShutdownTerminating:
		reason = 2
	}
	return &wire.Shutdown{
		Scope:  scopeToWire(m.Epoch, m.Connection),
		Reason: reason, Text: m.Text,
	}, nil
}

func shutdownFromWire(message *wire.Shutdown) (Shutdown, error) {
	var out Shutdown
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return Shutdown{}, ErrInvalidMessage
	}
	var reason ShutdownReason
	switch message.GetReason() {
	case 1:
		reason = ShutdownIdleExit
	case 2:
		reason = ShutdownTerminating
	default:
		return Shutdown{}, ErrInvalidMessage
	}
	if err := validateBrokerDisplayText(message.GetText(), ports.BrokerMaxErrorBytes); err != nil {
		return Shutdown{}, err
	}
	return Shutdown{Epoch: epoch, Connection: connection, Reason: reason, Text: message.GetText()}, nil
}
