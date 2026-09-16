package daemonmux

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

var (
	// ErrWrongDirection reports a daemonmux message presented to the wrong
	// directional codec. Direction is a wire property of the envelope oneof:
	// a client envelope never decodes as daemon output and vice versa.
	ErrWrongDirection = errors.New("daemonmux: wrong message direction")
	// ErrInvalidMessage reports a daemonmux message that fails semantic
	// validation: bad stream identity, text, taxonomy, bound, environment,
	// policy, or error detail.
	ErrInvalidMessage = errors.New("daemonmux: invalid message")
	// ErrTooLarge reports a stateless bound refusal established by limits.go:
	// an envelope, chunk, environment, policy token, or error text above its
	// ceiling.
	ErrTooLarge     = errors.New("daemonmux: message exceeds bound")
	errConvertRange = errors.New("daemonmux: wire value out of semantic range")
)

// ClientMessage is the closed set of broker-to-daemon daemonmux messages.
// Every variant converts to exactly one MuxClientEnvelope payload.
type ClientMessage interface{ muxClientMessage() }

// ServerMessage is the closed set of daemon-to-broker daemonmux messages.
// Every variant converts to exactly one MuxServerEnvelope payload.
type ServerMessage interface{ muxServerMessage() }

// PhysicalStreamID identifies one logical stream multiplexed over one
// physical daemon connection. It is asserted by the opening side and never
// crosses a physical connection; the engine (not this codec) enforces that a
// connection's IDs are strictly increasing, so the codec only rejects zero.
type PhysicalStreamID uint64

// Validate rejects the zero value: a stream is never routed without an
// explicit physical identity.
func (id PhysicalStreamID) Validate() error {
	if id == 0 {
		return errors.New("daemonmux: physical stream ID is zero")
	}
	return nil
}

// StreamRef is the full identity of one multiplexed logical stream: the
// mux-local physical stream, the broker epoch and client connection that own
// it, and the broker client stream it carries. It is carried by MuxOpen,
// MuxOpened, and MuxRefused only; later frames are routed by the physical
// stream ID alone.
type StreamRef struct {
	Physical   PhysicalStreamID
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Client     ports.BrokerStreamID
}

// Validate enforces the complete reference: a nonzero physical and broker
// stream, a nonzero epoch, and a nonzero 16-byte connection identity.
func (r StreamRef) Validate() error {
	if err := r.Physical.Validate(); err != nil {
		return err
	}
	if r.Epoch == 0 {
		return errors.New("daemonmux: stream ref has no broker epoch")
	}
	if err := r.Connection.Validate(); err != nil {
		return err
	}
	return r.Client.Validate()
}

// Open asks the daemon to open one independently cancellable logical stream
// over the physical connection. Env is the per-request session environment
// and is never inherited from the broker process environment.
type Open struct {
	Ref          StreamRef
	Purpose      ports.BrokerStreamPurpose
	Local        bool
	Endpoint     string
	Registration domain.RemoteRegistration
	Target       protocol.ExactSessionTarget
	Env          []string
	Policy       ports.BrokerPolicy
}

func (Open) muxClientMessage() {}

// Data carries one opaque stream frame. It travels in both directions over
// the same wire payload.
type Data struct {
	Physical PhysicalStreamID
	Data     []byte
}

func (Data) muxClientMessage() {}
func (Data) muxServerMessage() {}

// Close ends one logical stream. It travels in both directions over the same
// wire payload.
type Close struct {
	Physical PhysicalStreamID
}

func (Close) muxClientMessage() {}
func (Close) muxServerMessage() {}

// Reset aborts one logical stream. It travels in both directions over the
// same wire payload. HasError=false means no detail at all: encode refuses a
// nonzero Error rather than silently dropping it.
type Reset struct {
	Physical PhysicalStreamID
	Error    ErrorDetail
	HasError bool
}

func (Reset) muxClientMessage() {}
func (Reset) muxServerMessage() {}

// Opened confirms one logical stream is established.
type Opened struct {
	Ref StreamRef
}

func (Opened) muxServerMessage() {}

// Refused refuses one logical stream open. The typed error is mandatory: a
// refusal without a reason could not be classified.
type Refused struct {
	Ref   StreamRef
	Error ErrorDetail
}

func (Refused) muxServerMessage() {}

// MaxAdmissionCode is the highest admission-refusal code carried on
// ErrorDetail. The taxonomy is closed: 0 is none, 1 limit, 2 closed, 3 stale
// connection or stream, 4 invalid request (mirroring
// ports.BrokerAdmissionError).
const MaxAdmissionCode uint32 = 4

// MaxFailureKind bounds ErrorDetail.FailureKind to the domain
// transport-failure taxonomy. The zero value (RemoteFailureNone) is valid.
const MaxFailureKind = domain.RemoteFailureInvalidResponse

// ErrorDetail is the typed daemonmux failure carried to the broker: a closed
// error code plus bounded display text, the admission refusal (if any), and
// the sanitized transport cause. The underlying diagnostic cause is local-only
// and never travels on this wire.
type ErrorDetail struct {
	Code ports.BrokerErrorCode
	Text string
	// AdmissionCode is the closed admission-refusal taxonomy: 0 is none,
	// 1 limit, 2 closed, 3 stale connection or stream, 4 invalid request.
	AdmissionCode uint32
	FailureKind   domain.RemoteFailureKind
}

// validate enforces the transport contract of one ErrorDetail: a closed
// error code, bounded presentation-safe text, an admission code within the
// closed refusal taxonomy, and a failure kind within the domain taxonomy.
func (d ErrorDetail) validate() error {
	if err := d.Code.Validate(); err != nil {
		return ErrInvalidMessage
	}
	if d.AdmissionCode > MaxAdmissionCode {
		return ErrInvalidMessage
	}
	if d.FailureKind > MaxFailureKind {
		return ErrInvalidMessage
	}
	return validateDisplayText(d.Text, ports.BrokerMaxErrorBytes)
}
