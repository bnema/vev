package brokerwire

import (
	"errors"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

var (
	// ErrWrongDirection reports a broker message presented to the wrong
	// directional codec. Direction is a closed oneof property: a client
	// envelope never decodes as server output and vice versa.
	ErrWrongDirection = errors.New("brokerwire: wrong message direction")
	// ErrInvalidMessage reports a broker message that fails semantic
	// validation: bad identity, text, taxonomy, bound, time, environment,
	// policy, chunk, or snapshot part.
	ErrInvalidMessage = errors.New("brokerwire: invalid message")
	// ErrTooLarge reports a stateless bound refusal established by limits.go:
	// an envelope, chunk, environment, policy token, error text, snapshot
	// part, or catalogue value above its ceiling.
	ErrTooLarge     = errors.New("brokerwire: message exceeds bound")
	errConvertRange = errors.New("brokerwire: wire value out of semantic range")
)

// ClientMessage is the closed set of client-to-broker messages. Every
// variant converts to exactly one BrokerClientEnvelope payload.
type ClientMessage interface{ brokerClientMessage() }

// ServerMessage is the closed set of broker-to-client messages. Every
// variant converts to exactly one BrokerServerEnvelope payload.
type ServerMessage interface{ brokerServerMessage() }

// Register opens one client connection to the broker.
type Register struct{}

func (Register) brokerClientMessage() {}

// Subscribe asks for snapshot and lifecycle publications for one scope.
type Subscribe struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Generation SubscriptionGeneration
	// Passive marks a read-only snapshot reader that must never count as
	// observation demand.
	Passive bool
}

func (Subscribe) brokerClientMessage() {}

// Resync asks for a full snapshot publication after an epoch change or a
// gap in the client's subscription series.
type Resync struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Generation SubscriptionGeneration
}

func (Resync) brokerClientMessage() {}

// Unsubscribe stops publications for one scope.
type Unsubscribe struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Generation SubscriptionGeneration
}

func (Unsubscribe) brokerClientMessage() {}

// AddHost adds one configured host registration under an exact policy.
// Operation is the idempotency identity of this mutating operation. Policy
// is required authority: an absent or invalid policy is refused rather than
// inherited, so a membership addition can never be authorized by a zero
// policy.
type AddHost struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Operation  ports.BrokerOperationID
	Endpoint   string
	Policy     ports.BrokerPolicy
}

func (AddHost) brokerClientMessage() {}

// RemoveHost removes exactly one configured host registration: the request
// carries the full expected authority (endpoint, incarnation, and
// generation), never a bare endpoint name, so a stale removal can never
// retire a re-added host.
type RemoveHost struct {
	Epoch        ports.BrokerEpoch
	Connection   ports.BrokerConnectionID
	Operation    ports.BrokerOperationID
	Registration domain.RemoteRegistration
}

func (RemoveHost) brokerClientMessage() {}

// UpdateHostPolicy replaces the policy of exactly one configured
// registration. Registration is the exact expected authority and Policy is
// the required replacement, so an update can neither target an endpoint
// name nor be authorized by a zero policy.
type UpdateHostPolicy struct {
	Epoch        ports.BrokerEpoch
	Connection   ports.BrokerConnectionID
	Operation    ports.BrokerOperationID
	Registration domain.RemoteRegistration
	Policy       ports.BrokerPolicy
}

func (UpdateHostPolicy) brokerClientMessage() {}

// Reconcile hands the broker one exact registration to probe.
type Reconcile struct {
	Epoch        ports.BrokerEpoch
	Connection   ports.BrokerConnectionID
	Registration domain.RemoteRegistration
}

func (Reconcile) brokerClientMessage() {}

// OpenStream asks the broker to open one independently cancellable logical
// stream to the owning daemon. Env is the per-request session environment
// and is never inherited from the broker process environment. Admission
// selects the attachment admission variant; Name is the validated session
// name for create-named and is empty otherwise. StartMode is the explicit
// daemon-start authorization carried to the transport; it is never zero on an
// admitted request.
type OpenStream struct {
	Epoch        ports.BrokerEpoch
	Connection   ports.BrokerConnectionID
	Stream       ports.BrokerStreamID
	Purpose      ports.BrokerStreamPurpose
	Admission    ports.BrokerStreamAdmission
	Name         string
	Local        bool
	Endpoint     string
	Registration domain.RemoteRegistration
	Target       protocol.ExactSessionTarget
	Env          []string
	Policy       ports.BrokerPolicy
	StartMode    ports.BrokerDaemonStartMode
}

func (OpenStream) brokerClientMessage() {}

// ClientStreamData carries one opaque client-to-daemon stream frame.
type ClientStreamData struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Stream     ports.BrokerStreamID
	Data       []byte
}

func (ClientStreamData) brokerClientMessage() {}

// CloseStream closes one logical stream.
type CloseStream struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Stream     ports.BrokerStreamID
}

func (CloseStream) brokerClientMessage() {}

// StartPreview starts or replaces one connection-scoped live preview.
// Generation is the connection-scoped preview authority; Route names the
// observed daemon (the broker allocates the stream) and Preview is the bounded
// viewport request sent to it.
type StartPreview struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Generation ports.BrokerPreviewGeneration
	Route      ports.BrokerPreviewRoute
	Preview    protocol.RemotePreviewRequest
}

func (StartPreview) brokerClientMessage() {}

// CancelPreview cancels one connection-scoped live preview.
type CancelPreview struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Generation ports.BrokerPreviewGeneration
}

func (CancelPreview) brokerClientMessage() {}

// Registered confirms one accepted client connection and its assigned scope.
type Registered struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
}

func (Registered) brokerServerMessage() {}

// SnapshotPart is one fragment of a snapshot publication. Parts of one
// epoch generation and broker revision arrive in index order and carry
// exactly one nested payload.
type SnapshotPart struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	// Generation is the client's subscription series, echoed back so the
	// client can drop parts bound to a superseded subscription.
	Generation SubscriptionGeneration
	// Revision is the broker publication revision within the epoch.
	Revision ports.BrokerRevision
	Index    uint32
	Part     SnapshotPartPayload
}

func (SnapshotPart) brokerServerMessage() {}

// SnapshotPartPayload is the closed set of snapshot fragment payloads.
type SnapshotPartPayload interface{ snapshotPartPayload() }

// SnapshotBegin opens a snapshot publication with its element counts.
// HostCount counts every daemon part in the transfer (the local daemon plus
// every remote host); LocalPresent reports whether daemon index 0 is local.
type SnapshotBegin struct {
	HostCount      uint32
	SessionCount   uint32
	TombstoneCount uint32
	LocalPresent   bool
}

func (SnapshotBegin) snapshotPartPayload() {}

// SnapshotDaemonPart is one daemon projection. Sessions travel as separate
// SnapshotSessionPart parts bound by daemon index; SessionCount advertises
// how many follow. The embedded Daemon must carry no inline sessions.
type SnapshotDaemonPart struct {
	HostIndex    uint32
	Daemon       ports.BrokerDaemonObservation
	SessionCount uint32
}

func (SnapshotDaemonPart) snapshotPartPayload() {}

// SnapshotSessionPart is one catalogue session bound to its daemon by index.
// Local marks a session of the local daemon: HostIndex is then exactly 0.
type SnapshotSessionPart struct {
	HostIndex    uint32
	SessionIndex uint32
	Local        bool
	Session      catalogue.RemoteCatalogSession
}

func (SnapshotSessionPart) snapshotPartPayload() {}

// SnapshotTombstonePart fences one retired host registration so a stale
// probe cannot revive authority for that exact incarnation.
type SnapshotTombstonePart struct {
	TombstoneIndex  uint32
	Registration    domain.RemoteRegistration
	RetiredRevision ports.BrokerRevision
}

func (SnapshotTombstonePart) snapshotPartPayload() {}

// SnapshotEnd closes a snapshot publication.
type SnapshotEnd struct{}

func (SnapshotEnd) snapshotPartPayload() {}

// OperationResult is exactly one completion for one admitted mutating
// operation. Removed and Registration are presence-carrying authority: a
// successful addition or policy update carries the resulting registration,
// a successful removal reports Removed and carries no registration, and a
// failed or outcome-unknown result carries neither authority, only its
// typed error. Error carries the typed failure when Outcome is failed or
// outcome-unknown; HasError=false means no detail at all, and both a
// nonzero Error under HasError=false and an error on a successful outcome
// are refused rather than dropped.
type OperationResult struct {
	Epoch        ports.BrokerEpoch
	Connection   ports.BrokerConnectionID
	Operation    ports.BrokerOperationID
	Outcome      ports.BrokerMutationOutcome
	Removed      bool
	Registration domain.RemoteRegistration
	Error        ErrorDetail
	HasError     bool
}

func (OperationResult) brokerServerMessage() {}

// RegisterMutationKind is the closed taxonomy of membership mutations. A
// result's required authority depends on which request produced it, so the
// taxonomy is explicit instead of being re-derived from a message type at
// each result site.
type RegisterMutationKind uint8

const (
	// MutationKindAddHost is an AddHost request: its success carries the
	// authoritative registration.
	MutationKindAddHost RegisterMutationKind = iota + 1
	// MutationKindRemoveHost is a RemoveHost request: its success reports
	// removal and carries no registration.
	MutationKindRemoveHost
	// MutationKindUpdateHostPolicy is an UpdateHostPolicy request: its
	// success carries the advanced registration.
	MutationKindUpdateHostPolicy
)

func (k RegisterMutationKind) String() string {
	switch k {
	case MutationKindAddHost:
		return "add_host"
	case MutationKindRemoveHost:
		return "remove_host"
	case MutationKindUpdateHostPolicy:
		return "update_host_policy"
	default:
		return "unknown"
	}
}

// Validate rejects a kind outside the closed membership-mutation taxonomy.
func (k RegisterMutationKind) Validate() error {
	switch k {
	case MutationKindAddHost, MutationKindRemoveHost, MutationKindUpdateHostPolicy:
		return nil
	default:
		return errConvertRange
	}
}

// MutationKindOf classifies one client message that mutates host membership.
// It is the request-side authority a per-kind result rule needs; pending
// operation tracking is deliberately not wired here.
func MutationKindOf(message ClientMessage) (RegisterMutationKind, bool) {
	switch m := message.(type) {
	case AddHost:
		return MutationKindAddHost, true
	case *AddHost:
		if m == nil {
			return 0, false
		}
		return MutationKindAddHost, true
	case RemoveHost:
		return MutationKindRemoveHost, true
	case *RemoveHost:
		if m == nil {
			return 0, false
		}
		return MutationKindRemoveHost, true
	case UpdateHostPolicy:
		return MutationKindUpdateHostPolicy, true
	case *UpdateHostPolicy:
		if m == nil {
			return 0, false
		}
		return MutationKindUpdateHostPolicy, true
	default:
		return 0, false
	}
}

// validateResultAuthority enforces the presence rules of one mutating result
// that hold without knowing which request produced it: removal and
// registration never travel together, a successful result never carries an
// error, and a failed or outcome-unknown result carries neither authority,
// only its typed error. The per-kind rule (an AddHost or UpdateHostPolicy
// success must carry a registration, a RemoveHost success must not) is
// enforced by ValidateForMutation at the owning site, because the stateless
// codec has no pending-operation state to consult. A successful removal with
// no authority is legal: an absent host is an idempotent no-op, not an error.
func validateResultAuthority(outcome ports.BrokerMutationOutcome, removed, hasRegistration, hasError bool) error {
	if err := outcome.Validate(); err != nil {
		return ErrInvalidMessage
	}
	if removed && hasRegistration {
		return ErrInvalidMessage
	}
	if outcome == ports.BrokerOutcomeOK {
		if hasError {
			return ErrInvalidMessage
		}
		return nil
	}
	if removed || hasRegistration || !hasError {
		return ErrInvalidMessage
	}
	return nil
}

// ValidateForMutation enforces the full authority rule of one mutating result
// against the kind of request it completes. AddHost and UpdateHostPolicy
// succeed with the authoritative registration, RemoveHost succeeds with
// removal and no registration, and every failed or outcome-unknown result
// carries neither authority, only its validated typed error.
func (r OperationResult) ValidateForMutation(kind RegisterMutationKind) error {
	if err := kind.Validate(); err != nil {
		return err
	}
	hasRegistration := r.Registration != (domain.RemoteRegistration{})
	if err := validateResultAuthority(r.Outcome, r.Removed, hasRegistration, r.HasError); err != nil {
		return err
	}
	if r.HasError {
		if err := r.Error.validate(); err != nil {
			return err
		}
	}
	switch r.Outcome {
	case ports.BrokerOutcomeOK:
		switch kind {
		case MutationKindRemoveHost:
			if hasRegistration {
				return ErrInvalidMessage
			}
		default:
			if !hasRegistration {
				return ErrInvalidMessage
			}
		}
	default:
		if r.Removed || hasRegistration || !r.HasError {
			return ErrInvalidMessage
		}
	}
	return nil
}

// StreamOpened confirms one logical stream is established.
type StreamOpened struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Stream     ports.BrokerStreamID
}

func (StreamOpened) brokerServerMessage() {}

// ServerStreamData carries one opaque daemon-to-client stream frame.
type ServerStreamData struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Stream     ports.BrokerStreamID
	Data       []byte
}

func (ServerStreamData) brokerServerMessage() {}

// StreamClosed ends one logical stream. An absent error is an orderly
// close; a present error reports the typed failure that ended it.
// HasError=false means no detail at all: encode refuses a nonzero Error rather
// than dropping it.
type StreamClosed struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Stream     ports.BrokerStreamID
	Error      ErrorDetail
	HasError   bool
}

func (StreamClosed) brokerServerMessage() {}

// Progress reports one phase of a long-running broker operation.
type Progress struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Stream     ports.BrokerStreamID
	Phase      BrokerProgressPhase
	Text       string
}

func (Progress) brokerServerMessage() {}

// BrokerErrorMessage reports a broker-wide typed failure for one scope.
type BrokerErrorMessage struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Error      ErrorDetail
}

func (BrokerErrorMessage) brokerServerMessage() {}

// PreviewPublication is the newest result for one preview subscription.
// Every authority field is repeated so a client can reject a late
// completion. Exactly one of preview/error travels via the wire result
// oneof: a failed publication carries HasError with a typed ErrorDetail
// and no Preview; a successful one carries Preview and no error.
type PreviewPublication struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Generation ports.BrokerPreviewGeneration
	Preview    protocol.RemotePreview
	Error      ErrorDetail
	HasError   bool
}

func (PreviewPublication) brokerServerMessage() {}

// MaxBrokerAdmissionCode is the highest admission-refusal code carried on
// ErrorDetail. The taxonomy is closed: 0 is none, 1 limit, 2 closed, 3 stale
// connection or stream, 4 invalid request (mirroring
// ports.BrokerAdmissionError).
const MaxBrokerAdmissionCode uint32 = 4

// MaxBrokerFailureKind bounds ErrorDetail.FailureKind to the domain
// transport-failure taxonomy. The zero value (RemoteFailureNone) is valid.
const MaxBrokerFailureKind = domain.RemoteFailureInvalidResponse

// ErrorDetail is the typed broker failure carried to clients: a closed
// error code plus bounded display text, the admission refusal (if any),
// and the sanitized transport cause. The underlying diagnostic cause is
// local-only and never travels on this wire.
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
	if d.AdmissionCode > MaxBrokerAdmissionCode {
		return ErrInvalidMessage
	}
	if d.FailureKind > MaxBrokerFailureKind {
		return ErrInvalidMessage
	}
	return validateBrokerDisplayText(d.Text, ports.BrokerMaxErrorBytes)
}

type ShutdownReason uint8

const (
	// ShutdownUnspecified is the zero value: never sent on the wire.
	ShutdownUnspecified ShutdownReason = iota
	// ShutdownIdleExit reports the broker stopped for lack of work.
	ShutdownIdleExit
	// ShutdownTerminating reports the broker process is stopping.
	ShutdownTerminating
)

func (r ShutdownReason) validate() error {
	switch r {
	case ShutdownIdleExit, ShutdownTerminating:
		return nil
	default:
		return errConvertRange
	}
}

// Shutdown reports the broker process is stopping.
type Shutdown struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
	Reason     ShutdownReason
	Text       string
}

func (Shutdown) brokerServerMessage() {}

// BrokerProgressPhase is the closed taxonomy for long-running operation
// progress reports carried on Progress.
type BrokerProgressPhase uint32

const (
	// BrokerProgressStarted opens a long-running operation.
	BrokerProgressStarted BrokerProgressPhase = 1
	// BrokerProgressProbing reports host observation is in flight.
	BrokerProgressProbing BrokerProgressPhase = 2
	// BrokerProgressConnecting reports physical transport dialing.
	BrokerProgressConnecting BrokerProgressPhase = 3
	// BrokerProgressDone closes a long-running operation.
	BrokerProgressDone BrokerProgressPhase = 4
)

func (p BrokerProgressPhase) validate() error {
	switch p {
	case BrokerProgressStarted, BrokerProgressProbing, BrokerProgressConnecting, BrokerProgressDone:
		return nil
	default:
		return errConvertRange
	}
}

// SubscriptionGeneration identifies one client subscription series within a
// scope. The wire carries it as a bare uint64 on Subscribe/Resync/
// Unsubscribe and echoes it on every SnapshotPart; zero is valid.
type SubscriptionGeneration = uint64

// brokerTimeToWire splits one instant into seconds and nanoseconds so
// time.Time survives the wire exactly. The zero time maps to the zero
// timestamp and back.
func brokerTimeToWire(t time.Time) (int64, int32) {
	if t.IsZero() {
		return 0, 0
	}
	return t.Unix(), int32(t.Nanosecond())
}

// brokerTimeFromWire reassembles one timestamp. Out-of-range values fail:
// times must round-trip through time.Unix without overflow.
func brokerTimeFromWire(seconds int64, nanos int32) (time.Time, error) {
	if seconds == 0 && nanos == 0 {
		return time.Time{}, nil
	}
	if nanos < 0 || nanos > 999999999 {
		return time.Time{}, errConvertRange
	}
	// time.Unix panics only outside int64-derived ranges; seconds is
	// already int64, and the nanosecond field is bounded above, so this
	// cannot overflow on supported platforms beyond what time.Unix
	// itself rejects. Guard the documented extremes anyway.
	t := time.Unix(seconds, int64(nanos)).UTC()
	if t.Unix() != seconds {
		return time.Time{}, errConvertRange
	}
	return t, nil
}
