package ports

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Broker semantic contracts (Plan 001 P1.3).
//
// The broker is the machine-wide owner of connectivity: configured hosts,
// daemon reachability and catalogue snapshots, pooled physical transports,
// and multiplexed logical streams. It never owns picker UI, navigation
// decisions, sessions, PTYs, terminal geometry, or attachment semantics.
//
// Identity levels stay distinct:
//   - configured host registration/incarnation (domain.RemoteRegistration)
//   - authenticated daemon identity used for pooling (BrokerDaemonIdentity)
//   - daemon process incarnation used to detect restart
//     (BrokerDaemonIncarnation)
//   - exact session lifecycle identity (domain.SessionLifecycleID via
//     protocol.ExactSessionTarget)
//   - broker epoch/revision (BrokerEpoch/BrokerRevision)
//   - client connection identity (BrokerConnectionID)
//   - logical stream identity (BrokerStreamID)
//
// An epoch change forces a full resynchronization: stream IDs and events
// are scoped to one client connection and broker epoch, so stale
// completions cannot revive an attachment. Tombstones and registration
// incarnation fence remove/re-add races independently of the epoch.
//
// Raw frames, codecs, environment policy enforcement, and worker
// implementations live outside this package. Per-request session
// environment (BrokerOpenStreamRequest.Env) stays separate from the broker
// process environment.

const (
	// BrokerMaxHosts bounds one snapshot publication. It mirrors the
	// catalogue host budget so a broker snapshot never exceeds what
	// discovery can produce.
	BrokerMaxHosts = catalogue.RemoteCatalogMaxHosts
	// BrokerMaxSessionsPerHost bounds sessions carried per host in one
	// snapshot publication.
	BrokerMaxSessionsPerHost = catalogue.RemoteCatalogMaxSessions
	// BrokerMaxTombstones bounds retired registrations carried alongside
	// one snapshot for remove/re-add fencing.
	BrokerMaxTombstones = catalogue.RemoteCatalogMaxHosts
	// BrokerMaxErrorBytes bounds human-readable error text in broker
	// results. The underlying cause stays behind errors.Is/As where the
	// contract carries an error value.
	BrokerMaxErrorBytes = 512
	// BrokerMaxEnvEntries bounds per-request environment entries. The
	// broker never merges these into its own process environment.
	BrokerMaxEnvEntries = 128
	// BrokerMaxEnvEntryBytes bounds one per-request environment entry.
	BrokerMaxEnvEntryBytes = 4 << 10
	// BrokerMaxTransportBytes bounds the opaque transport policy token.
	BrokerMaxTransportBytes = 64
	// BrokerMaxIdentityBytes bounds the authenticated daemon identity token.
	BrokerMaxIdentityBytes = 256
	// BrokerMaxPolicyTokenBytes bounds each authoritative policy identity.
	BrokerMaxPolicyTokenBytes = 256
)

// Broker registry lifecycle sentinels. They are typed so a caller can classify
// why the registry refused a membership mutation without importing the use
// case or matching on message text.
var (
	// ErrBrokerRegistryClosed reports that the registry run has settled: its
	// durable writer is flushed, in-flight attempts are retired, and no further
	// membership mutation is acknowledged.
	ErrBrokerRegistryClosed = errors.New("ports: broker registry is closed")
	// ErrBrokerRevisionExhausted reports that the broker epoch has consumed its
	// entire revision series. The registry fails closed: it keeps the last valid
	// snapshot published and refuses further mutations instead of wrapping to
	// zero or publishing a revision below the newest one, either of which would
	// silently freeze durable persistence.
	ErrBrokerRevisionExhausted = errors.New("ports: broker revision series exhausted")
)

// BrokerEpoch identifies one broker process incarnation. Every broker
// process samples a fresh non-zero epoch at startup; every publication
// carries a revision within that epoch. Clients discard all broker state
// and resubscribe when the epoch changes.
type BrokerEpoch uint64

// BrokerRevision is monotonic within one broker epoch. Revisions never
// compare across epochs.
type BrokerRevision uint64

// BrokerConnectionID identifies one client connection to the broker,
// scoped to a broker epoch. It never identifies a session or a stream.
type BrokerConnectionID [16]byte

// BrokerStreamID identifies one logical stream within a client connection
// and broker epoch. It never crosses connections or epochs.
type BrokerStreamID uint64

// BrokerOperationID is the idempotency identity for one mutating broker
// operation. Clients refresh authoritative state on outcome-unknown and
// never blindly replay a non-idempotent operation.
type BrokerOperationID [16]byte

// BrokerDaemonIncarnation identifies one daemon process incarnation and
// detects restarts behind a stable authenticated identity.
type BrokerDaemonIncarnation [16]byte

func (id BrokerConnectionID) IsZero() bool { return id == (BrokerConnectionID{}) }

func (id BrokerConnectionID) Validate() error {
	if id.IsZero() {
		return errors.New("ports: broker connection ID is zero")
	}
	return nil
}

func (id BrokerStreamID) Validate() error {
	if id == 0 {
		return errors.New("ports: broker stream ID is zero")
	}
	return nil
}

func (id BrokerOperationID) IsZero() bool { return id == (BrokerOperationID{}) }

func (id BrokerOperationID) Validate() error {
	if id.IsZero() {
		return errors.New("ports: broker operation ID is zero")
	}
	return nil
}

func (id BrokerDaemonIncarnation) IsZero() bool { return id == (BrokerDaemonIncarnation{}) }

func (id BrokerDaemonIncarnation) Validate() error {
	if id.IsZero() {
		return errors.New("ports: broker daemon incarnation is zero")
	}
	return nil
}

// BrokerDaemonIdentity is the authenticated daemon/service identity used
// for physical transport pooling. Display aliases are never pooled: two
// endpoints share a transport only after authenticated identity and every
// trust, launch, isolation, and transport policy compare compatible.
type BrokerDaemonIdentity string

func (id BrokerDaemonIdentity) Validate() error {
	return validateBrokerToken(string(id), BrokerMaxIdentityBytes, "daemon identity")
}

// BrokerPolicy is the exact connection policy required before two
// endpoints may share a pooled physical transport. Pooling requires exact
// equality: conflicting policy is rejected, never merged or inherited
// from the first launching client.
type BrokerPolicy struct {
	ProtocolVersion      uint16
	CatalogSchemaVersion uint16
	EnvironmentPolicy    protocol.EnvironmentPolicy
	Transport            string
	// Opaque, authoritative policy identities, not display labels or secrets.
	Trust     string
	Launch    string
	Isolation string
}

func (p BrokerPolicy) Validate() error {
	for _, token := range []struct{ value, label string }{{p.Trust, "policy trust"}, {p.Launch, "policy launch"}, {p.Isolation, "policy isolation"}} {
		if err := validateBrokerToken(token.value, BrokerMaxPolicyTokenBytes, token.label); err != nil {
			return err
		}
	}
	if p.ProtocolVersion == 0 {
		return errors.New("ports: broker policy has no protocol version")
	}
	if p.CatalogSchemaVersion == 0 {
		return errors.New("ports: broker policy has no catalog schema version")
	}
	if err := protocol.ValidateEnvironmentPolicy(p.EnvironmentPolicy); err != nil {
		return fmt.Errorf("ports: broker policy: %w", err)
	}
	if err := validateBrokerToken(p.Transport, BrokerMaxTransportBytes, "policy transport"); err != nil {
		return err
	}
	return nil
}

// Compatible reports whether two validated policies may share a pooled
// physical transport. It requires exact equality across every field.
func (p BrokerPolicy) Compatible(other BrokerPolicy) bool {
	return p == other
}

// BrokerHostTombstone fences a removed host registration so a stale probe,
// snapshot, or completion bound to the retired incarnation cannot overwrite
// or revive current authority. A re-added endpoint carries a new
// incarnation and is never fenced by an older tombstone.
type BrokerHostTombstone struct {
	Endpoint        string
	Registration    domain.RemoteRegistration
	RetiredRevision BrokerRevision
}

func (t BrokerHostTombstone) Validate() error {
	if err := domain.ValidateRemoteHostTarget(t.Endpoint); err != nil {
		return fmt.Errorf("ports: broker tombstone: %w", err)
	}
	if err := t.Registration.Validate(); err != nil {
		return fmt.Errorf("ports: broker tombstone: %w", err)
	}
	if t.Endpoint != t.Registration.Endpoint {
		return errors.New("ports: broker tombstone endpoint does not match registration")
	}
	if t.RetiredRevision == 0 {
		return errors.New("ports: broker tombstone has no retired revision")
	}
	return nil
}

// Fences reports whether this tombstone retires the supplied registration:
// same endpoint and incarnation, with a retired generation at or past the
// candidate. A re-added endpoint with a fresh incarnation is never fenced.
func (t BrokerHostTombstone) Fences(candidate domain.RemoteRegistration) bool {
	return t.Endpoint == candidate.Endpoint &&
		t.Registration.Incarnation == candidate.Incarnation &&
		t.Registration.Generation >= candidate.Generation
}

// BrokerSnapshot is an immutable, fully defensive broker publication.
// Daemons carries the broker-native daemon observations (Plan 001 P5.2a):
// the local daemon entry first when present, then remote hosts in
// registration order. Removed carries the bounded tombstone set that fences
// stale authority. Nested slices are never mutated after publication.
// Tombstones and the local daemon observation are process-local state and
// are never durable: a BrokerSnapshotStore persists and reloads remote
// daemon observations only.
type BrokerSnapshot struct {
	Epoch    BrokerEpoch
	Revision BrokerRevision
	Daemons  []BrokerDaemonObservation
	Removed  []BrokerHostTombstone
}

func (s BrokerSnapshot) Validate() error {
	if s.Epoch == 0 {
		return errors.New("ports: broker snapshot has no epoch")
	}
	if s.Revision == 0 {
		return errors.New("ports: broker snapshot has no revision")
	}
	if len(s.Daemons) > BrokerMaxDaemonsPerSnapshot {
		return errors.New("ports: broker snapshot has too many daemons")
	}
	local := false
	seen := make(map[string]struct{}, len(s.Daemons))
	for i, daemon := range s.Daemons {
		if err := daemon.Validate(); err != nil {
			return fmt.Errorf("ports: broker snapshot daemon %d: %w", i, err)
		}
		if daemon.Local {
			if local {
				return errors.New("ports: broker snapshot has more than one local daemon")
			}
			// The wire layout carries the local daemon at index zero only, so a
			// local observation anywhere else is a publication the peer's
			// assembler refuses. Refuse it at the contract gate instead.
			if i != 0 {
				return fmt.Errorf("ports: broker snapshot carries its local daemon at index %d", i)
			}
			local = true
			continue
		}
		if _, dup := seen[daemon.Endpoint]; dup {
			return fmt.Errorf("ports: broker snapshot has duplicate host %q", daemon.Endpoint)
		}
		seen[daemon.Endpoint] = struct{}{}
	}
	if len(s.Removed) > BrokerMaxTombstones {
		return errors.New("ports: broker snapshot has too many tombstones")
	}
	for _, tombstone := range s.Removed {
		if err := tombstone.Validate(); err != nil {
			return err
		}
		if _, live := seen[tombstone.Endpoint]; live {
			return fmt.Errorf("ports: broker snapshot carries live host %q as tombstone", tombstone.Endpoint)
		}
	}
	return nil
}

// Clone returns a defensive copy with independent daemon, session, and
// tombstone slices.
func (s BrokerSnapshot) Clone() BrokerSnapshot {
	out := s
	out.Daemons = make([]BrokerDaemonObservation, len(s.Daemons))
	for i, daemon := range s.Daemons {
		out.Daemons[i] = daemon.Clone()
	}
	out.Removed = append([]BrokerHostTombstone(nil), s.Removed...)
	return out
}

// Find returns the observation for an exact remote endpoint identity. The
// local daemon has no endpoint and is located by its Local flag.
func (s BrokerSnapshot) Find(endpoint string) (BrokerDaemonObservation, bool) {
	for _, daemon := range s.Daemons {
		if !daemon.Local && daemon.Endpoint == endpoint {
			return daemon, true
		}
	}
	return BrokerDaemonObservation{}, false
}

// Supersedes reports whether s is newer than other. Any epoch change
// forces a full resynchronization: a snapshot with a valid epoch
// supersedes every snapshot from another epoch (or none), regardless of
// numeric order, because epochs are fresh per broker process and never
// ordered across processes. Within one epoch the higher revision wins.
// Revisions never compare across epochs. A zero-epoch snapshot is
// invalid and never supersedes.
func (s BrokerSnapshot) Supersedes(other BrokerSnapshot) bool {
	if s.Epoch == 0 {
		return false
	}
	if s.Epoch != other.Epoch {
		return true
	}
	return s.Revision > other.Revision
}

// BrokerCompletionIsStale reports whether a probe or operation completion
// must be rejected: a different broker epoch, or a registration that no
// longer exactly matches current authority (endpoint, incarnation, and
// generation must all match).
func BrokerCompletionIsStale(currentRegistration domain.RemoteRegistration, currentEpoch BrokerEpoch, completionRegistration domain.RemoteRegistration, completionEpoch BrokerEpoch) bool {
	if currentEpoch == 0 || completionEpoch == 0 {
		return true
	}
	if currentEpoch != completionEpoch {
		return true
	}
	return !currentRegistration.Equal(completionRegistration)
}

// BrokerStreamAdmission is the closed attachment-admission taxonomy
// (Plan 001 P5.3a, contract-only until the admission slice wires it):
// an attachment stream names exactly how the daemon must admit it.
// Control and observation streams carry no admission.
type BrokerStreamAdmission uint8

const (
	// BrokerAdmissionExact attaches to one exact session lifecycle identity;
	// the daemon revalidates it before attachment.
	BrokerAdmissionExact BrokerStreamAdmission = iota + 1
	// BrokerAdmissionCreateNamed creates one named session when the stream
	// attaches; the name is validated session-name authority.
	BrokerAdmissionCreateNamed
	// BrokerAdmissionCreateEphemeral creates one ephemeral session when the
	// stream attaches.
	BrokerAdmissionCreateEphemeral
)

func (a BrokerStreamAdmission) String() string {
	switch a {
	case BrokerAdmissionExact:
		return "exact"
	case BrokerAdmissionCreateNamed:
		return "create_named"
	case BrokerAdmissionCreateEphemeral:
		return "create_ephemeral"
	default:
		return fmt.Sprintf("invalid(%d)", uint8(a))
	}
}

func (a BrokerStreamAdmission) Validate() error {
	switch a {
	case BrokerAdmissionExact, BrokerAdmissionCreateNamed, BrokerAdmissionCreateEphemeral:
		return nil
	default:
		return errors.New("ports: invalid broker stream admission")
	}
}

// BrokerOpenStreamRequest asks the broker to open one independently
// cancellable logical stream to the owning daemon. The daemon revalidates
// the exact session identity before attachment. Admission selects the
// attachment admission variant; Name is the validated session name for
// BrokerAdmissionCreateNamed and is empty for every other variant. Env is
// the per-request session environment; it is never inherited from the
// broker process environment.
type BrokerOpenStreamRequest struct {
	Epoch        BrokerEpoch
	Purpose      BrokerStreamPurpose
	Admission    BrokerStreamAdmission
	Name         string
	Local        bool
	Connection   BrokerConnectionID
	Stream       BrokerStreamID
	Endpoint     string
	Registration domain.RemoteRegistration
	Target       protocol.ExactSessionTarget
	Env          []string
	Policy       BrokerPolicy
}

func (r BrokerOpenStreamRequest) Validate() error {
	if r.Epoch == 0 {
		return errors.New("ports: missing broker epoch")
	}
	if err := r.Connection.Validate(); err != nil {
		return err
	}
	if err := r.Stream.Validate(); err != nil {
		return err
	}
	if r.Local {
		if r.Endpoint != "" || r.Registration != (domain.RemoteRegistration{}) {
			return errors.New("ports: local endpoint carries remote registration")
		}
	} else {
		if err := r.Registration.Validate(); err != nil {
			return err
		}
		if r.Endpoint != r.Registration.Endpoint {
			return errors.New("ports: endpoint registration mismatch")
		}
	}
	switch r.Purpose {
	case BrokerStreamAttachment:
		if err := r.Admission.Validate(); err != nil {
			return err
		}
		switch r.Admission {
		case BrokerAdmissionExact:
			if err := r.Target.Validate(); err != nil {
				return err
			}
			if r.Name != "" {
				return errors.New("ports: exact admission carries a creation name")
			}
		case BrokerAdmissionCreateNamed:
			if r.Target != (protocol.ExactSessionTarget{}) {
				return errors.New("ports: named creation carries an exact target")
			}
			if err := domain.ValidateSessionName(r.Name); err != nil {
				return fmt.Errorf("ports: invalid creation session name: %w", err)
			}
		case BrokerAdmissionCreateEphemeral:
			if r.Target != (protocol.ExactSessionTarget{}) {
				return errors.New("ports: ephemeral creation carries an exact target")
			}
			if r.Name != "" {
				return errors.New("ports: ephemeral creation carries a session name")
			}
		}
	case BrokerStreamControl, BrokerStreamObservation:
		if r.Admission != 0 || r.Name != "" || r.Target != (protocol.ExactSessionTarget{}) || len(r.Env) != 0 {
			return errors.New("ports: control/observation carries attachment state")
		}
	default:
		return errors.New("ports: invalid stream purpose")
	}
	if uint64(len(r.Env)) > BrokerMaxEnvEntries {
		return errors.New("ports: broker open stream has too many environment entries")
	}
	for _, entry := range r.Env {
		if len(entry) > BrokerMaxEnvEntryBytes {
			return errors.New("ports: broker open stream environment entry too long")
		}
		if !utf8.ValidString(entry) {
			return errors.New("ports: broker open stream environment is not valid UTF-8")
		}
	}
	if err := r.Policy.Validate(); err != nil {
		return err
	}
	return nil
}

// BrokerMutationOutcome is the closed outcome taxonomy for a mutating
// broker operation. OutcomeUnknown means connectivity was lost after the
// mutation may have committed but before its result arrived: the client
// refreshes authoritative state and never blindly replays a
// non-idempotent operation.
type BrokerMutationOutcome uint8

const (
	BrokerOutcomeOK BrokerMutationOutcome = iota + 1
	BrokerOutcomeFailed
	BrokerOutcomeUnknown
)

func (o BrokerMutationOutcome) String() string {
	switch o {
	case BrokerOutcomeOK:
		return "ok"
	case BrokerOutcomeFailed:
		return "failed"
	case BrokerOutcomeUnknown:
		return "outcome_unknown"
	default:
		return "unknown"
	}
}

func (o BrokerMutationOutcome) Validate() error {
	switch o {
	case BrokerOutcomeOK, BrokerOutcomeFailed, BrokerOutcomeUnknown:
		return nil
	default:
		return errors.New("ports: invalid broker mutation outcome")
	}
}

// BrokerOperationResult is exactly one completion for one admitted mutating
// operation. Text is bounded and presentation-safe; the outcome decides
// whether the client may retry, must refresh, or must not replay.
type BrokerOperationResult struct {
	Operation BrokerOperationID
	Outcome   BrokerMutationOutcome
	Text      string
}

func (r BrokerOperationResult) Validate() error {
	if err := r.Operation.Validate(); err != nil {
		return err
	}
	if err := r.Outcome.Validate(); err != nil {
		return err
	}
	if err := validateBrokerDisplayText(r.Text, BrokerMaxErrorBytes, "operation result text"); err != nil {
		return err
	}
	if r.Outcome == BrokerOutcomeOK && r.Text != "" {
		return errors.New("ports: successful broker operation carries no text")
	}
	return nil
}

// BrokerErrorCode is the closed taxonomy for broker and stream lifecycle
// failures surfaced to clients. Every failure returns the client to the
// picker in the same process with a visible typed error; none terminates
// the client except an explicit exit or a fatal terminal failure.
type BrokerErrorCode uint8

const (
	BrokerErrorUnavailable BrokerErrorCode = iota + 1
	BrokerErrorIncompatible
	BrokerErrorTimeout
	BrokerErrorCancelled
	BrokerErrorOutcomeUnknown
	BrokerErrorAttachmentLost
	BrokerErrorStaleEpoch
	BrokerErrorConflictingPolicy
	BrokerErrorExplicitExit
	BrokerErrorFatalTerminal
)

func (c BrokerErrorCode) String() string {
	switch c {
	case BrokerErrorUnavailable:
		return "unavailable"
	case BrokerErrorIncompatible:
		return "incompatible"
	case BrokerErrorTimeout:
		return "timeout"
	case BrokerErrorCancelled:
		return "cancelled"
	case BrokerErrorOutcomeUnknown:
		return "outcome_unknown"
	case BrokerErrorAttachmentLost:
		return "attachment_lost"
	case BrokerErrorStaleEpoch:
		return "stale_epoch"
	case BrokerErrorConflictingPolicy:
		return "conflicting_policy"
	case BrokerErrorExplicitExit:
		return "explicit_exit"
	case BrokerErrorFatalTerminal:
		return "fatal_terminal"
	default:
		return "unknown"
	}
}

func (c BrokerErrorCode) Validate() error {
	switch c {
	case BrokerErrorUnavailable, BrokerErrorIncompatible, BrokerErrorTimeout,
		BrokerErrorCancelled, BrokerErrorOutcomeUnknown, BrokerErrorAttachmentLost,
		BrokerErrorStaleEpoch, BrokerErrorConflictingPolicy,
		BrokerErrorExplicitExit, BrokerErrorFatalTerminal:
		return nil
	default:
		return errors.New("ports: invalid broker error code")
	}
}

// BrokerError is the typed lifecycle failure carried to clients. Terminal
// reports whether the client process itself must exit: only an explicit
// exit or a fatal terminal failure is terminal; every other code returns
// to the picker with a visible notification.
type BrokerError struct {
	Code BrokerErrorCode
	Text string
	// Cause is local-only diagnostic authority, never serialized as display text.
	Cause error
}

func (e BrokerError) Error() string {
	if e.Text == "" {
		return "vev: broker " + e.Code.String()
	}
	return "vev: broker " + e.Code.String() + ": " + e.Text
}

func (e BrokerError) Unwrap() error { return e.Cause }

func (e BrokerError) Validate() error {
	if err := e.Code.Validate(); err != nil {
		return err
	}
	return validateBrokerDisplayText(e.Text, BrokerMaxErrorBytes, "error text")
}

// Terminal reports whether this failure ends the client process.
func (e BrokerError) Terminal() bool {
	return e.Code == BrokerErrorExplicitExit || e.Code == BrokerErrorFatalTerminal
}

// BrokerStreamLost is the independent typed loss event delivered per
// affected logical stream when its physical connection fails. One stream
// failure never closes unrelated streams; a physical loss fans out one
// event per stream.
type BrokerStreamLost struct {
	Connection BrokerConnectionID
	Stream     BrokerStreamID
	Epoch      BrokerEpoch
	Cause      domain.RemoteFailureKind
	Err        error
}

// Error and Unwrap retain exact stream scope while allowing generic lifecycle handling.
func (e BrokerStreamLost) Error() string { return "vev: broker attachment_lost" }
func (e BrokerStreamLost) Unwrap() error {
	return BrokerError{Code: BrokerErrorAttachmentLost, Cause: e.Err}
}

func (e BrokerStreamLost) Validate() error {
	if e.Cause < domain.RemoteFailureTransport || e.Cause > domain.RemoteFailureInvalidResponse {
		return errors.New("ports: broker stream loss has invalid cause")
	}
	if err := e.Connection.Validate(); err != nil {
		return err
	}
	if err := e.Stream.Validate(); err != nil {
		return err
	}
	if e.Epoch == 0 {
		return errors.New("ports: broker stream loss has no epoch")
	}
	return nil
}

// BrokerSubscription wakes one subscriber when a newer snapshot is
// available. Each subscriber owns a capacity-one channel and re-reads the
// newest snapshot on wake; channels are never shared between subscribers.
type BrokerSubscription interface {
	Changed() <-chan struct{}
	Close()
}

// BrokerService is the client-facing broker façade. All clients reach
// local and remote daemons through it, including the local daemon.
// Snapshot access performs no I/O; request methods are non-blocking
// coalesced hints except where the context explicitly bounds them.
type BrokerService interface {
	// ConnectionID returns the ID assigned when this client connection was
	// accepted. Clients carry it on stream operations to fence stale requests.
	ConnectionID() BrokerConnectionID
	// Done closes exactly once when this connection is terminal: the broker
	// retired it, the carriage failed, or the client closed it locally. Err is
	// stable afterwards. A client observes Done/Err to attribute a broker loss
	// to the connection even while it holds no logical stream.
	Done() <-chan struct{}
	// Err returns the terminal cause, stable once Done is closed. It is nil for
	// an orderly local Close and non-nil for a broker-side loss or failure.
	Err() error
	Snapshot() BrokerSnapshot
	Subscribe() (BrokerSubscription, error)
	OpenStream(ctx context.Context, request BrokerOpenStreamRequest) (BrokerLogicalConnection, error)
	CloseStream(connection BrokerConnectionID, stream BrokerStreamID) error
	AddHost(ctx context.Context, target string) error
	RemoveHost(ctx context.Context, target string) (bool, error)
	RequestReconcile(endpoint string)
	Close() error
}

// BrokerSnapshotStore persists the durable broker snapshot independently
// from session state. The broker is its sole writer; session persistence
// stays daemon-owned and is never moved here. Load is called once while the
// broker is constructed; Store runs on the broker's serialized writer
// goroutine. Store must return promptly and must never block indefinitely:
// this seam deliberately carries no cancellation context in P2.1, and broker
// shutdown waits for the in-flight write to finish, so a Store that blocks
// without bound blocks shutdown. Implementations must bound their own write
// time and return an error rather than wait forever.
type BrokerSnapshotStore interface {
	Load() (BrokerSnapshot, error)
	Store(BrokerSnapshot) error
}

// BrokerHostProbe observes one exact host registration without creating an
// attachment. The probe reports only observed state in a
// BrokerDaemonObservation: the observed daemon identity, incarnation,
// protocol version, capabilities, availability, failure, freshness, and
// session inventory. Configured authority (endpoint, registration, policy,
// display origin, rank) is stamped by the registry from its own records and
// never trusted from the probe; unknown identity, version, or inventory is
// zero and is never invented. The returned observation must satisfy the
// durable projection rules (see ValidateDurableHostProjection): the registry
// re-checks exactly those rules, so an over-bound or catalogue-invalid session
// inventory is classified as an invalid response and never published.
// Implementations should therefore validate the observation with
// catalogue.ValidateRemoteCatalog against the RemoteCatalogMax* bounds
// before returning it rather than relying on that rejection. Probe must
// return promptly once ctx is cancelled: the registry cancels the attempt
// context when a registration is replaced or removed, and when the run
// settles. A probe that ignores cancellation delays that retirement and can
// overlap a replacement attempt for the same endpoint. Stale completions
// that arrive anyway are fenced by epoch and registration identity before
// publication.
type BrokerHostProbe interface {
	Probe(ctx context.Context, registration domain.RemoteRegistration) (BrokerDaemonObservation, error)
}

// BrokerPhysicalConnection is one pooled physical transport to an
// authenticated daemon identity. It opens one independently cancellable
// typed session connection per logical stream and never shares stream
// state, identity, geometry, input, output, cancellation, or application
// errors between streams.
type BrokerPhysicalConnection interface {
	// Done is the physical terminal authority, independent of reads. On
	// physical failure, publish Err and FailureKind and close Done BEFORE
	// terminating affected logical streams. Both results are stable after Done.
	// FailureKind is nonzero for failure; None is allowed only for local Close.
	Done() <-chan struct{}
	Err() error
	FailureKind() domain.RemoteFailureKind
	Identity() BrokerDaemonIdentity
	Incarnation() BrokerDaemonIncarnation
	Policy() BrokerPolicy
	OpenStream(ctx context.Context, request BrokerOpenStreamRequest) (BrokerLogicalConnection, error)
	Close() error
}

// BrokerEndpointConnector establishes the pooled physical transport for
// one endpoint under an exact compatible policy. Conflicting policy is
// rejected; the first launching client never becomes policy authority.
type BrokerEndpointConnector interface {
	Connect(ctx context.Context, endpoint BrokerResolvedEndpoint) (BrokerPhysicalConnection, error)
}

// BrokerConnector establishes the client-facing broker connection: it connects
// to the per-user broker endpoint and returns the connection-scoped
// BrokerService for exactly that accepted connection. It is the client
// process's admission seam, distinct from BrokerEndpointConnector (which opens
// a pooled physical transport for one endpoint inside the broker) and from
// BrokerAuthority (which admits a listener-side accepted connection).
//
// Connect must honor cancellation: a cancelled connect returns promptly and
// leaves no service behind, so a client that abandons an attempt never leaks a
// connection. Each successful Connect is independent and the caller owns Close
// on the returned service; a connector retains no connection state of its own.
// Semantic rejection is reported as a typed BrokerError and cancellation as a
// context error.
//
// A successful service is independent of the setup context: Connect must be
// given a context that bounds only the setup, and once Connect returns a
// service the caller may cancel or let that context expire without closing,
// interrupting, or otherwise disturbing the established connection. The
// returned service's lifetime is bounded solely by its own Close and by the
// broker, so a caller that abandons the setup context (a bounded attempt) still
// owns a live connection it must Close.
type BrokerConnector interface {
	Connect(ctx context.Context) (BrokerService, error)
}

// BrokerAuthority admits one accepted client connection to the broker core
// and returns the BrokerService scoped to exactly that connection. It is the
// admission seam between the broker listener adapter and the broker use case:
// the listener owns the carriage and calls AdmitClient once per accepted
// connection, the implementation assigns the connection identity, and the
// returned service owns that connection until Close. The adapter never
// constructs a broker itself.
//
// ctx bounds admission only. It carries the listener's handshake deadline and
// is canceled as soon as admission returns or when the listener closes, so an
// implementation must not retain it or start work that outlives admission
// from it; honoring that cancellation is what lets a listener drop a client
// parked in admission immediately. The admitted service gets its own
// connection-lived context and must not derive from this one.
type BrokerAuthority interface {
	AdmitClient(ctx context.Context) (BrokerService, error)
}

// BrokerListener accepts client connections to the per-user broker
// endpoint. Each Accept returns a BrokerService bound to exactly one
// accepted client connection: the implementation assigns that
// connection's BrokerConnectionID at accept, ends the connection on
// Close, and rejects requests carrying a mismatched Connection as stale.
// Implementations own the owner-only directory/socket, peer credential
// checks, bounded clients and queues, and cancellation-safe accept loops.
type BrokerListener interface {
	Accept() (BrokerService, error)
	Close() error
	Addr() string
}

func validateBrokerToken(value string, maxBytes int, label string) error {
	if value == "" {
		return fmt.Errorf("ports: broker %s is empty", label)
	}
	if len(value) > maxBytes {
		return fmt.Errorf("ports: broker %s too long", label)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("ports: broker %s is not valid UTF-8", label)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("ports: broker %s has surrounding whitespace", label)
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) || unicode.Is(unicode.Bidi_Control, r) || r == '\u2028' || r == '\u2029' {
			return fmt.Errorf("ports: broker %s contains disallowed characters", label)
		}
	}
	return nil
}

func validateBrokerDisplayText(value string, maxBytes int, label string) error {
	if len(value) > maxBytes {
		return fmt.Errorf("ports: broker %s too long", label)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("ports: broker %s is not valid UTF-8", label)
	}
	for _, r := range value {
		if !brokerDisplayRuneAllowed(r) {
			return fmt.Errorf("ports: broker %s contains disallowed characters", label)
		}
	}
	return nil
}

// brokerDisplayRuneAllowed reports whether one rune may appear in broker
// display text. Controls, bidi controls, and the Unicode line separators are
// refused: none of them carries presentation value, and each lets a value
// reorder or split the text rendered around it.
func brokerDisplayRuneAllowed(r rune) bool {
	return !unicode.IsControl(r) && !unicode.Is(unicode.Bidi_Control, r) && r != '\u2028' && r != '\u2029'
}

// SanitizeBrokerDisplayText returns value with every rune broker display text
// refuses removed, and always as valid UTF-8. Producers apply it to the
// presentation hints they derive rather than receive: a hint derived from
// routing authority is not itself authority, so a configured target the
// routing validator accepts still yields text the display rule accepts instead
// of a publication the wire refuses. The result is not length-checked; the
// caller applies the bound for its own field.
func SanitizeBrokerDisplayText(value string) string {
	if utf8.ValidString(value) && strings.IndexFunc(value, func(r rune) bool { return !brokerDisplayRuneAllowed(r) }) < 0 {
		return value
	}
	var safe strings.Builder
	safe.Grow(len(value))
	for _, r := range value {
		if brokerDisplayRuneAllowed(r) {
			safe.WriteRune(r)
		}
	}
	return safe.String()
}
