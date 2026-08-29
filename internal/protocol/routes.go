package protocol

import (
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
)

// ErrInvalidRouteWire preserves the public error classification used by
// strict route codecs while route values live in the semantic protocol.
var ErrInvalidRouteWire = errors.New("invalid route wire message")

// RouteOrigin identifies how a client reached a daemon. The origin is client
// composition metadata, not an authority granted to the daemon.
type RouteOrigin uint8

const (
	RouteOriginLocal RouteOrigin = iota + 1
	RouteOriginRemote
	RouteOriginDiscovery
)

func (o RouteOrigin) valid() bool {
	switch o {
	case RouteOriginLocal, RouteOriginRemote, RouteOriginDiscovery:
		return true
	default:
		return false
	}
}

func (o RouteOrigin) Validate() error {
	if !o.valid() {
		return errors.New("invalid route origin")
	}
	return nil
}

// RouteKind identifies the kind of target represented by one display entry.
type RouteKind uint8

const (
	RouteKindLocal RouteKind = iota + 1
	RouteKindRemote
)

func (k RouteKind) valid() bool {
	switch k {
	case RouteKindLocal, RouteKindRemote:
		return true
	default:
		return false
	}
}

func (k RouteKind) Validate() error {
	if !k.valid() {
		return errors.New("invalid route kind")
	}
	return nil
}

// RouteReachability is a closed display hint. It is never used as proof that
// a route can be attached; the exact target and daemon still decide that.
type RouteReachability uint8

const (
	RouteReachabilityUnknown RouteReachability = iota
	RouteReachabilityReachable
	RouteReachabilityUnavailable
)

func (r RouteReachability) valid() bool {
	switch r {
	case RouteReachabilityUnknown, RouteReachabilityReachable, RouteReachabilityUnavailable:
		return true
	default:
		return false
	}
}

func (r RouteReachability) Validate() error {
	if !r.valid() {
		return errors.New("invalid route reachability")
	}
	return nil
}

// ValidateRouteLabel rejects malformed or terminal-control text before it can
// enter a daemon-rendered route snapshot. Empty labels are allowed only when
// the caller explicitly marks them optional.
func ValidateRouteLabel(value string, allowEmpty bool) error {
	if !allowEmpty && value == "" {
		return errors.New("route label is empty")
	}
	if len(value) > RouteLabelMaxBytes {
		return errors.New("route label is too long")
	}
	if !utf8.ValidString(value) {
		return errors.New("route label is not valid UTF-8")
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return errors.New("route label contains a control character")
		}
		if unicode.Is(unicode.Bidi_Control, r) || r == '\u2028' || r == '\u2029' {
			return errors.New("route label contains a bidirectional or line-separator control character")
		}
	}
	return nil
}

// ExactSessionTarget is the daemon-neutral identity required to attach to a
// specific session lifecycle. The transport endpoint is deliberately absent:
// the selected route's dialer owns that boundary.
type ExactSessionTarget struct {
	LifecycleID domain.SessionLifecycleID
	SessionName string
}

func (t ExactSessionTarget) Validate() error {
	if t.LifecycleID == (domain.SessionLifecycleID{}) {
		return errors.New("missing session lifecycle")
	}
	if err := domain.ValidateSessionName(t.SessionName); err != nil {
		return fmt.Errorf("invalid session name: %w", err)
	}
	return nil
}

// CommittedRouteIdentity is the identity a daemon commits after accepting an
// attach. It is separate from display data so a stale presentation cannot
// become an attach authority.
type CommittedRouteIdentity struct {
	Target    ExactSessionTarget
	Ephemeral bool
}

func (i CommittedRouteIdentity) Validate() error {
	if err := i.Target.Validate(); err != nil {
		return err
	}
	return nil
}

// RoutePosition is mutable per-client route state published by the daemon.
// Target binds the tab cursor to one exact session lifecycle so delayed frames
// cannot update another route.
type RoutePosition struct {
	Target      ExactSessionTarget
	ActiveTabID domain.TabStableID
}

func (p RoutePosition) Validate() error {
	if err := p.Target.Validate(); err != nil {
		return err
	}
	if err := domain.ValidateTabStableID(p.ActiveTabID); err != nil {
		return fmt.Errorf("invalid active tab ID: %w", err)
	}
	return nil
}

// RouteRef is an opaque client-ledger reference. It is intentionally not a
// lifecycle identifier and is meaningful only with the matching generation.
type RouteRef struct {
	Key        uint64
	Generation uint64
}

// RouteAttentionTarget binds one displayed route reference to an exact
// lifecycle on the daemon currently serving the client. It carries no dialer,
// credential, or endpoint.
type RouteAttentionTarget struct {
	Ref    RouteRef
	Target ExactSessionTarget
}

// RouteAttentionSubscription is the bounded, client-owned mapping a daemon
// uses solely to resolve live attention while rendering its route snapshot.
type RouteAttentionSubscription struct {
	Targets []RouteAttentionTarget
}

func (r RouteRef) IsZero() bool { return r.Key == 0 && r.Generation == 0 }

func (r RouteRef) Validate() error {
	if r.IsZero() {
		return nil
	}
	if r.Key == 0 || r.Generation == 0 {
		return errors.New("route reference must contain key and generation")
	}
	return nil
}

// RecentRouteEntry is the complete daemon-neutral identity and display view of
// one route. Target carries only the committed lifecycle UUID and name; it
// contains no dialer, credential, endpoint, or live session pointer.
type RecentRouteEntry struct {
	Key          uint64
	Generation   uint64
	Target       ExactSessionTarget
	Name         string
	HostLabel    string
	Kind         RouteKind
	Ephemeral    bool
	Attention    bool
	Reachability RouteReachability
}

// RecentRouteSnapshot is an immutable, bounded publication of the client
// route ledger. Entries are ordered from most recent to least recent.
type RecentRouteSnapshot struct {
	Generation uint64
	Active     RouteRef
	// ActiveEntry maps Active's committed lifecycle UUID to its presentation.
	// It is not a navigation candidate and therefore remains separate from
	// Entries, which are ordered from most recent to least recent.
	ActiveEntry RecentRouteEntry
	Previous    RouteRef
	Home        RouteRef
	Entries     []RecentRouteEntry
}

// RouteNavigationAction asks the client to resolve one entry from a specific
// complete snapshot. It carries no session name, endpoint, or credential.
type RouteNavigationAction struct {
	SnapshotGeneration uint64
	Key                uint64
	Generation         uint64
}

// RouteCreateSessionAction asks the client to create a named session through
// one exact route authority from the latest complete snapshot.
type RouteCreateSessionAction struct {
	RequestID          uint64
	SnapshotGeneration uint64
	Key                uint64
	Generation         uint64
	SessionName        string
}

func (a RouteCreateSessionAction) Validate() error {
	if a.RequestID == 0 || a.SnapshotGeneration == 0 || a.Key == 0 || a.Generation == 0 || domain.ValidateSessionName(a.SessionName) != nil {
		return ErrInvalidRouteWire
	}
	return nil
}

// SamePeerSwitchRequest confirms a daemon-offered endpoint-empty target. It
// carries only the exact lifecycle identity and the client-owned tab cursor;
// transport origin remains proven by the existing authenticated connection.
type SamePeerSwitchRequest struct {
	RequestID      uint64
	Target         ExactSessionTarget
	PreferredTabID domain.TabStableID
}

func (r SamePeerSwitchRequest) Validate() error {
	if r.RequestID == 0 {
		return ErrInvalidRouteWire
	}
	if err := r.Target.Validate(); err != nil {
		return ErrInvalidRouteWire
	}
	if r.PreferredTabID != "" {
		if err := domain.ValidateTabStableID(r.PreferredTabID); err != nil {
			return ErrInvalidRouteWire
		}
	}
	return nil
}

// SamePeerSwitchFailureCode is a closed pre-commit rejection taxonomy.
type SamePeerSwitchFailureCode uint8

const (
	SamePeerSwitchStaleTarget SamePeerSwitchFailureCode = iota + 1
	SamePeerSwitchUnavailable
)

func (c SamePeerSwitchFailureCode) valid() bool {
	return c == SamePeerSwitchStaleTarget || c == SamePeerSwitchUnavailable
}

func (c SamePeerSwitchFailureCode) Validate() error {
	if !c.valid() {
		return ErrInvalidRouteWire
	}
	return nil
}

// SamePeerSwitchFailure leaves the source attachment unchanged. RequestID
// rejects a delayed failure after a later user selection.
type SamePeerSwitchFailure struct {
	RequestID uint64
	Code      SamePeerSwitchFailureCode
}

func (f SamePeerSwitchFailure) Validate() error {
	if f.RequestID == 0 || !f.Code.valid() {
		return ErrInvalidRouteWire
	}
	return nil
}

// RouteNavigationFailure is a bounded taxonomy for a rejected or stale route
// action. User-facing text remains local to the receiving client.
type RouteNavigationFailure struct {
	Key        uint64
	Generation uint64
	Code       RouteFailureCode
}

type RouteFailureCode uint8

const (
	RouteFailureStaleSelection RouteFailureCode = iota + 1
	RouteFailureNoSuchRoute
	RouteFailureUnavailable
	RouteFailureTargetChanged
	RouteFailureOriginUnavailable
)

func (c RouteFailureCode) valid() bool {
	switch c {
	case RouteFailureStaleSelection, RouteFailureNoSuchRoute, RouteFailureUnavailable, RouteFailureTargetChanged, RouteFailureOriginUnavailable:
		return true
	default:
		return false
	}
}

func (c RouteFailureCode) Validate() error {
	if !c.valid() {
		return ErrInvalidRouteWire
	}
	return nil
}

// SessionCreationFailure reports a correlated pre-commit create transition
// failure after the client has restored the source route.
type SessionCreationFailure struct {
	RequestID uint64
	Code      RouteFailureCode
}

func (f SessionCreationFailure) Validate() error {
	if f.RequestID == 0 || !f.Code.valid() {
		return ErrInvalidRouteWire
	}
	return nil
}

const (
	// RouteSnapshotMaxEntries bounds one immutable publication and the amount
	// of work a receiver performs before returning to its transport loop. The
	// private client history may use a smaller product cap.
	RouteSnapshotMaxEntries = 32
	RouteLabelMaxBytes      = 256
)
