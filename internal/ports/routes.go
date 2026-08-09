package ports

import (
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
)

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

// RouteRef is an opaque client-ledger reference. It is intentionally not a
// lifecycle identifier and is meaningful only with the matching generation.
type RouteRef struct {
	Key        uint64
	Generation uint64
}

func (r RouteRef) empty() bool { return r.Key == 0 && r.Generation == 0 }

func (r RouteRef) Validate() error {
	if r.empty() {
		return nil
	}
	if r.Key == 0 || r.Generation == 0 {
		return errors.New("route reference must contain key and generation")
	}
	return nil
}

// RecentRouteEntry is the complete daemon-neutral display view of one route.
// It contains no dialer, credential, endpoint, live session pointer, or attach
// target. The client ledger keeps those private beside this value.
type RecentRouteEntry struct {
	Key          uint64
	Generation   uint64
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
	Previous   RouteRef
	Home       RouteRef
	Entries    []RecentRouteEntry
}

// RouteNavigationAction asks the client to resolve one captured route key and
// generation. It carries no session name, endpoint, or credential.
type RouteNavigationAction struct {
	Key        uint64
	Generation uint64
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

const (
	// RouteSnapshotMaxEntries bounds one immutable publication and the amount
	// of work a receiver performs before returning to its transport loop.
	RouteSnapshotMaxEntries = 32
	RouteLabelMaxBytes      = 128
)
