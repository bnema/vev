package protocol

import (
	"crypto/sha256"
	"encoding/hex"
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

const NoTabIndex int32 = -1

// SessionAttachTarget is the daemon-local, transport-neutral identity used by
// session handshakes and handoffs. Endpoint, display origin, and registration
// authority deliberately remain outside this contract.
type SessionAttachTarget struct {
	SessionID        domain.SessionID
	LifecycleID      domain.SessionLifecycleID
	SessionName      string
	TabID            domain.TabStableID
	TabIndex         int32
	TabRawName       string
	TabExpectedCount uint16
	Stopped          bool
}

func (t SessionAttachTarget) selector() domain.TabSelector {
	if t.TabID != "" {
		return domain.NewStableTabSelector(t.TabID)
	}
	if t.TabIndex != NoTabIndex {
		return domain.NewOrdinalTabSelector(uint16(t.TabIndex), t.TabRawName, t.TabExpectedCount)
	}
	return domain.TabSelector{}
}

func (t SessionAttachTarget) ResolveTab(tabs []domain.TabSelectorTab) (int, bool) {
	selector := t.selector()
	if selector == (domain.TabSelector{}) {
		return 0, len(tabs) <= 1
	}
	return selector.Resolve(tabs)
}

func (t SessionAttachTarget) Validate() error {
	if t.LifecycleID == (domain.SessionLifecycleID{}) {
		return errors.New("missing session lifecycle")
	}
	if err := domain.ValidateSessionName(t.SessionName); err != nil {
		return fmt.Errorf("invalid session name: %w", err)
	}
	if t.Stopped && t.SessionID != "" {
		return errors.New("stopped target carries a session ID")
	}
	switch {
	case t.TabID != "":
		if domain.ValidateTabStableID(t.TabID) != nil || t.TabIndex != NoTabIndex || t.TabRawName != "" || t.TabExpectedCount != 0 {
			return errors.New("invalid stable tab selector")
		}
	case t.TabIndex != NoTabIndex:
		if t.TabIndex < 0 {
			return errors.New("invalid ordinal tab selector")
		}
		if err := domain.NewOrdinalTabSelector(uint16(t.TabIndex), t.TabRawName, t.TabExpectedCount).Validate(); err != nil {
			return fmt.Errorf("invalid ordinal tab selector: %w", err)
		}
	case t.TabRawName != "" || t.TabExpectedCount != 0:
		return errors.New("incomplete tab selector")
	}
	if !t.Stopped && t.TabID == "" {
		return errors.New("live target requires a stable tab selector")
	}
	return nil
}

// SessionAttachTargetFromRemote strips routing and presentation data from a
// picker target before it enters the daemon session contract.
func SessionAttachTargetFromRemote(remote domain.RemoteSessionTarget) SessionAttachTarget {
	t := SessionAttachTarget{LifecycleID: remote.LifecycleID, SessionName: remote.SessionName, TabIndex: NoTabIndex, Stopped: remote.Stopped}
	if !remote.Stopped {
		t.TabID = remote.LiveTabID
		return t
	}
	switch remote.StoppedTab.Kind {
	case domain.TabSelectorByStableID:
		t.TabID = remote.StoppedTab.StableID
	case domain.TabSelectorByOrdinal:
		t.TabIndex = int32(remote.StoppedTab.Ordinal)
		t.TabRawName = remote.StoppedTab.RawName
		t.TabExpectedCount = remote.StoppedTab.ExpectedCount
	}
	return t
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
// lifecycle on the serving daemon or a monitored inventory source. It carries
// no dialer, credential, or endpoint.
type RouteAttentionTarget struct {
	Ref    RouteRef
	Target ExactSessionTarget
	// SourceKey is empty for the serving daemon, or an opaque monitored
	// inventory source. It never carries an endpoint or credential.
	SourceKey string
}

// RouteRetired invalidates one subscribed exact lifecycle, not its name.
// The reference fences delayed notifications against replacement routes.
type RouteRetired struct {
	Ref    RouteRef
	Target ExactSessionTarget
}

// Validate rejects a retirement without the nonzero reference that fences it
// and without a valid lifecycle target. Both encode and decode call it.
func (r RouteRetired) Validate() error {
	if r.Ref.IsZero() || r.Ref.Validate() != nil {
		return fmt.Errorf("%w: retired route reference is zero", ErrInvalidRouteWire)
	}
	if err := r.Target.Validate(); err != nil {
		return fmt.Errorf("%w: invalid retired route target: %v", ErrInvalidRouteWire, err)
	}
	return nil
}

// RemoteInventorySourceKey identifies a configured endpoint without disclosing it.
func RemoteInventorySourceKey(endpoint string) string {
	sum := sha256.Sum256([]byte("vev-inventory-remote\x00" + endpoint))
	return NavigationInventoryRemoteSourcePrefix + hex.EncodeToString(sum[:12])
}

// RouteAttentionSubscription is the bounded, client-owned mapping a daemon
// uses to resolve live attention and retire authoritatively absent lifecycles.
type RouteAttentionSubscription struct {
	Targets []RouteAttentionTarget
}

// Validate enforces the bounded, duplicate-free attention mapping invariants
// on both the encode and decode sides of the wire conversion.
func (r RouteAttentionSubscription) Validate() error {
	if len(r.Targets) > RouteSnapshotMaxEntries {
		return fmt.Errorf("%w: too many attention targets", ErrInvalidRouteWire)
	}
	refs := make(map[RouteRef]struct{}, len(r.Targets))
	for _, target := range r.Targets {
		if err := ValidateRouteLabel(target.SourceKey, true); err != nil {
			return fmt.Errorf("%w: invalid route source", ErrInvalidRouteWire)
		}
		if err := target.Ref.Validate(); err != nil || target.Ref.IsZero() {
			return fmt.Errorf("%w: invalid attention route reference", ErrInvalidRouteWire)
		}
		if err := target.Target.Validate(); err != nil {
			return fmt.Errorf("%w: invalid attention target: %v", ErrInvalidRouteWire, err)
		}
		if _, exists := refs[target.Ref]; exists {
			return fmt.Errorf("%w: duplicate attention route reference", ErrInvalidRouteWire)
		}
		refs[target.Ref] = struct{}{}
	}
	return nil
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
	// AttentionSeq orders attention onsets across every daemon the client
	// observes: a smaller value is older. It is zero without attention.
	AttentionSeq uint64
}

// RouteHost is one daemon the client can create a session on, other than
// the serving daemon. Its reference shares the entry namespace; it carries
// no endpoint, dialer, or credential.
type RouteHost struct {
	Key        uint64
	Generation uint64
	Label      string
	Kind       RouteKind
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
	// Hosts are the creation destinations besides the serving daemon.
	Hosts []RouteHost
}

// validateRecentRouteEntry enforces one entry's identity and display bounds.
func validateRecentRouteEntry(entry RecentRouteEntry) error {
	if entry.Key == 0 || entry.Generation == 0 {
		return fmt.Errorf("%w: route entry identity is zero", ErrInvalidRouteWire)
	}
	if err := entry.Target.Validate(); err != nil {
		return fmt.Errorf("%w: invalid route lifecycle target: %v", ErrInvalidRouteWire, err)
	}
	if entry.Name != entry.Target.SessionName {
		return fmt.Errorf("%w: route name does not match lifecycle target", ErrInvalidRouteWire)
	}
	if err := ValidateRouteLabel(entry.Name, false); err != nil {
		return fmt.Errorf("%w: route name: %v", ErrInvalidRouteWire, err)
	}
	if err := ValidateRouteLabel(entry.HostLabel, true); err != nil {
		return fmt.Errorf("%w: route host label: %v", ErrInvalidRouteWire, err)
	}
	if entry.Kind.Validate() != nil {
		return fmt.Errorf("%w: invalid route kind", ErrInvalidRouteWire)
	}
	if entry.Reachability.Validate() != nil {
		return fmt.Errorf("%w: invalid route reachability", ErrInvalidRouteWire)
	}
	if entry.AttentionSeq != 0 && !entry.Attention {
		return fmt.Errorf("%w: attention order without attention", ErrInvalidRouteWire)
	}
	return nil
}

func validateRouteHost(host RouteHost) error {
	if host.Key == 0 || host.Generation == 0 {
		return fmt.Errorf("%w: route host identity is zero", ErrInvalidRouteWire)
	}
	if err := ValidateRouteLabel(host.Label, false); err != nil {
		return fmt.Errorf("%w: route host label: %v", ErrInvalidRouteWire, err)
	}
	if host.Kind.Validate() != nil {
		return fmt.Errorf("%w: invalid route host kind", ErrInvalidRouteWire)
	}
	return nil
}

// Validate enforces the bounded, self-consistent invariants of one complete
// route publication: entry budget, nonzero generation for non-empty
// snapshots, matching active presentation, unique entry references, and
// previous/home references that resolve inside the snapshot. Both encode and
// decode call it so a malformed snapshot cannot be produced or accepted.
func (s RecentRouteSnapshot) Validate() error {
	if len(s.Entries) > RouteSnapshotMaxEntries {
		return fmt.Errorf("%w: too many route entries", ErrInvalidRouteWire)
	}
	if len(s.Hosts) > RouteSnapshotMaxHosts {
		return fmt.Errorf("%w: too many route hosts", ErrInvalidRouteWire)
	}
	if s.Generation == 0 && (len(s.Entries) != 0 || len(s.Hosts) != 0 || s.ActiveEntry != (RecentRouteEntry{})) {
		return fmt.Errorf("%w: non-empty snapshot has zero generation", ErrInvalidRouteWire)
	}
	for _, ref := range []RouteRef{s.Active, s.Previous, s.Home} {
		if err := ref.Validate(); err != nil {
			return fmt.Errorf("%w: route reference: %v", ErrInvalidRouteWire, err)
		}
	}
	if s.Active.IsZero() {
		if s.ActiveEntry != (RecentRouteEntry{}) {
			return fmt.Errorf("%w: active presentation has no active route", ErrInvalidRouteWire)
		}
	} else {
		if err := validateRecentRouteEntry(s.ActiveEntry); err != nil {
			return err
		}
		if s.Active != (RouteRef{Key: s.ActiveEntry.Key, Generation: s.ActiveEntry.Generation}) {
			return fmt.Errorf("%w: active presentation does not match active route", ErrInvalidRouteWire)
		}
	}
	if !s.Active.IsZero() && s.Active == s.Previous {
		return fmt.Errorf("%w: active and previous routes are identical", ErrInvalidRouteWire)
	}
	refs := make(map[RouteRef]struct{}, len(s.Entries))
	for _, entry := range s.Entries {
		if s.Active == (RouteRef{Key: entry.Key, Generation: entry.Generation}) {
			return fmt.Errorf("%w: active route must be metadata-only", ErrInvalidRouteWire)
		}
		if err := validateRecentRouteEntry(entry); err != nil {
			return err
		}
		ref := RouteRef{Key: entry.Key, Generation: entry.Generation}
		if _, exists := refs[ref]; exists {
			return fmt.Errorf("%w: duplicate route entry", ErrInvalidRouteWire)
		}
		refs[ref] = struct{}{}
	}
	for _, host := range s.Hosts {
		if err := validateRouteHost(host); err != nil {
			return err
		}
		ref := RouteRef{Key: host.Key, Generation: host.Generation}
		if _, exists := refs[ref]; exists || ref == s.Active {
			return fmt.Errorf("%w: duplicate route host", ErrInvalidRouteWire)
		}
		refs[ref] = struct{}{}
	}
	for _, item := range []struct {
		name string
		ref  RouteRef
	}{
		{name: "previous", ref: s.Previous},
		{name: "home", ref: s.Home},
	} {
		if item.ref.IsZero() || item.ref == s.Active {
			// Active is metadata-only and intentionally excluded from the
			// recent display entries. Home may point at active as well.
			continue
		}
		if _, exists := refs[item.ref]; !exists {
			return fmt.Errorf("%w: %s route reference is absent from snapshot", ErrInvalidRouteWire, item.name)
		}
	}
	return nil
}

// RouteNavigationAction asks the client to resolve one entry from a specific
// complete snapshot. It carries no session name, endpoint, or credential.
type RouteNavigationAction struct {
	CauseActionID      uint64
	SnapshotGeneration uint64
	Key                uint64
	Generation         uint64
}

// Validate rejects a navigation action whose snapshot/entry identity is zero;
// both encode and decode call it so a stale or malformed action never ships.
func (a RouteNavigationAction) Validate() error {
	if a.SnapshotGeneration == 0 || a.Key == 0 || a.Generation == 0 {
		return fmt.Errorf("%w: navigation action identity is zero", ErrInvalidRouteWire)
	}
	return nil
}

// RouteCreateSessionAction asks the client to create a named session through
// one exact route authority from the latest complete snapshot.
type RouteCreateSessionAction struct {
	CauseActionID      uint64
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

// Validate requires the nonzero entry identity that correlates the failure
// with its navigation action, plus a bounded failure code. Both encode and
// decode call it so a zero identity never ships.
func (f RouteNavigationFailure) Validate() error {
	if f.Key == 0 || f.Generation == 0 {
		return fmt.Errorf("%w: failure identity is zero", ErrInvalidRouteWire)
	}
	return f.Code.Validate()
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
	// RouteSnapshotMaxHosts bounds the creation destinations of one snapshot.
	RouteSnapshotMaxHosts = 32
	RouteLabelMaxBytes    = 256
)
