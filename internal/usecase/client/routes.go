package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// maxRouteLedgerEntries is the product history cap. It is deliberately
// smaller than the wire parser bound so an active metadata entry can be
// excluded without allowing an oversized client history.
const maxRouteLedgerEntries = 20

var (
	errRouteResumeUnavailable = errors.New("route resume unavailable")
	errRouteStaleSelection    = errors.New("route selection is stale")
	errRouteTargetChanged     = errors.New("route target changed")
)

// routeKey is process-local identity. It is never derived from a daemon
// lifecycle ID and is not exposed outside the client ledger. originKey keeps
// distinct daemon endpoints separate even when their lifecycle/name pairs
// happen to match; neither is serialized into the route snapshot.
type routeKey uint64

type routeGeneration uint64

type routeIdentity struct {
	key        routeKey
	generation routeGeneration
}

func (i routeIdentity) empty() bool { return i.key == 0 && i.generation == 0 }

func (i routeIdentity) wire() ports.RouteRef {
	return ports.RouteRef{Key: uint64(i.key), Generation: uint64(i.generation)}
}

func identityFromWire(ref ports.RouteRef) routeIdentity {
	return routeIdentity{key: routeKey(ref.Key), generation: routeGeneration(ref.Generation)}
}

// routePresentation contains only immutable display data. Connection and
// attach capabilities live in routeRecord beside it and are never copied into
// a RecentRouteSnapshot.
type routePresentation struct {
	name         string
	hostLabel    string
	kind         ports.RouteKind
	ephemeral    bool
	attention    bool
	reachability ports.RouteReachability
}

type routeCandidate struct {
	origin       ports.RouteOrigin
	originKey    string
	target       ports.ExactSessionTarget
	presentation routePresentation
	dialer       ports.Dialer
	request      AttachRequest
	resumeToken  uint64
	home         bool
}

type routeRecord struct {
	identity     routeIdentity
	origin       ports.RouteOrigin
	originKey    string
	target       ports.ExactSessionTarget
	presentation routePresentation
	dialer       ports.Dialer
	request      AttachRequest
	resumeToken  uint64
	home         bool
}

// routeLedger owns client-global route identity/history for one client
// process. Its lock protects both the bounded record set and active/previous
// references; snapshots are copied while holding the lock and are immutable
// after publication.
type routeLedger struct {
	mu         sync.RWMutex
	nextKey    routeKey
	generation routeGeneration
	entries    []routeRecord
	active     routeIdentity
	previous   routeIdentity
	home       routeIdentity

	discoveryMu    sync.Mutex
	discoveryCycle uint64
}

func newRouteLedger() *routeLedger { return &routeLedger{} }

func cloneCommittedIdentity(identity *ports.CommittedRouteIdentity) *ports.CommittedRouteIdentity {
	if identity == nil {
		return nil
	}
	copy := *identity
	return &copy
}

func routeCandidateForAttach(request AttachRequest, identity ports.CommittedRouteIdentity, dialer ports.Dialer, resumeToken uint64, home bool) routeCandidate {
	origin := normalizeRouteOrigin(request.Origin, request.Remote)
	originKey := normalizeRouteOriginKey(request.OriginKey, origin)
	kind := ports.RouteKindLocal
	hostLabel := ""
	if request.Remote || request.RemoteTarget != nil || origin != ports.RouteOriginLocal {
		kind = ports.RouteKindRemote
		if request.RemoteTarget != nil {
			hostLabel = request.RemoteTarget.DisplayOrigin
		}
	}
	request = cloneAttachRequest(request)
	request.Origin = origin
	request.OriginKey = originKey
	request.SessionName = identity.Target.SessionName
	request.ExactTarget = &identity.Target
	return routeCandidate{
		origin:    origin,
		originKey: originKey,
		target:    identity.Target,
		presentation: routePresentation{
			name:         identity.Target.SessionName,
			hostLabel:    hostLabel,
			kind:         kind,
			ephemeral:    identity.Ephemeral,
			reachability: ports.RouteReachabilityReachable,
		},
		dialer:      dialer,
		request:     request,
		resumeToken: resumeToken,
		home:        home,
	}
}

func normalizeRouteOrigin(origin ports.RouteOrigin, remote bool) ports.RouteOrigin {
	if origin != 0 {
		return origin
	}
	if remote {
		return ports.RouteOriginRemote
	}
	return ports.RouteOriginLocal
}

func normalizeRouteOriginKey(key string, origin ports.RouteOrigin) string {
	if key != "" {
		return key
	}
	switch origin {
	case ports.RouteOriginLocal:
		return "local"
	case ports.RouteOriginDiscovery:
		return "discovery"
	default:
		return "remote"
	}
}

func validateRoutePresentation(p routePresentation) error {
	if err := ports.ValidateRouteLabel(p.name, false); err != nil {
		return err
	}
	if err := ports.ValidateRouteLabel(p.hostLabel, true); err != nil {
		return err
	}
	if err := p.kind.Validate(); err != nil {
		return err
	}
	if err := p.reachability.Validate(); err != nil {
		return err
	}
	return nil
}

func (l *routeLedger) nextIdentityLocked() (routeIdentity, error) {
	if l.nextKey == ^routeKey(0) || l.generation == ^routeGeneration(0) {
		return routeIdentity{}, errors.New("route ledger identity space exhausted")
	}
	l.nextKey++
	l.generation++
	return routeIdentity{key: l.nextKey, generation: l.generation}, nil
}

func (l *routeLedger) findByTargetLocked(origin ports.RouteOrigin, originKey string, target ports.ExactSessionTarget) int {
	for i := range l.entries {
		entry := l.entries[i]
		if entry.origin == origin && entry.originKey == originKey && entry.target == target {
			return i
		}
	}
	return -1
}

func (l *routeLedger) moveToFrontLocked(index int, entry routeRecord) {
	if index < 0 {
		l.entries = append([]routeRecord{entry}, l.entries...)
		return
	}
	l.entries = append(l.entries[:index], l.entries[index+1:]...)
	l.entries = append([]routeRecord{entry}, l.entries...)
}

func (l *routeLedger) removeAtLocked(index int) {
	if index < 0 || index >= len(l.entries) {
		return
	}
	removed := l.entries[index].identity
	l.entries = append(l.entries[:index], l.entries[index+1:]...)
	for _, ref := range []*routeIdentity{&l.active, &l.previous, &l.home} {
		if *ref == removed {
			*ref = routeIdentity{}
		}
	}
}

func (l *routeLedger) evictLocked() {
	for len(l.entries) > maxRouteLedgerEntries {
		index := len(l.entries) - 1
		for i := len(l.entries) - 1; i >= 0; i-- {
			identity := l.entries[i].identity
			if identity != l.active && identity != l.previous && identity != l.home {
				index = i
				break
			}
		}
		l.removeAtLocked(index)
	}
}

func (l *routeLedger) commit(candidate routeCandidate) (routeIdentity, error) {
	if err := candidate.origin.Validate(); err != nil {
		return routeIdentity{}, err
	}
	if err := candidate.target.Validate(); err != nil {
		return routeIdentity{}, err
	}
	if err := validateRoutePresentation(candidate.presentation); err != nil {
		return routeIdentity{}, err
	}
	candidate.originKey = normalizeRouteOriginKey(candidate.originKey, candidate.origin)
	candidate.request = cloneAttachRequest(candidate.request)
	candidate.request.Origin = candidate.origin
	candidate.request.OriginKey = candidate.originKey
	candidate.request.ExactTarget = &candidate.target

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.commitLocked(candidate)
}

func (l *routeLedger) commitLocked(candidate routeCandidate) (routeIdentity, error) {
	identity, err := l.nextIdentityLocked()
	if err != nil {
		return routeIdentity{}, err
	}
	index := l.findByTargetLocked(candidate.origin, candidate.originKey, candidate.target)
	if index >= 0 {
		identity.key = l.entries[index].identity.key
	}
	entry := routeRecord{
		identity:     identity,
		origin:       candidate.origin,
		originKey:    candidate.originKey,
		target:       candidate.target,
		presentation: candidate.presentation,
		dialer:       candidate.dialer,
		request:      candidate.request,
		resumeToken:  candidate.resumeToken,
		home:         candidate.home,
	}
	oldActive := l.active
	l.moveToFrontLocked(index, entry)
	if !oldActive.empty() && oldActive.key != identity.key {
		l.previous = oldActive
	}
	if l.previous.key == identity.key {
		l.previous = routeIdentity{}
	}
	l.active = identity
	if candidate.home {
		l.home = identity
	}
	if l.home.key == identity.key {
		l.home.generation = identity.generation
	}
	if l.previous.key != 0 {
		if previousIndex := l.indexByIdentityLocked(l.previous); previousIndex < 0 {
			l.previous = routeIdentity{}
		}
	}
	l.evictLocked()
	return identity, nil
}

func (l *routeLedger) indexByIdentityLocked(identity routeIdentity) int {
	for i := range l.entries {
		if l.entries[i].identity == identity {
			return i
		}
	}
	return -1
}

func (l *routeLedger) lookup(ref ports.RouteRef) (routeRecord, bool) {
	identity := identityFromWire(ref)
	if identity.empty() {
		return routeRecord{}, false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, entry := range l.entries {
		if entry.identity == identity {
			return cloneRouteRecord(entry), true
		}
	}
	return routeRecord{}, false
}

func cloneRouteRecord(entry routeRecord) routeRecord {
	entry.request = cloneAttachRequest(entry.request)
	return entry
}

func (l *routeLedger) snapshot() ports.RecentRouteSnapshot {
	l.mu.RLock()
	defer l.mu.RUnlock()

	snapshot := ports.RecentRouteSnapshot{
		Generation: uint64(l.generation),
		Active:     l.active.wire(),
		Previous:   l.previous.wire(),
		Home:       l.home.wire(),
	}
	if len(l.entries) == 0 {
		return snapshot
	}
	snapshot.Entries = make([]ports.RecentRouteEntry, 0, len(l.entries))
	for _, entry := range l.entries {
		if entry.identity == l.active {
			continue
		}
		snapshot.Entries = append(snapshot.Entries, ports.RecentRouteEntry{
			Key:          uint64(entry.identity.key),
			Generation:   uint64(entry.identity.generation),
			Name:         entry.presentation.name,
			HostLabel:    entry.presentation.hostLabel,
			Kind:         entry.presentation.kind,
			Ephemeral:    entry.presentation.ephemeral,
			Attention:    entry.presentation.attention,
			Reachability: entry.presentation.reachability,
		})
	}
	return snapshot
}

func (l *routeLedger) activeRef() ports.RouteRef {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.active.wire()
}

func (l *routeLedger) previousRef() ports.RouteRef {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.previous.wire()
}

func (l *routeLedger) homeRef() ports.RouteRef {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.home.wire()
}

// routeTransitionPlan is the private lookup result used by navigation. It
// preserves the original origin and exact target while leaving display labels
// and transport capabilities separate.
type routeTransitionPlan struct {
	identity  routeIdentity
	record    routeRecord
	origin    ports.RouteOrigin
	originKey string
	target    ports.ExactSessionTarget
}

type routeTransitionCommit struct {
	identity     ports.CommittedRouteIdentity
	presentation routePresentation
	dialer       ports.Dialer
	resumeToken  uint64
}

type routeTransitionConnector interface {
	Resume(context.Context, routeRecord) (routeTransitionCommit, error)
	Attach(context.Context, routeRecord) (routeTransitionCommit, error)
	Restore(context.Context, routeRecord) error
}

// navigationRecord validates that an action belongs to the latest complete
// snapshot. The active route is metadata-only and selecting it is a no-op.
func (l *routeLedger) navigationRecord(action ports.RouteNavigationAction) (routeRecord, bool, bool) {
	if action.SnapshotGeneration == 0 || action.Key == 0 || action.Generation == 0 {
		return routeRecord{}, false, false
	}
	identity := identityFromWire(ports.RouteRef{Key: action.Key, Generation: action.Generation})
	l.mu.RLock()
	defer l.mu.RUnlock()
	if uint64(l.generation) != action.SnapshotGeneration {
		return routeRecord{}, false, false
	}
	if identity == l.active {
		return routeRecord{}, true, true
	}
	index := l.indexByIdentityLocked(identity)
	if index < 0 {
		return routeRecord{}, false, false
	}
	return cloneRouteRecord(l.entries[index]), true, false
}

// navigate resolves a captured key/generation, attempts resume before exact
// attach, and commits only after the connector returns a matching identity.
// A failed transition restores the prior live route and leaves history alone.
func (l *routeLedger) navigate(ctx context.Context, action ports.RouteNavigationAction, connector routeTransitionConnector) error {
	record, ok, noOp := l.navigationRecord(action)
	if !ok {
		return errRouteStaleSelection
	}
	if noOp {
		return nil
	}
	if connector == nil {
		return errors.New("route transition connector is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	selected := ports.RouteRef{Key: action.Key, Generation: action.Generation}
	plan := routeTransitionPlan{
		identity:  identityFromWire(selected),
		record:    record,
		origin:    record.origin,
		originKey: record.originKey,
		target:    record.target,
	}
	restoreOnFailure := func(err error) error {
		if restoreErr := connector.Restore(ctx, record); restoreErr != nil {
			return errors.Join(err, fmt.Errorf("restoring prior route: %w", restoreErr))
		}
		return err
	}

	var commit routeTransitionCommit
	var err error
	if record.resumeToken != 0 {
		commit, err = connector.Resume(ctx, record)
		if errors.Is(err, errRouteResumeUnavailable) {
			commit, err = connector.Attach(ctx, record)
		}
	} else {
		commit, err = connector.Attach(ctx, record)
	}
	if err != nil {
		return restoreOnFailure(err)
	}
	if err := commit.identity.Validate(); err != nil {
		return restoreOnFailure(fmt.Errorf("invalid committed route identity: %w", err))
	}
	if commit.identity.Target != plan.target {
		return restoreOnFailure(errRouteTargetChanged)
	}
	if err := validateRoutePresentation(commit.presentation); err != nil {
		return restoreOnFailure(err)
	}

	candidate := routeCandidate{
		origin:       plan.origin,
		originKey:    plan.originKey,
		target:       plan.target,
		presentation: commit.presentation,
		dialer:       commit.dialer,
		request:      cloneAttachRequest(plan.record.request),
		resumeToken:  commit.resumeToken,
		home:         plan.record.home,
	}
	candidate.request.Origin = plan.origin
	candidate.request.OriginKey = plan.originKey
	candidate.request.ExactTarget = &candidate.target
	if err := l.commitTransition(plan.identity, candidate); err != nil {
		return restoreOnFailure(err)
	}
	return nil
}

func (l *routeLedger) commitTransition(selected routeIdentity, candidate routeCandidate) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	index := l.indexByIdentityLocked(selected)
	if index < 0 {
		return errRouteStaleSelection
	}
	if _, err := l.commitLocked(candidate); err != nil {
		return err
	}
	return nil
}

// beginDiscoveryCycle and discoveryCycleCurrent deliberately use a lock
// separate from route history. A stale discovery result cannot clear or commit
// unrelated navigation history.
func (l *routeLedger) beginDiscoveryCycle() uint64 {
	l.discoveryMu.Lock()
	defer l.discoveryMu.Unlock()
	l.discoveryCycle++
	if l.discoveryCycle == 0 {
		l.discoveryCycle++
	}
	return l.discoveryCycle
}

func (l *routeLedger) discoveryCycleCurrent(cycle uint64) bool {
	if cycle == 0 {
		return false
	}
	l.discoveryMu.Lock()
	defer l.discoveryMu.Unlock()
	return l.discoveryCycle == cycle
}
