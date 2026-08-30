package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
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
// happen to match; it is not serialized into the route snapshot.
type routeKey uint64

type routeGeneration uint64

type routeIdentity struct {
	key        routeKey
	generation routeGeneration
}

func (i routeIdentity) empty() bool { return i.key == 0 && i.generation == 0 }

func (i routeIdentity) wire() protocol.RouteRef {
	return protocol.RouteRef{Key: uint64(i.key), Generation: uint64(i.generation)}
}

func identityFromWire(ref protocol.RouteRef) routeIdentity {
	return routeIdentity{key: routeKey(ref.Key), generation: routeGeneration(ref.Generation)}
}

// routePresentation contains only immutable display data. Connection and
// attach capabilities live in routeRecord beside it and are never copied into
// a RecentRouteSnapshot.
type routePresentation struct {
	name         string
	hostLabel    string
	kind         protocol.RouteKind
	ephemeral    bool
	attention    bool
	reachability protocol.RouteReachability
}

type routeCandidate struct {
	origin       protocol.RouteOrigin
	originKey    string
	target       protocol.ExactSessionTarget
	presentation routePresentation
	dialer       ports.ClientDialer
	request      AttachRequest
	resumeToken  uint64
	home         bool
}

type routeRecord struct {
	identity     routeIdentity
	origin       protocol.RouteOrigin
	originKey    string
	target       protocol.ExactSessionTarget
	presentation routePresentation
	dialer       ports.ClientDialer
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

func cloneCommittedIdentity(identity *protocol.CommittedRouteIdentity) *protocol.CommittedRouteIdentity {
	if identity == nil {
		return nil
	}
	clone := *identity
	return &clone
}

func cloneRoutePosition(position *protocol.RoutePosition) *protocol.RoutePosition {
	if position == nil {
		return nil
	}
	clone := *position
	return &clone
}

func routeCandidateForAttach(request AttachRequest, identity protocol.CommittedRouteIdentity, dialer ports.ClientDialer, resumeToken uint64) routeCandidate {
	origin := normalizeRouteOrigin(request.Origin, request.Remote)
	originKey := normalizeRouteOriginKey(request.OriginKey, origin)
	kind := protocol.RouteKindLocal
	hostLabel := ""
	if request.Remote || request.RemoteTarget != nil || origin != protocol.RouteOriginLocal {
		kind = protocol.RouteKindRemote
		if request.RemoteTarget != nil {
			hostLabel = domain.RemoteDisplayOrigin(request.RemoteTarget.DisplayOrigin)
		}
		if hostLabel == "" {
			hostLabel = domain.RemoteDisplayOrigin(request.HostLabel)
		}
		if hostLabel == "" {
			hostLabel = domain.RemoteDisplayOrigin(originKey)
		}
	}
	request = cloneAttachRequest(request)
	request.Origin = origin
	request.OriginKey = originKey
	request.SessionName = identity.Target.SessionName
	request.ExactTarget = &identity.Target
	request.HostLabel = hostLabel
	// Discovery targets are point-in-time attach authority, including a live
	// tab selector. Once Welcome commits an exact route, mutable tab memory is
	// carried independently by PreferredTabID.
	request.RemoteTarget = nil
	return routeCandidate{
		origin:    origin,
		originKey: originKey,
		target:    identity.Target,
		presentation: routePresentation{
			name:         identity.Target.SessionName,
			hostLabel:    hostLabel,
			kind:         kind,
			ephemeral:    identity.Ephemeral,
			reachability: protocol.RouteReachabilityReachable,
		},
		dialer:      dialer,
		request:     request,
		resumeToken: resumeToken,
	}
}

func routeCandidateForCommittedIdentity(active routeRecord, identity protocol.CommittedRouteIdentity) routeCandidate {
	request := cloneAttachRequest(active.request)
	request.SessionName = identity.Target.SessionName
	request.ExactTarget = &identity.Target
	request.RemoteTarget = nil
	if active.target != identity.Target {
		request.PreferredTabID = ""
	}
	return routeCandidate{
		origin:    active.origin,
		originKey: active.originKey,
		target:    identity.Target,
		presentation: routePresentation{
			name:         identity.Target.SessionName,
			hostLabel:    active.presentation.hostLabel,
			kind:         active.presentation.kind,
			ephemeral:    identity.Ephemeral,
			reachability: protocol.RouteReachabilityReachable,
		},
		dialer:      active.dialer,
		request:     request,
		resumeToken: active.resumeToken,
		home:        false,
	}
}

func normalizeRouteOrigin(origin protocol.RouteOrigin, remote bool) protocol.RouteOrigin {
	if origin != 0 {
		return origin
	}
	if remote {
		return protocol.RouteOriginRemote
	}
	return protocol.RouteOriginLocal
}

func routeFailureCode(err error) protocol.RouteFailureCode {
	var protocolErr *ProtocolError
	if errors.As(err, &protocolErr) {
		switch protocolErr.Code {
		case protocol.ErrNoSuchSession, protocol.ErrNoSuchTarget:
			return protocol.RouteFailureNoSuchRoute
		}
	}
	if errors.Is(err, errRouteTargetChanged) {
		return protocol.RouteFailureTargetChanged
	}
	return protocol.RouteFailureUnavailable
}

func normalizeRouteOriginKey(key string, origin protocol.RouteOrigin) string {
	if key != "" {
		return key
	}
	switch origin {
	case protocol.RouteOriginLocal:
		return "local"
	case protocol.RouteOriginDiscovery:
		return "discovery"
	default:
		return "remote"
	}
}

func validateRoutePresentation(p routePresentation) error {
	if err := protocol.ValidateRouteLabel(p.name, false); err != nil {
		return err
	}
	if err := protocol.ValidateRouteLabel(p.hostLabel, true); err != nil {
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

func (l *routeLedger) findByTargetLocked(origin protocol.RouteOrigin, originKey string, target protocol.ExactSessionTarget) int {
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

func prepareRouteCandidate(candidate routeCandidate) (routeCandidate, error) {
	if err := candidate.origin.Validate(); err != nil {
		return routeCandidate{}, err
	}
	if err := candidate.target.Validate(); err != nil {
		return routeCandidate{}, err
	}
	if err := validateRoutePresentation(candidate.presentation); err != nil {
		return routeCandidate{}, err
	}
	candidate.originKey = normalizeRouteOriginKey(candidate.originKey, candidate.origin)
	candidate.request = cloneAttachRequest(candidate.request)
	candidate.request.Origin = candidate.origin
	candidate.request.OriginKey = candidate.originKey
	candidate.request.ExactTarget = &candidate.target
	return candidate, nil
}

func (l *routeLedger) commit(candidate routeCandidate) (routeIdentity, error) {
	candidate, err := prepareRouteCandidate(candidate)
	if err != nil {
		return routeIdentity{}, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.commitLocked(candidate)
}

// commitAttach assigns the process home route while holding the same lock as
// the commit. Two concurrent initial attachments therefore cannot both claim
// an empty home reference.
func (l *routeLedger) commitAttach(candidate routeCandidate) (routeIdentity, error) {
	candidate, err := prepareRouteCandidate(candidate)
	if err != nil {
		return routeIdentity{}, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	candidate.home = l.home.empty()
	return l.commitLocked(candidate)
}

// commitCommittedIdentity snapshots the current active record and commits the
// daemon's local transition under one lock. The active route, origin, dialer,
// and attach request cannot be read from different ledger generations.
func (l *routeLedger) commitCommittedIdentity(identity protocol.CommittedRouteIdentity) (routeIdentity, error) {
	if err := identity.Validate(); err != nil {
		return routeIdentity{}, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	activeIndex := l.indexByIdentityLocked(l.active)
	if activeIndex < 0 {
		return routeIdentity{}, errors.New("active route unavailable")
	}
	active := l.entries[activeIndex]
	candidate, err := prepareRouteCandidate(routeCandidateForCommittedIdentity(active, identity))
	if err != nil {
		return routeIdentity{}, err
	}
	// A session rename changes its exact attach target but not its lifecycle.
	// Keep the active route's process-local identity so the obsolete name cannot
	// survive as a separately selectable history entry.
	if active.target.LifecycleID == candidate.target.LifecycleID {
		if l.generation == ^routeGeneration(0) {
			return routeIdentity{}, errors.New("route ledger identity space exhausted")
		}
		l.generation++
		l.entries[activeIndex] = routeRecord{
			identity:     active.identity,
			origin:       candidate.origin,
			originKey:    candidate.originKey,
			target:       candidate.target,
			presentation: candidate.presentation,
			dialer:       candidate.dialer,
			request:      candidate.request,
			resumeToken:  candidate.resumeToken,
			home:         active.home,
		}
		return active.identity, nil
	}
	return l.commitLocked(candidate)
}

// updateRoutePosition remembers a cursor only for the currently active exact
// route. Delayed frames from a prior transition are valid wire messages but do
// not mutate another route.
func (l *routeLedger) updateRoutePosition(position protocol.RoutePosition) error {
	if err := position.Validate(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	activeIndex := l.indexByIdentityLocked(l.active)
	if activeIndex < 0 || l.entries[activeIndex].target != position.Target {
		return nil
	}
	entry := &l.entries[activeIndex]
	if entry.request.PreferredTabID == position.ActiveTabID {
		return nil
	}
	// The tab cursor is mutable payload on an otherwise unchanged route.
	// Rotating the route generation here would invalidate navigation actions
	// captured immediately before a paint published the new cursor.
	entry.request.PreferredTabID = position.ActiveTabID
	entry.request.RemoteTarget = nil
	return nil
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
	isHome := candidate.home || (l.home.key != 0 && l.home.key == identity.key)
	entry := routeRecord{
		identity:     identity,
		origin:       candidate.origin,
		originKey:    candidate.originKey,
		target:       candidate.target,
		presentation: candidate.presentation,
		dialer:       candidate.dialer,
		request:      candidate.request,
		resumeToken:  candidate.resumeToken,
		home:         isHome,
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
	if isHome {
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

func (l *routeLedger) lookup(ref protocol.RouteRef) (routeRecord, bool) {
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

// attentionSubscription publishes only routes served by the active route's
// daemon. Other route origins remain private to their owning daemon.
func (l *routeLedger) attentionSubscription() protocol.RouteAttentionSubscription {
	l.mu.RLock()
	defer l.mu.RUnlock()

	activeIndex := l.indexByIdentityLocked(l.active)
	if activeIndex < 0 {
		return protocol.RouteAttentionSubscription{}
	}
	active := l.entries[activeIndex]
	subscription := protocol.RouteAttentionSubscription{Targets: make([]protocol.RouteAttentionTarget, 0, len(l.entries))}
	for _, entry := range l.entries {
		if entry.identity == active.identity || entry.origin != active.origin || entry.originKey != active.originKey {
			continue
		}
		subscription.Targets = append(subscription.Targets, protocol.RouteAttentionTarget{
			Ref:    entry.identity.wire(),
			Target: entry.target,
		})
	}
	return subscription
}

func (r routeRecord) snapshotEntry() protocol.RecentRouteEntry {
	return protocol.RecentRouteEntry{
		Key:          uint64(r.identity.key),
		Generation:   uint64(r.identity.generation),
		Target:       r.target,
		Name:         r.presentation.name,
		HostLabel:    r.presentation.hostLabel,
		Kind:         r.presentation.kind,
		Ephemeral:    r.presentation.ephemeral,
		Attention:    r.presentation.attention,
		Reachability: r.presentation.reachability,
	}
}

func (l *routeLedger) snapshot() protocol.RecentRouteSnapshot {
	l.mu.RLock()
	defer l.mu.RUnlock()

	snapshot := protocol.RecentRouteSnapshot{
		Generation: uint64(l.generation),
		Active:     l.active.wire(),
		Previous:   l.previous.wire(),
		Home:       l.home.wire(),
	}
	if len(l.entries) == 0 {
		return snapshot
	}
	snapshot.Entries = make([]protocol.RecentRouteEntry, 0, len(l.entries))
	for _, entry := range l.entries {
		if entry.identity == l.active {
			snapshot.ActiveEntry = entry.snapshotEntry()
			continue
		}
		snapshot.Entries = append(snapshot.Entries, entry.snapshotEntry())
	}
	return snapshot
}

// samePeerHandoff restores a previously committed route on the daemon already
// connected to by the client. The daemon deliberately sends no tab state: the
// ledger is the sole owner of per-route view memory.
func (l *routeLedger) samePeerHandoff(active AttachRequest, target protocol.AttachTarget) AttachRequest {
	origin := normalizeRouteOrigin(active.Origin, active.Remote)
	originKey := normalizeRouteOriginKey(active.OriginKey, origin)
	environmentPolicy := target.EnvironmentPolicy
	if origin == protocol.RouteOriginLocal {
		environmentPolicy = protocol.EnvironmentPolicyClientOwned
	}

	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, entry := range l.entries {
		if entry.origin != origin || entry.originKey != originKey || entry.target.SessionName != target.Session {
			continue
		}
		if target.ExactTarget != nil && entry.target != *target.ExactTarget {
			continue
		}
		request := cloneAttachRequest(entry.request)
		request.Intent = target.Intent
		// The active dialer already reaches this daemon. Retaining the route's
		// original discovery target would make its Hello request a second,
		// incorrect remote handoff instead of a same-peer session switch.
		request.RemoteTarget = nil
		if request.EnvironmentPolicy != environmentPolicy && environmentPolicy == protocol.EnvironmentPolicyClientOwned {
			request.NavigationCapabilities = 0
			request.StartupOverlay = protocol.StartupOverlayNone
		}
		request.EnvironmentPolicy = environmentPolicy
		request.ExactTarget = target.ExactTarget
		if target.ExactTarget == nil {
			request.PreferredTabID = ""
		}
		return request
	}

	request := cloneAttachRequest(active)
	request.Intent = target.Intent
	request.SessionName = target.Session
	request.ExactTarget = target.ExactTarget
	request.PreferredTabID = ""
	request.RemoteTarget = nil
	if request.EnvironmentPolicy != environmentPolicy && environmentPolicy == protocol.EnvironmentPolicyClientOwned {
		request.NavigationCapabilities = 0
		request.StartupOverlay = protocol.StartupOverlayNone
	}
	request.EnvironmentPolicy = environmentPolicy
	return request
}

func (l *routeLedger) activeRef() protocol.RouteRef {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.active.wire()
}

func (l *routeLedger) previousRef() protocol.RouteRef {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.previous.wire()
}

func (l *routeLedger) homeRef() protocol.RouteRef {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.home.wire()
}

// routeTransitionPlan is the private lookup result used by navigation. It
// preserves the original origin and exact target while leaving display labels
// and transport capabilities separate.
type routeTransitionPlan struct {
	identity           routeIdentity
	record             routeRecord
	snapshotGeneration routeGeneration
	active             routeIdentity
	origin             protocol.RouteOrigin
	originKey          string
	target             protocol.ExactSessionTarget
}

type routeTransitionCommit struct {
	identity     protocol.CommittedRouteIdentity
	presentation routePresentation
	dialer       ports.ClientDialer
	resumeToken  uint64
}

type routeTransitionConnector interface {
	Resume(context.Context, routeRecord) (routeTransitionCommit, error)
	Attach(context.Context, routeRecord) (routeTransitionCommit, error)
	Restore(context.Context, routeRecord) error
}

type routeNavigationSelection struct {
	selected           routeRecord
	prior              routeRecord
	snapshotGeneration routeGeneration
	active             routeIdentity
	noOp               bool
}

type killedRouteSelection struct {
	selected           routeRecord
	sourceAliases      []routeIdentity
	snapshotGeneration routeGeneration
	active             routeIdentity
}

// killedSelection captures the newest route that does not identify the active
// endpoint authority and exact lifecycle. Route origin is intentionally not
// part of alias equivalence: direct and discovery routes can retain the same
// canonical endpoint authority.
func (l *routeLedger) killedSelection() (killedRouteSelection, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	selection := killedRouteSelection{snapshotGeneration: l.generation, active: l.active}
	activeIndex := l.indexByIdentityLocked(l.active)
	if activeIndex < 0 {
		return selection, false
	}
	source := l.entries[activeIndex]
	for _, entry := range l.entries {
		if entry.originKey == source.originKey && entry.target.LifecycleID == source.target.LifecycleID {
			selection.sourceAliases = append(selection.sourceAliases, entry.identity)
			continue
		}
		if selection.selected.identity.empty() {
			selection.selected = cloneRouteRecord(entry)
		}
	}
	return selection, !selection.selected.identity.empty()
}

func (l *routeLedger) retireKilled(selection killedRouteSelection) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.generation != selection.snapshotGeneration || l.active != selection.active {
		return errRouteStaleSelection
	}
	l.removeKilledAliasesLocked(selection.sourceAliases)
	l.generation++
	return nil
}

func (l *routeLedger) commitKilledTransition(selection killedRouteSelection, candidate routeCandidate) error {
	candidate, err := prepareRouteCandidate(candidate)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.generation != selection.snapshotGeneration || l.active != selection.active ||
		l.indexByIdentityLocked(selection.selected.identity) < 0 {
		return errRouteStaleSelection
	}
	l.removeKilledAliasesLocked(selection.sourceAliases)
	if _, err = l.commitLocked(candidate); err != nil {
		return err
	}
	if l.previous.empty() && len(l.entries) > 1 {
		l.previous = l.entries[1].identity
	}
	return nil
}

func (l *routeLedger) removeKilledAliasesLocked(aliases []routeIdentity) {
	for i := len(l.entries) - 1; i >= 0; i-- {
		for _, alias := range aliases {
			if l.entries[i].identity == alias {
				l.removeAtLocked(i)
				break
			}
		}
	}
}

// navigationSelection validates an action against the latest complete
// snapshot and captures the selected route, live restoration route, snapshot
// generation, and active identity under one read lock. The commit phase uses
// the captured generation and active identity to reject interleavings that
// changed the ledger while dialing.
func (l *routeLedger) navigationSelection(action protocol.RouteNavigationAction) (routeNavigationSelection, bool) {
	if action.SnapshotGeneration == 0 || action.Key == 0 || action.Generation == 0 {
		return routeNavigationSelection{}, false
	}
	identity := identityFromWire(protocol.RouteRef{Key: action.Key, Generation: action.Generation})
	l.mu.RLock()
	defer l.mu.RUnlock()
	if uint64(l.generation) != action.SnapshotGeneration {
		return routeNavigationSelection{}, false
	}
	selection := routeNavigationSelection{
		snapshotGeneration: l.generation,
		active:             l.active,
	}
	if identity == l.active {
		selection.noOp = true
		return selection, true
	}
	selectedIndex := l.indexByIdentityLocked(identity)
	activeIndex := l.indexByIdentityLocked(l.active)
	if selectedIndex < 0 || activeIndex < 0 {
		return routeNavigationSelection{}, false
	}
	selection.selected = cloneRouteRecord(l.entries[selectedIndex])
	selection.prior = cloneRouteRecord(l.entries[activeIndex])
	return selection, true
}

func (l *routeLedger) creationSelection(action protocol.RouteCreateSessionAction) (routeNavigationSelection, bool) {
	if action.Validate() != nil {
		return routeNavigationSelection{}, false
	}
	selection, ok := l.navigationSelection(protocol.RouteNavigationAction{
		SnapshotGeneration: action.SnapshotGeneration,
		Key:                action.Key,
		Generation:         action.Generation,
	})
	if !ok || selection.noOp || selection.selected.presentation.kind != protocol.RouteKindLocal {
		return routeNavigationSelection{}, false
	}
	return selection, true
}

// navigationRecord validates that an action belongs to the latest complete
// snapshot. The active route is metadata-only and selecting it is a no-op.
func (l *routeLedger) navigationRecord(action protocol.RouteNavigationAction) (routeRecord, bool, bool) {
	selection, ok := l.navigationSelection(action)
	return selection.selected, ok, selection.noOp
}

// navigate resolves a captured key/generation, attempts resume before exact
// attach, and commits only after the connector returns a matching identity.
// A failed transition restores the prior live route and leaves history alone.
func (l *routeLedger) navigate(ctx context.Context, action protocol.RouteNavigationAction, connector routeTransitionConnector) error {
	selection, ok := l.navigationSelection(action)
	if !ok {
		return errRouteStaleSelection
	}
	if selection.noOp {
		return nil
	}
	record := selection.selected
	prior := selection.prior
	if connector == nil {
		return errors.New("route transition connector is nil")
	}
	if ctx == nil {
		return errors.New("route transition context is nil")
	}
	selected := protocol.RouteRef{Key: action.Key, Generation: action.Generation}
	plan := routeTransitionPlan{
		identity:           identityFromWire(selected),
		record:             record,
		snapshotGeneration: selection.snapshotGeneration,
		active:             selection.active,
		origin:             record.origin,
		originKey:          record.originKey,
		target:             record.target,
	}
	restoreOnFailure := func(err error) error {
		if restoreErr := connector.Restore(ctx, prior); restoreErr != nil {
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
	if err := l.commitTransition(plan.identity, plan.snapshotGeneration, plan.active, candidate); err != nil {
		return restoreOnFailure(err)
	}
	return nil
}

func (l *routeLedger) commitTransition(selected routeIdentity, snapshotGeneration routeGeneration, active routeIdentity, candidate routeCandidate) error {
	candidate, err := prepareRouteCandidate(candidate)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.generation != snapshotGeneration || l.active != active {
		return errRouteStaleSelection
	}
	if l.indexByIdentityLocked(selected) < 0 {
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
