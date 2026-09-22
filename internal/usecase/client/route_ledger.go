package client

import (
	"slices"
	"sort"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Client-owned route ledger (Plan 003 C4/E3).
//
// The serving daemon renders the status-bar MRU, the other-session bells, the
// jump-recent hints, and the palette's remote, recent-route, and
// create-on-host rows from one bounded protocol.RecentRouteSnapshot the client
// publishes. The ledger builds that snapshot from the broker catalogue, which
// already observes every daemon (local and remote), so no daemon monitors
// another one.
//
// It is pure: no clock, no I/O, no lock. The supervisor's run goroutine owns
// it and feeds it each broker publication together with the attachment's
// committed target.
//
//   - Every session lifecycle, and every daemon as a creation host, gets a
//     RouteRef that stays stable while it remains in the catalogue. A new
//     lifecycle of the same name is a new reference.
//   - Entries are ordered by this client's attach recency, then live local
//     sessions by catalogue LastUsedSeq, then live remote sessions in
//     catalogue order, then stopped sessions.
//   - The attached lifecycle is metadata only (ActiveEntry), never an entry.
//   - Attention is any(tab.Attention). AttentionSeq orders onsets as the
//     client observed them, across daemons, so the serving daemon can jump to
//     the oldest one.
//   - Generation advances only when the published content changes.

// routeAuthority names one daemon in the broker catalogue.
type routeAuthority struct {
	local    bool
	endpoint string
}

func routeAuthorityOf(observation ports.BrokerDaemonObservation) routeAuthority {
	if observation.Local {
		return routeAuthority{local: true}
	}
	return routeAuthority{endpoint: observation.Endpoint}
}

// routeID names one session lifecycle on one daemon. A zero lifecycle names
// the daemon itself as a creation host.
type routeID struct {
	authority routeAuthority
	lifecycle domain.SessionLifecycleID
}

// routeLedgerTarget is what one published reference resolves to.
type routeLedgerTarget struct {
	authority routeAuthority
	// target is zero for a creation host.
	target protocol.ExactSessionTarget
	host   bool
}

// routeActive is the attachment the snapshot is published over.
type routeActive struct {
	known     bool
	authority routeAuthority
	target    protocol.ExactSessionTarget
}

type routeLedger struct {
	nextKey      uint64
	keys         map[routeID]uint64
	attachSeq    uint64
	attached     map[routeID]uint64
	attentionSeq uint64
	attention    map[routeID]uint64
	lastActive   routeID
	generation   uint64
	published    protocol.RecentRouteSnapshot
	targets      map[protocol.RouteRef]routeLedgerTarget
}

func newRouteLedger() *routeLedger {
	return &routeLedger{
		keys:      make(map[routeID]uint64),
		attached:  make(map[routeID]uint64),
		attention: make(map[routeID]uint64),
		targets:   make(map[protocol.RouteRef]routeLedgerTarget),
	}
}

// routeCandidate is one catalogue session before ordering.
type routeCandidate struct {
	id       routeID
	entry    protocol.RecentRouteEntry
	tier     int
	lastUsed uint64
	daemon   int
	session  int
}

const (
	routeTierLiveLocal = iota
	routeTierLiveRemote
	routeTierStopped
)

// build folds one broker publication and the attachment's committed target
// into the next snapshot. It reports false when nothing visible changed, in
// which case the previously published snapshot is returned unchanged.
func (l *routeLedger) build(snapshot ports.BrokerSnapshot, active routeActive) (protocol.RecentRouteSnapshot, bool) {
	activeID := routeID{}
	if active.known {
		activeID = routeID{authority: active.authority, lifecycle: active.target.LifecycleID}
		if activeID != l.lastActive {
			l.attachSeq++
			l.attached[activeID] = l.attachSeq
			l.lastActive = activeID
		}
	}

	present := make(map[routeID]struct{})
	candidates := make([]routeCandidate, 0)
	attentive := make(map[routeID]struct{})
	var activeObservation *ports.BrokerDaemonObservation
	for i := range snapshot.Daemons {
		daemon := snapshot.Daemons[i]
		authority := routeAuthorityOf(daemon)
		if active.known && authority == active.authority {
			activeObservation = &snapshot.Daemons[i]
		}
		for j, session := range daemon.Sessions {
			if session.State == catalogue.RemoteCatalogSessionBroken {
				continue
			}
			id := routeID{authority: authority, lifecycle: session.LifecycleID}
			if _, dup := present[id]; dup {
				continue
			}
			entry := routeEntryFor(daemon, session)
			if validateRouteEntryShape(entry) != nil {
				continue
			}
			present[id] = struct{}{}
			if entry.Attention {
				attentive[id] = struct{}{}
			}
			tier := routeTierStopped
			if session.State == catalogue.RemoteCatalogSessionUp {
				tier = routeTierLiveRemote
				if daemon.Local {
					tier = routeTierLiveLocal
				}
			}
			candidates = append(candidates, routeCandidate{id: id, entry: entry, tier: tier, lastUsed: session.LastUsedSeq, daemon: i, session: j})
		}
	}

	// Attention onsets keep their order until the attention clears.
	for id := range l.attention {
		if _, ok := attentive[id]; !ok {
			delete(l.attention, id)
		}
	}
	for _, candidate := range candidates {
		if _, ok := attentive[candidate.id]; ok && l.attention[candidate.id] == 0 {
			l.attentionSeq++
			l.attention[candidate.id] = l.attentionSeq
		}
	}

	sort.SliceStable(candidates, func(a, b int) bool {
		left, right := candidates[a], candidates[b]
		if la, ra := l.attached[left.id], l.attached[right.id]; la != ra {
			return la > ra
		}
		if left.tier != right.tier {
			return left.tier < right.tier
		}
		leftLocal, rightLocal := left.id.authority.local, right.id.authority.local
		if leftLocal != rightLocal {
			return leftLocal
		}
		if leftLocal && left.lastUsed != right.lastUsed {
			return left.lastUsed > right.lastUsed
		}
		if left.daemon != right.daemon {
			return left.daemon < right.daemon
		}
		return left.session < right.session
	})

	live := make(map[routeID]struct{}, len(present)+len(snapshot.Daemons)+1)
	next := protocol.RecentRouteSnapshot{}
	targets := make(map[protocol.RouteRef]routeLedgerTarget)
	if active.known {
		live[activeID] = struct{}{}
		entry := routeActiveEntry(candidates, activeID, active, activeObservation)
		entry.Key, entry.Generation = l.keyFor(activeID), 1
		entry.AttentionSeq = l.attention[activeID]
		if validateRouteEntryShape(entry) == nil {
			next.ActiveEntry = entry
			next.Active = protocol.RouteRef{Key: entry.Key, Generation: entry.Generation}
			targets[next.Active] = routeLedgerTarget{authority: active.authority, target: entry.Target}
			if active.authority.local {
				next.Home = next.Active
			}
		}
	}
	for _, candidate := range candidates {
		if candidate.id == activeID && active.known {
			continue
		}
		if len(next.Entries) == protocol.RouteSnapshotMaxEntries {
			break
		}
		entry := candidate.entry
		entry.Key, entry.Generation = l.keyFor(candidate.id), 1
		entry.AttentionSeq = l.attention[candidate.id]
		ref := protocol.RouteRef{Key: entry.Key, Generation: entry.Generation}
		if next.Previous.IsZero() && l.attached[candidate.id] != 0 {
			next.Previous = ref
		}
		next.Entries = append(next.Entries, entry)
		targets[ref] = routeLedgerTarget{authority: candidate.id.authority, target: entry.Target}
		live[candidate.id] = struct{}{}
	}
	for _, daemon := range snapshot.Daemons {
		authority := routeAuthorityOf(daemon)
		if active.known && authority == active.authority {
			// The serving daemon creates in place; it is never a host row.
			continue
		}
		if len(next.Hosts) == protocol.RouteSnapshotMaxHosts {
			break
		}
		if daemon.Availability == domain.RemoteAvailabilityIncompatible || pickerObservationVersionMismatch(daemon) {
			continue
		}
		kind := protocol.RouteKindRemote
		if daemon.Local {
			kind = protocol.RouteKindLocal
		}
		label := pickerOriginLabel(daemon)
		if protocol.ValidateRouteLabel(label, false) != nil {
			continue
		}
		id := routeID{authority: authority}
		host := protocol.RouteHost{Key: l.keyFor(id), Generation: 1, Label: label, Kind: kind}
		next.Hosts = append(next.Hosts, host)
		targets[protocol.RouteRef{Key: host.Key, Generation: host.Generation}] = routeLedgerTarget{authority: authority, host: true}
		live[id] = struct{}{}
	}
	if len(next.Entries) == 0 {
		next.Entries = nil
	}

	// Identities that left the catalogue lose their reference and recency.
	for id := range l.keys {
		if _, ok := live[id]; !ok {
			delete(l.keys, id)
		}
	}
	for id := range l.attached {
		if _, ok := live[id]; !ok {
			delete(l.attached, id)
		}
	}

	if l.generation != 0 && routeSnapshotContentEqual(next, l.published) {
		return l.published, false
	}
	next.Generation = l.generation + 1
	if next.Validate() != nil {
		return l.published, false
	}
	l.generation = next.Generation
	l.published = next
	l.targets = targets
	return next, true
}

// resolve returns what one reference of the latest published snapshot names.
func (l *routeLedger) resolve(ref protocol.RouteRef) (routeLedgerTarget, bool) {
	target, ok := l.targets[ref]
	return target, ok
}

func (l *routeLedger) keyFor(id routeID) uint64 {
	if key, ok := l.keys[id]; ok {
		return key
	}
	l.nextKey++
	l.keys[id] = l.nextKey
	return l.nextKey
}

// routeEntryFor projects one catalogue session. Identity fields are filled by
// the caller.
func routeEntryFor(daemon ports.BrokerDaemonObservation, session catalogue.RemoteCatalogSession) protocol.RecentRouteEntry {
	entry := protocol.RecentRouteEntry{
		Target:       protocol.ExactSessionTarget{LifecycleID: session.LifecycleID, SessionName: session.Name},
		Name:         session.Name,
		Kind:         protocol.RouteKindLocal,
		Ephemeral:    session.Ephemeral,
		Reachability: routeReachability(daemon),
	}
	if !daemon.Local {
		entry.Kind = protocol.RouteKindRemote
		entry.HostLabel = pickerOriginLabel(daemon)
	}
	for _, tab := range session.Tabs {
		if tab.Attention {
			entry.Attention = true
			break
		}
	}
	return entry
}

// routeActiveEntry is the attached lifecycle's presentation: its catalogue
// row when the broker already observed it, otherwise the committed target.
func routeActiveEntry(candidates []routeCandidate, id routeID, active routeActive, observation *ports.BrokerDaemonObservation) protocol.RecentRouteEntry {
	for _, candidate := range candidates {
		if candidate.id == id {
			return candidate.entry
		}
	}
	entry := protocol.RecentRouteEntry{
		Target: active.target,
		Name:   active.target.SessionName,
		Kind:   protocol.RouteKindLocal,
	}
	if !active.authority.local {
		entry.Kind = protocol.RouteKindRemote
		if observation != nil {
			entry.HostLabel = pickerOriginLabel(*observation)
		}
	}
	return entry
}

func routeReachability(daemon ports.BrokerDaemonObservation) protocol.RouteReachability {
	switch {
	case daemon.Availability == domain.RemoteAvailabilityReachable:
		return protocol.RouteReachabilityReachable
	case pickerObservationFailing(daemon) || daemon.Availability == domain.RemoteAvailabilityIncompatible:
		return protocol.RouteReachabilityUnavailable
	default:
		return protocol.RouteReachabilityUnknown
	}
}

// validateRouteEntryShape checks the fields a catalogue row contributes, with
// a placeholder identity.
func validateRouteEntryShape(entry protocol.RecentRouteEntry) error {
	entry.Key, entry.Generation = 1, 1
	return protocol.RecentRouteSnapshot{Generation: 1, Entries: []protocol.RecentRouteEntry{entry}}.Validate()
}

func routeSnapshotContentEqual(a, b protocol.RecentRouteSnapshot) bool {
	return a.Active == b.Active && a.ActiveEntry == b.ActiveEntry && a.Previous == b.Previous && a.Home == b.Home &&
		slices.Equal(a.Entries, b.Entries) && slices.Equal(a.Hosts, b.Hosts)
}
