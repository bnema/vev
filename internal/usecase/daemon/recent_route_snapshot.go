package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func activeRouteEntryForLifecycle(snapshot protocol.RecentRouteSnapshot, lifecycle domain.SessionLifecycleID) (protocol.RecentRouteEntry, bool) {
	active := snapshot.ActiveEntry
	activeRef := protocol.RouteRef{Key: active.Key, Generation: active.Generation}
	if lifecycle == (domain.SessionLifecycleID{}) || active.Target.LifecycleID != lifecycle || snapshot.Active != activeRef {
		return protocol.RecentRouteEntry{}, false
	}
	return active, true
}

func recentRoutePresentationsFromSnapshot(snapshot protocol.RecentRouteSnapshot) []recentRoutePresentation {
	if snapshot.Entries == nil {
		return nil
	}
	out := make([]recentRoutePresentation, len(snapshot.Entries))
	for i, entry := range snapshot.Entries {
		kind := recentRouteLocal
		if entry.Kind == protocol.RouteKindRemote {
			kind = recentRouteRemote
		}
		out[i] = recentRoutePresentation{
			name:      entry.Name,
			hostLabel: domain.RemoteDisplayOrigin(entry.HostLabel),
			kind:      kind,
			ephemeral: entry.Ephemeral,
			attention: entry.Attention,
		}
	}
	return out
}

// historyRouteSnapshot keeps the routes the status-bar history and
// jump-recent ranks show: every route this client attached to, plus every
// route that rings. Attention is shared by all clients, so a fresh client
// still sees a bell on a session it never visited. Visited routes lead the
// snapshot, so their ranks are unaffected by ringing ones. The palette and
// attention jumps keep the full snapshot.
func historyRouteSnapshot(snapshot protocol.RecentRouteSnapshot) protocol.RecentRouteSnapshot {
	if snapshot.Entries == nil {
		return snapshot
	}
	history := make([]protocol.RecentRouteEntry, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if entry.Visited || entry.Attention {
			history = append(history, entry)
		}
	}
	snapshot.Entries = history
	return snapshot
}

func formatRecentRouteSnapshot(snapshot protocol.RecentRouteSnapshot) []recentRouteDisplay {
	return formatRecentRoutePresentations(recentRoutePresentationsFromSnapshot(snapshot))
}
