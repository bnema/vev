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

// visitedRouteSnapshot keeps only the routes this client attached to. The
// status-bar history and jump-recent ranks start from the current session and
// grow as the client visits others; the palette and attention keep the full
// snapshot.
func visitedRouteSnapshot(snapshot protocol.RecentRouteSnapshot) protocol.RecentRouteSnapshot {
	if snapshot.Entries == nil {
		return snapshot
	}
	visited := make([]protocol.RecentRouteEntry, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if entry.Visited {
			visited = append(visited, entry)
		}
	}
	snapshot.Entries = visited
	return snapshot
}

func formatRecentRouteSnapshot(snapshot protocol.RecentRouteSnapshot) []recentRouteDisplay {
	return formatRecentRoutePresentations(recentRoutePresentationsFromSnapshot(snapshot))
}
