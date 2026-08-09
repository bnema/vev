package daemon

import "github.com/bnema/vev/internal/ports"

func recentRoutePresentationsFromSnapshot(snapshot ports.RecentRouteSnapshot) []recentRoutePresentation {
	if snapshot.Entries == nil {
		return nil
	}
	out := make([]recentRoutePresentation, len(snapshot.Entries))
	for i, entry := range snapshot.Entries {
		kind := recentRouteLocal
		if entry.Kind == ports.RouteKindRemote {
			kind = recentRouteRemote
		}
		out[i] = recentRoutePresentation{
			name:      entry.Name,
			hostLabel: entry.HostLabel,
			kind:      kind,
			ephemeral: entry.Ephemeral,
			attention: entry.Attention,
		}
	}
	return out
}

func formatRecentRouteSnapshot(snapshot ports.RecentRouteSnapshot) []recentRouteDisplay {
	return formatRecentRoutePresentations(recentRoutePresentationsFromSnapshot(snapshot))
}
