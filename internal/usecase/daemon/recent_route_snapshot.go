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

// formatRecentRouteSnapshotForAttachment resolves attention against this
// daemon's live sessions. The client retains route ordering and presentation;
// it subscribes only the exact routes this daemon serves.
func (d *Daemon) formatRecentRouteSnapshotForAttachment(ac *attachedClient, snapshot ports.RecentRouteSnapshot) []recentRouteDisplay {
	presentations := recentRoutePresentationsFromSnapshot(snapshot)
	if d == nil || ac == nil {
		return formatRecentRoutePresentations(presentations)
	}
	for i, entry := range snapshot.Entries {
		target, ok := ac.routeAttentionTarget(ports.RouteRef{Key: entry.Key, Generation: entry.Generation})
		if ok {
			presentations[i].attention = d.routeHasAttention(target)
		}
	}
	return formatRecentRoutePresentations(presentations)
}

func (d *Daemon) routeHasAttention(target ports.ExactSessionTarget) bool {
	d.mu.Lock()
	sess := d.findByNameLocked(target.SessionName)
	d.mu.Unlock()
	if sess == nil {
		return false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.incarnation != target.LifecycleID {
		return false
	}
	for _, tb := range sess.tabs {
		if tb.attention {
			return true
		}
	}
	return false
}
