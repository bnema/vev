package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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

func formatRecentRouteSnapshot(snapshot protocol.RecentRouteSnapshot) []recentRouteDisplay {
	return formatRecentRoutePresentations(recentRoutePresentationsFromSnapshot(snapshot))
}

// formatRecentRouteSnapshotForAttachment resolves attention against this
// daemon's live sessions. The client retains route ordering and presentation;
// it subscribes only the exact routes this daemon serves.
func (d *Daemon) formatRecentRouteSnapshotForAttachment(ac *attachedClient, snapshot protocol.RecentRouteSnapshot) []recentRouteDisplay {
	presentations := recentRoutePresentationsFromSnapshot(snapshot)
	if d == nil || ac == nil {
		return formatRecentRoutePresentations(presentations)
	}
	directory := d.remoteDirectorySnapshot()
	for i, entry := range snapshot.Entries {
		target, ok := ac.routeAttentionTarget(protocol.RouteRef{Key: entry.Key, Generation: entry.Generation})
		if !ok {
			continue
		}
		if target.SourceKey == "" {
			presentations[i].attention = d.routeHasAttention(target.Target)
			continue
		}
		presentations[i].attention = directoryRouteHasAttention(directory, target)
	}
	return formatRecentRoutePresentations(presentations)
}

func directoryRouteHasAttention(directory ports.RemoteDirectorySnapshot, target protocol.RouteAttentionTarget) bool {
	for _, host := range directory.Hosts {
		if protocol.RemoteInventorySourceKey(host.Endpoint) != target.SourceKey {
			continue
		}
		for _, sess := range host.Sessions {
			if sess.LifecycleID != target.Target.LifecycleID || sess.Name != target.Target.SessionName {
				continue
			}
			for _, tab := range sess.Tabs {
				if tab.Attention {
					return true
				}
			}
			return false
		}
	}
	return false
}

func (d *Daemon) routeHasAttention(target protocol.ExactSessionTarget) bool {
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
