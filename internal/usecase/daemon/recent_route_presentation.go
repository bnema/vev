package daemon

// recentRouteKind identifies the origin represented by a presentation value.
// It is intentionally daemon-local in this layer; the client route protocol
// and its wire kind are introduced by the next stacked branch.
type recentRouteKind uint8

const (
	recentRouteLocal recentRouteKind = iota
	recentRouteRemote
)

// recentRoutePresentation is the compact immutable value shared by collection,
// formatting, and the future client-published snapshot. It contains only
// display data; it never carries session ownership, selection identity,
// transport capabilities, credentials, or attach targets.
type recentRoutePresentation struct {
	name      string
	hostLabel string
	kind      recentRouteKind
	ephemeral bool
	attention bool
}

// recentRouteDisplay is a formatted, render-ready copy of a presentation. The
// source value is never mutated while duplicate names are qualified.
type recentRouteDisplay struct {
	name      string
	kind      recentRouteKind
	ephemeral bool
	attention bool
}

// routePresentation returns the local daemon's display-only view of a
// selection value. Selection identity remains in recentSession for palette
// execution and is not passed into rendering.
func (entry recentSession) routePresentation() recentRoutePresentation {
	return recentRoutePresentation{
		name:      entry.name,
		kind:      recentRouteLocal,
		attention: entry.attention,
	}
}

func recentRoutePresentations(entries []recentSession) []recentRoutePresentation {
	if entries == nil {
		return nil
	}
	out := make([]recentRoutePresentation, len(entries))
	for i, entry := range entries {
		out[i] = entry.routePresentation()
	}
	return out
}

// formatRecentRoutePresentations is the single label formatter used by the
// normal MRU bar and ranked recent-session palette rendering. It returns an
// owned slice so callers can retain the result without consulting live state.
func formatRecentRoutePresentations(entries []recentRoutePresentation) []recentRouteDisplay {
	if entries == nil {
		return nil
	}
	nameCounts := make(map[string]int, len(entries))
	for _, entry := range entries {
		nameCounts[entry.name]++
	}

	out := make([]recentRouteDisplay, len(entries))
	for i, entry := range entries {
		out[i] = recentRouteDisplay{
			name:      formatRecentRouteName(entry, nameCounts[entry.name] > 1),
			kind:      entry.kind,
			ephemeral: entry.ephemeral,
			attention: entry.attention,
		}
	}
	return out
}

func formatRecentRouteName(entry recentRoutePresentation, ambiguous bool) string {
	name := entry.name
	if entry.ephemeral {
		name += "*"
	}
	if entry.kind != recentRouteRemote && !ambiguous {
		return name
	}

	host := entry.hostLabel
	if host == "" {
		if entry.kind == recentRouteRemote {
			host = "remote"
		} else {
			host = "local"
		}
	}
	return name + "@" + host
}
