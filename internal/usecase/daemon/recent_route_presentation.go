package daemon

// recentRouteKind identifies the origin represented by a presentation value.
// It is intentionally daemon-local in this layer; ports route kinds are
// projected into this render-only value at the daemon boundary.
type recentRouteKind uint8

const (
	recentRouteLocal recentRouteKind = iota
	recentRouteRemote
)

// recentRoutePresentation is the compact immutable value shared by snapshot
// projection and formatting. It contains only display data; it never carries
// session ownership, selection identity, transport capabilities, credentials,
// or attach targets.
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
