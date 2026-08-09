package daemon

import (
	"sort"

	"github.com/bnema/vev/internal/domain"
)

// recentSession is the immutable selection snapshot for one interaction. Its
// identity and MRU sequence are retained only for daemon-side execution and
// ordering; render paths project it into recentRoutePresentation, which has no
// selection identity or lifecycle authority.
type recentSession struct {
	id        domain.SessionID
	name      string
	attention bool
	mruAt     uint64
}

// recentSessions returns the current session's capped named-session MRU list.
// Callers may retain the returned values for the lifetime of an interaction.
func (d *Daemon) recentSessions(current *session) []recentSession {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	recent := make([]recentSession, 0, len(d.sessions))
	for _, sess := range d.sessions {
		if sess == nil || sess == current {
			continue
		}
		snap := sess.snapshotView(viewOptions{})
		if snap.ephemeral {
			continue
		}
		entry := recentSession{id: snap.id, name: snap.name, mruAt: snap.mruAt, attention: snap.hasAttention}
		recent = append(recent, entry)
	}
	d.mu.Unlock()
	sort.SliceStable(recent, func(i, j int) bool {
		if recent[i].mruAt == recent[j].mruAt {
			return recent[i].name < recent[j].name
		}
		return recent[i].mruAt > recent[j].mruAt
	})
	if len(recent) > maxMRUSessions {
		recent = recent[:maxMRUSessions]
	}
	return recent
}
