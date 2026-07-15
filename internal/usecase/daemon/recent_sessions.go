package daemon

import (
	"sort"

	"github.com/bnema/vev/internal/domain"
)

// recentSession is an immutable, value-only description of a session for one
// interaction. It deliberately does not retain a live session pointer.
type recentSession struct {
	id        domain.SessionID
	name      string
	ephemeral bool
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
		if sess == current {
			continue
		}
		sess.mu.Lock()
		if sess.ephemeral {
			sess.mu.Unlock()
			continue
		}
		entry := recentSession{
			id:        sess.id,
			name:      sess.name,
			ephemeral: sess.ephemeral,
			mruAt:     sess.mruAt.Load(),
		}
		for _, tb := range sess.tabs {
			if tb.attention {
				entry.attention = true
				break
			}
		}
		sess.mu.Unlock()
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
