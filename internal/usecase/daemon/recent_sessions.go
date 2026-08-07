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
	attention bool
	mruAt     uint64
}

// remoteRecentSession retains display-only recency for a remote view. It
// stores value identity rather than a view pointer so a retained warm view
// cannot gain navigation authority through the local status bar.
type remoteRecentSession struct {
	key           remoteViewKey
	displayOrigin string
	mruAt         uint64
}

func (r remoteRecentSession) name() string {
	origin := r.displayOrigin
	if origin == "" {
		origin = r.key.endpoint
	}
	if origin == "" {
		return r.key.sessionName
	}
	return r.key.sessionName + "@" + origin
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
	return trimRecentSessions(recent)
}

// recentSessionsForAttachment merges the daemon-wide local MRU with the
// attachment's display-only remote entries. Only chrome composition calls this
// method; picker, palette, and command routing keep using recentSessions.
func (d *Daemon) recentSessionsForAttachment(current *session, ac *attachedClient) []recentSession {
	recent := d.recentSessions(current)
	if ac == nil {
		return recent
	}
	return trimRecentSessions(append(recent, ac.remoteRecentSessionsExcept(remoteViewKey{})...))
}

// recentSessionsForRemoteAttachment merges daemon-local entries with this
// attachment's remote history while omitting the remote view currently rendered
// in the status bar.
func (d *Daemon) recentSessionsForRemoteAttachment(current *remoteView, ac *attachedClient) []recentSession {
	recent := d.recentSessions(nil)
	if ac == nil {
		return recent
	}
	var excluded remoteViewKey
	if current != nil {
		excluded = current.key
	}
	return trimRecentSessions(append(recent, ac.remoteRecentSessionsExcept(excluded)...))
}

func trimRecentSessions(recent []recentSession) []recentSession {
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

// recordRemoteRecent records only a successfully published remote owner. The
// caller invokes it after the attachment hand-off, outside daemon and view
// locks, so failed selections cannot affect the local status-bar history.
func (d *Daemon) recordRemoteRecent(ac *attachedClient, view *remoteView) {
	if d == nil || ac == nil || view == nil {
		return
	}
	view.mu.Lock()
	entry := remoteRecentSession{key: view.key, displayOrigin: view.displayOrigin}
	view.mu.Unlock()
	if entry.key.sessionName == "" {
		return
	}
	entry.mruAt = d.mruSeq.Add(1)
	ac.remoteRecent.With(func(entries *[]remoteRecentSession) {
		for i := range *entries {
			if (*entries)[i].key == entry.key {
				(*entries)[i] = entry
				return
			}
		}
		*entries = append(*entries, entry)
		if len(*entries) > maxMRUSessions {
			sort.SliceStable(*entries, func(i, j int) bool {
				return (*entries)[i].mruAt > (*entries)[j].mruAt
			})
			*entries = (*entries)[:maxMRUSessions]
		}
	})
}

func (ac *attachedClient) remoteRecentSessionsExcept(excluded remoteViewKey) []recentSession {
	if ac == nil {
		return nil
	}
	var recent []recentSession
	ac.remoteRecent.With(func(entries *[]remoteRecentSession) {
		recent = make([]recentSession, 0, len(*entries))
		for _, entry := range *entries {
			if entry.key == excluded {
				continue
			}
			recent = append(recent, recentSession{name: entry.name(), mruAt: entry.mruAt})
		}
	})
	return recent
}
