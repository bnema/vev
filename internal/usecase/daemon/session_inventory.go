package daemon

import (
	"time"

	"github.com/bnema/vev/internal/ports"
)

// inventoriedLiveSession pairs a live session pointer with its immutable
// snapshot view. The pointer preserves exact current-session identity while
// the view carries all display/policy facts, so projections never re-lock
// the registry to re-read the same session.
type inventoriedLiveSession struct {
	sess *session
	view sessionView
}

// sessionInventory is the daemon-owned common capture consumed by the
// palette, picker, and catalog export projections. It copies active/inactive
// records and remote-directory values once through this module; projections
// keep only UI-specific filtering, sorting, and labeling.
//
// d.mu is released before any per-session snapshot, preserving the
// established lock ordering. Copied values are coherent per exact target,
// not a distributed atomic transaction; callers revalidate at selection.
type sessionInventory struct {
	live        []inventoriedLiveSession
	stopped     []inactiveSession
	hosts       []ports.RemoteHostSnapshot
	monitored   bool
	initialized bool
	now         time.Time
}

// captureSessionInventory copies registry pointers and inactive records under
// d.mu, then snapshots each live session without holding that lock. When
// refreshTitles is true the focused-title refresh runs per session after the
// registry lock is released, matching the historical picker/catalog cost;
// palette passes false to avoid rich tab-title work it never displays.
func (d *Daemon) captureSessionInventory(opts viewOptions, refreshTitles bool) sessionInventory {
	var sessions []*session
	var stopped []inactiveSession
	var monitored bool
	if d != nil {
		d.mu.Lock()
		sessions = d.sessionsSnapshotLocked()
		stopped = make([]inactiveSession, 0, len(d.inactive))
		for _, entry := range d.inactive {
			stopped = append(stopped, entry)
		}
		monitored = d.remoteDirectory != nil
		d.mu.Unlock()
	}
	if refreshTitles && d != nil {
		for _, sess := range sessions {
			d.refreshSessionFocusedTitles(sess)
		}
	}
	live := make([]inventoriedLiveSession, 0, len(sessions))
	for _, sess := range sessions {
		if sess == nil {
			continue
		}
		live = append(live, inventoriedLiveSession{sess: sess, view: sess.snapshotView(opts)})
	}
	directory := d.remoteDirectorySnapshot()
	hosts := append([]ports.RemoteHostSnapshot(nil), directory.Hosts...)
	sortDirectoryHosts(hosts)
	return sessionInventory{
		live:        live,
		stopped:     stopped,
		hosts:       hosts,
		monitored:   monitored,
		initialized: directory.Initialized,
		now:         d.daemonNow(),
	}
}

// resumableStopped returns stopped sessions eligible for resume display,
// preserving the palette's historical canResume projection.
func (inv sessionInventory) resumableStopped() []inactiveSession {
	out := make([]inactiveSession, 0, len(inv.stopped))
	for _, entry := range inv.stopped {
		if entry.canResume() {
			out = append(out, entry)
		}
	}
	return out
}

// visibleStopped returns non-purging stopped sessions, preserving the
// picker and catalog export projections.
func (inv sessionInventory) visibleStopped() []inactiveSession {
	out := make([]inactiveSession, 0, len(inv.stopped))
	for _, entry := range inv.stopped {
		if entry.visible() {
			out = append(out, entry)
		}
	}
	return out
}
