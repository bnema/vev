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

// localSessionInventory is the remote-free capture consumed by the prepared
// local projections: the local catalogue export, the local navigation source
// group, and the local picker/control projection. It copies active and
// inactive records and never reads the remote directory, so a local-only
// projection cannot observe or depend on remote monitoring.
//
// d.mu is released before any per-session snapshot, preserving the
// established lock ordering. Copied values are coherent per exact target,
// not a distributed atomic transaction; callers revalidate at selection.
type localSessionInventory struct {
	live    []inventoriedLiveSession
	stopped []inactiveSession
	now     time.Time
}

// sessionInventory is the daemon-owned common capture consumed by the
// palette, picker, and catalog export projections. It composes the remote-free
// local capture (localSessionInventory) with the current foreign
// remote-directory rows. Monitoring state itself is not carried here: the
// hybrid picker projection reads it directly from currentRemoteDirectory, so
// there is exactly one source for it.
type sessionInventory struct {
	localSessionInventory
	hosts []ports.RemoteHostSnapshot
}

// captureLocalSessionInventory copies registry pointers and inactive records
// under d.mu, then snapshots each live session without holding that lock. When
// refreshTitles is true the focused-title refresh runs per session after the
// registry lock is released, matching the historical picker/catalog cost;
// palette passes false to avoid rich tab-title work it never displays. It
// performs no remote-directory read.
func (d *Daemon) captureLocalSessionInventory(opts viewOptions, refreshTitles bool) localSessionInventory {
	var sessions []*session
	var stopped []inactiveSession
	if d != nil {
		d.mu.Lock()
		sessions = d.sessionsSnapshotLocked()
		stopped = make([]inactiveSession, 0, len(d.inactive))
		for _, entry := range d.inactive {
			stopped = append(stopped, entry)
		}
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
	return localSessionInventory{live: live, stopped: stopped, now: d.daemonNow()}
}

// captureSessionInventory composes the remote-free local capture with the
// current foreign remote-directory rows. The daemon-side monitor stays
// intentionally active in this state (coordinated P7 removal set), so hybrid
// projections keep their foreign rows; a nil directory yields an empty,
// uninitialized publication and never gates local behavior.
func (d *Daemon) captureSessionInventory(opts viewOptions, refreshTitles bool) sessionInventory {
	local := d.captureLocalSessionInventory(opts, refreshTitles)
	hosts, _, _ := d.currentRemoteDirectory()
	return sessionInventory{localSessionInventory: local, hosts: hosts}
}

// resumableStopped returns stopped sessions eligible for resume display,
// preserving the palette's historical canResume projection.
func (inv localSessionInventory) resumableStopped() []inactiveSession {
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
func (inv localSessionInventory) visibleStopped() []inactiveSession {
	out := make([]inactiveSession, 0, len(inv.stopped))
	for _, entry := range inv.stopped {
		if entry.visible() {
			out = append(out, entry)
		}
	}
	return out
}
