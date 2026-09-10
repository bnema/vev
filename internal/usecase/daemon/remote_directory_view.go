package daemon

import (
	"context"
	"sort"

	"github.com/bnema/vev/internal/ports"
)

// remoteDirectorySnapshot returns the newest directory publication, or an
// empty uninitialized snapshot when monitoring is disabled. It performs no
// I/O and never waits for the service loop: empty state is valid and
// immediately renderable.
func (d *Daemon) remoteDirectorySnapshot() ports.RemoteDirectorySnapshot {
	if d == nil || d.remoteDirectory == nil {
		return ports.RemoteDirectorySnapshot{Hosts: []ports.RemoteHostSnapshot{}}
	}
	return d.remoteDirectory.Snapshot()
}

// sortDirectoryHosts orders hosts by registry rank, breaking ties by
// endpoint. Snapshot slice order is alphabetical; Rank carries registry
// (pinned-then-learned) order for presentation.
func sortDirectoryHosts(hosts []ports.RemoteHostSnapshot) {
	sort.Slice(hosts, func(i, j int) bool {
		if hosts[i].Rank != hosts[j].Rank {
			return hosts[i].Rank < hosts[j].Rank
		}
		return hosts[i].Endpoint < hosts[j].Endpoint
	})
}

// watchRemoteDirectory relays directory publications to open remote views.
// It is not an attachment: it claims no geometry, history, or keepalive,
// and losing it never affects session lifetime.
func (d *Daemon) watchRemoteDirectory(ctx context.Context) {
	directory := d.remoteDirectory
	if d == nil || directory == nil {
		return
	}
	sub := directory.Subscribe()
	defer sub.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.Changed():
			d.refreshRemoteDirectoryViews()
		}
	}
}

// refreshRemoteDirectoryViews rebuilds only open remote views from the
// newest snapshot, then invalidates through existing attachment generations
// after releasing state locks. Overlay entry builds from the latest snapshot
// immediately; overlay exit releases view state only.
func (d *Daemon) refreshRemoteDirectoryViews() {
	if d == nil {
		return
	}
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	d.mu.Unlock()
	for _, sess := range sessions {
		if sess == nil {
			continue
		}
		for _, ac := range sess.snapshotAttachments() {
			d.reconcileRouteHistory(ac)
			d.refreshRemoteDirectoryViewsFor(ac)
		}
	}
}

func (d *Daemon) refreshRemoteDirectoryViewsFor(ac *attachedClient) {
	if ac == nil || ac.overlays == nil {
		return
	}
	pickerOpen := ac.overlays.pickerActive()
	paletteOpen := ac.overlays.paletteActive()
	if !pickerOpen && !paletteOpen {
		return
	}
	if pickerOpen {
		d.refreshPickerOpts(ac, pickerRefreshOptions{preserveSelection: true, nearestRow: -1})
	}
	// Client-owned presentation has no overlay model: the row set changed,
	// so republish the interaction snapshot with a new revision instead.
	d.refreshPickerClientSnapshot(ac)
	if paletteOpen {
		d.refreshPalette(ac)
	}
	if entry := ac.currentAttachmentSession(); entry != nil {
		d.invalidateRender(entry, ac, true, "remote_directory_view.go")
	}
}
