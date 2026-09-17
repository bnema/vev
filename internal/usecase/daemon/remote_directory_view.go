package daemon

import (
	"context"
	"errors"
	"sort"

	"github.com/bnema/vev/internal/domain"
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

// currentRemoteDirectory returns a sorted defensive copy of the newest
// publication plus the monitoring state. It performs no I/O and never waits
// for the service loop. Centralizing it keeps the hybrid capture and the
// foreign picker projection reading exactly the same rows (coordinated P7
// removal set).
func (d *Daemon) currentRemoteDirectory() (hosts []ports.RemoteHostSnapshot, monitored, initialized bool) {
	directory := d.remoteDirectorySnapshot()
	hosts = append([]ports.RemoteHostSnapshot(nil), directory.Hosts...)
	sortDirectoryHosts(hosts)
	monitored = d != nil && d.remoteDirectory != nil
	return hosts, monitored, directory.Initialized
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
	d.notifyNewRemoteFailures(d.remoteDirectorySnapshot())
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

func (d *Daemon) notifyNewRemoteFailures(snapshot ports.RemoteDirectorySnapshot) {
	var notices []domain.Notification
	d.remoteFailureNoticeMu.Lock()
	if d.remoteFailureNoticed == nil {
		d.remoteFailureNoticed = make(map[string]uint64)
	}
	present := make(map[string]struct{}, len(snapshot.Hosts))
	for _, host := range snapshot.Hosts {
		present[host.Endpoint] = struct{}{}
		if host.LastFailure.Err == nil || host.ConsecutiveFailures == 0 {
			continue
		}
		if d.remoteFailureNoticed[host.Endpoint] == host.FailureEpisode {
			continue
		}
		d.remoteFailureNoticed[host.Endpoint] = host.FailureEpisode
		notices = append(notices, domain.Notification{
			Severity: domain.NoticeWarn,
			Code:     domain.NoticeRemoteObservation,
			Message:  remoteFailureNoticeMessage(host),
			Details:  host.LastFailure.Err.Error(),
		})
	}
	for endpoint := range d.remoteFailureNoticed {
		if _, ok := present[endpoint]; !ok {
			delete(d.remoteFailureNoticed, endpoint)
		}
	}
	d.remoteFailureNoticeMu.Unlock()
	for _, notice := range notices {
		d.notify(nil, notice.Severity, notice.Code, notice.Message, errors.New(notice.Details))
	}
}

func remoteFailureNoticeMessage(host ports.RemoteHostSnapshot) string {
	prefix := "Remote check failed: " + host.Endpoint + " — "
	switch host.LastFailure.Kind {
	case domain.RemoteFailureAuthentication:
		return prefix + "SSH authentication failed; verify non-interactive SSH access"
	case domain.RemoteFailureTrust:
		return prefix + "SSH host verification failed; verify the host key policy"
	case domain.RemoteFailureIncompatible:
		return prefix + "remote vev version is incompatible"
	case domain.RemoteFailureInvalidResponse:
		return prefix + "remote catalog response is invalid"
	case domain.RemoteFailureTimeout:
		return prefix + "SSH timed out"
	default:
		return prefix + "SSH connection failed; verify SSH access"
	}
}

func (d *Daemon) refreshRemoteDirectoryViewsFor(ac *attachedClient) {
	if ac == nil || ac.overlays == nil {
		return
	}
	pickerOpen := ac.overlays.pickerClientActive()
	paletteOpen := ac.overlays.paletteActive()
	if !pickerOpen && !paletteOpen {
		return
	}
	// The picker and the palette both read the directory: republish the
	// interaction snapshot with a new revision when the row set changed.
	if pickerOpen {
		d.refreshPickerSnapshot(ac)
	}
	if paletteOpen {
		d.refreshPalette(ac)
	}
	if entry := ac.currentAttachmentSession(); entry != nil {
		d.invalidateRender(entry, ac, true, "remote_directory_view.go")
	}
}
