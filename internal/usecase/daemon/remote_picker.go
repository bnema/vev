package daemon

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/picker"
)

type remotePickerCatalogEntry struct {
	entry  ports.RemoteCatalogCacheEntry
	status remoteHostStatus
}

// remotePickerInstance is an immutable ownership capability for one published
// picker lifecycle. The model identity prevents delayed registration from
// attaching ownership to a replacement within the same client.
type remotePickerInstance struct {
	ac         *attachedClient
	generation uint64
	model      *picker.Model
}

// remotePickerCatalogSnapshot copies the cache-derived state needed for a
// picker before any row sorting begins. No remote-state lock survives this
// snapshot.
func (d *Daemon) remotePickerCatalogSnapshot() []remotePickerCatalogEntry {
	if d == nil {
		return nil
	}
	d.remoteCatalog.mu.Lock()
	entries := make([]remotePickerCatalogEntry, 0, len(d.remoteCatalog.cache)+len(d.remoteCatalog.status))
	seen := make(map[string]struct{}, len(d.remoteCatalog.cache))
	for host, entry := range d.remoteCatalog.cache {
		entries = append(entries, remotePickerCatalogEntry{
			entry:  cloneRemoteCatalogEntry(entry),
			status: d.remoteCatalog.status[host],
		})
		seen[host] = struct{}{}
	}
	for host, status := range d.remoteCatalog.status {
		if _, exists := seen[host]; exists || (status != remoteHostUnreachable && status != remoteHostVersionMismatch) {
			continue
		}
		entries = append(entries, remotePickerCatalogEntry{
			entry:  ports.RemoteCatalogCacheEntry{Host: host},
			status: status,
		})
	}
	d.remoteCatalog.mu.Unlock()
	return entries
}

func (d *Daemon) remotePickerHostRanks() map[string]int {
	ranks := make(map[string]int)
	if d == nil || d.remoteHostStore == nil {
		return ranks
	}
	pinned, learned, err := d.remoteHostStore.Hosts()
	if err != nil {
		d.log.Warn("listing remote hosts for picker failed", "err", err)
		return ranks
	}
	for _, host := range append(pinned, learned...) {
		if _, exists := ranks[host]; !exists {
			ranks[host] = len(ranks)
		}
	}
	return ranks
}

func remotePickerAvailability(status remoteHostStatus) picker.RemoteAvailability {
	switch status {
	case remoteHostFresh:
		return picker.RemoteFresh
	case remoteHostUnreachable:
		return picker.RemoteStale
	case remoteHostVersionMismatch:
		return picker.RemoteVersionMismatch
	default:
		return picker.RemoteCached
	}
}

func remotePickerDetail(tabs uint16) string {
	if tabs == 1 {
		return "1 tab"
	}
	return fmt.Sprintf("%d tabs", tabs)
}

func remotePickerStatusDetail(status remoteHostStatus, fetchedAt time.Time) string {
	switch status {
	case remoteHostUnreachable:
		if !fetchedAt.IsZero() {
			return "stale since " + fetchedAt.Format(time.RFC3339)
		}
		return "unreachable"
	case remoteHostVersionMismatch:
		return "version mismatch"
	default:
		return ""
	}
}

func remotePickerView(key domain.RemoteSessionKey, session ports.RemoteCatalogSession, status remoteHostStatus, fetchedAt time.Time) picker.SessionView {
	detail := remotePickerStatusDetail(status, fetchedAt)
	if detail == "" {
		detail = remotePickerDetail(session.Tabs)
	}
	return picker.SessionView{
		ID:                 key.ID(),
		Name:               key.Display(),
		RemoteKey:          &key,
		RemoteHost:         key.Host,
		RemoteAvailability: remotePickerAvailability(status),
		RemoteDetail:       detail,
		ConnectOnly:        true,
		RemoteAttachReady:  status != remoteHostVersionMismatch,
		CannotAcceptMoves:  true,
	}
}

func remotePickerHostView(host string, status remoteHostStatus) picker.SessionView {
	return picker.SessionView{
		ID:                 domain.SessionID("remote-host:" + base64.RawURLEncoding.EncodeToString([]byte(host))),
		Name:               host,
		RemoteHost:         host,
		RemoteAvailability: remotePickerAvailability(status),
		RemoteDetail:       remotePickerStatusDetail(status, time.Time{}),
		ConnectOnly:        true,
		CannotAcceptMoves:  true,
	}
}

func remoteProxyPickerView(key domain.RemoteSessionKey, snap sessionView) picker.SessionView {
	view := snap.pickerView()
	// Proxy construction uses this same ID and label. Restating them from the
	// structured key makes the cache-to-live row upgrade independent of mutable
	// presentation or relayed metadata. Lifecycle is carried separately from
	// presentation so a display-name change can never spoof expiry.
	view.ID = key.ID()
	view.Name = key.Display()
	if snap.expired {
		view.Name += " [expired]"
	}
	view.TargetName = ""
	view.RemoteKey = &key
	view.RemoteAvailability = picker.RemoteFresh
	view.RemoteDetail = remotePickerDetail(uint16(snap.tabCount))
	view.ConnectOnly = true
	view.RemoteAttachReady = true
	if snap.expired {
		view.RemoteAvailability = picker.RemoteStale
		view.RemoteDetail = "expired"
		view.RemoteAttachReady = false
	}
	view.CannotAcceptMoves = true
	return view
}

func sortRemotePickerCatalog(entries []remotePickerCatalogEntry, ranks map[string]int) {
	for i := range entries {
		sort.Slice(entries[i].entry.Sessions, func(left, right int) bool {
			return entries[i].entry.Sessions[left].Name < entries[i].entry.Sessions[right].Name
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		left, right := entries[i].entry.Host, entries[j].entry.Host
		leftRank, leftKnown := ranks[left]
		rightRank, rightKnown := ranks[right]
		if leftKnown != rightKnown {
			return leftKnown
		}
		if leftKnown && leftRank != rightRank {
			return leftRank < rightRank
		}
		return left < right
	})
}

func (d *Daemon) remotePickerOpened(instance remotePickerInstance) {
	if d.registerRemotePicker(instance) {
		d.startRemotePickerRefresh(instance)
	}
}

// registerRemotePicker installs one exact picker generation as a refresh owner.
// Keeping this step separate gives refresh startup a second atomic validation
// point: a close can win between registration and startup without being undone
// by a delayed opener.
func (d *Daemon) registerRemotePicker(instance remotePickerInstance) bool {
	if d == nil || instance.ac == nil || instance.ac.overlays == nil || instance.model == nil {
		return false
	}
	rt := instance.ac.overlays
	// Picker ownership always nests pickerMu -> remoteCatalog.mu. No path may
	// acquire pickerMu while retaining the catalog lock.
	rt.pickerMu.Lock()
	defer rt.pickerMu.Unlock()
	if rt.pickerGeneration != instance.generation || rt.picker != instance.model {
		return false
	}
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.pickers[instance.ac] = struct{}{}
	d.remoteCatalog.mu.Unlock()
	return true
}

func (d *Daemon) remotePickerClosed(instance remotePickerInstance) {
	if d == nil || instance.ac == nil || instance.ac.overlays == nil || instance.model == nil {
		return
	}
	rt := instance.ac.overlays
	rt.pickerMu.Lock()
	current := rt.pickerGeneration == instance.generation && rt.picker == nil
	if !current {
		rt.pickerMu.Unlock()
		return
	}
	d.remoteCatalog.mu.Lock()
	delete(d.remoteCatalog.pickers, instance.ac)
	if len(d.remoteCatalog.pickers) != 0 {
		d.remoteCatalog.mu.Unlock()
		rt.pickerMu.Unlock()
		return
	}
	d.remoteCatalog.refresh++
	cancel := d.remoteCatalog.cancel
	d.remoteCatalog.cancel = nil
	d.remoteCatalog.mu.Unlock()
	rt.pickerMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (d *Daemon) cancelRemotePickerRefresh() {
	if d == nil {
		return
	}
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh++
	cancel := d.remoteCatalog.cancel
	d.remoteCatalog.cancel = nil
	d.remoteCatalog.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (d *Daemon) remotePickerHosts() ([]string, error) {
	if d == nil || d.remoteHostStore == nil {
		return nil, nil
	}
	pinned, learned, err := d.remoteHostStore.Hosts()
	if err != nil {
		return nil, err
	}
	hosts := make([]string, 0, len(pinned)+len(learned))
	seen := make(map[string]struct{}, cap(hosts))
	add := func(candidates []string) {
		for _, host := range candidates {
			if _, exists := seen[host]; exists {
				continue
			}
			seen[host] = struct{}{}
			hosts = append(hosts, host)
		}
	}
	add(pinned)
	add(learned)
	return hosts, nil
}

// startRemotePickerRefresh replaces the current refresh generation and launches
// one bounded catalog call per known host. Installing the generation is gated
// by the exact picker generation that registered ownership. Host-store and
// remote I/O happen after every architecture lock has been released.
func (d *Daemon) startRemotePickerRefresh(instance remotePickerInstance) uint64 {
	if d == nil || instance.ac == nil || instance.ac.overlays == nil || instance.model == nil {
		return 0
	}
	root := context.Background()
	if d.serveCtx != nil {
		root = d.serveCtx
	}
	ctx, cancel := context.WithCancel(root)

	rt := instance.ac.overlays
	rt.pickerMu.Lock()
	d.remoteCatalog.mu.Lock()
	_, owner := d.remoteCatalog.pickers[instance.ac]
	current := rt.pickerGeneration == instance.generation && rt.picker == instance.model
	if !current || !owner || len(d.remoteCatalog.pickers) == 0 {
		d.remoteCatalog.mu.Unlock()
		rt.pickerMu.Unlock()
		cancel()
		return 0
	}
	previous := d.remoteCatalog.cancel
	d.remoteCatalog.refresh++
	generation := d.remoteCatalog.refresh
	d.remoteCatalog.cancel = cancel
	d.remoteCatalog.mu.Unlock()
	rt.pickerMu.Unlock()
	if previous != nil {
		previous()
	}
	if d.remoteCatalogClient == nil {
		return generation
	}

	hosts, err := d.remotePickerHosts()
	if err != nil {
		d.log.Warn("listing remote hosts for refresh failed", "err", err)
		return generation
	}
	known := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		known[host] = struct{}{}
	}

	d.remoteCatalog.mu.Lock()
	if d.remoteCatalog.refresh != generation {
		d.remoteCatalog.mu.Unlock()
		cancel()
		return generation
	}
	cacheChanged := false
	for host := range d.remoteCatalog.cache {
		if _, exists := known[host]; exists {
			continue
		}
		delete(d.remoteCatalog.cache, host)
		delete(d.remoteCatalog.status, host)
		delete(d.remoteCatalog.failure, host)
		cacheChanged = true
	}
	for _, host := range hosts {
		d.remoteCatalog.status[host] = remoteHostRefreshing
	}
	d.remoteCatalog.mu.Unlock()
	if cacheChanged {
		d.persistRemoteCatalogCache()
		d.refreshRemoteOpenPickers()
	}

	for _, host := range hosts {
		d.sessWg.Go(func() {
			catalog, listErr := d.remoteCatalogClient.List(ctx, host)
			d.applyRemoteRefreshResult(generation, host, catalog, listErr)
		})
	}
	return generation
}

func containsRemotePickerHost(hosts []string, target string) bool {
	for _, host := range hosts {
		if host == target {
			return true
		}
	}
	return false
}

// applyRemoteRefreshResult publishes one host completion only if both its
// refresh generation and current host-registry membership remain authoritative.
// Its return reports whether durable cache state changed.
func (d *Daemon) applyRemoteRefreshResult(generation uint64, host string, catalog ports.RemoteCatalog, listErr error) bool {
	if d == nil {
		return false
	}
	d.remoteCatalog.mu.Lock()
	current := d.remoteCatalog.refresh == generation
	d.remoteCatalog.mu.Unlock()
	if !current {
		return false
	}

	hosts, hostsErr := d.remotePickerHosts()
	if hostsErr != nil {
		d.log.Warn("revalidating remote host refresh failed", "host", host, "err", hostsErr)
		return false
	}
	stillKnown := containsRemotePickerHost(hosts, host)
	now := d.clock.Now()
	if listErr == nil && catalog.ProtocolVersion != ports.ProtocolVersion {
		listErr = &ports.RemoteCatalogVersionMismatchError{Got: catalog.ProtocolVersion, Want: ports.ProtocolVersion}
	}

	d.remoteCatalog.mu.Lock()
	if d.remoteCatalog.refresh != generation {
		d.remoteCatalog.mu.Unlock()
		return false
	}
	if !stillKnown {
		_, cacheChanged := d.remoteCatalog.cache[host]
		delete(d.remoteCatalog.cache, host)
		delete(d.remoteCatalog.status, host)
		delete(d.remoteCatalog.failure, host)
		d.remoteCatalog.mu.Unlock()
		if cacheChanged {
			d.persistRemoteCatalogCache()
			d.refreshRemoteOpenPickers()
		}
		return cacheChanged
	}
	if listErr != nil {
		status := remoteHostUnreachable
		var mismatch *ports.RemoteCatalogVersionMismatchError
		if errors.As(listErr, &mismatch) {
			status = remoteHostVersionMismatch
		}
		d.remoteCatalog.status[host] = status
		d.remoteCatalog.failure[host] = now
		d.remoteCatalog.mu.Unlock()
		d.refreshRemoteOpenPickers()
		return false
	}
	d.remoteCatalog.cache[host] = ports.RemoteCatalogCacheEntry{
		Host:      host,
		FetchedAt: now,
		Sessions:  append([]ports.RemoteCatalogSession{}, catalog.Sessions...),
	}
	d.remoteCatalog.status[host] = remoteHostFresh
	delete(d.remoteCatalog.failure, host)
	d.remoteCatalog.mu.Unlock()

	d.persistRemoteCatalogCache()
	d.refreshRemoteOpenPickers()
	return true
}

func (d *Daemon) refreshRemoteOpenPickers() {
	if d == nil {
		return
	}
	d.remoteCatalog.mu.Lock()
	clients := make([]*attachedClient, 0, len(d.remoteCatalog.pickers))
	for ac := range d.remoteCatalog.pickers {
		clients = append(clients, ac)
	}
	d.remoteCatalog.mu.Unlock()

	for _, ac := range clients {
		if ac == nil || ac.overlays == nil {
			continue
		}
		d.refreshPickerOpts(ac, pickerRefreshOptions{preserveSelection: true, nearestRow: -1})
		if entry := ac.currentAttachmentSession(); entry != nil {
			d.invalidateRender(entry, ac, true, "remote_picker.go")
		}
	}
}
