package daemon

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
)

type remoteCatalogPresentationEntry struct {
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

func (i remotePickerInstance) discoveryInstance() remoteDiscoveryInstance {
	return remoteDiscoveryInstance{
		ac: i.ac, kind: remoteDiscoveryPicker, generation: i.generation, picker: i.model,
	}
}

type remoteDiscoveryInstance struct {
	ac         *attachedClient
	kind       remoteDiscoveryConsumerKind
	generation uint64
	picker     *picker.Model
	palette    *palette.Model
}

// remoteCatalogSnapshot copies the cache-derived discovery state before any
// overlay sorting begins. No remote-state lock survives this snapshot.
const remoteCatalogAttachTTL = 30 * time.Second

func (d *Daemon) remoteCatalogSnapshot() []remoteCatalogPresentationEntry {
	if d == nil {
		return nil
	}
	now := d.clock.Now()
	d.remoteCatalog.mu.Lock()
	for host, entry := range d.remoteCatalog.cache {
		if d.remoteCatalog.status[host] == remoteHostFresh && remoteCatalogExpired(entry.FetchedAt, now) {
			d.remoteCatalog.status[host] = remoteHostStale
		}
	}
	entries := make([]remoteCatalogPresentationEntry, 0, len(d.remoteCatalog.cache)+len(d.remoteCatalog.status))
	seen := make(map[string]struct{}, len(d.remoteCatalog.cache))
	for host, entry := range d.remoteCatalog.cache {
		entries = append(entries, remoteCatalogPresentationEntry{
			entry:  cloneRemoteCatalogEntry(entry),
			status: d.remoteCatalog.status[host],
		})
		seen[host] = struct{}{}
	}
	for host, status := range d.remoteCatalog.status {
		if _, exists := seen[host]; exists || (status != remoteHostUnreachable && status != remoteHostVersionMismatch && status != remoteHostMalformed) {
			continue
		}
		entries = append(entries, remoteCatalogPresentationEntry{
			entry:  ports.RemoteCatalogCacheEntry{Host: host},
			status: status,
		})
	}
	d.remoteCatalog.mu.Unlock()
	return entries
}

func (d *Daemon) remoteHostRanks() map[string]int {
	ranks := make(map[string]int)
	if d == nil || d.remoteHostStore == nil {
		return ranks
	}
	pinned, learned, err := d.remoteHostStore.Hosts()
	if err != nil {
		d.log.Warn("listing remote hosts for discovery failed", "err", err)
		return ranks
	}
	for _, host := range append(pinned, learned...) {
		if _, exists := ranks[host]; !exists {
			ranks[host] = len(ranks)
		}
	}
	return ranks
}

func remoteCatalogExpired(fetchedAt, now time.Time) bool {
	return fetchedAt.IsZero() || (!now.Before(fetchedAt) && now.Sub(fetchedAt) >= remoteCatalogAttachTTL)
}

func remotePickerAvailability(status remoteHostStatus) picker.RemoteAvailability {
	switch status {
	case remoteHostFresh:
		return picker.RemoteFresh
	case remoteHostStale, remoteHostUnreachable, remoteHostMalformed:
		return picker.RemoteStale
	case remoteHostVersionMismatch:
		return picker.RemoteVersionMismatch
	default:
		return picker.RemoteCached
	}
}

func remotePickerStatusDetail(status remoteHostStatus, fetchedAt time.Time) string {
	switch status {
	case remoteHostCached, remoteHostRefreshing:
		return "checking remote…"
	case remoteHostStale:
		if !fetchedAt.IsZero() {
			return "catalog stale since " + fetchedAt.Format(time.RFC3339)
		}
		return "catalog stale"
	case remoteHostUnreachable:
		if !fetchedAt.IsZero() {
			return "stale since " + fetchedAt.Format(time.RFC3339)
		}
		return "unreachable"
	case remoteHostVersionMismatch:
		return "version mismatch"
	case remoteHostMalformed:
		return "catalog malformed"
	default:
		return ""
	}
}

func remoteSessionStateStopped(state ports.RemoteCatalogSessionState) bool {
	return state == ports.RemoteCatalogSessionDown
}

func remoteCatalogSessionTarget(key domain.RemoteSessionKey, session ports.RemoteCatalogSession) (domain.RemoteSessionKey, domain.RemoteSessionTarget) {
	key.LifecycleID = session.LifecycleID
	key.DisplayOrigin = domain.RemoteDisplayOrigin(key.Host)
	target := domain.RemoteSessionTarget{
		Endpoint:      key.Host,
		DisplayOrigin: key.DisplayOrigin,
		LifecycleID:   session.LifecycleID,
		SessionName:   session.Name,
		Stopped:       remoteSessionStateStopped(session.State),
	}
	tabs := ports.CatalogTabs(session)
	if len(tabs) == 0 {
		return key, target
	}
	if target.Stopped {
		first := tabs[0]
		if first.ID != "" {
			target.StoppedTab = domain.NewStableTabSelector(domain.TabStableID(first.ID))
		} else {
			tabCount := min(len(tabs), math.MaxUint16)
			target.StoppedTab = domain.NewOrdinalTabSelector(0, first.Name, uint16(tabCount))
		}
		return key, target
	}
	active := 0
	for i, tab := range tabs {
		if session.ActiveTabID != "" && tab.ID == session.ActiveTabID {
			active = i
			break
		}
	}
	target.LiveTabID = domain.TabStableID(tabs[active].ID)
	return key, target
}

func remotePickerView(key domain.RemoteSessionKey, session ports.RemoteCatalogSession, status remoteHostStatus, fetchedAt time.Time) picker.SessionView {
	key, target := remoteCatalogSessionTarget(key, session)
	availability := remotePickerAvailability(status)
	stopped := remoteSessionStateStopped(session.State)
	broken := session.State == ports.RemoteCatalogSessionBroken
	tabs := ports.CatalogTabs(session)
	viewTabs := make([]picker.TabEntry, 0, len(tabs))
	active := 0
	for i, tab := range tabs {
		if session.ActiveTabID != "" && tab.ID == session.ActiveTabID {
			active = i
		}
		name := tab.Name
		if name == "" {
			name = fmt.Sprintf("%d", int(tab.Index)+1)
		}
		viewTabs = append(viewTabs, picker.TabEntry{
			TabID:     domain.TabStableID(tab.ID),
			Name:      name,
			RawName:   tab.Name,
			Detail:    tab.Detail,
			Attention: tab.Attention,
		})
	}
	// Keep the structured session identity on rows even when one tab ID is
	// malformed or a broken session cannot be activated. Cursor navigation
	// must remain possible, while sendRemoteAttachTargetForAttachment will
	// fail closed on the invalid target.
	remoteTarget := &target
	if stopped && len(viewTabs) == 0 {
		// A healthy durable record may intentionally have no retained tabs,
		// for example after a protocol reset. Mirror local stopped rows with
		// one default picker target; the owning daemon creates that sole tab.
		viewTabs = append(viewTabs, picker.TabEntry{})
	}
	reason := remoteReasonForStatus(status)
	targetValid := remoteTarget.Validate() == nil
	activation := picker.RemoteUnavailable
	if broken {
		reason = "session_broken"
	} else if !targetValid {
		reason = "identity_changed"
	} else if status == remoteHostFresh {
		if stopped {
			activation = picker.RemoteRestart
		} else {
			activation = picker.RemoteAttach
		}
	}

	detail := remotePickerStatusDetail(status, fetchedAt)
	if detail == "" {
		switch {
		case broken:
			detail = "session broken"
		case !targetValid:
			detail = "identity changed"
		case activation == picker.RemoteRestart:
			detail = "stopped — Enter to restart"
		case session.State == ports.RemoteCatalogSessionUp:
			detail = "up"
		case stopped:
			detail = "stopped"
		default:
			detail = string(session.State)
		}
	}
	return picker.SessionView{
		ID:                 key.ID(),
		Name:               key.Display(),
		RemoteKey:          &key,
		RemoteTarget:       remoteTarget,
		RemoteHost:         key.Host,
		RemoteAvailability: availability,
		RemoteDetail:       detail,
		RemoteReason:       reason,
		RemoteActivation:   activation,
		Tabs:               viewTabs,
		Active:             active,
		Stopped:            stopped,
		CannotAcceptMoves:  true,
	}
}

func remoteReasonForStatus(status remoteHostStatus) string {
	switch status {
	case remoteHostRefreshing:
		return "refreshing"
	case remoteHostStale:
		return "catalog_stale"
	case remoteHostUnreachable:
		return "host_unreachable"
	case remoteHostVersionMismatch:
		return "version_mismatch"
	case remoteHostMalformed:
		return "malformed"
	case remoteHostCached:
		return "refreshing"
	default:
		return ""
	}
}

func remotePickerHostView(host string, status remoteHostStatus) picker.SessionView {
	return picker.SessionView{
		ID:                 domain.SessionID("remote-host:" + base64.RawURLEncoding.EncodeToString([]byte(host))),
		Name:               host,
		RemoteHost:         host,
		RemoteReason:       remoteReasonForStatus(status),
		RemoteAvailability: remotePickerAvailability(status),
		RemoteDetail:       remotePickerStatusDetail(status, time.Time{}),
		RemoteActivation:   picker.RemoteUnavailable,
		CannotAcceptMoves:  true,
	}
}

func sortRemoteCatalog(entries []remoteCatalogPresentationEntry, ranks map[string]int) {
	for i := range entries {
		sort.Slice(entries[i].entry.Sessions, func(left, right int) bool {
			l, r := entries[i].entry.Sessions[left], entries[i].entry.Sessions[right]
			if l.LastUsedSeq != r.LastUsedSeq {
				return l.LastUsedSeq > r.LastUsedSeq
			}
			return l.Name < r.Name
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

func (d *Daemon) remoteDiscoveryOpened(instance remoteDiscoveryInstance) {
	if d.registerRemoteDiscoveryConsumer(instance) {
		d.startRemoteDiscoveryRefresh(instance)
	}
}

// registerRemoteDiscoveryConsumer installs one exact overlay generation as a
// refresh owner. Overlay ownership always nests its model lock before the
// catalog lock; no path may acquire an overlay lock while retaining catalog.mu.
func (d *Daemon) registerRemoteDiscoveryConsumer(instance remoteDiscoveryInstance) bool {
	if d == nil || instance.ac == nil || instance.ac.overlays == nil {
		return false
	}
	rt := instance.ac.overlays
	switch instance.kind {
	case remoteDiscoveryPicker:
		if instance.picker == nil {
			return false
		}
		rt.pickerMu.Lock()
		defer rt.pickerMu.Unlock()
		if rt.pickerGeneration != instance.generation || rt.picker != instance.picker {
			return false
		}
	case remoteDiscoveryPalette:
		if instance.palette == nil {
			return false
		}
		rt.paletteMu.Lock()
		defer rt.paletteMu.Unlock()
		if rt.paletteGeneration != instance.generation || rt.palette != instance.palette {
			return false
		}
	default:
		return false
	}
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.consumers[instance.ac] |= instance.kind
	d.remoteCatalog.mu.Unlock()
	return true
}

func (d *Daemon) remoteDiscoveryClosed(instance remoteDiscoveryInstance) {
	if d == nil || instance.ac == nil || instance.ac.overlays == nil {
		return
	}
	rt := instance.ac.overlays
	var current bool
	var unlock func()
	switch instance.kind {
	case remoteDiscoveryPicker:
		if instance.picker == nil {
			return
		}
		rt.pickerMu.Lock()
		unlock = rt.pickerMu.Unlock
		current = rt.pickerGeneration == instance.generation && rt.picker == nil
	case remoteDiscoveryPalette:
		if instance.palette == nil {
			return
		}
		rt.paletteMu.Lock()
		unlock = rt.paletteMu.Unlock
		current = rt.paletteGeneration == instance.generation && rt.palette == nil
	default:
		return
	}
	if !current {
		unlock()
		return
	}
	d.remoteCatalog.mu.Lock()
	owners := d.remoteCatalog.consumers[instance.ac]
	if owners&instance.kind == 0 {
		d.remoteCatalog.mu.Unlock()
		unlock()
		return
	}
	owners &^= instance.kind
	if owners == 0 {
		delete(d.remoteCatalog.consumers, instance.ac)
	} else {
		d.remoteCatalog.consumers[instance.ac] = owners
	}
	if len(d.remoteCatalog.consumers) != 0 {
		d.remoteCatalog.mu.Unlock()
		unlock()
		return
	}
	d.remoteCatalog.refresh++
	cancel := d.remoteCatalog.cancel
	d.remoteCatalog.cancel = nil
	d.remoteCatalog.mu.Unlock()
	unlock()
	if cancel != nil {
		cancel()
	}
}

func (d *Daemon) cancelRemoteDiscoveryRefresh() {
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

func (d *Daemon) remoteDiscoveryHosts() ([]string, error) {
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

// startRemoteDiscoveryRefresh replaces the current refresh generation and
// launches one bounded catalog call per known host. Installing the generation
// is gated by the exact overlay generation that registered ownership. Host
// store and remote I/O happen after every architecture lock is released.
func (d *Daemon) startRemoteDiscoveryRefresh(instance remoteDiscoveryInstance) uint64 {
	if d == nil || instance.ac == nil || instance.ac.overlays == nil {
		return 0
	}
	root := context.Background()
	if d.serveCtx != nil {
		root = d.serveCtx
	}
	ctx, cancel := context.WithCancel(root)

	rt := instance.ac.overlays
	install := func(current bool) (context.CancelFunc, uint64, bool) {
		d.remoteCatalog.mu.Lock()
		defer d.remoteCatalog.mu.Unlock()
		owner := d.remoteCatalog.consumers[instance.ac]&instance.kind != 0
		if !current || !owner || len(d.remoteCatalog.consumers) == 0 {
			return nil, 0, false
		}
		previous := d.remoteCatalog.cancel
		d.remoteCatalog.refresh++
		generation := d.remoteCatalog.refresh
		d.remoteCatalog.cancel = cancel
		return previous, generation, true
	}
	var previous context.CancelFunc
	var generation uint64
	var installed bool
	switch instance.kind {
	case remoteDiscoveryPicker:
		if instance.picker == nil {
			cancel()
			return 0
		}
		rt.pickerMu.Lock()
		previous, generation, installed = install(rt.pickerGeneration == instance.generation && rt.picker == instance.picker)
		rt.pickerMu.Unlock()
	case remoteDiscoveryPalette:
		if instance.palette == nil {
			cancel()
			return 0
		}
		rt.paletteMu.Lock()
		previous, generation, installed = install(rt.paletteGeneration == instance.generation && rt.palette == instance.palette)
		rt.paletteMu.Unlock()
	default:
		cancel()
		return 0
	}
	if !installed {
		cancel()
		return 0
	}
	if previous != nil {
		previous()
	}
	if d.remoteCatalogClient == nil {
		return generation
	}

	hosts, err := d.remoteDiscoveryHosts()
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
	}
	d.refreshRemoteDiscoveryConsumers()

	for _, host := range hosts {
		d.sessWg.Go(func() {
			catalog, listErr := d.remoteCatalogClient.List(ctx, host)
			d.applyRemoteRefreshResult(generation, host, catalog, listErr)
		})
	}
	return generation
}

func containsRemoteDiscoveryHost(hosts []string, target string) bool {
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

	hosts, hostsErr := d.remoteDiscoveryHosts()
	if hostsErr != nil {
		d.log.Warn("revalidating remote host refresh failed", "host", host, "err", hostsErr)
		return false
	}
	stillKnown := containsRemoteDiscoveryHost(hosts, host)
	now := d.clock.Now()
	if listErr == nil {
		if err := ports.ValidateRemoteCatalog(catalog); err != nil {
			listErr = err
		}
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
			d.refreshRemoteDiscoveryConsumers()
		}
		return cacheChanged
	}
	if listErr != nil {
		status := remoteHostMalformed
		var mismatch *ports.RemoteCatalogVersionMismatchError
		if errors.As(listErr, &mismatch) {
			status = remoteHostVersionMismatch
		} else if !errors.Is(listErr, ports.ErrInvalidRemoteCatalog) &&
			!errors.Is(listErr, ports.ErrRemoteCatalogTooLarge) &&
			!errors.Is(listErr, ports.ErrRemoteCatalogUnknownState) &&
			!errors.Is(listErr, ports.ErrRemoteCatalogInvalidReason) {
			status = remoteHostUnreachable
		}
		d.remoteCatalog.status[host] = status
		d.remoteCatalog.failure[host] = now
		d.remoteCatalog.mu.Unlock()
		d.refreshRemoteDiscoveryConsumers()
		return false
	}
	d.remoteCatalog.cache[host] = cloneRemoteCatalogEntry(ports.RemoteCatalogCacheEntry{
		Host:      host,
		FetchedAt: now,
		Sessions:  catalog.Sessions,
	})
	d.remoteCatalog.status[host] = remoteHostFresh
	delete(d.remoteCatalog.failure, host)
	d.remoteCatalog.mu.Unlock()

	d.persistRemoteCatalogCache()
	d.refreshRemoteDiscoveryConsumers()
	return true
}

func (d *Daemon) refreshRemoteDiscoveryConsumers() {
	if d == nil {
		return
	}
	type consumer struct {
		ac   *attachedClient
		kind remoteDiscoveryConsumerKind
	}
	d.remoteCatalog.mu.Lock()
	consumers := make([]consumer, 0, len(d.remoteCatalog.consumers))
	for ac, kind := range d.remoteCatalog.consumers {
		consumers = append(consumers, consumer{ac: ac, kind: kind})
	}
	d.remoteCatalog.mu.Unlock()

	for _, consumer := range consumers {
		ac := consumer.ac
		if ac == nil || ac.overlays == nil {
			continue
		}
		refreshed := false
		if consumer.kind&remoteDiscoveryPicker != 0 {
			d.refreshPickerOpts(ac, pickerRefreshOptions{preserveSelection: true, nearestRow: -1})
			refreshed = true
		}
		if consumer.kind&remoteDiscoveryPalette != 0 {
			d.refreshPalette(ac)
			refreshed = true
		}
		if !refreshed {
			continue
		}
		if entry := ac.currentAttachmentSession(); entry != nil {
			d.invalidateRender(entry, ac, true, "remote_picker.go")
		}
	}
}
