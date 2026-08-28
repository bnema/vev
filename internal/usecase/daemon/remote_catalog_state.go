package daemon

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/bnema/vev/internal/protocol/catalogue"
)

type remoteHostStatus uint8

const (
	remoteHostCached remoteHostStatus = iota
	remoteHostRefreshing
	remoteHostFresh
	remoteHostStale
	remoteHostUnreachable
	remoteHostVersionMismatch
	remoteHostMalformed
)

type remoteDiscoveryConsumerKind uint8

const (
	remoteDiscoveryPicker remoteDiscoveryConsumerKind = 1 << iota
	remoteDiscoveryPalette
)

// remoteCatalogState owns cache-derived discovery state. Its mutex never
// covers cache or transport I/O; writeMu serializes complete cache writes.
type remoteCatalogState struct {
	mu        sync.Mutex
	cache     map[string]catalogue.RemoteCatalogCacheEntry
	status    map[string]remoteHostStatus
	failure   map[string]time.Time
	refresh   uint64
	cancel    context.CancelFunc
	consumers map[*attachedClient]remoteDiscoveryConsumerKind
	writeMu   sync.Mutex
}

func newRemoteCatalogState() remoteCatalogState {
	return remoteCatalogState{
		cache:     make(map[string]catalogue.RemoteCatalogCacheEntry),
		status:    make(map[string]remoteHostStatus),
		failure:   make(map[string]time.Time),
		consumers: make(map[*attachedClient]remoteDiscoveryConsumerKind),
	}
}

func cloneRemoteCatalogEntry(entry catalogue.RemoteCatalogCacheEntry) catalogue.RemoteCatalogCacheEntry {
	sessions := make([]catalogue.RemoteCatalogSession, len(entry.Sessions))
	for i, session := range entry.Sessions {
		sessions[i] = session
		if tabs := catalogue.CatalogTabs(session); tabs != nil {
			sessions[i].Tabs = slices.Clone(tabs)
		}
	}
	return catalogue.RemoteCatalogCacheEntry{
		Host:      entry.Host,
		FetchedAt: entry.FetchedAt,
		Sessions:  sessions,
	}
}

func (s *remoteCatalogState) replaceCache(entries []catalogue.RemoteCatalogCacheEntry) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = make(map[string]catalogue.RemoteCatalogCacheEntry, len(entries))
	s.status = make(map[string]remoteHostStatus, len(entries))
	s.failure = make(map[string]time.Time)
	for _, entry := range entries {
		s.cache[entry.Host] = cloneRemoteCatalogEntry(entry)
		s.status[entry.Host] = remoteHostCached
	}
}

func (s *remoteCatalogState) cacheSnapshot() []catalogue.RemoteCatalogCacheEntry {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]catalogue.RemoteCatalogCacheEntry, 0, len(s.cache))
	for _, entry := range s.cache {
		entries = append(entries, cloneRemoteCatalogEntry(entry))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Host < entries[j].Host })
	return entries
}

func (d *Daemon) loadRemoteCatalogCache() {
	if d == nil || d.remoteCatalogCache == nil {
		return
	}
	entries, err := d.remoteCatalogCache.Load()
	if err != nil {
		d.log.Warn("loading remote catalog cache failed; starting empty", "err", err)
		return
	}
	d.remoteCatalog.replaceCache(entries)
}

func (d *Daemon) persistRemoteCatalogCache() {
	if d == nil || d.remoteCatalogCache == nil {
		return
	}
	d.remoteCatalog.writeMu.Lock()
	defer d.remoteCatalog.writeMu.Unlock()

	entries := d.remoteCatalog.cacheSnapshot()
	if err := d.remoteCatalogCache.Store(entries); err != nil {
		d.log.Warn("storing remote catalog cache failed", "err", err)
	}
}
