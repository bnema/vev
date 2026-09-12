package remote

import (
	"sync"

	appports "github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// SeededCatalogCache is a runner-local remote catalogue cache: it starts from
// the durable cache's entries and only ever writes in memory. A client monitor
// uses it so its own observations can never race, truncate, or overwrite the
// daemon's whole-directory snapshot file.
type SeededCatalogCache struct {
	seed appports.RemoteCatalogCache

	mu      sync.Mutex
	loaded  bool
	entries []catalogue.RemoteCatalogCacheEntry
	loadErr error
}

var _ appports.RemoteCatalogCache = (*SeededCatalogCache)(nil)

// NewSeededCatalogCache wraps the durable cache. Construction performs no I/O:
// the seed is read on the first Load.
func NewSeededCatalogCache(seed appports.RemoteCatalogCache) *SeededCatalogCache {
	return &SeededCatalogCache{seed: seed}
}

// Load returns the in-memory entries, reading the durable seed once.
func (c *SeededCatalogCache) Load() ([]catalogue.RemoteCatalogCacheEntry, error) {
	if c == nil {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded {
		c.loaded = true
		if c.seed != nil {
			entries, err := c.seed.Load()
			if err != nil {
				// A failed seed read is reported once and never retried: the
				// client monitor must not hammer a broken cache file.
				c.loadErr = err
			} else {
				c.entries = cloneCatalogEntries(entries)
			}
		}
	}
	if c.loadErr != nil {
		return nil, c.loadErr
	}
	return cloneCatalogEntries(c.entries), nil
}

// Store replaces the in-memory entries. Nothing reaches the durable file.
func (c *SeededCatalogCache) Store(entries []catalogue.RemoteCatalogCacheEntry) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loaded = true
	c.entries = cloneCatalogEntries(entries)
	return nil
}

// cloneCatalogEntries returns a defensive copy of the entries, including the
// nested session and tab slices the monitor publishes from.
func cloneCatalogEntries(entries []catalogue.RemoteCatalogCacheEntry) []catalogue.RemoteCatalogCacheEntry {
	if entries == nil {
		return nil
	}
	out := make([]catalogue.RemoteCatalogCacheEntry, len(entries))
	for i, entry := range entries {
		out[i] = entry
		out[i].Sessions = make([]catalogue.RemoteCatalogSession, len(entry.Sessions))
		for j, session := range entry.Sessions {
			out[i].Sessions[j] = session
			out[i].Sessions[j].Tabs = append([]catalogue.RemoteCatalogTab(nil), session.Tabs...)
		}
	}
	return out
}
