package daemon

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/bnema/vev/internal/ports"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/vt"
)

// snapshotChunkCacheLimit is deliberately small: history chunks remain owned
// by VT and the cache only retains their encoded VEVO representation.
const snapshotChunkCacheLimit = 16 << 20

type snapshotChunkCacheEntry struct {
	chunk  *vt.HistoryChunk
	object ports.SnapshotObject
}

// snapshotChunkCache is keyed by immutable HistoryChunk identity.  Entries
// never expose cache-owned bytes: publication gets a fresh slice, so a
// repository cannot mutate data used by an in-flight or later checkpoint.
type snapshotChunkCache struct {
	mu    sync.Mutex
	limit int
	used  int
	byPtr map[*vt.HistoryChunk]*list.Element
	lru   *list.List // front is most recently used; values are snapshotChunkCacheEntry
}

func newSnapshotChunkCache(limit int) *snapshotChunkCache {
	return &snapshotChunkCache{limit: limit, byPtr: make(map[*vt.HistoryChunk]*list.Element), lru: list.New()}
}

func (c *snapshotChunkCache) object(chunk *vt.HistoryChunk) (ports.SnapshotObject, error) {
	if chunk == nil {
		return ports.SnapshotObject{}, fmt.Errorf("snapshot: nil history chunk")
	}
	c.mu.Lock()
	if elem := c.byPtr[chunk]; elem != nil {
		c.lru.MoveToFront(elem)
		object := copySnapshotObject(elem.Value.(snapshotChunkCacheEntry).object)
		c.mu.Unlock()
		return object, nil
	}
	c.mu.Unlock()

	payload, err := vt.MarshalHistoryChunk(chunk)
	if err != nil {
		return ports.SnapshotObject{}, err
	}
	object, err := snapcodec.MarshalObject(snapcodec.HistoryChunk, payload)
	if err != nil {
		return ports.SnapshotObject{}, err
	}

	c.mu.Lock()
	// A second encoder can only occur in direct tests; retain one canonical
	// cache entry and let both publications own independent byte slices.
	if elem := c.byPtr[chunk]; elem != nil {
		c.lru.MoveToFront(elem)
		cached := copySnapshotObject(elem.Value.(snapshotChunkCacheEntry).object)
		c.mu.Unlock()
		return cached, nil
	}
	stored := copySnapshotObject(object)
	elem := c.lru.PushFront(snapshotChunkCacheEntry{chunk: chunk, object: stored})
	c.byPtr[chunk] = elem
	c.used += len(stored.Data)
	for c.used > c.limit && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		entry := oldest.Value.(snapshotChunkCacheEntry)
		delete(c.byPtr, entry.chunk)
		c.used -= len(entry.object.Data)
		c.lru.Remove(oldest)
	}
	c.mu.Unlock()
	return copySnapshotObject(object), nil
}

func copySnapshotObject(object ports.SnapshotObject) ports.SnapshotObject {
	object.Data = append([]byte(nil), object.Data...)
	return object
}

// incrementalPublication converts one immutable capture into complete VEVO
// objects plus its VEVM manifest. Encoding runs after pane locks are released.
// Sealed history uses pointer identity caching; tails and visible frames are
// intentionally not cached because they are copied mutable state.
func (d *Daemon) incrementalPublication(capture *snapshotCapture) (ports.SnapshotPublication, error) {
	if capture == nil || capture.name == "" {
		return ports.SnapshotPublication{}, fmt.Errorf("snapshot: empty capture")
	}
	if d.snapshotChunkCache == nil {
		d.snapshotChunkCache = newSnapshotChunkCache(snapshotChunkCacheLimit)
	}
	manifest := snapcodec.Manifest{Generation: capture.generation, Name: capture.name, CreatedAt: capture.createdAt, Active: capture.active, Tabs: make([]snapcodec.ManifestTab, 0, len(capture.tabs))}
	objects := make([]ports.SnapshotObject, 0)
	for _, tab := range capture.tabs {
		outTab := snapcodec.ManifestTab{StableID: tab.stableID, Cols: tab.cols, Rows: tab.rows, NextPaneID: tab.nextPaneID, Focus: tab.focus, Tree: tab.tree, Panes: make([]snapcodec.ManifestPane, 0, len(tab.panes))}
		for _, pane := range tab.panes {
			if pane.visibleErr != nil {
				return ports.SnapshotPublication{}, fmt.Errorf("snapshot visible: %w", pane.visibleErr)
			}
			outPane := snapcodec.ManifestPane{ID: pane.id, StableID: pane.stableID, Cwd: pane.cwd, Process: pane.process}
			sealedCount := pane.sealed.ChunkCount()
			sealedChunk := pane.sealed.Chunk
			if sealedCount == 0 && pane.history.ChunkCount() > 0 {
				sealedCount = pane.history.ChunkCount()
				sealedChunk = pane.history.Chunk
			}
			for i := 0; i < sealedCount; i++ {
				object, err := d.snapshotChunkCache.object(sealedChunk(i))
				if err != nil {
					return ports.SnapshotPublication{}, fmt.Errorf("snapshot history chunk: %w", err)
				}
				outPane.Sealed = append(outPane.Sealed, objectRef(snapcodec.HistoryChunk, object))
				objects = append(objects, object)
			}
			tail, err := vt.MarshalHistory(pane.tail)
			if pane.tail.Len() == 0 {
				tail, err = vt.MarshalEmptyHistoryTail()
			}
			if err != nil {
				return ports.SnapshotPublication{}, err
			}
			tailObject, err := snapcodec.MarshalObject(snapcodec.HistoryTail, tail)
			if err != nil {
				return ports.SnapshotPublication{}, err
			}
			visibleObject, err := snapcodec.MarshalObject(snapcodec.Visible, pane.visible)
			if err != nil {
				return ports.SnapshotPublication{}, err
			}
			outPane.Tail = objectRef(snapcodec.HistoryTail, tailObject)
			outPane.Visible = objectRef(snapcodec.Visible, visibleObject)
			objects = append(objects, tailObject, visibleObject)
			outTab.Panes = append(outTab.Panes, outPane)
		}
		manifest.Tabs = append(manifest.Tabs, outTab)
	}
	encoded, err := snapcodec.MarshalManifest(manifest)
	if err != nil {
		return ports.SnapshotPublication{}, err
	}
	return ports.SnapshotPublication{Name: capture.name, Generation: capture.generation, Manifest: encoded, Objects: objects}, nil
}

func objectRef(kind snapcodec.ObjectKind, object ports.SnapshotObject) snapcodec.ObjectRef {
	return snapcodec.ObjectRef{Kind: kind, Digest: object.Digest, Size: uint32(len(object.Data))}
}
