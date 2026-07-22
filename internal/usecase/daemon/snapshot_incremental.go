package daemon

import (
	"fmt"

	"github.com/bnema/vev/internal/ports"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/vt"
)

// snapshotChunkCacheLimit is deliberately small: history chunks remain owned
// by VT and each named-session cache only retains their encoded VEVO form.
const snapshotChunkCacheLimit = 16 << 20

type snapshotChunkCacheEntry struct {
	object ports.SnapshotObject
}

// snapshotChunkCache is keyed by immutable HistoryChunk identity. Its owner
// must hold the named session's snapshotMu for every operation. Entries never
// expose cache-owned bytes: a publication gets a fresh slice, so a repository
// cannot mutate data used by an in-flight or later checkpoint.
type snapshotChunkCache struct {
	limit int
	used  int
	byPtr map[*vt.HistoryChunk]snapshotChunkCacheEntry
	// marshalChunk makes cache admission measurable without coupling the cache
	// policy test to VT's wire encoding implementation.
	marshalChunk func(*vt.HistoryChunk) ([]byte, error)
}

func newSnapshotChunkCache(limit int) *snapshotChunkCache {
	return &snapshotChunkCache{
		limit:        limit,
		byPtr:        make(map[*vt.HistoryChunk]snapshotChunkCacheEntry),
		marshalChunk: vt.MarshalHistoryChunk,
	}
}

// objectLocked returns an independently owned object. Caller holds the owning
// session's snapshotMu.
func (c *snapshotChunkCache) objectLocked(chunk *vt.HistoryChunk) (ports.SnapshotObject, error) {
	if chunk == nil {
		return ports.SnapshotObject{}, fmt.Errorf("snapshot: nil history chunk")
	}
	if entry, ok := c.byPtr[chunk]; ok {
		return copySnapshotObject(entry.object), nil
	}

	payload, err := c.marshalChunk(chunk)
	if err != nil {
		return ports.SnapshotObject{}, err
	}
	object, err := snapcodec.MarshalObject(snapcodec.HistoryChunk, payload)
	if err != nil {
		return ports.SnapshotObject{}, err
	}

	// An oldest-to-newest snapshot scan can exceed the cache. LRU would admit
	// every early miss and evict the later working set, causing the next scan to
	// encode every immutable chunk again. Once full, retain the warmed set and
	// return later misses uncached. Pruning makes space when a retained chunk is
	// no longer reachable by a queued or in-flight capture.
	if len(object.Data) > c.limit || c.used > c.limit-len(object.Data) {
		return object, nil
	}
	stored := copySnapshotObject(object)
	c.byPtr[chunk] = snapshotChunkCacheEntry{object: stored}
	c.used += len(stored.Data)
	return object, nil
}

// pruneLocked releases cached encodings which no currently queued or in-flight
// capture can use. Caller holds the owning session's snapshotMu.
func (c *snapshotChunkCache) pruneLocked(referenced map[*vt.HistoryChunk]struct{}) {
	for chunk, entry := range c.byPtr {
		if _, keep := referenced[chunk]; !keep {
			delete(c.byPtr, chunk)
			c.used -= len(entry.object.Data)
		}
	}
}

func copySnapshotObject(object ports.SnapshotObject) ports.SnapshotObject {
	object.Data = append([]byte(nil), object.Data...)
	return object
}

// incrementalPublication converts one immutable capture into complete VEVO
// objects plus its VEVM manifest. Encoding runs after pane locks are released.
// Sealed history uses this capture's named-session pointer identity cache;
// tails and visible frames are intentionally not cached because they are copied
// mutable state.
func (d *Daemon) incrementalPublication(capture *snapshotCapture) (ports.SnapshotPublication, error) {
	if capture == nil || capture.session == nil || capture.name == "" {
		return ports.SnapshotPublication{}, fmt.Errorf("snapshot: empty capture")
	}
	if err := prepareSnapshotChunkCache(capture); err != nil {
		return ports.SnapshotPublication{}, err
	}
	manifest := snapcodec.Manifest{Generation: capture.generation, Name: capture.name, CreatedAt: capture.createdAt, Active: capture.active, Tabs: make([]snapcodec.ManifestTab, 0, len(capture.tabs))}
	objects := make([]ports.SnapshotObject, 0)
	for _, tab := range capture.tabs {
		outTab := snapcodec.ManifestTab{StableID: tab.stableID, Cols: tab.cols, Rows: tab.rows, NextPaneID: tab.nextPaneID, Focus: tab.focus, Tree: tab.tree, Panes: make([]snapcodec.ManifestPane, 0, len(tab.panes))}
		for _, pane := range tab.panes {
			// visible was copied under pane.mu. Marshal it here in the worker,
			// after all pane and session locks have been released.
			visible, err := pane.visible.Marshal()
			if err != nil {
				return ports.SnapshotPublication{}, fmt.Errorf("snapshot visible: %w", err)
			}
			outPane := snapcodec.ManifestPane{ID: pane.id, StableID: pane.stableID, Cwd: pane.cwd, Process: pane.process}
			for i := 0; i < pane.sealed.ChunkCount(); i++ {
				object, err := snapshotChunkObject(capture, pane.sealed.Chunk(i))
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
			visibleObject, err := snapcodec.MarshalObject(snapcodec.Visible, visible)
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

func prepareSnapshotChunkCache(capture *snapshotCapture) error {
	sess := capture.session
	sess.snapshotMu.Lock()
	defer sess.snapshotMu.Unlock()
	if sess.snapshotChunkCache == nil {
		if sess.name == "" || sess.ephemeral {
			return fmt.Errorf("snapshot: chunk cache unavailable")
		}
		sess.snapshotChunkCache = newSnapshotChunkCache(snapshotChunkCacheLimit)
	}
	pruneSnapshotChunkCacheLocked(sess, capture)
	return nil
}

func snapshotChunkObject(capture *snapshotCapture, chunk *vt.HistoryChunk) (ports.SnapshotObject, error) {
	sess := capture.session
	sess.snapshotMu.Lock()
	defer sess.snapshotMu.Unlock()
	return sess.snapshotChunkCache.objectLocked(chunk)
}

// pruneSnapshotChunkCacheLocked retains only chunks referenced by captures that
// can still be published. Caller holds sess.snapshotMu.
func pruneSnapshotChunkCacheLocked(sess *session, capture *snapshotCapture) {
	if sess.snapshotChunkCache == nil {
		return
	}
	referenced := make(map[*vt.HistoryChunk]struct{})
	collectSnapshotCaptureChunks(referenced, capture)
	collectSnapshotCaptureChunks(referenced, sess.snapshotQueuedCapture)
	collectSnapshotCaptureChunks(referenced, sess.snapshotInFlightCapture)
	sess.snapshotChunkCache.pruneLocked(referenced)
}

func collectSnapshotCaptureChunks(referenced map[*vt.HistoryChunk]struct{}, capture *snapshotCapture) {
	if capture == nil {
		return
	}
	for _, tab := range capture.tabs {
		for _, pane := range tab.panes {
			for i := 0; i < pane.sealed.ChunkCount(); i++ {
				if chunk := pane.sealed.Chunk(i); chunk != nil {
					referenced[chunk] = struct{}{}
				}
			}
		}
	}
}

func objectRef(kind snapcodec.ObjectKind, object ports.SnapshotObject) snapcodec.ObjectRef {
	return snapcodec.ObjectRef{Kind: kind, Digest: object.Digest, Size: uint32(len(object.Data))}
}
