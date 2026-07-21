package daemon

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func TestSnapshotChunkCacheIsScopedToNamedSession(t *testing.T) {
	d := New(nil, nil, nil)
	a := newSnapshotTestSession(t, "a", false, "/a")
	b := newSnapshotTestSession(t, "b", false, "/b")
	a.snapshotChunkCache = newSnapshotChunkCache(snapshotChunkCacheLimit)
	b.snapshotChunkCache = newSnapshotChunkCache(snapshotChunkCacheLimit)

	// Eleven maximum-size chunks exceed the per-session 16 MiB cache budget.
	// The capture retains independently owned publication objects while its LRU
	// sheds the oldest entry.
	aHistory := snapshotCacheHistory(t, 11, 'a')
	aCapture := snapshotCacheCapture(a, aHistory, 1)
	aFirst, err := d.incrementalPublication(aCapture)
	require.NoError(t, err)
	require.NotEmpty(t, aFirst.Objects)
	require.LessOrEqual(t, a.snapshotChunkCache.used, a.snapshotChunkCache.limit)
	require.LessOrEqual(t, a.snapshotChunkCache.limit, snapshotChunkCacheLimit)
	require.NotContains(t, a.snapshotChunkCache.byPtr, aHistory.Chunk(0), "session A should evict its oldest entry")

	bHistory := snapshotCacheHistory(t, 1, 'b')
	bCapture := snapshotCacheCapture(b, bHistory, 1)
	_, err = d.incrementalPublication(bCapture)
	require.NoError(t, err)
	require.Contains(t, b.snapshotChunkCache.byPtr, bHistory.Chunk(0))

	inFlightBytes := append([]byte(nil), aFirst.Objects[0].Data...)
	aSecond := snapshotCacheCapture(a, snapshotCacheHistory(t, 1, 'z'), 2)
	_, err = d.incrementalPublication(aSecond)
	require.NoError(t, err)
	require.True(t, bytes.Equal(inFlightBytes, aFirst.Objects[0].Data), "LRU eviction must not mutate queued publication bytes")
	require.LessOrEqual(t, a.snapshotChunkCache.used, snapshotChunkCacheLimit)

	_, err = d.incrementalPublication(snapshotCacheCapture(b, bHistory, 2))
	require.NoError(t, err)
	require.Contains(t, b.snapshotChunkCache.byPtr, bHistory.Chunk(0), "session A eviction must not affect session B")
	require.LessOrEqual(t, b.snapshotChunkCache.used, snapshotChunkCacheLimit)
}

func snapshotCacheHistory(t *testing.T, chunks int, first rune) vt.HistoryView {
	t.Helper()
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: chunks * 256, ChunkRows: 256})
	for row := 0; row < chunks*256; row++ {
		cells := make([]renderer.Cell, 160)
		for col := range cells {
			cells[col] = renderer.Cell{Rune: first + rune((row+col)%26)}
		}
		require.NoError(t, history.Append(cells))
	}
	return history.SealAndView()
}

func snapshotCacheCapture(sess *session, history vt.HistoryView, generation uint64) *snapshotCapture {
	return &snapshotCapture{
		session:    sess,
		name:       sess.name,
		generation: generation,
		tabs: []snapshotCaptureTab{{
			stableID: "tab",
			cols:     1,
			rows:     1,
			panes: []snapshotCapturePane{{
				id:       "pane",
				stableID: "pane",
				history:  history,
				visible:  vt.NewScreen(1, 1).PrimaryVisibleSnapshot(),
			}},
		}},
	}
}
