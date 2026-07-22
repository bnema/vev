package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func TestMarshalSnapshotTailSelectsCanonicalEncoder(t *testing.T) {
	for _, tt := range []struct {
		name string
		fill bool
	}{
		{name: "empty tail"},
		{name: "non-empty tail", fill: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			history := vt.NewHistory(vt.HistoryConfig{MaxRows: 2, ChunkRows: 2})
			if tt.fill {
				require.NoError(t, history.Append([]renderer.Cell{{Rune: 'x'}}))
			}
			tail := history.SnapshotView().Tail()

			got, err := marshalSnapshotTail(tail)
			require.NoError(t, err)
			if tt.fill {
				want, err := vt.MarshalHistory(tail)
				require.NoError(t, err)
				require.Equal(t, want, got)
				return
			}
			want, err := vt.MarshalEmptyHistoryTail()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestIncrementalPublicationEncodesAfterPaneUnlock(t *testing.T) {
	d := New(nil, nil, nil)
	sess := newSnapshotTestSession(t, "work", false, "/work")
	capture, ok := d.captureSnapshotState(sess, 1)
	if !ok {
		t.Fatal("capture rejected named session")
	}

	pane := sess.tabs[0].panes["pane-1"]
	pane.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := d.incrementalPublication(capture)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("encode capture: %v", err)
		}
	case <-time.After(testWaitTimeout):
		pane.mu.Unlock()
		t.Fatal("visible encoding waited for pane lock")
	}
	pane.mu.Unlock()
}

func TestSnapshotChunkCacheResistsWarmedOverCapacityHistoryScans(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 6, ChunkRows: 1})
	for i := range 6 {
		require.NoError(t, history.Append([]renderer.Cell{{Rune: rune('a' + i)}}))
	}
	view := history.SnapshotView()
	require.Equal(t, 6, view.ChunkCount())

	firstPayload, err := vt.MarshalHistoryChunk(view.Chunk(0))
	require.NoError(t, err)
	firstObject, err := snapcodec.MarshalObject(snapcodec.HistoryChunk, firstPayload)
	require.NoError(t, err)
	cache := newSnapshotChunkCache(2 * len(firstObject.Data))
	encodes := make(map[*vt.HistoryChunk]int)
	cache.marshalChunk = func(chunk *vt.HistoryChunk) ([]byte, error) {
		encodes[chunk]++
		return vt.MarshalHistoryChunk(chunk)
	}

	for pass := range 3 {
		for i := range view.ChunkCount() {
			_, _, err := cache.objectLocked(view.Chunk(i))
			require.NoError(t, err)
		}
		require.LessOrEqual(t, cache.used, cache.limit)
		if pass == 0 {
			require.Len(t, cache.byPtr, 2)
		}
	}

	// The cache admits the first bounded working set then rejects later scan
	// misses. Consequently, each warmed retained chunk is encoded only once
	// even though the history is repeatedly scanned oldest-to-newest.
	require.Equal(t, 1, encodes[view.Chunk(0)])
	require.Equal(t, 1, encodes[view.Chunk(1)])
	require.Equal(t, 3, encodes[view.Chunk(2)])
	require.Equal(t, 3, encodes[view.Chunk(5)])

	isolated := newSnapshotChunkCache(cache.limit)
	isolatedEncodes := 0
	isolated.marshalChunk = func(chunk *vt.HistoryChunk) ([]byte, error) {
		isolatedEncodes++
		return vt.MarshalHistoryChunk(chunk)
	}
	_, _, err = isolated.objectLocked(view.Chunk(0))
	require.NoError(t, err)
	require.Equal(t, 1, isolatedEncodes, "a session cache must not share encoded history with another session")
}

func TestIncrementalPublicationReusesSealedChunkObject(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 2, ChunkRows: 1})
	require.NoError(t, history.Append([]renderer.Cell{renderer.BlankCell()}))
	view := history.SnapshotView()
	visible := vt.NewScreen(1, 1).PrimaryVisibleSnapshot()
	d := New(nil, nil, nil)
	sess := newSnapshotTestSession(t, "work", false, "/work")
	capture := &snapshotCapture{session: sess, name: "work", generation: 1, tabs: []snapshotCaptureTab{{stableID: "t", cols: 1, rows: 1, panes: []snapshotCapturePane{{id: "p", stableID: "p", sealed: view, tail: view.Tail(), visible: visible}}}}}
	first, err := d.incrementalPublication(capture)
	if err != nil {
		t.Fatal(err)
	}
	capture.generation = 2
	second, err := d.incrementalPublication(capture)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Objects) == 0 || len(second.Objects) == 0 {
		t.Fatal("publication omitted required objects")
	}
	if got, want := sess.snapshotChunkCache.used, sess.snapshotChunkCache.limit; got > want {
		t.Fatalf("cache bytes = %d, limit = %d", got, want)
	}
}
