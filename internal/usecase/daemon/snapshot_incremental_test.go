package daemon

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
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
				appendHistoryRow(t, history, []renderer.Cell{{Rune: 'x'}})
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
		t.Fatal("recovery transcript encoding waited for pane lock")
	}
	pane.mu.Unlock()
}

func TestSnapshotCaptureOwnsRecoveryTranscriptAcrossLiveMutations(t *testing.T) {
	d := New(nil, nil, nil)
	sess := newSnapshotTestSession(t, "work", false, "/work")
	capture, ok := d.captureSnapshotState(sess, 1)
	require.True(t, ok)

	pane := sess.tabs[0].panes["pane-1"]
	pane.mu.Lock()
	pane.screen.Write([]byte("\x1b[1;1Hchanged"))
	pane.mu.Unlock()

	publication, err := d.incrementalPublication(capture)
	require.NoError(t, err)
	restored, err := sessionFromGeneration(snapshotGeneration(publication))
	require.NoError(t, err)
	view, err := vt.UnmarshalHistory(restored.Tabs[0].Panes[0].Transcript)
	require.NoError(t, err)
	require.Equal(t, "hello", strings.TrimRight(cellsString(view.Row(0)), " "))
}

func TestSnapshotChunkCacheResistsWarmedOverCapacityHistoryScans(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 6, ChunkRows: 1})
	for i := range 6 {
		appendHistoryRow(t, history, []renderer.Cell{{Rune: rune('a' + i)}})
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

func TestMarkSnapshotCaptureObjectsPublishedReleasesCachedEncoding(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 2, ChunkRows: 1})
	appendHistoryRow(t, history, []renderer.Cell{{Rune: 'x'}})
	view := history.SnapshotView()
	chunk := view.Chunk(0)
	transcript := vt.NewScreen(1, 1).RecoveryTranscriptSnapshot()
	d := New(nil, nil, nil)
	sess := newSnapshotTestSession(t, "work", false, "/work")
	capture := &snapshotCapture{session: sess, name: "work", incarnation: domain.IncarnationID{1}, generation: 1, tabs: []snapshotCaptureTab{{stableID: "t", cols: 1, rows: 1, panes: []snapshotCapturePane{{id: "p", stableID: "p", sealed: view, tail: view.Tail(), transcript: transcript}}}}}

	_, err := d.incrementalPublication(capture)
	require.NoError(t, err)
	entry, ok := sess.snapshotChunkCache.byPtr[chunk]
	require.True(t, ok)
	usedBefore := sess.snapshotChunkCache.used
	ref := capture.sealedRefs[chunk]

	markSnapshotCaptureObjectsPublished(capture)

	require.NotContains(t, sess.snapshotChunkCache.byPtr, chunk)
	require.Equal(t, usedBefore-len(entry.object.Data), sess.snapshotChunkCache.used)
	require.Equal(t, ref, sess.snapshotChunkCache.persisted[chunk])
	gotRef, object, err := sess.snapshotChunkCache.objectLocked(chunk)
	require.NoError(t, err)
	require.Equal(t, ref, gotRef)
	require.Nil(t, object)

	// Repeated acknowledgement must not release the same bytes twice.
	markSnapshotCaptureObjectsPublished(capture)
	require.Equal(t, usedBefore-len(entry.object.Data), sess.snapshotChunkCache.used)
	require.Equal(t, ref, sess.snapshotChunkCache.persisted[chunk])
}

func TestIncrementalPublicationReusesSealedChunkObject(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 2, ChunkRows: 1})
	appendHistoryRow(t, history, []renderer.Cell{renderer.BlankCell()})
	view := history.SnapshotView()
	transcript := vt.NewScreen(1, 1).RecoveryTranscriptSnapshot()
	d := New(nil, nil, nil)
	sess := newSnapshotTestSession(t, "work", false, "/work")
	capture := &snapshotCapture{session: sess, name: "work", incarnation: domain.IncarnationID{1}, generation: 1, tabs: []snapshotCaptureTab{{stableID: "t", cols: 1, rows: 1, panes: []snapshotCapturePane{{id: "p", stableID: "p", sealed: view, tail: view.Tail(), transcript: transcript}}}}}
	first, err := d.incrementalPublication(capture)
	if err != nil {
		t.Fatal(err)
	}
	firstRef := domain.CheckpointRef{Generation: first.Generation, ManifestDigest: snapcodec.ManifestDigest(first.Manifest)}
	capture.checkpoint = firstRef
	markSnapshotCaptureObjectsPublished(capture)
	capture.generation = 2
	capture.parentCheckpoint = &firstRef
	second, err := d.incrementalPublication(capture)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Objects) == 0 || len(second.Objects) == 0 {
		t.Fatal("publication omitted required objects")
	}
	require.Len(t, second.Objects, 2, "a generation must supply its tail and transcript once without resupplying persisted sealed chunks")
	kinds := make(map[snapcodec.ObjectKind]int)
	for _, object := range second.Objects {
		kind, _, err := snapcodec.PreflightObject(object.Data)
		require.NoError(t, err)
		kinds[kind]++
	}
	require.Equal(t, map[snapcodec.ObjectKind]int{snapcodec.HistoryTail: 1, snapcodec.RecoveryTranscript: 1}, kinds)
	require.Equal(t, &firstRef, second.ParentCheckpoint)
	manifest, err := snapcodec.UnmarshalManifest(second.Manifest)
	require.NoError(t, err)
	require.Equal(t, &firstRef, manifest.ParentCheckpoint)
	if got, want := sess.snapshotChunkCache.used, sess.snapshotChunkCache.limit; got > want {
		t.Fatalf("cache bytes = %d, limit = %d", got, want)
	}
}
