package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

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

func TestIncrementalPublicationReusesSealedChunkObject(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 2, ChunkRows: 1})
	history.Append([]renderer.Cell{renderer.BlankCell()})
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
