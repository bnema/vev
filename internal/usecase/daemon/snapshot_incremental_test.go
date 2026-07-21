package daemon

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func TestIncrementalPublicationReusesSealedChunkObject(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 2, ChunkRows: 1})
	history.Append([]renderer.Cell{renderer.BlankCell()})
	view := history.SealAndView()
	visible, err := vt.MarshalVisible(renderer.NewFrame(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	d := New(nil, nil, nil)
	capture := &snapshotCapture{name: "work", generation: 1, tabs: []snapshotCaptureTab{{stableID: "t", cols: 1, rows: 1, panes: []snapshotCapturePane{{id: "p", stableID: "p", history: view, visible: visible}}}}}
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
	if got, want := d.snapshotChunkCache.used, d.snapshotChunkCache.limit; got > want {
		t.Fatalf("cache bytes = %d, limit = %d", got, want)
	}
}
