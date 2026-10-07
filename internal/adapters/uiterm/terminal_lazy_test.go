package uiterm

import (
	"bytes"
	"context"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// TestTerminalSnapshotReadsCommittedRevision pins the lazy conversion: a
// Snapshot read after later uncommitted writes still returns the committed
// screen of the latest revision, and each revision converts its own state.
func TestTerminalSnapshotReadsCommittedRevision(t *testing.T) {
	terminal := newTestTerminal(t, 4, 1)
	for _, tc := range []struct {
		write    string
		flush    bool
		picker   bool
		revision uint64
		first    string
	}{
		{write: "A", flush: true, revision: 1, first: "A"},
		{write: "\rB", revision: 1, first: "A"},
		{flush: true, revision: 2, first: "B"},
		{picker: true, revision: 3, first: "B"},
		{write: "\rC", revision: 3, first: "B"},
		{flush: true, revision: 4, first: "C"},
	} {
		if tc.write != "" {
			if _, err := terminal.Write([]byte(tc.write)); err != nil {
				t.Fatal(err)
			}
		}
		if tc.flush {
			if err := terminal.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if tc.picker {
			// A context-only publication reuses the committed frame.
			if err := terminal.PublishContext(ports.UIContext{AttachmentHandle: "attachment", Status: ports.UIStatusPicker}); err != nil {
				t.Fatal(err)
			}
		}
		snapshot, err := terminal.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Revision != tc.revision || snapshot.Cells[0].Text != tc.first {
			t.Fatalf("after %+v: revision %d first %q, want %d %q", tc, snapshot.Revision, snapshot.Cells[0].Text, tc.revision, tc.first)
		}
		if tc.revision >= 3 && snapshot.Context.Status != ports.UIStatusPicker {
			t.Fatalf("after %+v: context %+v lost the picker publication", tc, snapshot.Context)
		}
	}
}

// BenchmarkTerminalMirrorFlush measures one mirrored flush that nobody reads,
// the steady state of an interactive client with UI control enabled.
func BenchmarkTerminalMirrorFlush(b *testing.B) {
	terminal, err := NewMirror(context.Background(), domain.Geometry{Size: domain.Size{Cols: 200, Rows: 50}}, "attachment")
	if err != nil {
		b.Fatal(err)
	}
	defer terminal.Close()
	line := append(bytes.Repeat([]byte("x"), 199), '\n')
	b.ReportAllocs()
	for b.Loop() {
		terminal.ObserveTerminalWrite(line)
		terminal.ObserveTerminalFlush()
	}
}
