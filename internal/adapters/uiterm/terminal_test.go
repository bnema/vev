package uiterm

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestTerminalPublishesOwnedSnapshotAtFlush(t *testing.T) {
	terminal := newTestTerminal(t, 4, 2)
	terminal.BeginOutput(ports.UIContext{AttachmentHandle: "attachment", Generation: 2, Status: ports.UIStatusAttached})
	if _, err := terminal.Write([]byte("\x1b[1;31;48;2;1;2;3mA界")); err != nil {
		t.Fatal(err)
	}
	if err := terminal.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := terminal.Snapshot(); err == nil {
		t.Fatal("nested flush exposed an output transaction")
	}
	terminal.EndOutput(true)

	snapshot, err := terminal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != 1 || snapshot.Columns != 4 || snapshot.Rows != 2 {
		t.Fatalf("snapshot metadata = %#v", snapshot)
	}
	if snapshot.Context.Generation != 2 || snapshot.Cells[0].Text != "A" || !snapshot.Cells[0].Style.Bold {
		t.Fatalf("first cell/context = %#v / %#v", snapshot.Cells[0], snapshot.Context)
	}
	if snapshot.Cells[0].Style.Foreground.Kind != ports.UIColorIndexed || snapshot.Cells[0].Style.Foreground.Index != 1 {
		t.Fatalf("foreground = %#v", snapshot.Cells[0].Style.Foreground)
	}
	if snapshot.Cells[0].Style.Background != (ports.UIColor{Kind: ports.UIColorRGB, R: 1, G: 2, B: 3}) {
		t.Fatalf("background = %#v", snapshot.Cells[0].Style.Background)
	}
	if snapshot.Cells[1].Text != "界" || snapshot.Cells[1].Width != 2 || !snapshot.Cells[2].Continuation {
		t.Fatalf("wide cells = %#v %#v", snapshot.Cells[1], snapshot.Cells[2])
	}

	snapshot.Cells[0].Text = "mutated"
	again, err := terminal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if again.Cells[0].Text != "A" {
		t.Fatal("caller mutated published snapshot")
	}
}

func TestTerminalEnterRawUsesVisualInitialization(t *testing.T) {
	terminal := newTestTerminal(t, 4, 2)
	restore, err := terminal.EnterRaw()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := terminal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.AutoWrap || snapshot.Cursor.Visible {
		t.Fatalf("enter modes = autoWrap %v cursorVisible %v", snapshot.AutoWrap, snapshot.Cursor.Visible)
	}
	if err := restore(); err != nil {
		t.Fatal(err)
	}
	if err := restore(); err != nil {
		t.Fatalf("second restore = %v", err)
	}
}

func TestTerminalQueuesTerminalResponsesOutsideScreenWrite(t *testing.T) {
	terminal := newTestTerminal(t, 4, 2)
	if _, err := terminal.Write([]byte("\x1b[?7$p")); err != nil {
		t.Fatal(err)
	}

	result := make(chan string, 1)
	go func() {
		buffer := make([]byte, len("\x1b[?7;1$y"))
		_, err := io.ReadFull(terminal.In(), buffer)
		if err != nil {
			result <- err.Error()
			return
		}
		result <- string(buffer)
	}()
	select {
	case got := <-result:
		if got != "\x1b[?7;1$y" {
			t.Fatalf("response = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal response was not drained")
	}
}

func TestTerminalFailedTransactionMakesCaptureUnavailable(t *testing.T) {
	terminal := newTestTerminal(t, 4, 2)
	terminal.BeginOutput(ports.UIContext{})
	_, _ = terminal.Write([]byte("data"))
	terminal.EndOutput(false)
	if _, err := terminal.Snapshot(); err != ports.ErrUIUnavailable {
		t.Fatalf("Snapshot error = %v", err)
	}
}

func TestTerminalRejectsInvalidGeometry(t *testing.T) {
	tests := []domain.Geometry{
		{Size: domain.Size{Cols: 0, Rows: 1}},
		{Size: domain.Size{Cols: 1, Rows: 0}},
		{Size: domain.Size{Cols: MaxColumns + 1, Rows: 1}},
		{Size: domain.Size{Cols: 1, Rows: MaxRows + 1}},
	}
	for _, geometry := range tests {
		if terminal, err := New(context.Background(), geometry, "handle"); err == nil {
			terminal.Close()
			t.Fatalf("New(%#v) succeeded", geometry)
		}
	}
}

func newTestTerminal(t *testing.T, columns, rows int) *Terminal {
	t.Helper()
	terminal, err := New(context.Background(), domain.Geometry{Size: domain.Size{Cols: columns, Rows: rows}}, "attachment")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(terminal.Close)
	return terminal
}
