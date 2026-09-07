package uiterm

import (
	"context"
	"errors"
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

func TestTerminalPublishesGeometryChangesImmediatelyOutsideOutputTransaction(t *testing.T) {
	terminal := newTestTerminal(t, 4, 2)
	terminal.ObserveTerminalResize(domain.Geometry{Size: domain.Size{Cols: 6, Rows: 3}})
	snapshot, err := terminal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Columns != 6 || snapshot.Rows != 3 || snapshot.Revision == 0 {
		t.Fatalf("resize snapshot = %#v", snapshot)
	}
}

func TestTerminalRecoversAfterOversizedGeometry(t *testing.T) {
	terminal := newTestTerminal(t, 4, 2)
	terminal.ObserveTerminalResize(domain.Geometry{Size: domain.Size{Cols: MaxColumns + 1, Rows: 2}})
	_, err := terminal.Snapshot()
	var uiErr *ports.UIError
	if !errors.As(err, &uiErr) || uiErr.Code != ports.UIErrCaptureTooLarge {
		t.Fatalf("oversized Snapshot() error = %v, want capture_too_large", err)
	}

	terminal.ObserveTerminalResize(domain.Geometry{Size: domain.Size{Cols: 6, Rows: 3}})
	snapshot, err := terminal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Columns != 6 || snapshot.Rows != 3 || snapshot.Revision == 0 {
		t.Fatalf("recovered resize snapshot = %#v", snapshot)
	}
}

func TestTerminalSignalsCaptureUnavailable(t *testing.T) {
	terminal := newTestTerminal(t, 4, 2)
	terminal.BeginOutput(ports.UIContext{})
	_, _ = terminal.Write([]byte("data"))
	terminal.EndOutput(false)
	select {
	case <-terminal.Changes():
	case <-time.After(time.Second):
		t.Fatal("capture unavailability did not signal Changes")
	}
}

func TestTerminalFailedTransactionMakesCaptureUnavailable(t *testing.T) {
	terminal := newTestTerminal(t, 4, 2)
	terminal.BeginOutput(ports.UIContext{})
	_, _ = terminal.Write([]byte("data"))
	terminal.EndOutput(false)
	if _, err := terminal.Snapshot(); !errors.Is(err, ports.ErrUIUnavailable) {
		t.Fatalf("Snapshot error = %v", err)
	}
}

func TestMirrorReportsOversizedGeometryWithoutRejectingTheTerminal(t *testing.T) {
	ctx := context.Background()
	mirror, err := NewMirror(ctx, domain.Geometry{Size: domain.Size{Cols: MaxColumns + 1, Rows: 2}}, "attachment")
	if err != nil {
		t.Fatal(err)
	}
	defer mirror.Close()

	if _, err := mirror.Write([]byte("still interactive")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := mirror.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	_, err = mirror.Snapshot()
	var uiErr *ports.UIError
	if !errors.As(err, &uiErr) || uiErr.Code != ports.UIErrCaptureTooLarge {
		t.Fatalf("Snapshot() error = %v, want capture_too_large", err)
	}

	validMirror, err := NewMirror(ctx, domain.Geometry{Size: domain.Size{Cols: 4, Rows: 2}}, "attachment")
	if err != nil {
		t.Fatal(err)
	}
	defer validMirror.Close()
	validMirror.ObserveTerminalResize(domain.Geometry{Size: domain.Size{Cols: MaxColumns + 1, Rows: 2}})
	if _, err := validMirror.Write([]byte("output while oversized")); err != nil {
		t.Fatalf("oversized mirror Write() error = %v", err)
	}
	if err := validMirror.Flush(); err != nil {
		t.Fatalf("oversized mirror Flush() error = %v", err)
	}
	validMirror.ObserveTerminalResize(domain.Geometry{Size: domain.Size{Cols: 4, Rows: 2}})
	_, err = validMirror.Snapshot()
	if !errors.As(err, &uiErr) || uiErr.Code != ports.UIErrCaptureTooLarge {
		t.Fatalf("recovered mirror Snapshot() error = %v, want capture_too_large", err)
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
