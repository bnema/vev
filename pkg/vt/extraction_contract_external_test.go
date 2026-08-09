package vt_test

import (
	"reflect"
	"testing"

	"github.com/bnema/vev/pkg/vt"
	"github.com/bnema/vev/pkg/vtcore"
)

func TestExternalScreenParserPreservesPublicTerminalModel(t *testing.T) {
	screen := vt.NewScreen(6, 2)
	screen.Write([]byte("A界"))

	if got := screen.Frame.At(0, 0); got.Rune != 'A' {
		t.Fatalf("ASCII cell = %#v, want A", got)
	}
	if got := screen.Frame.At(1, 0); got.Rune != '界' || got.Continuation {
		t.Fatalf("wide-rune lead cell = %#v, want 界 lead", got)
	}
	if got := screen.Frame.At(2, 0); !got.Continuation {
		t.Fatalf("wide-rune continuation = %#v, want continuation", got)
	}
	if got := vt.RuneWidth('界'); got != 2 {
		t.Fatalf("RuneWidth(界) = %d, want 2", got)
	}

	// The parser owns incomplete escape sequences across writes, so callers can
	// provide arbitrary PTY chunk boundaries.
	screen.Write([]byte("\x1b[2;2H\x1b[3"))
	screen.Write([]byte("1mX"))
	cell := screen.Frame.At(1, 1)
	if cell.Rune != 'X' || cell.Style.Foreground != 1 {
		t.Fatalf("split ANSI write = %#v, want red X", cell)
	}

	screen.Write([]byte("\x1b]2;public title\a"))
	if got := screen.Snapshot().Title(); got != "public title" {
		t.Fatalf("terminal title = %q, want public title", got)
	}
}

func TestExternalCallbacksAreSynchronousAndOrdered(t *testing.T) {
	screen := vt.NewScreen(8, 2)
	inWrite := false
	var events []string
	assertInWrite := func(name string) {
		if !inWrite {
			t.Errorf("%s callback ran after Write returned", name)
		}
	}

	screen.OnBell = func() {
		assertInWrite("bell")
		events = append(events, "bell")
	}
	screen.OnResponse = func(response []byte) {
		assertInWrite("response")
		events = append(events, "response:"+string(response))
	}
	screen.OnNotify = func(title, body string) {
		assertInWrite("notify")
		events = append(events, "notify:"+title+":"+body)
	}
	screen.OnProgress = func(errored bool) {
		assertInWrite("progress")
		if errored {
			events = append(events, "progress:error")
		} else {
			events = append(events, "progress:clear")
		}
	}
	screen.OnClipboard = func(data string) {
		assertInWrite("clipboard")
		events = append(events, "clipboard:"+data)
	}

	inWrite = true
	screen.Write([]byte("\a\x1b[5n\x1b]9;body\a\x1b]777;notify;title;body\a" +
		"\x1b]9;4;1\a\x1b]9;4;0\a\x1b]9;4;2\a" +
		"\x1b]52;c;SGVsbG8=\a"))
	inWrite = false

	want := []string{
		"bell",
		"response:\x1b[0n",
		"notify::body",
		"notify:title:body",
		"progress:clear",
		"progress:error",
		"clipboard:SGVsbG8=",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("callback events = %#v, want %#v", events, want)
	}
}

func TestExternalSnapshotsAndHistoryRespectOwnershipContracts(t *testing.T) {
	screen := vt.NewScreenWithHistory(4, 2, vt.HistoryConfig{MaxRows: 8, ChunkRows: 2})
	screen.Write([]byte("A"))
	snapshot := screen.Snapshot()

	owned := snapshot.Row(0)
	owned[0] = vtcore.Cell{Rune: 'X'}
	if got := snapshot.Row(0)[0].Rune; got != 'A' {
		t.Fatalf("owned snapshot row mutation changed snapshot to %q", got)
	}

	screen.Write([]byte("\rX"))
	if got := snapshot.Row(0)[0].Rune; got != 'A' {
		t.Fatalf("snapshot changed after later screen write: %q", got)
	}
	if got := snapshot.RowIDs(); len(got) != 2 || got[0] == 0 || got[1] == 0 {
		t.Fatalf("snapshot row IDs = %#v, want stable nonzero IDs", got)
	}
	rowIDs := snapshot.RowIDs()
	rowIDs[0] = 0
	if snapshot.RowID(0) == 0 {
		t.Fatal("RowIDs did not return an owned copy")
	}

	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 8, ChunkRows: 2})
	appendHistoryRow(t, history, 'a', 10)
	appendHistoryRow(t, history, 'b', 11)
	before := history.View()
	chunk := before.Chunk(0)
	if chunk == nil || before.FindRowID(11) != 1 {
		t.Fatalf("history view = len %d chunk %#v IDs %d/%d", before.Len(), chunk, before.RowID(0), before.RowID(1))
	}

	copyOfRow := before.Row(0)
	copyOfRow[0] = vtcore.Cell{Rune: 'x'}
	if got := before.Row(0)[0].Rune; got != 'a' {
		t.Fatalf("owned history row mutation changed view to %q", got)
	}
	if borrowed := before.BorrowedRow(0); &borrowed[0] == &copyOfRow[0] {
		t.Fatal("Row returned borrowed history storage instead of an owned copy")
	}

	appendHistoryRow(t, history, 'c', 12)
	after := history.View()
	if after.Chunk(0) != chunk {
		t.Fatal("sealed history chunk identity changed after an append")
	}
	if before.Len() != 2 || before.RowID(0) != 10 || before.RowID(1) != 11 {
		t.Fatalf("retained history view changed after append: len=%d IDs=%d/%d", before.Len(), before.RowID(0), before.RowID(1))
	}

	persistenceView := history.SnapshotView()
	appendHistoryRow(t, history, 'd', 13)
	if got := persistenceView.Tail().Row(0)[0].Rune; got != 'c' {
		t.Fatalf("snapshot tail changed after history append to %q", got)
	}
}

func TestExternalSingleOwnerUsageSerializesMutationAndCapture(t *testing.T) {
	screen := vt.NewScreen(4, 1)
	operations := make(chan func(), 3)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for operation := range operations {
			operation()
		}
	}()

	operations <- func() { screen.Write([]byte("A")) }
	captured := make(chan vt.ScreenSnapshot, 1)
	operations <- func() { captured <- screen.Snapshot() }
	close(operations)
	<-done

	if got := (<-captured).Row(0)[0].Rune; got != 'A' {
		t.Fatalf("single-owner capture = %q, want A", got)
	}
}

func appendHistoryRow(t *testing.T, history *vt.History, runeValue rune, id vt.RowID) {
	t.Helper()
	if err := history.AppendWithID([]vtcore.Cell{{Rune: runeValue}}, vt.LineBound{End: 1}, id); err != nil {
		t.Fatalf("AppendWithID(%d): %v", id, err)
	}
}
