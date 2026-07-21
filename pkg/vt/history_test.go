package vt

import (
	"errors"
	"math"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func requireHistoryAppend(t testing.TB, history *History, row []renderer.Cell) {
	t.Helper()
	if err := history.Append(row); err != nil {
		t.Fatalf("append history row: %v", err)
	}
}

func TestHistoryOrdersSealedChunksAndEvictsAtCapacity(t *testing.T) {
	tests := []struct {
		name      string
		maxRows   int
		chunkRows int
		rows      []string
		want      []string
	}{
		{
			name:      "oldest chunks are evicted before newer chunks",
			maxRows:   4,
			chunkRows: 2,
			rows:      []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"},
			want:      []string{"cccc", "dddd", "eeee", "ffff"},
		},
		{
			name:      "nonmultiple capacity evicts exactly the oldest row",
			maxRows:   5,
			chunkRows: 2,
			rows:      []string{"0000", "1111", "2222", "3333", "4444", "5555"},
			want:      []string{"1111", "2222", "3333", "4444", "5555"},
		},
		{
			name:      "rows preserve append order across chunk boundaries",
			maxRows:   4,
			chunkRows: 2,
			rows:      []string{"0000", "1111", "2222", "3333"},
			want:      []string{"0000", "1111", "2222", "3333"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := NewHistory(HistoryConfig{MaxRows: tt.maxRows, ChunkRows: tt.chunkRows})
			for _, text := range tt.rows {
				requireHistoryAppend(t, history, historyRow(text))
			}

			view := history.View()
			if got := view.Len(); got > tt.maxRows {
				t.Fatalf("retained row count = %d, exceeds capacity %d", got, tt.maxRows)
			}
			if got := historyViewTexts(view); !equalStrings(got, tt.want) {
				t.Fatalf("view rows = %#v, want %#v", got, tt.want)
			}
			if got, want := view.ChunkCount(), (len(tt.want)+1)/2; got != want {
				t.Fatalf("chunk count = %d, want %d", got, want)
			}
		})
	}
}

func TestHistoryTailRotationAndStableViews(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "a full tail seals and a later append cannot change an existing view"},
		{name: "rows passed to append are copied before the screen recycles them"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := NewHistory(HistoryConfig{MaxRows: 6, ChunkRows: 2})
			first := historyRow("AAAA")
			requireHistoryAppend(t, history, first)
			requireHistoryAppend(t, history, historyRow("BBBB"))
			view := history.View()
			if got, want := view.ChunkCount(), 1; got != want {
				t.Fatalf("sealed chunk count = %d, want %d", got, want)
			}

			first[0].Rune = 'X'
			requireHistoryAppend(t, history, historyRow("CCCC"))

			if got, want := historyViewTexts(view), []string{"AAAA", "BBBB"}; !equalStrings(got, want) {
				t.Fatalf("stable view rows = %#v, want %#v", got, want)
			}
			if got, want := historyViewTexts(history.View()), []string{"AAAA", "BBBB", "CCCC"}; !equalStrings(got, want) {
				t.Fatalf("current view rows = %#v, want %#v", got, want)
			}
		})
	}
}

func TestHistoryReusesSealedChunkIdentityAcrossViews(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "unchanged sealed chunks are shared by successive views"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := NewHistory(HistoryConfig{MaxRows: 8, ChunkRows: 2})
			requireHistoryAppend(t, history, historyRow("AAAA"))
			requireHistoryAppend(t, history, historyRow("BBBB"))
			before := history.View()

			requireHistoryAppend(t, history, historyRow("CCCC"))
			after := history.View()

			if before.ChunkCount() == 0 || after.ChunkCount() == 0 {
				t.Fatal("sealed chunk missing from view")
			}
			if before.Chunk(0) != after.Chunk(0) {
				t.Fatal("unchanged sealed chunk was copied instead of reused")
			}
		})
	}
}

func TestHistoryOwnedByScreenIgnoresAlternateScreenEvictions(t *testing.T) {
	tests := []struct {
		name  string
		write []byte
		want  []string
	}{
		{
			name:  "primary screen evictions enter terminal history",
			write: []byte("AAAA\r\nBBBB\r\nCCCC\r\n"),
			want:  []string{"AAAA"},
		},
		{
			name:  "alternate screen scrolling never enters terminal history",
			write: []byte("AAAA\r\nBBBB\r\nCCCC\r\n\x1b[?1049h1111\r\n2222\r\n3333\r\n4444\x1b[?1049l"),
			want:  []string{"AAAA"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			screen := NewScreenWithHistory(4, 3, HistoryConfig{MaxRows: 8, ChunkRows: 2})
			screen.Write(tt.write)

			if got := historyViewTexts(screen.History().View()); !equalStrings(got, tt.want) {
				t.Fatalf("terminal history rows = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestHistoryRecordsOnlyTopEdgeScrollEvictions(t *testing.T) {
	tests := []struct {
		name       string
		top        int
		bottom     int
		wantRows   int
		wantEvents int
	}{
		{
			name:       "interior scroll region does not enter global history",
			top:        1,
			bottom:     3,
			wantRows:   0,
			wantEvents: 0,
		},
		{
			name:       "top-edge scroll enters global history",
			top:        0,
			bottom:     3,
			wantRows:   1,
			wantEvents: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreenWithHistory(4, 4, HistoryConfig{MaxRows: 8, ChunkRows: 2})
			for y := range s.Frame.Height {
				copy(s.Frame.Row(y), historyRow(string([]byte{byte('A' + y), byte('A' + y), byte('A' + y), byte('A' + y)})))
			}
			events := 0
			s.OnLineEvicted = func([]renderer.Cell) { events++ }

			s.scrollUpRegion(tt.top, tt.bottom, 1)

			if got := s.History().View().Len(); got != tt.wantRows {
				t.Errorf("history rows = %d, want %d", got, tt.wantRows)
			}
			if events != tt.wantEvents {
				t.Errorf("eviction events = %d, want %d", events, tt.wantEvents)
			}
		})
	}
}

func TestHistoryBoundsRowsAndCellsWithExactRowEviction(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: 4, MaxCells: 5, ChunkRows: 2})
	for _, text := range []string{"aa", "bbb", "c", "dd"} {
		if err := history.Append(historyRow(text)); err != nil {
			t.Fatalf("append %q: %v", text, err)
		}
	}

	view := history.View()
	if got, want := historyViewTexts(view), []string{"c", "dd"}; !equalStrings(got, want) {
		t.Fatalf("retained rows = %#v, want %#v", got, want)
	}
	if got, want := history.Len(), 2; got != want {
		t.Fatalf("retained rows = %d, want %d", got, want)
	}
	if got, want := history.Cells(), 3; got != want {
		t.Fatalf("retained cells = %d, want %d", got, want)
	}
	if got, want := view.Cells(), 3; got != want {
		t.Fatalf("view cells = %d, want %d", got, want)
	}
}

func TestHistoryAppendIsNoOpForNilAndZeroValue(t *testing.T) {
	tests := []struct {
		name    string
		history *History
	}{
		{name: "nil history"},
		{name: "zero-value history", history: &History{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.history.Append(historyRow("row")); err != nil {
				t.Fatalf("append error = %v, want nil", err)
			}
			if got := tt.history.Len(); got != 0 {
				t.Fatalf("retained rows = %d, want 0", got)
			}
		})
	}
}

func TestHistoryRejectsRowWiderThanCellBudgetWithoutMutation(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: 2, MaxCells: 2, ChunkRows: 2})
	if err := history.Append(historyRow("ok")); err != nil {
		t.Fatalf("append retained row: %v", err)
	}
	before := history.View()

	err := history.Append(historyRow("wide"))
	if !errors.Is(err, ErrHistoryRowTooWide) {
		t.Fatalf("append oversized row error = %v, want ErrHistoryRowTooWide", err)
	}
	if got, want := historyViewTexts(history.View()), historyViewTexts(before); !equalStrings(got, want) {
		t.Fatalf("history mutated after rejected append: got %#v, want %#v", got, want)
	}
	if got, want := history.Cells(), 2; got != want {
		t.Fatalf("retained cells after rejected append = %d, want %d", got, want)
	}
}

func TestScreenDropsOversizedHistoryRowsWithoutInterruptingScroll(t *testing.T) {
	screen := NewScreenWithHistory(4, 2, HistoryConfig{MaxRows: 2, MaxCells: 3})
	events := 0
	screen.OnLineEvicted = func([]renderer.Cell) { events++ }
	copy(screen.Frame.Row(0), historyRow("AAAA"))
	copy(screen.Frame.Row(1), historyRow("BBBB"))
	screen.scrollUpRegion(0, 1, 1)

	if got := screen.History().Len(); got != 0 {
		t.Fatalf("oversized scrollback rows = %d, want 0", got)
	}
	if got := events; got != 1 {
		t.Fatalf("scroll callbacks = %d, want 1", got)
	}
	if got := lineText(screen, 0); got != "BBBB" {
		t.Fatalf("screen did not scroll after history drop: row 0 = %q", got)
	}
}

func TestHistoryDefaultCellBudgetDoesNotOverflow(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: math.MaxInt, ChunkRows: 1})
	if got, want := history.CellCap(), math.MaxInt; got != want {
		t.Fatalf("default cell cap = %d, want %d", got, want)
	}
}

func historyRow(text string) []renderer.Cell {
	row := make([]renderer.Cell, len(text))
	for i, r := range text {
		row[i] = renderer.BlankCell()
		row[i].Rune = r
	}
	return row
}

func historyViewTexts(view HistoryView) []string {
	rows := make([]string, view.Len())
	for i := range rows {
		rows[i] = rowText(view.Row(i))
	}
	return rows
}

func rowText(row []renderer.Cell) string {
	runes := make([]rune, len(row))
	for i, cell := range row {
		runes[i] = cell.Rune
	}
	return string(runes)
}
