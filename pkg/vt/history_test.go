package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestHistoryOrdersSealedChunksAndEvictsAtCapacity(t *testing.T) {
	tests := []struct {
		name string
		rows []string
		want []string
	}{
		{
			name: "oldest chunks are evicted before newer chunks",
			rows: []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"},
			want: []string{"cccc", "dddd", "eeee", "ffff"},
		},
		{
			name: "rows preserve append order across chunk boundaries",
			rows: []string{"0000", "1111", "2222", "3333"},
			want: []string{"0000", "1111", "2222", "3333"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := NewHistory(HistoryConfig{MaxRows: 4, ChunkRows: 2})
			for _, text := range tt.rows {
				history.Append(historyRow(text))
			}

			view := history.View()
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
			history.Append(first)
			history.Append(historyRow("BBBB"))
			view := history.View()
			if got, want := view.ChunkCount(), 1; got != want {
				t.Fatalf("sealed chunk count = %d, want %d", got, want)
			}

			first[0].Rune = 'X'
			history.Append(historyRow("CCCC"))

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
			history.Append(historyRow("AAAA"))
			history.Append(historyRow("BBBB"))
			before := history.View()

			history.Append(historyRow("CCCC"))
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
