package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestChunkCodecRoundTripsLosslessCellsAndStyles(t *testing.T) {
	indexed := renderer.Style{
		Bold:              true,
		Attrs:             renderer.AttrDim | renderer.AttrUnderline | renderer.AttrStrikethrough,
		Foreground:        123,
		Background:        45,
		UnderlineStyle:    renderer.UnderlineDashed,
		HasUnderlineColor: true,
		UnderlineColor:    201,
	}
	rgb := renderer.Style{
		Italic:               true,
		Inverse:              true,
		Attrs:                renderer.AttrBlink,
		HasForegroundRGB:     true,
		ForegroundRGB:        renderer.RGB{R: 1, G: 2, B: 3},
		HasBackgroundRGB:     true,
		BackgroundRGB:        renderer.RGB{R: 4, G: 5, B: 6},
		UnderlineStyle:       renderer.UnderlineCurly,
		HasUnderlineColorRGB: true,
		UnderlineColorRGB:    renderer.RGB{R: 7, G: 8, B: 9},
	}
	tests := []struct {
		name string
		rows [][]renderer.Cell
	}{
		{
			name: "indexed RGB blank and wide continuation cells are exact",
			rows: [][]renderer.Cell{
				{
					{Rune: 'I', Style: indexed},
					{Rune: 'R', Style: rgb},
					{Rune: ' ', Style: renderer.DefaultStyle()},
					{Rune: '界', Style: rgb},
					{Continuation: true, Style: rgb},
				},
				{
					{Rune: ' ', Style: renderer.DefaultStyle()},
					{Rune: 'X', Style: indexed},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := NewHistory(HistoryConfig{MaxRows: 8, ChunkRows: 2})
			for _, row := range tt.rows {
				history.Append(row)
			}

			encoded, err := MarshalHistory(history.View())
			if err != nil {
				t.Fatalf("marshal history: %v", err)
			}
			decoded, err := UnmarshalHistory(encoded)
			if err != nil {
				t.Fatalf("unmarshal history: %v", err)
			}
			assertHistoryRowsEqual(t, decoded, tt.rows)
		})
	}
}

func TestChunkCodecRejectsTruncatedAndTrailingPayloads(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: 4, ChunkRows: 2})
	history.Append(historyRow("AAAA"))
	history.Append(historyRow("BBBB"))
	encoded, err := MarshalHistory(history.View())
	if err != nil {
		t.Fatalf("marshal history: %v", err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "truncated prefix", data: encoded[:len(encoded)-1]},
		{name: "trailing byte", data: append(append([]byte(nil), encoded...), 0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := UnmarshalHistory(tt.data); err == nil {
				t.Fatal("unmarshal accepted malformed history payload")
			}
		})
	}
}

func assertHistoryRowsEqual(t *testing.T, view HistoryView, want [][]renderer.Cell) {
	t.Helper()
	if got := view.Len(); got != len(want) {
		t.Fatalf("row count = %d, want %d", got, len(want))
	}
	for y, wantRow := range want {
		gotRow := view.Row(y)
		if len(gotRow) != len(wantRow) {
			t.Fatalf("row %d width = %d, want %d", y, len(gotRow), len(wantRow))
		}
		for x, wantCell := range wantRow {
			if !gotRow[x].Equal(wantCell) {
				t.Fatalf("cell (%d,%d) = %#v, want %#v", x, y, gotRow[x], wantCell)
			}
		}
	}
}
