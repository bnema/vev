package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestSnapshotCodecsPreserveEveryTerminalCellAcrossSealedTailAndVisible(t *testing.T) {
	indexed := renderer.Style{
		Bold:              true,
		Italic:            true,
		Inverse:           true,
		Attrs:             renderer.AttrDim | renderer.AttrUnderline | renderer.AttrBlink | renderer.AttrStrikethrough,
		Foreground:        196,
		Background:        17,
		HasUnderlineColor: true,
		UnderlineColor:    203,
	}
	rgb := renderer.Style{
		Bold:                 true,
		Italic:               true,
		Inverse:              true,
		Attrs:                renderer.AttrDim | renderer.AttrUnderline | renderer.AttrBlink | renderer.AttrStrikethrough,
		HasForegroundRGB:     true,
		ForegroundRGB:        renderer.RGB{R: 1, G: 2, B: 3},
		HasBackgroundRGB:     true,
		BackgroundRGB:        renderer.RGB{R: 4, G: 5, B: 6},
		HasUnderlineColorRGB: true,
		UnderlineColorRGB:    renderer.RGB{R: 7, G: 8, B: 9},
	}
	underlines := []renderer.UnderlineStyle{
		renderer.UnderlineNone,
		renderer.UnderlineSingle,
		renderer.UnderlineDouble,
		renderer.UnderlineCurly,
		renderer.UnderlineDotted,
		renderer.UnderlineDashed,
	}
	makeRow := func(r rune, style renderer.Style) []renderer.Cell {
		style.UnderlineStyle = underlines[int(r)%len(underlines)]
		return []renderer.Cell{
			{Rune: r, Style: style},
			{Rune: ' ', Style: renderer.DefaultStyle()},
			{Rune: '界', Style: style},
			{Continuation: true, Style: style},
		}
	}

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "history view retains sealed chunks and mutable tail",
			run: func(t *testing.T) {
				history := NewHistory(HistoryConfig{MaxRows: 8, ChunkRows: 2})
				// A through F exercise every underline style; G remains in the
				// mutable tail so both history ownership paths are encoded.
				want := [][]renderer.Cell{
					makeRow('A', indexed), makeRow('B', rgb), makeRow('C', indexed),
					makeRow('D', rgb), makeRow('E', indexed), makeRow('F', rgb), makeRow('G', indexed),
				}
				for _, row := range want {
					requireHistoryAppend(t, history, row)
				}
				if got, wantChunks := history.View().ChunkCount(), 4; got != wantChunks {
					t.Fatalf("chunk count = %d, want three sealed chunks plus copied tail %d", got, wantChunks)
				}
				encoded, err := MarshalHistory(history.View())
				if err != nil {
					t.Fatalf("MarshalHistory: %v", err)
				}
				got, err := UnmarshalHistory(encoded)
				if err != nil {
					t.Fatalf("UnmarshalHistory: %v", err)
				}
				assertHistoryRowsEqual(t, got, want)
			},
		},
		{
			name: "visible frame retains blanks wide heads continuations and styles",
			run: func(t *testing.T) {
				want := renderer.NewFrame(4, 2)
				copy(want.Row(0), makeRow('D', rgb))
				copy(want.Row(1), makeRow('E', indexed))
				encoded, err := MarshalVisible(want)
				if err != nil {
					t.Fatalf("MarshalVisible: %v", err)
				}
				got, err := UnmarshalVisible(encoded)
				if err != nil {
					t.Fatalf("UnmarshalVisible: %v", err)
				}
				for y := range want.Height {
					for x, cell := range want.Row(y) {
						if !got.At(x, y).Equal(cell) {
							t.Fatalf("cell (%d,%d) = %#v, want %#v", x, y, got.At(x, y), cell)
						}
					}
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
