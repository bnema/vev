package vt

import (
	"encoding/binary"
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

func TestChunkCodecSupportsRepresentativeHistory(t *testing.T) {
	const (
		rows  = 10_000
		width = 120
	)

	encoded, err := MarshalHistory(historyViewWithDimensions(rows, width))
	if err != nil {
		t.Fatalf("marshal representative history: %v", err)
	}
	decoded, err := UnmarshalHistory(encoded)
	if err != nil {
		t.Fatalf("unmarshal representative history: %v", err)
	}
	if got := decoded.Len(); got != rows {
		t.Fatalf("decoded rows = %d, want %d", got, rows)
	}
	for _, rowIndex := range []int{0, rows - 1} {
		if got := len(decoded.Row(rowIndex)); got != width {
			t.Fatalf("decoded row %d width = %d, want %d", rowIndex, got, width)
		}
	}
}

func TestChunkCodecRejectsDimensionsBeyondSupportedLimits(t *testing.T) {
	const (
		supportedRows  = 12_000
		supportedWidth = 160
	)
	tests := []struct {
		name string
		view HistoryView
		data []byte
	}{
		{
			name: "too many rows",
			view: historyViewWithDimensions(supportedRows+1, 0),
			data: historyPayloadWithDimensions(supportedRows+1, 0),
		},
		{
			name: "too many cells in a row",
			view: historyViewWithDimensions(1, supportedWidth+1),
			data: historyPayloadWithDimensions(1, supportedWidth+1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := MarshalHistory(tt.view); err == nil {
				t.Fatal("marshal accepted dimensions beyond the supported limits")
			}
			if _, err := UnmarshalHistory(tt.data); err == nil {
				t.Fatal("unmarshal accepted dimensions beyond the supported limits")
			}
		})
	}
}

func TestChunkCodecRejectsHostileChunkAndRowDeclarations(t *testing.T) {
	tests := []struct {
		name       string
		chunkCount int
		rowCount   int
	}{
		{name: "more than twelve thousand chunks", chunkCount: 12_001, rowCount: 1},
		{name: "more than twelve thousand rows", chunkCount: 47, rowCount: maxHistoryChunkRows},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := UnmarshalHistory(hostileHistoryDeclarations(tt.chunkCount, tt.rowCount)); err == nil {
				t.Fatal("unmarshal accepted resource-exhausting history declarations")
			}
		})
	}
}

func historyViewWithDimensions(rowCount, width int) HistoryView {
	totalRows := rowCount
	row := make([]renderer.Cell, width)
	for i := range row {
		row[i].Rune = 'x'
	}
	chunks := make([]*HistoryChunk, 0, (rowCount+maxHistoryChunkRows-1)/maxHistoryChunkRows)
	for rowCount > 0 {
		chunkRows := min(rowCount, maxHistoryChunkRows)
		rows := make([][]renderer.Cell, chunkRows)
		for i := range rows {
			rows[i] = row
		}
		chunks = append(chunks, &HistoryChunk{rows: rows})
		rowCount -= chunkRows
	}
	return HistoryView{chunks: chunks, rows: totalRows}
}

func historyPayloadWithDimensions(rowCount, width int) []byte {
	data := make([]byte, 9)
	copy(data, historyMagic)
	data[4] = historyVersion
	chunkCount := (rowCount + maxHistoryChunkRows - 1) / maxHistoryChunkRows
	binary.BigEndian.PutUint32(data[5:], uint32(chunkCount))
	for rowCount > 0 {
		chunkRows := min(rowCount, maxHistoryChunkRows)
		data = binary.BigEndian.AppendUint32(data, uint32(chunkRows))
		for range chunkRows {
			data = binary.BigEndian.AppendUint32(data, uint32(width))
			//nolint:makezero // The header prefix is retained while zeroed cell records are appended.
			data = append(data, make([]byte, width*historyCellBytes)...)
		}
		rowCount -= chunkRows
	}
	return data
}

func hostileHistoryDeclarations(chunkCount, rowCount int) []byte {
	data := make([]byte, 9, 9+chunkCount*(4+rowCount*4))
	copy(data, historyMagic)
	data[4] = historyVersion
	binary.BigEndian.PutUint32(data[5:], uint32(chunkCount))
	for range chunkCount {
		data = binary.BigEndian.AppendUint32(data, uint32(rowCount))
		for range rowCount {
			data = binary.BigEndian.AppendUint32(data, 0)
		}
	}
	return data
}

func TestHistoryPreflightMatchesUnmarshalMalformedInput(t *testing.T) {
	valid, err := MarshalHistory(historyViewWithDimensions(1, 1))
	if err != nil {
		t.Fatalf("marshal valid history: %v", err)
	}
	invalidRune := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(invalidRune[17:21], ^uint32(0))
	invalidUnderlineStyle := append([]byte(nil), valid...)
	invalidUnderlineStyle[17+29] = byte(renderer.UnderlineDashed + 1)

	tests := []struct {
		name  string
		data  []byte
		valid bool
	}{
		{name: "valid", data: valid, valid: true},
		{name: "truncated cell", data: valid[:len(valid)-1]},
		{name: "trailing byte", data: append(append([]byte(nil), valid...), 0)},
		{name: "invalid rune", data: invalidRune},
		{name: "invalid underline style", data: invalidUnderlineStyle},
		{name: "aggregate row budget", data: hostileHistoryDeclarations(47, maxHistoryChunkRows)},
		{name: "zero row count", data: []byte{'V', 'T', 'H', '1', historyVersion, 0, 0, 0, 1, 0, 0, 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, preflightErr := PreflightHistoryBlob(tt.data)
			_, unmarshalErr := UnmarshalHistory(tt.data)
			if (preflightErr == nil) != tt.valid || (unmarshalErr == nil) != tt.valid {
				t.Fatalf("preflight error = %v, unmarshal error = %v, want valid = %t", preflightErr, unmarshalErr, tt.valid)
			}
		})
	}
}

func FuzzHistoryPreflightMatchesUnmarshal(f *testing.F) {
	valid, err := MarshalHistory(historyViewWithDimensions(1, 1))
	if err != nil {
		f.Fatalf("marshal valid history: %v", err)
	}
	invalidRune := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(invalidRune[17:21], ^uint32(0))
	f.Add(valid)
	f.Add(invalidRune)
	f.Add([]byte("VTH1\x01\x00\x00\x00\x00"))
	f.Add([]byte("not a history payload"))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, preflightErr := PreflightHistoryBlob(data)
		_, unmarshalErr := UnmarshalHistory(data)
		if (preflightErr == nil) != (unmarshalErr == nil) {
			t.Fatalf("preflight error = %v, unmarshal error = %v", preflightErr, unmarshalErr)
		}
	})
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
