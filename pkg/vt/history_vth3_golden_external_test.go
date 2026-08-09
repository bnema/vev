package vt_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/bnema/vev/pkg/vt"
	"github.com/bnema/vev/pkg/vtcore"
)

func TestExternalVTH3GoldenVectors(t *testing.T) {
	vectors := []struct {
		name  string
		hex   string
		stats vt.DecodeStats
	}{
		{
			name:  "empty",
			hex:   "565448310300000000000000000000002a",
			stats: vt.DecodeStats{},
		},
		{
			name:  "plain and continuation cells",
			hex:   "565448310300000001000000000000002a0000000100000000000000290000000200000041000000ffffffffffffffffffffffffffffffff00000000000000ffffffffffffffff00000000000000010000ffffffffffffffffffffffffffffffff00000000000000ffffffffffffffff0000000000000200",
			stats: vt.DecodeStats{Chunks: 1, Rows: 1, Cells: 2, Styles: 2, Bytes: 82},
		},
		{
			name:  "all style fields",
			hex:   "565448310300000001000000000000002b00000001000000000000002a000000010000754cff000ffffffffffffffff9000000000000010101020304050605fffffffffffffff70708090000000101",
			stats: vt.DecodeStats{Chunks: 1, Rows: 1, Cells: 1, Styles: 1, Bytes: 41},
		},
	}

	for _, vector := range vectors {
		t.Run(vector.name, func(t *testing.T) {
			data, err := hex.DecodeString(vector.hex)
			if err != nil {
				t.Fatal(err)
			}
			stats, err := vt.PreflightHistoryBlob(data)
			if err != nil {
				t.Fatalf("PreflightHistoryBlob: %v", err)
			}
			if stats != vector.stats {
				t.Fatalf("decode stats = %#v, want %#v", stats, vector.stats)
			}

			view, err := vt.UnmarshalHistory(data)
			if err != nil {
				t.Fatalf("UnmarshalHistory: %v", err)
			}
			reencoded, err := vt.MarshalHistory(view)
			if err != nil {
				t.Fatalf("MarshalHistory: %v", err)
			}
			if !bytes.Equal(reencoded, data) {
				t.Fatalf("canonical bytes changed:\n got %x\nwant %x", reencoded, data)
			}

			switch vector.name {
			case "empty":
				if view.Len() != 0 || view.ChunkCount() != 0 || view.NextRowID() != 42 {
					t.Fatalf("empty view = len %d chunks %d next %d", view.Len(), view.ChunkCount(), view.NextRowID())
				}
			case "plain and continuation cells":
				if view.Len() != 1 || view.RowID(0) != 41 || view.Bound(0) != (vt.LineBound{End: 2}) || view.NextRowID() != 42 {
					t.Fatalf("plain view = len %d row %d bound %#v next %d", view.Len(), view.RowID(0), view.Bound(0), view.NextRowID())
				}
				row := view.Row(0)
				if len(row) != 2 || row[0].Rune != 'A' || row[1].Rune != 0 || !row[1].Continuation {
					t.Fatalf("plain row = %#v", row)
				}
			case "all style fields":
				wantStyle := vtcore.Style{
					Bold:                 true,
					Italic:               true,
					Inverse:              true,
					Attrs:                vtcore.StyleAttrs(0x000f),
					Foreground:           -7,
					Background:           257,
					HasForegroundRGB:     true,
					ForegroundRGB:        vtcore.RGB{R: 1, G: 2, B: 3},
					HasBackgroundRGB:     true,
					BackgroundRGB:        vtcore.RGB{R: 4, G: 5, B: 6},
					UnderlineStyle:       vtcore.UnderlineDashed,
					UnderlineColor:       -9,
					HasUnderlineColorRGB: true,
					UnderlineColorRGB:    vtcore.RGB{R: 7, G: 8, B: 9},
				}
				if view.Len() != 1 || view.RowID(0) != 42 || view.Bound(0) != (vt.LineBound{End: 1, Soft: true}) || view.NextRowID() != 43 {
					t.Fatalf("styled view = len %d row %d bound %#v next %d", view.Len(), view.RowID(0), view.Bound(0), view.NextRowID())
				}
				row := view.Row(0)
				if len(row) != 1 || row[0].Rune != '界' || !row[0].Style.Equal(wantStyle) {
					t.Fatalf("styled row = %#v, want %#v", row, wantStyle)
				}
			}
		})
	}
}

func TestExternalVTH3RejectsMalformedVectors(t *testing.T) {
	valid, err := hex.DecodeString("565448310300000001000000000000002a0000000100000000000000290000000200000041000000ffffffffffffffffffffffffffffffff00000000000000ffffffffffffffff00000000000000010000ffffffffffffffffffffffffffffffff00000000000000ffffffffffffffff0000000000000200")
	if err != nil {
		t.Fatal(err)
	}

	mutate := func(fn func([]byte)) []byte {
		out := append([]byte(nil), valid...)
		fn(out)
		return out
	}
	const (
		rowIDOffset = 21
		cellOffset  = 33
		boundOffset = 115
	)
	cases := []struct {
		name string
		data []byte
	}{
		{name: "truncated", data: valid[:len(valid)-1]},
		{name: "trailing bytes", data: append(append([]byte(nil), valid...), 0)},
		{name: "bad magic", data: mutate(func(data []byte) { data[0] ^= 0xff })},
		{name: "bad version", data: mutate(func(data []byte) { data[4] = 2 })},
		{name: "zero next row ID", data: mutate(func(data []byte) { clear(data[9:17]) })},
		{name: "zero row ID", data: mutate(func(data []byte) { clear(data[rowIDOffset : rowIDOffset+8]) })},
		{name: "invalid rune", data: mutate(func(data []byte) { binary.BigEndian.PutUint32(data[cellOffset:], ^uint32(0)) })},
		{name: "invalid underline style", data: mutate(func(data []byte) { data[cellOffset+29] = byte(vtcore.UnderlineDashed + 1) })},
		{name: "bound exceeds row", data: mutate(func(data []byte) { binary.BigEndian.PutUint32(data[boundOffset:], 3) })},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := vt.PreflightHistoryBlob(test.data); err == nil {
				t.Fatal("PreflightHistoryBlob accepted malformed data")
			}
			if _, err := vt.UnmarshalHistory(test.data); err == nil {
				t.Fatal("UnmarshalHistory accepted malformed data")
			}
		})
	}
	for length := 0; length < len(valid); length++ {
		if _, err := vt.UnmarshalHistory(valid[:length]); err == nil {
			t.Fatalf("accepted truncated prefix of length %d", length)
		}
	}
}
