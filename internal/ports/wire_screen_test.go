package ports

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

func screenGoldenMessages() []struct {
	name string
	msg  ScreenUpdate
	hex  string
} {
	defaultCell := func(r rune) renderer.Cell {
		return renderer.Cell{Rune: r, Style: renderer.DefaultStyle()}
	}
	return []struct {
		name string
		msg  ScreenUpdate
		hex  string
	}{
		{
			name: "initial 1x1 snapshot",
			msg: ScreenUpdate{
				NewStateNum: 1, Kind: ScreenUpdateSnapshot, Size: domain.Size{Cols: 1, Rows: 1},
				Cursor: ScreenCursor{Visible: true},
				Spans:  []ScreenSpan{{Cells: []renderer.Cell{defaultCell(' ')}}},
			},
			hex: "000000000000000000000000000000010000000000000000010001000100000000000100000100010000ffffffffffff0000000000000000000000000000000100010000000121",
		},
		{
			name: "explicit zero cursor style",
			msg: ScreenUpdate{
				NewStateNum: 1, Kind: ScreenUpdateSnapshot, Size: domain.Size{Cols: 1, Rows: 1},
				Cursor: ScreenCursor{Visible: true, StyleSet: true},
				Spans:  []ScreenSpan{{Cells: []renderer.Cell{defaultCell(' ')}}},
			},
			hex: "000000000000000000000000000000010000000000000000010001000100000000000300000100010000ffffffffffff0000000000000000000000000000000100010000000121",
		},
		{
			name: "later base-zero snapshot",
			msg: ScreenUpdate{
				NewStateNum: 9, Kind: ScreenUpdateSnapshot, Size: domain.Size{Cols: 1, Rows: 1},
				Cursor: ScreenCursor{Visible: true},
				Spans:  []ScreenSpan{{Cells: []renderer.Cell{defaultCell(' ')}}},
			},
			hex: "000000000000000000000000000000090000000000000000010001000100000000000100000100010000ffffffffffff0000000000000000000000000000000100010000000121",
		},
		{
			name: "cursor-only delta",
			msg: ScreenUpdate{
				BaseStateNum: 1, NewStateNum: 2, EchoAck: 7, Kind: ScreenUpdateDelta,
				Size: domain.Size{Cols: 1, Rows: 1}, Cursor: ScreenCursor{Visible: true, StyleSet: true},
			},
			hex: "00000000000000010000000000000002000000000000000702000100010000000000030000000000",
		},
		{
			name: "upward scroll",
			msg: ScreenUpdate{
				BaseStateNum: 1, NewStateNum: 2, Kind: ScreenUpdateDelta, Size: domain.Size{Cols: 2, Rows: 2},
				Cursor: ScreenCursor{Visible: true}, Scroll: &ScreenScroll{Top: 0, Height: 2, Count: 1},
				Spans: []ScreenSpan{{Y: 1, Cells: []renderer.Cell{defaultCell('a'), defaultCell('b')}}},
			},
			hex: "000000000000000100000000000000020000000000000000020002000200000000000101000100010000000200010000ffffffffffff000000000000000000000001000000020001000000026263",
		},
		{
			name: "unicode and wide continuation",
			msg: ScreenUpdate{
				BaseStateNum: 1, NewStateNum: 2, Kind: ScreenUpdateDelta, Size: domain.Size{Cols: 3, Rows: 1},
				Cursor: ScreenCursor{Visible: true},
				Spans:  []ScreenSpan{{Cells: []renderer.Cell{defaultCell('界'), {Continuation: true, Style: renderer.DefaultStyle()}, defaultCell('x')}}},
			},
			hex: "000000000000000100000000000000020000000000000000020003000100000000000100000100010000ffffffffffff00000000000000000000000000000003000100000003cdea010079",
		},
	}
}

func TestScreenUpdateStateNumbers(t *testing.T) {
	snapshot := screenGoldenMessages()[0].msg
	delta := screenGoldenMessages()[3].msg
	valid := []ScreenUpdate{
		snapshot,
		func() ScreenUpdate {
			m := snapshot
			m.NewStateNum = 9
			return m
		}(),
		delta,
	}
	for _, m := range valid {
		wire, err := MarshalScreenUpdate(m)
		if err != nil {
			t.Fatalf("valid %d state numbers: %v", m.Kind, err)
		}
		if _, err := UnmarshalScreenUpdate(wire); err != nil {
			t.Fatalf("valid %d state numbers decode: %v", m.Kind, err)
		}
	}

	invalid := []struct {
		name string
		kind ScreenUpdateKind
		base uint64
		new  uint64
	}{
		{"snapshot nonzero base", ScreenUpdateSnapshot, 1, 1},
		{"snapshot zero new", ScreenUpdateSnapshot, 0, 0},
		{"delta zero base", ScreenUpdateDelta, 0, 1},
		{"delta zero new", ScreenUpdateDelta, 1, 0},
		{"delta equal", ScreenUpdateDelta, 1, 1},
		{"delta gap", ScreenUpdateDelta, 1, 3},
		{"delta overflow", ScreenUpdateDelta, ^uint64(0), 0},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			m := snapshot
			if tt.kind == ScreenUpdateDelta {
				m = delta
			}
			m.BaseStateNum, m.NewStateNum = tt.base, tt.new
			if _, err := MarshalScreenUpdate(m); !errors.Is(err, ErrInvalidScreenUpdate) {
				t.Fatalf("marshal err = %v", err)
			}

			validWire, err := MarshalScreenUpdate(map[ScreenUpdateKind]ScreenUpdate{
				ScreenUpdateSnapshot: snapshot,
				ScreenUpdateDelta:    delta,
			}[tt.kind])
			if err != nil {
				t.Fatal(err)
			}
			binary.BigEndian.PutUint64(validWire[0:8], tt.base)
			binary.BigEndian.PutUint64(validWire[8:16], tt.new)
			if _, err := UnmarshalScreenUpdate(validWire); !errors.Is(err, ErrInvalidScreenUpdate) {
				t.Fatalf("unmarshal err = %v", err)
			}
		})
	}
}

func TestScreenUpdateGoldens(t *testing.T) {
	for _, tt := range screenGoldenMessages() {
		t.Run(tt.name, func(t *testing.T) {
			want, err := hex.DecodeString(tt.hex)
			if err != nil {
				t.Fatal(err)
			}
			got, err := MarshalScreenUpdate(tt.msg)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("wire = %x, want %x", got, want)
			}
			decoded, err := UnmarshalScreenUpdate(want)
			if err != nil {
				t.Fatal(err)
			}
			assertScreenEqual(t, tt.msg, decoded)
		})
	}
}

func assertScreenEqual(t *testing.T, want, got ScreenUpdate) {
	t.Helper()
	if want.BaseStateNum != got.BaseStateNum || want.NewStateNum != got.NewStateNum || want.EchoAck != got.EchoAck || want.Kind != got.Kind || want.Size != got.Size || want.Cursor != got.Cursor || len(want.Spans) != len(got.Spans) {
		t.Fatalf("screen metadata = %+v, want %+v", got, want)
	}
	if (want.Scroll == nil) != (got.Scroll == nil) || want.Scroll != nil && *want.Scroll != *got.Scroll {
		t.Fatalf("scroll = %+v, want %+v", got.Scroll, want.Scroll)
	}
	for i := range want.Spans {
		if want.Spans[i].Y != got.Spans[i].Y || want.Spans[i].X != got.Spans[i].X || len(want.Spans[i].Cells) != len(got.Spans[i].Cells) {
			t.Fatalf("span %d = %+v, want %+v", i, got.Spans[i], want.Spans[i])
		}
		for j := range want.Spans[i].Cells {
			if !want.Spans[i].Cells[j].Equal(got.Spans[i].Cells[j]) {
				t.Fatalf("cell %d/%d = %+v, want %+v", i, j, got.Spans[i].Cells[j], want.Spans[i].Cells[j])
			}
		}
	}
}

func TestScreenUpdateStrictTruncationAndTrailing(t *testing.T) {
	for _, tt := range screenGoldenMessages() {
		data, err := hex.DecodeString(tt.hex)
		if err != nil {
			t.Fatal(err)
		}
		for n := 0; n < len(data); n++ {
			if _, err := UnmarshalScreenUpdate(data[:n]); !errors.Is(err, ErrInvalidScreenUpdate) {
				t.Fatalf("%s prefix %d: err = %v", tt.name, n, err)
			}
		}
		trailing := append(append([]byte(nil), data...), 0)
		if _, err := UnmarshalScreenUpdate(trailing); !errors.Is(err, ErrInvalidScreenUpdate) {
			t.Fatalf("%s trailing: err = %v", tt.name, err)
		}
	}
}

func TestScreenUpdateHostileFields(t *testing.T) {
	golden, _ := hex.DecodeString(screenGoldenMessages()[0].hex)
	tests := []struct {
		name string
		mut  func([]byte)
	}{
		{"zero columns", func(b []byte) { b[25], b[26] = 0, 0 }},
		{"unknown kind", func(b []byte) { b[24] = 3 }},
		{"unknown cursor flags", func(b []byte) { b[34] = 4 }},
		{"unknown style bits", func(b []byte) { b[40], b[41] = 0x08, 0x00 }},
		{"noncanonical foreground RGB", func(b []byte) { b[40], b[41] = 0, 0x80; b[42], b[43] = 0, 0 }},
		{"style ID mismatch", func(b []byte) { b[66], b[67] = 0, 1 }},
		{"run cell mismatch", func(b []byte) { b[68], b[69] = 0, 2 }},
		{"invalid Unicode", func(b []byte) { b[len(b)-1] = 0x81 }},
		{"overflow varint", func(b []byte) { b[len(b)-1] = 0x80 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := append([]byte(nil), golden...)
			tt.mut(data)
			if _, err := UnmarshalScreenUpdate(data); !errors.Is(err, ErrInvalidScreenUpdate) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	for _, tt := range []struct {
		name  string
		token []byte
	}{
		{"noncanonical varint", []byte{0xa1, 0x00}},
		{"overflow varint", append(bytes.Repeat([]byte{0xff}, 10), 0x02)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := append(append([]byte(nil), golden[:len(golden)-1]...), tt.token...)
			if _, err := UnmarshalScreenUpdate(data); !errors.Is(err, ErrInvalidScreenUpdate) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestScreenUpdateSpanAndShapeValidation(t *testing.T) {
	cell := renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()}
	base := ScreenUpdate{
		Kind: ScreenUpdateDelta, Size: domain.Size{Cols: 3, Rows: 2}, Cursor: ScreenCursor{Visible: true},
		Spans: []ScreenSpan{{Y: 0, X: 0, Cells: []renderer.Cell{cell}}, {Y: 1, X: 0, Cells: []renderer.Cell{cell}}},
	}
	tests := []struct {
		name string
		edit func(*ScreenUpdate)
	}{
		{"unordered", func(m *ScreenUpdate) { m.Spans[0].Y = 1; m.Spans[1].Y = 0 }},
		{"overlap", func(m *ScreenUpdate) { m.Spans[1].Y = 0; m.Spans[1].X = 0 }},
		{"out of bounds", func(m *ScreenUpdate) { m.Spans[1].X = 3 }},
		{"zero scroll", func(m *ScreenUpdate) { m.Scroll = &ScreenScroll{Top: 0, Height: 0, Count: 1} }},
		{"scroll count reaches height", func(m *ScreenUpdate) { m.Scroll = &ScreenScroll{Top: 0, Height: 1, Count: 1} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := base
			m.Spans = append([]ScreenSpan(nil), base.Spans...)
			tt.edit(&m)
			if _, err := MarshalScreenUpdate(m); !errors.Is(err, ErrInvalidScreenUpdate) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	tooWide := base
	tooWide.Size = domain.Size{Cols: 1, Rows: 1}
	tooWide.Spans = []ScreenSpan{{Cells: []renderer.Cell{cell, cell}}}
	if _, err := MarshalScreenUpdate(tooWide); !errors.Is(err, ErrInvalidScreenUpdate) {
		t.Fatalf("out-of-bounds span err = %v", err)
	}
	if _, err := MarshalScreenUpdate(ScreenUpdate{Kind: ScreenUpdateDelta, Size: domain.Size{Cols: 1, Rows: 1}, Cursor: ScreenCursor{}, Spans: []ScreenSpan{{Cells: []renderer.Cell{{Rune: rune(0xd800)}}}}}); !errors.Is(err, ErrInvalidScreenUpdate) {
		t.Fatalf("invalid rune err = %v", err)
	}
	if _, err := MarshalScreenUpdate(ScreenUpdate{Kind: ScreenUpdateSnapshot, Size: domain.Size{Cols: 2, Rows: 2}, Cursor: ScreenCursor{}, Spans: []ScreenSpan{{Cells: []renderer.Cell{cell, cell}}}}); !errors.Is(err, ErrInvalidScreenUpdate) {
		t.Fatalf("incomplete snapshot err = %v", err)
	}
	if _, err := MarshalScreenUpdate(ScreenUpdate{Kind: ScreenUpdateDelta, Size: domain.Size{Cols: 1, Rows: 1}, Cursor: ScreenCursor{StyleSet: true, Style: 7}, Spans: []ScreenSpan{{Cells: []renderer.Cell{cell}}}}); !errors.Is(err, ErrInvalidScreenUpdate) {
		t.Fatalf("cursor style err = %v", err)
	}
}

func TestScreenUpdateOwnership(t *testing.T) {
	golden, _ := hex.DecodeString(screenGoldenMessages()[0].hex)
	decoded, err := UnmarshalScreenUpdate(golden)
	if err != nil {
		t.Fatal(err)
	}
	golden[0] ^= 0xff
	golden[len(golden)-1] ^= 0xff
	want, err := MarshalScreenUpdate(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, mustDecodeHex(t, screenGoldenMessages()[0].hex)) {
		t.Fatal("decoded screen retained transport payload")
	}
}

func TestScreenUpdateLimits(t *testing.T) {
	if _, err := UnmarshalScreenUpdate(make([]byte, MaxFrameLen)); !errors.Is(err, ErrScreenUpdateTooLarge) {
		t.Fatalf("oversize err = %v", err)
	}
	tooMany := screenGoldenMessages()[0].msg
	tooMany.Spans = make([]ScreenSpan, screenSpanLimit+1)
	if _, err := MarshalScreenUpdate(tooMany); !errors.Is(err, ErrInvalidScreenUpdate) {
		t.Fatalf("span limit err = %v", err)
	}
}

func TestScreenUpdateScreenAreaLimit(t *testing.T) {
	boundary := ScreenUpdate{
		BaseStateNum: 1,
		NewStateNum:  2,
		Kind:         ScreenUpdateDelta,
		Size:         domain.Size{Cols: 512, Rows: 512},
		Cursor:       ScreenCursor{Visible: true},
	}
	wire, err := MarshalScreenUpdate(boundary)
	if err != nil {
		t.Fatalf("boundary marshal: %v", err)
	}
	if _, err := UnmarshalScreenUpdate(wire); err != nil {
		t.Fatalf("boundary unmarshal: %v", err)
	}

	over := boundary
	over.Size = domain.Size{Cols: 513, Rows: 512}
	if _, err := MarshalScreenUpdate(over); !errors.Is(err, ErrInvalidScreenUpdate) {
		t.Fatalf("over-cap marshal err = %v", err)
	}

	// Include a span descriptor and enough cell tokens that an unchecked
	// decoder would reach its per-span cell allocation.
	hostile := make([]byte, screenHeaderLen+8+4+513)
	hostile[24] = byte(ScreenUpdateDelta)
	binary.BigEndian.PutUint16(hostile[25:27], 513)
	binary.BigEndian.PutUint16(hostile[27:29], 512)
	binary.BigEndian.PutUint16(hostile[38:40], 1)
	binary.BigEndian.PutUint16(hostile[40:42], 0) // y
	binary.BigEndian.PutUint16(hostile[42:44], 0) // x
	binary.BigEndian.PutUint16(hostile[44:46], 513)
	binary.BigEndian.PutUint16(hostile[46:48], 1)
	binary.BigEndian.PutUint16(hostile[48:50], 0) // style ID
	binary.BigEndian.PutUint16(hostile[50:52], 513)
	if _, err := UnmarshalScreenUpdate(hostile); !errors.Is(err, ErrInvalidScreenUpdate) {
		t.Fatalf("over-cap hostile header err = %v", err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, err := UnmarshalScreenUpdate(hostile); !errors.Is(err, ErrInvalidScreenUpdate) {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("over-cap hostile header allocations = %v, want 0", allocs)
	}
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func FuzzScreenUpdate(f *testing.F) {
	for _, tt := range screenGoldenMessages() {
		b, err := hex.DecodeString(tt.hex)
		if err != nil {
			f.Fatalf("seed %s: %v", tt.name, err)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := UnmarshalScreenUpdate(data)
		if err != nil {
			return
		}
		encoded, err := MarshalScreenUpdate(decoded)
		if err != nil {
			t.Fatal(err)
		}
		roundtrip, err := UnmarshalScreenUpdate(encoded)
		if err != nil {
			t.Fatal(err)
		}
		assertScreenEqual(t, decoded, roundtrip)
	})
}
