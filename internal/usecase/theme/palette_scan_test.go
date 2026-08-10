package theme

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"

	renderer "github.com/bnema/vev-vt"
)

type paletteEvent struct {
	slot int
	rgb  renderer.RGB
}

func TestScannerExtractsPaletteResponses(t *testing.T) {
	tests := []struct {
		name        string
		chunks      []string
		wantPalette []paletteEvent
		wantColors  []int
		wantOut     string
	}{
		{
			name:        "BEL hash and ST rgb",
			chunks:      []string{"a\x1b]4;0;#112233\x07b\x1b]4;15;rgb:4455/6677/8899\x1b\\c"},
			wantPalette: []paletteEvent{{slot: 0, rgb: renderer.RGB{R: 0x11, G: 0x22, B: 0x33}}, {slot: 15, rgb: renderer.RGB{R: 0x44, G: 0x66, B: 0x88}}},
			wantOut:     "abc",
		},
		{
			name:        "split prefix body and terminator",
			chunks:      []string{"a\x1b]4;", "1;rgb:12", "34/abcd/ffff\x1b", "\\b"},
			wantPalette: []paletteEvent{{slot: 1, rgb: renderer.RGB{R: 0x12, G: 0xab, B: 0xff}}},
			wantOut:     "ab",
		},
		{
			name:        "interleaved defaults and multiple replies",
			chunks:      []string{"a\x1b]10;#010203\x07\x1b]4;2;#102030\x07\x1b]11;#040506\x07\x1b]4;2;rgb:aa/bb/cc\x07z"},
			wantPalette: []paletteEvent{{slot: 2, rgb: renderer.RGB{R: 0x10, G: 0x20, B: 0x30}}, {slot: 2, rgb: renderer.RGB{R: 0xaa, G: 0xbb, B: 0xcc}}},
			wantColors:  []int{10, 11},
			wantOut:     "az",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s Scanner
			var palette []paletteEvent
			var colors []int
			var out bytes.Buffer
			for _, chunk := range tt.chunks {
				s.Scan([]byte(chunk), func(kind int, rgb renderer.RGB) {
					colors = append(colors, kind)
				}, func(slot int, rgb renderer.RGB) {
					palette = append(palette, paletteEvent{slot: slot, rgb: rgb})
				}, func(bool) {}, func(b []byte) {
					out.Write(b)
				})
			}
			if !reflect.DeepEqual(palette, tt.wantPalette) {
				t.Fatalf("palette=%+v want %+v", palette, tt.wantPalette)
			}
			if !reflect.DeepEqual(colors, tt.wantColors) {
				t.Fatalf("colors=%v want %v", colors, tt.wantColors)
			}
			if out.String() != tt.wantOut {
				t.Fatalf("passthrough=%q want %q", out.String(), tt.wantOut)
			}
		})
	}
}

func TestScannerReportsEveryValidPaletteSlot(t *testing.T) {
	var data bytes.Buffer
	for slot := 0; slot < 16; slot++ {
		data.WriteString("\x1b]4;")
		data.WriteString(strconv.Itoa(slot))
		data.WriteString(";#010203\x07")
	}

	var s Scanner
	var slots []int
	var out bytes.Buffer
	s.Scan(data.Bytes(), func(int, renderer.RGB) {}, func(slot int, rgb renderer.RGB) {
		if rgb != (renderer.RGB{R: 1, G: 2, B: 3}) {
			t.Fatalf("slot %d rgb=%+v", slot, rgb)
		}
		slots = append(slots, slot)
	}, func(bool) {}, func(b []byte) {
		out.Write(b)
	})

	if want := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}; !reflect.DeepEqual(slots, want) {
		t.Fatalf("slots=%v want %v", slots, want)
	}
	if out.Len() != 0 {
		t.Fatalf("passthrough=%q want empty", out.String())
	}
}

func TestScannerForwardsMalformedPaletteResponses(t *testing.T) {
	const malformed = "\x1b]4;;#010203\x07" +
		"\x1b]4;-1;#010203\x07" +
		"\x1b]4;cat;#010203\x1b\\" +
		"\x1b]4;16;#010203\x07" +
		"\x1b]4;999999999999999999;#010203\x07" +
		"\x1b]4;3;not-a-color\x07"

	var s Scanner
	var out bytes.Buffer
	paletteCalls := 0
	s.Scan([]byte("before"+malformed+"after"), func(int, renderer.RGB) {}, func(int, renderer.RGB) {
		paletteCalls++
	}, func(bool) {}, func(b []byte) {
		out.Write(b)
	})

	if paletteCalls != 0 {
		t.Fatalf("palette callbacks=%d want 0", paletteCalls)
	}
	if got, want := out.String(), "before"+malformed+"after"; got != want {
		t.Fatalf("passthrough=%q want %q", got, want)
	}
}

func TestScannerFlushesTruncatedPaletteResponseAtPendingLimit(t *testing.T) {
	var s Scanner
	var out bytes.Buffer
	partial := append([]byte("\x1b]4;15;rgb:"), bytes.Repeat([]byte("a"), maxPending)...)
	s.Scan(partial, func(int, renderer.RGB) {}, func(int, renderer.RGB) {}, func(bool) {}, func(b []byte) {
		out.Write(b)
	})
	s.Scan([]byte("z"), func(int, renderer.RGB) {}, func(int, renderer.RGB) {}, func(bool) {}, func(b []byte) {
		out.Write(b)
	})

	if got, want := out.String(), string(partial)+"z"; got != want {
		t.Fatalf("passthrough=%q want %q", got, want)
	}
}
