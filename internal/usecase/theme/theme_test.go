package theme

import (
	"bytes"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestParseXColor(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want renderer.RGB
		ok   bool
	}{
		{name: "short rgb", in: "rgb:12/ab/FF", want: renderer.RGB{R: 0x12, G: 0xab, B: 0xff}, ok: true},
		{name: "wide rgb uses high byte", in: "rgb:1234/abcd/FFFF", want: renderer.RGB{R: 0x12, G: 0xab, B: 0xff}, ok: true},
		{name: "hex", in: "#01aBff", want: renderer.RGB{R: 0x01, G: 0xab, B: 0xff}, ok: true},
		{name: "mixed width rejected", in: "rgb:12/abcd/ff", ok: false},
		{name: "garbage rejected", in: "not-a-color", ok: false},
		{name: "bad hex rejected", in: "#xyzxyz", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseXColor(tt.in)
			if ok != tt.ok {
				t.Fatalf("ok=%v want %v", ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Fatalf("rgb=%+v want %+v", got, tt.want)
			}
		})
	}
}

func TestScannerExtractsBELAndSTColors(t *testing.T) {
	var s Scanner
	var colors []struct {
		kind int
		rgb  renderer.RGB
	}
	var out bytes.Buffer

	s.Scan([]byte("a\x1b]10;#112233\x07b\x1b]11;rgb:4455/6677/8899\x1b\\c"), func(kind int, rgb renderer.RGB) {
		colors = append(colors, struct {
			kind int
			rgb  renderer.RGB
		}{kind: kind, rgb: rgb})
	}, func(b []byte) { out.Write(b) })

	if out.String() != "abc" {
		t.Fatalf("passthrough=%q want %q", out.String(), "abc")
	}
	if len(colors) != 2 {
		t.Fatalf("colors len=%d want 2", len(colors))
	}
	if colors[0].kind != 10 || colors[0].rgb != (renderer.RGB{R: 0x11, G: 0x22, B: 0x33}) {
		t.Fatalf("first color=%+v", colors[0])
	}
	if colors[1].kind != 11 || colors[1].rgb != (renderer.RGB{R: 0x44, G: 0x66, B: 0x88}) {
		t.Fatalf("second color=%+v", colors[1])
	}
}

func TestScannerHandlesSplitSequences(t *testing.T) {
	var s Scanner
	var got []renderer.RGB
	var out bytes.Buffer
	onColor := func(kind int, rgb renderer.RGB) { got = append(got, rgb) }
	onBytes := func(b []byte) { out.Write(b) }

	s.Scan([]byte("x\x1b]10;#12"), onColor, onBytes)
	s.Scan([]byte("3456\x07y"), onColor, onBytes)

	if out.String() != "xy" {
		t.Fatalf("passthrough=%q want xy", out.String())
	}
	if len(got) != 1 || got[0] != (renderer.RGB{R: 0x12, G: 0x34, B: 0x56}) {
		t.Fatalf("colors=%+v", got)
	}
}

func TestScannerForwardsInterleavedGarbageInOrder(t *testing.T) {
	var s Scanner
	var out bytes.Buffer
	colors := 0

	s.Scan([]byte("ab\x1b]12;#112233\x07cd\x1b]10;nope\x07ef"), func(kind int, rgb renderer.RGB) { colors++ }, func(b []byte) { out.Write(b) })

	if colors != 0 {
		t.Fatalf("colors=%d want 0", colors)
	}
	want := "ab\x1b]12;#112233\x07cd\x1b]10;nope\x07ef"
	if out.String() != want {
		t.Fatalf("passthrough=%q want %q", out.String(), want)
	}
}

func TestScannerForwardsStandaloneEscapeImmediately(t *testing.T) {
	var s Scanner
	var out bytes.Buffer
	colors := 0

	s.Scan([]byte("\x1b"), func(kind int, rgb renderer.RGB) { colors++ }, func(b []byte) { out.Write(b) })

	if colors != 0 {
		t.Fatalf("colors=%d want 0", colors)
	}
	if out.String() != "\x1b" {
		t.Fatalf("passthrough=%q want ESC", out.String())
	}
}

func TestScannerDoesNotSplitSGRMouseReport(t *testing.T) {
	var s Scanner
	var chunks [][]byte
	colors := 0

	in := []byte("\x1b[<0;1;1M")
	s.Scan(in, func(kind int, rgb renderer.RGB) { colors++ }, func(b []byte) {
		chunks = append(chunks, append([]byte(nil), b...))
	})

	if colors != 0 {
		t.Fatalf("colors=%d want 0", colors)
	}
	if len(chunks) != 1 {
		t.Fatalf("chunks=%d want 1: %q", len(chunks), chunks)
	}
	if !bytes.Equal(chunks[0], in) {
		t.Fatalf("chunk=%q want %q", chunks[0], in)
	}
}

func TestScannerDoesNotSplitArrowKey(t *testing.T) {
	var s Scanner
	var chunks [][]byte

	in := []byte("\x1b[A")
	s.Scan(in, func(kind int, rgb renderer.RGB) {}, func(b []byte) {
		chunks = append(chunks, append([]byte(nil), b...))
	})

	if len(chunks) != 1 {
		t.Fatalf("chunks=%d want 1: %q", len(chunks), chunks)
	}
	if !bytes.Equal(chunks[0], in) {
		t.Fatalf("chunk=%q want %q", chunks[0], in)
	}
}

func TestScannerKeepsMouseReportContiguousAroundColorResponse(t *testing.T) {
	var s Scanner
	var chunks [][]byte
	var colors []struct {
		kind int
		rgb  renderer.RGB
	}

	s.Scan([]byte("abc\x1b[<64;5;5Mdef\x1b]11;rgb:11/22/33\x07ghi"), func(kind int, rgb renderer.RGB) {
		colors = append(colors, struct {
			kind int
			rgb  renderer.RGB
		}{kind: kind, rgb: rgb})
	}, func(b []byte) {
		chunks = append(chunks, append([]byte(nil), b...))
	})

	if len(colors) != 1 || colors[0].kind != 11 || colors[0].rgb != (renderer.RGB{R: 0x11, G: 0x22, B: 0x33}) {
		t.Fatalf("colors=%+v", colors)
	}

	var joined bytes.Buffer
	for _, c := range chunks {
		joined.Write(c)
	}
	if want := "abc\x1b[<64;5;5Mdef" + "ghi"; joined.String() != want {
		t.Fatalf("joined=%q want %q", joined.String(), want)
	}

	if len(chunks) == 0 || string(chunks[0]) != "abc\x1b[<64;5;5Mdef" {
		t.Fatalf("first chunk=%q want %q (must stay contiguous)", chunks[0], "abc\x1b[<64;5;5Mdef")
	}
}

func TestScannerFlushesOverflowingPartialQueue(t *testing.T) {
	var s Scanner
	var out bytes.Buffer

	s.Scan(append([]byte("\x1b]10;"), bytes.Repeat([]byte("a"), 70)...), func(kind int, rgb renderer.RGB) {}, func(b []byte) { out.Write(b) })
	s.Scan([]byte("Z"), func(kind int, rgb renderer.RGB) {}, func(b []byte) { out.Write(b) })

	if out.Len() == 0 || !bytes.Contains(out.Bytes(), []byte("\x1b]10;")) || !bytes.HasSuffix(out.Bytes(), []byte("Z")) {
		t.Fatalf("overflow passthrough=%q", out.String())
	}
}

func TestDimStyle(t *testing.T) {
	theme := Theme{Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 10, G: 20, B: 30}, HasFG: true, HasBG: true, Known: true, TrueColor: true}
	tests := []struct {
		name string
		in   renderer.Style
		want renderer.Style
	}{
		{
			name: "default foreground and background dim to theme colors",
			in:   renderer.DefaultStyle(),
			want: renderer.Style{
				Foreground:       -1,
				Background:       -1,
				HasForegroundRGB: true,
				ForegroundRGB:    Blend(theme.Foreground, theme.Background, 0.35),
				HasBackgroundRGB: true,
				BackgroundRGB:    theme.Background,
			},
		},
		{
			name: "indexed foreground is mapped before dimming",
			in:   renderer.Style{Foreground: 196, Background: -1},
			want: renderer.Style{
				Foreground:       196,
				Background:       -1,
				HasForegroundRGB: true,
				ForegroundRGB:    Blend(renderer.RGB{R: 255, G: 0, B: 0}, theme.Background, 0.35),
				HasBackgroundRGB: true,
				BackgroundRGB:    theme.Background,
			},
		},
		{
			name: "indexed foregrounds remain distinct",
			in:   renderer.Style{Foreground: 46, Background: -1},
			want: renderer.Style{
				Foreground:       46,
				Background:       -1,
				HasForegroundRGB: true,
				ForegroundRGB:    Blend(renderer.RGB{R: 0, G: 255, B: 0}, theme.Background, 0.35),
				HasBackgroundRGB: true,
				BackgroundRGB:    theme.Background,
			},
		},
		{
			name: "indexed background is mapped before dimming",
			in:   renderer.Style{Foreground: -1, Background: 21},
			want: renderer.Style{
				Foreground:       -1,
				Background:       21,
				HasForegroundRGB: true,
				ForegroundRGB:    Blend(theme.Foreground, theme.Background, 0.35),
				HasBackgroundRGB: true,
				BackgroundRGB:    Blend(renderer.RGB{R: 0, G: 0, B: 255}, theme.Background, 0.35),
			},
		},
		{
			name: "rgb foreground and background are dimmed",
			in: renderer.Style{
				Foreground:       -1,
				Background:       -1,
				HasForegroundRGB: true,
				ForegroundRGB:    renderer.RGB{R: 100, G: 50, B: 25},
				HasBackgroundRGB: true,
				BackgroundRGB:    renderer.RGB{R: 20, G: 100, B: 180},
			},
			want: renderer.Style{
				Foreground:       -1,
				Background:       -1,
				HasForegroundRGB: true,
				ForegroundRGB:    Blend(renderer.RGB{R: 100, G: 50, B: 25}, theme.Background, 0.35),
				HasBackgroundRGB: true,
				BackgroundRGB:    Blend(renderer.RGB{R: 20, G: 100, B: 180}, theme.Background, 0.35),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DimStyle(tt.in, theme)
			if !got.Equal(tt.want) {
				t.Fatalf("DimStyle()=%+v want %+v", got, tt.want)
			}
		})
	}
}

func TestStyleHelpersFallbackAndThemed(t *testing.T) {
	unknown := Theme{}
	if got := StatusBarStyle(unknown); !got.Equal(renderer.DefaultStyle()) {
		t.Fatalf("status fallback=%+v", got)
	}
	if got := BorderStyle(Theme{Known: true}); !got.Equal(renderer.DefaultStyle()) {
		t.Fatalf("border fallback=%+v", got)
	}
	inverse := renderer.DefaultStyle()
	inverse.Inverse = true
	if got := AccentStyle(unknown); !got.Equal(inverse) {
		t.Fatalf("accent fallback=%+v want inverse", got)
	}
	if got := SelectionStyle(unknown); !got.Equal(inverse) {
		t.Fatalf("selection fallback=%+v want inverse", got)
	}

	theme := Theme{Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 10, G: 20, B: 30}, HasFG: true, HasBG: true, Known: true, TrueColor: true}
	if got, want := Blend(renderer.RGB{R: 0, G: 0, B: 0}, renderer.RGB{R: 100, G: 200, B: 255}, 0.25), (renderer.RGB{R: 25, G: 50, B: 64}); got != want {
		t.Fatalf("blend=%+v want %+v", got, want)
	}
	status := StatusBarStyle(theme)
	if !status.HasBackgroundRGB || status.BackgroundRGB != (renderer.RGB{R: 33, G: 42, B: 50}) || !status.HasForegroundRGB || status.ForegroundRGB != theme.Foreground {
		t.Fatalf("status themed=%+v", status)
	}
	accent := AccentStyle(theme)
	if accent.Inverse || !accent.HasBackgroundRGB || accent.BackgroundRGB != (renderer.RGB{R: 58, G: 65, B: 73}) {
		t.Fatalf("accent themed=%+v", accent)
	}
	border := BorderStyle(theme)
	if !border.HasForegroundRGB || border.ForegroundRGB != (renderer.RGB{R: 124, G: 128, B: 132}) {
		t.Fatalf("border themed=%+v", border)
	}
}
