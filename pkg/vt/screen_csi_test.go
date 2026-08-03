package vt

import (
	"strconv"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestParseCSIInts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params string
		want   []int
	}{
		{name: "empty", params: "", want: nil},
		{name: "single", params: "5", want: []int{5}},
		{name: "multiple", params: "1;2;3", want: []int{1, 2, 3}},
		{name: "leading empty token", params: ";5", want: []int{0, 5}},
		{name: "double empty token", params: "5;;3", want: []int{5, 0, 3}},
		{name: "trailing empty token", params: "5;", want: []int{5, 0}},
		{name: "question prefix", params: "?25", want: []int{25}},
		{name: "greater prefix", params: ">0;1", want: []int{0, 1}},
		{name: "prefix order", params: "?>7", want: []int{7}},
		{name: "junk token", params: "12x;4", want: []int{0, 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseCSIInts(tt.params))
		})
	}
}

func TestParseSGRInts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params string
		want   []int
	}{
		{name: "empty", params: "", want: nil},
		{name: "single", params: "1", want: []int{1}},
		{name: "multiple", params: "1;31", want: []int{1, 31}},
		{name: "leading empty token", params: ";5", want: []int{0, 5}},
		{name: "double empty token", params: "5;;3", want: []int{5, 0, 3}},
		{name: "trailing empty token", params: "5;", want: []int{5, 0}},
		{name: "indexed foreground colon", params: "38:5:196", want: []int{38, 5, 196}},
		{name: "rgb foreground colon", params: "38:2:1:2:3", want: []int{38, 2, 1, 2, 3}},
		{name: "indexed background colon", params: "48:5:10", want: []int{48, 5, 10}},
		{name: "rgb foreground empty colorspace", params: "38:2::1:2:3", want: []int{38, 2, 1, 2, 3}},
		{name: "rgb foreground explicit colorspace", params: "38:2:9:1:2:3", want: []int{38, 2, 1, 2, 3}},
		{name: "non color colon group", params: "1:2:3", want: []int{1}},
		{name: "junk token", params: "abc;31", want: []int{0, 31}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseSGRInts(tt.params))
		})
	}
}

func TestSGR(t *testing.T) {
	tests := []struct {
		name  string
		seq   string
		check func(t *testing.T, s *Screen)
	}{
		{
			name: "SGR 0 resets to default style",
			seq:  "\x1b[0m",
			check: func(t *testing.T, s *Screen) {
				if s.Style != (renderer.DefaultStyle()) {
					t.Errorf("style after SGR 0 = %+v, want default", s.Style)
				}
			},
		},
		{
			name: "SGR 1 sets bold",
			seq:  "\x1b[1m",
			check: func(t *testing.T, s *Screen) {
				if !s.Style.Bold {
					t.Error("expected bold")
				}
			},
		},
		{
			name: "SGR 7 sets inverse",
			seq:  "\x1b[7m",
			check: func(t *testing.T, s *Screen) {
				if !s.Style.Inverse {
					t.Error("expected inverse")
				}
			},
		},
		{
			name: "SGR 31 sets red foreground",
			seq:  "\x1b[31m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != 1 {
					t.Errorf("foreground = %d, want 1 (red)", s.Style.Foreground)
				}
			},
		},
		{
			name: "SGR 44 sets blue background",
			seq:  "\x1b[44m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != 4 {
					t.Errorf("background = %d, want 4 (blue)", s.Style.Background)
				}
			},
		},
		{
			name: "SGR 38;5 sets 256-color foreground",
			seq:  "\x1b[38;5;82m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != 82 {
					t.Errorf("foreground = %d, want 82", s.Style.Foreground)
				}
			},
		},
		{
			name: "SGR 48;5 sets 256-color background",
			seq:  "\x1b[48;5;200m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != 200 {
					t.Errorf("background = %d, want 200", s.Style.Background)
				}
			},
		},
		{
			name: "SGR 91 sets bright red foreground",
			seq:  "\x1b[91m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != 9 {
					t.Errorf("foreground = %d, want 9 (bright red)", s.Style.Foreground)
				}
			},
		},
		{
			name: "SGR 107 sets bright white background",
			seq:  "\x1b[107m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != 15 {
					t.Errorf("background = %d, want 15 (bright white)", s.Style.Background)
				}
			},
		},
		{
			name: "SGR 22 resets bold",
			seq:  "\x1b[1mHello\x1b[22mWorld",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Bold {
					t.Error("bold should be reset after 22")
				}
			},
		},
		{
			name: "SGR 27 resets inverse",
			seq:  "\x1b[7m\x1b[27m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Inverse {
					t.Error("inverse should be reset after 27")
				}
			},
		},
		{
			name: "SGR 39 resets foreground to default",
			seq:  "\x1b[31m\x1b[39m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != -1 {
					t.Errorf("foreground should be default after 39, got %d", s.Style.Foreground)
				}
			},
		},
		{
			name: "SGR 49 resets background to default",
			seq:  "\x1b[44m\x1b[49m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != -1 {
					t.Errorf("background should be default after 49, got %d", s.Style.Background)
				}
			},
		},
		{
			name: "SGR style is applied to written cells",
			seq:  "\x1b[1;31mX",
			check: func(t *testing.T, s *Screen) {
				c := cellAt(s, 0, 0)
				if !c.Style.Bold {
					t.Error("cell should be bold")
				}
				if c.Style.Foreground != 1 {
					t.Errorf("cell foreground = %d, want 1", c.Style.Foreground)
				}
			},
		},
		{
			name: "multiple sequential SGR sequences accumulate",
			seq:  "\x1b[31m\x1b[1m\x1b[44mX",
			check: func(t *testing.T, s *Screen) {
				c := cellAt(s, 0, 0)
				if c.Style.Foreground != 1 || !c.Style.Bold || c.Style.Background != 4 {
					t.Errorf("cell style = %+v, want fg=1 bold bg=4", c.Style)
				}
			},
		},
		{
			name: "SGR 3 sets italic on style and cell, SGR 23 clears it",
			seq:  "\x1b[3mX\x1b[23mY",
			check: func(t *testing.T, s *Screen) {
				if !cellAt(s, 0, 0).Style.Italic {
					t.Errorf("first cell italic = false, want true")
				}
				if cellAt(s, 1, 0).Style.Italic {
					t.Errorf("second cell italic = true, want false after SGR 23")
				}
				if s.Style.Italic {
					t.Errorf("current style italic = true, want false after SGR 23")
				}
			},
		},
		{
			name: "SGR preserves dim underline blink and strikethrough with resets",
			seq:  "\x1b[2;4;5;9mA\x1b[22;24;25;29mB",
			check: func(t *testing.T, s *Screen) {
				first := cellAt(s, 0, 0).Style
				if first.Attrs&(renderer.AttrDim|renderer.AttrUnderline|renderer.AttrBlink|renderer.AttrStrikethrough) != renderer.AttrDim|renderer.AttrUnderline|renderer.AttrBlink|renderer.AttrStrikethrough {
					t.Errorf("first cell attrs = %b, want dim underline blink strikethrough", first.Attrs)
				}
				second := cellAt(s, 1, 0).Style
				if second.Attrs&(renderer.AttrDim|renderer.AttrUnderline|renderer.AttrBlink|renderer.AttrStrikethrough) != 0 {
					t.Errorf("second cell attrs = %b, want resets cleared", second.Attrs)
				}
			},
		},
		{
			name: "SGR 22 resets both bold and dim",
			seq:  "\x1b[1;2m\x1b[22m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Bold || s.Style.Attrs&renderer.AttrDim != 0 {
					t.Errorf("style after SGR 22 = bold:%v attrs:%b, want neither bold nor dim", s.Style.Bold, s.Style.Attrs)
				}
			},
		},
		{
			name: "SGR underline style and color are preserved and reset",
			seq:  "\x1b[4:3;58:2::255:0:0mA\x1b[59mB",
			check: func(t *testing.T, s *Screen) {
				first := cellAt(s, 0, 0).Style
				if first.Attrs&renderer.AttrUnderline == 0 || first.UnderlineStyle != renderer.UnderlineCurly {
					t.Errorf("first underline = attrs:%b style:%d, want curly underline", first.Attrs, first.UnderlineStyle)
				}
				want := renderer.RGB{R: 255, G: 0, B: 0}
				if !first.HasUnderlineColorRGB || first.UnderlineColorRGB != want {
					t.Errorf("first underline color = rgb:%v %+v, want %+v", first.HasUnderlineColorRGB, first.UnderlineColorRGB, want)
				}
				second := cellAt(s, 1, 0).Style
				if second.HasUnderlineColorRGB || second.HasUnderlineColor {
					t.Errorf("second underline color flags = indexed:%v rgb:%v, want reset", second.HasUnderlineColor, second.HasUnderlineColorRGB)
				}
			},
		},
		{
			name: "SGR 4 colon zero clears underline",
			seq:  "\x1b[4:3mA\x1b[4:0mB",
			check: func(t *testing.T, s *Screen) {
				first := cellAt(s, 0, 0).Style
				if first.Attrs&renderer.AttrUnderline == 0 || first.UnderlineStyle != renderer.UnderlineCurly {
					t.Errorf("first underline = attrs:%b style:%d, want curly underline", first.Attrs, first.UnderlineStyle)
				}
				second := cellAt(s, 1, 0).Style
				if second.Attrs&renderer.AttrUnderline != 0 || second.UnderlineStyle != renderer.UnderlineNone {
					t.Errorf("second underline = attrs:%b style:%d, want cleared", second.Attrs, second.UnderlineStyle)
				}
			},
		},
		{
			name: "empty SGR params reset style",
			seq:  "\x1b[31m\x1b[1m\x1b[m", // empty params → reset
			check: func(t *testing.T, s *Screen) {
				if s.Style != (renderer.DefaultStyle()) {
					t.Errorf("style after empty SGR = %+v, want default", s.Style)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 3)
			s.Write([]byte(tt.seq))
			tt.check(t, s)
		})
	}
}

func TestSGRTruecolor(t *testing.T) {
	tests := []struct {
		name  string
		seq   string
		check func(t *testing.T, s *Screen)
	}{
		{
			name: "SGR 38;2 sets truecolor foreground on style and cell",
			seq:  "\x1b[38;2;12;34;56mX",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 12, G: 34, B: 56}
				if !s.Style.HasForegroundRGB || s.Style.ForegroundRGB != want {
					t.Errorf("foreground RGB = (%v, %+v), want true/%+v", s.Style.HasForegroundRGB, s.Style.ForegroundRGB, want)
				}
				if got := cellAt(s, 0, 0).Style.ForegroundRGB; got != want {
					t.Errorf("cell foreground RGB = %+v, want %+v", got, want)
				}
			},
		},
		{
			name: "SGR 38:2 sets truecolor foreground",
			seq:  "\x1b[38:2:12:34:56mX",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 12, G: 34, B: 56}
				if !s.Style.HasForegroundRGB || s.Style.ForegroundRGB != want {
					t.Errorf("foreground RGB = (%v, %+v), want true/%+v", s.Style.HasForegroundRGB, s.Style.ForegroundRGB, want)
				}
				if got := cellAt(s, 0, 0).Style.ForegroundRGB; got != want {
					t.Errorf("cell foreground RGB = %+v, want %+v", got, want)
				}
			},
		},
		{
			name: "SGR 38:2 empty color space sets truecolor foreground",
			seq:  "\x1b[38:2::12:34:56mX",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 12, G: 34, B: 56}
				if !s.Style.HasForegroundRGB || s.Style.ForegroundRGB != want {
					t.Errorf("foreground RGB = (%v, %+v), want true/%+v", s.Style.HasForegroundRGB, s.Style.ForegroundRGB, want)
				}
				if got := cellAt(s, 0, 0).Style.ForegroundRGB; got != want {
					t.Errorf("cell foreground RGB = %+v, want %+v", got, want)
				}
			},
		},
		{
			name: "SGR 38:2 color space id sets truecolor foreground",
			seq:  "\x1b[38:2:1:12:34:56mX",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 12, G: 34, B: 56}
				if !s.Style.HasForegroundRGB || s.Style.ForegroundRGB != want {
					t.Errorf("foreground RGB = (%v, %+v), want true/%+v", s.Style.HasForegroundRGB, s.Style.ForegroundRGB, want)
				}
				if got := cellAt(s, 0, 0).Style.ForegroundRGB; got != want {
					t.Errorf("cell foreground RGB = %+v, want %+v", got, want)
				}
			},
		},
		{
			name: "mixed semicolon and colon SGR groups",
			seq:  "\x1b[1;38:2:10:20:30;7mX",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 10, G: 20, B: 30}
				if !s.Style.Bold || !s.Style.Inverse {
					t.Errorf("bold/inverse = %v/%v, want true/true", s.Style.Bold, s.Style.Inverse)
				}
				if !s.Style.HasForegroundRGB || s.Style.ForegroundRGB != want {
					t.Errorf("foreground RGB = (%v, %+v), want true/%+v", s.Style.HasForegroundRGB, s.Style.ForegroundRGB, want)
				}
			},
		},
		{
			name: "truncated colon truecolor group does not consume next parameter",
			seq:  "\x1b[38:2:1:2;31m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != 1 || s.Style.HasForegroundRGB {
					t.Errorf("foreground = %d rgb:%v, want 1 false", s.Style.Foreground, s.Style.HasForegroundRGB)
				}
			},
		},
		{
			name: "SGR 48;2 sets truecolor background",
			seq:  "\x1b[48;2;200;100;50m",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 200, G: 100, B: 50}
				if !s.Style.HasBackgroundRGB || s.Style.BackgroundRGB != want {
					t.Errorf("background RGB = (%v, %+v), want true/%+v", s.Style.HasBackgroundRGB, s.Style.BackgroundRGB, want)
				}
			},
		},
		{
			name: "SGR 48:2 sets truecolor background",
			seq:  "\x1b[48:2:200:100:50m",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 200, G: 100, B: 50}
				if !s.Style.HasBackgroundRGB || s.Style.BackgroundRGB != want {
					t.Errorf("background RGB = (%v, %+v), want true/%+v", s.Style.HasBackgroundRGB, s.Style.BackgroundRGB, want)
				}
			},
		},
		{
			name: "SGR 38:5 sets 256-color foreground",
			seq:  "\x1b[38:5:82m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != 82 || s.Style.HasForegroundRGB {
					t.Errorf("foreground = %d rgb:%v, want 82 false", s.Style.Foreground, s.Style.HasForegroundRGB)
				}
			},
		},
		{
			name: "SGR 48:5 sets 256-color background",
			seq:  "\x1b[48:5:200m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != 200 || s.Style.HasBackgroundRGB {
					t.Errorf("background = %d rgb:%v, want 200 false", s.Style.Background, s.Style.HasBackgroundRGB)
				}
			},
		},
		{
			name: "truncated colon foreground index group does not consume next parameter",
			seq:  "\x1b[38:5;31m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Foreground != 1 || s.Style.HasForegroundRGB {
					t.Errorf("foreground = %d rgb:%v, want 1 false", s.Style.Foreground, s.Style.HasForegroundRGB)
				}
			},
		},
		{
			name: "truncated colon background index group does not consume next parameter",
			seq:  "\x1b[48:5;44m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.Background != 4 || s.Style.HasBackgroundRGB {
					t.Errorf("background = %d rgb:%v, want 4 false", s.Style.Background, s.Style.HasBackgroundRGB)
				}
			},
		},
		{
			name: "SGR 39 after truecolor clears truecolor foreground",
			seq:  "\x1b[38;2;12;34;56m\x1b[39m",
			check: func(t *testing.T, s *Screen) {
				if s.Style.HasForegroundRGB || s.Style.Foreground != -1 {
					t.Errorf("foreground after reset = rgb:%v index:%d, want default", s.Style.HasForegroundRGB, s.Style.Foreground)
				}
			},
		},
		{
			name: "truecolor components are clamped to 0-255",
			seq:  "\x1b[38;2;-1;300;42m",
			check: func(t *testing.T, s *Screen) {
				want := renderer.RGB{R: 0, G: 255, B: 42}
				if s.Style.ForegroundRGB != want {
					t.Errorf("foreground RGB = %+v, want %+v", s.Style.ForegroundRGB, want)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 3)
			s.Write([]byte(tt.seq))
			tt.check(t, s)
		})
	}
}

func TestSGRColorModeTransitionsClearInactiveState(t *testing.T) {
	tests := []struct {
		name          string
		seq           string
		foreground    int
		background    int
		hasForeground bool
		hasBackground bool
	}{
		{name: "foreground rgb to bright", seq: "\x1b[38;2;1;2;3m\x1b[91m", foreground: 9, background: -1},
		{name: "foreground rgb to indexed", seq: "\x1b[38;2;1;2;3m\x1b[38;5;82m", foreground: 82, background: -1},
		{name: "foreground indexed to rgb", seq: "\x1b[38;5;82m\x1b[38;2;4;5;6m", foreground: -1, background: -1, hasForeground: true},
		{name: "background rgb to bright", seq: "\x1b[48;2;1;2;3m\x1b[107m", foreground: -1, background: 15},
		{name: "background rgb to indexed", seq: "\x1b[48;2;1;2;3m\x1b[48;5;200m", foreground: -1, background: 200},
		{name: "background rgb to default", seq: "\x1b[48;2;1;2;3m\x1b[49m", foreground: -1, background: -1},
		{name: "background indexed to rgb", seq: "\x1b[48;5;200m\x1b[48;2;4;5;6m", foreground: -1, background: -1, hasBackground: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 3)
			s.Write([]byte(tt.seq))

			if s.Style.Foreground != tt.foreground || s.Style.HasForegroundRGB != tt.hasForeground {
				t.Errorf("foreground = %d rgb:%v, want %d rgb:%v", s.Style.Foreground, s.Style.HasForegroundRGB, tt.foreground, tt.hasForeground)
			}
			if s.Style.Background != tt.background || s.Style.HasBackgroundRGB != tt.hasBackground {
				t.Errorf("background = %d rgb:%v, want %d rgb:%v", s.Style.Background, s.Style.HasBackgroundRGB, tt.background, tt.hasBackground)
			}
		})
	}
}

func TestClearAndErase(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "CSI 2J clears the whole screen",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("HelloWorldABCDE"))
				// Clear damage from writing.
				s.ClearDamage()

				s.Write([]byte("\x1b[2J"))

				// All cells should be blank.
				for y := range 3 {
					for x := range 5 {
						if c := cellAt(s, x, y); c.Rune != ' ' {
							t.Errorf("cell(%d,%d) = %q, want space after clear", x, y, c.Rune)
						}
					}
				}

				d := s.Damage()
				if !hasDamageKind(d, renderer.DamageClear) {
					t.Error("expected DamageClear after CSI 2 J")
				}
			},
		},
		{
			name: "CSI K clears from cursor to end of line",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("HelloWorld"))
				s.ClearDamage()

				// Position cursor at row 1 col 6 (0-indexed: 0, 5) then clear to end.
				s.Write([]byte("\x1b[1;6H"))
				s.Write([]byte("\x1b[K"))

				for x := 5; x < 10; x++ {
					if c := cellAt(s, x, 0); c.Rune != ' ' {
						t.Errorf("cell(%d,0) = %q, want space after clear line", x, c.Rune)
					}
				}
				// First 5 chars should remain.
				assertCell(t, s, 0, 0, 'H')

				d := s.Damage()
				if !hasDamageKind(d, renderer.DamageClear) {
					t.Error("expected DamageClear after CSI K")
				}
			},
		},
		{
			name: "erase display modes 0/1/2 (implicit, 1, 2/3)",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("abcdefghijklmno"))
				s.Write([]byte("\x1b[2;3H"))
				s.ClearDamage()
				s.Write([]byte("\x1b[J"))
				assertCell(t, s, 0, 0, 'a')
				assertCell(t, s, 1, 1, 'g')
				if c := cellAt(s, 2, 1); c.Rune != ' ' {
					t.Fatalf("cursor-to-end clear left %q at cursor", c.Rune)
				}

				s = NewScreen(5, 3)
				s.Write([]byte("abcdefghijklmno"))
				s.Write([]byte("\x1b[2;3H"))
				s.Write([]byte("\x1b[1J"))
				if c := cellAt(s, 0, 0); c.Rune != ' ' {
					t.Fatalf("start-to-cursor clear left %q at start", c.Rune)
				}
				assertCell(t, s, 3, 1, 'i')
			},
		},
		{
			name: "erase line modes 0/1/2",
			run: func(t *testing.T) {
				s := NewScreen(5, 2)
				s.Write([]byte("abcde"))
				s.Write([]byte("\x1b[1;3H"))
				s.Write([]byte("\x1b[1K"))
				if c := cellAt(s, 0, 0); c.Rune != ' ' {
					t.Fatalf("line start clear left %q", c.Rune)
				}
				assertCell(t, s, 3, 0, 'd')

				s = NewScreen(5, 2)
				s.Write([]byte("abcde"))
				s.Write([]byte("\x1b[1;3H"))
				s.Write([]byte("\x1b[2K"))
				for x := range 5 {
					if c := cellAt(s, x, 0); c.Rune != ' ' {
						t.Fatalf("line clear left %q at %d", c.Rune, x)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestEraseCharacters(t *testing.T) {
	maxInt := strconv.Itoa(int(^uint(0) >> 1))
	tests := []struct {
		name       string
		width      int
		row        int
		initial    string
		seq        string
		wantRow    string
		wantCol    int
		wantDamage []renderer.Damage
	}{
		{
			name:    "explicit count",
			width:   50,
			row:     1,
			initial: `        // scroll-method "on-button-down"`,
			seq:     "\x1b[19Gbutton 273\x1b[13X",
			wantRow: "        // scroll-button 273",
			wantCol: 28,
			wantDamage: []renderer.Damage{
				{Kind: renderer.DamageText, X: 18, Y: 1, Width: 10, Height: 1, Count: 1},
				{Kind: renderer.DamageClear, X: 28, Y: 1, Width: 13, Height: 1, Count: 1},
			},
		},
		{
			name:       "omitted count erases exactly one cell",
			width:      6,
			row:        1,
			initial:    "abcdef",
			seq:        "\x1b[3G\x1b[X",
			wantRow:    "ab def",
			wantCol:    2,
			wantDamage: []renderer.Damage{{Kind: renderer.DamageClear, X: 2, Y: 1, Width: 1, Height: 1, Count: 1}},
		},
		{
			name:       "maximum count clips to screen edge",
			width:      6,
			row:        1,
			initial:    "abcdef",
			seq:        "\x1b[4G\x1b[" + maxInt + "X",
			wantRow:    "abc",
			wantCol:    3,
			wantDamage: []renderer.Damage{{Kind: renderer.DamageClear, X: 3, Y: 1, Width: 3, Height: 1, Count: 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(tt.width, 2)
			s.Write([]byte("\x1b[2;1H"))
			s.Write([]byte(tt.initial))
			s.ClearDamage()
			s.Write([]byte(tt.seq))

			for x := range tt.width {
				want := ' '
				if x < len(tt.wantRow) {
					want = rune(tt.wantRow[x])
				}
				require.Equal(t, want, cellAt(s, x, tt.row).Rune, "cell(%d,%d)", x, tt.row)
			}
			require.Equal(t, tt.row, s.Row)
			require.Equal(t, tt.wantCol, s.Col)
			require.Equal(t, tt.wantDamage, s.Damage())
		})
	}
}

func TestBackgroundColorErase(t *testing.T) {
	tests := []struct {
		name      string
		width     int
		height    int
		seq       string
		startX    int
		wantStyle renderer.Style
	}{
		{
			name:      "indexed background for EL",
			width:     8,
			height:    1,
			seq:       "\x1b[1;31;48;5;236mX\x1b[K",
			startX:    1,
			wantStyle: renderer.Style{Foreground: -1, Background: 236},
		},
		{
			name:   "truecolor background for full ED",
			width:  3,
			height: 2,
			seq:    "abcdef\x1b[1;31;48;2;12;34;56m\x1b[2J",
			wantStyle: renderer.Style{
				Foreground:       -1,
				Background:       -1,
				HasBackgroundRGB: true,
				BackgroundRGB:    renderer.RGB{R: 12, G: 34, B: 56},
			},
		},
		{
			name:      "default background after SGR 49",
			width:     2,
			height:    1,
			seq:       "\x1b[48;2;12;34;56m\x1b[49m\x1b[K",
			wantStyle: renderer.DefaultStyle(),
		},
		{
			name:      "default background after SGR 0",
			width:     2,
			height:    1,
			seq:       "\x1b[48;2;12;34;56m\x1b[0m\x1b[K",
			wantStyle: renderer.DefaultStyle(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(tt.width, tt.height)
			s.Write([]byte(tt.seq))

			for y := range tt.height {
				for x := tt.startX; x < tt.width; x++ {
					cell := cellAt(s, x, y)
					if cell.Rune != ' ' || cell.Style != tt.wantStyle {
						t.Errorf("cell(%d,%d) = %+v, want space with style %+v", x, y, cell, tt.wantStyle)
					}
				}
			}
		})
	}
}

func TestCursorMove(t *testing.T) {
	tests := []struct {
		name    string
		seq     string
		initRow int
		initCol int
		wantRow int
		wantCol int
	}{
		{name: "CSI H moves to explicit row/col", seq: "\x1b[3;4H", wantRow: 2, wantCol: 3},
		{name: "CSI H clamps to bottom-right when out of bounds", seq: "\x1b[100;200H", wantRow: 4, wantCol: 9},
		{name: "CSI f moves to explicit row/col", seq: "\x1b[2;8f", wantRow: 1, wantCol: 7},
		{name: "CSI C moves forward after CR", seq: "\r\x1b[3C", wantRow: 0, wantCol: 3},
		{name: "CSI H with no params moves to (1,1)", seq: "\x1b[H", wantRow: 0, wantCol: 0},
		{name: "CSI ;5H uses default row with explicit column", seq: "\x1b[;5H", initRow: 3, initCol: 4, wantRow: 0, wantCol: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 5)
			s.Row, s.Col = tt.initRow, tt.initCol
			s.Write([]byte(tt.seq))

			if s.Row != tt.wantRow || s.Col != tt.wantCol {
				t.Errorf("cursor at row=%d col=%d, want row=%d col=%d", s.Row, s.Col, tt.wantRow, tt.wantCol)
			}
		})
	}
}

func TestCSIEditingSequences(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "directional cursor moves (A/B/C/D)",
			run: func(t *testing.T) {
				s := NewScreen(10, 4)
				s.Write([]byte("\x1b[3;5HX\x1b[2D<\x1b[1A^\x1b[1Bv\x1b[3C>"))

				assertCell(t, s, 4, 2, 'X')
				assertCell(t, s, 3, 2, '<')
				assertCell(t, s, 4, 1, '^')
				assertCell(t, s, 5, 2, 'v')
				assertCell(t, s, 9, 2, '>')
			},
		},
		{
			name: "insert and delete characters (@ and P)",
			run: func(t *testing.T) {
				s := NewScreen(8, 2)
				s.Write([]byte("abcdef"))
				s.Write([]byte("\x1b[1;3H\x1b[2@XY"))
				if got := lineText(s, 0); got != "abXYcdef" {
					t.Fatalf("line after ICH = %q, want %q", got, "abXYcdef")
				}

				s.Write([]byte("\x1b[1;4H\x1b[3P"))
				if got := lineText(s, 0); got != "abXef   " {
					t.Fatalf("line after DCH = %q, want %q", got, "abXef   ")
				}
			},
		},
		{
			name: "insert and delete lines (L and M)",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("11111222223333344444"))
				s.Write([]byte("\x1b[2;1H\x1b[L"))
				if got := lineText(s, 1); got != "     " {
					t.Fatalf("inserted line = %q, want blank", got)
				}
				if got := lineText(s, 2); got != "22222" {
					t.Fatalf("shifted line = %q, want 22222", got)
				}

				s.Write([]byte("\x1b[2;1H\x1b[M"))
				if got := lineText(s, 1); got != "22222" {
					t.Fatalf("line after delete = %q, want 22222", got)
				}
			},
		},
		{
			name: "scroll region (CSI r) confines LF scrolling",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("11111222223333344444"))
				s.Write([]byte("\x1b[2;3r\x1b[3;1H\n"))

				if got := lineText(s, 0); got != "11111" {
					t.Fatalf("row outside region changed: %q", got)
				}
				if got := lineText(s, 1); got != "33333" {
					t.Fatalf("region did not scroll up, row 1 = %q", got)
				}
				if got := lineText(s, 2); got != "     " {
					t.Fatalf("region bottom not blanked, row 2 = %q", got)
				}
				if got := lineText(s, 3); got != "44444" {
					t.Fatalf("row below region changed: %q", got)
				}
			},
		},
		{
			name: "DECOM homes and addresses relative to scroll region",
			run: func(t *testing.T) {
				s := NewScreen(6, 5)
				s.Write([]byte("\x1b[2;4r\x1b[?6h"))
				if s.Row != 1 || s.Col != 0 {
					t.Fatalf("DECOM set cursor = (%d,%d), want (1,0)", s.Row, s.Col)
				}

				s.Write([]byte("\x1b[2;3H"))
				if s.Row != 2 || s.Col != 2 {
					t.Fatalf("DECOM addressed cursor = (%d,%d), want (2,2)", s.Row, s.Col)
				}
				s.Write([]byte("\x1b[99;1H"))
				if s.Row != 3 || s.Col != 0 {
					t.Fatalf("DECOM clamped cursor = (%d,%d), want (3,0)", s.Row, s.Col)
				}
			},
		},
		{
			name: "DECOM already set homes DECSTBM to scroll region",
			run: func(t *testing.T) {
				s := NewScreen(6, 5)
				s.Write([]byte("\x1b[?6h\x1b[2;4r"))
				if s.Row != 1 || s.Col != 0 {
					t.Fatalf("DECSTBM cursor with DECOM set = (%d,%d), want (1,0)", s.Row, s.Col)
				}

				s.Write([]byte("\x1b[2;3H"))
				if s.Row != 2 || s.Col != 2 {
					t.Fatalf("DECOM addressed cursor after DECSTBM = (%d,%d), want (2,2)", s.Row, s.Col)
				}
				s.Write([]byte("\x1b[99;1H"))
				if s.Row != 3 || s.Col != 0 {
					t.Fatalf("DECOM clamped cursor after DECSTBM = (%d,%d), want (3,0)", s.Row, s.Col)
				}
			},
		},
		{
			name: "DECOM reset homes and restores full-frame addressing",
			run: func(t *testing.T) {
				s := NewScreen(6, 5)
				s.Write([]byte("\x1b[2;4r\x1b[?6h\x1b[?6l"))
				if s.Row != 0 || s.Col != 0 {
					t.Fatalf("DECOM reset cursor = (%d,%d), want (0,0)", s.Row, s.Col)
				}

				s.Write([]byte("\x1b[5;6H"))
				if s.Row != 4 || s.Col != 5 {
					t.Fatalf("full-frame cursor = (%d,%d), want (4,5)", s.Row, s.Col)
				}
			},
		},
		{
			name: "private alternate screen mode restores normal screen on exit",
			run: func(t *testing.T) {
				s := NewScreen(20, 5)
				s.Write([]byte("normal-line"))
				s.Write([]byte("\x1b[?1049h\x1b[2J\x1b[HLOCKED\x1b[3;4HAPP"))
				s.Write([]byte("\x1b[?1049lafter"))

				if got := lineText(s, 0); got != "normal-lineafter    " {
					t.Fatalf("row 0 = %q, want restored normal screen with post-exit text", got)
				}
				if got := lineText(s, 2); got != "                    " {
					t.Fatalf("alternate content leaked into row 2: %q", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestCSIScrollUp(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "CSI S with no param scrolls up by 1 and preserves cursor",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("AAAAABBBBBCCCCCDDDDD")) // fill all 4 rows
				s.ClearDamage()

				// CSI S with no param → scroll up by 1.
				s.Write([]byte("\x1b[S"))

				// Content should have shifted up: row 0 = BBBBB, row 1 = CCCCC, row 2 = DDDDD, row 3 = blank.
				assertCell(t, s, 0, 0, 'B')
				assertCell(t, s, 0, 1, 'C')
				assertCell(t, s, 0, 2, 'D')
				assertCell(t, s, 0, 3, ' ')

				// Cursor should NOT have moved.
				if s.Col != 5 || s.Row != 3 {
					t.Errorf("cursor at col=%d row=%d, want col=5 row=3", s.Col, s.Row)
				}

				d := s.Damage()
				if !hasDamageKind(d, renderer.DamageScrollUp) {
					t.Errorf("expected DamageScrollUp, got %v", damageKinds(d))
				}
			},
		},
		{
			name: "CSI 2S scrolls up by an explicit count",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("AAAAABBBBBCCCCCDDDDD")) // fill all 4 rows
				s.ClearDamage()

				// CSI 2 S → scroll up by 2.
				s.Write([]byte("\x1b[2S"))

				// Content: row 0 = CCCCC, row 1 = DDDDD, row 2 = blank, row 3 = blank.
				assertCell(t, s, 0, 0, 'C')
				assertCell(t, s, 0, 1, 'D')
				assertCell(t, s, 0, 2, ' ')
				assertCell(t, s, 0, 3, ' ')

				// Cursor preserved.
				if s.Col != 5 || s.Row != 3 {
					t.Errorf("cursor at col=%d row=%d, want col=5 row=3", s.Col, s.Row)
				}
			},
		},
		{
			name: "CSI 100S on a 3-row screen clamps to the full screen",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("AAAAABBBBBCCCCC")) // fill 3 rows
				s.ClearDamage()

				// CSI 100 S on a 3-row screen → clamp to 3 (entire screen blank).
				s.Write([]byte("\x1b[100S"))

				// All rows should be blank.
				for y := range 3 {
					for x := range 5 {
						assertCell(t, s, x, y, ' ')
					}
				}
			},
		},
		{
			name: "CSI 0S defaults to a scroll count of 1",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("AAAAABBBBBCCCCC")) // fill 3 rows
				s.ClearDamage()

				// CSI 0 S → default to 1 (parameter 0 means default in VT spec).
				s.Write([]byte("\x1b[0S"))

				// Row 0 should have shifted up: row 0 = BBBBB, row 1 = CCCCC, row 2 = blank.
				assertCell(t, s, 0, 0, 'B')
				assertCell(t, s, 0, 1, 'C')
				assertCell(t, s, 0, 2, ' ')
			},
		},
		{
			name: "CSI 3S reports scroll count 3 and blanked-row text damage",
			run: func(t *testing.T) {
				s := NewScreen(5, 4)
				s.Write([]byte("AAAAABBBBBCCCCCDDDDD")) // fill all 4 rows
				s.ClearDamage()

				s.Write([]byte("\x1b[3S")) // scroll up by 3

				d := s.Damage()
				var scrollDamage *renderer.Damage
				for i, dd := range d {
					if dd.Kind == renderer.DamageScrollUp {
						scrollDamage = &d[i]
						break
					}
				}
				if scrollDamage == nil {
					t.Fatal("expected DamageScrollUp")
				}
				if scrollDamage.Count != 3 {
					t.Errorf("scroll count = %d, want 3", scrollDamage.Count)
				}
				if scrollDamage.Width != 5 || scrollDamage.Height != 4 {
					t.Errorf("scroll size = %dx%d, want 5x4", scrollDamage.Width, scrollDamage.Height)
				}

				// Should also have text damage for the blanked rows (bottom 3 rows).
				if !hasDamageKind(d, renderer.DamageText) {
					t.Error("expected DamageText for blanked rows")
				}
			},
		},
		{
			name: "cursor position is preserved across a scroll-up",
			run: func(t *testing.T) {
				s := NewScreen(10, 5)
				s.Write([]byte("AAAAA\nBBBBB\nCCCCC\nDDDDD\nEEEEE")) // fill all 5 rows
				s.ClearDamage()

				// Move cursor to middle of row 2.
				s.Write([]byte("\x1b[3;3H")) // row=3, col=3
				s.Write([]byte("\x1b[2S"))   // scroll up by 2

				// Cursor should still be at 1-indexed (3,3) = 0-indexed (2,2).
				if s.Row != 2 || s.Col != 2 {
					t.Errorf("cursor at row=%d col=%d, want row=2 col=2", s.Row, s.Col)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
