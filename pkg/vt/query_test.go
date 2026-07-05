package vt

import (
	"bytes"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestScreenQueryResponses(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "primary DA bare", input: "\x1b[c", want: "\x1b[?6c"},
		{name: "primary DA with 0", input: "\x1b[0c", want: "\x1b[?6c"},
		{name: "secondary DA", input: "\x1b[>c", want: "\x1b[>0;0;0c"},
		{name: "secondary DA with 0", input: "\x1b[>0c", want: "\x1b[>0;0;0c"},
		{name: "DSR status report", input: "\x1b[5n", want: "\x1b[0n"},
		{name: "CPR at home", input: "\x1b[6n", want: "\x1b[1;1R"},
		{name: "CPR after CUP", input: "\x1b[3;7H\x1b[6n", want: "\x1b[3;7R"},
		{name: "private CPR", input: "\x1b[3;7H\x1b[?6n", want: "\x1b[?3;7R"},
		{name: "DECRQM 2026 reset", input: "\x1b[?2026$p", want: "\x1b[?2026;2$y"},
		{name: "DECRQM 2026 set", input: "\x1b[?2026h\x1b[?2026$p", want: "\x1b[?2026;1$y"},
		{name: "DECRQM 2031 reset", input: "\x1b[?2031$p", want: "\x1b[?2031;2$y"},
		{name: "DECRQM 2031 set", input: "\x1b[?2031h\x1b[?2031$p", want: "\x1b[?2031;1$y"},
		{name: "color scheme DSR dark", input: "\x1b[?996n", want: ColorSchemeReportDark},
		{name: "color scheme DSR light", input: "\x1b[?996n", want: ColorSchemeReportLight},
		{name: "DECRQM unknown mode", input: "\x1b[?1337$p", want: "\x1b[?1337;0$y"},
		{name: "kitty keyboard query unanswered", input: "\x1b[?u", want: ""},
		{name: "XTVERSION unanswered", input: "\x1b[>0q", want: ""},
		{name: "DA split across writes", input: "", want: "\x1b[?6c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScreen(80, 24)
			var got bytes.Buffer
			s.OnResponse = func(b []byte) { got.Write(b) }
			switch tc.name {
			case "DA split across writes":
				s.Write([]byte("\x1b["))
				s.Write([]byte("0c"))
			case "color scheme DSR light":
				s.SetColorScheme(true)
				s.Write([]byte(tc.input))
			case "color scheme DSR dark":
				s.SetColorScheme(false)
				s.Write([]byte(tc.input))
			default:
				s.Write([]byte(tc.input))
			}
			if got.String() != tc.want {
				t.Fatalf("response = %q, want %q", got.String(), tc.want)
			}
		})
	}
}

func TestScreenOSCDefaultColorQueries(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "foreground BEL", input: "\x1b]10;?\a", want: "\x1b]10;rgb:1212/3434/5656\a"},
		{name: "background BEL", input: "\x1b]11;?\a", want: "\x1b]11;rgb:7878/9a9a/bcbc\a"},
		{name: "foreground ST", input: "\x1b]10;?\x1b\\", want: "\x1b]10;rgb:1212/3434/5656\x1b\\"},
		{name: "background ST", input: "\x1b]11;?\x1b\\", want: "\x1b]11;rgb:7878/9a9a/bcbc\x1b\\"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScreen(80, 24)
			s.SetDefaultColors(
				renderer.RGB{R: 0x12, G: 0x34, B: 0x56},
				renderer.RGB{R: 0x78, G: 0x9a, B: 0xbc},
				true,
			)
			var got bytes.Buffer
			s.OnResponse = func(b []byte) { got.Write(b) }
			s.Write([]byte(tc.input))
			if got.String() != tc.want {
				t.Fatalf("response = %q, want %q", got.String(), tc.want)
			}
		})
	}
}

func TestScreenOSCDefaultColorQueriesUnknownAreSilent(t *testing.T) {
	s := NewScreen(80, 24)
	s.SetDefaultColors(renderer.RGB{R: 1, G: 2, B: 3}, renderer.RGB{R: 4, G: 5, B: 6}, false)
	var got bytes.Buffer
	s.OnResponse = func(b []byte) { got.Write(b) }
	s.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	if got.Len() != 0 {
		t.Fatalf("response = %q, want silence", got.String())
	}
}

func TestScreenOSCDefaultColorQuerySplitAcrossWrites(t *testing.T) {
	s := NewScreen(80, 24)
	s.SetDefaultColors(renderer.RGB{R: 0x01, G: 0x02, B: 0x03}, renderer.RGB{R: 0x04, G: 0x05, B: 0x06}, true)
	var got bytes.Buffer
	s.OnResponse = func(b []byte) { got.Write(b) }
	s.Write([]byte("\x1b]10"))
	s.Write([]byte(";?\a"))
	if got.String() != "\x1b]10;rgb:0101/0202/0303\a" {
		t.Fatalf("response = %q", got.String())
	}
}

func TestScreenQueriesWithNilResponderDoNotPanic(t *testing.T) {
	s := NewScreen(80, 24)
	s.Write([]byte("\x1b[c\x1b[6n\x1b[?2026$p"))
	s.SetDefaultColors(renderer.RGB{R: 1, G: 2, B: 3}, renderer.RGB{R: 4, G: 5, B: 6}, true)
	s.Write([]byte("\x1b]10;?\a\x1b]11;?\x1b\\"))
}

func TestCSIuDispatch(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantRow int
		wantCol int
	}{
		{name: "bare u restores cursor", input: "\x1b[5;10H\x1b[s\x1b[2;2H\x1b[u", wantRow: 4, wantCol: 9},
		{name: "kitty query leaves cursor", input: "\x1b[5;10H\x1b[s\x1b[2;2H\x1b[?u", wantRow: 1, wantCol: 1},
		{name: "kitty push leaves cursor", input: "\x1b[5;10H\x1b[s\x1b[2;2H\x1b[>1u", wantRow: 1, wantCol: 1},
		{name: "kitty pop leaves cursor", input: "\x1b[5;10H\x1b[s\x1b[2;2H\x1b[<u", wantRow: 1, wantCol: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScreen(80, 24)
			s.Write([]byte(tc.input))
			if s.Row != tc.wantRow || s.Col != tc.wantCol {
				t.Fatalf("cursor = %d;%d, want %d;%d", s.Row, s.Col, tc.wantRow, tc.wantCol)
			}
		})
	}
}

func TestScreenColorSchemeQueriesUnknownAreSilent(t *testing.T) {
	s := NewScreen(80, 24)
	s.SetColorScheme(true)
	s.ClearColorScheme()
	var got bytes.Buffer
	s.OnResponse = func(b []byte) { got.Write(b) }
	s.Write([]byte("\x1b[?996n"))
	if got.Len() != 0 {
		t.Fatalf("response = %q, want silence", got.String())
	}
}

func TestScreenResetClearsColorSchemeModeSubscription(t *testing.T) {
	s := NewScreen(80, 24)
	var got bytes.Buffer
	s.OnResponse = func(b []byte) { got.Write(b) }

	s.Write([]byte("\x1b[?2031h"))
	s.Write([]byte("\x1bc"))

	s.SetColorScheme(true)
	if got.Len() != 0 {
		t.Fatalf("unsolicited response after reset = %q, want silence", got.String())
	}

	got.Reset()
	s.Write([]byte("\x1b[?2031$p"))
	if got.String() != "\x1b[?2031;2$y" {
		t.Fatalf("DECRQM 2031 after reset = %q, want reset state", got.String())
	}
}

func TestScreenColorSchemeUnsolicitedOnlySubscribedAndChanged(t *testing.T) {
	s := NewScreen(80, 24)
	var got bytes.Buffer
	s.OnResponse = func(b []byte) { got.Write(b) }

	s.SetColorScheme(true)
	if got.Len() != 0 {
		t.Fatalf("unsubscribed response = %q, want silence", got.String())
	}

	s.Write([]byte("\x1b[?2031h"))
	if !s.ColorSchemeMode() {
		t.Fatalf("ColorSchemeMode() = false, want true")
	}
	s.SetColorScheme(true)
	if got.Len() != 0 {
		t.Fatalf("unchanged response = %q, want silence", got.String())
	}
	s.SetColorScheme(false)
	if got.String() != ColorSchemeReportDark {
		t.Fatalf("changed response = %q, want dark notification", got.String())
	}
	got.Reset()
	s.Write([]byte("\x1b[?2031l"))
	s.SetColorScheme(true)
	if got.Len() != 0 {
		t.Fatalf("disabled response = %q, want silence", got.String())
	}
}
