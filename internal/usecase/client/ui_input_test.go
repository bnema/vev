package client

import (
	"strings"
	"testing"
)

func TestEncodeUIKeys(t *testing.T) {
	for _, tt := range []struct {
		name    string
		keys    []string
		app     bool
		want    string
		invalid bool
	}{
		{name: "literal", keys: []string{"a", "Space", "Enter", "Tab", "Backspace", "Escape"}, want: "a \r\t\x7f\x1b"},
		{name: "navigation", keys: []string{"Up", "Down", "Right", "Left", "Home", "End", "PageUp", "PageDown"}, want: "\x1b[A\x1b[B\x1b[C\x1b[D\x1b[H\x1b[F\x1b[5~\x1b[6~"},
		{name: "application", keys: []string{"Up", "Home", "End"}, app: true, want: "\x1bOA\x1bOH\x1bOF"},
		{name: "modifiers", keys: []string{"Ctrl+A", "Ctrl+@", "Ctrl+_", "Alt+Space", "Alt+x"}, want: "\x01\x00\x1f\x1b \x1bx"},
		{name: "alt ASCII names", keys: []string{"Alt+Enter", "Alt+Tab", "Alt+Backspace", "Alt+Escape"}, want: "\x1b\r\x1b\t\x1b\x7f\x1b\x1b"},
		{name: "empty", invalid: true},
		{name: "case", keys: []string{"enter"}, invalid: true},
		{name: "combined", keys: []string{"Ctrl+Alt+A"}, invalid: true},
		{name: "modified navigation", keys: []string{"Alt+Up"}, invalid: true},
		{name: "raw", keys: []string{"\x1b[A"}, invalid: true},
		{name: "paste assembled", keys: []string{"Escape", "[", "2", "0", "0", "~"}, invalid: true},
		{name: "paste close assembled", keys: []string{"Alt+[", "2", "0", "1", "~"}, invalid: true},
		{name: "theme assembled", keys: []string{"Escape", "]", "1", "0", ";"}, invalid: true},
		{name: "marker assembled", keys: []string{"Escape", "[", "?", "2", "0", "3", "1"}, invalid: true},
		{name: "scheme assembled", keys: []string{"Escape", "[", "?", "9", "9", "7"}, invalid: true},
		{name: "too many", keys: make([]string, 257), invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeUIKeys(tt.keys, tt.app)
			if (err != nil) != tt.invalid || string(got) != tt.want {
				t.Fatalf("got %q, %v; want %q invalid=%v", got, err, tt.want, tt.invalid)
			}
		})
	}
	// Every permitted unmodified Alt ASCII token is usable on its own.
	for b := byte(0x20); b <= 0x7e; b++ {
		got, err := encodeUIKeys([]string{"Alt+" + string(b)}, false)
		if err != nil || string(got) != "\x1b"+string(b) {
			t.Fatalf("Alt ASCII %q: %q %v", b, got, err)
		}
	}
}

func TestEncodeUIText(t *testing.T) {
	for _, tt := range []struct {
		name, text string
		invalid    bool
	}{
		{"printable", "hello 世界", false}, {"spaces", "  ", false}, {"limit", strings.Repeat("x", uiMaxInputBytes), false},
		{"empty", "", true}, {"newline", "hello\n", true}, {"escape", "\x1b", true}, {"delete", "\x7f", true}, {"C1", "\u009b", true}, {"invalid UTF8", "\xff", true}, {"over limit", strings.Repeat("x", uiMaxInputBytes+1), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeUIText(tt.text)
			if (err != nil) != tt.invalid {
				t.Fatalf("error=%v invalid=%v", err, tt.invalid)
			}
			if err == nil && string(got) != tt.text {
				t.Fatal("text changed")
			}
		})
	}
}
