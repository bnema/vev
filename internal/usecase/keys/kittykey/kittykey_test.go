package kittykey

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		ev      Event
		ok      bool
		partial bool
	}{
		{name: "plain", in: "\x1b[97u", ev: Event{Code: 'a'}, ok: true},
		{name: "ctrl", in: "\x1b[49;5u", ev: Event{Code: '1', Mods: ModCtrl}, ok: true},
		{name: "azerty ctrl 1 with base", in: "\x1b[38::49;5u", ev: Event{Code: '&', Base: '1', Mods: ModCtrl}, ok: true},
		{name: "shifted and base", in: "\x1b[97:65:97;2u", ev: Event{Code: 'a', Shifted: 'A', Base: 'a', Mods: ModShift}, ok: true},
		{name: "locks stripped", in: "\x1b[97;197u", ev: Event{Code: 'a', Mods: ModCtrl}, ok: true},
		{name: "release", in: "\x1b[97;1:3u", ev: Event{Code: 'a', Release: true}, ok: true},
		{name: "text field ignored", in: "\x1b[97;1;97u", ev: Event{Code: 'a'}, ok: true},
		{name: "trailing garbage", in: "\x1b[97uxyz", ev: Event{Code: 'a'}, ok: true},
		{name: "truncated", in: "\x1b[49;5", partial: true},
		{name: "truncated csi", in: "\x1b[", partial: true},
		{name: "truncated esc", in: "\x1b", partial: true},
		{name: "empty params", in: "\x1b[u"},
		{name: "zero code", in: "\x1b[0u"},
		{name: "mods zero", in: "\x1b[97;0u"},
		{name: "bad event", in: "\x1b[97;1:4u"},
		{name: "too many fields", in: "\x1b[97;1;2;3u"},
		{name: "other final", in: "\x1b[1;5A"},
		{name: "private reply", in: "\x1b[?5u"},
		{name: "huge number", in: "\x1b[99999999u"},
		{name: "not csi", in: "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, n, ok, partial := Parse([]byte(tt.in))
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.partial, partial)
			if tt.ok {
				require.Equal(t, tt.ev, ev)
				want := len(tt.in)
				if tt.name == "trailing garbage" {
					want -= len("xyz")
				}
				require.Equal(t, want, n)
			}
		})
	}
}

func TestTranslateLegacy(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "legacy passthrough", in: "hello\x1b[A\x1bj\r", want: "hello\x1b[A\x1bj\r"},
		{name: "escape", in: "\x1b[27u", want: "\x1b"},
		{name: "alt escape", in: "\x1b[27;3u", want: "\x1b\x1b"},
		{name: "enter", in: "\x1b[13u", want: "\r"},
		{name: "tab", in: "\x1b[9u", want: "\t"},
		{name: "shift tab", in: "\x1b[9;2u", want: "\x1b[Z"},
		{name: "backspace", in: "\x1b[127u", want: "\x7f"},
		{name: "ctrl backspace", in: "\x1b[127;5u", want: "\x08"},
		{name: "ctrl i", in: "\x1b[105;5u", want: "\t"},
		{name: "ctrl a", in: "\x1b[97;5u", want: "\x01"},
		{name: "ctrl alt a", in: "\x1b[97;7u", want: "\x1b\x01"},
		{name: "ctrl shift a", in: "\x1b[97:65;6u", want: "\x01"},
		{name: "ctrl space", in: "\x1b[32;5u", want: "\x00"},
		{name: "ctrl 1 has no control byte", in: "\x1b[49;5u", want: "1"},
		{name: "ctrl 3", in: "\x1b[51;5u", want: "\x1b"},
		{name: "azerty ctrl ampersand", in: "\x1b[38::49;5u", want: "&"},
		{name: "cyrillic ctrl uses base", in: "\x1b[1092::97;5u", want: "\x01"},
		{name: "shift letter with shifted", in: "\x1b[97:65;2u", want: "A"},
		{name: "shift letter without shifted", in: "\x1b[97;2u", want: "A"},
		{name: "alt utf8", in: "\x1b[233;3u", want: "\x1bé"},
		{name: "super dropped", in: "\x1b[97;9u", want: ""},
		{name: "release dropped", in: "\x1b[97;1:3u", want: ""},
		{name: "private use dropped", in: "\x1b[57441u", want: ""},
		{name: "keypad digit", in: "\x1b[57400u", want: "1"},
		{name: "keypad enter", in: "\x1b[57414u", want: "\r"},
		{name: "ctrl keypad left", in: "\x1b[57417;5u", want: "\x1b[1;5D"},
		{name: "ctrl keypad delete", in: "\x1b[57426;5u", want: "\x1b[3;5~"},
		{name: "keypad begin", in: "\x1b[57427u", want: "\x1b[E"},
		{name: "keypad separator", in: "\x1b[57416u", want: ","},
		{name: "f1 unmodified", in: "\x1b[P", want: "\x1bOP"},
		{name: "f2 modified kept", in: "\x1b[1;5Q", want: "\x1b[1;5Q"},
		{name: "mixed", in: "a\x1b[97;5ub\x1b[27uc", want: "a\x01b\x1bc"},
		{name: "truncated prefix untouched", in: "x\x1b[97;5", want: "x\x1b[97;5"},
		{name: "trailing garbage", in: "\x1b[97;5u\x1b[zz", want: "\x01\x1b[zz"},
		{name: "paste body untouched", in: "\x1b[200~\x1b[97;5u\x1b[201~\x1b[97;5u", want: "\x1b[200~\x1b[97;5u\x1b[201~\x01"},
		{name: "unterminated paste untouched", in: "\x1b[200~\x1b[97;5u", want: "\x1b[200~\x1b[97;5u"},
		{name: "mouse untouched", in: "\x1b[<0;1;1M", want: "\x1b[<0;1;1M"},
		{name: "keyboard reply untouched", in: "\x1b[?5u", want: "\x1b[?5u"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, []byte(tt.want), append([]byte{}, Translate([]byte(tt.in), 0)...))
		})
	}
}

func TestTranslateKittyReceiver(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		flags int
		want  string
	}{
		{name: "same flags identity", in: "\x1b[38::49;5u", flags: OuterFlags, want: "\x1b[38::49;5u"},
		{name: "strip alternates", in: "\x1b[38::49;5u", flags: FlagDisambiguate, want: "\x1b[38;5u"},
		{name: "keep shifted", in: "\x1b[97:65;2u", flags: FlagDisambiguate | FlagAlternateKeys, want: "\x1b[97:65;2u"},
		{name: "unmodified", in: "\x1b[27u", flags: FlagDisambiguate, want: "\x1b[27u"},
		{name: "release dropped without events", in: "\x1b[97;1:3u", flags: FlagDisambiguate, want: ""},
		{name: "release kept with events", in: "\x1b[97;1:3u", flags: FlagDisambiguate | FlagReportEvents, want: "\x1b[97;1:3u"},
		{name: "f1 stays csi", in: "\x1b[P", flags: FlagDisambiguate, want: "\x1b[P"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, []byte(tt.want), append([]byte{}, Translate([]byte(tt.in), tt.flags)...))
		})
	}
}

func TestTranslateReturnsInputWhenUnchanged(t *testing.T) {
	in := []byte("plain \x1b[A text")
	out := Translate(in, 0)
	require.Same(t, &in[0], &out[0])
}

func TestEventDigit(t *testing.T) {
	tests := []struct {
		ev   Event
		want int
		ok   bool
	}{
		{ev: Event{Code: '1'}, want: 1, ok: true},
		{ev: Event{Code: '&', Base: '1'}, want: 1, ok: true},
		{ev: Event{Code: 'é', Base: '2'}, want: 2, ok: true},
		{ev: Event{Code: '0'}},
		{ev: Event{Code: 'a'}},
	}
	for _, tt := range tests {
		got, ok := tt.ev.Digit()
		require.Equal(t, tt.ok, ok, "%+v", tt.ev)
		require.Equal(t, tt.want, got)
	}
}

func FuzzTranslate(f *testing.F) {
	for _, seed := range []string{"\x1b[97;5u", "\x1b[38::49;5u", "\x1b[200~x\x1b[201~", "\x1b[1;5P", "a\x1b["} {
		f.Add([]byte(seed), 0)
		f.Add([]byte(seed), OuterFlags)
	}
	f.Fuzz(func(t *testing.T, data []byte, flags int) {
		out := Translate(data, flags&0x1f)
		// Translation never grows input by more than a bounded factor.
		require.LessOrEqual(t, len(out), 2*len(data)+16)
	})
}

func BenchmarkTranslateLegacyText(b *testing.B) {
	data := []byte("the quick brown fox jumps over the lazy dog\r")
	b.ReportAllocs()
	for b.Loop() {
		_ = Translate(data, 0)
	}
}
