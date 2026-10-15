package kittykey

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizerRewritesRequestFlagsToOuterFlags(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "legacy passthrough", in: "hello\x1b[A\x1bOP\r", want: "hello\x1b[A\x1bOP\r"},
		{name: "outer csi u passthrough", in: "\x1b[97;5u\x1b[38::49;5u", want: "\x1b[97;5u\x1b[38::49;5u"},
		{name: "text key", in: "\x1b[97;;97u", want: "a"},
		{name: "text key without text field", in: "\x1b[97u", want: "a"},
		{name: "shift text", in: "\x1b[97:65;2;65u", want: "A"},
		{name: "shift without text uses shifted", in: "\x1b[97:65;2u", want: "A"},
		{name: "composed text event", in: "\x1b[0;;229u", want: "å"},
		{name: "multi codepoint text", in: "\x1b[101;;101:769u", want: "e\u0301"},
		{name: "control codes dropped from text", in: "\x1b[0;;7:97u", want: "a"},
		{name: "repeat is press", in: "\x1b[97;1:2;97u", want: "a"},
		{name: "release dropped", in: "\x1b[97;1:3u", want: ""},
		{name: "space", in: "\x1b[32;;32u", want: " "},
		{name: "enter", in: "\x1b[13u", want: "\r"},
		{name: "tab", in: "\x1b[9u", want: "\t"},
		{name: "backspace", in: "\x1b[127u", want: "\x7f"},
		{name: "shift enter stays csi u", in: "\x1b[13;2u", want: "\x1b[13;2u"},
		{name: "escape", in: "\x1b[27u", want: "\x1b[27u"},
		{name: "ctrl letter", in: "\x1b[99;5u", want: "\x1b[99;5u"},
		{name: "ctrl digit with base", in: "\x1b[38::49;5u", want: "\x1b[38::49;5u"},
		{name: "alt letter without text", in: "\x1b[97;3u", want: "\x1b[97;3u"},
		{name: "altgr text wins over modifiers", in: "\x1b[113;7;64u", want: "@"},
		{name: "long ime commit", in: "\x1b[0;;" + strings.Repeat("19968:", 11) + "19968u", want: strings.Repeat("一", 12)},
		{name: "caps lock dropped from text key", in: "\x1b[97;65;65u", want: "A"},
		{name: "caps lock kept on ctrl letter", in: "\x1b[97;69u", want: "\x1b[97;5u"},
		{name: "num lock kept on escape", in: "\x1b[27;129u", want: "\x1b[27;129u"},
		{name: "caps lock kept on arrow", in: "\x1b[1;65A", want: "\x1b[1;65A"},
		{name: "caps lock arrow repeat", in: "\x1b[1;65:2A", want: "\x1b[1;65A"},
		{name: "caps lock delete", in: "\x1b[3;65~", want: "\x1b[3;65~"},
		{name: "ctrl press dropped", in: "\x1b[57442;5u", want: ""},
		{name: "ctrl release dropped", in: "\x1b[57442;1:3u", want: ""},
		{name: "shift key dropped", in: "\x1b[57441;2u", want: ""},
		{name: "caps lock dropped", in: "\x1b[57358;65u", want: ""},
		{name: "arrow unmodified passthrough", in: "\x1b[A", want: "\x1b[A"},
		{name: "arrow repeat", in: "\x1b[1;1:2A", want: "\x1b[A"},
		{name: "arrow release dropped", in: "\x1b[1;1:3A", want: ""},
		{name: "ctrl arrow", in: "\x1b[1;5C", want: "\x1b[1;5C"},
		{name: "ctrl arrow repeat", in: "\x1b[1;5:2C", want: "\x1b[1;5C"},
		{name: "delete release dropped", in: "\x1b[3;1:3~", want: ""},
		{name: "delete repeat", in: "\x1b[3;1:2~", want: "\x1b[3~"},
		{name: "ctrl delete", in: "\x1b[3;5~", want: "\x1b[3;5~"},
		{name: "mouse untouched", in: "\x1b[<0;1;1M", want: "\x1b[<0;1;1M"},
		{name: "keyboard reply untouched", in: "\x1b[?5u", want: "\x1b[?5u"},
		{name: "cursor report untouched", in: "\x1b[12;40R", want: "\x1b[12;40R"},
		{name: "paste body untouched", in: "\x1b[200~\x1b[97;1:3u\x1b[201~\x1b[97;1:3u", want: "\x1b[200~\x1b[97;1:3u\x1b[201~"},
		{name: "lone esc held", in: "x\x1b", want: "x"},
		{name: "trailing garbage", in: "\x1b[97;;97u\x1b[zz", want: "a\x1b[zz"},
		{name: "bad event type kept", in: "\x1b[97;1:4u", want: "\x1b[97;1:4u"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n Normalizer
			require.Equal(t, tt.want, string(n.Normalize([]byte(tt.in))))
		})
	}
}

func TestNormalizerHoldsSplitSequences(t *testing.T) {
	tests := []struct {
		name  string
		reads []string
		want  []string
	}{
		{name: "split csi u", reads: []string{"a\x1b[97;", ";98u"}, want: []string{"a", "b"}},
		{name: "split csi prefix", reads: []string{"\x1b[", "57442;1:3u"}, want: []string{"", ""}},
		{name: "split paste close", reads: []string{"\x1b[200~x\x1b[20", "1~\x1b[97;;97u"}, want: []string{"\x1b[200~x", "\x1b[201~a"}},
		{name: "split after esc", reads: []string{"\x1b", "[57442;5u", "\x1b", "[97;;97u"}, want: []string{"", "", "", "a"}},
		{name: "split paste open", reads: []string{"\x1b[20", "0~\x1b[97;1:3u\x1b[201~"}, want: []string{"", "\x1b[200~\x1b[97;1:3u\x1b[201~"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n Normalizer
			for i, read := range tt.reads {
				require.Equal(t, tt.want[i], string(n.Normalize([]byte(read))), "read %d", i)
			}
		})
	}
}

// TestNormalizerEverySplitMatchesOneRead feeds each input split at every byte
// and requires the same output and Ctrl state as one read.
func TestNormalizerEverySplitMatchesOneRead(t *testing.T) {
	inputs := []string{
		"\x1b[57442;5u\x1b[49;5u\x1b[49;5:3u",
		"a\x1b[97;;97u\x1b[57442;1:3u",
		"\x1b[200~\x1b[97;1:3u\x1b[201~\x1b[1;65:2A",
		"\x1b[0;;19968:19968u\x1b[27u",
	}
	for _, in := range inputs {
		var whole Normalizer
		want := string(whole.Normalize([]byte(in)))
		for cut := 1; cut < len(in); cut++ {
			var n Normalizer
			got := string(n.Normalize([]byte(in[:cut]))) + string(n.Normalize([]byte(in[cut:])))
			require.Equal(t, want, got, "input %q cut %d", in, cut)
			require.Equal(t, whole.CtrlHeld(), n.CtrlHeld(), "input %q cut %d", in, cut)
		}
	}
}

func TestNormalizerTracksCtrl(t *testing.T) {
	tests := []struct {
		name  string
		reads []string
		held  []bool
	}{
		{name: "left ctrl press and release", reads: []string{"\x1b[57442;5u", "\x1b[57442;1:3u"}, held: []bool{true, false}},
		{name: "right ctrl", reads: []string{"\x1b[57448;5u", "\x1b[57448;1:3u"}, held: []bool{true, false}},
		{name: "ctrl repeat keeps held", reads: []string{"\x1b[57442;5u", "\x1b[57442;5:2u"}, held: []bool{true, true}},
		{name: "one of two ctrl released", reads: []string{"\x1b[57442;5u", "\x1b[57448;5u", "\x1b[57448;5:3u"}, held: []bool{true, true, true}},
		{name: "ctrl digit keeps held", reads: []string{"\x1b[57442;5u", "\x1b[49;5u", "\x1b[49;5:3u"}, held: []bool{true, true, true}},
		{name: "key without ctrl clears", reads: []string{"\x1b[57442;5u", "\x1b[97;;97u"}, held: []bool{true, false}},
		{name: "arrow without ctrl clears", reads: []string{"\x1b[57442;5u", "\x1b[1;1:2A"}, held: []bool{true, false}},
		{name: "ctrl shortcut clears", reads: []string{"\x1b[57442;5u", "\x1b[114;5u", "\x1b[114;5:3u"}, held: []bool{true, false, false}},
		{name: "ctrl arrow clears", reads: []string{"\x1b[57442;5u", "\x1b[1;5C"}, held: []bool{true, false}},
		{name: "azerty ctrl digit keeps held", reads: []string{"\x1b[57442;5u", "\x1b[38::49;5u"}, held: []bool{true, true}},
		{name: "ctrl shift keeps held", reads: []string{"\x1b[57442;5u", "\x1b[57441;6u", "\x1b[57441;5:3u"}, held: []bool{true, true, true}},
		{name: "composed text keeps held", reads: []string{"\x1b[57442;5u", "\x1b[0;;229u"}, held: []bool{true, true}},
		{name: "legacy text never sets", reads: []string{"\x1b[97;5u"}, held: []bool{false}},
		{name: "shift does not set", reads: []string{"\x1b[57441;2u"}, held: []bool{false}},
		{name: "ctrl in paste ignored", reads: []string{"\x1b[200~\x1b[57442;5u\x1b[201~"}, held: []bool{false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n Normalizer
			for i, read := range tt.reads {
				n.Normalize([]byte(read))
				require.Equal(t, tt.held[i], n.CtrlHeld(), "read %d", i)
			}
		})
	}
	var n Normalizer
	n.Normalize([]byte("\x1b[57442;5u"))
	n.ReleaseCtrl()
	require.False(t, n.CtrlHeld())
}

func FuzzNormalize(f *testing.F) {
	for _, seed := range []string{"\x1b[97;;97u", "\x1b[57442;5u", "\x1b[200~x\x1b[201~", "\x1b[1;5:3A", "a\x1b[", "\x1b[0;;229:7u"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var n Normalizer
		out := n.Normalize(data)
		out = append(out, n.Normalize(nil)...)
		require.LessOrEqual(t, len(out), 4*len(data)+16)
	})
}
