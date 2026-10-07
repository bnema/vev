package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResumeInputDecoder(t *testing.T) {
	for _, tt := range []struct {
		name, input string
		cancel      bool
	}{
		{"grouped ctrl-c", "text\x03tail", true},
		{"paste escape followed by cancel", "\x1b[200~a\x1b\x1b[201~\x03", true},
		{"back to back escape", "\x1b\x1b[27u", true},
		{"ctrl-c in unfinished CSI", "\x1b[\x03", true},
		{"kitty ctrl-c", "\x1b[99;5u", true},
		{"kitty escape", "\x1b[27u", true},
		{"arrow", "\x1b[A", false},
		{"function", "\x1bOP", false},
		{"paste", "\x1b[200~a\x03\x1b[27u\x1b[201~", false},
		{"text", "hello", false},
		{"alt enter", "\x1b\r", false},
		{"alt tab", "\x1b\t", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for split := 0; split <= len(tt.input); split++ {
				var d resumeInputDecoder
				first, cancelled := d.feed([]byte(tt.input[:split]))
				second := []byte(nil)
				if !cancelled {
					second, cancelled = d.feed([]byte(tt.input[split:]))
				}
				require.Equal(t, tt.cancel, cancelled, "split %d", split)
				if !tt.cancel {
					got := append(first, second...)
					got = append(got, d.flush()...)
					require.Equal(t, tt.input, string(got), "split %d", split)
				}
			}
		})
	}
}

func TestResumeInputDecoderUndecidedEscape(t *testing.T) {
	var d resumeInputDecoder
	held, cancel := d.feed([]byte("\x1b"))
	require.Empty(t, held)
	require.False(t, cancel)
	require.True(t, d.loneEscape())
	require.Equal(t, []byte{0x1b}, d.flush())
	require.False(t, d.loneEscape())
}
