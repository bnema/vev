package vt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnBellFiresForLoneBEL(t *testing.T) {
	s := NewScreen(10, 2)
	rang := 0
	s.OnBell = func() { rang++ }
	s.Write([]byte("hi\ahi\a"))
	require.Equal(t, 2, rang)
	require.Equal(t, "hihi", strings.TrimRight(rowString(s.Frame.Row(0)), " "))
}

func TestOnBellIgnoresOSCTerminator(t *testing.T) {
	s := NewScreen(10, 2)
	rang := 0
	s.OnBell = func() { rang++ }
	s.Write([]byte("\x1b]0;title\a")) // BEL terminates the OSC, it is not a bell
	require.Equal(t, 0, rang)
}

func TestBELWithoutCallbackIsDiscarded(t *testing.T) {
	s := NewScreen(10, 2)
	s.Write([]byte("a\ab")) // no OnBell set: must not panic
	require.Equal(t, "ab", strings.TrimRight(rowString(s.Frame.Row(0)), " "))
}

func TestTerminalTitleRetainsOSC0AndOSC2(t *testing.T) {
	tests := []struct {
		name string
		seq  string
		want string
	}{
		{name: "OSC 0 BEL terminated", seq: "\x1b]0;zero title\x07", want: "zero title"},
		{name: "OSC 0 ST terminated", seq: "\x1b]0;zero title\x1b\\", want: "zero title"},
		{name: "OSC 2 BEL terminated", seq: "\x1b]2;two title\x07", want: "two title"},
		{name: "OSC 2 ST terminated", seq: "\x1b]2;two title\x1b\\", want: "two title"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 2)
			bells := 0
			s.OnBell = func() { bells++ }
			s.ClearDamage()

			s.Write([]byte(tt.seq))

			require.Equal(t, tt.want, s.TerminalTitle())
			require.Equal(t, 0, bells)
			require.Empty(t, s.Damage())
			require.Equal(t, strings.Repeat(" ", s.Frame.Width), rowString(s.Frame.Row(0)))
		})
	}
}

func TestTerminalTitleHandlesSplitWritesReplacementAndClear(t *testing.T) {
	s := NewScreen(10, 2)

	s.Write([]byte("\x1b]0;split"))
	s.Write([]byte(" title\x1b"))
	require.Empty(t, s.TerminalTitle())
	s.Write([]byte("\\"))
	require.Equal(t, "split title", s.TerminalTitle())

	s.Write([]byte("\x1b]2;replacement\x07"))
	require.Equal(t, "replacement", s.TerminalTitle())

	s.Write([]byte("\x1b]0;\x07"))
	require.Empty(t, s.TerminalTitle())
}

func TestTerminalTitleIgnoresUnrelatedAndOversizedOSC(t *testing.T) {
	s := NewScreen(10, 2)
	s.Write([]byte("\x1b]0;retained\x07"))

	for _, seq := range []string{
		"\x1b]1;icon title\x07",
		"\x1b]20;not selector two\x1b\\",
		"\x1b]777;other;x;y\x07",
	} {
		s.Write([]byte(seq))
		require.Equal(t, "retained", s.TerminalTitle())
	}

	payload := strings.Repeat("x", maxEscapeBufferLen+1)
	s.Write([]byte("\x1b]2;" + payload))
	require.Equal(t, "retained", s.TerminalTitle())
	require.Empty(t, s.escapeBuf)
}

func TestOnNotifyOSC9(t *testing.T) {
	s := NewScreen(10, 2)
	var gotTitle, gotBody string
	calls := 0
	s.OnNotify = func(title, body string) { gotTitle, gotBody = title, body; calls++ }
	s.Write([]byte("\x1b]9;agent done\x07"))
	require.Equal(t, 1, calls)
	require.Equal(t, "", gotTitle)
	require.Equal(t, "agent done", gotBody)
}

func TestOSC9ProgressTransitions(t *testing.T) {
	tests := []struct {
		name string
		seqs []string
		want []bool
	}{
		{
			name: "finish fires once",
			seqs: []string{"9;4;1;50", "9;4;0;100", "9;4;0;100"},
			want: []bool{false},
		},
		{
			name: "error fires once repeat no fire",
			seqs: []string{"9;4;2;0", "9;4;2;50"},
			want: []bool{true},
		},
		{
			name: "indeterminate clear fires",
			seqs: []string{"9;4;3", "9;4;0"},
			want: []bool{false},
		},
		{
			name: "paused clear fires",
			seqs: []string{"9;4;4;80", "9;4;0;80"},
			want: []bool{false},
		},
		{
			name: "ongoing updates silent",
			seqs: []string{"9;4;1;10", "9;4;1;50", "9;4;3;90", "9;4;4;90"},
		},
		{
			name: "bare clear silent",
			seqs: []string{"9;4;0"},
		},
		{
			name: "bare progress ignored",
			seqs: []string{"9;4"},
		},
		{
			name: "unparseable ignored",
			seqs: []string{"9;4;bogus", "9;4;99", "9;4;1", "9;4;0"},
			want: []bool{false},
		},
		{
			name: "error active clear fires error then finish",
			seqs: []string{"9;4;2;0", "9;4;1;50", "9;4;0;100"},
			want: []bool{true, false},
		},
		{
			name: "error clear only fires error",
			seqs: []string{"9;4;2;0", "9;4;0;100"},
			want: []bool{true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(10, 2)
			var got []bool
			s.OnProgress = func(errored bool) { got = append(got, errored) }
			for _, seq := range tt.seqs {
				s.Write([]byte("\x1b]" + seq + "\x07"))
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestOSC9ProgressSplitAcrossWritesFiresOnce(t *testing.T) {
	s := NewScreen(10, 2)
	calls := 0
	s.OnProgress = func(bool) { calls++ }
	s.Write([]byte("\x1b]9;4;1"))
	s.Write([]byte(";50\x07\x1b]9;4;0;100\x07"))
	require.Equal(t, 1, calls)
}

func TestOSC9ProgressNilCallbackDoesNotPanic(t *testing.T) {
	s := NewScreen(10, 2)
	require.NotPanics(t, func() {
		s.Write([]byte("\x1b]9;4;1;50\x07"))
		s.Write([]byte("\x1b]9;4;0;100\x07"))
		s.Write([]byte("\x1b]9;4;2;0\x07"))
	})
}

func TestOSC9ProgressSTTerminated(t *testing.T) {
	s := NewScreen(10, 2)
	var got []bool
	s.OnProgress = func(errored bool) { got = append(got, errored) }
	s.Write([]byte("\x1b]9;4;1;50\x1b\\\x1b]9;4;0;100\x1b\\"))
	require.Equal(t, []bool{false}, got)
}

func TestOSC9ProgressStateTrackedBeforeCallbackSet(t *testing.T) {
	s := NewScreen(10, 2)
	s.Write([]byte("\x1b]9;4;1;50\x07"))
	var got []bool
	s.OnProgress = func(errored bool) { got = append(got, errored) }
	s.Write([]byte("\x1b]9;4;0;100\x07"))
	require.Equal(t, []bool{false}, got)
}

func TestOnNotifyIgnoresOSC9Progress(t *testing.T) {
	s := NewScreen(10, 2)
	calls := 0
	s.OnNotify = func(string, string) { calls++ }
	s.Write([]byte("\x1b]9;4;1;50\x07"))
	s.Write([]byte("\x1b]9;4;0;100\x07"))
	s.Write([]byte("\x1b]9;4;2;0\x07"))
	require.Equal(t, 0, calls)
}

func TestOnNotifyOSC940RemainsGeneric(t *testing.T) {
	s := NewScreen(10, 2)
	var gotTitle, gotBody string
	calls := 0
	s.OnNotify = func(title, body string) { gotTitle, gotBody = title, body; calls++ }
	s.Write([]byte("\x1b]9;40;not progress\x07"))
	require.Equal(t, 1, calls)
	require.Equal(t, "", gotTitle)
	require.Equal(t, "40;not progress", gotBody)
}

func TestOnNotifyOSC777STTerminated(t *testing.T) {
	s := NewScreen(10, 2)
	var gotTitle, gotBody string
	calls := 0
	s.OnNotify = func(title, body string) { gotTitle, gotBody = title, body; calls++ }
	s.Write([]byte("\x1b]777;notify;Claude;needs input\x1b\\"))
	require.Equal(t, 1, calls)
	require.Equal(t, "Claude", gotTitle)
	require.Equal(t, "needs input", gotBody)
}

func TestOnNotifyIgnoresOtherOSC(t *testing.T) {
	s := NewScreen(10, 2)
	calls := 0
	s.OnNotify = func(string, string) { calls++ }
	s.Write([]byte("\x1b]0;window title\x07"))  // title: discarded
	s.Write([]byte("\x1b]777;other;x;y\x1b\\")) // not "notify": discarded
	require.Equal(t, 0, calls)
}

func TestOnNotifySplitAcrossWrites(t *testing.T) {
	s := NewScreen(10, 2)
	calls := 0
	s.OnNotify = func(string, string) { calls++ }
	s.Write([]byte("\x1b]9;par"))
	s.Write([]byte("tial\x07"))
	require.Equal(t, 1, calls)
}

func TestOnClipboardBELTerminated(t *testing.T) {
	s := NewScreen(10, 2)
	var got string
	calls := 0
	s.OnClipboard = func(b64 string) { got = b64; calls++ }
	s.Write([]byte("\x1b]52;c;aGVsbG8=\x07"))
	require.Equal(t, 1, calls)
	require.Equal(t, "aGVsbG8=", got)
}

func TestOnClipboardSTTerminated(t *testing.T) {
	s := NewScreen(10, 2)
	var got string
	calls := 0
	s.OnClipboard = func(b64 string) { got = b64; calls++ }
	s.Write([]byte("\x1b]52;c;aGVsbG8=\x1b\\"))
	require.Equal(t, 1, calls)
	require.Equal(t, "aGVsbG8=", got)
}

func TestOnClipboardQueryIgnored(t *testing.T) {
	s := NewScreen(10, 2)
	calls := 0
	s.OnClipboard = func(string) { calls++ }
	s.Write([]byte("\x1b]52;c;?\x07"))
	require.Equal(t, 0, calls)
}

func TestOnClipboardEmptySelection(t *testing.T) {
	s := NewScreen(10, 2)
	var got string
	calls := 0
	s.OnClipboard = func(b64 string) { got = b64; calls++ }
	s.Write([]byte("\x1b]52;;aGVsbG8=\x07"))
	require.Equal(t, 1, calls)
	require.Equal(t, "aGVsbG8=", got)
}

func TestOnClipboardNilCallbackDoesNotPanic(t *testing.T) {
	s := NewScreen(10, 2)
	require.NotPanics(t, func() {
		s.Write([]byte("\x1b]52;c;aGVsbG8=\x07"))
	})
}

func TestOnClipboardSplitAcrossWrites(t *testing.T) {
	s := NewScreen(10, 2)
	var got string
	calls := 0
	s.OnClipboard = func(b64 string) { got = b64; calls++ }
	s.Write([]byte("\x1b]52;c;aGVs"))
	s.Write([]byte("bG8=\x07"))
	require.Equal(t, 1, calls)
	require.Equal(t, "aGVsbG8=", got)
}

func TestOnClipboardNoSecondSemicolonIgnored(t *testing.T) {
	s := NewScreen(10, 2)
	calls := 0
	s.OnClipboard = func(string) { calls++ }
	s.Write([]byte("\x1b]52;c\x07")) // no second ";" -> no data field
	require.Equal(t, 0, calls)
}

func TestOSCSequences(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "BEL-terminated OSC is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]0;~/Projects/ymux - fish\x07fish$ "))

				assertCell(t, s, 0, 0, 'f')
				assertCell(t, s, 1, 0, 'i')
				assertCell(t, s, 2, 0, 's')
				assertCell(t, s, 3, 0, 'h')
				assertCell(t, s, 4, 0, '$')
				assertCell(t, s, 5, 0, ' ')
				if s.Col != 6 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=6 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "ST-terminated OSC is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]133;A;click_events=1\x1b\\> "))

				assertCell(t, s, 0, 0, '>')
				assertCell(t, s, 1, 0, ' ')
				if s.Col != 2 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=2 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "multiple OSC sequences in a row are all ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]7;file://host/tmp\x07\x1b]11;?\\=\x07\x1b]133;B\x1b\\prompt"))

				assertCell(t, s, 0, 0, 'p')
				assertCell(t, s, 1, 0, 'r')
				assertCell(t, s, 2, 0, 'o')
				assertCell(t, s, 3, 0, 'm')
				assertCell(t, s, 4, 0, 'p')
				assertCell(t, s, 5, 0, 't')
				if s.Col != 6 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=6 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "BEL-terminated OSC split across writes is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]0;~/Projects/ymux"))
				s.Write([]byte(" - fish\x07fish$ "))

				assertCell(t, s, 0, 0, 'f')
				assertCell(t, s, 1, 0, 'i')
				assertCell(t, s, 2, 0, 's')
				assertCell(t, s, 3, 0, 'h')
				assertCell(t, s, 4, 0, '$')
				assertCell(t, s, 5, 0, ' ')
				if s.Col != 6 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=6 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "ST-terminated OSC split across writes is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1b]133;A;click_events=1\x1b"))
				s.Write([]byte("\\> "))

				assertCell(t, s, 0, 0, '>')
				assertCell(t, s, 1, 0, ' ')
				if s.Col != 2 || s.Row != 0 {
					t.Errorf("cursor at col=%d row=%d, want col=2 row=0", s.Col, s.Row)
				}
			},
		},
		{
			name: "unterminated OSC sequence is bounded and dropped",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				payload := strings.Repeat("x", maxEscapeBufferLen+1)
				s.Write([]byte("\x1b]0;" + payload))
				s.Write([]byte("OK"))

				assertCell(t, s, 0, 0, 'O')
				assertCell(t, s, 1, 0, 'K')
				if len(s.escapeBuf) != 0 {
					t.Fatalf("escapeBuf length = %d, want 0 after overlong unterminated OSC", len(s.escapeBuf))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
