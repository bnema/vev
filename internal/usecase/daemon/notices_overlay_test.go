package daemon

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/notices"
	themeui "github.com/bnema/vev/internal/usecase/theme"
)

func TestCaptureNoticesOverlayUsesTerminalBackgroundOutsideSelection(t *testing.T) {
	terminal := renderer.DefaultStyle()
	inactive := renderer.Style{HasBackgroundRGB: true, BackgroundRGB: renderer.RGB{R: 10, G: 20, B: 30}}
	selection := renderer.Style{HasBackgroundRGB: true, BackgroundRGB: renderer.RGB{R: 40, G: 50, B: 60}}
	styles := themeui.Styles{
		PickerBase: terminal, SurfaceInactive: inactive, PickerSelection: selection,
		PickerSelectionName: selection, PickerSelectionMuted: selection,
		PickerDescription: terminal,
	}
	model := notices.New([]domain.Notification{
		{Code: domain.NoticePaneSpawn, Message: "selected", Time: time.Unix(2, 0)},
		{Code: domain.NoticeTabSpawn, Message: "inactive", Time: time.Unix(1, 0)},
	}, time.Unix(3, 0))
	state := capturedRenderState{styles: styles, layout: capturedTabLayout{area: domain.Rect{Width: 100, Height: 38}}}
	snap := &overlayRenderSnapshot{noticesOverlayActive: true, noticesOverlayModel: model}

	captureOverlayLayers(&state, snap, domain.PaletteConfig{})

	inner := state.overlays.noticesOverlay.inner
	require.True(t, inner.At(inner.Width-1, 1).Style.Equal(terminal), "inactive row keeps the terminal background")
	require.True(t, inner.At(inner.Width-1, 2).Style.Equal(terminal), "unused interior keeps the terminal background")
	require.True(t, inner.At(inner.Width-1, 0).Style.Equal(selection), "selected row keeps the accent background")
}

func TestNoticesOpenViaPaletteCommandShowsModalWithHistory(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	d.notices.record(domain.Notification{Code: domain.NoticePaneSpawn, Severity: domain.NoticeError, Message: "couldn't open pane", Time: time.Unix(1, 0)})

	d.enterPalette(sess, ac)
	awaitFrame(t, sends, wire.MsgOutput)
	d.handlePaletteInput(ac, []byte("NTC\r"))
	out := awaitFrame(t, sends, wire.MsgOutput)

	require.False(t, ac.overlays.paletteActive())
	require.NotNil(t, ac.overlays.noticesOverlay)
	msg, err := wire.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "Notifications")
	require.Contains(t, string(msg.Data), "pane-spawn")
	require.Contains(t, string(msg.Data), "couldn't open pane")
}

func TestNoticesJKNavigatesSelection(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	d.notices.record(domain.Notification{Code: domain.NoticePaneSpawn, Message: "first", Time: time.Unix(1, 0)})
	d.notices.record(domain.Notification{Code: domain.NoticeTabSpawn, Message: "second", Time: time.Unix(2, 0)})
	d.notices.record(domain.Notification{Code: domain.NoticeInternal, Message: "third", Time: time.Unix(3, 0)})

	d.enterNotices(sess, ac)
	awaitFrame(t, sends, wire.MsgOutput)
	selected, ok := ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "third", selected.Message)

	d.handleInput(sess, ac, []byte("j"))
	awaitFrame(t, sends, wire.MsgOutput)
	selected, ok = ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "second", selected.Message)

	d.handleInput(sess, ac, []byte("j"))
	awaitFrame(t, sends, wire.MsgOutput)
	selected, ok = ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "first", selected.Message)

	// One more 'j' at the bottom must clamp rather than wrap.
	d.handleInput(sess, ac, []byte("j"))
	awaitFrame(t, sends, wire.MsgOutput)
	selected, ok = ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "first", selected.Message)

	d.handleInput(sess, ac, []byte("k"))
	awaitFrame(t, sends, wire.MsgOutput)
	selected, ok = ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "second", selected.Message)
}

func TestNoticesQAndCtrlCCloseImmediatelyAndClearState(t *testing.T) {
	for _, key := range []byte{'q', 0x03} {
		t.Run(fmt.Sprintf("key=%q", key), func(t *testing.T) {
			p, release := newBlockingPTY(t)
			defer release()
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			d.notices.record(domain.Notification{Code: domain.NoticePaneSpawn, Message: "m", Time: time.Unix(1, 0)})

			d.enterNotices(sess, ac)
			awaitFrame(t, sends, wire.MsgOutput)
			require.True(t, ac.overlays.noticesActive())

			d.handleInput(sess, ac, []byte{key})
			awaitFrame(t, sends, wire.MsgOutput)

			require.False(t, ac.overlays.noticesActive())
			require.Nil(t, ac.overlays.noticesOverlay)
		})
	}
}

func TestNoticesLoneEscapeClosesAfterDelay(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	d.notices.record(domain.Notification{Code: domain.NoticePaneSpawn, Message: "m", Time: time.Unix(1, 0)})

	d.enterNotices(sess, ac)
	awaitFrame(t, sends, wire.MsgOutput)

	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	require.True(t, ac.overlays.noticesActive())

	timer.ch <- time.Now()
	require.Eventually(t, func() bool { return !ac.overlays.noticesActive() }, time.Second, 5*time.Millisecond)
	awaitFrame(t, sends, wire.MsgOutput)
}

func TestNoticesSplitArrowNavigatesWithoutClosing(t *testing.T) {
	cases := []struct {
		name  string
		input [][]byte
		want  string
	}{
		{name: "escape then CSI down", input: [][]byte{[]byte("\x1b"), []byte("[B")}, want: "first"},
		{name: "split CSI down", input: [][]byte{[]byte("\x1b["), []byte("B")}, want: "first"},
		{name: "split SS3 down", input: [][]byte{[]byte("\x1bO"), []byte("B")}, want: "first"},
		{name: "split CSI up", input: [][]byte{[]byte("j"), []byte("\x1b"), []byte("[A")}, want: "second"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			defer release()
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			d.notices.record(domain.Notification{Code: domain.NoticePaneSpawn, Message: "first", Time: time.Unix(1, 0)})
			d.notices.record(domain.Notification{Code: domain.NoticeTabSpawn, Message: "second", Time: time.Unix(2, 0)})

			d.enterNotices(sess, ac)
			awaitFrame(t, sends, wire.MsgOutput)
			for _, input := range tc.input {
				d.handleInput(sess, ac, input)
			}

			require.True(t, ac.overlays.noticesActive())
			selected, ok := ac.overlays.noticesOverlay.Selected()
			require.True(t, ok)
			require.Equal(t, tc.want, selected.Message)
		})
	}
}

func TestHandleListInputConsumesSplitEscapePrefix(t *testing.T) {
	for _, tt := range []struct {
		name     string
		prefix   []byte
		tail     byte
		wantUp   int
		wantDown int
	}{
		{name: "CSI up", prefix: []byte("\x1b["), tail: 'A', wantUp: 1},
		{name: "CSI down", prefix: []byte("\x1b["), tail: 'B', wantDown: 1},
		{name: "SS3 up", prefix: []byte("\x1bO"), tail: 'A', wantUp: 1},
		{name: "SS3 down", prefix: []byte("\x1bO"), tail: 'B', wantDown: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var pending, custom []byte
			var up, down int
			state := listInputState{
				pending:  &pending,
				esc:      &pendingByteTimer{},
				moveUp:   func() { up++ },
				moveDown: func() { down++ },
			}
			action := func(b byte) listInputResult {
				custom = append(custom, b)
				return listInputResult{}
			}

			handleListInputLocked(nil, tt.prefix, state, action)
			handleListInputLocked(nil, []byte{tt.tail}, state, action)

			require.Equal(t, tt.wantUp, up)
			require.Equal(t, tt.wantDown, down)
			require.Empty(t, custom, "escape prefix bytes must not reach the custom action")
			require.Empty(t, pending)
		})
	}
}

func TestHandleListInputConsumesUnsupportedCompleteEscapeSequences(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input [][]byte
	}{
		{name: "complete CSI right", input: [][]byte{[]byte("\x1b[C")}},
		{name: "complete SS3 left", input: [][]byte{[]byte("\x1bOD")}},
		{name: "parameterized CSI", input: [][]byte{[]byte("\x1b[1;5C")}},
		{name: "split CSI right", input: [][]byte{[]byte("\x1b["), []byte("C")}},
		{name: "split SS3 left", input: [][]byte{[]byte("\x1bO"), []byte("D")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var pending, custom []byte
			state := listInputState{
				pending:  &pending,
				esc:      &pendingByteTimer{},
				moveUp:   func() {},
				moveDown: func() {},
			}
			action := func(b byte) listInputResult {
				custom = append(custom, b)
				return listInputResult{action: b}
			}

			var result listInputResult
			for _, input := range tt.input {
				result = handleListInputLocked(nil, input, state, action)
			}

			require.False(t, result.exit)
			require.Zero(t, result.action)
			require.Empty(t, custom, "complete escape bytes must not reach the custom action")
			require.Empty(t, pending)
		})
	}
}

func TestHandleListInputStopsAfterExitInBatchedInput(t *testing.T) {
	for _, tt := range []struct {
		name       string
		input      []byte
		custom     func(byte) listInputResult
		wantExit   bool
		wantStop   bool
		wantAction byte
		wantCalls  []byte
	}{
		{
			name:     "q ignores trailing navigation and actions",
			input:    []byte("qjy"),
			custom:   func(b byte) listInputResult { return listInputResult{action: b} },
			wantExit: true,
		},
		{
			name:     "ctrl-c ignores trailing navigation and actions",
			input:    []byte{0x03, 'j', 'y'},
			custom:   func(b byte) listInputResult { return listInputResult{action: b} },
			wantExit: true,
		},
		{
			name:       "invalid escape exit ignores trailing navigation and actions",
			input:      []byte{'\x1b', 'x', 'j', 'y'},
			custom:     func(b byte) listInputResult { return listInputResult{action: b} },
			wantExit:   true,
			wantCalls:  nil,
			wantAction: 0,
		},
		{
			name:  "custom exit retains action and ignores trailing bytes",
			input: []byte("xjy"),
			custom: func(b byte) listInputResult {
				if b == 'x' {
					return listInputResult{action: b, exit: true}
				}
				return listInputResult{action: b}
			},
			wantExit: true, wantAction: 'x', wantCalls: []byte("x"),
		},
		{
			name:  "custom stop retains action and ignores trailing bytes",
			input: []byte("xjy"),
			custom: func(b byte) listInputResult {
				if b == 'x' {
					return listInputResult{action: b, stop: true}
				}
				return listInputResult{action: b}
			},
			wantStop: true, wantAction: 'x', wantCalls: []byte("x"),
		},
		{
			name:  "custom exit and stop retain both flags and action",
			input: []byte("xjy"),
			custom: func(b byte) listInputResult {
				if b == 'x' {
					return listInputResult{action: b, exit: true, stop: true}
				}
				return listInputResult{action: b}
			},
			wantExit: true, wantStop: true, wantAction: 'x', wantCalls: []byte("x"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var pending, calls []byte
			var down int
			state := listInputState{
				pending:  &pending,
				esc:      &pendingByteTimer{},
				moveUp:   func() {},
				moveDown: func() { down++ },
			}

			result := handleListInputLocked(nil, tt.input, state, func(b byte) listInputResult {
				calls = append(calls, b)
				return tt.custom(b)
			})

			require.Equal(t, tt.wantExit, result.exit)
			require.Equal(t, tt.wantStop, result.stop)
			require.Equal(t, tt.wantAction, result.action)
			require.Equal(t, tt.wantCalls, calls)
			require.Zero(t, down, "trailing j must not be processed")
		})
	}
}

func TestNoticesOpenWithEmptyHistoryShowsPlaceholderWithoutPanic(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()

	require.NotPanics(t, func() { d.enterNotices(sess, ac) })
	out := awaitFrame(t, sends, wire.MsgOutput)

	require.NotNil(t, ac.overlays.noticesOverlay)
	msg, err := wire.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "Notifications")
	require.Contains(t, string(msg.Data), "No notifications")
}

func TestNoticeYankPayload(t *testing.T) {
	at := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name string
		n    domain.Notification
		want string
	}{
		{
			name: "single occurrence, no details",
			n:    domain.Notification{Code: domain.NoticePaneSpawn, Severity: domain.NoticeInfo, Message: "opened pane", Time: at},
			want: "[2024-01-02T03:04:05Z] info pane-spawn\nopened pane\n",
		},
		{
			name: "coalesced count and details",
			n:    domain.Notification{Code: domain.NoticePaneSpawn, Severity: domain.NoticeError, Message: "couldn't open pane", Details: "boom ← permission denied", Count: 3, Time: at},
			want: "[2024-01-02T03:04:05Z] error pane-spawn ×3\ncouldn't open pane\ndetails: boom ← permission denied\n",
		},
		{
			name: "warn severity",
			n:    domain.Notification{Code: domain.NoticeClipboard, Severity: domain.NoticeWarn, Message: "selection too large", Time: at},
			want: "[2024-01-02T03:04:05Z] warn clipboard\nselection too large\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, noticeYankPayload(tt.n))
		})
	}
}

func TestYankLastNotificationCommandCopiesOSC52AndShowsFeedback(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	n := domain.Notification{Code: domain.NoticePaneSpawn, Severity: domain.NoticeError, Message: "couldn't open pane", Details: "boom", Time: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)}
	d.notices.record(n)
	want := noticeYankPayload(n)

	d.enterPalette(sess, ac)
	awaitFrame(t, sends, wire.MsgOutput)
	d.handlePaletteInput(ac, []byte("YLN\r"))

	out := awaitFrame(t, sends, wire.MsgOutput)
	msg, err := wire.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	payload := string(msg.Data)
	require.True(t, strings.HasPrefix(payload, "\x1b]52;c;"), "OSC52 payload = %q", payload)
	require.True(t, strings.HasSuffix(payload, "\a"), "OSC52 payload = %q", payload)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(strings.TrimPrefix(payload, "\x1b]52;c;"), "\a"))
	require.NoError(t, err)
	require.Equal(t, want, string(decoded))

	live := awaitFrame(t, sends, wire.MsgOutput)
	liveMsg, err := wire.UnmarshalOutput(live.Payload)
	require.NoError(t, err)
	require.Contains(t, string(liveMsg.Data), "copied notification details")
}

func TestYankLastNotificationWithEmptyHistoryShowsWarnToast(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()

	d.enterPalette(sess, ac)
	awaitFrame(t, sends, wire.MsgOutput)
	d.handlePaletteInput(ac, []byte("YLN\r"))

	out := awaitFrame(t, sends, wire.MsgOutput)
	msg, err := wire.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "no notifications yet")

	history := d.notices.history()
	require.Len(t, history, 1)
	require.Equal(t, domain.NoticeClipboard, history[0].Code)
	require.Equal(t, domain.NoticeWarn, history[0].Severity)
	require.Equal(t, "no notifications yet", history[0].Message)
}

func TestNoticesYKeyYanksSelectedNotification(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	d.notices.record(domain.Notification{Code: domain.NoticePaneSpawn, Severity: domain.NoticeWarn, Message: "first", Time: time.Unix(1, 0)})
	d.notices.record(domain.Notification{Code: domain.NoticeTabSpawn, Severity: domain.NoticeError, Message: "second", Time: time.Unix(2, 0)})

	d.enterNotices(sess, ac)
	awaitFrame(t, sends, wire.MsgOutput)
	selected, ok := ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "second", selected.Message)
	want := noticeYankPayload(selected)

	d.handleInput(sess, ac, []byte("y"))

	out := awaitFrame(t, sends, wire.MsgOutput)
	msg, err := wire.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	payload := string(msg.Data)
	require.True(t, strings.HasPrefix(payload, "\x1b]52;c;"), "OSC52 payload = %q", payload)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(strings.TrimPrefix(payload, "\x1b]52;c;"), "\a"))
	require.NoError(t, err)
	require.Equal(t, want, string(decoded))

	// The overlay stays open after a quick yank; it isn't a modal commit like
	// copy mode's own 'y'.
	require.True(t, ac.overlays.noticesActive())
}

func TestNoticesBatchedYankCapturesSelectionAtY(t *testing.T) {
	tests := []struct {
		name         string
		input        []byte
		wantYanked   string
		wantSelected string
	}{
		{name: "yank then down", input: []byte("yj"), wantYanked: "third", wantSelected: "second"},
		{name: "down then yank", input: []byte("jy"), wantYanked: "second", wantSelected: "second"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			defer release()
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			d.notices.record(domain.Notification{Code: domain.NoticePaneSpawn, Message: "first", Time: time.Unix(1, 0)})
			d.notices.record(domain.Notification{Code: domain.NoticeTabSpawn, Message: "second", Time: time.Unix(2, 0)})
			d.notices.record(domain.Notification{Code: domain.NoticeInternal, Message: "third", Time: time.Unix(3, 0)})

			d.enterNotices(sess, ac)
			awaitFrame(t, sends, wire.MsgOutput)
			d.handleInput(sess, ac, tt.input)

			out := awaitFrame(t, sends, wire.MsgOutput)
			msg, err := wire.UnmarshalOutput(out.Payload)
			require.NoError(t, err)
			payload := strings.TrimSuffix(strings.TrimPrefix(string(msg.Data), "\x1b]52;c;"), "\a")
			decoded, err := base64.StdEncoding.DecodeString(payload)
			require.NoError(t, err)
			require.Contains(t, string(decoded), "\n"+tt.wantYanked+"\n")

			selected, ok := ac.overlays.noticesOverlay.Selected()
			require.True(t, ok)
			require.Equal(t, tt.wantSelected, selected.Message)
		})
	}
}

func TestNoticesYKeyWithNoSelectionIsNoOp(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()

	require.NotPanics(t, func() { d.enterNotices(sess, ac) })
	awaitFrame(t, sends, wire.MsgOutput)

	require.NotPanics(t, func() { d.handleInput(sess, ac, []byte("y")) })
	require.True(t, ac.overlays.noticesActive())
}
