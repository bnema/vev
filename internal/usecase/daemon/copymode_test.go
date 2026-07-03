package daemon

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/pkg/renderer"
)

// --- test doubles -----------------------------------------------------------

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestScrollbackModeAltUInterceptsAndDoesNotForward(t *testing.T) {
	writes := make(chan []byte, 1)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].scrollback = scopy.NewScrollback(4)
	sess.tabs[0].screen.Write([]byte("live"))

	d.handleInput(sess, ac, []byte("\x1bu"))

	if ac.copyMode == nil {
		t.Fatal("scrollback mode not entered")
	}
	select {
	case got := <-writes:
		t.Fatalf("scrollback binding forwarded to PTY: %q", got)
	default:
	}
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if got := string(msg.Data); !strings.Contains(got, "[SCROLL]") || strings.Contains(got, "[COPY]") {
		t.Fatalf("scrollback mode paint = %q, want [SCROLL] without [COPY]", got)
	}

	d.handleInput(sess, ac, []byte(" "))
	out = awaitFrame(t, sends, ports.MsgOutput)
	msg, err = ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if got := string(msg.Data); !strings.Contains(got, "[VISUAL]") || strings.Contains(got, "[SCROLL]") {
		t.Fatalf("visual selection paint = %q, want [VISUAL] without [SCROLL]", got)
	}
}

func TestCopyModeInputNotForwardedAndOSC52Copy(t *testing.T) {
	writes := make(chan []byte, 1)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].scrollback = scopy.NewScrollback(4)
	sess.tabs[0].scrollback.Append(testRow("old1    "))
	sess.tabs[0].scrollback.Append(testRow("old2    "))
	sess.tabs[0].screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte{'g', ' ', 'j', 'y'})

	select {
	case got := <-writes:
		t.Fatalf("copy-mode navigation forwarded to PTY: %q", got)
	default:
	}
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if got, want := string(msg.Data), "\x1b]52;c;b2xkMQpvbGQy\x07"; got != want {
		t.Fatalf("OSC52 = %q, want %q", got, want)
	}
	if ac.copyMode != nil {
		t.Fatal("copy mode still active after yank")
	}
	live := awaitFrame(t, sends, ports.MsgOutput)
	liveMsg, err := ports.UnmarshalOutput(live.Payload)
	require.NoError(t, err)
	if strings.Contains(string(liveMsg.Data), "[COPY]") || strings.Contains(string(liveMsg.Data), "[SCROLL]") {
		t.Fatalf("live repaint still contains copy/scroll status: %q", string(liveMsg.Data))
	}
	if !strings.Contains(string(liveMsg.Data), "copied 9 chars to clipboard") {
		t.Fatalf("live repaint = %q, want copy success feedback", string(liveMsg.Data))
	}
	if !strings.Contains(string(liveMsg.Data), "live") {
		t.Fatalf("live repaint = %q, want live screen", string(liveMsg.Data))
	}

	d.paint(sess, ac, true)
	followup := awaitFrame(t, sends, ports.MsgOutput)
	followupMsg, err := ports.UnmarshalOutput(followup.Payload)
	require.NoError(t, err)
	if strings.Contains(string(followupMsg.Data), "copied 9 chars to clipboard") {
		t.Fatalf("copy feedback persisted after next repaint: %q", string(followupMsg.Data))
	}
}

func TestScrollbackEvictionFeedsCopyModeYank(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	reads := make(chan []byte, 64)
	readDone := make(chan struct{})
	var closeOnce sync.Once
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		select {
		case data := <-reads:
			return copy(b, data), nil
		case <-readDone:
			return 0, io.EOF
		}
	}).Maybe()
	p.EXPECT().Write(mock.Anything).Return(0, errors.New("unexpected PTY write")).Maybe()
	p.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	p.EXPECT().Close().RunAndReturn(func() error { closeOnce.Do(func() { close(readDone) }); return nil }).Maybe()
	p.EXPECT().Pid().Return(4242).Maybe()

	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr, sends, releaseConn := newConn(t, mustHello(ports.IntentNew, "work", domain.Size{Cols: 16, Rows: 5}))
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgOutput)

	for i := range 12 {
		reads <- fmt.Appendf(nil, "line-%02d\r\n", i)
	}
	require.Eventually(t, func() bool {
		sess := firstSession(d)
		if sess == nil {
			return false
		}
		win := sess.activeTab()
		if win == nil {
			return false
		}
		win.mu.Lock()
		defer win.mu.Unlock()
		return len(scopy.NewSnapshot(win.scrollback, win.screen.Frame).Rows) >= 12
	}, 2*time.Second, 5*time.Millisecond)

	sess := firstSession(d)
	require.NotNil(t, sess)
	ac := sess.client
	require.NotNil(t, ac)
	d.handleInput(sess, ac, []byte("\x1bu"))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte{'g', ' ', 'G', 'y'})

	var payload string
	require.Eventually(t, func() bool {
		out := awaitFrame(t, sends, ports.MsgOutput)
		msg, err := ports.UnmarshalOutput(out.Payload)
		require.NoError(t, err)
		payload = string(msg.Data)
		return strings.HasPrefix(payload, "\x1b]52;c;")
	}, 2*time.Second, 5*time.Millisecond)
	require.True(t, strings.HasPrefix(payload, "\x1b]52;c;"), "OSC52 payload = %q", payload)
	require.True(t, strings.HasSuffix(payload, "\a"), "OSC52 payload = %q", payload)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(strings.TrimPrefix(payload, "\x1b]52;c;"), "\a"))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(decoded), "line-00"), "decoded yank = %q", string(decoded))

	releaseConn()
	closeOnce.Do(func() { close(readDone) })
	hg.Wait()
	d.sessWg.Wait()
}

func TestCopyModeEscapeRestoresLiveFullRepaint(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].scrollback = scopy.NewScrollback(4)
	sess.tabs[0].screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("q"))

	if ac.copyMode != nil {
		t.Fatal("copy mode still active after q")
	}
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if strings.Contains(string(msg.Data), "[COPY]") || !strings.Contains(string(msg.Data), "live") {
		t.Fatalf("exit repaint = %q, want live full repaint without copy status", string(msg.Data))
	}
}

func TestCopyModeSplitArrowDoesNotExit(t *testing.T) {
	cases := []struct {
		name       string
		input      [][]byte
		wantCursor int
	}{
		{name: "escape then up arrow", input: [][]byte{[]byte("\x1b"), []byte("[A")}, wantCursor: 23},
		{name: "escape then down arrow", input: [][]byte{[]byte("g"), []byte("\x1b"), []byte("[B")}, wantCursor: 1},
		{name: "split up arrow", input: [][]byte{[]byte("\x1b["), []byte("A")}, wantCursor: 23},
		{name: "split page down", input: [][]byte{[]byte("g"), []byte("\x1b[6"), []byte("~")}, wantCursor: 23},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newBlockingPTY(t)
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			sess.tabs[0].scrollback = scopy.NewScrollback(4)
			sess.tabs[0].scrollback.Append(testRow("old1    "))
			sess.tabs[0].scrollback.Append(testRow("old2    "))
			sess.tabs[0].screen.Write([]byte("live"))

			d.enterCopyMode(sess, ac)
			awaitFrame(t, sends, ports.MsgOutput)
			for _, input := range tc.input {
				d.handleInput(sess, ac, input)
			}

			require.NotNil(t, ac.copyMode)
			require.Equal(t, tc.wantCursor, ac.copyMode.Cursor)
		})
	}
}

func TestCopyModeOversizedYankShowsTooLargeFeedback(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].scrollback = scopy.NewScrollback(1)
	longLine := strings.Repeat("x", scopy.OSC52MaxPayloadBytes+1)
	sess.tabs[0].screen.Frame = renderer.NewFrame(len(longLine), 1)
	copy(sess.tabs[0].screen.Frame.Row(0), testRow(longLine))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte{' ', 'y'})

	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if strings.Contains(string(msg.Data), "\x1b]52;") {
		t.Fatalf("oversized yank emitted OSC52: %q", string(msg.Data))
	}
	if !strings.Contains(string(msg.Data), "selection too large to copy") {
		t.Fatalf("oversized yank repaint = %q, want too-large feedback", string(msg.Data))
	}
}

func TestCopyModeLoneEscapeExitsAfterDelay(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	sess.tabs[0].scrollback = scopy.NewScrollback(4)
	sess.tabs[0].screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	ac.copyMu.Lock()
	require.NotNil(t, ac.copyMode)
	ac.copyMu.Unlock()
	timer.ch <- time.Now()
	require.Eventually(t, func() bool {
		ac.copyMu.Lock()
		defer ac.copyMu.Unlock()
		return ac.copyMode == nil
	}, time.Second, 5*time.Millisecond)
}

func TestCopyModePendingEscapeDoesNotCloseNewMode(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	sess.tabs[0].scrollback = scopy.NewScrollback(4)
	sess.tabs[0].screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)

	timer.ch <- time.Now()
	require.Never(t, func() bool {
		ac.copyMu.Lock()
		defer ac.copyMu.Unlock()
		return ac.copyMode == nil
	}, 50*time.Millisecond, 5*time.Millisecond)
	ac.copyMu.Lock()
	require.Nil(t, ac.copyESC.timer)
	require.Nil(t, ac.copyESC.done)
	require.Empty(t, ac.copyPending)
	ac.copyMu.Unlock()
}

func TestCopyModeEmptyYankDoesNotClearClipboard(t *testing.T) {
	cases := []struct {
		name  string
		input []byte
	}{
		{name: "yank", input: []byte("y")},
		{name: "enter", input: []byte("\r")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newBlockingPTY(t)
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			sess.tabs[0].scrollback = scopy.NewScrollback(4)
			sess.tabs[0].screen.Write([]byte("live"))

			d.enterCopyMode(sess, ac)
			awaitFrame(t, sends, ports.MsgOutput)
			d.handleInput(sess, ac, tc.input)

			if ac.copyMode != nil {
				t.Fatal("copy mode still active after empty yank")
			}
			out := awaitFrame(t, sends, ports.MsgOutput)
			msg, err := ports.UnmarshalOutput(out.Payload)
			require.NoError(t, err)
			if strings.Contains(string(msg.Data), "\x1b]52;") {
				t.Fatalf("empty yank emitted OSC52 clipboard clear: %q", string(msg.Data))
			}
			if strings.Contains(string(msg.Data), "[COPY]") || !strings.Contains(string(msg.Data), "live") {
				t.Fatalf("empty yank repaint = %q, want live full repaint", string(msg.Data))
			}
		})
	}
}

func TestCopyModeEnterExitConcurrentWithPaintRace(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].scrollback = scopy.NewScrollback(4)
	sess.tabs[0].screen.Write([]byte("live"))

	done := make(chan struct{})
	var drain sync.WaitGroup
	drain.Go(func() {
		for {
			select {
			case <-sends:
			case <-done:
				return
			}
		}
	})

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			d.enterCopyMode(sess, ac)
			d.handleInput(sess, ac, []byte("q"))
		})
		wg.Go(func() {
			d.paint(sess, ac, true)
		})
	}
	wg.Wait()
	close(done)
	drain.Wait()
}

func TestCursorTailHidesInCopyMode(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	data := mustOutputData(t, sends)
	require.Contains(t, string(data), "\x1b[?25l")
}
