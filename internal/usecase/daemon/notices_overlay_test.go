package daemon

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestNoticesOpenViaPaletteCommandShowsModalWithHistory(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	d.notices.record(domain.Notification{Code: domain.NoticePaneSpawn, Severity: domain.NoticeError, Message: "couldn't open pane", Time: time.Unix(1, 0)})

	d.enterPalette(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handlePaletteInput(ac, []byte("NTC\r"))
	out := awaitFrame(t, sends, ports.MsgOutput)

	require.False(t, ac.overlays.paletteActive())
	require.NotNil(t, ac.overlays.noticesOverlay)
	msg, err := ports.UnmarshalOutput(out.Payload)
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
	awaitFrame(t, sends, ports.MsgOutput)
	selected, ok := ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "third", selected.Message)

	d.handleInput(sess, ac, []byte("j"))
	awaitFrame(t, sends, ports.MsgOutput)
	selected, ok = ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "second", selected.Message)

	d.handleInput(sess, ac, []byte("j"))
	awaitFrame(t, sends, ports.MsgOutput)
	selected, ok = ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "first", selected.Message)

	// One more 'j' at the bottom must clamp rather than wrap.
	d.handleInput(sess, ac, []byte("j"))
	awaitFrame(t, sends, ports.MsgOutput)
	selected, ok = ac.overlays.noticesOverlay.Selected()
	require.True(t, ok)
	require.Equal(t, "first", selected.Message)

	d.handleInput(sess, ac, []byte("k"))
	awaitFrame(t, sends, ports.MsgOutput)
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
			awaitFrame(t, sends, ports.MsgOutput)
			require.True(t, ac.overlays.noticesActive())

			d.handleInput(sess, ac, []byte{key})
			awaitFrame(t, sends, ports.MsgOutput)

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
	awaitFrame(t, sends, ports.MsgOutput)

	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	require.True(t, ac.overlays.noticesActive())

	timer.ch <- time.Now()
	require.Eventually(t, func() bool { return !ac.overlays.noticesActive() }, time.Second, 5*time.Millisecond)
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestNoticesOpenWithEmptyHistoryShowsPlaceholderWithoutPanic(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()

	require.NotPanics(t, func() { d.enterNotices(sess, ac) })
	out := awaitFrame(t, sends, ports.MsgOutput)

	require.NotNil(t, ac.overlays.noticesOverlay)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "Notifications")
	require.Contains(t, string(msg.Data), "No notifications")
}
