package daemon

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

// --- test doubles -----------------------------------------------------------

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestStatusCompositionGolden(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()
	sess.active = 1
	sess.name = "work"

	win := sess.activeTab()
	win.screen = vt.NewScreen(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}
	win.screen.Write([]byte("hello"))

	frame, damage := composeClientFrame(sess, win, true, "")

	require.Equal(t, 12, frame.Width)
	require.Equal(t, 3, frame.Height)
	require.Equal(t, "hello       ", rowText(frame.Row(0)))
	require.Equal(t, " work  1  2 ", rowText(frame.Row(2)))
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
	for i, c := range frame.Row(2) {
		if i >= len(" work  1 ") && i < len(" work  1  2 ") {
			require.True(t, c.Style.Inverse, "active tab segment cell %d should be inverse", i)
		}
	}
}

func TestStatusCompositionUsesTruecolorTheme(t *testing.T) {
	p, release := newBlockingPTY(t)
	_, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	win := sess.activeTab()
	win.screen = vt.NewScreen(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}
	ac.setTheme(themeui.Theme{
		Foreground: renderer.RGB{R: 220, G: 220, B: 220},
		Background: renderer.RGB{R: 10, G: 20, B: 30},
		HasFG:      true,
		HasBG:      true,
		TrueColor:  true,
		Known:      true,
	})

	d := newThemeStyles(ac.getTheme())
	frame, damage := composeClientFrame(sess, win, true, "", d)
	out, err := renderer.New(renderer.Capabilities{}).Draw(frame, damage)

	require.NoError(t, err)
	require.Contains(t, string(out), ";48;2;")
}

func TestStatusApplyThemeStoresClientAndPropagatesScreens(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()
	msg := ports.Theme{HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3}, HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6}}

	d.applyTheme(sess, ac, msg)

	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, ac.getTheme().Foreground)
	for _, tb := range sess.tabs {
		var got []byte
		tb.screen.OnResponse = func(b []byte) { got = append(got, b...) }
		tb.screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
		require.True(t, strings.Contains(string(got), "rgb:0101/0202/0303"), string(got))
		require.True(t, strings.Contains(string(got), "rgb:0404/0505/0606"), string(got))
	}
}

func TestAttachClientClearsStaleScreenDefaultColors(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
	})
	tr, _ := newCapturingTransport(t)

	d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24})

	var got []byte
	tb := sess.activeTab()
	tb.screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.Empty(t, got)
}

func TestApplyThemeIgnoresReplacedClient(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, old, _ := newManualSessionWithPTYs(t, p)
	defer release()
	tr, _ := newCapturingTransport(t)
	d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24})

	d.applyTheme(sess, old, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
	})

	var got []byte
	tb := sess.activeTab()
	tb.screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.Empty(t, got)
}

func TestStatusMarksEphemeralSession(t *testing.T) {
	p, release := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p)
	defer release()
	sess.name = "0"
	sess.ephemeral = true
	win := sess.activeTab()
	win.screen = vt.NewScreen(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}

	frame, _ := composeClientFrame(sess, win, true, "")

	require.Equal(t, " 0*  1      ", rowText(frame.Row(2)))
}

func TestStatusCopyFeedbackRendersOnlyWhenFullyFits(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	sess.name = "work"
	win := sess.activeTab()
	win.screen = vt.NewScreen(30, 2)
	win.size = domain.Size{Cols: 30, Rows: 2}

	frame, _ := composeClientFrame(sess, win, true, "ok")
	require.Equal(t, " work  1                    ok", rowText(frame.Row(2)))

	frame, _ = composeClientFrame(sess, win, true, "1234567890123456789")
	require.Equal(t, " work  1   1234567890123456789", rowText(frame.Row(2)))

	frame, _ = composeClientFrame(sess, win, true, "selection too large to copy")
	require.Equal(t, " work  1                      ", rowText(frame.Row(2)))
}

func TestStatusRepaintsOnCreateSwitchAndResize(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2), stubClock{})
	tr, sends, releaseConn := newConn(t,
		mustHello(ports.IntentNew, "work", domain.Size{Cols: 20, Rows: 5}),
		frameInput([]byte("\x1b ")),
		frameInput([]byte("CNT\r")),
		frameInput([]byte("\x1b1")),
		ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: domain.Size{Cols: 22, Rows: 6}})},
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	first := awaitFrame(t, sends, ports.MsgOutput)
	palette := awaitFrame(t, sends, ports.MsgOutput)
	created := awaitFrame(t, sends, ports.MsgOutput)
	switched := awaitFrame(t, sends, ports.MsgOutput)
	resized := awaitFrame(t, sends, ports.MsgOutput)

	_ = palette
	for _, f := range []ports.Frame{first, created, switched, resized} {
		out, err := ports.UnmarshalOutput(f.Payload)
		require.NoError(t, err)
		require.Contains(t, string(out.Data), "work")
		require.Contains(t, string(out.Data), ";7m", "active status tab should be inverse-highlighted")
	}

	releaseConn()
	releasePTY1()
	releasePTY2()
	hg.Wait()
	d.sessWg.Wait()
}
