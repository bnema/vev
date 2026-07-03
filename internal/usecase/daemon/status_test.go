package daemon

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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
		frameInput([]byte("\x1bc")),
		frameInput([]byte("\x1b1")),
		ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: domain.Size{Cols: 22, Rows: 6}})},
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	first := awaitFrame(t, sends, ports.MsgOutput)
	created := awaitFrame(t, sends, ports.MsgOutput)
	switched := awaitFrame(t, sends, ports.MsgOutput)
	resized := awaitFrame(t, sends, ports.MsgOutput)

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
