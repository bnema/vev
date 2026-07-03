package daemon

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// --- test doubles -----------------------------------------------------------

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestAltDigitSwitchesBetweenThreeTabs(t *testing.T) {
	writes1 := make(chan []byte, 1)
	writes2 := make(chan []byte, 1)
	writes3 := make(chan []byte, 1)
	p1, releasePTY1 := newBlockingPTYWithWrites(t, writes1)
	p2, releasePTY2 := newBlockingPTYWithWrites(t, writes2)
	p3, releasePTY3 := newBlockingPTYWithWrites(t, writes3)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2, p3), stubClock{})
	tr, sends, releaseConn := newConn(t,
		mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}),
		frameInput([]byte("\x1b ")),
		frameInput([]byte("CNT\r")),
		frameInput([]byte("\x1b ")),
		frameInput([]byte("CNT\r")),
		frameInput([]byte("\x1b1")),
		frameInput([]byte("A")),
		frameInput([]byte("\x1b2")),
		frameInput([]byte("B")),
		frameInput([]byte("\x1b3")),
		frameInput([]byte("C")),
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgOutput)

	require.Eventually(t, func() bool {
		sessions := listSessions(t, d)
		return len(sessions.Sessions) == 1 && sessions.Sessions[0].Tabs == 3
	}, 2*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return len(writes1) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("A"), <-writes1)
	require.Eventually(t, func() bool { return len(writes2) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("B"), <-writes2)
	require.Eventually(t, func() bool { return len(writes3) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("C"), <-writes3)

	releaseConn()
	releasePTY1()
	releasePTY2()
	releasePTY3()
	hg.Wait()
	d.sessWg.Wait()
}

func TestAltCForwardsToPTY(t *testing.T) {
	writes := make(chan []byte, 2)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)

	d.handleInput(sess, ac, []byte("\x1bc"))

	require.Equal(t, []byte("\x1bc"), <-writes)
	releasePTY()
}

func TestPaletteNextPreviousSwitchActiveTab(t *testing.T) {
	cases := []struct {
		name      string
		start     int
		query     []byte
		wantIndex int
	}{
		{name: "next advances", start: 0, query: []byte("NXT\r"), wantIndex: 1},
		{name: "next wraps", start: 2, query: []byte("NXT\r"), wantIndex: 0},
		{name: "previous moves back", start: 2, query: []byte("PVT\r"), wantIndex: 1},
		{name: "previous wraps", start: 0, query: []byte("PVT\r"), wantIndex: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 3)
			defer func() {
				for _, release := range releases {
					release()
				}
			}()
			sess.active = tc.start

			d.handleInput(sess, ac, []byte("\x1b "))
			d.handleInput(sess, ac, tc.query)

			require.Equal(t, tc.wantIndex, activeTabIndex(sess))
		})
	}
}

func TestAltXClosesActiveTabAndSelectsRemaining(t *testing.T) {
	writes := make(chan []byte, 1)
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	p3, releasePTY3 := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2, p3)
	sess.active = 1

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("CLT\r"))

	require.Equal(t, 1, sessionCount(d))
	require.Len(t, sess.tabs, 2)
	require.Equal(t, 1, activeTabIndex(sess), "closing middle tab selects the next remaining tab")
	d.handleInput(sess, ac, []byte("Z"))
	require.Eventually(t, func() bool { return len(writes) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("Z"), <-writes)

	releasePTY1()
	releasePTY2()
	releasePTY3()
}

func TestAltDDetachesCurrentClient(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("DET\r"))

	require.Nil(t, sess.client)
	f := awaitFrame(t, sends, ports.MsgDetached)
	det, err := ports.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonDetach, det.Reason)
}

func TestAltRPromotesEphemeralSessionPromptlessly(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()
	sess.ephemeral = true
	sess.name = "0"

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("RNS\r"))

	require.False(t, sess.ephemeral)
	require.Equal(t, "0", sess.name)
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestMouseWheelEntersScrollbackModeAndExitsAtBottom(t *testing.T) {
	writes := make(chan []byte, 4)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	win := sess.tabs[0]
	win.screen.Write([]byte("live"))

	d.handleInput(sess, ac, []byte("\x1b[<64;1;1M"))
	data := mustOutputData(t, sends)
	require.NotNil(t, ac.copyMode)
	require.Equal(t, 19, ac.copyMode.Cursor)
	require.Contains(t, string(data), "[SCROLL]")

	d.handleInput(sess, ac, []byte("\x1b[<65;1;1M"))
	mustOutputData(t, sends)
	require.Nil(t, ac.copyMode)

	d.handleInput(sess, ac, []byte("\x1b[<64;1;1Mq"))
	mustOutputData(t, sends)
	mustOutputData(t, sends)
	require.Nil(t, ac.copyMode, "q after wheel in same input must be routed after copy mode is entered")
	select {
	case got := <-writes:
		t.Fatalf("mouse/copy input forwarded to PTY: %q", got)
	default:
	}
}

func TestMouseAltScreenWheelMapsToArrows(t *testing.T) {
	writes := make(chan []byte, 2)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	sess.tabs[0].screen.Write([]byte("\x1b[?1049h"))

	d.handleInput(sess, ac, []byte("\x1b[<64;1;1M"))
	d.handleInput(sess, ac, []byte("\x1b[<65;1;1M"))

	require.Equal(t, []byte("\x1b[A\x1b[A\x1b[A"), <-writes)
	require.Equal(t, []byte("\x1b[B\x1b[B\x1b[B"), <-writes)
	require.Nil(t, ac.copyMode)
}

func TestMouseChildForwardingStatusDropAndPressDrop(t *testing.T) {
	writes := make(chan []byte, 4)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)

	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M"))
	select {
	case got := <-writes:
		t.Fatalf("press without child mouse mode forwarded: %q", got)
	default:
	}

	sess.tabs[0].screen.Write([]byte("\x1b[?1000h"))
	raw := []byte("\x1b[<0;2;3M")
	d.handleInput(sess, ac, raw)
	select {
	case got := <-writes:
		t.Fatalf("SGR report forwarded to child without SGR mode: %q", got)
	default:
	}

	sess.tabs[0].screen.Write([]byte("\x1b[?1006h"))
	d.handleInput(sess, ac, raw)
	require.Equal(t, raw, <-writes)

	d.handleInput(sess, ac, []byte("\x1b[<0;1;24M"))
	select {
	case got := <-writes:
		t.Fatalf("status-row mouse report forwarded: %q", got)
	default:
	}
}

func TestMouseSplitReportPreservesOrder(t *testing.T) {
	writes := make(chan []byte, 2)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].screen.Write([]byte("live"))

	d.handleInput(sess, ac, []byte("\x1b[<64;"))
	d.handleInput(sess, ac, []byte("1;1Mq"))

	mustOutputData(t, sends)
	mustOutputData(t, sends)
	require.Nil(t, ac.copyMode)
	select {
	case got := <-writes:
		t.Fatalf("split mouse/copy bytes forwarded to PTY: %q", got)
	default:
	}
}
