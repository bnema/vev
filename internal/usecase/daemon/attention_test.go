package daemon

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/pkg/renderer"
)

func TestNoteAttentionFlagsBackgroundTab(t *testing.T) {
	d, sess, _, _, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	sess.active = 0

	d.noteAttention(sess, sess.tabs[1])

	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.True(t, sess.tabs[1].attention)
	require.False(t, sess.tabs[1].attentionAt.IsZero())
	require.False(t, sess.tabs[0].attention)
}

func TestNoteAttentionIgnoresVisibleTab(t *testing.T) {
	d, sess, _, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	sess.active = 0

	d.noteAttention(sess, sess.tabs[0])

	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.False(t, sess.tabs[0].attention)
	require.True(t, sess.tabs[0].attentionAt.IsZero())
}

func TestNoteAttentionFlagsDetachedActiveTab(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	sess.active = 0
	require.True(t, sess.detachIfCurrent(ac))

	d.noteAttention(sess, sess.tabs[0])

	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.True(t, sess.tabs[0].attention)
	require.False(t, sess.tabs[0].attentionAt.IsZero())
}

func TestPTYReaderBellMarksBackgroundTab(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2 := newScriptPTY([][]byte{[]byte("\a")})
	d, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	sess.active = 0

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[1], sess.tabs[1].focusedPane())

	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.tabs[1].attention && !sess.tabs[1].attentionAt.IsZero()
	}, 2*time.Second, 5*time.Millisecond)

	_ = p2.Close()
	d.sessWg.Wait()
}

// TestNoteAttentionDoesNotBlockOnWedgedOtherClient is the regression test for
// the reader-blocks-on-a-slow-client bug: noteAttention runs on the PTY
// reader goroutine (see ptyReader in render.go), so it must never repaint
// synchronously — paint ends in Transport.Send, which has no deadline, and a
// stalled client's socket would otherwise stall the reader of an unrelated
// session. A second session's client whose Send never returns must not stop
// noteAttention from returning promptly.
func TestNoteAttentionDoesNotBlockOnWedgedOtherClient(t *testing.T) {
	d, sess, _, _, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	sess.active = 0

	trW := portsmocks.NewMockTransport(t)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	trW.EXPECT().Send(mock.Anything).RunAndReturn(func(ports.Frame) error {
		<-block
		return nil
	}).Maybe()
	trW.EXPECT().Close().Return(nil).Maybe()
	acW := &attachedClient{tr: trW, rend: renderer.New(renderer.Capabilities{}), size: domain.Size{Cols: 80, Rows: 24}}
	sctxW, cancelW := context.WithCancel(d.serveCtx)
	t.Cleanup(cancelW)
	tabW := newTestTabWithContext(newScriptPTY(nil), sctxW, cancelW)
	sessW := &session{id: "wedged", name: "wedged", ctx: sctxW, cancel: cancelW, tabs: []*tab{tabW}, client: acW}
	acW.setSession(sessW)
	acW.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: acW})
	d.sessions[sessW.id] = sessW

	done := make(chan struct{})
	go func() {
		d.noteAttention(sess, sess.tabs[1])
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("noteAttention blocked on a wedged other-session client's Send")
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.True(t, sess.tabs[1].attention)
}

type scriptPTY struct {
	mu     sync.Mutex
	reads  [][]byte
	closed chan struct{}
	once   sync.Once
}

func newScriptPTY(reads [][]byte) *scriptPTY {
	return &scriptPTY{reads: reads, closed: make(chan struct{})}
}

func (p *scriptPTY) Read(b []byte) (int, error) {
	p.mu.Lock()
	if len(p.reads) > 0 {
		next := p.reads[0]
		p.reads = p.reads[1:]
		p.mu.Unlock()
		return copy(b, next), nil
	}
	p.mu.Unlock()
	<-p.closed
	return 0, io.EOF
}

func (p *scriptPTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *scriptPTY) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}
func (p *scriptPTY) Resize(domain.Size) error     { return nil }
func (p *scriptPTY) Pid() int                     { return 4242 }
func (p *scriptPTY) ForegroundPgid() (int, error) { return 4242, nil }

func TestAckAttentionClearsOnlyPaintedVisibleTab(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	sess.active = 0
	sess.mu.Lock()
	sess.tabs[0].attention = true
	sess.tabs[0].attentionAt = time.Unix(1, 0)
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(2, 0)
	sess.mu.Unlock()

	d.paint(sess, ac, true)
	_ = mustOutputData(t, sends)
	// The deferred repaint (from ackAttention) targets all attached clients,
	// including this one, but the bars it would draw are already exactly what
	// the full paint above just sent: no second frame should follow.
	select {
	case f := <-sends:
		t.Fatalf("unexpected extra frame after ack: %v", f.Type)
	case <-time.After(50 * time.Millisecond):
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.False(t, sess.tabs[0].attention)
	require.True(t, sess.tabs[0].attentionAt.IsZero())
	require.True(t, sess.tabs[1].attention)
	require.False(t, sess.tabs[1].attentionAt.IsZero())
}

func TestPaintPreservesBackgroundAttention(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	sess.active = 0
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(2, 0)
	sess.mu.Unlock()
	d.setAttentionFrame(1)

	d.paint(sess, ac, true)
	data := mustOutputData(t, sends)

	require.Contains(t, string(data), "")
	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.True(t, sess.tabs[1].attention)
}

func TestSwitchTabClearsAttentionEndToEnd(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	sess.active = 0
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(2, 0)
	sess.mu.Unlock()

	require.True(t, sess.switchTab(1))
	d.paint(sess, ac, true)
	data := mustOutputData(t, sends)
	// As above: the deferred all-clients repaint has nothing new to say about
	// this client's bars, so no second frame follows.
	select {
	case f := <-sends:
		t.Fatalf("unexpected extra frame after ack: %v", f.Type)
	case <-time.After(50 * time.Millisecond):
	}

	require.NotContains(t, string(data), "")
	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.False(t, sess.tabs[1].attention)
	require.True(t, sess.tabs[1].attentionAt.IsZero())
}

func TestCloseRingingTabClearsClientBar(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	sess.active = 0
	sess.mu.Lock()
	ringing := sess.tabs[1]
	ringing.attention = true
	ringing.attentionAt = time.Unix(2, 0)
	sess.mu.Unlock()
	d.setAttentionFrame(1)

	d.paint(sess, ac, true)
	before := mustOutputData(t, sends)
	require.Contains(t, string(before), "")

	d.closeTab(sess, ringing, true)
	after := mustOutputData(t, sends)
	require.NotContains(t, string(after), "")
}

// TestAnimationRepaintConfinedToBarRows is the regression test for the pulse
// animator: repaintAttachedClients must diff against the warm barCache
// (reset=false), so a repaint with nothing new to say sends no frame at all,
// and a repaint following only a pulse-frame advance touches the bar rows
// without re-emitting screen content.
func TestAnimationRepaintConfinedToBarRows(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	sess.active = 0
	sess.tabs[0].focusedPane().screen.Write([]byte("MIDSCREENMARKER"))
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(1, 0)
	sess.mu.Unlock()

	d.paint(sess, ac, true)
	full := mustOutputData(t, sends)
	require.Contains(t, string(full), "MIDSCREENMARKER")

	// Nothing changed: a repaint against the warm cache must send nothing.
	d.repaintAttachedClients(sess)
	select {
	case f := <-sends:
		t.Fatalf("unexpected frame on unchanged repaint: %v", f.Type)
	case <-time.After(50 * time.Millisecond):
	}

	// Advance the pulse frame (only the bells' rendered style changes) and
	// repaint: the bytes sent must not repaint screen content.
	d.advanceAttentionFrame()
	d.repaintAttachedClients(sess)
	data := mustOutputData(t, sends)
	require.NotContains(t, string(data), "MIDSCREENMARKER")
}

// TestComposeClientFrameWithStateAttentionFrameChangeDamagesOnlyBars pins the
// same invariant at the composeClientFrameWithState level: with a warm
// barCache and no screen damage, changing only bars.attentionFrame must
// produce damage confined to row 0 (top bar) and the last row (bottom bar).
func TestComposeClientFrameWithStateAttentionFrameChangeDamagesOnlyBars(t *testing.T) {
	d, sess, _, _, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	sess.active = 0
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(1, 0)
	sess.mu.Unlock()

	tb := sess.tabs[0]
	var cache barCache
	bars := d.barStateFor(sess, "")
	_, damage := composeClientFrameWithState(bars, tb, true, &cache)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
	tb.focusedPane().screen.ClearDamage()

	bars.attentionFrame++
	_, damage = composeClientFrameWithState(bars, tb, false, &cache)
	require.NotEmpty(t, damage)
	lastRow := tb.focusedPane().screen.Frame.Height + 1
	for _, dmg := range damage {
		require.Contains(t, []int{0, lastRow}, dmg.Y, "expected damage confined to bar rows, got y=%d", dmg.Y)
	}
}

// TestCloseRingingTabRefreshesOtherSessionBottomBar covers Fix 2c: closing a
// ringing tab in one session must refresh the bottom bar of a client attached
// to a DIFFERENT session, since that client's bar shows the ringing session's
// bell until it stops.
func TestCloseRingingTabRefreshesOtherSessionBottomBar(t *testing.T) {
	pA1, releaseA1 := newBlockingPTY(t)
	pA2, releaseA2 := newBlockingPTY(t)
	pB, releaseB := newBlockingPTY(t)
	defer releaseA1()
	defer releaseA2()
	defer releaseB()

	d, sessA, _, sendsA := newManualSessionWithPTYs(t, pA1, pA2)
	sessA.name = "ringer"
	ringing := sessA.tabs[1]
	sessA.mu.Lock()
	ringing.attention = true
	ringing.attentionAt = time.Unix(1, 0)
	sessA.mu.Unlock()
	d.setAttentionFrame(1) // pulse frame 0 renders the bell glyph as blank

	trB, sendsB := newCapturingTransport(t)
	acB := &attachedClient{tr: trB, rend: renderer.New(renderer.Capabilities{}), size: domain.Size{Cols: 80, Rows: 24}}
	sctxB, cancelB := context.WithCancel(d.serveCtx)
	t.Cleanup(cancelB)
	tbB := newTestTabWithContext(pB, sctxB, cancelB)
	sessB := &session{id: "sessB", name: "other", ctx: sctxB, cancel: cancelB, tabs: []*tab{tbB}, client: acB}
	acB.setSession(sessB)
	acB.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: acB})
	d.sessions[sessB.id] = sessB

	d.paint(sessB, acB, true)
	before := mustOutputData(t, sendsB)
	require.Contains(t, string(before), string(attentionGlyph))

	d.closeTab(sessA, ringing, true)
	_ = mustOutputData(t, sendsA)

	after := mustOutputData(t, sendsB)
	require.NotContains(t, string(after), string(attentionGlyph))
}

func TestAltAJumpAttentionSelectsOldestLocalTab(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 3)
	defer releases[0]()
	defer releases[1]()
	defer releases[2]()
	sess.active = 0
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(20, 0)
	sess.tabs[2].attention = true
	sess.tabs[2].attentionAt = time.Unix(10, 0)
	sess.mu.Unlock()

	d.handleInput(sess, ac, []byte("\x1ba"))

	require.Equal(t, 2, activeTabIndex(sess))
	_ = mustOutputData(t, sends)
}

func TestJumpAttentionCrossesSessionsWhenNoLocalBells(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	p3, release3 := newBlockingPTY(t)
	defer release1()
	defer release2()
	defer release3()
	d, sess1, ac, sends := newManualSessionWithPTYs(t, p1)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel2()
	tab2a := newTestTabWithContext(p2, sctx2, cancel2)
	tab2b := newTestTabWithContext(p3, sctx2, cancel2)
	tab2a.attention = true
	tab2a.attentionAt = time.Unix(9, 0)
	tab2b.attention = true
	tab2b.attentionAt = time.Unix(5, 0)
	sess2 := &session{
		id:     "other",
		name:   "other",
		ctx:    sctx2,
		cancel: cancel2,
		tabs:   []*tab{tab2a, tab2b},
	}
	d.sessions[sess2.id] = sess2

	d.jumpAttention(sess1, ac)

	require.Same(t, sess2, ac.currentSession())
	require.Same(t, ac, sess2.client)
	require.Nil(t, sess1.client)
	require.Equal(t, 1, activeTabIndex(sess2))
	_ = mustOutputData(t, sends)
}

func TestJumpAttentionNoopsWithNoBells(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	sess.active = 0

	d.jumpAttention(sess, ac)

	require.Same(t, sess, ac.currentSession())
	require.Equal(t, 0, activeTabIndex(sess))
	select {
	case f := <-sends:
		t.Fatalf("unexpected frame on no-op jump: %v", f.Type)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestAttentionAnimatorParksAdvancesAndResets(t *testing.T) {
	d, sess, _, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	clk := newManualAttentionClock()
	d.clock = clk
	sess.active = 0

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.attentionAnimator(ctx)
		close(done)
	}()

	select {
	case <-clk.timers:
		t.Fatal("parked animator requested timer with no attention")
	case <-time.After(20 * time.Millisecond):
	}

	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(1, 0)
	sess.mu.Unlock()
	d.pokeAttentionTicker()

	timer := clk.nextTimer(t)
	timer.fire()
	_ = mustOutputData(t, sends)
	require.Equal(t, 1, d.attentionFrame())

	sess.mu.Lock()
	sess.tabs[1].attention = false
	sess.tabs[1].attentionAt = time.Time{}
	sess.mu.Unlock()
	d.pokeAttentionTicker()

	require.Eventually(t, func() bool { return d.attentionFrame() == 0 }, time.Second, time.Millisecond)
	_ = mustOutputData(t, sends)
	select {
	case <-clk.timers:
		t.Fatal("animator requested new timer after attention cleared")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestPokeAttentionTickerIsNonBlockingCapOne(t *testing.T) {
	d := New(nil, stubClock{}, nil)
	require.Equal(t, 1, cap(d.animWake))
	d.pokeAttentionTicker()
	d.pokeAttentionTicker()
	require.Len(t, d.animWake, 1)
}

type manualAttentionClock struct {
	timers chan *manualAttentionTimer
}

func newManualAttentionClock() *manualAttentionClock {
	return &manualAttentionClock{timers: make(chan *manualAttentionTimer, 8)}
}

func (c *manualAttentionClock) Now() time.Time { return time.Unix(100, 0) }
func (c *manualAttentionClock) NewTimer(time.Duration) ports.Timer {
	t := &manualAttentionTimer{ch: make(chan time.Time, 1)}
	c.timers <- t
	return t
}
func (c *manualAttentionClock) nextTimer(t *testing.T) *manualAttentionTimer {
	t.Helper()
	select {
	case timer := <-c.timers:
		return timer
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for attention timer")
		return nil
	}
}

type manualAttentionTimer struct {
	ch chan time.Time
}

func (t *manualAttentionTimer) C() <-chan time.Time      { return t.ch }
func (t *manualAttentionTimer) Reset(time.Duration) bool { return true }
func (t *manualAttentionTimer) Stop() bool               { return true }
func (t *manualAttentionTimer) fire()                    { t.ch <- time.Now() }
