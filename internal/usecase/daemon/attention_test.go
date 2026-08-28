package daemon

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/ui"
)

func TestNoteAttentionFlagsBackgroundTab(t *testing.T) {
	d, sess, _, _, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	selectTestAttachmentTab(sess, 0)

	d.noteAttention(sess, sess.tabs[1])

	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.True(t, sess.tabs[1].attention)
	require.False(t, sess.tabs[1].attentionAt.IsZero())
	require.False(t, sess.tabs[0].attention)
}

func TestNoteAttentionFlagsVisibleTabUntilPainted(t *testing.T) {
	d, sess, _, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	selectTestAttachmentTab(sess, 0)

	d.noteAttention(sess, sess.tabs[0])

	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.True(t, sess.tabs[0].attention)
	require.False(t, sess.tabs[0].attentionAt.IsZero())
}

func TestPaintDoesNotAckActiveAttentionOnBlankPulseFrame(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()
	selectTestAttachmentTab(sess, 0)
	d.setAttentionFrame(0)
	d.noteAttention(sess, sess.tabs[0])

	d.paint(sess, ac, true, nil)
	data := mustOutputData(t, sends)
	require.NotContains(t, string(data), string(ui.AttentionGlyph))

	sess.mu.Lock()
	require.True(t, sess.tabs[0].attention)
	sess.mu.Unlock()

	d.setAttentionFrame(1)
	d.paint(sess, ac, true, nil)
	data = mustOutputData(t, sends)
	require.Contains(t, string(data), string(ui.AttentionGlyph))

	sess.mu.Lock()
	require.False(t, sess.tabs[0].attention)
	sess.mu.Unlock()
}

func TestPTYReaderActiveVisibleAgentNotificationRendersBellBeforeAck(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "bell", data: []byte("\a")},
		{name: "osc notify", data: []byte("\x1b]777;notify;Claude;needs input\x1b\\")},
		{name: "osc progress complete", data: []byte("\x1b]9;4;1;50\a\x1b]9;4;0;100\a")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty := newScriptPTY([][]byte{tt.data})
			d, sess, ac, sends := newManualSessionWithPTYs(t, pty)
			selectTestAttachmentTab(sess, 0)
			d.setAttentionFrame(1)

			d.sessWg.Add(1)
			go d.ptyReader(sess, sess.tabs[0], sess.tabs[0].focusedPane())

			require.Eventually(t, func() bool {
				sess.mu.Lock()
				defer sess.mu.Unlock()
				return sess.tabs[0].attention && !sess.tabs[0].attentionAt.IsZero()
			}, 2*time.Second, 5*time.Millisecond)

			d.paint(sess, ac, true, nil)
			data := mustOutputData(t, sends)
			require.Contains(t, string(data), string(ui.AttentionGlyph))

			sess.mu.Lock()
			require.False(t, sess.tabs[0].attention)
			require.True(t, sess.tabs[0].attentionAt.IsZero())
			sess.mu.Unlock()

			cleared := mustOutputData(t, sends)
			require.NotContains(t, string(cleared), string(ui.AttentionGlyph))

			_ = pty.Close()
			d.sessWg.Wait()
		})
	}
}

func TestNoteAttentionFlagsDetachedActiveTab(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	selectTestAttachmentTab(sess, 0)
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
	selectTestAttachmentTab(sess, 0)

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
	selectTestAttachmentTab(sess, 0)

	trW := portsmocks.NewMockTransport(t)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	trW.EXPECT().Send(mock.Anything).RunAndReturn(func(wire.Frame) error {
		<-block
		return nil
	}).Maybe()
	trW.EXPECT().Close().Return(nil).Maybe()
	acW := &attachedClient{tr: trW, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	acW.initOverlays()
	sctxW, cancelW := context.WithCancel(d.serveCtx)
	t.Cleanup(cancelW)
	tabW := newTestTabWithContext(newScriptPTY(nil), sctxW, cancelW)
	sessW := &session{sessionCore: sessionCore{id: "wedged", name: "wedged", attachments: map[*attachedClient]struct{}{acW: {}}}, ctx: sctxW, cancel: cancelW, tabs: []*tab{tabW}}
	acW.setSession(sessW)
	acW.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: acW}, nil)
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
func (p *scriptPTY) Resize(domain.Geometry) error { return nil }
func (p *scriptPTY) Pid() int                     { return 4242 }
func (p *scriptPTY) ForegroundPgid() (int, error) { return 4242, nil }

func TestAckAttentionClearsOnlyPaintedVisibleTab(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	selectTestAttachmentTab(sess, 0)
	sess.mu.Lock()
	sess.tabs[0].attention = true
	sess.tabs[0].attentionAt = time.Unix(1, 0)
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(2, 0)
	sess.mu.Unlock()

	d.paint(sess, ac, true, nil)
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
	selectTestAttachmentTab(sess, 0)
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(2, 0)
	sess.mu.Unlock()
	d.setAttentionFrame(1)

	d.paint(sess, ac, true, nil)
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
	selectTestAttachmentTab(sess, 0)
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(2, 0)
	sess.mu.Unlock()

	require.True(t, selectTestAttachmentTab(sess, 1))
	d.paint(sess, ac, true, nil)
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
	selectTestAttachmentTab(sess, 0)
	sess.mu.Lock()
	ringing := sess.tabs[1]
	ringing.attention = true
	ringing.attentionAt = time.Unix(2, 0)
	sess.mu.Unlock()
	d.setAttentionFrame(1)

	d.paint(sess, ac, true, nil)
	before := mustOutputData(t, sends)
	require.Contains(t, string(before), "")

	require.NoError(t, d.closeTab(sess, ringing, true))
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
	selectTestAttachmentTab(sess, 0)
	sess.tabs[0].focusedPane().screen.Write([]byte("MIDSCREENMARKER"))
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(1, 0)
	sess.mu.Unlock()

	d.paint(sess, ac, true, nil)
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

// TestComposeFrameAttentionFrameChangeDamagesOnlyBars pins the same invariant
// through the production composition pipeline: with a warm cache and no pane
// damage, changing only bars.attentionFrame damages only the chrome rows.
func TestComposeFrameAttentionFrameChangeDamagesOnlyBars(t *testing.T) {
	d, sess, _, _, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	selectTestAttachmentTab(sess, 0)
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(1, 0)
	sess.mu.Unlock()

	tb := sess.tabs[0]
	pane := tb.focusedPane()
	pane.screen.Write([]byte("ATTENTION-PANE-CONTENT"))
	bars := d.barStateFor(sess, "")
	area := domain.Rect{Width: pane.screen.Frame.Width, Height: pane.screen.Frame.Height}
	placement := layout.Placement{ID: pane.id, Content: area}
	state := capturedRenderState{
		reset:  true,
		layout: capturedTabLayout{area: area, focus: pane.id, placements: []layout.Placement{placement}, valid: true},
		panes: []capturedPaneRenderState{{
			id: pane.id, frame: pane.screen.Frame.Clone(), placement: placement, focused: true,
			damage: []renderer.Damage{renderer.FullRedraw()},
		}},
		bars: bars,
	}
	out := composeFrame(state, composeCacheInput{})
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, out.damage)
	require.Len(t, state.panes, 1)
	require.Contains(t, rowText(out.frame.Row(1)), "ATTENTION-PANE-CONTENT")
	firstContent := rowText(out.frame.Row(1))

	// The pane was included in the warmed cache; it has no new damage when the
	// attention pulse advances, so the subsequent compose must retain it.
	bars.attentionFrame++
	state.reset, state.bars = false, bars
	state.panes[0].damage = nil
	out = composeFrame(state, out.cache)
	damage := out.damage
	require.Len(t, state.panes, 1, "pane state must remain present when it has no damage")
	require.Empty(t, state.panes[0].damage)
	require.Equal(t, firstContent, rowText(out.frame.Row(1)), "unchanged pane content must remain in the composed frame")
	require.Contains(t, rowText(out.frame.Row(1)), "ATTENTION-PANE-CONTENT")
	require.NotEmpty(t, damage)
	lastRow := pane.screen.Frame.Height + 1
	for _, dmg := range damage {
		require.Contains(t, []int{0, lastRow}, dmg.Y, "expected damage confined to bar rows, got y=%d", dmg.Y)
	}
}

func TestAltAJumpAttentionSelectsOldestLocalTab(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 3)
	sess.mu.Lock()
	sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	d.ptys = newBlockingOpenFactory(t, d)
	defer releases[0]()
	defer releases[1]()
	defer releases[2]()
	selectTestAttachmentTab(sess, 0)
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(20, 0)
	sess.tabs[2].attention = true
	sess.tabs[2].attentionAt = time.Unix(10, 0)
	sess.mu.Unlock()

	d.handleInput(sess, ac, []byte("\x1ba"))

	require.Equal(t, 2, testAttachmentTabIndex(sess))
	requireFloatingInitialized(t, testAttachmentTab(sess))
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
	sess2 := &session{sessionCore: sessionCore{id: "other",
		name: "other"}, ctx: sctx2,
		cancel: cancel2,
		tabs:   []*tab{tab2a, tab2b},
	}
	d.sessions[sess2.id] = sess2

	require.NoError(t, d.jumpAttention(sess1, ac))

	require.Same(t, sess2, ac.currentSession())
	require.Contains(t, sess2.snapshotAttachments(), ac)
	require.Empty(t, sess1.snapshotAttachments())
	require.Equal(t, 1, testAttachmentTabIndex(sess2))
	_ = mustOutputData(t, sends)
}

func TestJumpAttentionNoopsWithNoBells(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	selectTestAttachmentTab(sess, 0)

	require.NoError(t, d.jumpAttention(sess, ac), "no target to jump to is not a failure")

	require.Same(t, sess, ac.currentSession())
	require.Equal(t, 0, testAttachmentTabIndex(sess))
	select {
	case f := <-sends:
		t.Fatalf("unexpected frame on no-op jump: %v", f.Type)
	case <-time.After(20 * time.Millisecond):
	}
}

// TestJumpAttentionSwitchFailureReportsNotice drives jumpAttention's
// cross-session path once a target has already been chosen, then makes the
// commit itself fail (the origin session's client no longer matches ac —
// simulating a client that detached between target selection and commit).
// Unlike the no-target case, a target existed and the switch to it failed:
// that is a genuine error, not a benign no-op, and must reach the user.
func TestJumpAttentionSwitchFailureReportsNotice(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()
	d, sess1, ac, _ := newManualSessionWithPTYs(t, p1)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel2()
	tab2 := newTestTabWithContext(p2, sctx2, cancel2)
	tab2.attention = true
	tab2.attentionAt = time.Unix(9, 0)
	sess2 := &session{sessionCore: sessionCore{id: "other", name: "other"}, ctx: sctx2, cancel: cancel2, tabs: []*tab{tab2}}
	d.sessions[sess2.id] = sess2

	sess1.mu.Lock()
	clearAttachmentsForTestLocked(sess1)
	sess1.mu.Unlock()

	err := d.jumpAttention(sess1, ac)

	require.Error(t, err)
	require.Same(t, sess1, ac.currentSession(), "a failed switch must leave the client on its origin session")

	d.reportError(sess1, err)
	history := d.notices.history()
	require.Len(t, history, 1)
	require.Equal(t, domain.NoticeSessionUnavailable, history[0].Code)
}

func TestAttentionAnimatorParksAdvancesAndResets(t *testing.T) {
	d, sess, _, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	clk := newManualAttentionClock()
	d.clock = clk
	selectTestAttachmentTab(sess, 0)

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
