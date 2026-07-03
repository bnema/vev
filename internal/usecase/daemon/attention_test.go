package daemon

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/vt"
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
	go d.ptyReader(sess, sess.tabs[1])

	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.tabs[1].attention && !sess.tabs[1].attentionAt.IsZero()
	}, 2*time.Second, 5*time.Millisecond)

	_ = p2.Close()
	d.sessWg.Wait()
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
func (p *scriptPTY) Resize(domain.Size) error { return nil }
func (p *scriptPTY) Pid() int                 { return 4242 }

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
	_ = mustOutputData(t, sends) // deferred repaint after acknowledging the visible tab

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
	_ = mustOutputData(t, sends) // deferred repaint after acknowledging the visited tab

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
	sess2 := &session{
		id:     "other",
		name:   "other",
		ctx:    sctx2,
		cancel: cancel2,
		tabs: []*tab{
			{
				pty:         p2,
				screen:      vt.NewScreen(80, 23),
				dirty:       make(chan struct{}, 1),
				size:        domain.Size{Cols: 80, Rows: 23},
				ctx:         sctx2,
				cancel:      cancel2,
				attention:   true,
				attentionAt: time.Unix(9, 0),
			},
			{
				pty:         p3,
				screen:      vt.NewScreen(80, 23),
				dirty:       make(chan struct{}, 1),
				size:        domain.Size{Cols: 80, Rows: 23},
				ctx:         sctx2,
				cancel:      cancel2,
				attention:   true,
				attentionAt: time.Unix(5, 0),
			},
		},
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
	require.Equal(t, 1, d.attentionFrame(time.Time{}))

	sess.mu.Lock()
	sess.tabs[1].attention = false
	sess.tabs[1].attentionAt = time.Time{}
	sess.mu.Unlock()
	d.pokeAttentionTicker()

	require.Eventually(t, func() bool { return d.attentionFrame(time.Time{}) == 0 }, time.Second, time.Millisecond)
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
