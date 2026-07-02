package keys

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

type fakeClock struct{ timers []*fakeTimer }

func (c *fakeClock) Now() time.Time { return time.Time{} }
func (c *fakeClock) NewTimer(d time.Duration) ports.Timer {
	t := &fakeTimer{ch: make(chan time.Time, 1), d: d}
	c.timers = append(c.timers, t)
	return t
}

type fakeTimer struct {
	ch      chan time.Time
	d       time.Duration
	stopped bool
}

func (t *fakeTimer) C() <-chan time.Time        { return t.ch }
func (t *fakeTimer) Reset(d time.Duration) bool { t.d = d; return t.stopped }
func (t *fakeTimer) Stop() bool                 { t.stopped = true; return true }
func (t *fakeTimer) fire()                      { t.ch <- time.Time{} }

type captureHandler struct {
	forwards [][]byte
	actions  []Action
	notify   chan struct{}
}

func (h *captureHandler) Forward(data []byte) {
	h.forwards = append(h.forwards, append([]byte(nil), data...))
	if h.notify != nil {
		h.notify <- struct{}{}
	}
}
func (h *captureHandler) Action(a Action) { h.actions = append(h.actions, a) }

func TestRouterInterceptsSameReadAltBindings(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want Action
	}{
		{"Alt+c", []byte{ESC, 'c'}, ActionCreateWindow},
		{"Alt+n", []byte{ESC, 'n'}, ActionNextWindow},
		{"Alt+p", []byte{ESC, 'p'}, ActionPreviousWindow},
		{"Alt+d", []byte{ESC, 'd'}, ActionDetach},
		{"Alt+x", []byte{ESC, 'x'}, ActionCloseWindow},
		{"Alt+u", []byte{ESC, 'u'}, ActionCopyMode},
		{"Alt+r", []byte{ESC, 'r'}, ActionRenameSession},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			NewRouter(clk, h).Route(tc.in)
			require.Equal(t, []Action{tc.want}, h.actions)
			require.Empty(t, h.forwards)
			require.Empty(t, clk.timers)
		})
	}
}

func TestRouterInterceptsAltDigits(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)
	for b := byte('1'); b <= '9'; b++ {
		r.Route([]byte{ESC, b})
	}
	require.Equal(t, []Action{
		ActionSwitchWindow1, ActionSwitchWindow2, ActionSwitchWindow3,
		ActionSwitchWindow4, ActionSwitchWindow5, ActionSwitchWindow6,
		ActionSwitchWindow7, ActionSwitchWindow8, ActionSwitchWindow9,
	}, h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterForwardsOnlyNonInterceptedBytes(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	NewRouter(clk, h).Route([]byte{'a', ESC, 'c', 'b'})
	require.Equal(t, []Action{ActionCreateWindow}, h.actions)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, h.forwards)
}

func TestRouterPassesThroughTerminalEscapePrefixes(t *testing.T) {
	cases := [][]byte{{ESC, '[', 'A'}, {ESC, 'O', 'P'}}
	for _, in := range cases {
		clk := &fakeClock{}
		h := &captureHandler{}
		NewRouter(clk, h).Route(in)
		require.Empty(t, h.actions)
		require.Equal(t, [][]byte{in}, h.forwards)
		require.Empty(t, clk.timers)
	}
}

func TestRouterRetainsSplitESCAndInterceptsNextBoundByte(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)
	r.Route([]byte{ESC})
	require.Len(t, clk.timers, 1)
	require.Equal(t, ESCDelay, clk.timers[0].d)
	require.Empty(t, h.forwards)

	r.Route([]byte{'n'})
	require.True(t, clk.timers[0].stopped)
	require.Equal(t, []Action{ActionNextWindow}, h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterCancelsPendingESCWaiterWhenNextReadConsumesESC(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)

	before := retainESCWaiters()
	r.Route([]byte{ESC})
	require.Eventually(t, func() bool { return retainESCWaiters() > before }, time.Second, time.Millisecond)

	r.Route([]byte{'n'})
	require.Eventually(t, func() bool { return retainESCWaiters() == before }, time.Second, time.Millisecond)
	require.True(t, clk.timers[0].stopped)
	require.Equal(t, []Action{ActionNextWindow}, h.actions)
	require.Empty(t, h.forwards)
}

func retainESCWaiters() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "github.com/bnema/vev/internal/usecase/keys.(*Router).retainESC.func1")
}

func TestRouterFlushesLoneESCAfterTimer(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{notify: make(chan struct{}, 1)}
	r := NewRouter(clk, h)
	r.Route([]byte{ESC})
	clk.timers[0].fire()
	select {
	case <-h.notify:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for flushed ESC")
	}
	require.Equal(t, [][]byte{{ESC}}, h.forwards)
	require.Empty(t, h.actions)
}

func TestRouterFlushesPendingESCBeforeOtherByte(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)
	r.Route([]byte{ESC})
	r.Route([]byte{'z'})
	require.Equal(t, [][]byte{{ESC}, []byte("z")}, h.forwards)
	require.Empty(t, h.actions)
}

func TestPartialInputSuffixLen(t *testing.T) {
	require.Equal(t, 1, PartialInputSuffixLen([]byte("abc\x1b")))
	require.Equal(t, 0, PartialInputSuffixLen([]byte("abc")))
}
