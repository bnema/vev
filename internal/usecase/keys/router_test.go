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

func TestRouterInterceptsAltSpaceForPalette(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	NewRouter(clk, h).Route([]byte{ESC, ' '})
	require.Equal(t, []Action{ActionOpenPalette}, h.actions)
	require.Empty(t, h.forwards)
	require.Empty(t, clk.timers)
}

func TestRouterInterceptsAltDigits(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)
	for b := byte('1'); b <= '9'; b++ {
		r.Route([]byte{ESC, b})
	}
	require.Equal(t, switchTabActions(), h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterInterceptsAltAZERTYTopRowAsDigits(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)
	for _, key := range []string{"&", "é", "\"", "'", "(", "-", "è", "_", "ç"} {
		r.Route(append([]byte{ESC}, []byte(key)...))
	}
	require.Equal(t, switchTabActions(), h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterInterceptsAltAZERTYUTF8SplitAcrossReads(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)

	r.Route([]byte{ESC, 0xc3})
	require.Empty(t, h.actions)
	require.Empty(t, h.forwards)
	require.Len(t, clk.timers, 1)

	r.Route([]byte{0xa9})
	require.Equal(t, []Action{ActionSwitchTab2}, h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterInterceptsRetainedAltAZERTYUTF8SplitAcrossReads(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)

	r.Route([]byte{ESC})
	r.Route([]byte{0xc3})
	require.Empty(t, h.actions)
	require.Empty(t, h.forwards)
	require.Len(t, clk.timers, 2)

	r.Route([]byte{0xa9})
	require.Equal(t, []Action{ActionSwitchTab2}, h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterForwardsUnboundAltUTF8SplitAcrossReads(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)

	r.Route([]byte{ESC, 0xc3})
	r.Route([]byte{0xb1})

	require.Empty(t, h.actions)
	require.Equal(t, [][]byte{{ESC, 0xc3, 0xb1}}, h.forwards)
}

func TestTopRowDigitIndexAcceptsQWERTYAndAZERTYVariants(t *testing.T) {
	cases := []struct {
		key  rune
		want int
	}{
		{key: '1', want: 0}, {key: '&', want: 0},
		{key: '2', want: 1}, {key: 'é', want: 1},
		{key: '3', want: 2}, {key: '"', want: 2},
		{key: '4', want: 3}, {key: '\'', want: 3},
		{key: '5', want: 4}, {key: '(', want: 4},
		{key: '6', want: 5}, {key: '-', want: 5},
		{key: '7', want: 6}, {key: 'è', want: 6},
		{key: '8', want: 7}, {key: '_', want: 7},
		{key: '9', want: 8}, {key: 'ç', want: 8},
	}
	for _, tc := range cases {
		got, ok := topRowDigitIndex(tc.key)
		require.True(t, ok, "key %q", tc.key)
		require.Equal(t, tc.want, got, "key %q", tc.key)
	}

	_, ok := topRowDigitIndex('0')
	require.False(t, ok)
}

func switchTabActions() []Action {
	return []Action{
		ActionSwitchTab1, ActionSwitchTab2, ActionSwitchTab3,
		ActionSwitchTab4, ActionSwitchTab5, ActionSwitchTab6,
		ActionSwitchTab7, ActionSwitchTab8, ActionSwitchTab9,
	}
}

func TestRouterInterceptsAltAForJumpAttention(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	NewRouter(clk, h).Route([]byte{ESC, 'a'})
	require.Equal(t, []Action{ActionJumpAttention}, h.actions)
	require.Empty(t, h.forwards)
	require.Empty(t, clk.timers)
}

func TestRouterForwardsRemovedAltLetterBindings(t *testing.T) {
	for _, b := range []byte("cnpdxurt") {
		t.Run(string(b), func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			NewRouter(clk, h).Route([]byte{ESC, b})
			require.Empty(t, h.actions)
			require.Equal(t, [][]byte{{ESC, b}}, h.forwards)
		})
	}
}

func TestRouterForwardsOnlyNonInterceptedBytes(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	NewRouter(clk, h).Route([]byte{'a', ESC, ' ', 'b'})
	require.Equal(t, []Action{ActionOpenPalette}, h.actions)
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

	r.Route([]byte{' '})
	require.True(t, clk.timers[0].stopped)
	require.Equal(t, []Action{ActionOpenPalette}, h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterForwardsBareT(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	NewRouter(clk, h).Route([]byte{'t'})
	require.Empty(t, h.actions)
	require.Equal(t, [][]byte{[]byte("t")}, h.forwards)
}

func TestRouterForwardsCSIAfterRetainedESCAcrossReads(t *testing.T) {
	cases := []struct {
		name        string
		secondInput []byte
		want        []byte
	}{
		{name: "CSI up", secondInput: []byte("[A"), want: []byte{ESC, '[', 'A'}},
		{name: "SS3 function key", secondInput: []byte("OP"), want: []byte{ESC, 'O', 'P'}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			r := NewRouter(clk, h)
			r.Route([]byte{ESC})
			require.Len(t, clk.timers, 1)
			require.Empty(t, h.forwards)

			r.Route(tc.secondInput)

			require.True(t, clk.timers[0].stopped)
			require.Empty(t, h.actions)
			require.Equal(t, [][]byte{tc.want[:2], tc.want[2:]}, h.forwards)
		})
	}
}

func TestRouterRoutesBytesAfterRetainedEscapePrefix(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)
	r.Route([]byte{ESC})

	r.Route([]byte{'[', 'A', ESC, ' '})

	require.True(t, clk.timers[0].stopped)
	require.Equal(t, [][]byte{{ESC, '['}, []byte("A")}, h.forwards)
	require.Equal(t, []Action{ActionOpenPalette}, h.actions)
}

func TestRouterCancelsPendingESCWaiterWhenNextReadConsumesESC(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h)

	before := retainESCWaiters()
	r.Route([]byte{ESC})
	require.Eventually(t, func() bool { return retainESCWaiters() > before }, time.Second, time.Millisecond)

	r.Route([]byte{' '})
	require.Eventually(t, func() bool { return retainESCWaiters() == before }, time.Second, time.Millisecond)
	require.True(t, clk.timers[0].stopped)
	require.Equal(t, []Action{ActionOpenPalette}, h.actions)
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
