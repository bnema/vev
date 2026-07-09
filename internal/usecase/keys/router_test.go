package keys

import (
	"runtime"
	"strings"
	"sync/atomic"
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

func TestDefaultBindingsParityRoutesBuiltInBindings(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want Action
	}{
		{name: "palette", in: []byte{ESC, ' '}, want: ActionOpenPalette},
		{name: "floating pane", in: []byte{ESC, 'f'}, want: ActionToggleFloatingPane},
		{name: "jump attention", in: []byte{ESC, 'a'}, want: ActionJumpAttention},
		{name: "focus left", in: []byte{ESC, 'h'}, want: ActionFocusPaneLeft},
		{name: "focus down", in: []byte{ESC, 'j'}, want: ActionFocusPaneDown},
		{name: "focus up", in: []byte{ESC, 'k'}, want: ActionFocusPaneUp},
		{name: "focus right", in: []byte{ESC, 'l'}, want: ActionFocusPaneRight},
		{name: "arrow left", in: []byte{ESC, '[', '1', ';', '3', 'D'}, want: ActionFocusPaneLeft},
		{name: "arrow right", in: []byte{ESC, '[', '1', ';', '3', 'C'}, want: ActionFocusPaneRight},
		{name: "arrow up", in: []byte{ESC, '[', '1', ';', '3', 'A'}, want: ActionFocusPaneUp},
		{name: "arrow down", in: []byte{ESC, '[', '1', ';', '3', 'B'}, want: ActionFocusPaneDown},
		{name: "digit", in: []byte{ESC, '1'}, want: ActionSwitchTab1},
		{name: "digit alias", in: append([]byte{ESC}, []byte("é")...), want: ActionSwitchTab2},
	}

	var bindings atomic.Pointer[Bindings]
	bindings.Store(DefaultBindings())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			NewRouter(clk, h, &bindings).Route(tc.in)
			require.Equal(t, []Action{tc.want}, h.actions)
			require.Empty(t, h.forwards)
		})
	}
}

func TestRouterInterceptsAltSpaceForPalette(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	NewRouter(clk, h, nil).Route([]byte{ESC, ' '})
	require.Equal(t, []Action{ActionOpenPalette}, h.actions)
	require.Empty(t, h.forwards)
	require.Empty(t, clk.timers)
}

func TestRouterInterceptsAltDigits(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)
	for b := byte('1'); b <= '9'; b++ {
		r.Route([]byte{ESC, b})
	}
	require.Equal(t, switchTabActions(), h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterInterceptsAltLayoutDigitAliases(t *testing.T) {
	cases := []struct {
		name string
		keys []string
	}{
		{name: "QWERTY", keys: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}},
		{name: "French AZERTY", keys: []string{"&", "é", "\"", "'", "(", "-", "è", "_", "ç"}},
		{name: "Belgian AZERTY", keys: []string{"&", "é", "\"", "'", "(", "§", "è", "!", "ç"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			r := NewRouter(clk, h, nil)
			for _, key := range tc.keys {
				r.Route(append([]byte{ESC}, []byte(key)...))
			}
			require.Equal(t, switchTabActions(), h.actions)
			require.Empty(t, h.forwards)
		})
	}
}

func TestRouterInterceptsAltAZERTYUTF8SplitAcrossReads(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)

	r.Route([]byte{ESC, 0xc3})
	require.Empty(t, h.actions)
	require.Empty(t, h.forwards)
	require.Len(t, clk.timers, 1)

	r.Route([]byte{0xa9})
	require.Equal(t, []Action{ActionSwitchTab2}, h.actions)
	require.Empty(t, h.forwards)
	assertRouterPendingCleared(t, r)

	r.Route([]byte{ESC, ' '})
	require.Equal(t, []Action{ActionSwitchTab2, ActionOpenPalette}, h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterInterceptsRetainedAltAZERTYUTF8SplitAcrossReads(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)

	r.Route([]byte{ESC})
	r.Route([]byte{0xc3})
	require.Empty(t, h.actions)
	require.Empty(t, h.forwards)
	require.Len(t, clk.timers, 2)

	r.Route([]byte{0xa9})
	require.Equal(t, []Action{ActionSwitchTab2}, h.actions)
	require.Empty(t, h.forwards)
	assertRouterPendingCleared(t, r)

	r.Route([]byte{ESC, ' '})
	require.Equal(t, []Action{ActionSwitchTab2, ActionOpenPalette}, h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterForwardsUnboundAltUTF8SplitAcrossReads(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)

	r.Route([]byte{ESC, 0xc3})
	r.Route([]byte{0xb1})

	require.Empty(t, h.actions)
	require.Equal(t, [][]byte{{ESC, 0xc3, 0xb1}}, h.forwards)
	assertRouterPendingCleared(t, r)

	r.Route([]byte("Z"))
	require.Empty(t, h.actions)
	require.Equal(t, [][]byte{{ESC, 0xc3, 0xb1}, []byte("Z")}, h.forwards)
}

func TestRouterForwardsInvalidSplitAltUTF8WithoutDroppingBytes(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)

	r.Route([]byte{ESC, 0xc3})
	r.Route([]byte("X"))

	require.Empty(t, h.actions)
	require.Equal(t, [][]byte{{ESC, 0xc3}, []byte("X")}, h.forwards)
	assertRouterPendingCleared(t, r)

	r.Route([]byte{ESC, ' '})
	require.Equal(t, []Action{ActionOpenPalette}, h.actions)
	require.Equal(t, [][]byte{{ESC, 0xc3}, []byte("X")}, h.forwards)
}

func assertRouterPendingCleared(t *testing.T, r *Router) {
	t.Helper()
	require.False(t, r.pending)
	require.Nil(t, r.pendingAlt)
	require.Nil(t, r.timer)
	require.Nil(t, r.pendingDone)
}

func TestTopRowDigitIndexAcceptsLayoutAliases(t *testing.T) {
	for want, aliases := range [][]rune{
		{'1', '&'},
		{'2', 'é'},
		{'3', '"'},
		{'4', '\''},
		{'5', '('},
		{'6', '-', '§'},
		{'7', 'è'},
		{'8', '_', '!'},
		{'9', 'ç'},
	} {
		for _, key := range aliases {
			got, ok := topRowDigitIndex(key)
			require.True(t, ok, "key %q", key)
			require.Equal(t, want, got, "key %q", key)
		}
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

func TestSwitchTabActionsRemainContiguous(t *testing.T) {
	for i, action := range switchTabActions() {
		require.Equal(t, ActionSwitchTab1+Action(i), action)
	}
	require.False(t, ActionToggleFloatingPane >= ActionSwitchTab1 && ActionToggleFloatingPane <= ActionSwitchTab9)
}

func TestRouterInterceptsAltAForJumpAttention(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	NewRouter(clk, h, nil).Route([]byte{ESC, 'a'})
	require.Equal(t, []Action{ActionJumpAttention}, h.actions)
	require.Empty(t, h.forwards)
	require.Empty(t, clk.timers)
}

func TestRouterInterceptsAltHJKLForPaneFocus(t *testing.T) {
	cases := []struct {
		name string
		key  byte
		want Action
	}{
		{name: "left", key: 'h', want: ActionFocusPaneLeft},
		{name: "down", key: 'j', want: ActionFocusPaneDown},
		{name: "up", key: 'k', want: ActionFocusPaneUp},
		{name: "right", key: 'l', want: ActionFocusPaneRight},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			NewRouter(clk, h, nil).Route([]byte{ESC, tc.key})
			require.Equal(t, []Action{tc.want}, h.actions)
			require.Empty(t, h.forwards)
			require.Empty(t, clk.timers)
		})
	}
}

func TestRouterForwardsRemovedAltLetterBindings(t *testing.T) {
	for _, b := range []byte("cnpdxurt") {
		t.Run(string(b), func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			NewRouter(clk, h, nil).Route([]byte{ESC, b})
			require.Empty(t, h.actions)
			require.Equal(t, [][]byte{{ESC, b}}, h.forwards)
		})
	}
}

func TestRouterForwardsOnlyNonInterceptedBytes(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	NewRouter(clk, h, nil).Route([]byte{'a', ESC, ' ', 'b'})
	require.Equal(t, []Action{ActionOpenPalette}, h.actions)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, h.forwards)
}

func TestRouterPassesThroughTerminalEscapePrefixes(t *testing.T) {
	cases := [][]byte{{ESC, '[', 'A'}, {ESC, 'O', 'P'}}
	for _, in := range cases {
		clk := &fakeClock{}
		h := &captureHandler{}
		NewRouter(clk, h, nil).Route(in)
		require.Empty(t, h.actions)
		require.Equal(t, [][]byte{in}, h.forwards)
		require.Empty(t, clk.timers)
	}
}

func TestRouterInterceptsAltArrowCSIForPaneFocus(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want Action
	}{
		{name: "alt up", in: []byte{ESC, '[', '1', ';', '3', 'A'}, want: ActionFocusPaneUp},
		{name: "alt down", in: []byte{ESC, '[', '1', ';', '3', 'B'}, want: ActionFocusPaneDown},
		{name: "alt right", in: []byte{ESC, '[', '1', ';', '3', 'C'}, want: ActionFocusPaneRight},
		{name: "alt left", in: []byte{ESC, '[', '1', ';', '3', 'D'}, want: ActionFocusPaneLeft},
		{name: "meta up", in: []byte{ESC, '[', '1', ';', '9', 'A'}, want: ActionFocusPaneUp},
		{name: "meta down", in: []byte{ESC, '[', '1', ';', '9', 'B'}, want: ActionFocusPaneDown},
		{name: "meta right", in: []byte{ESC, '[', '1', ';', '9', 'C'}, want: ActionFocusPaneRight},
		{name: "meta left", in: []byte{ESC, '[', '1', ';', '9', 'D'}, want: ActionFocusPaneLeft},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			NewRouter(clk, h, nil).Route(tc.in)
			require.Equal(t, []Action{tc.want}, h.actions)
			require.Empty(t, h.forwards)
			require.Empty(t, clk.timers)
		})
	}
}

func TestRouterAltArrowCSIPartialsAcrossReads(t *testing.T) {
	cases := []struct {
		name   string
		reads  [][]byte
		want   Action
		timers int
	}{
		{name: "esc then rest", reads: [][]byte{{ESC}, []byte("[1;3D")}, want: ActionFocusPaneLeft, timers: 1},
		{name: "csi prefix then rest", reads: [][]byte{{ESC, '['}, []byte("1;3C")}, want: ActionFocusPaneRight, timers: 1},
		{name: "modifier prefix then final", reads: [][]byte{{ESC, '[', '1', ';', '3'}, []byte("A")}, want: ActionFocusPaneUp, timers: 1},
		{name: "meta modifier prefix then final", reads: [][]byte{{ESC, '[', '1', ';', '9'}, []byte("B")}, want: ActionFocusPaneDown, timers: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			r := NewRouter(clk, h, nil)
			for _, read := range tc.reads {
				r.Route(read)
			}
			require.Equal(t, []Action{tc.want}, h.actions)
			require.Empty(t, h.forwards)
			require.Len(t, clk.timers, tc.timers)
			assertRouterPendingCleared(t, r)
		})
	}
}

func TestRouterAltArrowCSIPrefixRouting(t *testing.T) {
	cases := []struct {
		name         string
		reads        [][]byte
		wantActions  []Action
		wantForwards [][]byte
	}{
		{
			name:        "two alt arrows in one read",
			reads:       [][]byte{{ESC, '[', '1', ';', '3', 'A', ESC, '[', '1', ';', '3', 'A'}},
			wantActions: []Action{ActionFocusPaneUp, ActionFocusPaneUp},
		},
		{
			name:         "alt arrow followed by normal byte",
			reads:        [][]byte{{ESC, '[', '1', ';', '3', 'A', 'x'}},
			wantActions:  []Action{ActionFocusPaneUp},
			wantForwards: [][]byte{[]byte("x")},
		},
		{
			name:         "bare arrow passthrough",
			reads:        [][]byte{{ESC, '[', 'A'}},
			wantForwards: [][]byte{{ESC, '[', 'A'}},
		},
		{
			name:        "retained escape alt arrow across reads",
			reads:       [][]byte{{ESC}, []byte("[1;3A")},
			wantActions: []Action{ActionFocusPaneUp},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			r := NewRouter(clk, h, nil)
			for _, read := range tc.reads {
				r.Route(read)
			}
			require.Equal(t, tc.wantActions, h.actions)
			require.Equal(t, tc.wantForwards, h.forwards)
		})
	}
}

func TestRouterPassesThroughBareArrows(t *testing.T) {
	for _, in := range [][]byte{
		{ESC, '[', 'A'},
		{ESC, '[', 'B'},
		{ESC, '[', 'C'},
		{ESC, '[', 'D'},
	} {
		t.Run(string(in), func(t *testing.T) {
			clk := &fakeClock{}
			h := &captureHandler{}
			NewRouter(clk, h, nil).Route(in)
			require.Empty(t, h.actions)
			require.Equal(t, [][]byte{in}, h.forwards)
			require.Empty(t, clk.timers)
		})
	}
}

func TestRouterRetainsSplitESCAndInterceptsNextBoundByte(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)
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
	NewRouter(clk, h, nil).Route([]byte{'t'})
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
			r := NewRouter(clk, h, nil)
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
	r := NewRouter(clk, h, nil)
	r.Route([]byte{ESC})

	r.Route([]byte{'[', 'A', ESC, ' '})

	require.True(t, clk.timers[0].stopped)
	require.Equal(t, [][]byte{{ESC, '['}, []byte("A")}, h.forwards)
	require.Equal(t, []Action{ActionOpenPalette}, h.actions)
}

func TestRouterCancelsPendingESCWaiterWhenNextReadConsumesESC(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)

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
	r := NewRouter(clk, h, nil)
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
	r := NewRouter(clk, h, nil)
	r.Route([]byte{ESC})
	r.Route([]byte{'z'})
	require.Equal(t, [][]byte{{ESC}, []byte("z")}, h.forwards)
	require.Empty(t, h.actions)
}

// pasteBindingLookalikes are content bytes that, seen outside a paste, each map
// to a binding: Alt-j, Alt-space, Alt-1, and an Alt-arrow CSI.
var pasteBindingLookalikes = []byte("\x1bj\x1b \x1b1\x1b[1;3A")

func TestRouterForwardsSingleFramePasteVerbatim(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)

	paste := append(append(append([]byte(nil), ports.BracketedPasteOpenMarker...), pasteBindingLookalikes...), ports.BracketedPasteCloseMarker...)
	r.Route(paste)

	require.Empty(t, h.actions, "paste content must not fire any binding")
	require.Equal(t, [][]byte{paste}, h.forwards, "the whole paste must be forwarded byte-identical")
	require.Empty(t, clk.timers, "a fully bracketed paste retains no trailing ESC")
}

func TestRouterFiresBindingsForSameBytesOutsidePaste(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)

	r.Route(pasteBindingLookalikes)

	require.Equal(t, []Action{
		ActionFocusPaneDown, // Alt-j
		ActionOpenPalette,   // Alt-space
		ActionSwitchTab1,    // Alt-1
		ActionFocusPaneUp,   // Alt-arrow up
	}, h.actions)
	require.Empty(t, h.forwards)
}

func TestRouterPasteMarkerSplitAcrossFramesKeepsCurrentBehavior(t *testing.T) {
	clk := &fakeClock{}
	h := &captureHandler{}
	r := NewRouter(clk, h, nil)

	// Opening marker + content in one frame, closing marker in the next: with
	// no closing marker in-frame the router keeps its historical per-byte
	// routing, forwarding each frame's bytes as they arrive.
	r.Route([]byte("\x1b[200~hello"))
	r.Route([]byte("\x1b[201~"))

	require.Empty(t, h.actions)
	require.Equal(t, [][]byte{[]byte("\x1b[200~hello"), []byte("\x1b[201~")}, h.forwards)
}
