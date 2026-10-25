package client

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

// pcTimerControl retains only test-side firing state; the clock and timers
// passed to production are generated mocks.
type pcTimerControl struct {
	clock  *portsmocks.MockClock
	mu     sync.Mutex
	timers []*pcTimerState
}

type pcTimerState struct {
	ch      chan time.Time
	stopped bool
	fired   bool
}

func newPCTimerControl(t *testing.T) *pcTimerControl {
	c := &pcTimerControl{clock: portsmocks.NewMockClock(t)}
	c.clock.EXPECT().Now().Return(time.Time{}).Maybe()
	c.clock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(time.Duration) ports.Timer {
		c.mu.Lock()
		defer c.mu.Unlock()
		state := &pcTimerState{ch: make(chan time.Time, 1)}
		c.timers = append(c.timers, state)
		timer := portsmocks.NewMockTimer(t)
		timer.EXPECT().C().Return((<-chan time.Time)(state.ch)).Maybe()
		timer.EXPECT().Reset(mock.Anything).RunAndReturn(func(time.Duration) bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return !state.stopped
		}).Maybe()
		timer.EXPECT().Stop().RunAndReturn(func() bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			wasStopped := state.stopped
			state.stopped = true
			return !wasStopped
		}).Maybe()
		return timer
	}).Maybe()
	return c
}

func (c *pcTimerControl) fireLast() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.timers) - 1; i >= 0; i-- {
		timer := c.timers[i]
		if !timer.stopped && !timer.fired {
			timer.fired = true
			timer.ch <- time.Time{}
			return
		}
	}
}

// pcCollector records emit calls under a mutex so a timer goroutine's emit is
// safe to observe from the test goroutine.
type pcCollector struct {
	mu    sync.Mutex
	emits [][]byte
}

func (c *pcCollector) emit(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.emits = append(c.emits, append([]byte(nil), data...))
}

func (c *pcCollector) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.emits))
	copy(out, c.emits)
	return out
}

func (c *pcCollector) waitEmits(t *testing.T, n int) {
	t.Helper()
	synctest.Wait()
	require.GreaterOrEqual(t, len(c.snapshot()), n)
}

func newTestCoalescer(t *testing.T) (*pasteCoalescer, *pcTimerControl, *pcCollector) {
	clk := newPCTimerControl(t)
	col := &pcCollector{}
	return newPasteCoalescer(clk.clock, col.emit), clk, col
}

// samplePaste is a multiline bracketed paste whose content deliberately embeds
// sequences that would otherwise be interpreted (a lone ESC+letter, an Alt
// arrow CSI, a marker-looking prefix) to prove content is carried verbatim.
var samplePaste = []byte("\x1b[200~line one\nline\x1btwo\n\x1b[1;3Athree\x1b[20not-a-marker\x1b[201~")

func TestPasteCoalescerSplitAtEveryOffsetTwoReads(t *testing.T) {
	for s := 0; s <= len(samplePaste); s++ {
		t.Run("split_"+strconv.Itoa(s), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pc, _, col := newTestCoalescer(t)
				defer pc.Close()
				pc.Scan(samplePaste[:s])
				pc.Scan(samplePaste[s:])
				require.Equal(t, [][]byte{samplePaste}, col.snapshot(),
					"paste split at offset %d must emit exactly once, byte-identical", s)
			})
		})
	}
}

func TestPasteCoalescerByteByByte(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pc, _, col := newTestCoalescer(t)
		defer pc.Close()
		for i := 0; i < len(samplePaste); i++ {
			pc.Scan(samplePaste[i : i+1])
		}
		require.Equal(t, [][]byte{samplePaste}, col.snapshot(),
			"paste fed one byte at a time must still emit as one contiguous paste")
	})
}

func TestPasteCoalescerBackToBackPastesInOneRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pc, _, col := newTestCoalescer(t)
		defer pc.Close()

		first := []byte("\x1b[200~first\x1b[201~")
		second := []byte("\x1b[200~second\x1b[201~")
		pc.Scan(append(append([]byte(nil), first...), second...))

		require.Equal(t, [][]byte{first, second}, col.snapshot(),
			"back-to-back pastes in one read must emit as two intact paste frames")
	})
}

func TestPasteCoalescerLoneEscapeFlushedAfterTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pc, clk, col := newTestCoalescer(t)
		defer pc.Close()

		pc.Scan([]byte{0x1b})
		require.Empty(t, col.snapshot(), "a trailing ESC must be held, not emitted immediately")

		clk.fireLast()
		col.waitEmits(t, 1)
		require.Equal(t, [][]byte{{0x1b}}, col.snapshot(), "held ESC must flush unchanged after the timer")
	})
}

func TestPasteCoalescerEscapeThenOpenMarkerNextRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pc, _, col := newTestCoalescer(t)
		defer pc.Close()

		pc.Scan([]byte{0x1b})
		require.Empty(t, col.snapshot(), "ESC held pending the rest of the marker")

		pc.Scan(samplePaste[1:]) // "[200~...\x1b[201~"
		require.Equal(t, [][]byte{samplePaste}, col.snapshot(),
			"ESC recombined with the following marker must emit a single paste")
	})
}

func TestPasteCoalescerPlainTextPassthrough(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pc, _, col := newTestCoalescer(t)
		defer pc.Close()

		pc.Scan([]byte("hello world"))
		pc.Scan([]byte("second read"))
		require.Equal(t, [][]byte{[]byte("hello world"), []byte("second read")}, col.snapshot(),
			"plain text must pass through unchanged at the same call granularity")
	})
}

func TestPasteCoalescerTextPasteTextInOneRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pc, _, col := newTestCoalescer(t)
		defer pc.Close()

		paste := []byte("\x1b[200~pasted\x1b[201~")
		read := append(append([]byte("AB"), paste...), []byte("CD")...)
		pc.Scan(read)
		require.Equal(t, [][]byte{[]byte("AB"), paste, []byte("CD")}, col.snapshot(),
			"text + paste + text in one read must emit three chunks in order")
	})
}

func TestPasteCoalescerBufferCapOverflowFlushesVerbatim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pc, _, col := newTestCoalescer(t)
		defer pc.Close()

		big := append([]byte("\x1b[200~"), bytes.Repeat([]byte("x"), maxPasteBuffer+16)...)
		pc.Scan(big)
		require.Equal(t, [][]byte{big}, col.snapshot(),
			"an oversized unterminated paste must flush verbatim without dropping bytes")

		// The coalescer must be back in normal mode and keep working.
		pc.Scan([]byte("after"))
		require.Equal(t, [][]byte{big, []byte("after")}, col.snapshot())
	})
}

func TestPasteCoalescerSafetyTimeoutFlushesVerbatim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pc, clk, col := newTestCoalescer(t)
		defer pc.Close()

		partial := []byte("\x1b[200~unterminated paste")
		pc.Scan(partial)
		require.Empty(t, col.snapshot(), "an open paste with no closing marker is buffered, not emitted")

		clk.fireLast() // safety timer
		col.waitEmits(t, 1)
		require.Equal(t, [][]byte{partial}, col.snapshot(),
			"the safety timeout must flush buffered paste bytes verbatim")

		pc.Scan([]byte("normal"))
		require.Equal(t, [][]byte{partial, []byte("normal")}, col.snapshot(),
			"after the safety flush the coalescer must resume normal scanning")
	})
}

func TestTrailingOpenPrefixLen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cases := []struct {
			in   string
			want int
		}{
			{"", 0},
			{"abc", 0},
			{"abc\x1b", 1},
			{"abc\x1b[", 2},
			{"\x1b[2", 3},
			{"\x1b[20", 4},
			{"\x1b[200", 5},
			{"\x1b[200~", 0}, // full marker is not a strict prefix
			{"[200", 0},      // no ESC: not a marker start
		}
		for _, tc := range cases {
			require.Equal(t, tc.want, trailingOpenPrefixLen([]byte(tc.in)), "input %q", tc.in)
		}
	})
}
