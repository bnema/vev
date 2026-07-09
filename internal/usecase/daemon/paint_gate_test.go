package daemon

import (
	"io"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

func requireNoOutputFrame(t *testing.T, sends chan ports.Frame) {
	t.Helper()
	deadline := time.After(50 * time.Millisecond)
	for {
		select {
		case f := <-sends:
			if f.Type == ports.MsgOutput {
				t.Fatalf("unexpected output frame: %+v", f)
			}
		case <-deadline:
			return
		}
	}
}

func TestPaintDefersWhenOutputAckLagsAtCap(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().screen.Write([]byte("A"))

	// Client is maxUnackedOutputStates behind (outputAck stays 0).
	ac.output.next = maxUnackedOutputStates
	d.paint(sess, ac, false)

	requireNoOutputFrame(t, sends)
	ac.sendMu.Lock()
	deferred := ac.output.deferred
	ac.sendMu.Unlock()
	require.True(t, deferred, "diff paint at ack cap must defer")
}

func TestDeferredPaintFlushesOnceWhenAcksCatchUp(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().screen.Write([]byte("A"))

	ac.output.next = maxUnackedOutputStates
	d.paint(sess, ac, false)
	requireNoOutputFrame(t, sends)

	// Drive a real MsgAck through the connection loop so the flush is exercised
	// end-to-end: UnmarshalAck → advanceOutputAck → takeDeferredPaint → paint.
	// One ack frame, then Recv blocks until the test ends.
	recvCh := make(chan ports.Frame, 1)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	tr, ok := ac.transport().(*portsmocks.MockTransport)
	require.True(t, ok, "manual session transport must be the capturing mock")
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case f := <-recvCh:
			return f, nil
		case <-done:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	recvCh <- ports.Frame{Type: ports.MsgAck, Payload: ports.MarshalAck(ports.Ack{AckedStateNum: maxUnackedOutputStates})}
	go d.runConnLoop(ac)

	// The cumulative flush paint must be triggered by the MsgAck handler itself
	// (the renderer shadow only advanced on send, so the skip coalesces here).
	awaitFrame(t, sends, ports.MsgOutput)
	requireNoOutputFrame(t, sends)
	ac.sendMu.Lock()
	deferred := ac.output.deferred
	ac.sendMu.Unlock()
	require.False(t, deferred, "flag must clear after the flush paint")
}

func TestResetPaintCoalescesAtAckGate(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().screen.Write([]byte("A"))

	// At the cap, repeated reset paints coalesce instead of growing retained
	// state and transport queues without bound.
	ac.output.next = maxUnackedOutputStates
	for range 32 {
		d.paint(sess, ac, true)
	}

	requireNoOutputFrame(t, sends)
	ac.sendMu.Lock()
	deferred := ac.output.deferred
	deferredReset := ac.output.deferredReset
	retained := ac.output.outstanding()
	ac.sendMu.Unlock()
	require.True(t, deferred)
	require.True(t, deferredReset)
	require.LessOrEqual(t, retained, uint64(maxUnackedOutputStates))
}

func TestCoalescedResetFlushesLatestFrameAfterAck(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	for range maxUnackedOutputStates {
		ac.output.frame([]byte("in flight"), false, 0)
	}

	pane := sess.tabs[0].focusedPane()
	var latest byte
	for i := range 32 {
		latest = byte('a' + i%26)
		pane.screen.Write([]byte{'\r', latest})
		d.paint(sess, ac, true)
	}
	requireNoOutputFrame(t, sends)
	require.Equal(t, uint64(maxUnackedOutputStates), ac.output.outstanding())

	ac.ackOutputState(maxUnackedOutputStates)
	reset, ok := ac.takeDeferredPaint()
	require.True(t, ok)
	require.True(t, reset)
	d.paint(sess, ac, reset)

	client := vt.NewScreen(80, 25)
	out := mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	require.Zero(t, out.BaseStateNum)
	require.Equal(t, rune(latest), client.Frame.At(0, 1).Rune)
	require.Equal(t, uint64(1), ac.output.outstanding())
	requireNoOutputFrame(t, sends)
}
