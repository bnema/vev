package daemon

import (
	"io"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
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
	ac.nextStateNum = maxUnackedOutputStates
	d.paint(sess, ac, false)

	requireNoOutputFrame(t, sends)
	ac.sendMu.Lock()
	deferred := ac.paintDeferred
	ac.sendMu.Unlock()
	require.True(t, deferred, "diff paint at ack cap must defer")
}

func TestDeferredPaintFlushesOnceWhenAcksCatchUp(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().screen.Write([]byte("A"))

	ac.nextStateNum = maxUnackedOutputStates
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
	deferred := ac.paintDeferred
	ac.sendMu.Unlock()
	require.False(t, deferred, "flag must clear after the flush paint")
}

func TestResetPaintBypassesAckGate(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().screen.Write([]byte("A"))

	// At the cap and already deferred: a reset paint is a full invalidation that
	// must paint anyway and clear the deferral.
	ac.nextStateNum = maxUnackedOutputStates
	ac.paintDeferred = true
	d.paint(sess, ac, true)

	awaitFrame(t, sends, ports.MsgOutput)
	ac.sendMu.Lock()
	deferred := ac.paintDeferred
	ac.sendMu.Unlock()
	require.False(t, deferred, "reset paint must clear the deferral")
}
