package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func requireNoOutputFrame(t *testing.T, sends chan ports.Frame) {
	t.Helper()
	select {
	case f := <-sends:
		if f.Type == ports.MsgOutput {
			t.Fatalf("unexpected output frame: %+v", f)
		}
	case <-time.After(50 * time.Millisecond):
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

	// Acks catch up: the deferred paint becomes flushable and emits exactly one
	// cumulative frame (the renderer shadow only advanced on send, so the skip
	// coalesces into this paint).
	ac.advanceOutputAck(maxUnackedOutputStates)
	require.True(t, ac.takeDeferredPaint(), "deferred paint must flush once acks catch up")
	d.paint(sess, ac, false)

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
