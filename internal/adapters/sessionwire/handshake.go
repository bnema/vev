package sessionwire

import (
	"context"
	"sync"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
)

// preambleRunner runs one directional preamble and returns the ceilings it
// negotiated.
type preambleRunner func(context.Context, wire.Transport, protoCeilings) (protoCeilings, error)

// handshake is the lazy, once-only preamble lifecycle shared by client and
// server connections: the fixed absolute deadline, the negotiated ceilings,
// the completion signal, and the completion hooks.
type handshake struct {
	raw      wire.Transport
	ceilings protoCeilings
	// ceilingsMu guards ceilings. The lazy preamble publishes the negotiated
	// values from whichever goroutine first uses the connection, while another
	// goroutine may already be asking for capabilities or starting a send, so
	// the once-guarded write is not by itself a happens-before edge for them.
	ceilingsMu sync.Mutex

	preambleOnce sync.Once
	preambleErr  error
	deadline     time.Time

	// preambleDone closes exactly once when the handshake has run, whether it
	// succeeded or failed. It backs the handshake completion seam a logical mux
	// connection exposes; the absolute deadline is never restarted.
	preambleDone chan struct{}

	hooksMu       sync.Mutex
	handshakeDone bool
	hooksRun      bool
	hooks         []func()
}

func newHandshake(raw wire.Transport, deadline time.Time) handshake {
	return handshake{raw: raw, ceilings: defaultProtoCeilings(), deadline: deadline, preambleDone: make(chan struct{})}
}

// HandshakeDeadline returns the absolute local deadline of the session
// handshake this connection started with. It is the one accepted deadline the
// preamble, Hello/Welcome, and first committed publication share; the value is
// fixed for the connection's lifetime and is never restarted.
func (h *handshake) HandshakeDeadline() time.Time { return h.deadline }

// HandshakeDone returns a channel that is closed exactly once when the
// handshake has run, whether it succeeded or failed.
func (h *handshake) HandshakeDone() <-chan struct{} { return h.preambleDone }

// OnHandshakeComplete registers fn to run exactly once when the handshake has
// run. Registering after completion runs fn immediately; a nil fn is ignored.
func (h *handshake) OnHandshakeComplete(fn func()) {
	if fn == nil {
		return
	}
	h.hooksMu.Lock()
	if h.handshakeDone {
		h.hooksMu.Unlock()
		fn()
		return
	}
	h.hooks = append(h.hooks, fn)
	h.hooksMu.Unlock()
}

// ensurePreamble runs the preamble once, bounded by the handshake deadline.
// A failed preamble closes the raw transport.
func (h *handshake) ensurePreamble(run preambleRunner) error {
	h.preambleOnce.Do(func() {
		ctx, cancel := context.WithDeadline(context.Background(), h.deadline)
		defer cancel()
		next, err := run(ctx, h.raw, h.limits())
		h.publishLimits(next)
		h.preambleErr = err
		if err != nil {
			_ = h.raw.Close()
		}
		h.finishPreamble()
	})
	h.runHandshakeHooks()
	return h.preambleErr
}

// finishPreamble publishes handshake completion from inside the preamble's
// sync.Once: it closes the done channel and marks the handshake complete so a
// later registration runs immediately. It deliberately runs no registered hook,
// because a hook that calls back into the connection would re-enter sync.Once
// and deadlock; runHandshakeHooks runs the hooks after the Once body returns.
// It tolerates a zero-value handshake that never allocated the done channel
// (test literals and seam-only construction), so closing is nil-safe.
func (h *handshake) finishPreamble() {
	if h.preambleDone != nil {
		close(h.preambleDone)
	}
	h.hooksMu.Lock()
	h.handshakeDone = true
	h.hooksMu.Unlock()
}

// runHandshakeHooks runs the completion hooks registered before the handshake
// finished exactly once, after the preamble's sync.Once body has returned.
// Running them outside the Once is what lets a hook call back into the
// connection - for example to send or receive a message - without re-entering
// sync.Once and deadlocking. Every ensurePreamble caller invokes it; only the
// first drains the hooks, and a registrant that arrives after completion
// observes the completed flag and runs itself.
func (h *handshake) runHandshakeHooks() {
	h.hooksMu.Lock()
	if h.hooksRun {
		h.hooksMu.Unlock()
		return
	}
	h.hooksRun = true
	hooks := h.hooks
	h.hooks = nil
	h.hooksMu.Unlock()
	for _, fn := range hooks {
		fn()
	}
}

// limits returns the negotiated ceilings under the lock the preamble publishes
// them through. Send paths may read them without the lock once ensurePreamble
// has returned, because that call orders the publish before the read.
func (h *handshake) limits() protoCeilings {
	h.ceilingsMu.Lock()
	defer h.ceilingsMu.Unlock()
	return h.ceilings
}

// publishLimits records the ceilings one preamble negotiation produced.
func (h *handshake) publishLimits(next protoCeilings) {
	h.ceilingsMu.Lock()
	h.ceilings = next
	h.ceilingsMu.Unlock()
}
