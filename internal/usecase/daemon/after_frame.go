package daemon

import "sync"

// afterFrameQueue orders a control after the attachment's next composed frame.
//
// A client overlay (the session or move picker) suppresses attachment output
// from the moment it opens, so the frame that erases a daemon overlay closed by
// the same action must reach the client first. Queued work is released once a
// frame captured after its registration has been emitted. The transport is one
// ordered stream, so that frame is queued on the wire before anything the work
// sends.
type afterFrameQueue struct {
	mu       sync.Mutex
	captures uint64 // captures started so far
	pending  []afterFrameWork
	ready    []func()
}

type afterFrameWork struct {
	minCapture uint64 // first capture that observes state at registration
	run        func()
}

func (q *afterFrameQueue) add(run func()) {
	q.mu.Lock()
	q.pending = append(q.pending, afterFrameWork{minCapture: q.captures + 1, run: run})
	q.mu.Unlock()
}

// beginCapture numbers one frame capture. Paint calls it under sendMu before
// snapshotting overlays, so a capture numbered after a registration observes
// every state change that preceded it.
func (q *afterFrameQueue) beginCapture() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.captures++
	return q.captures
}

// emitted releases the work satisfied by the emitted capture.
func (q *afterFrameQueue) emitted(capture uint64) {
	if capture == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	kept := q.pending[:0]
	for _, work := range q.pending {
		if work.minCapture <= capture {
			q.ready = append(q.ready, work.run)
		} else {
			kept = append(kept, work)
		}
	}
	clear(q.pending[len(kept):])
	q.pending = kept
}

// runReady starts released work on its own goroutine: the painting goroutine
// (often a render wake) must not wait on picker projections or a blocking
// control send, and its caller may hold dispatch locks.
func (q *afterFrameQueue) runReady() {
	q.mu.Lock()
	ready := q.ready
	q.ready = nil
	q.mu.Unlock()
	for _, run := range ready {
		go run()
	}
}

// afterNextFrame runs work with a fresh effect once a frame composed after this
// call reached the client. A transient freeze of the same attachment delays the
// work until the freeze ends (the frame is already on the wire); a changed
// capability (detach, suspension, transport replacement, session switch) or the
// session ending drops it.
func (d *Daemon) afterNextFrame(sess *session, ac *attachedClient, effect *attachmentEffect, work func(*attachmentEffect)) {
	d.queueAfterNextFrame(ac, effect, work)
	d.invalidateRender(sess, ac, true, "after_frame.go")
}

// queueAfterNextFrame registers work without requesting the frame.
func (d *Daemon) queueAfterNextFrame(ac *attachedClient, effect *attachmentEffect, work func(*attachmentEffect)) {
	capability := effect.capability()
	ac.afterFrame.add(func() {
		fresh, admitted := waitAttachmentEffect(capability)
		if !admitted {
			if d.log != nil {
				d.log.Debug("dropping after-frame work for a replaced attachment")
			}
			return
		}
		defer fresh.End()
		work(fresh)
	})
}

// waitAttachmentEffect admits capability, waiting out a transient freeze while
// it stays current. It gives up once the capability is replaced or its session
// ends.
func waitAttachmentEffect(capability attachmentCapability) (*attachmentEffect, bool) {
	ac := capability.ac
	if ac == nil || capability.sess == nil {
		return nil, false
	}
	var done <-chan struct{}
	if ctx := capability.sess.ctx; ctx != nil {
		done = ctx.Done()
	}
	for {
		// Take the change signal before trying, so a thaw between the failed
		// admission and the wait is never missed.
		g := &ac.lifecycle
		g.mu.Lock()
		g.initLocked()
		changed := g.changed
		g.mu.Unlock()
		if fresh, admitted := ac.beginAttachmentEffect(capability); admitted {
			return fresh, true
		}
		if !capability.current() {
			return nil, false
		}
		select {
		case <-changed:
		case <-done:
			return nil, false
		}
	}
}
