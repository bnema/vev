package daemon

import "sync"

// afterFrameQueue orders a control after the attachment's next composed frame.
//
// A client overlay (the session or move picker) suppresses attachment output
// from the moment it opens, so the frame that erases a daemon overlay closed by
// the same action must reach the client first. Queued work runs once a frame
// captured after its registration has been emitted, outside every attachment,
// session, tab, and pane lock. The transport is one ordered stream, so that
// frame is on the wire before anything the work sends.
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

// runReady runs released work. Callers hold no attachment or session lock.
func (q *afterFrameQueue) runReady() {
	q.mu.Lock()
	ready := q.ready
	q.ready = nil
	q.mu.Unlock()
	for _, run := range ready {
		run()
	}
}

// afterNextFrame runs work with a fresh effect once a frame composed after this
// call reached the client. Work whose attachment capability changed meanwhile
// (detach, transport replacement, session switch) is dropped.
func (d *Daemon) afterNextFrame(sess *session, ac *attachedClient, effect *attachmentEffect, work func(*attachmentEffect)) {
	capability := effect.capability()
	ac.afterFrame.add(func() {
		fresh, admitted := ac.beginAttachmentEffect(capability)
		if !admitted {
			return
		}
		defer fresh.End()
		work(fresh)
	})
	d.invalidateRender(sess, ac, true, "after_frame.go")
}
