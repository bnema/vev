package daemon

import (
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// echoAckDelay is how long sequenced input must have been applied before the
// daemon acknowledges it in Output.Echo. Like mosh's ECHO_TIMEOUT, it gives the
// PTY time to echo the input, so the screen published with the acknowledgement
// already contains whatever echo the input produced.
const echoAckDelay = 50 * time.Millisecond

// maxPendingEchoes bounds the per-attachment input history. Acknowledgements
// are cumulative, so dropping the oldest entry only delays its confirmation to
// a newer one.
const maxPendingEchoes = 256

type pendingEcho struct {
	seq uint64
	at  time.Time
}

// echoAckTracker turns applied input sequence numbers into the cumulative
// Output.Echo acknowledgement. echoAck is written only under mu.
type echoAckTracker struct {
	mu      sync.Mutex
	pending []pendingEcho
	timer   pendingByteTimer
}

// noteInputApplied records that the input numbered seq reached the session and
// schedules its acknowledgement. Sequence zero is unsequenced input.
func (d *Daemon) noteInputApplied(ac *attachedClient, seq uint64) {
	if ac == nil || seq == 0 {
		return
	}
	t := &ac.echo
	t.mu.Lock()
	defer t.mu.Unlock()
	if seq <= ac.echoAck.Load() || len(t.pending) > 0 && seq <= t.pending[len(t.pending)-1].seq {
		return
	}
	if len(t.pending) == maxPendingEchoes {
		t.pending = append(t.pending[:0], t.pending[1:]...)
	}
	t.pending = append(t.pending, pendingEcho{seq: seq, at: d.clock.Now()})
	if t.timer.timer == nil {
		d.armEchoAckLocked(ac, echoAckDelay)
	}
}

// armEchoAckLocked starts the single echo timer. The caller holds echo.mu.
func (d *Daemon) armEchoAckLocked(ac *attachedClient, delay time.Duration) {
	t := &ac.echo
	t.timer.retain(d.clock, delay, func(fired ports.Timer) {
		if d.advanceEchoAck(ac, fired) {
			d.sendEchoAck(ac)
		}
	})
}

// armEchoAckRetryLocked re-sends the current acknowledgement after one more
// echo delay. The caller holds echo.mu.
func (d *Daemon) armEchoAckRetryLocked(ac *attachedClient) {
	t := &ac.echo
	t.timer.retain(d.clock, echoAckDelay, func(fired ports.Timer) {
		t.mu.Lock()
		current := t.timer.timer == fired
		if current {
			t.timer.timer, t.timer.done = nil, nil
			if len(t.pending) > 0 {
				// New input owns the next timer and its ack covers this one.
				d.armEchoAckLocked(ac, max(echoAckDelay-d.clock.Now().Sub(t.pending[0].at), 0))
				current = false
			}
		}
		t.mu.Unlock()
		if current {
			d.sendEchoAck(ac)
		}
	})
}

// advanceEchoAck moves echoAck to the newest input applied at least
// echoAckDelay ago and re-arms for the rest. It reports whether echoAck moved.
func (d *Daemon) advanceEchoAck(ac *attachedClient, fired ports.Timer) bool {
	t := &ac.echo
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timer.timer != fired {
		return false // stopped by a reset or superseded
	}
	t.timer.timer, t.timer.done = nil, nil
	now := d.clock.Now()
	acked := uint64(0)
	n := 0
	for n < len(t.pending) && now.Sub(t.pending[n].at) >= echoAckDelay {
		acked = t.pending[n].seq
		n++
	}
	t.pending = append(t.pending[:0], t.pending[n:]...)
	if len(t.pending) > 0 {
		d.armEchoAckLocked(ac, max(echoAckDelay-now.Sub(t.pending[0].at), 0))
	}
	if acked <= ac.echoAck.Load() {
		return false
	}
	ac.echoAck.Store(acked)
	return true
}

// sendEchoAck publishes a new acknowledgement in an empty side-effect Output,
// so the client learns about it even when the screen does not change.
//
// Side effects bypass the output window. While the window is full, the frame
// holding the acknowledged echo may still be withheld, and an early ack would
// make the client judge its guesses against an older screen. The ack then
// waits: the next frame carries it, and a retry covers a screen that never
// changes.
func (d *Daemon) sendEchoAck(ac *attachedClient) {
	if ac.attachmentActivity() != attachmentActive {
		return
	}
	sess := ac.currentAttachmentSession()
	if sess == nil {
		return
	}
	if ac.output != nil && ac.output.atCapacity() {
		t := &ac.echo
		t.mu.Lock()
		if t.timer.timer == nil {
			d.armEchoAckRetryLocked(ac)
		}
		t.mu.Unlock()
		return
	}
	failed, err := d.boundedSendOutputErrTransport(ac, nil)
	if err != nil && failed != nil {
		d.detachOnSendError(sess, ac, failed)
	}
}

// resetEchoAck forgets every acknowledgement. A resumed client restarts its
// input numbering, so the replacement link must start from zero too.
func (ac *attachedClient) resetEchoAck() {
	t := &ac.echo
	t.mu.Lock()
	defer t.mu.Unlock()
	t.timer.stop()
	t.pending = nil
	ac.echoAck.Store(0)
}
