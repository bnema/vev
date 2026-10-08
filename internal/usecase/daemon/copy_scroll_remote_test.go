package daemon

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
)

// TestCopyScrollOverDelayedAcks measures how a wheel flick looks on a link
// whose acknowledgements arrive rtt late. One tick is one animation frame
// (copyScrollFrame). It logs, per tick, the rows jumped by the frame the client
// would see, so stalls (0) and catch-up jumps (>1) are visible.
func TestCopyScrollOverDelayedAcks(t *testing.T) {
	// flick: 12 notches at once. sustained: one notch per tick for 30 ticks.
	for _, scenario := range []struct {
		name             string
		upfront, perTick int
	}{{"flick", 12, 0}, {"sustained", 1, 1}} {
		for _, rtt := range []time.Duration{0, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond} {
			t.Run(scenario.name+"/"+rtt.String(), func(t *testing.T) {
				f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, panes: 1, historyRows: 400})
				f.d.enterCopyMode(f.sess, f.ac)
				f.d.copyWheel(f.sess, f.ac, -150)
				f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
				clock := newCoordinatorMockClock(t, 64)
				f.d.clock = clock.clock
				rt := f.ac.overlays

				type ack struct {
					at          int
					epoch, seen uint64
				}
				var (
					acks    []ack
					tick    int
					emitted = make(chan struct{}, 64)
				)
				// emit runs under sendMu: only signal here, read ACK coordinates later.
				f.ac.renderStages.emit = func() { emitted <- struct{}{} }
				drainTimers := func() *coordinatorMockTimer {
					var frame *coordinatorMockTimer
					for {
						select {
						case tm := <-clock.timers:
							if tm.duration == copyScrollFrame {
								frame = tm
							}
						default:
							return frame
						}
					}
				}
				viewport := func() int {
					rt.copyMu.Lock()
					defer rt.copyMu.Unlock()
					return rt.copyMode.ViewportTop
				}
				pending := func() int {
					rt.copyMu.Lock()
					defer rt.copyMu.Unlock()
					return rt.copyScroll.remaining
				}
				delivered := func() bool {
					select {
					case <-emitted:
						f.ac.sendMu.Lock()
						epoch, state := f.ac.output.currentEpoch(), f.ac.output.next
						f.ac.sendMu.Unlock()
						acks = append(acks, ack{at: tick + int((rtt+copyScrollFrame-1)/copyScrollFrame), epoch: epoch, seen: state})
						return true
					case <-time.After(15 * time.Millisecond):
						return false
					}
				}

				start := viewport()
				last := start
				maxJump := 0
				var jumps []string
				// A fast flick: 12 notches of 3 rows arrive in the first tick.
				fed := 0
				feed := func(n int) {
					for range n {
						f.d.smoothCopyWheel(f.sess, f.ac, -3)
						fed++
					}
				}
				feed(scenario.upfront)
				shown := func() {
					if delivered() {
						top := viewport()
						jumps = append(jumps, fmt.Sprint(last-top))
						maxJump = max(maxJump, last-top)
						last = top
					} else {
						jumps = append(jumps, "-")
					}
				}
				shown()
				for ; tick < 80 && (pending() != 0 || len(acks) != 0 || (scenario.perTick > 0 && tick < 30)); tick++ {
					if tick < 30 {
						feed(scenario.perTick)
					}
					for len(acks) > 0 && acks[0].at <= tick {
						f.ac.ackOutputState(acks[0].epoch, acks[0].seen)
						if rc := f.sess.renderCoordinator(); rc != nil {
							rc.notifyAck()
						}
						acks = acks[1:]
					}
					// With a full window no timer is armed: the server is stalled
					// until an ACK arrives, so keep ticking to let it release.
					if frame := drainTimers(); frame != nil {
						frame.ch <- time.Time{}
					}
					shown()
				}
				t.Logf("%s rtt=%-6v rows per tick (- = nothing sent): %s", scenario.name, rtt, strings.Join(jumps, " "))
				// A slow link may delay frames but must not make the picture jump
				// further than one animation frame allows.
				if maxJump > copyScrollMaxStep {
					t.Errorf("one frame jumped %d rows, want at most %d", maxJump, copyScrollMaxStep)
				}
				// Whatever the link, every requested row is eventually shown.
				if got, want := start-viewport(), 3*fed; got != want {
					t.Fatalf("scrolled %d rows, want %d", got, want)
				}
			})
		}
	}
}
