package daemon

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// TestCopyScrollOverDelayedAcks replays a wheel scroll on a link whose
// acknowledgements arrive rtt late, for several output windows. One tick is one
// animation frame (copyScrollFrame). The log shows, per tick, the rows the
// viewport moved: "-" is a stall, a value above one frame's worth is a catch-up
// jump. The test asserts that no frame jumps further than copyScrollMaxStep,
// that every requested row is eventually shown, and that the window the daemon
// is allowed to fill bounds the stalls.
func TestCopyScrollOverDelayedAcks(t *testing.T) {
	// flick: 12 notches at once. sustained: one notch per tick for 30 ticks.
	scenarios := []struct {
		name             string
		upfront, perTick int
	}{{"flick", 12, 0}, {"sustained", 1, 1}}
	rtts := []time.Duration{0, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	for _, scenario := range scenarios {
		for _, window := range []uint8{1, protocol.MaxOutputWindow} {
			for _, rtt := range rtts {
				t.Run(fmt.Sprintf("%s/window%d/%v", scenario.name, window, rtt), func(t *testing.T) {
					res := replayCopyScroll(t, scenario.upfront, scenario.perTick, window, rtt)
					t.Logf("rows per tick (- = stall): %s", res.log)
					if res.maxJump > copyScrollMaxStep {
						t.Errorf("one frame jumped %d rows, want at most %d", res.maxJump, copyScrollMaxStep)
					}
					if res.scrolled != res.requested {
						t.Fatalf("scrolled %d rows, want %d", res.scrolled, res.requested)
					}
				})
			}
		}
	}
}

// TestCopyScrollWindowBoundsStalls pins the reason the output window matters:
// on a 100 ms link a one-frame window leaves the viewport still most ticks,
// while the default window keeps it moving.
func TestCopyScrollWindowBoundsStalls(t *testing.T) {
	rtt := 100 * time.Millisecond
	narrow := replayCopyScroll(t, 1, 1, 1, rtt)
	wide := replayCopyScroll(t, 1, 1, protocol.MaxOutputWindow, rtt)
	if wide.stalls >= narrow.stalls {
		t.Fatalf("window %d stalled %d ticks, window 1 stalled %d: a wider window must stall less", protocol.MaxOutputWindow, wide.stalls, narrow.stalls)
	}
}

type copyScrollReplay struct {
	log                 string
	maxJump, stalls     int
	scrolled, requested int
}

// replayCopyScroll feeds upfront notches at once, then perTick notches per tick
// for 30 ticks, acknowledging each emitted frame rtt later. A fake clock drives
// the animation and every step waits for its exact expected effect (a frame
// emitted, or the frame timer re-armed while the window is full), so the
// timeouts below only guard against hangs and never decide a result.
func replayCopyScroll(t *testing.T, upfront, perTick int, window uint8, rtt time.Duration) copyScrollReplay {
	t.Helper()
	f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, panes: 1, historyRows: 400})
	f.ac.output.setWindow(window)
	f.d.enterCopyMode(f.sess, f.ac)
	f.d.copyWheel(f.sess, f.ac, -150)
	f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
	clock := newCoordinatorMockClock(t, 256)
	f.d.clock = clock.clock
	rt := f.ac.overlays

	type ack struct {
		at          int
		epoch, seen uint64
	}
	type emitted struct{ epoch, state uint64 }
	var (
		acks       []ack
		tick       int
		frameTimer *coordinatorMockTimer
		// emit runs under sendMu inside the render, so the epoch and state
		// it reports are exactly those of the frame sent.
		sent = make(chan emitted, 256)
	)
	f.ac.renderStages.emit = func() { sent <- emitted{f.ac.output.currentEpoch(), f.ac.output.next} }
	ticksPerRTT := int((rtt + copyScrollFrame - 1) / copyScrollFrame)

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
	// collectTimer keeps the newest armed animation frame timer, if any.
	collectTimer := func() {
		for {
			select {
			case tm := <-clock.timers:
				if tm.duration == copyScrollFrame {
					frameTimer = tm
				}
			default:
				return
			}
		}
	}
	frames := 0
	awaitFrame := func() {
		select {
		case e := <-sent:
			acks = append(acks, ack{at: tick + ticksPerRTT, epoch: e.epoch, seen: e.state})
			frames++
		case <-time.After(5 * time.Second):
			t.Fatal("expected a scroll frame, none was emitted")
		}
	}
	awaitTimer := func() {
		deadline := time.After(5 * time.Second)
		for frameTimer == nil {
			select {
			case tm := <-clock.timers:
				if tm.duration == copyScrollFrame {
					frameTimer = tm
				}
			case <-deadline:
				t.Fatal("held animation did not re-arm its frame timer")
			}
		}
	}
	fed := 0
	feed := func(n int) {
		for range n {
			idle, full := frameTimer == nil, f.ac.output.atCapacity()
			f.d.smoothCopyWheel(f.sess, f.ac, -3)
			fed++
			if idle && !full {
				awaitFrame()
			}
			collectTimer()
		}
	}
	fire := func() {
		if frameTimer == nil {
			return
		}
		full := f.ac.output.atCapacity()
		timer := frameTimer
		frameTimer = nil
		timer.ch <- time.Time{}
		if full {
			awaitTimer()
			return
		}
		awaitFrame()
		collectTimer()
	}

	var res copyScrollReplay
	var log []string
	start := viewport()
	last := start
	record := func() {
		if frames == 0 {
			log = append(log, "-")
			res.stalls++
			return
		}
		top := viewport()
		log = append(log, fmt.Sprint(last-top))
		res.maxJump = max(res.maxJump, last-top)
		last = top
		frames = 0
	}

	feed(upfront)
	record()
	for ; tick < 1200 && (pending() != 0 || len(acks) != 0 || tick < 30 && perTick > 0); tick++ {
		if tick < 30 {
			feed(perTick)
		}
		for len(acks) > 0 && acks[0].at <= tick {
			f.ac.ackOutputState(acks[0].epoch, acks[0].seen)
			if rc := f.sess.renderCoordinator(); rc != nil {
				rc.notifyAck()
			}
			acks = acks[1:]
		}
		fire()
		record()
	}
	res.log = strings.Join(log, " ")
	res.scrolled = start - viewport()
	res.requested = 3 * fed
	return res
}
