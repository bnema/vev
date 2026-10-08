package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestCopyScrollAnimationEasesAndPreservesDistance(t *testing.T) {
	for _, burst := range []int{1, 6} {
		t.Run(map[int]string{1: "notch", 6: "burst"}[burst], func(t *testing.T) {
			f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, panes: 1, historyRows: 200})
			f.d.enterCopyMode(f.sess, f.ac)
			f.d.copyWheel(f.sess, f.ac, -50)
			f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
			clock := newCoordinatorMockClock(t, 16)
			f.d.clock = clock.clock
			emitted := make(chan struct{}, 16)
			f.ac.renderStages.emit = func() { emitted <- struct{}{} }
			await := func() {
				select {
				case <-emitted:
				case <-time.After(time.Second):
					t.Fatal("scroll frame not emitted")
				}
				// Wait for publication and capture the ACK coordinates under
				// the same lock that protects the in-flight output transaction.
				f.ac.sendMu.Lock()
				epoch, state := f.ac.output.currentEpoch(), f.ac.output.next
				f.ac.sendMu.Unlock()
				f.ac.ackOutputState(epoch, state)
			}
			rt := f.ac.overlays
			start := rt.copyMode.ViewportTop
			for range burst {
				f.d.smoothCopyWheel(f.sess, f.ac, -3)
			}
			await()
			require.Equal(t, start-1, rt.copyMode.ViewportTop, "first row responds immediately; bursts wait for the next frame")
			for frame := 0; ; frame++ {
				rt.copyMu.Lock()
				pending := rt.copyScroll.remaining
				rt.copyMu.Unlock()
				if pending == 0 {
					break
				}
				require.Less(t, frame, 16, "animation must settle")
				timer := awaitCoordinatorScheduledTimer(t, clock)
				require.Equal(t, copyScrollFrame, timer.duration)
				timer.ch <- time.Time{}
				await()
			}
			require.Equal(t, start-3*burst, rt.copyMode.ViewportTop)
		})
	}
}

func TestCopyScrollAnimationReversalAndCancellation(t *testing.T) {
	f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, panes: 1, historyRows: 200})
	f.d.enterCopyMode(f.sess, f.ac)
	f.d.copyWheel(f.sess, f.ac, -50)
	f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
	clock := newCoordinatorMockClock(t, 8)
	f.d.clock = clock.clock
	rt := f.ac.overlays
	start := rt.copyMode.ViewportTop
	f.d.smoothCopyWheel(f.sess, f.ac, -3)
	old := awaitCoordinatorScheduledTimer(t, clock)
	f.d.smoothCopyWheel(f.sess, f.ac, 3)
	require.Equal(t, start, rt.copyMode.ViewportTop, "reversal responds without finishing the old tail")
	select {
	case <-old.stopped:
	default:
		t.Fatal("old direction timer not canceled")
	}
	current := awaitCoordinatorScheduledTimer(t, clock)
	rt.copyMu.Lock()
	rt.invalidateCopyPointerLocked(true)
	require.Zero(t, rt.copyScroll.remaining)
	require.Nil(t, rt.copyScroll.timer.timer)
	rt.copyMu.Unlock()
	select {
	case <-current.stopped:
	default:
		t.Fatal("pointer lifecycle must stop inertia")
	}
}

func TestCopyScrollAnimationTailDeadline(t *testing.T) {
	f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, panes: 1, historyRows: 200})
	f.d.enterCopyMode(f.sess, f.ac)
	clock := newCoordinatorMockClock(t, 8)
	f.d.clock = clock.clock
	rt := f.ac.overlays
	rt.copyMu.Lock()
	start := rt.copyMode.ViewportTop
	rt.copyScroll.remaining = -60
	rt.copyScroll.lastInput = time.Time{}.Add(-copyScrollTail)
	changed, exit := f.d.advanceCopyScrollLocked(f.sess, f.ac)
	require.True(t, changed)
	require.False(t, exit)
	require.Equal(t, start-60, rt.copyMode.ViewportTop)
	require.Zero(t, rt.copyScroll.remaining)
	require.Nil(t, rt.copyScroll.timer.timer)
	rt.copyMu.Unlock()
}

func TestCopyScrollReducedMotionAndKeyboardCancellation(t *testing.T) {
	f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, panes: 1, historyRows: 200})
	f.d.enterCopyMode(f.sess, f.ac)
	f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
	clock := newCoordinatorMockClock(t, 8)
	f.d.clock = clock.clock
	updateRuntimeConfig(f.d, func(rc *runtimeConfig) { rc.copy = domain.CopyConfig{ReduceMotion: true} })
	rt := f.ac.overlays
	start := rt.copyMode.ViewportTop
	f.d.smoothCopyWheel(f.sess, f.ac, -3)
	require.Equal(t, start-3, rt.copyMode.ViewportTop)
	require.Empty(t, clock.timers, "reduced motion must not arm an animation")
	updateRuntimeConfig(f.d, func(rc *runtimeConfig) { rc.copy = domain.CopyConfig{} })
	f.d.smoothCopyWheel(f.sess, f.ac, -3)
	timer := awaitCoordinatorScheduledTimer(t, clock)
	require.True(t, rt.HandleInput(f.d, []byte("k")))
	rt.copyMu.Lock()
	require.Zero(t, rt.copyScroll.remaining)
	require.Nil(t, rt.copyScroll.timer.timer)
	rt.copyMu.Unlock()
	select {
	case <-timer.stopped:
	default:
		t.Fatal("keyboard navigation must cancel inertia")
	}
}

// TestCopyScrollAnimationFullOutputWindow covers a remote link whose output
// window is full: the animation holds still, drains the backlog in capped
// steps once the window reopens, and gives up after about a second of hold.
func TestCopyScrollAnimationFullOutputWindow(t *testing.T) {
	tests := []struct {
		name       string
		heldFrames int
		reopen     bool
		wantMoved  int
		wantActive bool
	}{
		{name: "full window holds the viewport", heldFrames: 1, wantMoved: 0, wantActive: true},
		{name: "reopened window drains in capped steps", heldFrames: 1, reopen: true, wantMoved: copyScrollMaxStep, wantActive: true},
		{name: "long hold abandons the scroll", heldFrames: copyScrollMaxHeldFrames + 1, wantMoved: 0, wantActive: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, panes: 1, historyRows: 200})
			f.ac.output.setWindow(1)
			f.d.enterCopyMode(f.sess, f.ac)
			clock := newCoordinatorMockClock(t, 2*copyScrollMaxHeldFrames)
			f.d.clock = clock.clock
			require.True(t, f.ac.output.atCapacity(), "entering copy mode fills a one-frame window")
			rt := f.ac.overlays
			rt.copyMu.Lock()
			defer rt.copyMu.Unlock()
			start := rt.copyMode.ViewportTop
			// Past the tail deadline the whole backlog is due at once.
			rt.copyScroll.remaining = -60
			rt.copyScroll.lastInput = time.Time{}.Add(-copyScrollTail)
			for range tt.heldFrames {
				// Each frame is driven directly, as its timer callback would:
				// retire the previous frame timer before advancing.
				rt.copyScroll.timer.stop()
				changed, exit := f.d.advanceCopyScrollLocked(f.sess, f.ac)
				require.False(t, changed || exit, "a held frame must not move or exit")
			}
			if tt.reopen {
				f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
				rt.copyScroll.timer.stop()
				changed, exit := f.d.advanceCopyScrollLocked(f.sess, f.ac)
				require.True(t, changed)
				require.False(t, exit)
			}
			require.Equal(t, tt.wantMoved, start-rt.copyMode.ViewportTop)
			require.Equal(t, tt.wantActive, rt.copyScroll.remaining != 0)
			require.Equal(t, tt.wantActive, rt.copyScroll.timer.timer != nil, "the frame timer stays armed only while the scroll is active")
			rt.copyScroll.timer.stop()
		})
	}
}
