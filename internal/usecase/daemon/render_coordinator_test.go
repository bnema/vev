package daemon

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
)

// --- coordinator harness ------------------------------------------------------

// coordinatorMockClock uses generated mocks while retaining deterministic timer
// channels and recorded deadlines for coordinator scheduling assertions.
type coordinatorMockClock struct {
	clock  *portsmocks.MockClock
	timers chan *coordinatorMockTimer
	inert  bool
}

type coordinatorMockTimer struct {
	mock     *portsmocks.MockTimer
	ch       chan time.Time
	duration time.Duration
}

func newCoordinatorMockClock(t *testing.T, capacity int) *coordinatorMockClock {
	t.Helper()
	clk := &coordinatorMockClock{
		clock:  portsmocks.NewMockClock(t),
		timers: make(chan *coordinatorMockTimer, capacity),
	}
	clk.clock.EXPECT().Now().Return(time.Time{}).Maybe()
	clk.clock.EXPECT().NewTimer(mock.MatchedBy(func(d time.Duration) bool {
		return d == urgentRenderDeadline ||
			(d >= minOutputRenderDeadline && d <= maxOutputRenderDeadline) ||
			d == maxSyncUpdateDuration
	})).RunAndReturn(func(d time.Duration) ports.Timer {
		timer := &coordinatorMockTimer{
			mock:     portsmocks.NewMockTimer(t),
			duration: d,
		}
		if !clk.inert {
			timer.ch = make(chan time.Time, 1)
		}
		// A normal deadline reads C both in its worker and in the inert-clock
		// guard; a watchdog reads it in its worker. The generated expectation
		// makes every channel access observable without a hand-written Timer.
		timer.mock.EXPECT().C().Maybe().Return((<-chan time.Time)(timer.ch))
		timer.mock.EXPECT().Stop().Maybe().Return(true)
		clk.timers <- timer
		return timer.mock
	}).Maybe()
	return clk
}

func newInertCoordinatorMockClock(t *testing.T, capacity int) *coordinatorMockClock {
	clk := newCoordinatorMockClock(t, capacity)
	clk.inert = true
	return clk
}

// coordinatorHarness wires one coordinator to generated clock/timer mocks and
// recording hooks. Every assertion is channel- or counter-based; nothing sleeps.
type coordinatorHarness struct {
	clk        *coordinatorMockClock
	wakes      chan renderWake
	previews   chan renderWake
	ackReady   atomic.Bool
	syncActive atomic.Bool
	rc         *renderCoordinator
}

func newCoordinatorHarness(t *testing.T) *coordinatorHarness {
	t.Helper()
	h := &coordinatorHarness{
		clk:      newCoordinatorMockClock(t, 16),
		wakes:    make(chan renderWake, 16),
		previews: make(chan renderWake, 16),
	}
	h.ackReady.Store(true)
	h.rc = newRenderCoordinator(renderCoordinatorOptions{
		clock:      h.clk.clock,
		wake:       func(w renderWake) { h.wakes <- w },
		ackReady:   func() bool { return h.ackReady.Load() },
		syncActive: func() bool { return h.syncActive.Load() },
	})
	return h
}

// armedTimers drains every deadline armed so far. Arming happens
// synchronously inside the coordinator call, so a non-blocking drain is
// deterministic.
func (h *coordinatorHarness) armedTimers(t *testing.T) []*coordinatorMockTimer {
	t.Helper()
	var timers []*coordinatorMockTimer
	for {
		select {
		case tm := <-h.clk.timers:
			timers = append(timers, tm)
		default:
			return timers
		}
	}
}

// Coordinator calls and fake-clock advancement are synchronous test steps.
// These helpers deliberately never wait on wall time: a missing event is a
// deterministic contract failure, not a slow behavior to poll for.
func awaitWake(t *testing.T, ch chan renderWake) renderWake {
	t.Helper()
	for range 4096 {
		select {
		case w := <-ch:
			return w
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("coordinator did not publish a wake after fake-clock advancement")
	return renderWake{}
}

func requireNoWake(t *testing.T, ch chan renderWake) {
	t.Helper()
	select {
	case w := <-ch:
		t.Fatalf("unexpected coordinator wake: %+v", w)
	default:
	}
}

func awaitInvalidation(t *testing.T, ch chan renderInvalidation) renderInvalidation {
	t.Helper()
	for range 4096 {
		select {
		case inv := <-ch:
			return inv
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("producer did not publish a coordinator invalidation")
	return renderInvalidation{}
}

func requireNoInvalidation(t *testing.T, ch chan renderInvalidation) {
	t.Helper()
	select {
	case inv := <-ch:
		t.Fatalf("unexpected coordinator invalidation: %+v", inv)
	default:
	}
}

func awaitCoordinatorScheduledTimer(t *testing.T, clk *coordinatorMockClock) *coordinatorMockTimer {
	t.Helper()
	select {
	case tm := <-clk.timers:
		return tm
	default:
		t.Fatal("coordinator did not synchronously arm a fake-clock timer")
		return nil
	}
}

func requireNoCoordinatorOutputFrame(t *testing.T, sends chan ports.Frame) {
	t.Helper()
	select {
	case frame := <-sends:
		t.Fatalf("unexpected output frame: %+v", frame)
	default:
	}
}

// --- wake coalescing and deadlines --------------------------------------------

func TestRenderCoordinatorCoalescesWakesUnderOneDeadline(t *testing.T) {
	cases := []struct {
		name          string
		invalidations []renderInvalidation
		wantTimers    int
		wantMin       time.Duration
		wantMax       time.Duration
		wantWake      renderWake
	}{
		{
			name:          "urgent transition wakes within two milliseconds",
			invalidations: []renderInvalidation{{class: invalidateUrgent, reset: true, producer: "input.go"}},
			wantTimers:    1,
			wantMax:       urgentRenderDeadline,
			wantWake:      renderWake{reset: true, urgent: true, coalesced: 1},
		},
		{
			name: "output burst coalesces into one adaptive deadline",
			invalidations: []renderInvalidation{
				{class: invalidateOutput, producer: "render.go"},
				{class: invalidateOutput, producer: "render.go"},
				{class: invalidateOutput, producer: "render.go"},
			},
			wantTimers: 1,
			wantMin:    minOutputRenderDeadline,
			wantMax:    maxOutputRenderDeadline,
			wantWake:   renderWake{coalesced: 3},
		},
		{
			name: "full redraw stays sticky across later incremental transitions",
			invalidations: []renderInvalidation{
				{class: invalidateOutput},
				{class: invalidateOutput, reset: true},
				{class: invalidateOutput},
			},
			wantTimers: 1,
			wantMin:    minOutputRenderDeadline,
			wantMax:    maxOutputRenderDeadline,
			wantWake:   renderWake{reset: true, coalesced: 3},
		},
		{
			name: "urgent transition tightens a pending output deadline",
			invalidations: []renderInvalidation{
				{class: invalidateOutput},
				{class: invalidateUrgent},
			},
			wantTimers: 2,
			wantMax:    urgentRenderDeadline,
			wantWake:   renderWake{urgent: true, coalesced: 2},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCoordinatorHarness(t)
			for _, inv := range tc.invalidations {
				h.rc.invalidate(inv)
			}
			timers := h.armedTimers(t)
			require.Lenf(t, timers, tc.wantTimers,
				"cap-1 coalescing must arm exactly %d deadline timer(s) for %d invalidations",
				tc.wantTimers, len(tc.invalidations))
			last := timers[len(timers)-1]
			require.GreaterOrEqual(t, last.duration, tc.wantMin)
			require.LessOrEqual(t, last.duration, tc.wantMax)

			last.ch <- time.Time{}
			require.Equal(t, tc.wantWake, awaitWake(t, h.wakes),
				"one wake must deliver the latest coalesced state")
			requireNoWake(t, h.wakes)
		})
	}
}

func TestRenderCoordinatorAdaptsOutputDeadlineAfterBurstAndDecays(t *testing.T) {
	h := newCoordinatorHarness(t)

	// A burst establishes pressure without extending its already-armed first
	// deadline; the following output deadline grows to the 16ms ceiling.
	for range 9 {
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
	}
	burst := h.armedTimers(t)
	require.Len(t, burst, 1)
	require.Equal(t, minOutputRenderDeadline, burst[0].duration)
	burst[0].ch <- time.Time{}
	awaitWake(t, h.wakes)

	h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
	atCap := h.armedTimers(t)
	require.Len(t, atCap, 1)
	require.Equal(t, maxOutputRenderDeadline, atCap[0].duration,
		"burst pressure must be capped at the 16ms output deadline")
	atCap[0].ch <- time.Time{}
	awaitWake(t, h.wakes)

	// Quiet singleton batches decay pressure toward the 8ms floor.
	var previous = maxOutputRenderDeadline
	for range 8 {
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
		timers := h.armedTimers(t)
		require.Len(t, timers, 1)
		require.LessOrEqual(t, timers[0].duration, previous)
		require.GreaterOrEqual(t, timers[0].duration, minOutputRenderDeadline)
		previous = timers[0].duration
		timers[0].ch <- time.Time{}
		awaitWake(t, h.wakes)
	}
	require.Equal(t, minOutputRenderDeadline, previous)
}

func TestRenderCoordinatorUrgentDeadlineCannotBeExtended(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.rc.invalidate(renderInvalidation{class: invalidateOutput})
	h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	for range 32 {
		h.rc.invalidate(renderInvalidation{class: invalidateOutput})
		h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	}
	timers := h.armedTimers(t)
	require.Len(t, timers, 2, "only the urgent promotion may replace the output timer")
	require.Equal(t, urgentRenderDeadline, timers[len(timers)-1].duration)
}

// --- ACK gating ----------------------------------------------------------------

func TestRenderCoordinatorAckGateBlocksCompositionUntilAck(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.ackReady.Store(false)

	h.rc.invalidate(renderInvalidation{class: invalidateOutput})
	h.rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true})
	h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	for _, timer := range h.armedTimers(t) {
		timer.ch <- time.Time{}
	}
	requireNoWake(t, h.wakes)

	h.ackReady.Store(true)
	h.rc.notifyAck()
	w := awaitWake(t, h.wakes)
	require.True(t, w.reset, "the deferred full redraw must stay sticky through the ack gate")
	require.Equal(t, 3, w.coalesced, "the post-ack wake must carry every deferred transition")
	requireNoWake(t, h.wakes)

	h.rc.notifyAck()
	requireNoWake(t, h.wakes)
}

// --- synchronized output ---------------------------------------------------------

func TestRenderCoordinatorSynchronizedOutput(t *testing.T) {
	t.Run("completion flushes pending state in one wake", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		h.syncActive.Store(true)
		h.rc.noteSyncBegin(1)
		watchdogs := h.armedTimers(t)
		require.NotEmpty(t, watchdogs, "sync begin must arm the completion watchdog")
		require.Equal(t, maxSyncUpdateDuration, watchdogs[len(watchdogs)-1].duration)

		h.rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true})
		for _, timer := range h.armedTimers(t) {
			timer.ch <- time.Time{}
		}
		requireNoWake(t, h.wakes)

		h.syncActive.Store(false)
		h.rc.noteSyncEnd(1)
		w := awaitWake(t, h.wakes)
		require.True(t, w.reset)
		requireNoWake(t, h.wakes)
	})

	t.Run("watchdog ends a wedged batch before it flushes", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		h.syncActive.Store(true)
		forced := make(chan struct{}, 1)
		h.rc.noteSyncBegin(7, func() { forced <- struct{}{} })
		watchdogs := h.armedTimers(t)
		require.NotEmpty(t, watchdogs, "sync begin must arm the completion watchdog")
		h.rc.invalidate(renderInvalidation{class: invalidateOutput})

		watchdogs[0].ch <- time.Time{}
		<-forced
		w := awaitWake(t, h.wakes)
		require.True(t, w.watchdog, "a wedged synchronized batch must be force-flushed by the watchdog")
		requireNoWake(t, h.wakes)
	})

	t.Run("stale watchdog generation cannot wake a completed batch", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		h.syncActive.Store(true)
		h.rc.noteSyncBegin(1)
		watchdogs := h.armedTimers(t)
		require.NotEmpty(t, watchdogs, "sync begin must arm the completion watchdog")
		h.syncActive.Store(false)
		h.rc.noteSyncEnd(1)

		watchdogs[0].ch <- time.Time{}
		requireNoWake(t, h.wakes)
	})
}

// --- preview subscription --------------------------------------------------------

func TestRenderCoordinatorPreviewSubscription(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.rc.subscribePreview(func(w renderWake) { h.previews <- w })

	h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	timers := h.armedTimers(t)
	require.NotEmpty(t, timers, "invalidate must arm a deadline timer")
	timers[len(timers)-1].ch <- time.Time{}
	owner := awaitWake(t, h.wakes)
	preview := awaitWake(t, h.previews)
	require.Equal(t, owner, preview, "a subscribed preview observes the same coalesced wake")
	requireNoWake(t, h.previews)

	h.rc.teardownPreview()
	h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	timers = h.armedTimers(t)
	require.NotEmpty(t, timers, "invalidate must arm a deadline timer")
	timers[len(timers)-1].ch <- time.Time{}
	awaitWake(t, h.wakes)
	requireNoWake(t, h.previews)
}

// --- lifecycle and stale callbacks -----------------------------------------------

func TestRenderCoordinatorPreviewSubscriptionsAreIndependent(t *testing.T) {
	h := newCoordinatorHarness(t)
	one, two := &attachedClient{}, &attachedClient{}
	first, second := make(chan renderWake, 1), make(chan renderWake, 1)
	h.rc.subscribePreviewFor(one, func(w renderWake) { first <- w })
	h.rc.subscribePreviewFor(two, func(w renderWake) { second <- w })
	h.rc.attach(&attachedClient{})
	h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "preview"})
	last := h.armedTimers(t)
	last[len(last)-1].ch <- time.Time{}
	awaitWake(t, first)
	awaitWake(t, second)

	h.rc.teardownPreviewFor(one)
	h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "preview"})
	last = h.armedTimers(t)
	last[len(last)-1].ch <- time.Time{}
	requireNoWake(t, first)
	awaitWake(t, second)
}

func TestRenderCoordinatorLifecycleDropsStaleWakes(t *testing.T) {
	cases := []struct {
		name     string
		teardown func(rc *renderCoordinator, owner *attachedClient)
	}{
		{"detach", func(rc *renderCoordinator, owner *attachedClient) { rc.noteDetach(owner) }},
		{"park", func(rc *renderCoordinator, owner *attachedClient) { rc.notePark(owner) }},
		{"session teardown", func(rc *renderCoordinator, _ *attachedClient) { rc.noteSessionTeardown() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCoordinatorHarness(t)
			owner := &attachedClient{}
			h.rc.attach(owner)
			h.rc.invalidate(renderInvalidation{class: invalidateOutput})
			stale := h.armedTimers(t)
			require.NotEmpty(t, stale, "invalidate must arm a deadline timer")

			tc.teardown(h.rc, owner)
			for _, timer := range stale {
				timer.ch <- time.Time{}
			}
			requireNoWake(t, h.wakes)

			h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
			for _, timer := range h.armedTimers(t) {
				timer.ch <- time.Time{}
			}
			requireNoWake(t, h.wakes)
		})
	}

	t.Run("replacement attachment wakes independently", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		owner := &attachedClient{}
		replacement := &attachedClient{}
		h.rc.attach(owner)
		h.rc.invalidate(renderInvalidation{class: invalidateOutput})
		stale := h.armedTimers(t)
		require.NotEmpty(t, stale, "invalidate must arm a deadline timer")

		h.rc.noteReplace(owner, replacement)
		h.rc.attach(replacement)
		for _, timer := range stale {
			timer.ch <- time.Time{}
		}
		requireNoWake(t, h.wakes)

		h.rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true})
		fresh := h.armedTimers(t)
		require.NotEmpty(t, fresh, "a replacement attachment must arm its own deadline")
		fresh[len(fresh)-1].ch <- time.Time{}
		w := awaitWake(t, h.wakes)
		require.True(t, w.reset, "the replacement starts from an independent full state")
		requireNoWake(t, h.wakes)
	})
}

// --- resize metadata ownership -----------------------------------------------------

func TestRenderCoordinatorResizeMetadata(t *testing.T) {
	sz := func(cols, rows int) domain.Size { return domain.Size{Cols: cols, Rows: rows} }
	cases := []struct {
		name string
		run  func(t *testing.T, rc *renderCoordinator, owner, replacement *attachedClient)
	}{
		{
			name: "latest request wins with strictly increasing epochs",
			run: func(t *testing.T, rc *renderCoordinator, owner, _ *attachedClient) {
				require.Equal(t, uint64(1), rc.recordResizeRequest(sz(100, 30), owner))
				require.Equal(t, uint64(2), rc.recordResizeRequest(sz(110, 32), owner))
				require.Equal(t, uint64(3), rc.recordResizeRequest(sz(120, 40), owner))
				snap := rc.resizeSnapshot()
				require.Equal(t, sz(120, 40), snap.size)
				require.Same(t, owner, snap.source)
				require.Equal(t, uint64(3), snap.epoch)

				// Metadata ownership is independent of the retained PR #71
				// attachment dispatch state.
				owner.sendMu.Lock()
				generation, pending := owner.resizePaintGeneration, owner.resizePaintPending
				owner.sendMu.Unlock()
				require.Zero(t, generation, "recording metadata must not touch the PR #71 generation")
				require.False(t, pending, "recording metadata must not arm the PR #71 dispatch")
			},
		},
		{
			name: "duplicate sizes still advance the epoch deterministically",
			run: func(t *testing.T, rc *renderCoordinator, owner, _ *attachedClient) {
				require.Equal(t, uint64(1), rc.recordResizeRequest(sz(120, 40), owner))
				require.Equal(t, uint64(2), rc.recordResizeRequest(sz(120, 40), owner))
				snap := rc.resizeSnapshot()
				require.Equal(t, sz(120, 40), snap.size)
				require.Equal(t, uint64(2), snap.epoch)
			},
		},
		{
			name: "stale resize callbacks preserve latest metadata through lifecycle transitions",
			run: func(t *testing.T, rc *renderCoordinator, owner, replacement *attachedClient) {
				type lifecycleCase struct {
					name       string
					installNew bool
				}
				for _, tc := range []lifecycleCase{
					{name: "detach"},
					{name: "park"},
					{name: "session teardown"},
					{name: "replacement", installNew: true},
				} {
					t.Run(tc.name, func(t *testing.T) {
						h := newCoordinatorHarness(t)
						staleOwner := &attachedClient{}
						freshOwner := &attachedClient{}
						h.rc.attach(staleOwner)
						require.Equal(t, uint64(1), h.rc.recordResizeRequest(sz(120, 40), staleOwner))

						// This models a retained PR #71 callback which captured the old
						// attachment before lifecycle ownership changed.
						staleCallback := func() uint64 {
							return h.rc.recordResizeRequest(sz(80, 20), staleOwner)
						}

						switch tc.name {
						case "detach":
							h.rc.noteDetach(staleOwner)
						case "park":
							h.rc.notePark(staleOwner)
						case "session teardown":
							h.rc.noteSessionTeardown()
						case "replacement":
							h.rc.noteReplace(staleOwner, freshOwner)
							h.rc.attach(freshOwner)
							require.Equal(t, uint64(2), h.rc.recordResizeRequest(sz(100, 50), freshOwner))
						}

						require.Zero(t, staleCallback(), "stale callback must not advance the resize epoch")
						snap := h.rc.resizeSnapshot()
						if tc.installNew {
							require.Equal(t, sz(100, 50), snap.size)
							require.Equal(t, uint64(2), snap.epoch)
							require.Same(t, freshOwner, snap.source)
							return
						}
						require.Equal(t, sz(120, 40), snap.size)
						require.Equal(t, uint64(1), snap.epoch)
						require.Same(t, staleOwner, snap.source,
							"the stale callback must not replace the recorded attachment identity")
					})
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCoordinatorHarness(t)
			owner := &attachedClient{}
			replacement := &attachedClient{}
			h.rc.attach(owner)
			tc.run(t, h.rc, owner, replacement)
		})
	}
}

// --- producer fan-in ------------------------------------------------------------

// producerFiles is the current production direct-paint inventory. Every file
// gets exactly one exercised state transition in TestProducerInvalidations.
var producerFiles = []string{
	"attention.go", "client.go", "copymode.go", "floating.go", "input.go",
	"palette.go", "pane_actions.go", "picker.go", "prompt.go", "render.go",
	"session.go", "session_back.go",
}

func TestProducerInvalidations(t *testing.T) {
	cases := []struct {
		file string
		name string
		tabs int
		run  func(t *testing.T, d *Daemon, sess *session, ac *attachedClient)
	}{
		{
			file: "attention.go",
			name: "attention repaint",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.repaintAttachedClients(sess)
			},
		},
		{
			file: "client.go",
			name: "client theme application",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.applyTheme(sess, ac, ports.Theme{TrueColor: true})
			},
		},
		{
			file: "copymode.go",
			name: "copy mode entry",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.enterCopyMode(sess, ac)
			},
		},
		{
			file: "floating.go",
			name: "floating toggle to visible",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				fp := newPaneWithStableID(layout.PaneID("floating"), "float-producer", newQuietPTY(), domain.Size{Cols: 40, Rows: 10})
				tb := sess.activeTab()
				tb.mu.Lock()
				tb.floating.pane = fp
				tb.floating.state = floatingHidden
				tb.floating.generation = 1
				tb.mu.Unlock()
				require.NoError(t, d.toggleFloating(sess, ac))
			},
		},
		{
			file: "input.go",
			name: "switch tab key action",
			tabs: 2,
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionSwitchTab2)
				require.Equal(t, 1, activeTabIndex(sess))
			},
		},
		{
			file: "palette.go",
			name: "palette entry",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.enterPalette(sess, ac)
			},
		},
		{
			file: "pane_actions.go",
			name: "pane split",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				require.NoError(t, d.splitPane(sess, ac, layout.Right))
			},
		},
		{
			file: "picker.go",
			name: "picker entry",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.enterPicker(sess, ac)
			},
		},
		{
			file: "prompt.go",
			name: "prompt entry",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.enterPrompt(sess, ac, "rename", "", func(string) error { return nil })
			},
		},
		{
			file: "render.go",
			name: "retained resize dispatch",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				clk := newCoordinatorMockClock(t, 2)
				d.clock = clk.clock
				d.resize(sess, ac, domain.Size{Cols: 100, Rows: 26})
				awaitCoordinatorScheduledTimer(t, clk).ch <- time.Time{}
			},
		},
		{
			file: "session.go",
			name: "tab close repaint",
			tabs: 2,
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				sess.mu.Lock()
				tb := sess.tabs[1]
				sess.mu.Unlock()
				d.closeTab(sess, tb, true)
			},
		},
		{
			file: "session_back.go",
			name: "back session fallback without a target",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.backSession(sess, ac)
				require.Same(t, sess, ac.currentSession())
			},
		},
	}

	exercised := make([]string, 0, len(cases))
	for _, tc := range cases {
		exercised = append(exercised, tc.file)
	}
	require.ElementsMatch(t, producerFiles, exercised,
		"every current direct-paint producer file needs exactly one exercised transition")

	for _, tc := range cases {
		t.Run(tc.file+"/"+tc.name, func(t *testing.T) {
			tabs := tc.tabs
			if tabs == 0 {
				tabs = 1
			}
			d, sess, ac, sends, releases := newManualTabSession(t, tabs)
			defer releaseAll(releases)
			invs := make(chan renderInvalidation, 8)
			sess.installRenderCoordinator(newRenderCoordinator(renderCoordinatorOptions{
				clock:        d.clock,
				wake:         func(renderWake) {},
				onInvalidate: func(inv renderInvalidation) { invs <- inv },
			}))

			tc.run(t, d, sess, ac)

			awaitInvalidation(t, invs)
			requireNoInvalidation(t, invs)
			requireNoCoordinatorOutputFrame(t, sends)
		})
	}
}

// TestProducerInvalidationInventory is a supplementary inventory guard only:
// it keeps producerFiles aligned with the daemon source layout so the
// behavioral table above cannot drift silently. It is never acceptance
// evidence for coordinator behavior.
func TestProducerInvalidationInventory(t *testing.T) {
	for _, name := range producerFiles {
		_, err := os.Stat(filepath.Join(".", name))
		require.NoErrorf(t, err, "producer file %s is gone; update TestProducerInvalidations", name)
	}
}

// --- retained PR #71 resize dispatch ----------------------------------------------

func TestConcurrentPaintInitializesOverlayUnderSendOwnership(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	// Exercise the original seam: two fallback paints reach lazy initialization
	// together before either can compose.
	ac.overlays = nil
	ac.overlayOnce = sync.Once{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			<-start
			d.paint(sess, ac, true)
		}()
	}
	close(start)
	wg.Wait()
	require.NotNil(t, ac.overlays)
}

func TestStartPaneGoroutinesAccountsForOneReader(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	tb := sess.activeTab()
	require.NotNil(t, tb)
	d.startPaneGoroutines(sess, tb, tb.focusedPane())
	release()
	select {
	case <-waitGroupDone(&d.sessWg):
	case <-time.After(time.Second):
		t.Fatal("one reader must balance exactly one WaitGroup count")
	}
}

func requireWorkerExit(t *testing.T, done <-chan struct{}) {
	t.Helper()
	for range 4096 {
		select {
		case <-done:
			return
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("cancelled coordinator timer worker did not exit")
}

func TestRenderCoordinatorStopsInertTimerWorkers(t *testing.T) {
	lifecycle := []struct {
		name string
		end  func(*renderCoordinator, *attachedClient)
	}{
		{"detach", func(rc *renderCoordinator, ac *attachedClient) { rc.noteDetach(ac) }},
		{"park", func(rc *renderCoordinator, ac *attachedClient) { rc.notePark(ac) }},
		{"teardown", func(rc *renderCoordinator, _ *attachedClient) { rc.noteSessionTeardown() }},
	}
	for _, tc := range lifecycle {
		t.Run(tc.name, func(t *testing.T) {
			clk := newInertCoordinatorMockClock(t, 2)
			rc := newRenderCoordinator(renderCoordinatorOptions{clock: clk.clock})
			ac := &attachedClient{}
			rc.attach(ac)
			rc.invalidate(renderInvalidation{class: invalidateOutput})
			normal := <-clk.timers
			rc.mu.Lock()
			normalDone := rc.normalWorkerDone
			rc.mu.Unlock()
			tc.end(rc, ac)
			normal.mock.AssertNumberOfCalls(t, "Stop", 1)
			requireWorkerExit(t, normalDone)

			// Use a fresh coordinator because detach/teardown intentionally makes
			// an attachment ineligible for a new synchronized batch.
			rc = newRenderCoordinator(renderCoordinatorOptions{clock: clk.clock})
			rc.attach(ac)
			rc.noteSyncBegin(1)
			syncTimer := <-clk.timers
			rc.mu.Lock()
			syncDone := rc.syncWorkerDone
			rc.mu.Unlock()
			tc.end(rc, ac)
			syncTimer.mock.AssertNumberOfCalls(t, "Stop", 1)
			requireWorkerExit(t, syncDone)
		})
	}
}

func TestRenderCoordinatorRetainsPR71ResizeDispatch(t *testing.T) {
	newResizeFixture := func(t *testing.T) (*Daemon, *session, *attachedClient, chan ports.Frame, *coordinatorMockClock, chan renderInvalidation) {
		t.Helper()
		p, releasePTY := newBlockingPTY(t)
		t.Cleanup(releasePTY)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		clk := newCoordinatorMockClock(t, 4)
		d.clock = clk.clock
		invs := make(chan renderInvalidation, 4)
		sess.installRenderCoordinator(newRenderCoordinator(renderCoordinatorOptions{
			clock:        clk.clock,
			wake:         func(renderWake) {},
			onInvalidate: func(inv renderInvalidation) { invs <- inv },
		}))
		return d, sess, ac, sends, clk, invs
	}

	t.Run("bounded timer records metadata and dispatches through the coordinator", func(t *testing.T) {
		d, sess, ac, sends, clk, invs := newResizeFixture(t)

		d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
		timer := awaitCoordinatorScheduledTimer(t, clk)
		require.GreaterOrEqual(t, timer.duration, minOutputRenderDeadline)
		require.LessOrEqual(t, timer.duration, maxOutputRenderDeadline)
		ac.sendMu.Lock()
		generation, pending := ac.resizePaintGeneration, ac.resizePaintPending
		ac.sendMu.Unlock()
		require.Equal(t, uint64(1), generation, "PR #71 generation ownership must stay with the attachment")
		require.True(t, pending, "PR #71 pending dispatch must remain armed")

		snap := sess.renderCoordinator().resizeSnapshot()
		require.Equal(t, domain.Size{Cols: 120, Rows: 24}, snap.size,
			"the coordinator must record the latest requested resize before delegating")
		require.Same(t, ac, snap.source)
		require.Equal(t, uint64(1), snap.epoch)
		requireNoCoordinatorOutputFrame(t, sends)

		timer.ch <- time.Time{}
		inv := awaitInvalidation(t, invs)
		require.True(t, inv.reset, "the resize dispatch must request a full-redraw invalidation")
		requireNoInvalidation(t, invs)
		requireNoCoordinatorOutputFrame(t, sends)
	})

	t.Run("stale generations stay rejected", func(t *testing.T) {
		d, sess, ac, sends, clk, invs := newResizeFixture(t)

		d.resize(sess, ac, domain.Size{Cols: 100, Rows: 24})
		first := awaitCoordinatorScheduledTimer(t, clk)
		d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
		latest := awaitCoordinatorScheduledTimer(t, clk)
		require.Equal(t, uint64(2), sess.renderCoordinator().resizeSnapshot().epoch,
			"every resize request must advance the coordinator epoch")

		first.ch <- time.Time{}
		requireNoInvalidation(t, invs)
		requireNoCoordinatorOutputFrame(t, sends)

		latest.ch <- time.Time{}
		inv := awaitInvalidation(t, invs)
		require.True(t, inv.reset)
		requireNoInvalidation(t, invs)
		requireNoCoordinatorOutputFrame(t, sends)
		require.Equal(t, domain.Size{Cols: 120, Rows: 24}, sess.renderCoordinator().resizeSnapshot().size)
	})

	t.Run("cancellation still drops the pending dispatch", func(t *testing.T) {
		d, sess, ac, sends, clk, invs := newResizeFixture(t)

		d.resize(sess, ac, domain.Size{Cols: 90, Rows: 30})
		timer := awaitCoordinatorScheduledTimer(t, clk)
		ac.cancelResizePaint()

		timer.ch <- time.Time{}
		requireNoInvalidation(t, invs)
		requireNoCoordinatorOutputFrame(t, sends)
	})
}
