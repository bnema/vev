package daemon

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
)

// This acceptance boundary exercises the attached resize, render, and ACK
// path rather than merely listing the marks the implementation should emit.
func TestTransportObservabilityDaemonBoundaries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observer, rec := newRuntimeObserver(t, nil)
		d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY(t))
		d.runtimeObserver = observer
		// One in-flight state makes the second public resize take the ACK-blocked
		// path. The real coordinator owns both renders and the ACK wake.
		ac.output = newOutputStateStream(1)
		rc := d.attachCoordinator(sess, nil, ac, true)

		if !d.resizeForFirstPaint(sess, ac, domain.Size{Cols: 90, Rows: 25}) {
			t.Fatal("first resize was not accepted")
		}
		first := requireFrame(t, sends, "Output")
		if envelopeMessageName(t, first.Payload) != "Output" {
			t.Fatalf("first frame = %v, want output", envelopeMessageName(t, first.Payload))
		}
		output := unmarshalTestOutput(t, first.Payload)
		if !d.resizeForFirstPaint(sess, ac, domain.Size{Cols: 100, Rows: 26}) {
			t.Fatal("second resize was not accepted")
		}
		ac.ackOutputState(output.Epoch, output.New)
		rc.notifyAck()
		second := requireFrame(t, sends, "Output")
		if envelopeMessageName(t, second.Payload) != "Output" {
			t.Fatalf("second frame = %v, want output", envelopeMessageName(t, second.Payload))
		}

		want := []ports.RuntimeMarkKind{
			ports.RuntimeResizeRequested, ports.RuntimeResizeCommitted,
			ports.RuntimeQueueEnqueued, ports.RuntimeQueueDequeued,
			ports.RuntimeCaptureStart, ports.RuntimeCaptureEnd,
			ports.RuntimeComposeStart, ports.RuntimeComposeEnd,
			ports.RuntimeDiffStart, ports.RuntimeDiffEnd,
			ports.RuntimeEmitStart, ports.RuntimeEmitEnd,
			ports.RuntimeResizeRequested, ports.RuntimeResizeCommitted,
			ports.RuntimeQueueEnqueued, ports.RuntimeACKBlockedStart,
			ports.RuntimeACKBlockedEnd, ports.RuntimeQueueDequeued,
			ports.RuntimeCaptureStart, ports.RuntimeCaptureEnd,
			ports.RuntimeComposeStart, ports.RuntimeComposeEnd,
			ports.RuntimeDiffStart, ports.RuntimeDiffEnd,
			ports.RuntimeEmitStart, ports.RuntimeEmitEnd,
		}
		marks := rec.snapshot()
		if len(marks) != len(want) {
			t.Fatalf("runtime marks = %#v, want kinds %#v", marks, want)
		}
		for i, kind := range want {
			if marks[i].Kind != kind {
				t.Fatalf("runtime mark %d = %q, want %q; all=%#v", i, marks[i].Kind, kind, marks)
			}
		}
	})
}

// runtimeObserverMock returns a generated RuntimeObserver mock whose marks are
// recorded; onMark (optional) runs for every mark after it is recorded.
type runtimeMarks struct {
	mu    sync.Mutex
	marks []ports.RuntimeMark
}

func (r *runtimeMarks) snapshot() []ports.RuntimeMark {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ports.RuntimeMark(nil), r.marks...)
}

func newRuntimeObserver(t testing.TB, before func(ports.RuntimeMark)) (*portsmocks.MockRuntimeObserver, *runtimeMarks) {
	t.Helper()
	rec := &runtimeMarks{}
	obs := portsmocks.NewMockRuntimeObserver(t)
	obs.EXPECT().ObserveRuntime(mock.Anything).Run(func(mark ports.RuntimeMark) {
		if before != nil {
			before(mark)
		}
		rec.mu.Lock()
		rec.marks = append(rec.marks, mark)
		rec.mu.Unlock()
	}).Maybe()
	return obs, rec
}

// newBlockingRuntimeObserver blocks the first observed mark until release closes.
func newBlockingRuntimeObserver(t testing.TB) (obs *portsmocks.MockRuntimeObserver, entered, release chan struct{}) {
	t.Helper()
	entered, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	obs = portsmocks.NewMockRuntimeObserver(t)
	obs.EXPECT().ObserveRuntime(mock.Anything).Run(func(ports.RuntimeMark) {
		once.Do(func() {
			close(entered)
			<-release
		})
	}).Maybe()
	return obs, entered, release
}

// A runtime observer may synchronously write JSONL. Its I/O must run only
// after the render transaction has released attachment, session, and pane
// ownership, so a blocked trace cannot stall live output processing.
func TestTransportObservabilityBlockedRenderObserverReleasesArchitectureLocks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
		observer, observerEntered, observerRelease := newBlockingRuntimeObserver(t)
		d.runtimeObserver = observer

		paintDone := make(chan struct{})
		go func() {
			d.paint(sess, ac, true, nil)
			close(paintDone)
		}()
		requireReceived(t, observerEntered, "blocked render observer")
		frame := requireFrame(t, sends, "Output") // The frame was emitted before the observer began its I/O.
		output := unmarshalTestOutput(t, frame.Payload)
		ac.ackOutputState(output.Epoch, output.New)

		pane := testAttachmentTab(sess).focusedPane()
		for _, lock := range []struct {
			name string
			fn   func()
		}{
			{"attachment", func() { ac.sendMu.Lock(); runtime.Gosched(); ac.sendMu.Unlock() }},
			{"session", func() { sess.mu.Lock(); runtime.Gosched(); sess.mu.Unlock() }},
			{"pane", func() { pane.mu.Lock(); runtime.Gosched(); pane.mu.Unlock() }},
		} {
			done := make(chan struct{})
			go func(fn func()) { fn(); close(done) }(lock.fn)
			requireReceived(t, done, lock.name+" lock")
		}

		livePTY := make(chan struct{})
		go func() {
			d.processPTYData(sess, testAttachmentTab(sess), pane, []byte("live"), false)
			close(livePTY)
		}()
		requireReceived(t, livePTY, "live PTY processing")

		// A second render can complete its actual pipeline while the first trace
		// write is blocked; it is not serialized behind the first attachment lock.
		secondPaint := make(chan struct{})
		go func() {
			d.paint(sess, ac, false, nil)
			close(secondPaint)
		}()
		// The second paint's observer call queues on the observer's sync.Once mutex
		// (not durably blocked, so synctest.Wait would not return); a plain receive
		// still fails as a bubble deadlock if the frame were gated behind it.
		_ = <-sends // The second frame is live even though serialized observer I/O waits.

		close(observerRelease)
		requireReceived(t, paintDone, "first render completion")
		requireReceived(t, secondPaint, "unrelated render completion")
	})
}

// ACK-blocked start is captured while coordinator state changes, then emitted
// after c.mu is unlocked. This closes the remaining coordinator-observer path.
func TestTransportObservabilityACKBlockedSpansEndExactlyOnce(t *testing.T) {
	cases := []struct {
		name   string
		finish func(*renderCoordinator, *bool)
	}{
		{
			name: "notify ACK",
			finish: func(rc *renderCoordinator, ready *bool) {
				*ready = true
				rc.notifyAck()
			},
		},
		{
			name: "successful consume",
			finish: func(rc *renderCoordinator, ready *bool) {
				*ready = true
				rc.fire(rc.normalLane.generation, false, false)
			},
		},
		{
			name: "teardown",
			finish: func(rc *renderCoordinator, ready *bool) {
				rc.beginSessionTeardown().finish()
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				observer, rec := newRuntimeObserver(t, nil)
				ready := false
				rc := newRenderCoordinator(renderCoordinatorOptions{
					observer:    observer,
					ackReadyFor: func(*attachedClient) bool { return ready },
				})
				rc.attach(&attachedClient{})
				rc.invalidate(renderInvalidation{class: invalidateOutput})
				rc.fire(rc.normalLane.generation, false, true)
				tt.finish(rc, &ready)
				// A late notification after consuming or tearing down must not emit a
				// second end for the already-cleared blocked interval.
				rc.notifyAck()

				var starts, ends []ports.RuntimeMark
				for _, mark := range rec.snapshot() {
					switch mark.Kind {
					case ports.RuntimeACKBlockedStart:
						starts = append(starts, mark)
					case ports.RuntimeACKBlockedEnd:
						ends = append(ends, mark)
					}
				}
				if len(starts) != 1 || len(ends) != 1 {
					t.Fatalf("ACK-blocked span marks: starts=%+v ends=%+v all=%+v", starts, ends, rec.snapshot())
				}
				if starts[0].Sequence != ends[0].Sequence || starts[0].RequestID != ends[0].RequestID || starts[0].Epoch != ends[0].Epoch {
					t.Fatalf("ACK-blocked span correlation changed: start=%+v end=%+v", starts[0], ends[0])
				}
			})
		})
	}
}

// A raw observer supplied through daemon configuration is serialized by the
// daemon. Once Start has been published, a blocked End must not delay the ACK
// that consumes deferred work and delivers its wake.
func TestTransportObservabilityBlockedACKEndDoesNotDelayNotifyAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		startObserved, endEntered, releaseEnd := make(chan struct{}), make(chan struct{}), make(chan struct{})
		observer := portsmocks.NewMockRuntimeObserver(t)
		observer.EXPECT().ObserveRuntime(mock.Anything).Run(func(mark ports.RuntimeMark) {
			switch mark.Kind {
			case ports.RuntimeACKBlockedStart:
				close(startObserved)
			case ports.RuntimeACKBlockedEnd:
				close(endEntered)
				<-releaseEnd
			}
		}).Maybe()
		reporter := observability.NewSerialized(observer, 64)
		defer reporter.Close()
		d := New(nil, nil, nil, WithRuntimeObserver(reporter))
		wakes := make(chan renderWake, 1)
		var ready atomic.Bool
		rc := newRenderCoordinator(renderCoordinatorOptions{
			observer:    d.runtimeObserver,
			ackReadyFor: func(*attachedClient) bool { return ready.Load() },
			wake:        func(w renderWake) { wakes <- w },
		})
		rc.attach(&attachedClient{})
		rc.invalidate(renderInvalidation{class: invalidateOutput})

		// Enter the ACK-blocked state and wait until Start has reached the injected
		// observer. End is the only deliberately blocked call in this test.
		rc.fire(rc.normalLane.generation, false, true)
		requireReceived(t, startObserved, "ACK-blocked start observer")

		ready.Store(true)
		ackDone := make(chan struct{})
		go func() {
			rc.notifyAck()
			close(ackDone)
		}()
		requireReceived(t, endEntered, "blocked ACK-blocked end observer")
		requireReceived(t, ackDone, "nonblocking ACK progress")
		awaitWake(t, wakes)
		rc.mu.Lock()
		pending := rc.pending
		rc.mu.Unlock()
		if pending {
			t.Fatal("notifyAck left consumed ACK-deferred work pending")
		}

		close(releaseEnd)
		reporter.Close()
	})
}

func TestTransportObservabilityBlockedCoordinatorObserverReleasesLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observer, observerEntered, observerRelease := newBlockingRuntimeObserver(t)
		rc := newRenderCoordinator(renderCoordinatorOptions{
			observer:    observer,
			ackReadyFor: func(*attachedClient) bool { return false },
		})
		rc.attach(&attachedClient{})
		rc.mu.Lock()
		rc.pending = true
		gen := rc.normalLane.generation
		rc.mu.Unlock()

		fired := make(chan struct{})
		go func() {
			rc.fire(gen, false, true)
			close(fired)
		}()
		requireReceived(t, observerEntered, "blocked ACK observer")

		acquired := make(chan struct{})
		go func() {
			rc.mu.Lock()
			runtime.Gosched()
			rc.mu.Unlock()
			close(acquired)
		}()
		requireReceived(t, acquired, "coordinator lock")

		close(observerRelease)
		requireReceived(t, fired, "ACK fire completion")
	})
}

// An ACK can consume deferred work while the start mark is still waiting on
// observer I/O. The End must wait for that Start publication, but the ACK
// itself must not wait for either observer call.
func TestTransportObservabilityACKBlockedStartPublishesBeforeEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		startEntered, releaseStart := make(chan struct{}), make(chan struct{})
		observer, rec := newRuntimeObserver(t, func(mark ports.RuntimeMark) {
			if mark.Kind == ports.RuntimeACKBlockedStart {
				close(startEntered)
				<-releaseStart
			}
		})
		ready := false
		rc := newRenderCoordinator(renderCoordinatorOptions{
			observer:    observer,
			ackReadyFor: func(*attachedClient) bool { return ready },
		})
		rc.attach(&attachedClient{})
		rc.mu.Lock()
		rc.pending = true
		gen := rc.normalLane.generation
		rc.mu.Unlock()

		fireDone := make(chan struct{})
		go func() {
			rc.fire(gen, false, true)
			close(fireDone)
		}()
		requireReceived(t, startEntered, "ACK-blocked start observer")

		ready = true
		ackDone := make(chan struct{})
		go func() {
			rc.notifyAck()
			close(ackDone)
		}()
		requireReceived(t, ackDone, "nonblocking ACK progress")
		if marks := rec.snapshot(); len(marks) != 0 {
			t.Fatalf("ACK-blocked end published before its blocked start: %#v", marks)
		}

		close(releaseStart)
		requireReceived(t, fireDone, "ACK-blocked start completion")
		marks := rec.snapshot()
		if len(marks) != 2 || marks[0].Kind != ports.RuntimeACKBlockedStart || marks[1].Kind != ports.RuntimeACKBlockedEnd {
			t.Fatalf("ACK-blocked marks = %#v, want one ordered start/end pair", marks)
		}
		if marks[0].Sequence != marks[1].Sequence || marks[0].RequestID != marks[1].RequestID || marks[0].Epoch != marks[1].Epoch {
			t.Fatalf("ACK-blocked span correlation changed: start=%+v end=%+v", marks[0], marks[1])
		}
	})
}
