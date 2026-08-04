package daemon

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// This acceptance boundary exercises the attached resize, render, and ACK
// path rather than merely listing the marks the implementation should emit.
func TestTransportObservabilityDaemonBoundaries(t *testing.T) {
	observer := &daemonRuntimeObserver{}
	d, sess, ac, sends := newManualSessionWithPTYs(t, &quietPTY{})
	d.runtimeObserver = observer
	// One in-flight state makes the second public resize take the ACK-blocked
	// path. The real coordinator owns both renders and the ACK wake.
	ac.output = newOutputStateStream(1)
	rc := d.attachCoordinator(sess, nil, ac, true)

	if !d.resizeForFirstPaint(sess, ac, domain.Size{Cols: 90, Rows: 25}) {
		t.Fatal("first resize was not accepted")
	}
	first := awaitFrame(t, sends, ports.MsgOutput)
	if first.Type != ports.MsgOutput {
		t.Fatalf("first frame = %v, want output", first.Type)
	}
	output, err := ports.UnmarshalOutput(first.Payload)
	if err != nil {
		t.Fatalf("decode first output: %v", err)
	}
	if !d.resizeForFirstPaint(sess, ac, domain.Size{Cols: 100, Rows: 26}) {
		t.Fatal("second resize was not accepted")
	}
	ac.ackOutputState(output.Epoch, output.New)
	rc.notifyAck()
	second := awaitFrame(t, sends, ports.MsgOutput)
	if second.Type != ports.MsgOutput {
		t.Fatalf("second frame = %v, want output", second.Type)
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
	if len(observer.marks) != len(want) {
		t.Fatalf("runtime marks = %#v, want kinds %#v", observer.marks, want)
	}
	for i, kind := range want {
		if observer.marks[i].Kind != kind {
			t.Fatalf("runtime mark %d = %q, want %q; all=%#v", i, observer.marks[i].Kind, kind, observer.marks)
		}
	}
}

type daemonRuntimeObserver struct{ marks []ports.RuntimeMark }

func (o *daemonRuntimeObserver) ObserveRuntime(mark ports.RuntimeMark) {
	o.marks = append(o.marks, mark)
}

type blockingDaemonRuntimeObserver struct {
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	blocked bool
}

func newBlockingDaemonRuntimeObserver() *blockingDaemonRuntimeObserver {
	return &blockingDaemonRuntimeObserver{entered: make(chan struct{}), release: make(chan struct{})}
}

func (o *blockingDaemonRuntimeObserver) ObserveRuntime(ports.RuntimeMark) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.blocked {
		o.blocked = true
		close(o.entered)
		<-o.release
	}
}

func awaitDaemonObserver(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// A runtime observer may synchronously write JSONL. Its I/O must run only
// after the render transaction has released attachment, session, and pane
// ownership, so a blocked trace cannot stall live output processing.
func TestTransportObservabilityBlockedRenderObserverReleasesArchitectureLocks(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	observer := newBlockingDaemonRuntimeObserver()
	d.runtimeObserver = observer

	paintDone := make(chan struct{})
	go func() {
		d.paint(sess, ac, true, nil)
		close(paintDone)
	}()
	awaitDaemonObserver(t, observer.entered, "blocked render observer")
	frame := awaitFrame(t, sends, ports.MsgOutput) // The frame was emitted before the observer began its I/O.
	output, err := ports.UnmarshalOutput(frame.Payload)
	if err != nil {
		t.Fatalf("decode emitted output: %v", err)
	}
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
		awaitDaemonObserver(t, done, lock.name+" lock")
	}

	livePTY := make(chan struct{})
	go func() {
		d.processPTYData(sess, testAttachmentTab(sess), pane, []byte("live"), false)
		close(livePTY)
	}()
	awaitDaemonObserver(t, livePTY, "live PTY processing")

	// A second render can complete its actual pipeline while the first trace
	// write is blocked; it is not serialized behind the first attachment lock.
	secondPaint := make(chan struct{})
	go func() {
		d.paint(sess, ac, false, nil)
		close(secondPaint)
	}()
	_ = awaitFrame(t, sends, ports.MsgOutput) // The second frame is live even though serialized observer I/O waits.

	close(observer.release)
	awaitDaemonObserver(t, paintDone, "first render completion")
	awaitDaemonObserver(t, secondPaint, "unrelated render completion")
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
			observer := &daemonRuntimeObserver{}
			ready := false
			rc := newRenderCoordinator(renderCoordinatorOptions{
				observer: observer,
				ackReady: func() bool { return ready },
			})
			rc.attach(&attachedClient{})
			rc.invalidate(renderInvalidation{class: invalidateOutput})
			rc.fire(rc.normalLane.generation, false, true)
			tt.finish(rc, &ready)
			// A late notification after consuming or tearing down must not emit a
			// second end for the already-cleared blocked interval.
			rc.notifyAck()

			var starts, ends []ports.RuntimeMark
			for _, mark := range observer.marks {
				switch mark.Kind {
				case ports.RuntimeACKBlockedStart:
					starts = append(starts, mark)
				case ports.RuntimeACKBlockedEnd:
					ends = append(ends, mark)
				}
			}
			if len(starts) != 1 || len(ends) != 1 {
				t.Fatalf("ACK-blocked span marks: starts=%+v ends=%+v all=%+v", starts, ends, observer.marks)
			}
			if starts[0].Sequence != ends[0].Sequence || starts[0].RequestID != ends[0].RequestID || starts[0].Epoch != ends[0].Epoch {
				t.Fatalf("ACK-blocked span correlation changed: start=%+v end=%+v", starts[0], ends[0])
			}
		})
	}
}

// A raw observer supplied through daemon configuration is serialized by the
// daemon. Once Start has been published, a blocked End must not delay the ACK
// that consumes deferred work and delivers its wake.
func TestTransportObservabilityBlockedACKEndDoesNotDelayNotifyAck(t *testing.T) {
	observer := &blockingACKEndObserver{
		startObserved: make(chan struct{}),
		endEntered:    make(chan struct{}),
		releaseEnd:    make(chan struct{}),
	}
	reporter := ports.NewSerializedRuntimeObserver(observer, 64)
	defer reporter.Close()
	d := New(nil, nil, nil, WithRuntimeObserver(reporter))
	wakes := make(chan renderWake, 1)
	var ready atomic.Bool
	rc := newRenderCoordinator(renderCoordinatorOptions{
		observer: d.runtimeObserver,
		ackReady: ready.Load,
		wake:     func(w renderWake) { wakes <- w },
	})
	rc.attach(&attachedClient{})
	rc.invalidate(renderInvalidation{class: invalidateOutput})

	// Enter the ACK-blocked state and wait until Start has reached the injected
	// observer. End is the only deliberately blocked call in this test.
	rc.fire(rc.normalLane.generation, false, true)
	awaitDaemonObserver(t, observer.startObserved, "ACK-blocked start observer")

	ready.Store(true)
	ackDone := make(chan struct{})
	go func() {
		rc.notifyAck()
		close(ackDone)
	}()
	awaitDaemonObserver(t, observer.endEntered, "blocked ACK-blocked end observer")
	awaitDaemonObserver(t, ackDone, "nonblocking ACK progress")
	awaitWake(t, wakes)
	rc.mu.Lock()
	pending := rc.pending
	rc.mu.Unlock()
	if pending {
		t.Fatal("notifyAck left consumed ACK-deferred work pending")
	}

	close(observer.releaseEnd)
	reporter.Close()
}

type blockingACKEndObserver struct {
	startObserved chan struct{}
	endEntered    chan struct{}
	releaseEnd    chan struct{}
}

func (o *blockingACKEndObserver) ObserveRuntime(mark ports.RuntimeMark) {
	switch mark.Kind {
	case ports.RuntimeACKBlockedStart:
		close(o.startObserved)
	case ports.RuntimeACKBlockedEnd:
		close(o.endEntered)
		<-o.releaseEnd
	}
}

func TestTransportObservabilityBlockedCoordinatorObserverReleasesLock(t *testing.T) {
	observer := newBlockingDaemonRuntimeObserver()
	rc := newRenderCoordinator(renderCoordinatorOptions{
		observer: observer,
		ackReady: func() bool { return false },
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
	awaitDaemonObserver(t, observer.entered, "blocked ACK observer")

	acquired := make(chan struct{})
	go func() {
		rc.mu.Lock()
		runtime.Gosched()
		rc.mu.Unlock()
		close(acquired)
	}()
	awaitDaemonObserver(t, acquired, "coordinator lock")

	close(observer.release)
	awaitDaemonObserver(t, fired, "ACK fire completion")
}

type ackSpanInterleavingObserver struct {
	mu           sync.Mutex
	marks        []ports.RuntimeMark
	startEntered chan struct{}
	releaseStart chan struct{}
}

func newACKSpanInterleavingObserver() *ackSpanInterleavingObserver {
	return &ackSpanInterleavingObserver{startEntered: make(chan struct{}), releaseStart: make(chan struct{})}
}

func (o *ackSpanInterleavingObserver) ObserveRuntime(mark ports.RuntimeMark) {
	if mark.Kind == ports.RuntimeACKBlockedStart {
		close(o.startEntered)
		<-o.releaseStart
	}
	o.mu.Lock()
	o.marks = append(o.marks, mark)
	o.mu.Unlock()
}

func (o *ackSpanInterleavingObserver) snapshot() []ports.RuntimeMark {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]ports.RuntimeMark(nil), o.marks...)
}

// An ACK can consume deferred work while the start mark is still waiting on
// observer I/O. The End must wait for that Start publication, but the ACK
// itself must not wait for either observer call.
func TestTransportObservabilityACKBlockedStartPublishesBeforeEnd(t *testing.T) {
	observer := newACKSpanInterleavingObserver()
	ready := false
	rc := newRenderCoordinator(renderCoordinatorOptions{
		observer: observer,
		ackReady: func() bool { return ready },
	})
	rc.mu.Lock()
	rc.pending = true
	gen := rc.normalLane.generation
	rc.mu.Unlock()

	fireDone := make(chan struct{})
	go func() {
		rc.fire(gen, false, true)
		close(fireDone)
	}()
	awaitDaemonObserver(t, observer.startEntered, "ACK-blocked start observer")

	ready = true
	ackDone := make(chan struct{})
	go func() {
		rc.notifyAck()
		close(ackDone)
	}()
	awaitDaemonObserver(t, ackDone, "nonblocking ACK progress")
	if marks := observer.snapshot(); len(marks) != 0 {
		t.Fatalf("ACK-blocked end published before its blocked start: %#v", marks)
	}

	close(observer.releaseStart)
	awaitDaemonObserver(t, fireDone, "ACK-blocked start completion")
	marks := observer.snapshot()
	if len(marks) != 2 || marks[0].Kind != ports.RuntimeACKBlockedStart || marks[1].Kind != ports.RuntimeACKBlockedEnd {
		t.Fatalf("ACK-blocked marks = %#v, want one ordered start/end pair", marks)
	}
	if marks[0].Sequence != marks[1].Sequence || marks[0].RequestID != marks[1].RequestID || marks[0].Epoch != marks[1].Epoch {
		t.Fatalf("ACK-blocked span correlation changed: start=%+v end=%+v", marks[0], marks[1])
	}
}
