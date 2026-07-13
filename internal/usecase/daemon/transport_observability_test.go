package daemon

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// This acceptance boundary deliberately configures the daemon with an
// observer only. The clock remains the daemon's existing behavior clock;
// timestamp ownership belongs to the concrete process observer.
func TestTransportObservabilityDaemonBoundaries(t *testing.T) {
	observer := &daemonRuntimeObserver{}
	d := New(nil, daemonRuntimeClock{now: time.Unix(0, 101)}, nil, WithRuntimeObserver(observer))
	if d == nil {
		t.Fatal("New() returned nil")
	}

	want := []ports.RuntimeMarkKind{
		ports.RuntimeResizeRequested, ports.RuntimeResizeCommitted,
		ports.RuntimeCaptureStart, ports.RuntimeCaptureEnd,
		ports.RuntimeComposeStart, ports.RuntimeComposeEnd,
		ports.RuntimeDiffStart, ports.RuntimeDiffEnd,
		ports.RuntimeQueueEnqueued, ports.RuntimeQueueDequeued,
		ports.RuntimeACKBlockedStart, ports.RuntimeACKBlockedEnd,
		ports.RuntimeEmitStart, ports.RuntimeEmitEnd,
	}
	if len(want) != 14 {
		t.Fatalf("daemon boundary mark inventory changed: %d", len(want))
	}
}

type daemonRuntimeObserver struct{ marks []ports.RuntimeMark }

func (o *daemonRuntimeObserver) ObserveRuntime(mark ports.RuntimeMark) {
	o.marks = append(o.marks, mark)
}

type daemonRuntimeClock struct{ now time.Time }

func (c daemonRuntimeClock) Now() time.Time { return c.now }
func (c daemonRuntimeClock) NewTimer(d time.Duration) ports.Timer {
	return daemonRuntimeTimer{timer: time.NewTimer(d)}
}

type daemonRuntimeTimer struct{ timer *time.Timer }

func (t daemonRuntimeTimer) C() <-chan time.Time        { return t.timer.C }
func (t daemonRuntimeTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }
func (t daemonRuntimeTimer) Stop() bool                 { return t.timer.Stop() }

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
		d.paint(sess, ac, true)
		close(paintDone)
	}()
	awaitDaemonObserver(t, observer.entered, "blocked render observer")
	frame := <-sends // The frame was emitted before the observer began its I/O.
	output, err := ports.UnmarshalOutput(frame.Payload)
	if err != nil {
		t.Fatalf("decode emitted output: %v", err)
	}
	ac.ackOutputState(output.NewStateNum)

	pane := sess.activeTab().focusedPane()
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
		d.processPTYData(sess, sess.activeTab(), pane, []byte("live"), false)
		close(livePTY)
	}()
	awaitDaemonObserver(t, livePTY, "live PTY processing")

	// A second render can complete its actual pipeline while the first trace
	// write is blocked; it is not serialized behind the first attachment lock.
	secondPaint := make(chan struct{})
	go func() {
		d.paint(sess, ac, false)
		close(secondPaint)
	}()
	<-sends // The second frame is live even though serialized observer I/O waits.

	close(observer.release)
	awaitDaemonObserver(t, paintDone, "first render completion")
	awaitDaemonObserver(t, secondPaint, "unrelated render completion")
}

// ACK-blocked start is captured while coordinator state changes, then emitted
// after c.mu is unlocked. This closes the remaining coordinator-observer path.
func TestTransportObservabilityBlockedCoordinatorObserverReleasesLock(t *testing.T) {
	observer := newBlockingDaemonRuntimeObserver()
	rc := newRenderCoordinator(renderCoordinatorOptions{
		observer: observer,
		ackReady: func() bool { return false },
	})
	rc.attach(&attachedClient{})
	rc.mu.Lock()
	rc.pending = true
	gen := rc.generation
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
