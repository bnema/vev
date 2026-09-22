package client

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Resize presentation-invalidation tests (Plan 001, uncommitted slice).
//
// The mechanism under test is deliberately split across two owners:
//
//   - the attachment host's geometry collector records the latest valid
//     Terminal.Geometry and emits two independent coalesced wakeups: the
//     attachment sequence wakeup (geometryUpdate) that a granted foreground
//     consumes through Resize, and the presentation-invalidation wakeup
//     (presentationUpdate) that only the supervisor's serialized control path
//     consumes;
//   - the supervisor consumes presentationUpdate in every wait it owns
//     (attempt, ready, termination, backoff) and repaints only a picker
//     presentation (PickerPresentation), sampling Terminal.Geometry at render
//     time. The attachment settlement wait never consumes it: Begin has already
//     admitted a foreground that can write session output through the same
//     terminal before MarkAttached and the initial publication, so the
//     invalidation stays buffered for the next picker state instead of being
//     painted over it.
//
// Every test below is deterministic: a fake clock, scripted fakes, bounded
// waits, and channel signals instead of sleeps. The resize channel is pushed
// by the test itself, so no ordering is hoped for.

// resizeTestTerminal is the supervisor-owned terminal double with a
// test-driven ResizeEvents channel and a Geometry that reports the latest
// pushed size. It embeds the attachment-supervisor terminal double so it also
// satisfies the UI publication seam, and only overrides geometry.
type resizeTestTerminal struct {
	*attachTestTerminal
	resizes    chan domain.Geometry
	geometryMu sync.Mutex
	geometry   domain.Geometry
}

func newResizeTestTerminal(in io.Reader, geometry domain.Geometry) *resizeTestTerminal {
	return &resizeTestTerminal{
		attachTestTerminal: &attachTestTerminal{in: in},
		resizes:            make(chan domain.Geometry, 8),
		geometry:           geometry,
	}
}

func (t *resizeTestTerminal) ResizeEvents() <-chan domain.Geometry { return t.resizes }

func (t *resizeTestTerminal) Geometry() (domain.Geometry, error) {
	t.geometryMu.Lock()
	defer t.geometryMu.Unlock()
	return t.geometry, nil
}

// resize records the new size as the terminal's current geometry and then
// publishes the event, exactly like a physical terminal whose size query and
// resize event agree.
func (t *resizeTestTerminal) resize(geometry domain.Geometry) {
	t.geometryMu.Lock()
	t.geometry = geometry
	t.geometryMu.Unlock()
	t.resizes <- geometry
}

func (t *resizeTestTerminal) closeResizeEvents() { close(t.resizes) }

// setWritten replaces the terminal's committed output, so a test can seed the
// bytes an admitted foreground would have written and then prove no picker
// repaint overpainted them.
func (t *resizeTestTerminal) setWritten(data string) {
	t.attachTestTerminal.mu.Lock()
	defer t.attachTestTerminal.mu.Unlock()
	t.attachTestTerminal.out.Reset()
	t.attachTestTerminal.out.WriteString(data)
}

// resizeRenderSample is one observed Render call: the exact state handed to the
// renderer plus the terminal geometry the renderer would have sampled, exactly
// as the offline composition does when it paints the picker.
type resizeRenderSample struct {
	state    State
	geometry domain.Geometry
}

// resizeRenderRecorder records every Render call both in a channel (for
// ordered waits) and in a slice (for counting), so a test can prove that a
// repaint happened, in what state, and against which geometry.
type resizeRenderRecorder struct {
	terminal *resizeTestTerminal
	renders  chan resizeRenderSample

	mu  sync.Mutex
	all []resizeRenderSample
}

func newResizeRenderRecorder(terminal *resizeTestTerminal) *resizeRenderRecorder {
	return &resizeRenderRecorder{terminal: terminal, renders: make(chan resizeRenderSample, 256)}
}

// resizeRenderPaint is the picker frame marker the recording renderer writes.
// It stands for the production composition's picker bytes: a test can prove the
// renderer ran and that its output never landed over session output.
const resizeRenderPaint = "<picker-frame>"

func (r *resizeRenderRecorder) render(state State) {
	geometry, _ := r.terminal.Geometry()
	sample := resizeRenderSample{state: state, geometry: geometry}
	r.mu.Lock()
	r.all = append(r.all, sample)
	r.mu.Unlock()
	// Exactly like the production offline composition, the recording renderer
	// writes its frame only while the shared fence admits the state. A mutation
	// that widened the fence would therefore land bytes a test can observe.
	if PickerPresentation(state) {
		_, _ = r.terminal.Out().Write([]byte(resizeRenderPaint))
	}
	select {
	case r.renders <- sample:
	default:
	}
}

// drain discards every render already delivered to the ordered channel; the
// counted slice is retained so a caller can still take a baseline count.
func (r *resizeRenderRecorder) drain() {
	for {
		select {
		case <-r.renders:
		default:
			return
		}
	}
}

func (r *resizeRenderRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.all)
}

// last returns the most recent render sample, or the zero sample when nothing
// was rendered.
func (r *resizeRenderRecorder) last() resizeRenderSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.all) == 0 {
		return resizeRenderSample{}
	}
	return r.all[len(r.all)-1]
}

// samples returns every recorded render sample in call order.
func (r *resizeRenderRecorder) samples() []resizeRenderSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]resizeRenderSample(nil), r.all...)
}

// awaitResizeRender waits for the next render matching match, bounded.
func awaitResizeRender(t *testing.T, recorder *resizeRenderRecorder, match func(resizeRenderSample) bool) resizeRenderSample {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case sample := <-recorder.renders:
			if match(sample) {
				return sample
			}
		case <-timeout:
			t.Fatal("the supervisor never repainted the presentation for the resize")
			return resizeRenderSample{}
		}
	}
}

// pendingPresentationSignals reports how many coalesced presentation
// invalidations are buffered. Reading len of a channel is synchronized, so it
// is safe from the test goroutine while the collector runs.
func (h *attachmentHost) pendingPresentationSignals() int { return len(h.presentationUpdate) }

// pendingGeometrySignals reports how many attachment geometry wakeups are
// buffered, so a test can prove the two signals are independent.
func (h *attachmentHost) pendingGeometrySignals() int { return len(h.geometryUpdate) }

// geometryProgress reports how many valid resizes the collector has recorded.
func (h *attachmentHost) geometryProgress() uint64 {
	h.geometryMu.Lock()
	defer h.geometryMu.Unlock()
	return h.geometrySeq
}

// resizeSupervisorHarness is one running supervisor with its fakes: a
// test-driven resize channel, a recording renderer, and a fake clock.
type resizeSupervisorHarness struct {
	sup       *Supervisor
	service   *supervisorTestService
	clock     *supervisorTestClock
	terminal  *resizeTestTerminal
	reader    *attachTestReader
	connector *supervisorTestConnector
	recorder  *resizeRenderRecorder

	cancel  context.CancelFunc
	runDone chan struct{}

	runMu  sync.Mutex
	runErr error
}

type resizeHarnessOptions struct {
	// picker is the supervisor's picker seam; nil runs the picker-less driver.
	picker pickerHost
	// geometry is the terminal's initial reported geometry.
	geometry domain.Geometry
	// closeResizes hands the supervisor an already closed ResizeEvents channel.
	closeResizes bool
	// connect scripts the broker attempts; nil returns the harness service.
	connect func(ctx context.Context, call int) (ports.BrokerService, error)
}

// startResizeHarness builds and runs one supervisor over the resize-aware
// fakes. It returns as soon as Run is started, so a test can park on any wait.
func startResizeHarness(t *testing.T, options resizeHarnessOptions) *resizeSupervisorHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clock := newSupervisorTestClock()
	reader := newAttachTestReader()
	geometry := options.geometry
	if geometry == (domain.Geometry{}) {
		geometry = domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}
	}
	terminal := newResizeTestTerminal(reader, geometry)
	if options.closeResizes {
		terminal.closeResizeEvents()
	}
	service := newSupervisorTestService(ports.BrokerConnectionID{1})
	service.publish(3, 1)
	connect := options.connect
	if connect == nil {
		connect = func(context.Context, int) (ports.BrokerService, error) { return service, nil }
	}
	connector := newSupervisorTestConnector(connect)
	recorder := newResizeRenderRecorder(terminal)
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
		Picker:    options.picker,
		Render:    recorder.render,
	})

	harness := &resizeSupervisorHarness{
		sup:       sup,
		service:   service,
		clock:     clock,
		terminal:  terminal,
		reader:    reader,
		connector: connector,
		recorder:  recorder,
		cancel:    cancel,
		runDone:   make(chan struct{}),
	}
	go func() {
		err := sup.Run(ctx)
		harness.runMu.Lock()
		harness.runErr = err
		harness.runMu.Unlock()
		close(harness.runDone)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-harness.runDone:
		case <-time.After(5 * time.Second):
			t.Error("the supervisor did not stop")
		}
		// Release the single input lifetime's reader so the pump goroutine that
		// outlives Run, exactly like a real stdin that never reaches EOF, ends.
		reader.close()
	})
	return harness
}

func (h *resizeSupervisorHarness) waitRun(t *testing.T) error {
	t.Helper()
	select {
	case <-h.runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the supervisor did not stop")
	}
	h.runMu.Lock()
	defer h.runMu.Unlock()
	return h.runErr
}

// resizeStreamRegistry admits one fresh scripted logical stream per open call
// and lets a test reach the stream of a specific attachment attempt.
type resizeStreamRegistry struct {
	mu      sync.Mutex
	streams []*sessionTestStream
}

func (r *resizeStreamRegistry) open(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	stream := newSessionTestStream()
	r.mu.Lock()
	r.streams = append(r.streams, stream)
	r.mu.Unlock()
	return stream, nil
}

// stream waits until the index-th stream was admitted and returns it.
func (r *resizeStreamRegistry) stream(t *testing.T, index int) *sessionTestStream {
	t.Helper()
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.streams) > index
	}, 5*time.Second, time.Millisecond, "the supervisor never admitted stream %d", index)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.streams[index]
}

// streamResizes returns the typed resize messages one attachment stream sent.
func streamResizes(stream *sessionTestStream) []protocol.Resize {
	var resizes []protocol.Resize
	for _, message := range stream.messages() {
		if resize, ok := message.(protocol.Resize); ok {
			resizes = append(resizes, resize)
		}
	}
	return resizes
}

// awaitStreamResizes waits until the stream sent at least want resize messages.
func awaitStreamResizes(t *testing.T, stream *sessionTestStream, want int) []protocol.Resize {
	t.Helper()
	require.Eventually(t, func() bool { return len(streamResizes(stream)) >= want }, 5*time.Second, time.Millisecond,
		"the attachment stream never sent %d resize messages", want)
	return streamResizes(stream)
}

// TestSupervisorResizeBurstCoalescesAndRenderObservesLatestGeometry holds the
// supervisor's state lock across a whole resize burst, so the collector records
// every geometry while the repaint consumer cannot observe a partially applied
// burst. The burst must coalesce into at most one pending invalidation and the
// repaint that follows must sample the latest Terminal.Geometry: no render may
// observe an intermediate size.
func TestSupervisorResizeBurstCoalescesAndRenderObservesLatestGeometry(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startResizeHarness(t, resizeHarnessOptions{picker: picker})
	require.Eventually(t, func() bool { return harness.sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)
	harness.recorder.drain()
	baseline := harness.recorder.count()

	burst := []domain.Geometry{
		{Size: domain.Size{Cols: 90, Rows: 30}},
		{Size: domain.Size{Cols: 100, Rows: 35}},
		{Size: domain.Size{Cols: 120, Rows: 40}},
	}
	var pending int
	func() {
		harness.sup.mu.Lock()
		defer harness.sup.mu.Unlock()
		for _, geometry := range burst {
			harness.terminal.resize(geometry)
		}
		require.Eventually(t, func() bool {
			return harness.sup.attachments.geometryProgress() == uint64(len(burst))
		}, 5*time.Second, time.Millisecond, "the collector records every geometry of the burst")
		pending = harness.sup.attachments.pendingPresentationSignals()
	}()
	require.Equal(t, 1, pending,
		"a resize burst coalesces into exactly one pending presentation invalidation")

	latest := burst[len(burst)-1]
	require.Eventually(t, func() bool { return harness.recorder.count() > baseline }, 5*time.Second, time.Millisecond,
		"the coalesced burst produces a repaint")
	for _, sample := range harness.recorder.samples()[baseline:] {
		require.Equal(t, PresentPicker, sample.state.Presentation)
		require.Equal(t, latest.Size, sample.geometry.Size,
			"every repaint after the burst observes the latest Terminal.Geometry")
	}
	require.Equal(t, latest.Size, harness.recorder.last().geometry.Size)
}

// TestSupervisorIdlePickerRepaintsOnResizeWithoutInputOrPublication proves the
// core idle behavior: a resize wakes the established-connection wait and
// repaints the picker even though no terminal input arrived and no broker
// snapshot was published.
func TestSupervisorIdlePickerRepaintsOnResizeWithoutInputOrPublication(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startResizeHarness(t, resizeHarnessOptions{picker: picker})
	require.Eventually(t, func() bool { return harness.sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)

	revision := harness.service.hub.current().Revision
	harness.recorder.drain()

	resized := domain.Geometry{Size: domain.Size{Cols: 132, Rows: 43}}
	harness.terminal.resize(resized)

	sample := awaitResizeRender(t, harness.recorder, func(sample resizeRenderSample) bool {
		return sample.state.Presentation == PresentPicker && sample.state.Connectivity == ConnectivityReady
	})
	require.Equal(t, State{Presentation: PresentPicker, Connectivity: ConnectivityReady, Generation: 1}, sample.state,
		"a resize repaint reports the untouched ready state")
	require.Equal(t, resized.Size, sample.geometry.Size, "the render samples the latest terminal geometry")
	require.Equal(t, revision, harness.service.hub.current().Revision, "a resize repaint must not need a broker publication")
	require.Zero(t, picker.consumedCount(), "a resize repaint must not consume terminal input")
	require.Zero(t, harness.sup.attachments.pendingPresentationSignals(), "the coalesced invalidation was consumed")
}

// TestSupervisorResizeRepaintsConnectingRetryAndNonRetryableWaiting proves the
// resize invalidation reaches every pre-attachment waiting state the supervisor
// parks in: an in-flight broker connect, the retry cadence, and the
// non-retryable picker wait.
func TestSupervisorResizeRepaintsConnectingRetryAndNonRetryableWaiting(t *testing.T) {
	unavailable := ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker down"}
	incompatible := ports.BrokerError{Code: ports.BrokerErrorIncompatible, Text: "protocol"}

	tests := []struct {
		name string
		// start parks one supervisor on the wait under test and returns the state
		// it must be presenting when the resize arrives.
		start func(t *testing.T) (*resizeSupervisorHarness, State)
	}{
		{
			name: "broker connect in flight",
			start: func(t *testing.T) (*resizeSupervisorHarness, State) {
				harness := startResizeHarness(t, resizeHarnessOptions{
					connect: func(ctx context.Context, _ int) (ports.BrokerService, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					},
				})
				require.Equal(t, 1, harness.connector.awaitStart(t))
				return harness, State{Presentation: PresentPicker, Connectivity: ConnectivityConnectingBroker, Generation: 1}
			},
		},
		{
			name: "retry cadence wait",
			start: func(t *testing.T) (*resizeSupervisorHarness, State) {
				harness := startResizeHarness(t, resizeHarnessOptions{
					connect: func(context.Context, int) (ports.BrokerService, error) { return nil, unavailable },
				})
				harness.clock.awaitTimer(t)
				// The cadence exists because the first attempt already failed, so its
				// connector-start notification is still queued. Consume it now; the
				// retry assertion below must observe the next attempt's notification,
				// not this initial one.
				require.Equal(t, 1, harness.connector.awaitStart(t))
				return harness, State{Presentation: PresentPicker, Connectivity: ConnectivityRetryWait, Attempt: 1, Generation: 1, Err: unavailable}
			},
		},
		{
			name: "non-retryable picker wait",
			start: func(t *testing.T) (*resizeSupervisorHarness, State) {
				harness := startResizeHarness(t, resizeHarnessOptions{
					connect: func(context.Context, int) (ports.BrokerService, error) { return nil, incompatible },
				})
				require.Eventually(t, func() bool {
					state := harness.sup.State()
					return state.Connectivity == ConnectivityDisconnected && state.Err != nil
				}, 5*time.Second, time.Millisecond)
				return harness, State{Presentation: PresentPicker, Connectivity: ConnectivityDisconnected, Generation: 1, Err: incompatible}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness, want := tt.start(t)
			require.Equal(t, want, harness.sup.State(), "the supervisor is parked exactly on the wait under test")
			harness.recorder.drain()

			var retryTimer *supervisorTestTimer
			if tt.name == "retry cadence wait" {
				retryTimer = harness.clock.snapshotTimers()[0]
				require.Equal(t, 1, harness.clock.timerCount())
				require.Equal(t, 1, harness.clock.liveTimers())
			}

			resized := domain.Geometry{Size: domain.Size{Cols: 111, Rows: 41}}
			harness.terminal.resize(resized)

			sample := awaitResizeRender(t, harness.recorder, func(sample resizeRenderSample) bool {
				return sample.state.Presentation == PresentPicker
			})
			require.Equal(t, want, sample.state, "a resize repaint reports the parked state unchanged")
			require.Equal(t, resized.Size, sample.geometry.Size, "the render samples the latest terminal geometry")
			require.Zero(t, harness.sup.attachments.pendingPresentationSignals(), "the coalesced invalidation was consumed")
			require.Equal(t, want.Connectivity, harness.sup.State().Connectivity, "a repaint never advances connectivity")
			if retryTimer != nil {
				require.Equal(t, 1, harness.clock.timerCount(), "resize must not replace the retry timer")
				require.Equal(t, 1, harness.clock.liveTimers(), "the original retry timer remains live")
				retryTimer.fire()
				require.Equal(t, 2, harness.connector.awaitStart(t), "the original timer advances the next attempt")
			}
		})
	}
}

// TestAttachmentHostResizeBurstCoalescesAndKeepsGeometryWakeupsIndependent pins
// the collector itself: a burst of valid resizes is coalesced into exactly one
// buffered presentation invalidation while the attachment geometry sequence
// still advances to the latest geometry, and draining the presentation signal
// cannot steal the attachment's own geometry wakeup.
func TestAttachmentHostResizeBurstCoalescesAndKeepsGeometryWakeupsIndependent(t *testing.T) {
	resizes := make(chan domain.Geometry, 8)
	host := newAttachmentHost(attachmentHostConfig{})
	host.resizes = resizes
	host.ownerBoundary = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host.startGeometry(ctx)
	defer host.stopGeometry()

	fg := host.newForeground(AttachmentToken{Generation: 1, Attempt: 1}, newSessionTestStream())
	require.True(t, host.authority.grant(fg))

	burst := []domain.Geometry{
		{Size: domain.Size{Cols: 90, Rows: 30}},
		{Size: domain.Size{Cols: 100, Rows: 35}},
		{Size: domain.Size{Cols: 120, Rows: 40}},
	}
	for _, geometry := range burst {
		resizes <- geometry
	}
	require.Eventually(t, func() bool {
		return host.geometryProgress() == uint64(len(burst)) && host.pendingPresentationSignals() == 1
	}, 5*time.Second, time.Millisecond, "a resize burst must coalesce into one presentation invalidation")
	require.Equal(t, 1, host.pendingGeometrySignals(), "the attachment geometry wakeup coalesces independently")

	latest, ok := host.latestGeometry()
	require.True(t, ok)
	require.Equal(t, burst[len(burst)-1].Size, latest.Size, "the latest geometry wins the coalesced burst")

	// Draining the presentation invalidation must not touch the attachment's
	// geometry wakeup: the foreground still receives the latest geometry.
	<-host.presentationUpdate
	geometry, ok := fg.Resize(context.Background())
	require.True(t, ok)
	require.Equal(t, burst[len(burst)-1].Size, geometry.Size)

	next := domain.Geometry{Size: domain.Size{Cols: 132, Rows: 43}}
	resizes <- next
	geometry, ok = fg.Resize(context.Background())
	require.True(t, ok)
	require.Equal(t, next.Size, geometry.Size)
	require.Equal(t, 1, host.pendingPresentationSignals(), "every resize still raises a coalesced invalidation")
	fg.finish()
}

// TestSupervisorResizeDuringAttachmentSettlementPreservesSessionOutput pins the
// safe boundary this slice was narrowed to. Once Begin has admitted the
// foreground it owns the shared terminal writer before MarkAttached and the
// initial publication, so a resize invalidation that lands in the settlement
// wait must not be consumed there: the composition's renderer would write picker
// bytes over the session output the foreground is writing. The invalidation
// stays coalesced and buffered through connecting and attached, and is consumed
// with a repaint at the latest geometry only once the attachment returns to the
// picker.
func TestSupervisorResizeDuringAttachmentSettlementPreservesSessionOutput(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startResizeHarness(t, resizeHarnessOptions{picker: picker})
	registry := &resizeStreamRegistry{}
	harness.service.setOpenStream(registry.open)

	picker.commit(sessionTestRequest(true))
	stream := registry.stream(t, 0)
	require.Eventually(t, func() bool {
		return harness.sup.State().Presentation == PresentConnecting
	}, 5*time.Second, time.Millisecond, "attachment settlement exposes connecting presentation")
	harness.recorder.drain()

	resized := domain.Geometry{Size: domain.Size{Cols: 109, Rows: 39}}
	harness.terminal.resize(resized)
	require.Eventually(t, func() bool { return harness.sup.attachments.pendingPresentationSignals() == 1 },
		5*time.Second, time.Millisecond, "the collector still records the resize while connecting")

	// The admitted foreground commits its initial publication through the same
	// terminal. The supervisor must not have painted over it, and the resize must
	// not have driven a connecting repaint at the new geometry.
	deliverReadyStream(t, stream)
	awaitAttachedState(t, harness.sup)
	// The attached worker's palette query draws nothing; strip it.
	written := strings.ReplaceAll(harness.terminal.written(), paletteColorBatch, "")
	at := strings.Index(written, "\x1b[Hready")
	require.GreaterOrEqual(t, at, 0, "the admitted foreground committed its initial publication")
	require.Equal(t, "\x1b[Hready", written[at:],
		"a resize during settlement must not append picker bytes over session output")
	for _, sample := range harness.recorder.samples() {
		require.False(t, sample.state.Presentation == PresentConnecting && sample.geometry.Size == resized.Size,
			"a resize during settlement must not invoke the picker renderer for the connecting presentation")
	}
	require.Equal(t, 1, harness.sup.attachments.pendingPresentationSignals(),
		"the invalidation stays buffered through connecting and attached")

	// Back at the picker the preserved invalidation is consumed and repaints at
	// the latest geometry. The transition back to the picker renders once; the
	// consumed invalidation renders once more, so exactly two picker renders at
	// the resized geometry follow the attachment. A run that never emitted the
	// invalidation cannot reach this point, and a run that consumed it without a
	// repaint leaves only one.
	stream.deliver(protocol.Detached{})
	awaitPickerState(t, harness.sup)
	pickerAtResize := func(sample resizeRenderSample) bool {
		return sample.state.Presentation == PresentPicker && sample.geometry.Size == resized.Size
	}
	awaitResizeRender(t, harness.recorder, pickerAtResize)
	awaitResizeRender(t, harness.recorder, pickerAtResize)
	require.Eventually(t, func() bool { return harness.sup.attachments.pendingPresentationSignals() == 0 },
		5*time.Second, time.Millisecond, "the picker wait consumes the preserved invalidation")
}

// TestSupervisorResizeConnectingBurstStaysCoalescedAndBuffered proves the
// invalidation a settlement wait must not consume is still coalesced: a burst of
// resizes while the connecting presentation holds an admitted foreground leaves
// exactly one pending signal, and the whole burst is then consumed by the picker
// wait after the attachment ends without any intermediate repaint.
func TestSupervisorResizeConnectingBurstStaysCoalescedAndBuffered(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startResizeHarness(t, resizeHarnessOptions{picker: picker})
	registry := &resizeStreamRegistry{}
	harness.service.setOpenStream(registry.open)

	picker.commit(sessionTestRequest(true))
	stream := registry.stream(t, 0)
	require.Eventually(t, func() bool {
		return harness.sup.State().Presentation == PresentConnecting
	}, 5*time.Second, time.Millisecond)

	burst := []domain.Geometry{
		{Size: domain.Size{Cols: 90, Rows: 30}},
		{Size: domain.Size{Cols: 100, Rows: 35}},
		{Size: domain.Size{Cols: 120, Rows: 40}},
	}
	for _, geometry := range burst {
		harness.terminal.resize(geometry)
	}
	require.Eventually(t, func() bool {
		return harness.sup.attachments.geometryProgress() == uint64(len(burst))
	}, 5*time.Second, time.Millisecond, "the collector records every geometry of the burst")
	require.Eventually(t, func() bool { return harness.sup.attachments.pendingPresentationSignals() == 1 },
		5*time.Second, time.Millisecond, "a connecting resize burst coalesces into one buffered invalidation")

	deliverReadyStream(t, stream)
	awaitAttachedState(t, harness.sup)
	require.Equal(t, 1, harness.sup.attachments.pendingPresentationSignals(),
		"the coalesced burst is preserved through connecting and attached")

	stream.deliver(protocol.Detached{})
	awaitPickerState(t, harness.sup)
	pickerAtLatest := func(sample resizeRenderSample) bool {
		return sample.state.Presentation == PresentPicker && sample.geometry.Size == burst[len(burst)-1].Size
	}
	awaitResizeRender(t, harness.recorder, pickerAtLatest)
	awaitResizeRender(t, harness.recorder, pickerAtLatest)
	require.Eventually(t, func() bool { return harness.sup.attachments.pendingPresentationSignals() == 0 },
		5*time.Second, time.Millisecond, "the coalesced burst is consumed by the picker wait, once")
}

// awaitAttachedSettlement waits until the supervisor's live attachment has
// claimed the attached transition and the settlement wait has drained every
// earlier invalidation. It is the unambiguous boundary the attached-resize
// assertion needs: once the current foreground reports attached, the
// settlement wait has disarmed its presentation arm, so a resize pushed after
// this point must stay buffered for the picker instead of being consumed.
func awaitAttachedSettlement(t *testing.T, harness *resizeSupervisorHarness) {
	t.Helper()
	require.Eventually(t, func() bool {
		harness.sup.mu.Lock()
		attachedPresentation := harness.sup.state.Presentation == PresentAttached
		harness.sup.mu.Unlock()
		foreground := harness.sup.attachments.authority.foreground()
		return attachedPresentation && foreground != nil && foreground.Attached()
	}, 5*time.Second, time.Millisecond, "the attachment never claimed the attached boundary")
	require.Eventually(t, func() bool { return harness.sup.attachments.pendingPresentationSignals() == 0 },
		5*time.Second, time.Millisecond, "a pre-resize invalidation was not drained before the boundary")
}

// TestSupervisorAttachedResizeKeepsGeometryWakeupAndPreservesRepaint proves an
// attached foreground is not painted over and its geometry wakeup is not
// stolen: the live attachment receives the typed resize while the coalesced
// presentation invalidation stays buffered for the picker, and is then consumed
// by the supervisor's next picker wait.
func TestSupervisorAttachedResizeKeepsGeometryWakeupAndPreservesRepaint(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startResizeHarness(t, resizeHarnessOptions{picker: picker})
	registry := &resizeStreamRegistry{}
	harness.service.setOpenStream(registry.open)

	picker.commit(sessionTestRequest(true))
	stream := registry.stream(t, 0)
	deliverReadyStream(t, stream)
	// Settle the attached boundary before the resize: the presentation is
	// attached and the live foreground has claimed attached, and every earlier
	// invalidation is drained, so the resize below lands unambiguously on an
	// attached settlement wait.
	awaitAttachedSettlement(t, harness)
	harness.recorder.drain()
	baseline := harness.recorder.count()

	resized := domain.Geometry{Size: domain.Size{Cols: 100, Rows: 40}}
	harness.terminal.resize(resized)

	resizes := awaitStreamResizes(t, stream, 1)
	require.Equal(t, resized.Size, resizes[0].Size, "the live attachment still receives its geometry wakeup")
	// The attachment geometry wakeup and the presentation invalidation are
	// pushed by the collector independently, and the worker can observe the new
	// geometry through the recorded sequence before the invalidation is pushed.
	// Wait for the coalesced token to be buffered; a supervisor that wrongly
	// consumed it while attached could never make this count return to one, so
	// this stays a behavioral check rather than a sleep.
	require.Eventually(t, func() bool { return harness.sup.attachments.pendingPresentationSignals() == 1 },
		5*time.Second, time.Millisecond, "the supervisor does not consume the invalidation while attached")
	require.Equal(t, baseline, harness.recorder.count(), "an attached presentation is never painted over")
	require.Equal(t, 1, harness.sup.attachments.pendingPresentationSignals(), "the invalidation stays buffered until the picker wait")

	stream.deliver(protocol.Detached{})
	awaitPickerState(t, harness.sup)
	require.Eventually(t, func() bool { return harness.sup.attachments.pendingPresentationSignals() == 0 }, 5*time.Second, time.Millisecond,
		"the preserved invalidation is consumed by the picker wait")
}

// TestSupervisorResizeAfterDetachReachesOnlyReplacementAttachment proves a
// resize is delivered to the valid replacement attachment token only. A resize
// that lands in the detach/reattach gap reaches no stream at all: it repaints the
// picker, the retired token's stream keeps exactly its own earlier resize, and
// the replacement inherits the gap geometry and receives only its own later
// resize under its own (generation, attempt) identity.
func TestSupervisorResizeAfterDetachReachesOnlyReplacementAttachment(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startResizeHarness(t, resizeHarnessOptions{picker: picker})
	registry := &resizeStreamRegistry{}
	harness.service.setOpenStream(registry.open)

	picker.commit(sessionTestRequest(true))
	first := registry.stream(t, 0)
	deliverReadyStream(t, first)
	awaitAttachedState(t, harness.sup)
	require.Equal(t, AttachmentToken{Generation: 1, Attempt: 1}, harness.sup.attachments.authority.currentToken())

	firstResize := domain.Geometry{Size: domain.Size{Cols: 101, Rows: 41}}
	harness.terminal.resize(firstResize)
	firstResizes := awaitStreamResizes(t, first, 1)
	require.Equal(t, firstResize.Size, firstResizes[0].Size)

	// The attachment detaches: its generation is retired before any replacement
	// exists.
	first.deliver(protocol.Detached{})
	awaitPickerState(t, harness.sup)
	require.True(t, harness.sup.attachments.authority.currentToken().IsZero(), "the retired token no longer owns the foreground")

	// A resize in the detach/reattach gap: no attachment owns the foreground, so
	// the retired stream must not receive it, only the picker repaints, and the
	// signal is consumed by the picker wait.
	harness.recorder.drain()
	gapResize := domain.Geometry{Size: domain.Size{Cols: 110, Rows: 45}}
	harness.terminal.resize(gapResize)
	gapSample := awaitResizeRender(t, harness.recorder, func(sample resizeRenderSample) bool {
		return sample.state.Presentation == PresentPicker && sample.geometry.Size == gapResize.Size
	})
	require.Equal(t, gapResize.Size, gapSample.geometry.Size, "a gap resize repaints the picker")
	require.Eventually(t, func() bool { return harness.sup.attachments.pendingPresentationSignals() == 0 }, 5*time.Second, time.Millisecond,
		"the gap invalidation is consumed by the picker wait")
	require.Len(t, streamResizes(first), 1, "the retired attachment token must not receive a gap resize")

	// A replacement attachment claims the next attempt identity. It carries the
	// latest geometry in its own session state, which is the gap resize, so the
	// only stream that ever observes it is the valid replacement.
	picker.commit(sessionTestRequest(true))
	second := registry.stream(t, 1)
	deliverReadyStream(t, second)
	awaitAttachedState(t, harness.sup)
	require.Equal(t, AttachmentToken{Generation: 1, Attempt: 2}, harness.sup.attachments.authority.currentToken())
	inherited := awaitStreamResizes(t, second, 1)
	require.Equal(t, gapResize.Size, inherited[0].Size, "the replacement inherits the latest geometry")

	// The replacement's own resize reaches it and nothing else.
	secondResize := domain.Geometry{Size: domain.Size{Cols: 120, Rows: 50}}
	harness.terminal.resize(secondResize)
	replacement := awaitStreamResizes(t, second, 2)
	require.Equal(t, secondResize.Size, replacement[1].Size, "only the replacement token receives its resize")
	require.Len(t, streamResizes(first), 1, "a retired attachment token must never receive a replacement's resize")
}

// TestResizeCollectorAndSupervisorJoinOnClosedEventsAndCancellation proves the
// collector and the supervisor's owned waits end cleanly when ResizeEvents is
// already closed and when the process context is cancelled: no path blocks on a
// closed or abandoned channel.
func TestResizeCollectorAndSupervisorJoinOnClosedEventsAndCancellation(t *testing.T) {
	t.Run("collector joins a closed ResizeEvents channel", func(t *testing.T) {
		closed := make(chan domain.Geometry)
		close(closed)
		host := newAttachmentHost(attachmentHostConfig{})
		host.resizes = closed
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		host.startGeometry(ctx)
		select {
		case <-host.geometryDone:
		case <-time.After(5 * time.Second):
			t.Fatal("the collector did not end on closed resize events")
		}
		host.stopGeometry()
		require.Nil(t, host.geometryDone)
		require.Nil(t, host.geometryCancel)

		// The host can be started again with a live channel and joined by
		// cancellation, even with a coalesced invalidation still pending.
		resizes := make(chan domain.Geometry, 2)
		host.resizes = resizes
		host.startGeometry(ctx)
		resizes <- domain.Geometry{Size: domain.Size{Cols: 90, Rows: 30}}
		resizes <- domain.Geometry{Size: domain.Size{Cols: 91, Rows: 31}}
		require.Eventually(t, func() bool { return host.pendingPresentationSignals() == 1 }, 5*time.Second, time.Millisecond)
		cancel()
		select {
		case <-host.geometryDone:
		case <-time.After(5 * time.Second):
			t.Fatal("the collector did not end on cancellation")
		}
		host.stopGeometry()
		require.Nil(t, host.geometryDone)
	})

	t.Run("supervisor run joins on closed ResizeEvents and cancellation", func(t *testing.T) {
		harness := startResizeHarness(t, resizeHarnessOptions{
			closeResizes: true,
			connect: func(context.Context, int) (ports.BrokerService, error) {
				return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "down"}
			},
		})
		require.Equal(t, 1, harness.connector.awaitStart(t))
		harness.cancel()
		require.ErrorIs(t, harness.waitRun(t), context.Canceled)
		_, _, restores := harness.terminal.counts()
		require.Equal(t, 1, restores, "raw mode is restored exactly once")
		require.Nil(t, harness.sup.attachments.geometryDone, "Run joined the geometry collector")
		require.Nil(t, harness.sup.attachments.geometryCancel)
	})
}

// TestSupervisorResizeInvalidationRendersOnlyPickerPresentations pins the
// presentation fence directly: only PresentPicker may be repainted by a resize,
// so the pre-attachment connecting transition, an attached foreground, and a
// terminating process are never painted over. It also proves the fence is
// behavioral, not just counted: with the session byte already in the terminal,
// a connecting repaint would change the renderer count and append picker bytes
// over the session output. A supervisor without a renderer is a no-op.
func TestSupervisorResizeInvalidationRendersOnlyPickerPresentations(t *testing.T) {
	terminal := newResizeTestTerminal(newAttachTestReader(), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
	recorder := newResizeRenderRecorder(terminal)
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: newSupervisorTestConnector(nil),
		Terminal:  terminal,
		Clock:     newSupervisorTestClock(),
		Render:    recorder.render,
	})

	tests := []struct {
		name         string
		presentation Presentation
		wantRender   bool
	}{
		{name: "picker is repainted", presentation: PresentPicker, wantRender: true},
		{name: "connecting is never painted over", presentation: PresentConnecting},
		{name: "attached is never painted over", presentation: PresentAttached},
		{name: "terminating is never repainted", presentation: PresentTerminating},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A session frame as an admitted foreground would have written it, so
			// any picker repaint for a refused presentation is observable.
			const session = "\x1b[Hsession output"
			terminal.setWritten(session)
			sup.mu.Lock()
			sup.state = State{Presentation: tt.presentation, Connectivity: ConnectivityReady}
			sup.mu.Unlock()
			recorder.drain()
			baseline := recorder.count()

			sup.renderResizeInvalidation()

			if tt.wantRender {
				sample := awaitResizeRender(t, recorder, func(resizeRenderSample) bool { return true })
				require.Equal(t, tt.presentation, sample.state.Presentation)
				return
			}
			require.Equal(t, baseline, recorder.count(), "presentation %s must not be repainted by a resize", tt.presentation)
			require.Equal(t, session, terminal.written(),
				"presentation %s must never overpaint session output", tt.presentation)
		})
	}

	// A supervisor with no renderer configured through the internal zero value
	// never panics, including on a nil receiver.
	(&Supervisor{}).renderResizeInvalidation()
	var nilSupervisor *Supervisor
	nilSupervisor.renderResizeInvalidation()
}

// TestSupervisorPresentationInvalidationNilHostIsSafe pins the nil-host
// contract of the presentation invalidation helper and of a wait that selects
// on it. A supervisor whose attachment host is absent (the zero value, or a run
// whose host was detached) must expose a nil signal and still park on its
// remaining signals instead of dereferencing a nil host.
func TestSupervisorPresentationInvalidationNilHostIsSafe(t *testing.T) {
	t.Run("helper returns nil without a host", func(t *testing.T) {
		var nilSupervisor *Supervisor
		require.Nil(t, nilSupervisor.presentationInvalidation(), "a nil receiver exposes no invalidation")

		zero := &Supervisor{}
		require.Nil(t, zero.presentationInvalidation(), "a zero-value supervisor exposes no invalidation")

		// A fully built supervisor exposes its host's signal; once the host is
		// gone the helper must fall back to nil rather than panic.
		terminal := newResizeTestTerminal(newAttachTestReader(), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
		sup := mustSupervisor(t, SupervisorConfig{
			Connector: newSupervisorTestConnector(nil),
			Terminal:  terminal,
			Clock:     newSupervisorTestClock(),
		})
		require.NotNil(t, sup.presentationInvalidation(), "a live host exposes its coalesced invalidation")
		sup.attachments = nil
		require.Nil(t, sup.presentationInvalidation(), "a detached host exposes no invalidation")
	})

	t.Run("a wait with a nil host still settles", func(t *testing.T) {
		terminal := newResizeTestTerminal(newAttachTestReader(), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
		clock := newSupervisorTestClock()
		sup := mustSupervisor(t, SupervisorConfig{
			Connector: newSupervisorTestConnector(nil),
			Terminal:  terminal,
			Clock:     clock,
			Jitter:    func() float64 { return 0 },
		})
		sup.attachments = nil
		require.Nil(t, sup.presentationInvalidation())
		// One transient failure makes the cadence non-zero, so waitBackoff
		// arms a real timer.
		sup.transition(supervisorEvent{kind: supervisorTransientFailure})

		// The reader never yields, so the EOF arm stays closed and the timer is
		// the only signal that can settle this wait.
		reader := newAttachTestReader()
		lifetime := startTerminalInputLifetime(reader, nil)
		t.Cleanup(func() {
			reader.close()
			lifetime.stop()
		})

		settled := make(chan struct{})
		var terminated bool
		var waitErr error
		go func() {
			defer close(settled)
			terminated, waitErr = sup.waitBackoff(context.Background(), lifetime)
		}()
		timer := clock.awaitTimer(t)
		timer.fire()
		select {
		case <-settled:
		case <-time.After(5 * time.Second):
			t.Fatal("a nil-host backoff wait never settled on its timer")
		}
		require.False(t, terminated)
		require.NoError(t, waitErr)
	})

	t.Run("a wait with a nil host settles on cancellation", func(t *testing.T) {
		terminal := newResizeTestTerminal(newAttachTestReader(), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
		clock := newSupervisorTestClock()
		sup := mustSupervisor(t, SupervisorConfig{
			Connector: newSupervisorTestConnector(nil),
			Terminal:  terminal,
			Clock:     clock,
			Jitter:    func() float64 { return 0 },
		})
		sup.attachments = nil
		sup.transition(supervisorEvent{kind: supervisorTransientFailure})

		reader := newAttachTestReader()
		lifetime := startTerminalInputLifetime(reader, nil)
		t.Cleanup(func() {
			reader.close()
			lifetime.stop()
		})

		ctx, cancel := context.WithCancel(context.Background())
		settled := make(chan struct{})
		var terminated bool
		var waitErr error
		go func() {
			defer close(settled)
			terminated, waitErr = sup.waitBackoff(ctx, lifetime)
		}()
		clock.awaitTimer(t)
		cancel()
		select {
		case <-settled:
		case <-time.After(5 * time.Second):
			t.Fatal("a nil-host backoff wait never settled on cancellation")
		}
		require.True(t, terminated)
		require.ErrorIs(t, waitErr, context.Canceled)
	})
}
