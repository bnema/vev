package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Committed attachment supervisor tests (Plan 001 P5.3b). They drive the real
// Run loop over the in-package broker fakes, with a fake clock, no sleeps, and
// no production wiring.

// attachTestTerminal is the supervisor-owned terminal double: it records output,
// flushes, raw entrances/restores, and UI publication boundaries so a test can
// prove exactly what a committed attachment wrote.
type attachTestTerminal struct {
	in io.Reader

	mu             sync.Mutex
	out            bytes.Buffer
	flushes        int
	restores       int
	publications   []ports.UIContext
	successes      []bool
	publishEntered chan struct{}
	publishRelease <-chan struct{}
}

func (t *attachTestTerminal) EnterRaw() (func() error, error) {
	return func() error {
		t.mu.Lock()
		t.restores++
		t.mu.Unlock()
		return nil
	}, nil
}

func (t *attachTestTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil
}

func (t *attachTestTerminal) ResizeEvents() <-chan domain.Geometry { return nil }

func (t *attachTestTerminal) In() io.Reader { return t.in }

func (t *attachTestTerminal) Out() io.Writer { return &attachTestWriter{term: t} }

func (t *attachTestTerminal) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.flushes++
	return nil
}

func (t *attachTestTerminal) BeginOutput(ports.UIContext) {}

func (t *attachTestTerminal) EndOutput(success bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.successes = append(t.successes, success)
}

func (t *attachTestTerminal) PublishContext(uiContext ports.UIContext) error {
	if t.publishEntered != nil {
		select {
		case t.publishEntered <- struct{}{}:
		default:
		}
	}
	if t.publishRelease != nil {
		<-t.publishRelease
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.publications = append(t.publications, uiContext)
	return nil
}

func (t *attachTestTerminal) written() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.out.String()
}

func (t *attachTestTerminal) counts() (writes int, flushes int, restores int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.out.Bytes()), t.flushes, t.restores
}

type attachTestWriter struct{ term *attachTestTerminal }

func (w *attachTestWriter) Write(data []byte) (int, error) {
	w.term.mu.Lock()
	defer w.term.mu.Unlock()
	return w.term.out.Write(data)
}

// attachTestReader blocks until a chunk is pushed, then returns EOF once
// closed. It is the supervisor's single terminal reader.
type attachTestReader struct {
	chunks chan []byte
}

func newAttachTestReader() *attachTestReader {
	return &attachTestReader{chunks: make(chan []byte, 8)}
}

func (r *attachTestReader) Read(buffer []byte) (int, error) {
	chunk, ok := <-r.chunks
	if !ok {
		return 0, io.EOF
	}
	return copy(buffer, chunk), nil
}

func (r *attachTestReader) push(data []byte) { r.chunks <- data }

func (r *attachTestReader) close() { close(r.chunks) }

// attachTestPicker is the supervisor's picker seam double. It records input
// ownership transitions, resolves exactly the committed key to one canned
// request, and coalesces commit wakeups exactly like the real controller.
type attachTestPicker struct {
	mu        sync.Mutex
	opsReady  chan struct{}
	ownsInput bool
	consumed  int
	op        pickerOp
	key       string
	request   ports.BrokerOpenStreamRequest
	resolved  int
}

func newAttachTestPicker() *attachTestPicker {
	return &attachTestPicker{opsReady: make(chan struct{}, 1), ownsInput: true}
}

func (p *attachTestPicker) ApplySnapshot(ports.BrokerSnapshot) {}

func (p *attachTestPicker) ConsumeTerminalRead([]byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.ownsInput {
		return false
	}
	p.consumed++
	return true
}

func (p *attachTestPicker) TakeOp() (pickerOp, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	op, key := p.op, p.key
	p.op, p.key = pickerOp{}, ""
	return op, key
}

func (p *attachTestPicker) ResolveInitial(navigation InitialNavigation, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	if navigation != InitialNavigationCreateEphemeral {
		return ports.BrokerOpenStreamRequest{}, errors.New("unexpected initial navigation")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resolved++
	request := p.request
	request.Admission = ports.BrokerAdmissionCreateEphemeral
	request.Target = protocol.ExactSessionTarget{}
	request.Local = true
	request.Connection = base.Connection
	request.Stream = base.Stream
	return request, nil
}

func (p *attachTestPicker) ResolveKey(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resolved++
	request := p.request
	request.Connection = base.Connection
	request.Stream = base.Stream
	return request, nil
}

func (p *attachTestPicker) SetOwnsInput(owns bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ownsInput = owns
}

func (p *attachTestPicker) OpsReady() <-chan struct{} { return p.opsReady }

func (p *attachTestPicker) commit(request ports.BrokerOpenStreamRequest) {
	p.mu.Lock()
	p.op.commit = true
	p.key = "row"
	p.request = request
	p.mu.Unlock()
	select {
	case p.opsReady <- struct{}{}:
	default:
	}
}

func (p *attachTestPicker) owns() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ownsInput
}

func (p *attachTestPicker) consumedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.consumed
}

func (p *attachTestPicker) resolveCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.resolved
}

// attachTestHarness is one running supervisor with its scripted broker.
type attachTestHarness struct {
	sup      *Supervisor
	service  *supervisorTestService
	clock    *supervisorTestClock
	terminal *attachTestTerminal
	reader   *attachTestReader
	picker   *attachTestPicker
	cancel   context.CancelFunc
	runDone  chan struct{}

	runMu  sync.Mutex
	runErr error
}

// startAttachHarness runs one supervisor over a scripted picker and service.
// The caller installs service.setOpenStream before committing a selection.
func startAttachHarness(t *testing.T, picker pickerHost) *attachTestHarness {
	return startAttachHarnessConfig(t, picker, nil)
}

func startAttachHarnessConfig(t *testing.T, picker pickerHost, configure func(*SupervisorConfig)) *attachTestHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clock := newSupervisorTestClock()
	reader := newAttachTestReader()
	terminal := &attachTestTerminal{in: reader}
	service := newSupervisorTestService(ports.BrokerConnectionID{1})
	service.publish(3, 1)

	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		return service, nil
	})
	cfg := SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
		Picker:    picker,
	}
	if configure != nil {
		configure(&cfg)
	}
	sup := mustSupervisor(t, cfg)

	harness := &attachTestHarness{sup: sup, service: service, clock: clock, terminal: terminal, reader: reader, cancel: cancel, runDone: make(chan struct{})}
	go func() {
		err := sup.Run(ctx)
		harness.runMu.Lock()
		harness.runErr = err
		harness.runMu.Unlock()
		close(harness.runDone)
	}()
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)

	t.Cleanup(func() {
		cancel()
		select {
		case <-harness.runDone:
		case <-time.After(5 * time.Second):
			t.Error("the supervisor did not stop")
		}
	})
	return harness
}

// waitRun joins the supervisor and returns its terminal error.
func (h *attachTestHarness) waitRun(t *testing.T) error {
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

// snapshotTimers returns the armed timers for assertions without racing the
// clock's own bookkeeping.
func (c *supervisorTestClock) snapshotTimers() []*supervisorTestTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*supervisorTestTimer(nil), c.timers...)
}

// awaitAttachedState waits until the supervisor presents the attached state.
func awaitAttachedState(t *testing.T, sup *Supervisor) {
	t.Helper()
	require.Eventually(t, func() bool { return sup.State().Presentation == PresentAttached }, 5*time.Second, time.Millisecond)
}

// awaitPickerState waits until the supervisor is back in the picker.
func awaitPickerState(t *testing.T, sup *Supervisor) {
	t.Helper()
	require.Eventually(t, func() bool { return sup.State().Presentation == PresentPicker }, 5*time.Second, time.Millisecond)
}

// awaitStreamHello waits until the admitted stream has sent its first client
// message, polling the guarded stream pointer rather than a stale snapshot.
func awaitStreamHello(t *testing.T, mu *sync.Mutex, stream **sessionTestStream) {
	t.Helper()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return *stream != nil && len((*stream).messages()) >= 1
	}, 5*time.Second, time.Millisecond)
}

// deliverReadyStream scripts Welcome plus one committed initial publication.
func deliverReadyStream(t *testing.T, stream *sessionTestStream) {
	t.Helper()
	stream.deliver(protocol.Welcome{SessionName: "alpha"})
	stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
}

// TestSupervisorReducerAttachmentPresentation pins the presentation
// transitions of one attachment: connecting until the committed publication,
// attached, then back to the picker without disturbing connectivity.
func TestSupervisorReducerAttachmentPresentation(t *testing.T) {
	ready := State{Presentation: PresentPicker, Connectivity: ConnectivityReady, Generation: 2}
	connecting := reduceSupervisor(ready, supervisorEvent{kind: supervisorAttachBegin})
	require.Equal(t, PresentConnecting, connecting.Presentation)
	require.Equal(t, ConnectivityReady, connecting.Connectivity)
	require.Zero(t, connecting.Attempt)

	attached := reduceSupervisor(connecting, supervisorEvent{kind: supervisorAttached})
	require.Equal(t, PresentAttached, attached.Presentation)
	require.Equal(t, ConnectivityReady, attached.Connectivity)

	returned := reduceSupervisor(attached, supervisorEvent{kind: supervisorAttachEnded, err: errAttachmentDeadline})
	require.Equal(t, PresentPicker, returned.Presentation)
	require.Equal(t, ConnectivityReady, returned.Connectivity)
	require.ErrorIs(t, returned.Err, errAttachmentDeadline)
}

// TestSupervisorAttachmentLocalRemoteParity proves the same supervisor path
// opens the exact resolved request for a local and a remote selection, with no
// dialer choice in the client.
func TestSupervisorAttachmentLocalRemoteParity(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "remote"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			picker := newAttachTestPicker()
			harness := startAttachHarness(t, picker)
			var (
				streamMu sync.Mutex
				stream   *sessionTestStream
			)
			harness.service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				admitted := newSessionTestStream()
				streamMu.Lock()
				stream = admitted
				streamMu.Unlock()
				return admitted, nil
			})
			picker.commit(sessionTestRequest(local))

			require.Eventually(t, func() bool { return len(harness.service.openedRequests()) == 1 }, 5*time.Second, time.Millisecond)
			opened := harness.service.openedRequests()[0]
			require.Equal(t, local, opened.Local)
			require.Equal(t, ports.BrokerStreamAttachment, opened.Purpose)
			require.Equal(t, ports.BrokerAdmissionExact, opened.Admission)
			require.Equal(t, "alpha", opened.Target.SessionName)
			require.Equal(t, ports.BrokerConnectionID{1}, opened.Connection)
			require.Equal(t, ports.BrokerStreamID(1), opened.Stream)
			if !local {
				require.Equal(t, "user@example", opened.Endpoint)
			}

			// The stream now runs the typed protocol and commits the initial
			// publication before attachment is reported.
			streamMu.Lock()
			admitted := stream
			streamMu.Unlock()
			require.NotNil(t, admitted)
			require.Eventually(t, func() bool { return len(admitted.messages()) >= 1 }, 5*time.Second, time.Millisecond)
			require.Equal(t, PresentConnecting, harness.sup.State().Presentation, "connecting until the publication commits")
			require.False(t, picker.owns(), "the picker released input for the attachment")
			deliverReadyStream(t, admitted)
			awaitAttachedState(t, harness.sup)
			require.Equal(t, "\x1b[Hready", harness.terminal.written())
			require.Equal(t, []bool{true}, harness.terminal.successes, "the frame committed through the UI transaction")
		})
	}
}

// TestSupervisorAttachmentDeadlineElapsedDuringOpenFailsWithoutRestart proves a
// deadline that passed during stream open fails the attachment immediately
// instead of starting a second budget.
func TestSupervisorAttachmentDeadlineElapsedDuringOpenFailsWithoutRestart(t *testing.T) {
	picker := newAttachTestPicker()
	var (
		streamMu sync.Mutex
		stream   *sessionTestStream
	)
	harness := startAttachHarness(t, picker)
	clock := harness.clock
	harness.service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		// The accepted absolute deadline elapsed before this client saw it.
		admitted := &sessionTestStreamWithDeadline{
			sessionTestStream: newSessionTestStream(),
			deadline:          clock.Now().Add(-time.Second),
		}
		streamMu.Lock()
		stream = admitted.sessionTestStream
		streamMu.Unlock()
		return admitted, nil
	})
	picker.commit(sessionTestRequest(true))

	awaitPickerState(t, harness.sup)
	require.Eventually(t, func() bool { return harness.sup.State().Err != nil }, 5*time.Second, time.Millisecond)
	var typed ports.BrokerError
	require.ErrorAs(t, harness.sup.State().Err, &typed)
	require.Equal(t, ports.BrokerErrorTimeout, typed.Code)

	streamMu.Lock()
	require.NotNil(t, stream)
	require.True(t, stream.closedNow(), "an expired stream is closed")
	streamMu.Unlock()

	// Exactly one deadline timer was ever armed, and it was stopped: the
	// elapsed accepted deadline never restarted a fresh budget.
	require.Equal(t, 1, harness.clock.timerCount(), "the elapsed deadline must not restart a budget")
	require.True(t, harness.clock.allStopped(), "the pre-open budget is retired")
}

// TestSupervisorAttachmentDeadlineNotExtendedAcrossStages proves one absolute
// deadline spans the whole attachment: adopting the accepted deadline arms
// exactly its remaining time, and crossing Welcome never restarts it.
func TestSupervisorAttachmentDeadlineNotExtendedAcrossStages(t *testing.T) {
	picker := newAttachTestPicker()
	var (
		streamMu sync.Mutex
		stream   *sessionTestStream
	)
	harness := startAttachHarness(t, picker)
	clock := harness.clock
	harness.service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := &sessionTestStreamWithDeadline{
			sessionTestStream: newSessionTestStream(),
			deadline:          clock.Now().Add(5 * time.Second),
		}
		streamMu.Lock()
		stream = admitted.sessionTestStream
		streamMu.Unlock()
		return admitted, nil
	})
	picker.commit(sessionTestRequest(true))

	awaitStreamHello(t, &streamMu, &stream)

	streamMu.Lock()
	admitted := stream
	streamMu.Unlock()

	// Wait until adoption armed the second timer.
	require.Eventually(t, func() bool { return harness.clock.timerCount() == 2 }, 5*time.Second, time.Millisecond)
	timers := harness.clock.snapshotTimers()
	require.Equal(t, protocol.HandshakeTimeout, timers[0].delay, "the pre-open budget bounds stream open")
	require.Equal(t, 5*time.Second, timers[1].delay, "the accepted deadline is adopted verbatim, never restarted")
	require.True(t, timers[0].stopped(), "the pre-open budget is replaced by the accepted deadline")
	require.False(t, timers[1].stopped())

	// Crossing Welcome does not arm another deadline.
	admitted.deliver(protocol.Welcome{SessionName: "alpha"})
	require.Never(t, func() bool { return harness.clock.timerCount() > 2 }, 20*time.Millisecond, 2*time.Millisecond, "no stage may restart the deadline")

	// Firing the one adopted deadline ends the attachment as a typed timeout
	// before publication, without claiming attachment.
	timers[1].fire()
	awaitPickerState(t, harness.sup)
	require.Eventually(t, func() bool { return harness.sup.State().Err != nil }, 5*time.Second, time.Millisecond)
	var typed ports.BrokerError
	require.ErrorAs(t, harness.sup.State().Err, &typed)
	require.Equal(t, ports.BrokerErrorTimeout, typed.Code)
	require.Empty(t, harness.terminal.publications, "a timed-out attachment never published")
}

// TestSupervisorAttachmentLaterAcceptedDeadlineNeverExtendsBudget proves the
// one absolute deadline is the earlier of the client's own pre-open budget and
// the admitted stream's accepted deadline. The accepted deadline is anchored
// before this client's attempt began, so it is normally the later of the two;
// folding it in verbatim would repay the open stage with a fresh budget and let
// the attachment outlive the deadline.
func TestSupervisorAttachmentLaterAcceptedDeadlineNeverExtendsBudget(t *testing.T) {
	picker := newAttachTestPicker()
	var (
		streamMu sync.Mutex
		stream   *sessionTestStream
	)
	harness := startAttachHarness(t, picker)
	clock := harness.clock
	harness.service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := &sessionTestStreamWithDeadline{
			sessionTestStream: newSessionTestStream(),
			deadline:          clock.Now().Add(4 * protocol.HandshakeTimeout),
		}
		streamMu.Lock()
		stream = admitted.sessionTestStream
		streamMu.Unlock()
		return admitted, nil
	})
	picker.commit(sessionTestRequest(true))

	awaitStreamHello(t, &streamMu, &stream)

	require.Eventually(t, func() bool { return harness.clock.timerCount() == 2 }, 5*time.Second, time.Millisecond)
	timers := harness.clock.snapshotTimers()
	require.Equal(t, protocol.HandshakeTimeout, timers[0].delay, "the pre-open budget bounds stream open")
	require.Equal(t, protocol.HandshakeTimeout, timers[1].delay,
		"a later accepted deadline never repays the open stage with a second budget")
	require.True(t, timers[0].stopped())
	require.False(t, timers[1].stopped())

	// The one deadline still ends the attachment as a typed timeout before
	// publication, and it does so no later than the client's own budget.
	timers[1].fire()
	awaitPickerState(t, harness.sup)
	require.Eventually(t, func() bool { return harness.sup.State().Err != nil }, 5*time.Second, time.Millisecond)
	var typed ports.BrokerError
	require.ErrorAs(t, harness.sup.State().Err, &typed)
	require.Equal(t, ports.BrokerErrorTimeout, typed.Code)
	require.Empty(t, harness.terminal.publications, "a timed-out attachment never published")
}

// TestSupervisorAttachmentPreOpenBudgetWithoutProvider proves a stream without
// the optional seam keeps the one pre-open budget: no second timer, no restart.
func TestSupervisorAttachmentPreOpenBudgetWithoutProvider(t *testing.T) {
	picker := newAttachTestPicker()
	var (
		streamMu sync.Mutex
		stream   *sessionTestStream
	)
	harness := startAttachHarness(t, picker)
	harness.service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := newSessionTestStream()
		streamMu.Lock()
		stream = admitted
		streamMu.Unlock()
		return admitted, nil
	})
	picker.commit(sessionTestRequest(true))

	awaitStreamHello(t, &streamMu, &stream)

	streamMu.Lock()
	admitted := stream
	streamMu.Unlock()
	require.Equal(t, 1, harness.clock.timerCount())
	timers := harness.clock.snapshotTimers()
	require.Equal(t, protocol.HandshakeTimeout, timers[0].delay)
	require.False(t, timers[0].stopped())

	timers[0].fire()
	awaitPickerState(t, harness.sup)
	require.Eventually(t, func() bool { return harness.sup.State().Err != nil }, 5*time.Second, time.Millisecond)
	var typed ports.BrokerError
	require.ErrorAs(t, harness.sup.State().Err, &typed)
	require.Equal(t, ports.BrokerErrorTimeout, typed.Code)
	require.True(t, admitted.closedNow(), "the expired stream is closed")
}

func TestSupervisorAttachmentDeadlineDisarmedAfterPublication(t *testing.T) {
	picker := newAttachTestPicker()
	var stream *sessionTestStream
	var streamMu sync.Mutex
	harness := startAttachHarness(t, picker)
	harness.service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := newSessionTestStream()
		streamMu.Lock()
		stream = admitted
		streamMu.Unlock()
		return admitted, nil
	})
	picker.commit(sessionTestRequest(true))
	awaitStreamHello(t, &streamMu, &stream)
	streamMu.Lock()
	admitted := stream
	streamMu.Unlock()
	deliverReadyStream(t, admitted)
	awaitAttachedState(t, harness.sup)

	timer := harness.clock.snapshotTimers()[0]
	require.True(t, timer.stopped(), "the initial-publication commit disarms the budget")
	timer.fire()
	// This bounded negative window gives the retired deadline goroutine an
	// opportunity to mishandle the late fire; no sleep is used for ordering.
	require.Never(t, func() bool {
		return harness.sup.State().Presentation != PresentAttached || admitted.closedNow() || harness.sup.State().Err != nil
	}, 50*time.Millisecond, time.Millisecond)
}

func TestAttachmentDeadlineConcurrentExpiryAndRetirement(t *testing.T) {
	tests := []struct {
		name   string
		retire func(*attachmentDeadline)
	}{
		{name: "disarm", retire: (*attachmentDeadline).disarm},
		{name: "adopt", retire: func(deadline *attachmentDeadline) {
			_ = deadline.adopt(&sessionTestStreamWithDeadline{sessionTestStream: newSessionTestStream(), deadline: time.Unix(2000, 0)})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newSupervisorTestClock()
			deadline := startAttachmentDeadline(context.Background(), clock)
			timer := clock.snapshotTimers()[0]
			done := make(chan struct{})
			go func() {
				timer.fire()
				tt.retire(deadline)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("concurrent expiry and retirement hung")
			}
			deadline.finish()
			require.True(t, clock.allStopped())
		})
	}
}

// TestSupervisorAttachmentPostAttachmentLossReturnsToPicker proves a stream
// that fails after attachment never keeps the attached presentation: the
// supervisor returns to the picker with the typed loss.
func TestSupervisorAttachmentPostAttachmentLossReturnsToPicker(t *testing.T) {
	picker := newAttachTestPicker()
	var (
		streamMu sync.Mutex
		stream   *sessionTestStream
	)
	harness := startAttachHarness(t, picker)
	harness.service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := newSessionTestStream()
		streamMu.Lock()
		stream = admitted
		streamMu.Unlock()
		return admitted, nil
	})
	picker.commit(sessionTestRequest(true))
	require.Eventually(t, func() bool { return len(harness.service.openedRequests()) == 1 }, 5*time.Second, time.Millisecond)

	streamMu.Lock()
	admitted := stream
	streamMu.Unlock()
	deliverReadyStream(t, admitted)
	awaitAttachedState(t, harness.sup)

	admitted.fail(ports.BrokerStreamLost{
		Connection: ports.BrokerConnectionID{1},
		Stream:     1,
		Epoch:      3,
		Cause:      domain.RemoteFailureTransport,
		Err:        errors.New("attachment lost"),
	})
	awaitPickerState(t, harness.sup)
	require.Eventually(t, func() bool { return harness.sup.State().Err != nil }, 5*time.Second, time.Millisecond)
	require.NotEqual(t, PresentAttached, harness.sup.State().Presentation, "a failed stream is never still attached")
}

// TestSupervisorAttachmentCancellationRetiresGeneration proves cancellation
// mid-attach closes the stream, ends the run, and never reports attachment.
func TestSupervisorAttachmentCancellationRetiresGeneration(t *testing.T) {
	picker := newAttachTestPicker()
	var (
		streamMu sync.Mutex
		stream   *sessionTestStream
	)
	harness := startAttachHarness(t, picker)
	harness.service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := newSessionTestStream()
		streamMu.Lock()
		stream = admitted
		streamMu.Unlock()
		return admitted, nil
	})
	picker.commit(sessionTestRequest(true))
	require.Eventually(t, func() bool { return len(harness.service.openedRequests()) == 1 }, 5*time.Second, time.Millisecond)

	harness.cancel()
	require.ErrorIs(t, harness.waitRun(t), context.Canceled)
	streamMu.Lock()
	admitted := stream
	streamMu.Unlock()
	require.True(t, admitted.closedNow(), "the supervisor close authority released the stream")
	require.Equal(t, PresentTerminating, harness.sup.State().Presentation)
	require.Empty(t, harness.terminal.publications, "a cancelled attachment never published")
}

// TestSupervisorPickerInputNeverReachesSession proves picker bytes produce no
// stream open, no session write, and no terminal write.
func TestSupervisorPickerInputNeverReachesSession(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startAttachHarness(t, picker)
	opened := false
	harness.service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		opened = true
		return newSessionTestStream(), nil
	})

	harness.reader.push([]byte("j"))
	require.Eventually(t, func() bool { return picker.consumedCount() >= 1 }, 5*time.Second, time.Millisecond)
	require.False(t, opened, "picker input must never open a stream")
	require.Zero(t, picker.resolveCount(), "picker input must never resolve a selection")
	require.Empty(t, harness.service.openedRequests())
	_, flushes, _ := harness.terminal.counts()
	require.Zero(t, flushes, "picker input must never flush session output")
	require.Empty(t, harness.terminal.written(), "picker input must never write session bytes")
}

// TestSupervisorAttachmentReachableFromRealPicker drives the real client-owned
// picker: a committed row resolves to one exact stream request and its
// publication attaches.
func TestSupervisorAttachmentReachableFromRealPicker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := newSupervisorTestClock()
	reader := newPickerChunkReader()
	t.Cleanup(reader.close)
	terminal := &attachTestTerminal{in: reader}
	service := newSupervisorTestService(ports.BrokerConnectionID{1})
	service.hub.publish(pickerTestSnapshot(3, 1, clock.Now(), "alpha"))
	admittedStreams := make(chan *sessionTestStream, 1)
	service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := newSessionTestStream()
		admittedStreams <- admitted
		return admitted, nil
	})

	controller := newPickerController(clock, pickerTestFreshness)
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) { return service, nil })
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
		Picker:    controller,
	})
	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	require.Eventually(t, func() bool { return controller.Catalogue().Revision() == 1 }, 5*time.Second, time.Millisecond)
	require.Equal(t, []string{"alpha"}, pickerSessionLabels(controller.Catalogue().Lines()))

	// The user commits the row under the cursor through the shared reader.
	reader.push([]byte("\r"))
	require.Eventually(t, func() bool { return len(service.openedRequests()) == 1 }, 5*time.Second, time.Millisecond)
	opened := service.openedRequests()[0]
	require.Equal(t, "alpha", opened.Target.SessionName)
	require.True(t, opened.Local)
	require.Equal(t, ports.BrokerAdmissionExact, opened.Admission)
	require.Equal(t, ports.BrokerStreamID(1), opened.Stream)

	var stream *sessionTestStream
	select {
	case stream = <-admittedStreams:
	case <-time.After(5 * time.Second):
		t.Fatal("the resolved stream was never admitted")
	}
	deliverReadyStream(t, stream)
	awaitAttachedState(t, sup)

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
	require.True(t, stream.closedNow())
	require.Equal(t, 1, terminal.restores, "the supervisor restored raw mode exactly once")
}

// TestSupervisorAttachmentConcurrentCommitAndCancellation deterministically
// retires attachment authority at each blocking boundary under review.
func TestSupervisorAttachmentConcurrentCommitAndCancellation(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T, *attachTestHarness, *attachTestPicker) *sessionTestStream
	}{
		{name: "open in flight", run: func(t *testing.T, harness *attachTestHarness, picker *attachTestPicker) *sessionTestStream {
			entered := make(chan struct{})
			release := make(chan struct{})
			stream := newSessionTestStream()
			harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				close(entered)
				<-release
				return stream, nil
			})
			picker.commit(sessionTestRequest(true))
			<-entered
			harness.cancel()
			close(release)
			return stream
		}},
		{name: "publication commit in flight", run: func(t *testing.T, harness *attachTestHarness, picker *attachTestPicker) *sessionTestStream {
			stream := newSessionTestStream()
			harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				return stream, nil
			})
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			harness.terminal.publishEntered = entered
			harness.terminal.publishRelease = release
			picker.commit(sessionTestRequest(true))
			awaitHello(t, stream)
			stream.deliver(protocol.Welcome{SessionName: "alpha"})
			stream.deliver(sessionTestOutput(1, "initial"))
			<-entered
			harness.cancel()
			close(release)
			return stream
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			harness := startAttachHarness(t, picker)
			stream := tt.run(t, harness, picker)
			// Cancellation revokes picker authority immediately, before teardown
			// completes. Inject work in that teardown window and prove neither path
			// can consume it.
			resolved, consumed := picker.resolveCount(), picker.consumedCount()
			picker.commit(sessionTestRequest(true))
			harness.reader.push([]byte("retired"))
			require.False(t, picker.owns())
			require.Equal(t, resolved, picker.resolveCount(), "revoked picker operations must not resolve")
			require.Equal(t, consumed, picker.consumedCount(), "revoked input must not be consumed")
			require.ErrorIs(t, harness.waitRun(t), context.Canceled)
			require.True(t, stream.closedNow(), "the admitted stream must be released")
			require.NotEqual(t, PresentAttached, harness.sup.State().Presentation)
		})
	}
}
