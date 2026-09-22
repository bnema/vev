package client

import (
	"bytes"
	"context"
	"errors"
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
	pending        ports.UIContext
	revision       uint64
	txDepth        int
	txFailed       bool
	available      bool
	successes      []bool
	publishEntered chan struct{}
	publishRelease <-chan struct{}
	// publishMatch narrows the publication block to one matching transaction.
	// A nil predicate blocks every publication, which is what the existing
	// initial-publication tests rely on.
	publishMatch func(ports.UIContext) bool
	// detachedSignals receives one signal per committed Picker publication, so
	// a test observes attachment release without polling.
	detachedSignals chan struct{}
	// events records the transaction order: each begin, each publication, and
	// each successful drain. A test proves release ordering against it instead
	// of inferring it from separate slices.
	events  []attachTestEvent
	changes chan struct{}
}

// attachTestEvent is one ordered terminal transaction event.
type attachTestEvent struct {
	kind   string // begin | publish | drain
	status ports.UIPresentationStatus
	state  uint64
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

func (t *attachTestTerminal) BeginOutput(ctx ports.UIContext) {
	t.mu.Lock()
	if ctx.AttachmentHandle == "" && len(t.publications) != 0 && ctx.Status == ports.UIStatusAttached {
		ctx.AttachmentHandle = t.publications[len(t.publications)-1].AttachmentHandle
	}
	if ctx.Generation == 0 && len(t.publications) != 0 && ctx.Status == ports.UIStatusAttached {
		ctx.Generation = t.publications[len(t.publications)-1].Generation
	}
	if t.txDepth == 0 {
		t.pending = ctx
		t.txFailed = false
		t.events = append(t.events, attachTestEvent{kind: "begin", status: ctx.Status, state: ctx.OutputState})
	}
	t.txDepth++
	t.mu.Unlock()
}

func (t *attachTestTerminal) EndOutput(success bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.successes = append(t.successes, success)
	if t.txDepth == 0 {
		return
	}
	if !success {
		t.txFailed = true
	}
	t.txDepth--
	if t.txDepth != 0 {
		return
	}
	if t.txFailed {
		t.available = false
		t.pending = ports.UIContext{}
		return
	}
	t.publications = append(t.publications, t.pending)
	t.revision++
	t.events = append(t.events, attachTestEvent{kind: "drain", status: t.pending.Status, state: t.pending.OutputState})
	t.available = true
	t.pending = ports.UIContext{}
}

func (t *attachTestTerminal) PublishContext(uiContext ports.UIContext) error {
	if t.publishMatch == nil || t.publishMatch(uiContext) {
		if t.publishEntered != nil {
			select {
			case t.publishEntered <- struct{}{}:
			default:
			}
		}
		if t.publishRelease != nil {
			<-t.publishRelease
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.available {
		return ports.ErrUIUnavailable
	}
	// The composition terminal applies the published-context shape rule the real
	// uiterm terminal applies: only an Attached presentation inherits a missing
	// handle or generation; Picker and Connecting are recorded exactly as stated.
	if uiContext.Status == ports.UIStatusAttached {
		if uiContext.AttachmentHandle == "" && len(t.publications) != 0 {
			uiContext.AttachmentHandle = t.publications[len(t.publications)-1].AttachmentHandle
		}
		if uiContext.Generation == 0 && len(t.publications) != 0 {
			uiContext.Generation = t.publications[len(t.publications)-1].Generation
		}
	}
	t.publications = append(t.publications, uiContext)
	t.revision++
	t.events = append(t.events, attachTestEvent{kind: "publish", status: uiContext.Status, state: uiContext.OutputState})
	if uiContext.Status == ports.UIStatusPicker && t.detachedSignals != nil {
		select {
		case t.detachedSignals <- struct{}{}:
		default:
		}
	}
	return nil
}

func (t *attachTestTerminal) Snapshot() (ports.UISnapshot, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var ctx ports.UIContext
	if len(t.publications) != 0 {
		ctx = t.publications[len(t.publications)-1]
	}
	if !t.available || t.revision == 0 {
		return ports.UISnapshot{}, ports.ErrUIUnavailable
	}
	return ports.UISnapshot{Revision: t.revision, Context: ctx}, nil
}

func (t *attachTestTerminal) Changes() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.changes == nil {
		t.changes = make(chan struct{})
	}
	return t.changes
}

func (t *attachTestTerminal) publicationList() []ports.UIContext {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]ports.UIContext(nil), t.publications...)
}

// eventIndex reports the position of the first matching transaction event, or
// -1 when it never happened.
func (t *attachTestTerminal) eventIndex(kind string, status ports.UIPresentationStatus, state uint64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, event := range t.events {
		if event.kind == kind && event.status == status && event.state == state {
			return i
		}
	}
	return -1
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
	// resolveErr makes ResolveKey refuse locally, which is how a test reaches
	// the local-refusal classification without dialing anything.
	resolveErr error
	request    ports.BrokerOpenStreamRequest
	resolved   int
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

func (p *attachTestPicker) ResolveKey(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resolved++
	if p.resolveErr != nil {
		return ports.BrokerOpenStreamRequest{}, p.resolveErr
	}
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
	// available starts true, exactly like a real composition terminal: a
	// presentation-only publication is observable before any frame drains.
	terminal := &attachTestTerminal{in: reader, available: true}
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

// authorityState reports the current foreground slot state without racing the
// supervisor's own transitions.
func (a *attachmentAuthority) authorityState() attachmentAuthorityState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
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
func TestSupervisorUIActionBindingUsesSingleAttachmentPump(t *testing.T) {
	picker := newAttachTestPicker()
	var stream *sessionTestStream
	var streamMu sync.Mutex
	var ui *UI
	harness := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) {
		ui = NewUI(cfg.Terminal.(ports.UIState), cfg.Clock)
		cfg.UI = ui
	})
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := newSessionTestStream()
		streamMu.Lock()
		stream = admitted
		streamMu.Unlock()
		return admitted, nil
	})

	// Picker ownership has no UI binding, so this would fail if actions could
	// collide with or bypass the picker's exclusive input claim.
	_, err := ui.Action(t.Context(), ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Text: "picker"})
	var pickerErr *ports.UIError
	require.ErrorAs(t, err, &pickerErr)
	require.Equal(t, ports.UIErrUnavailable, pickerErr.Code)

	picker.commit(sessionTestRequest(true))
	require.Eventually(t, func() bool {
		streamMu.Lock()
		defer streamMu.Unlock()
		return stream != nil && len(stream.messages()) > 0
	}, 5*time.Second, time.Millisecond)
	streamMu.Lock()
	admitted := stream
	streamMu.Unlock()
	deliverReadyStream(t, admitted)
	awaitAttachedState(t, harness.sup)
	attachedOutput := sessionTestOutput(2, "\x1b[Hattached")
	attachedOutput.Full = false
	attachedOutput.Base = 1
	attachedOutput.New = 2
	admitted.deliver(attachedOutput)

	var snapshot ports.UISnapshot
	require.Eventually(t, func() bool {
		var captureErr error
		snapshot, captureErr = ui.Capture(ui.Handle())
		return captureErr == nil && snapshot.Context.Status == ports.UIStatusAttached
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, ports.UIStatusAttached, snapshot.Context.Status)
	require.NotZero(t, snapshot.Context.Generation)
	actionDone := make(chan error, 1)
	go func() {
		_, actionErr := ui.Action(t.Context(), ports.UIActionRequest{Attachment: ui.Handle(), Generation: snapshot.Context.Generation, Text: "automated"})
		actionDone <- actionErr
	}()
	var actionID uint64
	require.Eventually(t, func() bool {
		var fenced bool
		for _, message := range admitted.messages() {
			switch typed := message.(type) {
			case protocol.Input:
				if string(typed.Data) == "automated" {
					actionID = typed.ActionID
				}
			case protocol.UIFence:
				fenced = actionID != 0 && typed.ActionID == actionID
			}
		}
		return fenced
	}, 5*time.Second, time.Millisecond, "admitted actions must be followed by their UI fence")
	processedOutput := sessionTestOutput(3, "\x1b[Hprocessed")
	processedOutput.Full = false
	processedOutput.Base = 2
	processedOutput.New = 3
	processedOutput.Context.Publication = 3
	admitted.deliver(processedOutput)
	var boundary ports.UIActionResult
	require.Eventually(t, func() bool {
		ui.mu.Lock()
		boundary = ui.boundary
		ui.mu.Unlock()
		return boundary.Revision != 0 && boundary.Context.OutputState == processedOutput.New && boundary.Context.ViewPublication == processedOutput.Context.Publication
	}, 5*time.Second, time.Millisecond, "the action-specific output must be the committed processing boundary")
	admitted.deliver(protocol.UIReceipt{ActionID: actionID, Epoch: 1, State: 3, ViewPublication: 3, Outcome: protocol.UIReceiptProcessed})
	select {
	case actionErr := <-actionDone:
		require.NoError(t, actionErr, "receipt routing must complete the admitted action as processed")
	case <-time.After(5 * time.Second):
		t.Fatal("processed UI action did not complete")
	}

	// Ending the stream releases before picker reacquisition. A stale request
	// would be admitted here if finalization forgot to retire the binding.
	admitted.deliver(protocol.Detached{})
	awaitPickerState(t, harness.sup)
	ui.mu.Lock()
	require.Nil(t, ui.input, "attachment finalization must revoke the UI pump binding")
	require.Zero(t, ui.consumer, "attachment finalization must revoke the UI consumer")
	require.Nil(t, ui.foreground, "attachment finalization must retire the UI foreground")
	ui.mu.Unlock()
	_, err = ui.Action(t.Context(), ports.UIActionRequest{Attachment: ui.Handle(), Generation: snapshot.Context.Generation, Text: "stale"})
	var staleErr *ports.UIError
	require.ErrorAs(t, err, &staleErr)
	require.Equal(t, ports.UIErrUnavailable, staleErr.Code)

	// Reconnecting installs a fresh UI-owned generation. The prior generation
	// remains stale even while another attachment is actionable.
	reconnected := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return reconnected, nil
	})
	picker.commit(sessionTestRequest(true))
	awaitHello(t, reconnected)
	deliverReadyStream(t, reconnected)
	awaitAttachedState(t, harness.sup)
	reconnectedOutput := sessionTestOutput(2, "\x1b[Hreconnected")
	reconnectedOutput.Full = false
	reconnectedOutput.Base = 1
	reconnectedOutput.New = 2
	reconnected.deliver(reconnectedOutput)
	require.Eventually(t, func() bool {
		current, captureErr := ui.Capture(ui.Handle())
		return captureErr == nil && current.Context.Status == ports.UIStatusAttached && current.Context.Generation != snapshot.Context.Generation
	}, 5*time.Second, time.Millisecond)
	_, err = ui.Action(t.Context(), ports.UIActionRequest{Attachment: ui.Handle(), Generation: snapshot.Context.Generation, Text: "still-stale"})
	staleErr = nil
	require.ErrorAs(t, err, &staleErr)
	require.Equal(t, ports.UIErrStaleAttachment, staleErr.Code)
	select {
	case <-actionDone: // completion may be receipt-driven or retired on release.
	default:
	}
}

// TestSupervisorUIActionReleaseWaitsForInFlightOutput pins GO-201: finalization
// must not release the UI foreground binding while a foreground output
// transaction is in flight. The release publishes Picker, so releasing it
// early would publish an unattached presentation while a newer session frame
// was still being written, and would return the picker claim before the last
// frame committed. The mutation this test pins moves releaseForeground before
// lease.stop in attachmentForeground.finalize.
func TestSupervisorUIActionReleaseWaitsForInFlightOutput(t *testing.T) {
	const inFlightState = 2
	picker := newAttachTestPicker()
	var (
		streamMu sync.Mutex
		stream   *sessionTestStream
		ui       *UI
	)
	harness := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) {
		ui = NewUI(cfg.Terminal.(ports.UIState), cfg.Clock)
		cfg.UI = ui
	})
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := newSessionTestStream()
		streamMu.Lock()
		stream = admitted
		streamMu.Unlock()
		return admitted, nil
	})

	// Installed before the commit, so the supervisor's publication goroutine
	// always observes them: only the attached frame with inFlightState blocks.
	terminal := harness.terminal
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseOutput := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseOutput()
	detached := make(chan struct{}, 4)
	terminal.publishMatch = func(ctx ports.UIContext) bool {
		return ctx.Status == ports.UIStatusAttached && ctx.OutputState == inFlightState
	}
	terminal.publishEntered = entered
	terminal.publishRelease = release
	terminal.detachedSignals = detached

	picker.commit(sessionTestRequest(true))
	awaitStreamHello(t, &streamMu, &stream)
	streamMu.Lock()
	admitted := stream
	streamMu.Unlock()
	deliverReadyStream(t, admitted)
	awaitAttachedState(t, harness.sup)

	// Block one foreground output transaction after BeginOutput and before its
	// commit, so the send lease is held while finalization begins.
	inFlight := sessionTestOutput(inFlightState, "\x1b[Hin-flight")
	inFlight.Full = false
	inFlight.Base = 1
	inFlight.New = inFlightState
	admitted.deliver(inFlight)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the foreground output transaction never reached its publication boundary")
	}
	require.Equal(t, -1, terminal.eventIndex("drain", ports.UIStatusAttached, inFlightState), "the in-flight frame must still hold its transaction")

	// Cancel the attachment while that transaction is in flight.
	harness.service.lose(errors.New("attachment lost"))
	require.Eventually(t, func() bool {
		return harness.sup.attachments.authority.authorityState() == authorityFinalizing
	}, 5*time.Second, time.Millisecond, "attachment finalization must begin while the frame is in flight")

	// Finalization cannot complete, and Picker cannot be published, until the
	// in-flight transaction drains.
	require.Never(t, func() bool {
		select {
		case <-detached:
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, time.Millisecond, "Picker must not publish while a foreground transaction is in flight")
	require.Equal(t, authorityFinalizing, harness.sup.attachments.authority.authorityState(), "finalize still waits on the output transaction")
	require.False(t, admitted.closedNow(), "the retired stream is released only after the drain")
	require.False(t, picker.owns(), "the picker claim returns only after the drain")
	require.Equal(t, PresentAttached, harness.sup.State().Presentation, "the attachment is still presented while its last frame drains")
	ui.mu.Lock()
	require.NotNil(t, ui.input, "the UI foreground binding is released only after the drain")
	require.NotNil(t, ui.foreground, "the UI foreground context is retired only after the drain")
	ui.mu.Unlock()

	releaseOutput()
	select {
	case <-detached:
	case <-time.After(5 * time.Second):
		t.Fatal("attachment finalization never published Picker")
	}

	// The newer frame commits before Picker, and Picker carries no committed
	// output boundary: an unattached presentation never presents session
	// metadata.
	drained := terminal.eventIndex("drain", ports.UIStatusAttached, inFlightState)
	published := terminal.eventIndex("publish", ports.UIStatusPicker, 0)
	require.GreaterOrEqual(t, drained, 0, "the in-flight frame must commit once it drains")
	require.Greater(t, published, drained, "Picker must be published only after the newer frame commits")
	publications := terminal.publicationList()
	require.GreaterOrEqual(t, len(publications), 2)
	last := publications[len(publications)-1]
	require.Equal(t, ports.UIStatusPicker, last.Status)
	require.Zero(t, last.OutputState, "Picker carries no committed output boundary")
	prior := publications[len(publications)-2]
	require.Equal(t, ports.UIStatusAttached, prior.Status)
	require.Equal(t, uint64(inFlightState), prior.OutputState)

	// Finalization completes and hands the picker claim back.
	require.Eventually(t, func() bool {
		ui.mu.Lock()
		released := ui.input == nil && ui.consumer == 0 && ui.foreground == nil
		ui.mu.Unlock()
		return released && picker.owns() && harness.sup.attachments.authority.authorityState() == authorityIdle && admitted.closedNow()
	}, 5*time.Second, time.Millisecond, "finalization must complete and return the picker claim")
	require.Equal(t, PresentPicker, harness.sup.State().Presentation)
}

func TestSupervisorUIViewUpdateCommitsActionBoundaryWithoutBytes(t *testing.T) {
	picker := newAttachTestPicker()
	var ui *UI
	harness := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) {
		ui = NewUI(cfg.Terminal.(ports.UIState), cfg.Clock)
		cfg.UI = ui
	})
	stream := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})
	picker.commit(sessionTestRequest(true))
	awaitHello(t, stream)
	deliverReadyStream(t, stream)
	awaitAttachedState(t, harness.sup)
	initialOutput := sessionTestOutput(2, "\x1b[Hmetadata-base")
	initialOutput.Full = false
	initialOutput.Base = 1
	initialOutput.New = 2
	initialOutput.Context.Publication = 2
	stream.deliver(initialOutput)

	var snapshot ports.UISnapshot
	require.Eventually(t, func() bool {
		var err error
		snapshot, err = ui.Capture(ui.Handle())
		return err == nil && snapshot.Context.Status == ports.UIStatusAttached && snapshot.Context.OutputState == 2
	}, 5*time.Second, time.Millisecond)
	actionDone := make(chan error, 1)
	go func() {
		_, err := ui.Action(t.Context(), ports.UIActionRequest{Attachment: ui.Handle(), Generation: snapshot.Context.Generation, Text: "metadata"})
		actionDone <- err
	}()
	var actionID uint64
	require.Eventually(t, func() bool {
		for _, message := range stream.messages() {
			if fence, ok := message.(protocol.UIFence); ok {
				actionID = fence.ActionID
				return actionID != 0
			}
		}
		return false
	}, 5*time.Second, time.Millisecond)
	view := protocol.ViewContext{Publication: 3, Route: snapshot.Context.Route, TabID: "metadata-tab", FocusedPaneID: "metadata-pane"}
	view.Route.Target = protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "alpha"}
	stream.deliver(protocol.UIViewUpdate{Epoch: 1, State: 2, Context: view})
	require.Eventually(t, func() bool {
		current, err := ui.Capture(ui.Handle())
		return err == nil && current.Context.ViewPublication == 3 && current.Context.TabID == "metadata-tab"
	}, 5*time.Second, time.Millisecond, "metadata-only update must commit through the foreground transaction")
	stream.deliver(protocol.UIReceipt{ActionID: actionID, Epoch: 1, State: 2, ViewPublication: 3, Outcome: protocol.UIReceiptProcessed})
	select {
	case err := <-actionDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("metadata-only committed boundary did not complete action")
	}
}

func TestSupervisorUIViewUpdateRequestsOneCoalescedReset(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startAttachHarness(t, picker)
	stream := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})
	picker.commit(sessionTestRequest(true))
	awaitHello(t, stream)
	deliverReadyStream(t, stream)
	awaitAttachedState(t, harness.sup)
	bad := protocol.UIViewUpdate{Epoch: 9, State: 9, Context: protocol.ViewContext{Publication: 2, Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "alpha"}}, TabID: "future", FocusedPaneID: "future"}}
	stream.deliver(bad)
	stream.deliver(bad)
	require.Eventually(t, func() bool {
		count := 0
		for _, message := range stream.messages() {
			if _, ok := message.(protocol.OutputResetRequest); ok {
				count++
			}
		}
		return count == 1
	}, 5*time.Second, time.Millisecond, "future view dependencies must request one coalesced reset")
}

func TestSupervisorWithoutUIDoesNotBindActions(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startAttachHarness(t, picker)
	require.Nil(t, harness.sup.cfg.UI, "the optional seam must preserve publication-only composition")
	require.Nil(t, harness.sup.attachments.actionUI, "mutation that binds an implicit UI changes no-UI behavior")
}

func TestUIActionWithoutInputReturnsUnavailable(t *testing.T) {
	ui := NewUI(&attachTestTerminal{}, newSupervisorTestClock())
	_, err := ui.Action(t.Context(), ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Text: "x"})
	var uiErr *ports.UIError
	require.ErrorAs(t, err, &uiErr)
	require.Equal(t, ports.UIErrUnavailable, uiErr.Code)
}

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
			// The attached worker's palette query draws nothing; strip it.
			require.Equal(t, "\x1b[Hready", strings.ReplaceAll(harness.terminal.written(), paletteColorBatch, ""))
			// Two transactions commit on this attachment: the initial frame under the
			// pre-attach Connecting presentation, then the committed Attached
			// presentation with no bytes of its own.
			require.Equal(t, []bool{true, true}, harness.terminal.successes, "both publications committed through the UI transaction")
			publications := harness.terminal.publicationList()
			require.Equal(t, ports.UIStatusConnecting, publications[0].Status)
			require.Equal(t, ports.UIStatusAttached, publications[len(publications)-1].Status)
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

// TestSupervisorConnectingWhileAcquisitionBlockedPublishesNoSessionMetadata pins
// the blocked-acquisition presentation: while the logical stream open is still
// in flight, the published presentation is Connecting with the run's handle and
// no session metadata or actionable generation, and capture and wait remain
// usable in that state.
func TestSupervisorConnectingWhileAcquisitionBlockedPublishesNoSessionMetadata(t *testing.T) {
	picker := newAttachTestPicker()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var ui *UI
	harness := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) {
		ui = NewUI(cfg.Terminal.(ports.UIState), cfg.Clock)
		cfg.UI = ui
	})
	harness.service.setOpenStream(func(ctx context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		select {
		case <-release:
			return newSessionTestStream(), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	picker.commit(sessionTestRequest(true))
	require.Eventually(t, func() bool { return len(harness.service.openedRequests()) == 1 }, 5*time.Second, time.Millisecond, "the acquisition is in flight")

	// The acquisition is blocked, so the run publishes Connecting and stays
	// usable: capture and wait both work in that presentation.
	connecting := ports.UIStatusConnecting
	matched, err := ui.Wait(t.Context(), ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &connecting}})
	require.NoError(t, err)
	require.Equal(t, ports.UIStatusConnecting, matched.Context.Status)
	require.Zero(t, matched.Context.Generation, "a blocked acquisition never publishes an actionable generation")
	require.Equal(t, protocol.ExactSessionTarget{}, matched.Context.Route.Target, "Connecting presents no session identity")
	require.Equal(t, ui.Handle(), matched.Context.AttachmentHandle, "Connecting still presents the run's stable handle")
	snapshot, err := ui.Capture(ui.Handle())
	require.NoError(t, err)
	require.Equal(t, ports.UIStatusConnecting, snapshot.Context.Status)

	// Input actions are attachment actions: refused while the attachment has not
	// committed.
	_, err = ui.Action(t.Context(), ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Keys: []string{"Escape"}})
	var actionErr *ports.UIError
	require.ErrorAs(t, err, &actionErr)
	require.Equal(t, ports.UIErrUnavailable, actionErr.Code)

	releaseOnce.Do(func() { close(release) })
	harness.cancel()
	require.ErrorIs(t, harness.waitRun(t), context.Canceled)
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
	terminal := &attachTestTerminal{in: reader, available: true}
	service := newSupervisorTestService(ports.BrokerConnectionID{1})
	service.hub.publish(pickerTestSnapshot(3, 1, clock.Now(), "alpha"))
	admittedStreams := make(chan *sessionTestStream, 1)
	service.setOpenStream(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		admitted := newSessionTestStream()
		admittedStreams <- admitted
		return admitted, nil
	})

	controller := newPickerController(clock, pickerTestFreshness, true)
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
