package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// workerTestStream is one scripted broker logical stream. It records typed
// client messages and unblocks ReceiveServer when the supervisor closes it,
// exactly like a real logical stream's Close contract.
type workerTestStream struct {
	mu        sync.Mutex
	sent      []protocol.ClientMessage
	closed    chan struct{}
	closeOnce sync.Once
	// closeHook, when set, runs during Close. It lets a test observe the
	// supervisor's revocation-before-close ordering.
	closeHook func()
}

func newWorkerTestStream() *workerTestStream {
	return &workerTestStream{closed: make(chan struct{})}
}

func (s *workerTestStream) SendClient(message protocol.ClientMessage) error {
	select {
	case <-s.closed:
		return errors.New("worker test stream is closed")
	default:
	}
	s.mu.Lock()
	s.sent = append(s.sent, message)
	s.mu.Unlock()
	return nil
}

func (s *workerTestStream) ReceiveServer() (protocol.ServerMessage, error) {
	<-s.closed
	return nil, io.EOF
}

func (s *workerTestStream) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}

func (s *workerTestStream) LinkState() ports.LinkState { return 0 }

func (s *workerTestStream) LinkEvents() <-chan ports.LinkEvent { return nil }

func (s *workerTestStream) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.closeHook != nil {
			s.closeHook()
		}
	})
	return nil
}

func (s *workerTestStream) Done() <-chan struct{} { return s.closed }

func (s *workerTestStream) Err() error { return nil }

func (s *workerTestStream) messages() []protocol.ClientMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.ClientMessage(nil), s.sent...)
}

func (s *workerTestStream) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// workerTestTerminal is the supervisor-owned terminal: it counts raw restores
// so a test can prove the worker mechanism never restores, captures written
// output, and doubles as the UI publication transaction.
type workerTestTerminal struct {
	mu         sync.Mutex
	out        bytes.Buffer
	flushes    int
	restores   int
	resizes    chan domain.Geometry
	published  []ports.UIContext
	steps      []string
	successes  []bool
	writeErr   error
	shortWrite int
	flushErr   error
	publishErr error
}

func newWorkerTestTerminal() *workerTestTerminal {
	return &workerTestTerminal{resizes: make(chan domain.Geometry, 4)}
}

func (t *workerTestTerminal) EnterRaw() (func() error, error) {
	return func() error {
		t.mu.Lock()
		t.restores++
		t.mu.Unlock()
		return nil
	}, nil
}

func (t *workerTestTerminal) Geometry() (domain.Geometry, error) { return domain.Geometry{}, nil }

func (t *workerTestTerminal) ResizeEvents() <-chan domain.Geometry { return t.resizes }

func (t *workerTestTerminal) In() io.Reader { return nil }

func (t *workerTestTerminal) Out() io.Writer { return &workerTestWriter{t} }

func (t *workerTestTerminal) Flush() error {
	t.mu.Lock()
	t.flushes++
	t.steps = append(t.steps, "flush")
	t.mu.Unlock()
	return t.flushErr
}

func (t *workerTestTerminal) BeginOutput(ports.UIContext) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.steps = append(t.steps, "begin")
}

func (t *workerTestTerminal) EndOutput(success bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.steps = append(t.steps, "end")
	t.successes = append(t.successes, success)
}

func (t *workerTestTerminal) PublishContext(context ports.UIContext) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.published = append(t.published, context)
	t.steps = append(t.steps, "publish")
	return t.publishErr
}

func (t *workerTestTerminal) written() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.out.String()
}

func (t *workerTestTerminal) flushCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flushes
}

func (t *workerTestTerminal) restoreCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.restores
}

func (t *workerTestTerminal) publications() []ports.UIContext {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]ports.UIContext(nil), t.published...)
}

type workerTestWriter struct{ term *workerTestTerminal }

func (w *workerTestWriter) Write(data []byte) (int, error) {
	w.term.mu.Lock()
	defer w.term.mu.Unlock()
	w.term.steps = append(w.term.steps, "write")
	if w.term.writeErr != nil {
		return 0, w.term.writeErr
	}
	if w.term.shortWrite > 0 {
		n := min(w.term.shortWrite, len(data))
		_, _ = w.term.out.Write(data[:n])
		return n, nil
	}
	return w.term.out.Write(data)
}

// fakeWorker adapts a function to the AttachmentWorker contract.
type fakeWorker struct {
	run func(ctx context.Context, fg AttachmentForeground) AttachmentEvent
}

func (w *fakeWorker) Run(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
	return w.run(ctx, fg)
}

// blockingWorker ignores input/output and parks until its context ends.
func blockingWorker() *fakeWorker {
	return &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		<-ctx.Done()
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}
}

func newWorkerTestHost(term *workerTestTerminal, input *terminalInputPump, onAttached func(AttachmentToken)) *attachmentHost {
	return newAttachmentHost(attachmentHostConfig{
		Terminal:   term,
		Clock:      newSupervisorTestClock(),
		Input:      input,
		OnAttached: onAttached,
	})
}

// TestAttachmentWorkerExclusiveForeground proves exactly one worker generation
// owns the terminal foreground at a time: a second Begin is refused while the
// first is live and succeeds once the first is cancelled.
func TestAttachmentWorkerExclusiveForeground(t *testing.T) {
	term := newWorkerTestTerminal()
	host := newWorkerTestHost(term, nil, nil)

	run1, ok := host.Begin(context.Background(), AttachmentToken{Generation: 1, Attempt: 1}, blockingWorker(), newWorkerTestStream())
	require.True(t, ok)
	require.NotNil(t, run1)

	_, ok = host.Begin(context.Background(), AttachmentToken{Generation: 2, Attempt: 1}, blockingWorker(), newWorkerTestStream())
	require.False(t, ok, "second foreground must be refused while the first is live")
	require.Equal(t, AttachmentToken{Generation: 1, Attempt: 1}, host.authority.foreground().Token())

	run1.Cancel()
	require.Nil(t, host.authority.foreground())

	run2, ok := host.Begin(context.Background(), AttachmentToken{Generation: 2, Attempt: 1}, blockingWorker(), newWorkerTestStream())
	require.True(t, ok, "a fresh generation may own the foreground after cancellation")
	run2.Cancel()
	require.Nil(t, host.authority.foreground())
}

// TestAttachmentWorkerZeroTokenRefused proves an unassigned token is never
// granted, so a worker can never run without a supervisor generation.
func TestAttachmentWorkerZeroTokenRefused(t *testing.T) {
	host := newWorkerTestHost(newWorkerTestTerminal(), nil, nil)
	_, ok := host.Begin(context.Background(), AttachmentToken{}, blockingWorker(), newWorkerTestStream())
	require.False(t, ok)
	require.Nil(t, host.authority.foreground())
}

// TestAttachmentWorkerCanceledGenerationCannotAct proves cancellation revokes
// input/output authority: a canceled generation cannot write, flush, consume or
// forward input, resize, publish UI, or mark itself attached, and its grant is
// closed.
func TestAttachmentWorkerCanceledGenerationCannotAct(t *testing.T) {
	term := newWorkerTestTerminal()
	host := newWorkerTestHost(term, nil, nil)
	stream := newWorkerTestStream()

	fgReady := make(chan AttachmentForeground, 1)
	worker := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		fgReady <- fg
		<-ctx.Done()
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}
	run, ok := host.Begin(context.Background(), AttachmentToken{Generation: 4, Attempt: 2}, worker, stream)
	require.True(t, ok)
	fg := <-fgReady
	require.Equal(t, AttachmentToken{Generation: 4, Attempt: 2}, fg.Token())

	run.Cancel()

	select {
	case <-fg.Done():
	default:
		t.Fatal("grant Done must be closed after cancellation")
	}
	require.ErrorIs(t, fg.Output(ports.UIContext{}, []byte("late")), errAttachmentForegroundRevoked)
	require.ErrorIs(t, fg.Output(ports.UIContext{AttachmentHandle: "late"}, nil), errAttachmentForegroundRevoked)
	require.False(t, fg.MarkAttached())
	require.False(t, fg.Attached())

	_, okInput := fg.Input(context.Background())
	require.False(t, okInput)
	_, okResize := fg.Resize(context.Background())
	require.False(t, okResize)

	require.Empty(t, term.written(), "a canceled generation must not write the terminal")
	require.Empty(t, term.publications(), "a canceled generation must not publish UI")
	require.True(t, stream.isClosed(), "the supervisor closes the owned stream")
}

// TestAttachmentWorkerConsumesAndForwardsInput proves an authorized worker
// consumes the shared terminal pump and forwards the bytes on its own stream.
func TestAttachmentWorkerConsumesAndForwardsInput(t *testing.T) {
	term := newWorkerTestTerminal()
	pr, pw := io.Pipe()
	pump := newTerminalInputPump(pr)
	pump.start()
	defer pump.stop()
	defer pw.Close()

	host := newWorkerTestHost(term, pump, nil)
	stream := newWorkerTestStream()

	worker := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		event, ok := fg.Input(ctx)
		if !ok {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: errors.New("no authorized input")}
		}
		fg.AckInput()
		if err := fg.Stream().SendClient(protocol.Input{InputSeq: 1, Data: event.Data}); err != nil {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: err}
		}
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}

	go func() { _, _ = pw.Write([]byte("hello")) }()

	event, adopted := host.Run(context.Background(), AttachmentToken{Generation: 1, Attempt: 1}, worker, stream)
	require.True(t, adopted)
	require.Equal(t, AttachmentEventEnded, event.Kind)

	messages := stream.messages()
	require.Len(t, messages, 1)
	inputMessage, ok := messages[0].(protocol.Input)
	require.True(t, ok)
	require.Equal(t, []byte("hello"), inputMessage.Data)
}

// TestAttachmentWorkerResizeAuthorized proves an authorized worker observes the
// supervisor's resize events and forwards them on its stream.
func TestAttachmentWorkerResizeAuthorized(t *testing.T) {
	term := newWorkerTestTerminal()
	host := newWorkerTestHost(term, nil, nil)
	stream := newWorkerTestStream()
	geometry := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}

	saw := make(chan domain.Geometry, 1)
	worker := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		term.resizes <- geometry
		resize, ok := fg.Resize(ctx)
		if !ok {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: errors.New("no authorized resize")}
		}
		saw <- resize
		if err := fg.Stream().SendClient(protocol.Resize{Size: resize.Size, PixelWidth: resize.PixelWidth, PixelHeight: resize.PixelHeight}); err != nil {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: err}
		}
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}

	event, adopted := host.Run(context.Background(), AttachmentToken{Generation: 1, Attempt: 1}, worker, stream)
	require.True(t, adopted)
	require.Equal(t, AttachmentEventEnded, event.Kind)
	require.Equal(t, geometry, <-saw)

	messages := stream.messages()
	require.Len(t, messages, 1)
	_, ok := messages[0].(protocol.Resize)
	require.True(t, ok)
}

// TestAttachmentWorkerPublishesUIAuthorized proves an authorized worker may
// publish UI context and mark the attached transition, and that the grant's
// generation is stamped onto the publication.
func TestAttachmentWorkerPublishesUIAuthorized(t *testing.T) {
	term := newWorkerTestTerminal()
	attached := make(chan AttachmentToken, 1)
	host := newWorkerTestHost(term, nil, func(token AttachmentToken) { attached <- token })
	stream := newWorkerTestStream()

	worker := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		if err := fg.Output(ports.UIContext{}, nil); err != nil {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: err}
		}
		if !fg.MarkAttached() {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: errors.New("attach refused")}
		}
		if !fg.Attached() {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: errors.New("attach not recorded")}
		}
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}

	event, adopted := host.Run(context.Background(), AttachmentToken{Generation: 7, Attempt: 2}, worker, stream)
	require.True(t, adopted)
	require.Equal(t, AttachmentEventEnded, event.Kind)

	publications := term.publications()
	require.Len(t, publications, 1)
	require.Equal(t, uint64(7), publications[0].Generation, "the granted generation is stamped")
	require.Equal(t, AttachmentToken{Generation: 7, Attempt: 2}, <-attached)
}

// TestAttachmentWorkerEventReturnsToPickerWithoutTerminalRestore proves a
// worker end, stream loss, or typed failure returns a typed event stamped with
// the granted token while the worker mechanism never restores the terminal and
// never closes the process.
func TestAttachmentWorkerEventReturnsToPickerWithoutTerminalRestore(t *testing.T) {
	cases := []struct {
		name string
		kind AttachmentEventKind
		err  error
	}{
		{name: "ended", kind: AttachmentEventEnded},
		{name: "lost", kind: AttachmentEventLost, err: errors.New("carriage lost")},
		{name: "failed", kind: AttachmentEventFailed, err: &ProtocolError{Code: 7, Text: "bad welcome"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			term := newWorkerTestTerminal()
			host := newWorkerTestHost(term, nil, nil)
			stream := newWorkerTestStream()
			// The worker reports a spoofed token; the host must override it with
			// the granted generation/attempt.
			worker := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
				return AttachmentEvent{Token: AttachmentToken{Generation: 999}, Kind: tc.kind, Err: tc.err}
			}}

			event, adopted := host.Run(context.Background(), AttachmentToken{Generation: 3, Attempt: 5}, worker, stream)
			require.True(t, adopted)
			require.Equal(t, tc.kind, event.Kind)
			require.Equal(t, AttachmentToken{Generation: 3, Attempt: 5}, event.Token)
			if tc.err != nil {
				require.ErrorIs(t, event.Err, tc.err)
			} else {
				require.NoError(t, event.Err)
			}
			require.Equal(t, 0, term.restoreCount(), "the worker mechanism must not restore the terminal")
			require.True(t, stream.isClosed(), "the supervisor closes the stream after the worker settles")
			require.Nil(t, host.authority.foreground(), "the foreground is released after the worker settles")
		})
	}
}

// TestAttachmentWorkerLateEventDiscardedOnCancellation proves a worker that
// settles after the supervisor cancelled is joined and its late event is
// discarded rather than returned as an adopted result.
func TestAttachmentWorkerLateEventDiscardedOnCancellation(t *testing.T) {
	term := newWorkerTestTerminal()
	host := newWorkerTestHost(term, nil, nil)
	stream := newWorkerTestStream()
	run, ok := host.Begin(context.Background(), AttachmentToken{Generation: 1, Attempt: 1}, blockingWorker(), stream)
	require.True(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	event, adopted := run.Wait(ctx)
	require.False(t, adopted)
	require.Equal(t, AttachmentEvent{}, event, "a late event from a canceled run is discarded")
	require.True(t, stream.isClosed())
	require.Nil(t, host.authority.foreground())
}

// TestAttachmentWorkerPreservesResidualInputAcrossAttempts proves the reused
// input pump keeps undecided bytes for the next authorized generation instead of
// duplicating a reader.
func TestAttachmentWorkerPreservesResidualInputAcrossAttempts(t *testing.T) {
	term := newWorkerTestTerminal()
	pr, pw := io.Pipe()
	pump := newTerminalInputPump(pr)
	pump.start()
	defer pump.stop()
	defer pw.Close()

	host := newWorkerTestHost(term, pump, nil)

	observed := make(chan []byte, 2)
	first := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		event, ok := fg.Input(ctx)
		if !ok {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: errors.New("first attempt saw no input")}
		}
		observed <- append([]byte(nil), event.Data...)
		// Leave the bytes undecided: the next attempt must see them again.
		fg.PreserveInput(event.Data)
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}
	second := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		event, ok := fg.Input(ctx)
		if !ok {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: errors.New("second attempt saw no input")}
		}
		fg.AckInput()
		observed <- append([]byte(nil), event.Data...)
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}

	go func() { _, _ = pw.Write([]byte("ab")) }()

	_, adopted := host.Run(context.Background(), AttachmentToken{Generation: 1, Attempt: 1}, first, newWorkerTestStream())
	require.True(t, adopted)
	_, adopted = host.Run(context.Background(), AttachmentToken{Generation: 2, Attempt: 1}, second, newWorkerTestStream())
	require.True(t, adopted)

	require.Equal(t, []byte("ab"), <-observed)
	require.Equal(t, []byte("ab"), <-observed, "the replacement generation replays the preserved bytes")
	go func() { _, _ = pw.Write([]byte("fresh")) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, adopted = host.Run(ctx, AttachmentToken{Generation: 3, Attempt: 3}, second, newWorkerTestStream())
	require.True(t, adopted)
	require.Equal(t, []byte("fresh"), <-observed, "third attempt must not replay the original pending read")

}

// TestAttachmentWorkerRevokesBeforeClosingStream proves the supervisor revokes
// input/output authority before it closes the owned stream: at the instant of
// Close the foreground is already released and a late write is refused.
func TestAttachmentWorkerRevokesBeforeClosingStream(t *testing.T) {
	term := newWorkerTestTerminal()
	host := newWorkerTestHost(term, nil, nil)
	stream := newWorkerTestStream()

	fgReady := make(chan AttachmentForeground, 1)
	worker := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		fgReady <- fg
		<-ctx.Done()
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}
	run, ok := host.Begin(context.Background(), AttachmentToken{Generation: 1, Attempt: 1}, worker, stream)
	require.True(t, ok)
	fg := <-fgReady

	var closeWriteErr error
	stream.closeHook = func() {
		require.Nil(t, host.authority.foreground(), "authority must be revoked before the stream closes")
		closeWriteErr = fg.Output(ports.UIContext{}, []byte("late"))
	}

	run.Cancel()
	require.ErrorIs(t, closeWriteErr, errAttachmentForegroundRevoked)
	require.Empty(t, term.written())
}

// TestAttachmentWorkerConcurrentBeginSingleOwner proves concurrent Begin calls
// admit exactly one foreground owner under the race detector.
func TestAttachmentWorkerConcurrentBeginSingleOwner(t *testing.T) {
	term := newWorkerTestTerminal()
	host := newWorkerTestHost(term, nil, nil)

	const contenders = 8
	var wg sync.WaitGroup
	var successes atomic.Int64
	runs := make(chan *attachmentRun, contenders)
	start := make(chan struct{})
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			run, ok := host.Begin(context.Background(), AttachmentToken{Generation: uint64(i + 1), Attempt: 1}, blockingWorker(), newWorkerTestStream())
			if ok {
				successes.Add(1)
				runs <- run
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(runs)

	require.Equal(t, int64(1), successes.Load(), "exactly one concurrent Begin may own the foreground")
	for run := range runs {
		run.Cancel()
	}
	require.Nil(t, host.authority.foreground())
}

// TestAttachmentWorkerRepeatedAttemptsNoLeak proves repeated grant/cancel cycles
// release the foreground, close every stream, and never leave a live owner.
func TestAttachmentWorkerRepeatedAttemptsNoLeak(t *testing.T) {
	term := newWorkerTestTerminal()
	host := newWorkerTestHost(term, nil, nil)

	const attempts = 64
	for i := 0; i < attempts; i++ {
		stream := newWorkerTestStream()
		worker := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
			if err := fg.Output(ports.UIContext{}, []byte("x")); err != nil {
				return AttachmentEvent{Kind: AttachmentEventFailed, Err: err}
			}
			return AttachmentEvent{Kind: AttachmentEventEnded}
		}}
		event, adopted := host.Run(context.Background(), AttachmentToken{Generation: uint64(i + 1), Attempt: 1}, worker, stream)
		require.True(t, adopted)
		require.Equal(t, AttachmentEventEnded, event.Kind)
		require.True(t, stream.isClosed())
		require.Nil(t, host.authority.foreground())
	}
	require.Equal(t, attempts, term.flushCount())
	require.Equal(t, 0, term.restoreCount(), "the mechanism never enters or restores raw mode")
}

func TestAttachmentWorkerRetiredOutcomeNeverAdopted(t *testing.T) {
	host := newWorkerTestHost(newWorkerTestTerminal(), nil, nil)
	run, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, blockingWorker(), newWorkerTestStream())
	require.True(t, ok)
	run.Cancel()
	for range 3 {
		event, adopted := run.Wait(context.Background())
		require.False(t, adopted)
		require.Zero(t, event)
	}
}

func TestAttachmentWorkerBoundedJoin(t *testing.T) {
	clock := newSupervisorTestClock()
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	pump := newTerminalInputPump(pr)
	pump.start()
	defer pump.stop()
	host := newAttachmentHost(attachmentHostConfig{Clock: clock, Input: pump})
	unblock := make(chan struct{})
	defer close(unblock)
	run, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, &fakeWorker{run: func(context.Context, AttachmentForeground) AttachmentEvent {
		<-unblock
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}, newWorkerTestStream())
	require.True(t, ok)
	cancelled := make(chan struct{})
	go func() { run.Cancel(); close(cancelled) }()
	timer := clock.awaitTimer(t)
	require.Equal(t, attachmentWorkerJoinTimeout, timer.delay)
	select {
	case <-run.retired:
	default:
		t.Fatal("retirement must precede join")
	}
	// During the final input window the grant is finalizing, not revoked: Done is
	// closed, output and new input are refused, the retained consumer still holds
	// the shared reader, and the stream is still open.
	require.Same(t, run.fg, host.authority.foreground())
	select {
	case <-run.fg.Done():
	default:
		t.Fatal("finalize must close Done before the join")
	}
	require.ErrorIs(t, run.fg.Output(ports.UIContext{}, []byte("late")), errAttachmentForegroundRevoked)
	_, ok = host.Begin(context.Background(), AttachmentToken{2, 1}, blockingWorker(), newWorkerTestStream())
	require.False(t, ok, "a finalizing grant blocks a competing Begin")
	_, claimed := pump.tryClaim()
	require.False(t, claimed, "the consumer is retained during the final input window")
	require.False(t, run.stream.(*workerTestStream).isClosed(), "the stream closes after the join")
	timer.fire()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("uncooperative worker pinned cancel")
	}
	require.True(t, timer.stopped())
	// After the timeout the supervisor revokes anyway: slot freed, consumer
	// released, and the stream closed.
	require.Nil(t, host.authority.foreground())
	consumer, claimed := pump.tryClaim()
	require.True(t, claimed, "revocation releases the consumer after the join")
	pump.revoke(consumer)
	require.True(t, run.stream.(*workerTestStream).isClosed())
	_, adopted := run.Wait(context.Background())
	require.False(t, adopted)
}

func TestAttachmentWorkerPumpClaimAndCancelRace(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	pump := newTerminalInputPump(pr)
	pump.start()
	defer pump.stop()
	host := newWorkerTestHost(newWorkerTestTerminal(), pump, nil)
	rival := pump.claim()
	_, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, blockingWorker(), newWorkerTestStream())
	require.False(t, ok)
	require.Nil(t, host.authority.foreground())
	pump.revoke(rival)
	for i := range 100 {
		run, ok := host.Begin(context.Background(), AttachmentToken{uint64(i + 1), 1}, blockingWorker(), newWorkerTestStream())
		require.True(t, ok)
		var next *attachmentRun
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); run.Cancel() }()
		go func() {
			defer wg.Done()
			next, _ = host.Begin(context.Background(), AttachmentToken{uint64(i + 2), 2}, blockingWorker(), newWorkerTestStream())
		}()
		wg.Wait()
		if next != nil {
			next.Cancel()
		}
		consumer, claimed := pump.tryClaim()
		require.True(t, claimed)
		pump.revoke(consumer)
	}
}

func TestAttachmentWorkerClosedResizeDisabled(t *testing.T) {
	term := newWorkerTestTerminal()
	close(term.resizes) // uiterm-like terminal: no physical resize events
	host := newWorkerTestHost(term, nil, nil)
	run, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, blockingWorker(), newWorkerTestStream())
	require.True(t, ok)
	result := make(chan bool, 1)
	go func() { _, ok := run.fg.Resize(context.Background()); result <- ok }()
	select {
	case <-result:
		t.Fatal("closed resize source terminated attachment")
	case <-time.After(10 * time.Millisecond):
	}
	require.True(t, run.fg.MarkAttached())
	run.Cancel()
	select {
	case ok := <-result:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("authority Done did not stop resize")
	}
}

func TestAttachmentWorkerOutputTransaction(t *testing.T) {
	failure := errors.New("output failure")
	for _, tc := range []struct {
		name                           string
		writeErr, flushErr, publishErr error
		shortWrite                     int
		wantErr                        error
		steps                          []string
	}{
		{name: "success", steps: []string{"begin", "publish", "write", "flush", "end"}},
		{name: "short write", shortWrite: 2, wantErr: io.ErrShortWrite, steps: []string{"begin", "publish", "write", "end"}},
		{name: "write failure", writeErr: failure, wantErr: failure, steps: []string{"begin", "publish", "write", "end"}},
		{name: "flush failure", flushErr: failure, wantErr: failure, steps: []string{"begin", "publish", "write", "flush", "end"}},
		{name: "publish failure", publishErr: failure, wantErr: failure, steps: []string{"begin", "publish", "end"}},
		{name: "publish unavailable tolerated", publishErr: ports.ErrUIUnavailable, steps: []string{"begin", "publish", "write", "flush", "end"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			term := newWorkerTestTerminal()
			term.writeErr, term.flushErr, term.publishErr, term.shortWrite = tc.writeErr, tc.flushErr, tc.publishErr, tc.shortWrite
			host := newWorkerTestHost(term, nil, nil)
			run, ok := host.Begin(context.Background(), AttachmentToken{9, 2}, blockingWorker(), newWorkerTestStream())
			require.True(t, ok)
			defer run.Cancel()
			err := run.fg.Output(ports.UIContext{Generation: 999}, []byte("frame"))
			success := tc.wantErr == nil
			if success {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Equal(t, tc.steps, term.steps)
			require.Equal(t, []bool{success}, term.successes)
			require.Equal(t, uint64(9), term.published[0].Generation)
		})
	}
}

// Hide the transaction methods to model a physical terminal without UI output.
type workerBareTerminal struct{ ports.Terminal }

func TestAttachmentWorkerNilSeams(t *testing.T) {
	var nilTerm *workerTestTerminal
	var nilWorker *fakeWorker
	var nilStream *workerTestStream
	var nilClock *supervisorTestClock
	for _, cfg := range []attachmentHostConfig{
		{},
		{Terminal: nilTerm, UI: nilTerm, Clock: nilClock},
		{Terminal: workerBareTerminal{newWorkerTestTerminal()}, UI: nilTerm},
	} {
		host := newAttachmentHost(cfg)
		_, ok := host.Begin(nil, AttachmentToken{1, 1}, nilWorker, newWorkerTestStream())
		require.False(t, ok)
		_, ok = host.Begin(nil, AttachmentToken{1, 1}, blockingWorker(), nilStream)
		require.False(t, ok)
		run, ok := host.Begin(nil, AttachmentToken{1, 1}, blockingWorker(), newWorkerTestStream())
		require.True(t, ok)
		require.ErrorIs(t, run.fg.Output(ports.UIContext{}, nil), ports.ErrUIUnavailable)
		run.Cancel()
		require.ErrorIs(t, run.fg.Output(ports.UIContext{}, nil), errAttachmentForegroundRevoked)
	}
}

// TestAttachmentWorkerGenerationIsSupervisorSupplied proves the host no longer
// allocates a UI generation: it grants and stamps the caller's generation, and
// the same value can be re-granted across sequential runs because the
// supervisor owns the counter. UI action binding stays deferred to the P5
// supervisor, so no UI admission is published here; the transactional output
// path still stamps the granted generation on every publication.
func TestAttachmentWorkerGenerationIsSupervisorSupplied(t *testing.T) {
	term := newWorkerTestTerminal()
	pump := newTerminalInputPump(nil)
	host := newWorkerTestHost(term, pump, nil)
	for i := uint64(1); i <= 3; i++ {
		run, ok := host.Begin(context.Background(), AttachmentToken{i, i + 10}, blockingWorker(), newWorkerTestStream())
		require.True(t, ok)
		require.Equal(t, AttachmentToken{i, i + 10}, run.fg.Token())
		require.NoError(t, run.fg.Output(ports.UIContext{Generation: 999}, nil))
		require.Equal(t, i, term.publications()[i-1].Generation)
		run.Cancel()
	}
}

// TestAttachmentWorkerBeginRefusalsRetainStream proves every Begin refusal path
// leaves the caller's stream untouched: it is not closed, receives no message,
// no foreground is granted, and the caller keeps ownership.
func TestAttachmentWorkerBeginRefusalsRetainStream(t *testing.T) {
	t.Run("nil host", func(t *testing.T) {
		var host *attachmentHost
		stream := newWorkerTestStream()
		run, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, blockingWorker(), stream)
		require.False(t, ok)
		require.Nil(t, run)
		require.False(t, stream.isClosed())
		require.Empty(t, stream.messages())
	})
	t.Run("zero token", func(t *testing.T) {
		host := newWorkerTestHost(newWorkerTestTerminal(), nil, nil)
		stream := newWorkerTestStream()
		_, ok := host.Begin(context.Background(), AttachmentToken{}, blockingWorker(), stream)
		require.False(t, ok)
		require.False(t, stream.isClosed())
		require.Empty(t, stream.messages())
		require.Nil(t, host.authority.foreground())
	})
	t.Run("nil worker", func(t *testing.T) {
		host := newWorkerTestHost(newWorkerTestTerminal(), nil, nil)
		stream := newWorkerTestStream()
		_, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, nil, stream)
		require.False(t, ok)
		require.False(t, stream.isClosed())
		require.Nil(t, host.authority.foreground())
	})
	t.Run("nil stream", func(t *testing.T) {
		host := newWorkerTestHost(newWorkerTestTerminal(), nil, nil)
		_, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, blockingWorker(), nil)
		require.False(t, ok)
		require.Nil(t, host.authority.foreground())
	})
	t.Run("foreground already live", func(t *testing.T) {
		host := newWorkerTestHost(newWorkerTestTerminal(), nil, nil)
		first, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, blockingWorker(), newWorkerTestStream())
		require.True(t, ok)
		stream := newWorkerTestStream()
		_, ok = host.Begin(context.Background(), AttachmentToken{2, 1}, blockingWorker(), stream)
		require.False(t, ok)
		require.False(t, stream.isClosed(), "a refused Begin must not close the caller's stream")
		require.Empty(t, stream.messages())
		require.Same(t, first.fg, host.authority.foreground())
		first.Cancel()
	})
	t.Run("rival consumer owns input", func(t *testing.T) {
		pump := newTerminalInputPump(nil)
		host := newWorkerTestHost(newWorkerTestTerminal(), pump, nil)
		rival := pump.claim()
		defer pump.revoke(rival)
		stream := newWorkerTestStream()
		_, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, blockingWorker(), stream)
		require.False(t, ok)
		require.False(t, stream.isClosed())
		require.Empty(t, stream.messages())
		require.Nil(t, host.authority.foreground())
	})
}

// TestAttachmentWorkerFinalizePreservesUndecidedInput proves the deterministic
// final input window: cancellation wakes the worker through Done while the pump
// consumer is still authorized for Ack/Preserve only, so the undecided read
// survives to the next generation exactly once, and a duplicate PreserveInput
// for the same delivery can never overwrite it.
func TestAttachmentWorkerFinalizePreservesUndecidedInput(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	pump := newTerminalInputPump(pr)
	pump.start()
	defer pump.stop()
	host := newAttachmentHost(attachmentHostConfig{Input: pump})

	var mu sync.Mutex
	var refusals []string
	refuse := func(what string) {
		mu.Lock()
		refusals = append(refusals, what)
		mu.Unlock()
	}
	delivered := make(chan []byte, 1)
	preserved := make(chan struct{})
	first := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		event, ok := fg.Input(ctx)
		if !ok {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: errors.New("first attempt saw no input")}
		}
		delivered <- append([]byte(nil), event.Data...)
		// The supervisor finalizes: Done wakes us while Ack/Preserve stay
		// authorized and every other action is refused.
		<-fg.Done()
		if _, ok := fg.Input(ctx); ok {
			refuse("new input")
		}
		if _, ok := fg.Resize(ctx); ok {
			refuse("resize")
		}
		if err := fg.Output(ports.UIContext{}, []byte("late")); !errors.Is(err, errAttachmentForegroundRevoked) {
			refuse("output")
		}
		fg.PreserveInput(event.Data)
		fg.PreserveInput([]byte("overwrite"))
		close(preserved)
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}

	written := make(chan error, 1)
	go func() { _, err := pw.Write([]byte("ab")); written <- err }()

	run, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, first, newWorkerTestStream())
	require.True(t, ok)
	select {
	case data := <-delivered:
		require.Equal(t, []byte("ab"), data)
	case <-time.After(time.Second):
		t.Fatal("first attempt never received input")
	}
	run.Cancel()
	select {
	case <-preserved:
	case <-time.After(time.Second):
		t.Fatal("worker was not woken to preserve input")
	}
	mu.Lock()
	require.Empty(t, refusals, "finalization must refuse every action except Ack/Preserve")
	mu.Unlock()
	require.NoError(t, <-written)
	require.Nil(t, host.authority.foreground())

	// The next generation replays the preserved bytes once, not the rejected
	// duplicate.
	observed := make(chan []byte, 2)
	second := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		event, ok := fg.Input(ctx)
		if !ok {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: errors.New("second attempt saw no input")}
		}
		fg.AckInput()
		observed <- append([]byte(nil), event.Data...)
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}
	_, adopted := host.Run(context.Background(), AttachmentToken{2, 1}, second, newWorkerTestStream())
	require.True(t, adopted)
	require.Equal(t, []byte("ab"), <-observed)

	// A third generation sees only fresh input: the preserved read was consumed
	// exactly once.
	freshWritten := make(chan error, 1)
	go func() { _, err := pw.Write([]byte("fresh")); freshWritten <- err }()
	_, adopted = host.Run(context.Background(), AttachmentToken{3, 1}, second, newWorkerTestStream())
	require.True(t, adopted)
	require.Equal(t, []byte("fresh"), <-observed)
	require.NoError(t, <-freshWritten)
}

func TestAttachmentWorkerRetirementPrecedesWorkerCancellation(t *testing.T) {
	host := newWorkerTestHost(newWorkerTestTerminal(), nil, nil)
	ready := make(chan *attachmentRun, 1)
	checked := make(chan bool, 1)
	worker := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		run := <-ready
		<-ctx.Done()
		select {
		case <-run.retired:
			checked <- true
		default:
			checked <- false
		}
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}
	run, ok := host.Begin(context.Background(), AttachmentToken{1, 1}, worker, newWorkerTestStream())
	require.True(t, ok)
	ready <- run
	run.Cancel()
	require.True(t, <-checked)
	_, adopted := run.Wait(context.Background())
	require.False(t, adopted)
}

func TestAttachmentWorkerBoundEventUsesGrantedGeneration(t *testing.T) {
	host := newAttachmentHost(attachmentHostConfig{Input: newTerminalInputPump(nil)})
	event, adopted := host.Run(context.Background(), AttachmentToken{99, 42}, &fakeWorker{run: func(context.Context, AttachmentForeground) AttachmentEvent {
		return AttachmentEvent{Token: AttachmentToken{999, 999}, Kind: AttachmentEventEnded}
	}}, newWorkerTestStream())
	require.True(t, adopted)
	require.Equal(t, AttachmentToken{99, 42}, event.Token)
}
