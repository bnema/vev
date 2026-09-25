// Physical pump over an abstract framed carrier.
//
// Pump is the in-memory physical half of one daemonmux connection. It
// composes the stateless directional codec (codec.go), the StreamEngine
// (engine_state.go), the outbound fair Scheduler (scheduler.go), and the
// publish-once terminalState (terminal.go) over a private FramedCarrier
// abstraction, and drives exactly one reader goroutine and one writer
// goroutine. It owns no socket, listener, sessionwire conversion, or stream
// framing: carriage framing is the carrier's, and the pump never imports a
// concrete physical implementation.
//
// The reader strict-decodes every inbound directional envelope with the
// negotiated MuxCeilings and applies it to the engine: Open/Data/Close/Reset
// when the inbound direction is DirectionClient (the local side is the
// daemon), Opened/Refused/Data/Close/Reset when it is DirectionServer (the
// local side is the broker client). The writer dequeues the scheduler's
// fairly ordered, already-encoded immutable envelopes and sends them
// verbatim; it never re-encodes, mutates, or reorders them. Between frames it
// blocks on a coalescing wake channel that every successful Send signals
// after enqueuing, so the writer never polls or sleeps and a lost wakeup is
// impossible: a signal raised while the writer drains is re-tested by the
// next dequeue. Flush is the explicit write barrier: it waits until the
// scheduler is empty and no send is in flight (or the context or the
// connection ends), because an accepted Send only means the frame is queued,
// never that it reached the carrier. Close is not a flush; a caller that must
// publish a final frame flushes first.
//
// The reader retires the outbound half of a stream the peer terminalizes: a
// successful inbound Close or Reset resets the matching scheduler record, so
// queued outbound frames are discarded rather than sent after the peer
// stopped reading. A queue overflow in the outbound scheduler is settled
// symmetrically: the overflowing stream's engine record is reset to free its
// admission slot and exactly one terminal Reset is scheduled for the peer.
//
// Failures are split into two classes, exactly as the engine does. A
// malformed outer envelope (a strict-scan, direction, or bound refusal of a
// complete payload) and any carrier Receive/Send error are physical-fatal:
// the pump publishes its own terminal state first, then the engine's, so
// Done closes before every stream is terminalized, and the carrier is closed
// to unblock the peer goroutine. A stream-local semantic, reference, or queue
// refusal (an engine error that leaves the physical connection healthy)
// schedules exactly one Reset for the offending stream, settles that stream
// locally, and lets every sibling keep flowing: one stream's refusal never
// terminalizes the physical connection or another stream.
//
// Cancellation is an orderly close. Cancelling the context passed to Start
// (the carrier returns the context error from its blocked Send or Receive) or
// calling Close stops both goroutines, closes the carrier, joins the reader
// and writer, and publishes an orderly outcome: a nil Err and
// RemoteFailureNone. A physical failure that already won keeps its cause; the
// pump's terminal authority and the engine's are both first-wins, and the
// pump serializes its two publications so they can never disagree. Close
// unblocks an in-flight carrier Send or Receive through the adapter
// prompt-close contract and joins both goroutines bounded by it: a carrier
// whose Close promptly releases its blocked I/O lets Close return as soon as
// the reader and writer observe the release.
//
// The pump itself performs no admission and no outbound encoding policy: a
// locally initiated stream is admitted through Engine (Open) and confirmed
// through Engine (Opened) by its owner before Send queues the matching frame,
// exactly as the scheduler's admission gate requires.
package daemonmux

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// FramedCarrier is the private physical carriage one Pump drives: one
// complete framed application envelope in each direction over one connection.
// Send writes one immutable envelope and reports the write result; it must not
// retain or mutate the payload. Receive blocks for the next complete envelope;
// it returns the receiver's own copy. Both must return promptly once their
// context is done, which is what makes context cancellation an orderly close
// without a concurrent Close. Close is safe to call concurrently with Send,
// Receive, and itself, and it must unblock an in-flight Send or Receive
// promptly: that is the adapter prompt-close contract Pump.Close relies on to
// join both goroutines in bounded time.
type FramedCarrier interface {
	Send(ctx context.Context, payload []byte) error
	Receive(ctx context.Context) ([]byte, error)
	Close() error
}

// ErrPumpConfig reports a pump constructed without a carrier or with an
// unknown inbound direction.
var ErrPumpConfig = errors.New("daemonmux: invalid pump configuration")

// ErrDataNeedsCredit reports a Data frame handed to Send: stream data must go
// through SendData so it is sent under the stream's flow-control credit.
var ErrDataNeedsCredit = errors.New("daemonmux: stream data must be sent with SendData")

// ErrAdmissionObserverLate reports an admission-observer registration attempted
// after Start. The reader goroutine is already running and may have admitted an
// inbound Open, so a late observer would silently miss it; a consumer must
// register before Start.
var ErrAdmissionObserverLate = errors.New("daemonmux: admission observer registered after start")

// Pump is the physical pump of one daemonmux connection. It is safe for
// concurrent use: the engine and scheduler it owns are themselves
// concurrency-safe, and Send, Close, Start, and the accessors may be called
// from any goroutine. It performs no stream framing and owns no socket,
// listener, or session conversion.
type Pump struct {
	carrier   FramedCarrier
	engine    *StreamEngine
	scheduler *Scheduler
	ceilings  MuxCeilings
	inbound   EnvelopeDirection
	local     EnvelopeDirection
	terminal  *terminalState

	// wake coalesces outbound wakeups: one pending token means the writer
	// must re-check the scheduler, and a signal raised while a token is
	// already pending is redundant rather than lost.
	wake chan struct{}

	// flushMu guards the flush bookkeeping. inflight counts frames the writer
	// has dequeued but not finished sending, and flushCh is closed and replaced
	// whenever the pump may have become idle (no in-flight send and an empty
	// scheduler), so Flush can wait for the transition without polling. The
	// scheduler lock is always taken after flushMu, never before it.
	flushMu  sync.Mutex
	inflight int
	flushCh  chan struct{}

	// watchMu guards the per-stream inbound watch table. Each admitted stream
	// a consumer is watching owns one channel that the reader closes and
	// replaces whenever it applies an inbound frame for that stream, so a
	// consumer can block for new inbound data or a stream state change without
	// polling. The table only holds entries for streams a consumer created with
	// Watch and are dropped again by ReleaseWatch, so it stays bounded by the
	// number of live logical connections rather than by every ID ever opened.
	watchMu sync.Mutex
	watch   map[PhysicalStreamID]chan struct{}

	// admitMu guards admitFn, the optional inbound-admission observer. The
	// reader reads it on every admitted Open; its owner sets it before Start.
	admitMu sync.Mutex
	admitFn func(Open)

	mu      sync.Mutex
	started bool
	closed  bool
	cancel  context.CancelFunc

	startOnce sync.Once
	closeOnce sync.Once
	stopOnce  sync.Once

	// termMu serializes the pump-level and engine-level terminal
	// publications so both always carry the same outcome and only the first
	// publisher applies.
	termMu  sync.Mutex
	settled bool

	readerDone chan struct{}
	writerDone chan struct{}
	closeErr   error
}

// NewPump returns a pump over carrier that reads inbound envelopes of the
// given direction and writes the opposite one. The negotiated MuxCeilings are
// validated and copied: they build the engine with NewStreamEngineWithCeilings
// and the scheduler (gated on that engine) with NewSchedulerWithCeilings, so
// the pump enforces exactly the ceilings one physical connection negotiated
// for its lifetime. An invalid advertisement is refused with ErrInvalidCeilings
// and a missing carrier or unknown direction with ErrPumpConfig. Goroutines
// start on Start.
func NewPump(carrier FramedCarrier, inbound EnvelopeDirection, ceilings MuxCeilings) (*Pump, error) {
	if carrier == nil {
		return nil, ErrPumpConfig
	}
	if inbound != DirectionClient && inbound != DirectionServer {
		return nil, ErrPumpConfig
	}
	if err := ceilings.Validate(); err != nil {
		return nil, err
	}
	engine, err := NewStreamEngineWithCeilings(ceilings)
	if err != nil {
		return nil, err
	}
	scheduler, err := NewSchedulerWithCeilings(engine, ceilings)
	if err != nil {
		return nil, err
	}
	return &Pump{
		carrier:    carrier,
		engine:     engine,
		scheduler:  scheduler,
		ceilings:   ceilings,
		inbound:    inbound,
		local:      oppositeDirection(inbound),
		terminal:   newTerminalState(),
		wake:       make(chan struct{}, 1),
		flushCh:    make(chan struct{}),
		readerDone: make(chan struct{}),
		writerDone: make(chan struct{}),
	}, nil
}

// Start launches the reader and writer goroutines under ctx. It is
// idempotent: only the first call takes effect, so a later call with a
// different context is ignored rather than starting a second pair of
// goroutines. A nil context is replaced with context.Background. Start after
// Close does nothing, because the connection already reached its terminal
// outcome.
func (p *Pump) Start(ctx context.Context) {
	if p == nil {
		return
	}
	p.startOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		runCtx, cancel := context.WithCancel(ctx)
		p.cancel = cancel
		p.started = true
		p.mu.Unlock()
		go p.readLoop(runCtx)
		go p.writeLoop(runCtx)
	})
}

// Close orderly closes the pump: it cancels the run context, closes the
// carrier through the adapter prompt-close contract to unblock the reader and
// writer, joins both goroutines, and publishes an orderly terminal outcome
// unless a physical failure already won. It returns the carrier's Close error.
// Close is idempotent and safe to call concurrently; every caller observes the
// same result. A Close before Start never starts goroutines and never joins.
//
// Close is not a flush: it cancels the writer rather than draining it, so
// frames accepted by Send but not yet written are dropped. A caller that must
// publish a final frame (for example the last Close) calls Flush before Close;
// a caller that races Close against the writer may lose the queued frame.
func (p *Pump) Close() error {
	if p == nil {
		return ErrPumpConfig
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		started := p.started
		p.mu.Unlock()
		p.stop()
		if started {
			<-p.readerDone
			<-p.writerDone
		}
		p.settleOrderly()
	})
	return p.closeErr
}

// Done returns the pump's terminal channel: closed exactly once, before every
// stream of the connection is terminalized.
func (p *Pump) Done() <-chan struct{} {
	if p == nil || p.terminal == nil {
		return nil
	}
	return p.terminal.Done()
}

// Err returns the pump's terminal cause: nil while open and for an orderly
// Close or context cancellation.
func (p *Pump) Err() error {
	if p == nil || p.terminal == nil {
		return nil
	}
	return p.terminal.Err()
}

// FailureKind returns the pump's terminal classification: RemoteFailureNone
// while open and for an orderly Close.
func (p *Pump) FailureKind() domain.RemoteFailureKind {
	if p == nil || p.terminal == nil {
		return domain.RemoteFailureNone
	}
	return p.terminal.FailureKind()
}

// Engine returns the stream engine the pump drives. The reader applies every
// inbound envelope to it, and a stream's owner uses it to admit a locally
// initiated stream and to consume inbound chunks with Take.
func (p *Pump) Engine() *StreamEngine {
	if p == nil {
		return nil
	}
	return p.engine
}

// Take removes the oldest queued inbound chunk of one stream. It is the
// pump's convenience delegation to the engine and reports false when the
// stream is unknown or its queue is empty. Draining the last queued chunk of a
// stream that already received an orderly Close terminalizes it here, and the
// pump rotates that stream's watch channel so a consumer blocked on the queue
// transition wakes and observes the end instead of waiting for a frame that
// can never arrive.
func (p *Pump) Take(physical PhysicalStreamID) ([]byte, bool) {
	if p == nil || p.engine == nil {
		return nil, false
	}
	chunk, ok, finalized := p.engine.take(physical)
	if finalized {
		p.signalWatch(physical)
	}
	if ok {
		p.returnCredit(physical)
	}
	return chunk, ok
}

// returnCredit grants consumed inbound credit back to the peer once a batch
// is due. A refusal for a settled stream is dropped; an aggregate overflow
// resets the stream like any outbound overflow, because the lost grant would
// otherwise leave the peer's writer short of credit forever.
func (p *Pump) returnCredit(physical PhysicalStreamID) {
	grant := p.engine.takeReturn(physical)
	if grant == 0 || p.isTerminal() {
		return
	}
	_ = p.enqueue(WindowUpdate{Physical: physical, Credit: grant})
}

// SendData sends one stream data chunk under the stream's flow-control
// credit. It blocks while the peer has not granted enough credit, so a slow
// network or a slow remote consumer slows the writer instead of resetting the
// stream. It returns when the chunk is queued, the stream or the connection
// settles, or cancel closes.
func (p *Pump) SendData(physical PhysicalStreamID, chunk []byte, cancel <-chan struct{}) error {
	if p == nil {
		return ErrPumpConfig
	}
	for {
		if p.isTerminal() {
			return ErrPhysicalClosed
		}
		ok, wake, err := p.engine.reserveSend(physical, len(chunk))
		if err != nil {
			return err
		}
		if ok {
			break
		}
		select {
		case <-wake:
		case <-cancel:
			return ErrLogicalClosed
		case <-p.Done():
			return ErrPhysicalClosed
		}
	}
	if p.isTerminal() {
		p.engine.refundSend(physical, len(chunk))
		return ErrPhysicalClosed
	}
	if err := p.enqueue(Data{Physical: physical, Data: chunk}); err != nil {
		p.engine.refundSend(physical, len(chunk))
		return err
	}
	return nil
}

// Ceilings returns the negotiated ceilings the pump enforces. The value is
// immutable for the pump's lifetime.
func (p *Pump) Ceilings() MuxCeilings {
	if p == nil {
		return MuxCeilings{}
	}
	return p.ceilings
}

// Inbound returns the envelope direction the pump's reader decodes.
func (p *Pump) Inbound() EnvelopeDirection {
	if p == nil {
		return 0
	}
	return p.inbound
}

// Local returns the envelope direction the pump's writer emits: the opposite
// of Inbound.
func (p *Pump) Local() EnvelopeDirection {
	if p == nil {
		return 0
	}
	return p.local
}

// OnAdmitted registers fn to observe every inbound logical stream the pump's
// reader admits: it runs once per admitted Open, immediately after the engine
// accepted it and before any later frame for that stream can be applied (and
// before the opening side is confirmed with Opened). It carries the decoded
// admission request, which the reader never mutates afterwards.
//
// fn runs on the reader goroutine, so it must not block: it is the one
// goroutine that applies every inbound frame of the physical connection. The
// daemon-side Listener registers its accept-queue admission here. Registering
// a second observer replaces the first - one physical connection has exactly
// one daemon-side admission consumer - and a nil fn clears it.
//
// The observer must be registered before the first inbound Open, that is before
// Start: a registration attempted on a pump that already started fails with
// ErrAdmissionObserverLate rather than silently missing the Opens the reader
// may already have admitted. A nil pump fails with ErrPumpConfig.
func (p *Pump) OnAdmitted(fn func(Open)) error {
	if p == nil {
		return ErrPumpConfig
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return ErrAdmissionObserverLate
	}
	p.admitMu.Lock()
	p.admitFn = fn
	p.admitMu.Unlock()
	return nil
}

// notifyAdmitted runs the registered admission observer, if any.
func (p *Pump) notifyAdmitted(message Open) {
	p.admitMu.Lock()
	fn := p.admitFn
	p.admitMu.Unlock()
	if fn != nil {
		fn(message)
	}
}

// Watch returns a channel the pump's reader closes and replaces whenever it
// applies an inbound frame for the physical stream: Open, Opened, Refused,
// Data, Close, or Reset. A consumer blocks on it to wait for new inbound data
// or a stream state change without polling. The channel is stable until the
// next applied frame, so a consumer must re-read the stream after every wake
// and must also select on the stream's terminal channel and the connection's
// Done to observe a terminal outcome. A zero, unknown, or evicted identity,
// and a stream that already reached its terminal state, return an
// already-closed channel and retain no watch entry, so the watch table stays
// bounded by the number of live logical connections. Watch is safe for
// concurrent use and the same channel is returned to every caller until the
// next applied frame.
func (p *Pump) Watch(physical PhysicalStreamID) <-chan struct{} {
	if p == nil || p.engine == nil || physical.Validate() != nil {
		return closedChannel()
	}
	p.watchMu.Lock()
	defer p.watchMu.Unlock()
	if status, ok := p.engine.Status(physical); !ok || status.State == StreamTerminal {
		// No further frame can ever be applied for this stream: drop any stale
		// entry and hand back a closed channel instead of retaining a watch
		// nobody will ever signal.
		delete(p.watch, physical)
		return closedChannel()
	}
	if ch, ok := p.watch[physical]; ok {
		return ch
	}
	ch := make(chan struct{})
	if p.watch == nil {
		p.watch = make(map[PhysicalStreamID]chan struct{})
	}
	p.watch[physical] = ch
	return ch
}

// cause returns the retained publish-once terminal authority of one admitted
// stream, or nil when the pump is unset or the identity was never admitted.
// The pointer outlives the engine's bounded record, so a stream's owner
// captures it at open and keeps reporting the exact terminal cause even after
// eviction.
func (p *Pump) cause(physical PhysicalStreamID) *terminalState {
	if p == nil || p.engine == nil {
		return nil
	}
	cause, _ := p.engine.cause(physical)
	return cause
}

// ReleaseWatch drops the watch channel of one terminal or abandoned physical
// stream so the watch table stays bounded by the number of live logical
// connections. It is safe to call concurrently with Watch; a later Watch of
// an already-terminal stream returns a closed channel without retaining an
// entry.
func (p *Pump) ReleaseWatch(physical PhysicalStreamID) {
	if p == nil {
		return
	}
	p.watchMu.Lock()
	defer p.watchMu.Unlock()
	delete(p.watch, physical)
}

// signalWatch closes and replaces the current watch channel of one stream so
// every blocked watcher wakes and re-reads the engine. It is a no-op for a
// stream nobody watches. The caller must not hold the watch lock, and the
// engine lock is never held when it runs.
func (p *Pump) signalWatch(physical PhysicalStreamID) {
	if p == nil || physical.Validate() != nil {
		return
	}
	p.watchMu.Lock()
	defer p.watchMu.Unlock()
	ch, ok := p.watch[physical]
	if !ok {
		return
	}
	close(ch)
	p.watch[physical] = make(chan struct{})
}

// closedChannel returns a shared, already-closed channel for watchers of an
// unknown or terminal stream.
var closedWatchChan = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

func closedChannel() <-chan struct{} { return closedWatchChan }

// watchClosed reports whether a channel returned by Watch is already closed,
// which means its stream is terminal or unknown and can never receive another
// applied frame.
func watchClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Send encodes one outbound message with the negotiated ceilings, queues it
// on the fair scheduler, and wakes the writer. The message's direction must
// match the pump's local side: DirectionClient accepts ClientMessage and
// DirectionServer accepts ServerMessage; the shared Data, Close, and Reset
// values satisfy both. A wrong-direction value fails with ErrWrongDirection
// before anything is encoded or queued, a message the codec refuses propagates
// the codec's error, and a pump that already reached its terminal outcome
// fails with ErrPhysicalClosed. Data is refused with ErrDataNeedsCredit:
// stream data goes through SendData. On success the encoded bytes are
// immutable and owned by the writer.
//
// An accepted Send only means the frame is queued, never that it reached the
// wire: the writer drains asynchronously. Call Flush to wait until every
// accepted frame has been handed to the carrier.
//
// A stream-local scheduler refusal propagates the scheduler's error. A queue
// overflow is additionally isolated: the stream's engine record is reset to
// free its admission slot and exactly one terminal Reset is scheduled so the
// peer learns the stream was aborted; the caller still observes the overflow
// error for the frame it tried to send.
func (p *Pump) Send(message any) error {
	if p == nil {
		return ErrPumpConfig
	}
	if message == nil {
		return ErrInvalidMessage
	}
	if _, ok := message.(Data); ok {
		return ErrDataNeedsCredit
	}
	if p.isTerminal() {
		return ErrPhysicalClosed
	}
	return p.enqueue(message)
}

// enqueue queues one validated outbound message on the scheduler in the
// pump's local direction and isolates a stream-local overflow.
func (p *Pump) enqueue(message any) error {
	switch p.local {
	case DirectionClient:
		client, ok := message.(ClientMessage)
		if !ok {
			return ErrWrongDirection
		}
		physical, _, err := clientFrame(client)
		if err != nil {
			return err
		}
		return p.admitOutbound(physical, func() error { return p.scheduler.EnqueueClient(client) })
	case DirectionServer:
		server, ok := message.(ServerMessage)
		if !ok {
			return ErrWrongDirection
		}
		physical, _, err := serverFrame(server)
		if err != nil {
			return err
		}
		return p.admitOutbound(physical, func() error { return p.scheduler.EnqueueServer(server) })
	default:
		return ErrPumpConfig
	}
}

// Flush blocks until every frame the scheduler accepted has been handed to the
// carrier and no send is in flight, or until ctx is done. Send accepted means
// queued only, so Flush is the explicit barrier that makes a frame observable
// on the wire before the connection is closed. It returns ctx.Err() when the
// deadline or cancellation wins, and the pump's terminal outcome when the
// connection became terminal before the scheduler drained (a physical failure
// carries its cause; an orderly Close carries ErrPhysicalClosed when frames
// were still queued). It never polls: the writer signals an idle transition.
func (p *Pump) Flush(ctx context.Context) error {
	if p == nil {
		return ErrPumpConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		p.flushMu.Lock()
		if p.inflight == 0 && !p.scheduler.Pending() {
			p.flushMu.Unlock()
			return nil
		}
		wait := p.flushCh
		p.flushMu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		case <-p.terminal.Done():
			p.flushMu.Lock()
			idle := p.inflight == 0 && !p.scheduler.Pending()
			p.flushMu.Unlock()
			if idle {
				return nil
			}
			if err := p.Err(); err != nil {
				return err
			}
			return ErrPhysicalClosed
		}
	}
}

// admitOutbound queues one already-typed outbound frame, isolates a queue
// overflow, and wakes the writer only when the scheduler accepted the frame,
// so a refused frame never produces a spurious wakeup.
func (p *Pump) admitOutbound(physical PhysicalStreamID, admit func() error) error {
	if err := admit(); err != nil {
		p.handleOutboundRefusal(physical, err)
		return err
	}
	p.signal()
	return nil
}

// handleOutboundRefusal isolates one refused outbound frame. A queue overflow
// is settled exactly like an inbound stream-local refusal: the stream's engine
// record is reset to free its admission slot and exactly one terminal Reset is
// scheduled towards the peer. Every other refusal (a settled or unadmitted
// identity) is left untouched.
func (p *Pump) handleOutboundRefusal(physical PhysicalStreamID, err error) {
	if errors.Is(err, ErrSchedulerFull) {
		p.scheduleReset(physical, err)
	}
}

// signalFlushLocked broadcasts one possible idle transition to every Flush
// waiter by closing the current channel and installing a fresh one. The caller
// holds flushMu.
func (p *Pump) signalFlushLocked() {
	if p.flushCh != nil {
		close(p.flushCh)
	}
	p.flushCh = make(chan struct{})
}

// maybeSignalFlush broadcasts an idle transition when the scheduler is empty
// and no send is in flight, so a pending Flush can return. It is called after
// the writer finishes a send and after a scheduler reset discards queued
// frames.
func (p *Pump) maybeSignalFlush() {
	p.flushMu.Lock()
	if p.inflight == 0 && !p.scheduler.Pending() {
		p.signalFlushLocked()
	}
	p.flushMu.Unlock()
}

// signal raises one coalesced writer wakeup without ever blocking.
func (p *Pump) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// readLoop is the reader goroutine: it receives one complete framed envelope
// at a time, strict-decodes the inbound direction, and applies it to the
// engine. A carrier Receive error that is not the local cancellation is
// physical-fatal; so is a malformed outer envelope. Either way Done closes
// before any stream is terminalized and the peer goroutine is unblocked.
func (p *Pump) readLoop(ctx context.Context) {
	defer p.settleOrderly()
	defer close(p.readerDone)
	for {
		if ctx.Err() != nil {
			return
		}
		payload, err := p.carrier.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.settleFailure(domain.RemoteFailureTransport, err)
			return
		}
		if fatal := p.handleInbound(payload); fatal != nil {
			p.settleFailure(domain.RemoteFailureInvalidResponse, fatal)
			return
		}
	}
}

// writeLoop is the writer goroutine: it dequeues the scheduler's fairly
// ordered immutable envelopes and sends them verbatim, then blocks on the
// coalescing wake channel until more work arrives or the run context ends. It
// never polls, sleeps, or reorders.
func (p *Pump) writeLoop(ctx context.Context) {
	defer p.settleOrderly()
	defer close(p.writerDone)
	for {
		p.flushMu.Lock()
		envelope, ok := p.scheduler.Dequeue()
		if ok {
			p.inflight++
		}
		p.flushMu.Unlock()
		if ok {
			err := p.carrier.Send(ctx, envelope.Bytes)
			p.flushMu.Lock()
			if p.inflight > 0 {
				p.inflight--
			}
			idle := p.inflight == 0 && !p.scheduler.Pending()
			if idle {
				p.signalFlushLocked()
			}
			p.flushMu.Unlock()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				p.settleFailure(domain.RemoteFailureTransport, err)
				return
			}
			continue
		}
		// The scheduler is empty and no send is in flight: release any Flush
		// waiter before blocking on the coalescing wake channel.
		p.maybeSignalFlush()
		select {
		case <-p.wake:
		case <-ctx.Done():
			return
		}
	}
}

// handleInbound strict-decodes one complete inbound payload in the pump's
// direction and applies it to the engine. It returns the physical-fatal cause
// of a malformed outer envelope, or nil when the payload was applied, when a
// stream-local refusal was isolated with one Reset, or when the physical
// connection was already terminal.
func (p *Pump) handleInbound(payload []byte) error {
	switch p.inbound {
	case DirectionClient:
		message, err := DecodeClient(payload, p.ceilings.MaxReceiveEnvelopeBytes, p.ceilings.StreamChunkLimit)
		if err != nil {
			return err
		}
		return p.applyClient(message)
	case DirectionServer:
		message, err := DecodeServer(payload, p.ceilings.MaxReceiveEnvelopeBytes, p.ceilings.StreamChunkLimit)
		if err != nil {
			return err
		}
		return p.applyServer(message)
	default:
		return ErrPumpConfig
	}
}

// applyClient applies one broker-to-daemon message to the engine.
func (p *Pump) applyClient(message ClientMessage) error {
	switch m := message.(type) {
	case Open:
		return p.applyOpen(m)
	case *Open:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyClient(*m)
	case Data:
		return p.applyData(m)
	case *Data:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyData(*m)
	case Close:
		disposition, err := p.engine.Close(m)
		return p.applyStreamTerminal(m.Physical, disposition, err)
	case *Close:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyClient(*m)
	case Reset:
		disposition, err := p.engine.Reset(m)
		p.logPeerReset(m, disposition)
		return p.applyStreamTerminal(m.Physical, disposition, err)
	case *Reset:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyClient(*m)
	case WindowUpdate:
		return p.applyWindowUpdate(m)
	case *WindowUpdate:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyClient(*m)
	default:
		return ErrWrongDirection
	}
}

// applyOpen admits one incoming logical stream and notifies the admission
// observer exactly when the engine accepted it. A refusal never notifies: a
// stream that was never admitted has no daemon-side consumer. The observer runs
// on the reader goroutine, between admission and any later frame for that
// stream.
//
// The two refusal classes are deliberately different. A duplicate or replayed
// Open (an identity at or below the high-water mark the engine already owns)
// is a protocol violation that must not disturb the stream it names: it is
// ignored, so it neither resets nor alters the existing live or retired stream,
// and it never produces an outbound frame. A fresh Open the engine refused
// before recording anything (capacity, a closed physical connection, or an
// invalid authority) is signalled with exactly one typed Mux Refused carrying
// its admission code; no stream is recorded for it, and a sibling continues
// unaffected.
func (p *Pump) applyOpen(message Open) error {
	err := p.engine.Open(message)
	switch {
	case err == nil:
		p.notifyAdmitted(message)
		return nil
	case errors.Is(err, ErrStreamIDReused):
		return nil
	default:
		p.refuseFreshOpen(message.Ref, err)
		return nil
	}
}

// refuseFreshOpen signals the peer that one fresh Open was refused before the
// engine admitted anything. It schedules exactly one typed Mux Refused through
// the scheduler's fresh-refusal exception, because the engine recorded no
// stream to confirm, and it records no engine stream itself. A pump that
// already reached its terminal outcome, or a Refused the scheduler refuses,
// leaves the peer unsignalled rather than faulting the shared physical
// connection.
func (p *Pump) refuseFreshOpen(ref StreamRef, cause error) {
	if p == nil || p.isTerminal() {
		return
	}
	refused := Refused{Ref: ref, Error: openRefusalDetail(cause)}
	if err := p.scheduler.Refuse(refused); err != nil {
		return
	}
	p.signal()
}

// applyServer applies one daemon-to-broker message to the engine.
func (p *Pump) applyServer(message ServerMessage) error {
	switch m := message.(type) {
	case Opened:
		return p.applyStreamError(m.Ref.Physical, p.engine.Opened(m))
	case *Opened:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyServer(*m)
	case Refused:
		return p.applyStreamError(m.Ref.Physical, p.engine.Refused(m))
	case *Refused:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyServer(*m)
	case Data:
		return p.applyData(m)
	case *Data:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyData(*m)
	case Close:
		disposition, err := p.engine.Close(m)
		return p.applyStreamTerminal(m.Physical, disposition, err)
	case *Close:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyServer(*m)
	case Reset:
		disposition, err := p.engine.Reset(m)
		p.logPeerReset(m, disposition)
		return p.applyStreamTerminal(m.Physical, disposition, err)
	case *Reset:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyServer(*m)
	case WindowUpdate:
		return p.applyWindowUpdate(m)
	case *WindowUpdate:
		if m == nil {
			return ErrInvalidMessage
		}
		return p.applyServer(*m)
	default:
		return ErrWrongDirection
	}
}

// applyStreamTerminal applies one inbound Close or Reset. When the engine
// accepted it, the matching outbound scheduler record is reset so queued
// outbound frames for the peer-terminalized stream are discarded rather than
// sent after the peer stopped reading it, and the record is retired. A refused
// terminal falls back to the stream-local refusal path.
func (p *Pump) applyStreamTerminal(physical PhysicalStreamID, disposition StreamDisposition, err error) error {
	p.signalWatch(physical)
	if err != nil {
		return p.applyStreamError(physical, err)
	}
	if disposition == StreamAccepted {
		p.scheduler.Reset(physical)
		p.maybeSignalFlush()
	}
	return nil
}

// logPeerReset records one stream the peer aborted, with the reason it sent.
// A late reset for an already settled stream is not logged.
func (p *Pump) logPeerReset(message Reset, disposition StreamDisposition) {
	if disposition != StreamAccepted || !message.HasError {
		return
	}
	slog.Warn("daemonmux_stream_reset", "side", p.local.String(), "stream", uint64(message.Physical), "initiator", "peer", "kind", message.Error.FailureKind.String(), "code", message.Error.Code.String(), "cause", message.Error.Text)
}

// applyWindowUpdate grows one stream's send credit. A credit violation resets
// only that stream.
func (p *Pump) applyWindowUpdate(message WindowUpdate) error {
	_, err := p.engine.WindowUpdate(message)
	if err == nil {
		return nil
	}
	return p.applyStreamError(message.Physical, err)
}

// applyData queues one inbound chunk and classifies the engine's refusal.
func (p *Pump) applyData(message Data) error {
	_, err := p.engine.Data(message)
	return p.applyStreamError(message.Physical, err)
}

// applyStreamError classifies one engine refusal. A stream-local refusal
// schedules exactly one Reset for the offending stream and returns nil so the
// reader continues; the physical connection stays open. ErrPhysicalClosed only
// occurs once the physical connection is already terminal, so the reader stops
// without a second outcome. A nil refusal applies nothing.
func (p *Pump) applyStreamError(physical PhysicalStreamID, err error) error {
	p.signalWatch(physical)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrPhysicalClosed) {
		return nil
	}
	p.scheduleReset(physical, err)
	return nil
}

// scheduleReset isolates one stream-local refusal: it settles the offending
// stream locally, freeing its admission slot, and schedules exactly one Reset
// frame towards the peer. An identity the engine never admitted cannot be
// signalled and is left alone. The engine settlement is always applied first
// and unconditionally, so the slot is freed even when the terminal frame
// cannot be scheduled; the scheduler enqueue result decides only whether the
// peer can be signalled, so a stream that already drained its one Reset is not
// reset twice and a repeated refusal stays idempotent. Siblings and the
// physical connection are untouched.
func (p *Pump) scheduleReset(physical PhysicalStreamID, cause error) {
	if physical.Validate() != nil {
		return
	}
	if _, known := p.engine.Status(physical); !known {
		return
	}
	detail := resetErrorDetail(cause)
	reset := Reset{Physical: physical, Error: detail, HasError: true}
	slog.Warn("daemonmux_stream_reset", "side", p.local.String(), "stream", uint64(physical), "initiator", "local", "kind", detail.FailureKind.String(), "cause", cause)
	_, _ = p.engine.Reset(reset)
	var err error
	if p.local == DirectionServer {
		err = p.scheduler.EnqueueServer(reset)
	} else {
		err = p.scheduler.EnqueueClient(reset)
	}
	if err != nil {
		// The peer cannot be signalled (the record is sealed or the identity
		// is not schedulable); the stream is already settled locally.
		return
	}
	p.signal()
}

// settleFailure publishes one physical failure outcome: the pump's terminal
// state first, then the engine's, so Done closes before every stream is
// terminalized, and then closes the carrier to unblock the peer goroutine.
// The two publications are serialized and first-wins, so a concurrent orderly
// close or a second failure can never make them disagree.
func (p *Pump) settleFailure(kind domain.RemoteFailureKind, cause error) {
	p.termMu.Lock()
	if !p.settled {
		p.settled = true
		slog.Warn("daemonmux_physical_failed", "side", p.local.String(), "kind", kind.String(), "live_streams", p.engine.Live(), "cause", cause)
		p.terminal.Fail(kind, cause)
		p.engine.Fail(kind, cause)
	}
	p.termMu.Unlock()
	p.stop()
}

// settleOrderly publishes the orderly local outcome unless a failure already
// won, then closes the carrier. The pump's terminal state is published before
// the engine's, exactly as a failure publishes it.
func (p *Pump) settleOrderly() {
	p.termMu.Lock()
	if !p.settled {
		p.settled = true
		p.terminal.Close()
		p.engine.Terminate()
	}
	p.termMu.Unlock()
	p.stop()
}

// stop cancels the run context exactly once and closes the carrier exactly
// once, unblocking an in-flight Send or Receive so both goroutines can exit.
func (p *Pump) stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		cancel := p.cancel
		p.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		p.closeErr = p.carrier.Close()
	})
}

// isTerminal reports whether the pump already reached its terminal outcome.
func (p *Pump) isTerminal() bool {
	select {
	case <-p.terminal.Done():
		return true
	default:
		return false
	}
}

// oppositeDirection returns the direction a pump writes when it reads inbound.
func oppositeDirection(inbound EnvelopeDirection) EnvelopeDirection {
	if inbound == DirectionClient {
		return DirectionServer
	}
	return DirectionClient
}

// openRefusalDetail builds the typed refusal one fresh Open carries. The
// admission code is the closed taxonomy the broker classifies without matching
// message text: capacity is a limit (1), a closed physical connection is closed
// (2), and every other refusal is an invalid request (4). The text is a fixed,
// bounded, presentation-safe summary; the underlying diagnostic cause stays
// local and never travels on this wire.
func openRefusalDetail(cause error) ErrorDetail {
	detail := ErrorDetail{
		Code:          ports.BrokerErrorUnavailable,
		AdmissionCode: 4,
		Text:          "invalid open request",
		FailureKind:   domain.RemoteFailureNone,
	}
	switch {
	case errors.Is(cause, ErrTooManyStreams):
		detail.AdmissionCode = 1
		detail.Text = "stream limit reached"
	case errors.Is(cause, ErrPhysicalClosed):
		detail.AdmissionCode = 2
		detail.Text = "physical connection closed"
	}
	return detail
}

// resetErrorDetail builds the typed detail one scheduled Reset carries. The
// cause is always one of the package's bounded sentinels, so its text is
// presentation-safe and within the display bound; a queue overflow is a
// transport classification and every other stream-local refusal is an invalid
// response.
func resetErrorDetail(cause error) ErrorDetail {
	kind := domain.RemoteFailureInvalidResponse
	if errors.Is(cause, ErrSchedulerFull) {
		// The local aggregate safety net: a transport-class abort, not an
		// invalid peer frame.
		kind = domain.RemoteFailureTransport
	}
	return ErrorDetail{
		Code:        ports.BrokerErrorUnavailable,
		Text:        cause.Error(),
		FailureKind: kind,
	}
}
