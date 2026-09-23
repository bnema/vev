// Logical connections over one physical Pump.
//
// A LogicalConnection carries complete raw session envelopes across the broker,
// without decoding them. Typed preview and observation are adapted by their
// consumers, never by this mux connection.
//
// The carriage between the two is a private, ordered, bounded byte stream
// (muxStreamPipe). Its read side pulls already-queued chunks from the pump
// engine with Take, waking on the pump's per-stream Watch; the pump reader is
// therefore never blocked by a slow or stalled consumer, and a consumer that
// never reads lets the engine reset only its own stream. Its write side splits
// each outbound byte slice into chunks at or below the negotiated
// StreamChunkLimit and hands them to Pump.Send as Data frames, so streamframe
// sees one ordered byte stream and applies the 4-byte session framing on top.
//
// The open side maps a ports.BrokerOpenStreamRequest to one mux Open and its
// StreamRef, admits the stream locally, sends the Open, and waits under the
// caller's context for the daemon's Opened or Refused. An independent Close
// sends one mux Close and settles orderly; a malformed inner session frame
// resets exactly that stream (one mux Reset) and leaves every sibling and the
// physical connection untouched. Done, Err, and a physical-loss
// ports.BrokerStreamLost are published once and stay stable afterwards.
package daemonmux

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/bnema/vev/internal/adapters/streamframe"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Logical connection sentinels. They are typed so a caller can classify a
// rejection without matching on message text.
var (
	// ErrLogicalConfig reports an invalid logical-connection configuration: a
	// nil or server-side pump, or an unencodable open request.
	ErrLogicalConfig = errors.New("daemonmux: invalid logical connection configuration")
	// ErrLogicalClosed reports session work presented to a logical connection
	// that already reached its terminal outcome.
	ErrLogicalClosed = errors.New("daemonmux: logical stream is closed")
)

// malformedResetText is the bounded, presentation-safe refusal text carried by
// the stream-local Reset a logical connection sends when the inner session
// stream is malformed. The diagnostic cause stays local in the terminal error.
const malformedResetText = "malformed session stream"

// LogicalConnector opens typed logical streams over one client-side Pump. It
// owns the mux-local physical stream identity allocator: physical IDs are
// strictly increasing and never reused, as the engine requires. One connector
// is bound to exactly one physical connection. Its Open is safe for concurrent
// use; ID allocation and local admission are serialized so engine admission
// always sees strictly increasing IDs, while the Opened/Refused wait runs
// outside that lock.
type LogicalConnector struct {
	pump *Pump

	openMu sync.Mutex
	next   uint64
}

// NewLogicalConnector returns a connector over one broker-side pump: the pump
// must decode daemon-to-broker frames (Inbound == DirectionServer) and emit
// broker-to-daemon frames. A nil, engine-less, or server-side pump is refused.
func NewLogicalConnector(pump *Pump) (*LogicalConnector, error) {
	if pump == nil || pump.Engine() == nil || pump.Local() != DirectionClient {
		return nil, ErrLogicalConfig
	}
	return &LogicalConnector{pump: pump}, nil
}

// Pump returns the physical pump this connector opens streams over.
func (c *LogicalConnector) Pump() *Pump { return c.pump }

// Open maps request to one mux Open and StreamRef, admits the stream locally,
// sends the Open, and waits under ctx for the daemon's Opened or Refused. A
// locally admitted refusal, a stream-reset, or a physical loss during the wait
// is returned as a typed error and the local reservation is released. A
// cancelled context releases the local reservation and returns ctx.Err().
func (c *LogicalConnector) Open(ctx context.Context, request ports.BrokerOpenStreamRequest) (*LogicalConnection, error) {
	if c == nil || c.pump == nil {
		return nil, ErrLogicalConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	physical, err := c.reserve(request)
	if err != nil {
		return nil, err
	}
	status, err := c.awaitOpened(ctx, physical)
	if err != nil {
		c.pump.ReleaseWatch(physical)
		_ = c.abortReserved(physical)
		return nil, err
	}
	return newLogicalConnection(c.pump, status.Ref, status.Done, c.pump.cause(physical)), nil
}

// awaitOpened blocks until the admitted stream is confirmed open, refused, or
// settles. It watches the pump's per-stream channel so it never polls, and it
// also honors ctx and a physical terminal outcome.
func (c *LogicalConnector) awaitOpened(ctx context.Context, physical PhysicalStreamID) (StreamStatus, error) {
	engine := c.pump.Engine()
	for {
		watch := c.pump.Watch(physical)
		status, ok := engine.Status(physical)
		if !ok {
			return StreamStatus{}, openFailure(ErrInvalidStream, c.pump.Err())
		}
		switch status.State {
		case StreamOpen:
			return status, nil
		case StreamTerminal:
			return StreamStatus{}, refusedOrLost(status, c.pump.Err())
		}
		select {
		case <-watch:
		case <-ctx.Done():
			return StreamStatus{}, ctx.Err()
		case <-c.pump.Done():
			status, ok := engine.Status(physical)
			if !ok {
				return StreamStatus{}, openFailure(c.pump.Err(), c.pump.Err())
			}
			return StreamStatus{}, refusedOrLost(status, c.pump.Err())
		}
	}
}

// abortReserved releases one locally admitted reservation that never became a
// live connection: it settles the engine record and best-effort signals the
// peer with a Reset, without touching the physical connection or a sibling.
func (c *LogicalConnector) abortReserved(physical PhysicalStreamID) error {
	reset := Reset{Physical: physical}
	_, _ = c.pump.Engine().Reset(reset)
	_ = c.pump.Send(reset)
	return nil
}

// openMessage maps one validated request to its mux Open and StreamRef.
func openMessage(physical PhysicalStreamID, request ports.BrokerOpenStreamRequest) Open {
	return Open{
		Ref: StreamRef{
			Physical:   physical,
			Epoch:      request.Epoch,
			Connection: request.Connection,
			Client:     request.Stream,
		},
		Purpose:      request.Purpose,
		Admission:    request.Admission,
		Name:         request.Name,
		Local:        request.Local,
		Endpoint:     request.Endpoint,
		Registration: request.Registration,
		Target:       request.Target,
		Env:          append([]string(nil), request.Env...),
		Policy:       request.Policy,
		// The daemon-start authorization travels with the mux Open, so the
		// daemon-side transport receives exactly the authority the broker
		// resolved and can never widen it into a spawn.
		StartMode: request.StartMode,
	}
}

// openAdmissionError classifies one refused local admission as a typed
// admission error so the caller can retry with a fresh identity.
func openAdmissionError(err error) error {
	switch {
	case errors.Is(err, ErrTooManyStreams):
		return ports.BrokerAdmissionLimit
	case errors.Is(err, ErrPhysicalClosed):
		return ports.BrokerAdmissionClosed
	case errors.Is(err, ErrStreamIDReused):
		return ports.BrokerAdmissionStale
	default:
		return ports.BrokerAdmissionInvalid
	}
}

// refusedOrLost classifies one terminal status observed while opening: an
// explicit refusal, or a stream reset / physical loss.
func refusedOrLost(status StreamStatus, physicalErr error) error {
	if status.Refused {
		if status.Refusal.AdmissionCode != 0 {
			switch status.Refusal.AdmissionCode {
			case 1:
				return ports.BrokerAdmissionLimit
			case 2:
				return ports.BrokerAdmissionClosed
			case 3:
				return ports.BrokerAdmissionStale
			case 4:
				return ports.BrokerAdmissionInvalid
			}
		}
		return ports.BrokerError{Code: status.Refusal.Code, Text: status.Refusal.Text}
	}
	if physicalErr != nil {
		return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: physicalErr}
	}
	return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: status.Err}
}

// openFailure maps one non-status transport failure on open.
func openFailure(err error, physicalErr error) error {
	if physicalErr != nil {
		return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: physicalErr}
	}
	return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: err}
}

// discardQueuedInbound drains every inbound chunk the engine already accepted
// for one physical stream, releasing its bytes from the per-stream and
// aggregate budgets. A locally closing stream calls it before the engine's
// orderly Close, because a local Close stops this side's reader: nothing else
// would ever drain a preserved queue, and the engine only reaches its terminal
// state - releasing the admission slot and the queued bytes - once that queue
// is empty. It is a no-op for an unknown, empty, or already terminal stream.
func discardQueuedInbound(pump *Pump, physical PhysicalStreamID) {
	for {
		if _, ok := pump.Take(physical); !ok {
			return
		}
	}
}

// LogicalConnection is one client-side logical stream over a physical Pump.
// It implements the raw envelope port; Close, Done and Err manage its lifetime.
type LogicalConnection struct {
	pump   *Pump
	ref    StreamRef
	pipe   *muxStreamPipe
	raw    *muxTransport
	stream <-chan struct{}
	// cause is the stream's retained publish-once terminal authority, captured
	// at open. It outlives the engine's bounded record, so a settled stream
	// keeps its exact cause even after the record is evicted.
	cause *terminalState

	terminal  *terminalState
	watchDone chan struct{}
	closeOnce sync.Once
	closeErr  error
}

var _ ports.BrokerEnvelopeStream = (*LogicalConnection)(nil)

// newLogicalConnection builds the framed carriage over one confirmed open
// stream and starts the terminal watcher. It never blocks. cause is the
// stream's retained terminal authority, captured at open so the connection
// reports its exact outcome independent of the engine's bounded record.
func newLogicalConnection(pump *Pump, ref StreamRef, streamDone <-chan struct{}, cause *terminalState) *LogicalConnection {
	limit := pump.Ceilings().StreamChunkLimit
	pipe := newMuxStreamPipe(pump, ref.Physical, limit, streamDone)
	raw := newMuxTransport(pipe)
	connection := &LogicalConnection{
		pump:      pump,
		ref:       ref,
		pipe:      pipe,
		raw:       raw,
		stream:    streamDone,
		cause:     cause,
		terminal:  newTerminalState(),
		watchDone: make(chan struct{}),
	}
	go connection.watch()
	return connection
}

// Ref returns the complete stream reference recorded at admission.
func (c *LogicalConnection) Ref() StreamRef { return c.ref }

// Done returns the logical terminal channel independent of reads. It closes
// exactly once, before Err becomes stable.
func (c *LogicalConnection) Done() <-chan struct{} { return c.terminal.Done() }

// Err returns the stable terminal cause: nil for an orderly local or peer
// Close, a ports.BrokerStreamLost for a peer Reset, a local malformed-inner
// reset, or a physical loss.
func (c *LogicalConnection) Err() error { return c.terminal.Err() }

// FailureKind classifies Err. It is None while open and for an orderly Close.
func (c *LogicalConnection) FailureKind() domain.RemoteFailureKind { return c.terminal.FailureKind() }

func (c *LogicalConnection) SendEnvelope(payload []byte) error {
	if c.settled() {
		return c.terminalError()
	}
	if len(payload) > wire.AbsoluteEnvelopeLimit {
		return streamframe.ErrTooLarge
	}
	return c.raw.Send(wire.Envelope{Payload: payload})
}

func (c *LogicalConnection) RecvEnvelope() ([]byte, error) {
	envelope, err := c.raw.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			c.syncFromEngine()
			return nil, c.terminalError()
		}
		return nil, c.abortMalformed(err)
	}
	return envelope.Payload, nil
}

// Close independently closes the logical stream: it discards the inbound
// chunks the engine already accepted, settles this stream locally, sends
// exactly one mux Close, publishes the orderly nil outcome, and unblocks every
// read and write. It never touches the physical connection or a sibling
// stream. Close is idempotent and concurrent-safe; every caller observes the
// same stored transport Close error.
func (c *LogicalConnection) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		// Discard what the engine already accepted before closing locally: this
		// side stops reading as soon as it closes, so draining first is what lets
		// the engine's orderly Close reach its terminal state and release the
		// stream's admission slot and aggregate bytes instead of parking the
		// stream in closing with a queue nothing will ever drain.
		discardQueuedInbound(c.pump, c.ref.Physical)
		_, _ = c.pump.Engine().Close(Close{Physical: c.ref.Physical})
		// A chunk the reader accepted between the drain and the Close parked the
		// stream in closing instead of terminal; no new chunk is accepted after
		// the Close, so this drain terminalizes the stream and releases its slot
		// and bytes.
		discardQueuedInbound(c.pump, c.ref.Physical)
		_ = c.pump.Send(Close{Physical: c.ref.Physical})
		c.terminal.Close()
		c.closeErr = c.raw.Close()
		<-c.watchDone
		c.pump.ReleaseWatch(c.ref.Physical)
	})
	return c.closeErr
}

// watch publishes the logical terminal outcome when the stream settles on its
// own (peer Close, peer Reset, queue overflow, or physical loss) independently
// of reads, and unblocks the byte stream. A local Close settles first and makes
// this a no-op.
func (c *LogicalConnection) watch() {
	defer c.pump.ReleaseWatch(c.ref.Physical)
	defer close(c.watchDone)
	select {
	case <-c.stream:
		// The stream reached its terminal state. Publish the retained cause,
		// not a fresh engine lookup: the engine record may already be evicted.
		c.syncFromEngine()
	case <-c.pump.Done():
		// A physical terminal outcome wakes every stream. A stream-local cause
		// recorded before the physical failure stays authoritative.
		if !c.settleFromCause() {
			if err := c.pump.Err(); err != nil {
				c.settleLoss(c.pump.FailureKind(), err)
			} else {
				c.terminal.Close()
			}
		}
	case <-c.pipe.Done():
		return
	}
	// Closing the framed transport also closes the byte stream and joins
	// streamframe's writer, so a self-terminating stream leaves no goroutine
	// behind.
	_ = c.raw.Close()
}

// syncFromEngine settles this connection from the stream's retained terminal
// authority when it published, and otherwise from the pump's own terminal
// outcome. It closes the race between a blocked read observing the inner-stream
// end and the watcher publishing the same outcome, and it never lets a physical
// failure surface as a clean io.EOF: the retained authority outlives the
// bounded engine record, so an evicted stream still reports its exact cause.
func (c *LogicalConnection) syncFromEngine() {
	if c.settled() {
		return
	}
	if c.settleFromCause() {
		return
	}
	if err := c.pump.Err(); err != nil {
		c.settleLoss(c.pump.FailureKind(), err)
		return
	}
	if status, ok := c.pump.Engine().Status(c.ref.Physical); ok && status.State == StreamTerminal {
		c.settleFromStatus(status)
		return
	}
	c.terminal.Close()
}

// settleFromCause publishes the terminal outcome recorded by the stream's
// retained publish-once authority. The authority is captured when the stream
// opens and stays valid for the connection's lifetime even after the engine
// evicts the bounded record, so a settled stream keeps its exact cause
// independent of retention: a Reset never degrades into an orderly close. It
// reports whether the authority had published.
func (c *LogicalConnection) settleFromCause() bool {
	cause := c.cause
	if cause == nil {
		return false
	}
	select {
	case <-cause.Done():
	default:
		return false
	}
	if err := cause.Err(); err != nil {
		c.settleLoss(cause.FailureKind(), err)
	} else {
		c.terminal.Close()
	}
	return true
}

// settleFromStatus publishes the terminal outcome recorded by the engine.
func (c *LogicalConnection) settleFromStatus(status StreamStatus) {
	switch {
	case status.Refused:
		c.terminal.Fail(domain.RemoteFailureInvalidResponse, ports.BrokerError{Code: status.Refusal.Code, Text: status.Refusal.Text})
	case status.Err == nil:
		c.terminal.Close()
	default:
		c.settleLoss(status.FailureKind, status.Err)
	}
}

// abortMalformed resets exactly this stream after a malformed inner session
// frame: it settles the local engine record, best-effort signals the peer with
// one Reset, publishes the stream-local failure, unblocks the byte stream, and
// returns the stable terminal error.
func (c *LogicalConnection) abortMalformed(cause error) error {
	reset := Reset{
		Physical: c.ref.Physical,
		Error: ErrorDetail{
			Code:        ports.BrokerErrorUnavailable,
			Text:        malformedResetText,
			FailureKind: domain.RemoteFailureInvalidResponse,
		},
		HasError: true,
	}
	_, _ = c.pump.Engine().Reset(reset)
	_ = c.pump.Send(reset)
	c.settleLoss(domain.RemoteFailureInvalidResponse, cause)
	_ = c.raw.Close()
	return c.terminalError()
}

// settleLoss publishes one stream-local loss carrying the exact
// epoch/connection/stream scope of this logical connection.
func (c *LogicalConnection) settleLoss(kind domain.RemoteFailureKind, cause error) {
	if kind < domain.RemoteFailureTransport || kind > domain.RemoteFailureInvalidResponse {
		kind = domain.RemoteFailureTransport
	}
	c.terminal.Fail(kind, ports.BrokerStreamLost{
		Connection: c.ref.Connection, Stream: c.ref.Client, Epoch: c.ref.Epoch,
		Cause: kind, Err: cause,
	})
}

// settled reports whether the logical connection already reached its terminal
// outcome.
func (c *LogicalConnection) settled() bool {
	select {
	case <-c.terminal.Done():
		return true
	default:
		return false
	}
}

// terminalError is the stable error of a settled connection: the published
// cause, or io.EOF for an orderly close.
func (c *LogicalConnection) terminalError() error {
	if err := c.terminal.Err(); err != nil {
		return err
	}
	return io.EOF
}

// muxStreamPipe is the private ordered, bounded byte stream of one logical mux
// stream. Read pulls already-queued inbound chunks from the pump engine with
// Take, blocking on the pump's per-stream Watch and ending on the stream's
// terminal channel or the pump's Done; Write splits an outbound slice at the
// negotiated chunk ceiling and sends each piece as one Data frame. Close
// unblocks both directions. Exactly one reader is expected (streamframe
// serializes Recv), and concurrent writes are serialized by streamframe too.
type muxStreamPipe struct {
	pump       *Pump
	id         PhysicalStreamID
	limit      uint64
	streamDone <-chan struct{}
	done       chan struct{}
	once       sync.Once
	mu         sync.Mutex
	pending    []byte
}

// newMuxStreamPipe builds the ordered byte stream of one mux stream. limit is
// the negotiated StreamChunkLimit; it must be nonzero.
func newMuxStreamPipe(pump *Pump, id PhysicalStreamID, limit uint64, streamDone <-chan struct{}) *muxStreamPipe {
	if limit == 0 {
		limit = MinMuxChunkBytes
	}
	return &muxStreamPipe{pump: pump, id: id, limit: limit, streamDone: streamDone, done: make(chan struct{})}
}

// done returns the pipe's closed-on-close channel.
func (s *muxStreamPipe) Done() <-chan struct{} { return s.done }

func (s *muxStreamPipe) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// Read returns the next ordered inbound bytes. It drains whatever the engine
// still holds and returns io.EOF once the stream or the physical connection
// reaches its terminal outcome.
func (s *muxStreamPipe) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		s.mu.Lock()
		if len(s.pending) > 0 {
			n := copy(p, s.pending)
			s.pending = s.pending[n:]
			s.mu.Unlock()
			return n, nil
		}
		s.mu.Unlock()
		if s.closed() {
			return 0, io.EOF
		}
		watch := s.pump.Watch(s.id)
		if chunk, ok := s.pump.Take(s.id); ok {
			s.mu.Lock()
			s.pending = chunk
			s.mu.Unlock()
			continue
		}
		if watchClosed(watch) {
			// Watch returns an already-closed channel only for a terminal or
			// unknown stream, so no further inbound chunk can ever be queued.
			return 0, io.EOF
		}
		select {
		case <-watch:
		case <-s.done:
			return 0, io.EOF
		case <-s.streamDone:
			return 0, io.EOF
		case <-s.pump.Done():
			return 0, io.EOF
		}
	}
}

// Write splits p into ordered chunks at or below the negotiated chunk ceiling
// and sends each as one Data frame. It reports the bytes fully sent before an
// error.
func (s *muxStreamPipe) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		if s.closed() {
			return written, ErrLogicalClosed
		}
		n := len(p)
		if uint64(n) > s.limit {
			n = int(s.limit)
		}
		if err := s.pump.Send(Data{Physical: s.id, Data: p[:n]}); err != nil {
			if s.closed() {
				return written, ErrLogicalClosed
			}
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

// Close unblocks every read and write on the byte stream exactly once.
func (s *muxStreamPipe) Close() error {
	s.once.Do(func() { close(s.done) })
	return nil
}

// muxTransport adapts the ordered byte stream to wire.Transport: streamframe
// owns the 4-byte length framing, and sessionwire owns any typed conversion at the consuming endpoint. Closing it closes the byte stream and joins streamframe's writer.
type muxTransport struct {
	pipe   *muxStreamPipe
	framer *streamframe.Framer
}

var _ wire.Transport = (*muxTransport)(nil)

// newMuxTransport frames the byte stream into typed envelopes.
func newMuxTransport(pipe *muxStreamPipe) *muxTransport {
	return &muxTransport{pipe: pipe, framer: streamframe.NewFramer(pipe, pipe, pipe.Close)}
}

func (t *muxTransport) Send(envelope wire.Envelope) error { return t.framer.Send(envelope.Payload) }

func (t *muxTransport) Recv() (wire.Envelope, error) {
	payload, err := t.framer.Recv()
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.Envelope{Payload: payload}, nil
}

func (t *muxTransport) Close() error { return t.framer.Close() }

// reserve allocates the next strictly increasing mux-local physical stream ID,
// admits it locally, and enqueues its Open frame under one lock, so both the
// engine's strictly increasing admission order and the outbound frame order
// match the allocation order even when Opens run concurrently. The lock is
// released before the open waits on the round trip, so a slow peer never blocks
// a sibling reservation.
func (c *LogicalConnector) reserve(request ports.BrokerOpenStreamRequest) (PhysicalStreamID, error) {
	c.openMu.Lock()
	defer c.openMu.Unlock()
	c.next++
	physical := PhysicalStreamID(c.next)
	open := openMessage(physical, request)
	if err := c.pump.Engine().Open(open); err != nil {
		return 0, openAdmissionError(err)
	}
	if err := c.pump.Send(open); err != nil {
		_ = c.abortReserved(physical)
		return 0, openFailure(err, c.pump.Err())
	}
	return physical, nil
}
