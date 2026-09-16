// In-memory stream state and admission (P3.2).
//
// StreamEngine owns the admission and lifecycle of the logical streams
// multiplexed over one physical daemon connection. It performs no I/O and
// carries no framing, pump, socket, scheduler, or writer-fairness policy: it
// holds the stream table, the strictly-increasing physical stream identity
// fence, the per-stream lifecycle, the bounded inbound queues, and the
// terminal publication that fences the physical connection and every stream
// it owns.
//
// One physical connection admits at most the negotiated stream ceiling
// (MuxCeilings.MaxStreams, 1..128) pending plus live streams: opening, open,
// or closing - a closing stream keeps its slot until Take drains its preserved
// data. A fresh incoming Open must carry a nonzero physical stream ID that
// is strictly above every ID already admitted; an ID is never reused,
// including after its stream reached a terminal state. Traffic at or below
// the high-water mark that is no longer live belongs to a retired stream and
// is ignored; traffic above the high-water mark was never opened and is
// refused. The high-water mark alone fences a retired identity, so it keeps
// classifying late traffic even after its record was evicted past the
// retention bound.
//
// Every stream walks one exact lifecycle: opening (admitted, not yet
// confirmed), open (confirmed), closing (an orderly inbound Close arrived
// while the stream still held unread data), and terminal (closed, reset,
// refused, or lost with the physical connection). Data is queued only while
// open. An orderly Close that finds the inbound queue empty terminalizes the
// stream immediately; a Close that finds already-queued data marks the stream
// closing instead, so the consumer keeps draining exactly the chunks that were
// accepted before the Close while no new chunk is accepted, and the stream
// reaches terminal - releasing its admission slot and Done - only once Take
// drains the last queued chunk. Reset is an abnormal, stream-local termination
// that always discards the queue immediately. Neither Close nor Reset ever
// terminalizes the physical connection or a sibling stream.
//
// The inbound queue is bounded, and bytes are charged against every budget
// before the chunk is copied into the queue: at most MaxMuxStreamQueueChunks
// chunks and MaxMuxStreamQueueBytes bytes per stream, the negotiated aggregate
// ceiling (MuxCeilings.MaxAggregateBytes) across the connection, and the
// negotiated chunk ceiling (MuxCeilings.StreamChunkLimit) per chunk. A chunk
// above the negotiated chunk ceiling is refused statelessly with ErrTooLarge
// and mutates nothing, exactly as the codec refuses it. A chunk that would
// exceed a queue bound resets exactly that stream and releases the bytes it
// held; sibling streams keep their budget and keep progressing. The per-stream
// byte bound is defensive: the codec already refuses a chunk above the
// negotiated chunk ceiling, so a conforming caller never reaches it.
//
// Terminal publication is deliberately ordered: a physical terminal outcome
// publishes its own Done first and only then terminalizes every live stream
// independently, so a reader woken by any stream always observes the physical
// connection already terminal. A stream that already reached its own outcome
// keeps it.
//
// A settled stream stays in the table so its reference, state, Done, and cause
// remain observable; only an opening, open, or closing stream holds a slot
// against the admission ceiling, and a closing stream retains that slot until
// Take drains its preserved data. Retired records are themselves bounded: the
// engine retains at most MaxRetiredStreamRecords settled records and evicts the
// oldest first, so a long-lived connection cannot accumulate an unbounded
// stream table. An evicted identity is still classified as retired rather than
// future because the high-water mark is never rolled back.
package daemonmux

import (
	"errors"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Stream queue bounds. The stream-count and aggregate-bytes ceilings are the
// negotiated maxima from limits.go (MaxMuxStreams, MaxMuxAggregateBytes); the
// per-stream chunk and byte ceilings are engine-local.
const (
	// MaxMuxStreamQueueChunks bounds the inbound chunks queued for one stream.
	MaxMuxStreamQueueChunks = 8
	// MaxMuxStreamQueueBytes bounds the inbound bytes queued for one stream:
	// 32 MiB. It is only reachable by a caller that hands the engine a chunk
	// above the negotiated chunk ceiling, which the codec already refuses.
	MaxMuxStreamQueueBytes = 32 << 20

	// MaxRetiredStreamRecords bounds the settled stream records one engine
	// retains so a late frame is still classified as retired instead of
	// mistaken for a future stream. It mirrors the negotiated stream ceiling
	// and is evicted oldest-first, so a long-lived connection's stream table
	// stays bounded by the live ceiling plus this retention bound.
	MaxRetiredStreamRecords = 128
)

// Stream admission and lifecycle sentinels. They are typed so a caller can
// classify a refused frame without matching on message text.
var (
	// ErrInvalidStream reports a missing or zero physical stream identity.
	ErrInvalidStream = errors.New("daemonmux: invalid physical stream")
	// ErrStreamIDReused reports a physical stream identity that is not
	// strictly above every admitted identity: IDs strictly increase and are
	// never reused, including after a stream reached a terminal state.
	ErrStreamIDReused = errors.New("daemonmux: stream ID is not fresh")
	// ErrFutureStream reports a frame for a physical stream identity that was
	// never opened. The physical connection's high-water mark fences it.
	ErrFutureStream = errors.New("daemonmux: stream was never opened")
	// ErrTooManyStreams reports the pending-plus-live stream ceiling of one
	// physical connection.
	ErrTooManyStreams = errors.New("daemonmux: too many live streams")
	// ErrStreamState reports an event that is illegal in the stream's current
	// state.
	ErrStreamState = errors.New("daemonmux: invalid stream state")
	// ErrStreamRefMismatch reports an Opened or Refused whose full stream
	// reference does not match the one recorded at admission.
	ErrStreamRefMismatch = errors.New("daemonmux: stream reference mismatch")
	// ErrStreamQueueFull reports an inbound chunk that would exceed a queue
	// bound. The offending stream is reset and its budget released.
	ErrStreamQueueFull = errors.New("daemonmux: stream queue overflow")
	// ErrPhysicalClosed reports admission or stream work presented to a
	// physical connection that already reached its terminal outcome.
	ErrPhysicalClosed = errors.New("daemonmux: physical connection is closed")
	// ErrOpenPolicyRefused reports a fresh inbound Open whose requested policy
	// does not exactly match the accepted physical policy an engine was
	// restricted to. The stream is refused before it is recorded or admitted,
	// so it never holds an admission slot and never reaches a consumer.
	ErrOpenPolicyRefused = errors.New("daemonmux: open policy refused")
)

// StreamState is the closed lifecycle of one logical stream over a physical
// daemon connection.
type StreamState uint8

const (
	// StreamOpening is an admitted stream the opening side has not confirmed.
	StreamOpening StreamState = iota + 1
	// StreamOpen is a confirmed stream that accepts data.
	StreamOpen
	// StreamClosing is a stream whose opening side sent an orderly Close while
	// inbound data was still queued. It accepts no new data; its already-queued
	// chunks stay readable until Take drains them, at which point the stream
	// reaches terminal and releases its slot.
	StreamClosing
	// StreamTerminal is a settled stream: no further frame is applied.
	StreamTerminal
)

func (s StreamState) String() string {
	switch s {
	case StreamOpening:
		return "opening"
	case StreamOpen:
		return "open"
	case StreamClosing:
		return "closing"
	case StreamTerminal:
		return "terminal"
	default:
		return "unknown"
	}
}

// StreamDisposition classifies one stream frame: applied, or a late frame for
// a retired stream that is ignored without effect.
type StreamDisposition uint8

const (
	// StreamAccepted reports the frame was applied.
	StreamAccepted StreamDisposition = iota + 1
	// StreamDiscarded reports a late frame for a retired stream. It is
	// ignored and never revives, reopens, or terminalizes anything.
	StreamDiscarded
)

// StreamStatus is one immutable read of a tracked stream.
type StreamStatus struct {
	// Ref is the full reference recorded at admission.
	Ref StreamRef
	// State is the stream lifecycle state.
	State StreamState
	// Done closes exactly once when the stream reaches its terminal state.
	Done <-chan struct{}
	// Err is the stream's terminal cause: nil while live and for an orderly
	// Close, and the failure cause for a Reset, a queue overflow, or a
	// physical loss.
	Err error
	// FailureKind classifies Err: RemoteFailureNone while live, for an
	// orderly Close, and for a refusal.
	FailureKind domain.RemoteFailureKind
	// Refused reports that the opening side refused the stream.
	Refused bool
	// Refusal is the typed refusal carried by Refused when Refused is true.
	Refusal ErrorDetail
	// QueuedChunks and QueuedBytes are the currently queued inbound chunks
	// and bytes.
	QueuedChunks int
	QueuedBytes  int
}

// muxStream is one tracked logical stream. Every field is guarded by the
// owning StreamEngine's lock; terminal is its own publish-once authority and
// is safe to read independently once published.
type muxStream struct {
	ref      StreamRef
	state    StreamState
	terminal *terminalState
	queue    [][]byte
	bytes    int
	refused  bool
	refusal  ErrorDetail
}

// StreamEngine is the in-memory stream table and admission authority of one
// physical daemon connection. It is safe for concurrent use. It owns no I/O.
// The admission, chunk, and aggregate budgets are copied from the negotiated
// MuxCeilings at construction; the per-stream queue bounds are engine-local.
// They are fields so the engine's private ceilings are fixed per instance and
// a test can tighten them.
type StreamEngine struct {
	mu        sync.Mutex
	physical  *terminalState
	streams   map[PhysicalStreamID]*muxStream
	retired   []PhysicalStreamID
	highest   PhysicalStreamID
	live      int
	aggregate int

	maxStreams        int
	maxChunkBytes     uint64
	maxStreamBytes    int
	maxStreamFrames   int
	maxAggregateBytes int

	// restricted, when set, binds every fresh inbound Open to the exact
	// accepted physical policy in policy. It is configured once, before any
	// stream is admitted, by a daemon-side consumer that owns the connection's
	// authoritative binding (RestrictAdmissions).
	restricted bool
	policy     ports.BrokerPolicy
}

// NewStreamEngine returns an open engine bound to one physical connection
// under the default MuxCeilings: the full envelope ceiling, the maximum
// chunk, the maximum stream count (pending-plus-live), and the maximum
// aggregate buffered bytes.
func NewStreamEngine() *StreamEngine {
	return newStreamEngine(DefaultMuxCeilings())
}

// NewStreamEngineWithCeilings returns an open engine bound to one physical
// connection whose admission, chunk, and aggregate budgets enforce the given
// negotiated MuxCeilings. The value is validated and copied, so the ceilings
// an engine enforces are fixed for its lifetime. An invalid advertisement is
// refused with ErrInvalidCeilings.
func NewStreamEngineWithCeilings(ceilings MuxCeilings) (*StreamEngine, error) {
	if err := ceilings.Validate(); err != nil {
		return nil, err
	}
	return newStreamEngine(ceilings), nil
}

// newStreamEngine builds an engine from already-validated ceilings. The
// exported constructors validate first; NewStreamEngine delegates with the
// package defaults, which are valid by construction.
func newStreamEngine(ceilings MuxCeilings) *StreamEngine {
	return &StreamEngine{
		physical:          newTerminalState(),
		streams:           make(map[PhysicalStreamID]*muxStream, int(ceilings.MaxStreams)),
		maxStreams:        int(ceilings.MaxStreams),
		maxChunkBytes:     ceilings.StreamChunkLimit,
		maxStreamBytes:    MaxMuxStreamQueueBytes,
		maxStreamFrames:   MaxMuxStreamQueueChunks,
		maxAggregateBytes: int(ceilings.MaxAggregateBytes),
	}
}

// Done returns the physical connection's terminal channel. It is closed
// exactly once, before every live stream is terminalized.
func (e *StreamEngine) Done() <-chan struct{} {
	if e == nil || e.physical == nil {
		return nil
	}
	return e.physical.Done()
}

// Err returns the physical connection's terminal cause: nil while open and
// for an orderly Terminate.
func (e *StreamEngine) Err() error {
	if e == nil || e.physical == nil {
		return nil
	}
	return e.physical.Err()
}

// FailureKind returns the physical connection's terminal classification:
// RemoteFailureNone while open and for an orderly Terminate.
func (e *StreamEngine) FailureKind() domain.RemoteFailureKind {
	if e == nil || e.physical == nil {
		return domain.RemoteFailureNone
	}
	return e.physical.FailureKind()
}

// Closed reports whether the physical connection already reached its terminal
// outcome.
func (e *StreamEngine) Closed() bool {
	if e == nil {
		return true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closedLocked()
}

// Live returns the number of admitted streams that have not yet reached their
// terminal state: opening, open, or closing (a closing stream still holds the
// admission slot and its queued bytes until Take drains them).
func (e *StreamEngine) Live() int {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.live
}

// AggregateBytes returns the inbound bytes currently charged across every
// stream. It never exceeds MaxMuxAggregateBytes.
func (e *StreamEngine) AggregateBytes() int {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.aggregate
}

// RestrictAdmissions binds the engine to one exact accepted physical policy:
// every fresh inbound Open must then carry a policy exactly compatible with
// policy, and an Open that differs in any field is refused with
// ErrOpenPolicyRefused before it is recorded or admitted. A replay or
// duplicate Open is still classified by the identity fence first, so a
// restriction never resets or disturbs the live stream a duplicate names.
//
// It must be configured before the first inbound Open - that is, before the
// pump driving the engine starts - because changing the accepted policy after
// admission would leave earlier streams bound to a policy the engine no longer
// enforces. A second call, a call after any admission, an already-terminal
// connection, or an invalid policy is refused without changing enforcement.
func (e *StreamEngine) RestrictAdmissions(policy ports.BrokerPolicy) error {
	if e == nil {
		return ErrPhysicalClosed
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closedLocked() {
		return ErrPhysicalClosed
	}
	if e.restricted || e.highest != 0 {
		return ErrStreamState
	}
	e.policy = policy
	e.restricted = true
	return nil
}

// Open admits one incoming logical stream in opening state. The reference
// must be complete, its physical ID must be strictly above every admitted ID
// (IDs are never reused), and the pending-plus-live ceiling must not be
// reached. When the engine is restricted to an accepted physical policy
// (RestrictAdmissions), a fresh Open whose requested policy does not exactly
// match is refused before it takes an admission slot. A physical connection
// that already reached its terminal outcome refuses all admission.
func (e *StreamEngine) Open(msg Open) error {
	if e == nil {
		return ErrPhysicalClosed
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closedLocked() {
		return ErrPhysicalClosed
	}
	if err := msg.Ref.Validate(); err != nil {
		return ErrInvalidStream
	}
	id := msg.Ref.Physical
	if id <= e.highest {
		return ErrStreamIDReused
	}
	if e.restricted && !e.policy.Compatible(msg.Policy) {
		return ErrOpenPolicyRefused
	}
	if e.live >= e.maxStreams {
		return ErrTooManyStreams
	}
	e.streams[id] = &muxStream{ref: msg.Ref, state: StreamOpening, terminal: newTerminalState()}
	e.highest = id
	e.live++
	return nil
}

// Opened confirms one admitted stream and moves it to open. It is legal only
// for an opening stream, and the full reference must match the one recorded
// at admission.
func (e *StreamEngine) Opened(msg Opened) error {
	if e == nil {
		return ErrPhysicalClosed
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	stream, state, err := e.lookupLocked(msg.Ref.Physical)
	if err != nil {
		return err
	}
	if state != StreamOpening {
		return ErrStreamState
	}
	if stream.ref != msg.Ref {
		return ErrStreamRefMismatch
	}
	stream.state = StreamOpen
	return nil
}

// Refused settles one admitted stream after the opening side refused it. It is
// legal only for an opening stream, and the full reference must match. A
// refusal is an orderly settlement: the stream's Done closes with a nil Err,
// and the typed refusal is retained for the caller.
func (e *StreamEngine) Refused(msg Refused) error {
	if e == nil {
		return ErrPhysicalClosed
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	stream, state, err := e.lookupLocked(msg.Ref.Physical)
	if err != nil {
		return err
	}
	if state != StreamOpening {
		return ErrStreamState
	}
	if stream.ref != msg.Ref {
		return ErrStreamRefMismatch
	}
	stream.refused = true
	stream.refusal = msg.Error
	e.retireLocked(stream)
	stream.terminal.Close()
	return nil
}

// Data queues one already-validated chunk of an open stream. Data is legal
// only while open: a retired stream discards it, and a stream that was never
// opened is refused. A stream that already received an orderly Close
// (closing) accepts no new data and refuses it with ErrStreamState, exactly as
// an unconfirmed stream refuses data. A chunk above the negotiated chunk
// ceiling is refused statelessly with ErrTooLarge and mutates nothing. A chunk
// that would exceed the per-stream chunk, the per-stream byte, or the
// aggregate byte bound resets exactly this stream and releases the bytes it
// held, leaving every sibling untouched.
func (e *StreamEngine) Data(msg Data) (StreamDisposition, error) {
	if e == nil {
		return 0, ErrPhysicalClosed
	}
	if uint64(len(msg.Data)) > e.maxChunkBytes {
		return 0, ErrTooLarge
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	stream, state, err := e.lookupLocked(msg.Physical)
	if err != nil {
		return 0, err
	}
	switch state {
	case StreamTerminal:
		return StreamDiscarded, nil
	case StreamOpening, StreamClosing:
		return 0, ErrStreamState
	}
	if e.queueOverflowsLocked(stream, len(msg.Data)) {
		e.retireLocked(stream)
		stream.terminal.Fail(domain.RemoteFailureTransport, ErrStreamQueueFull)
		return 0, ErrStreamQueueFull
	}
	e.enqueueLocked(stream, msg.Data)
	return StreamAccepted, nil
}

// Close orderly terminates one opening, open, or closing stream. A late close
// for an already terminal or closing stream is discarded. It never
// terminalizes the physical connection or a sibling stream.
//
// An orderly Close preserves data that was already accepted: when the inbound
// queue holds unread chunks, the stream moves to closing, accepts no new data,
// and keeps its queue readable until Take drains it; the stream only reaches
// terminal, releasing its admission slot and closing Done, on the Take that
// empties the queue. A Close with an empty queue terminalizes immediately.
func (e *StreamEngine) Close(msg Close) (StreamDisposition, error) {
	if e == nil {
		return 0, ErrPhysicalClosed
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	stream, state, err := e.lookupLocked(msg.Physical)
	if err != nil {
		return 0, err
	}
	if state == StreamTerminal || state == StreamClosing {
		return StreamDiscarded, nil
	}
	if len(stream.queue) == 0 {
		e.retireLocked(stream)
		stream.terminal.Close()
		return StreamAccepted, nil
	}
	stream.state = StreamClosing
	return StreamAccepted, nil
}

// Reset abnormally terminates one opening or open stream. The termination is
// stream-local: it never terminalizes the physical connection or a sibling
// stream. A late reset for a retired stream is discarded, so cancellation is
// idempotent.
func (e *StreamEngine) Reset(msg Reset) (StreamDisposition, error) {
	if e == nil {
		return 0, ErrPhysicalClosed
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	stream, state, err := e.lookupLocked(msg.Physical)
	if err != nil {
		return 0, err
	}
	if state == StreamTerminal {
		return StreamDiscarded, nil
	}
	e.retireLocked(stream)
	stream.terminal.Fail(msg.Error.FailureKind, resetCause(msg))
	return StreamAccepted, nil
}

// Fail publishes the physical connection's terminal failure and then
// terminalizes every live stream independently with the same cause. Done is
// closed before any stream is terminalized; a stream that already settled
// keeps its own outcome. All queued bytes are released.
//
// Physical publication is first-wins: a Fail after a Terminate is a no-op
// that leaves the orderly nil Err and RemoteFailureNone, exactly as a
// Terminate after a Fail leaves the failure cause. Streams that were live at
// the winning publication all observe that one outcome.
func (e *StreamEngine) Fail(kind domain.RemoteFailureKind, err error) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.physical.Fail(kind, err)
	e.terminalizeAllLocked(func(stream *muxStream) { stream.terminal.Fail(kind, err) })
}

// Terminate orderly closes the physical connection and then terminalizes every
// live stream independently with a nil error. Done is closed before any stream
// is terminalized. All queued bytes are released.
//
// Physical publication is first-wins: a Terminate after a Fail is a no-op
// that leaves the failure cause and its kind, exactly as a Fail after a
// Terminate leaves the orderly nil Err and RemoteFailureNone.
func (e *StreamEngine) Terminate() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.physical.Close()
	e.terminalizeAllLocked(func(stream *muxStream) { stream.terminal.Close() })
}

// Status returns one immutable read of a tracked stream and whether it exists.
func (e *StreamEngine) Status(physical PhysicalStreamID) (StreamStatus, bool) {
	if e == nil {
		return StreamStatus{}, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	stream := e.streams[physical]
	if stream == nil {
		return StreamStatus{}, false
	}
	return StreamStatus{
		Ref:          stream.ref,
		State:        stream.state,
		Done:         stream.terminal.Done(),
		Err:          stream.terminal.Err(),
		FailureKind:  stream.terminal.FailureKind(),
		Refused:      stream.refused,
		Refusal:      stream.refusal,
		QueuedChunks: len(stream.queue),
		QueuedBytes:  stream.bytes,
	}, true
}

// cause returns the retained publish-once terminal authority of one admitted
// stream. The pointer stays valid for the connection's lifetime even after the
// engine evicts the bounded record, so a settled stream keeps its exact cause
// independent of retention. It reports false for a physical identity the
// engine never admitted.
func (e *StreamEngine) cause(physical PhysicalStreamID) (*terminalState, bool) {
	if e == nil {
		return nil, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	stream := e.streams[physical]
	if stream == nil {
		return nil, false
	}
	return stream.terminal, true
}

// Take removes and returns the oldest queued inbound chunk of one stream.
// It reports false when the stream does not exist or its queue is empty. The
// returned chunk is an engine-owned copy: it releases the chunk's bytes from
// both budgets. Draining the last queued chunk of a closing stream terminalizes
// it, releasing its admission slot and closing its Done.
func (e *StreamEngine) Take(physical PhysicalStreamID) ([]byte, bool) {
	chunk, ok, _ := e.take(physical)
	return chunk, ok
}

// take is Take plus whether this pop drained a closing stream to its terminal
// state, so the pump can rotate the stream's watch channel and wake a consumer
// blocked on the queue transition. It holds the engine lock only for the pop
// and the terminal publication.
func (e *StreamEngine) take(physical PhysicalStreamID) ([]byte, bool, bool) {
	if e == nil {
		return nil, false, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	stream := e.streams[physical]
	if stream == nil || len(stream.queue) == 0 {
		return nil, false, false
	}
	chunk := stream.queue[0]
	stream.queue[0] = nil
	stream.queue = stream.queue[1:]
	stream.bytes -= len(chunk)
	e.aggregate -= len(chunk)
	if stream.state == StreamClosing && len(stream.queue) == 0 {
		e.retireLocked(stream)
		stream.terminal.Close()
		return chunk, true, true
	}
	return chunk, true, false
}

// lookupLocked classifies one frame: the tracked record of a live or settled
// stream, a retired stream at or below the high-water mark, or a future
// identity that was never opened.
func (e *StreamEngine) lookupLocked(id PhysicalStreamID) (*muxStream, StreamState, error) {
	if err := id.Validate(); err != nil {
		return nil, 0, ErrInvalidStream
	}
	if stream, ok := e.streams[id]; ok {
		return stream, stream.state, nil
	}
	if id > e.highest {
		return nil, 0, ErrFutureStream
	}
	return nil, StreamTerminal, nil
}

// terminalizeAllLocked publishes one terminal outcome on every live stream.
// The physical outcome is published by the caller before this runs; settled
// streams are left untouched so they keep their own cause.
func (e *StreamEngine) terminalizeAllLocked(publish func(*muxStream)) {
	for _, stream := range e.streams {
		if stream.state == StreamTerminal {
			continue
		}
		e.retireLocked(stream)
		publish(stream)
	}
}

// retireLocked moves one stream to terminal, releases the bytes it holds, and
// records it for bounded retention: the oldest retired record is evicted past
// MaxRetiredStreamRecords. The high-water mark is never rolled back, so an
// evicted identity still classifies as retired rather than future.
func (e *StreamEngine) retireLocked(stream *muxStream) {
	e.releaseQueueLocked(stream)
	stream.state = StreamTerminal
	e.live--
	e.retired = append(e.retired, stream.ref.Physical)
	for len(e.retired) > MaxRetiredStreamRecords {
		evicted := e.retired[0]
		e.retired = e.retired[1:]
		if tracked := e.streams[evicted]; tracked != nil && tracked.state == StreamTerminal {
			delete(e.streams, evicted)
		}
	}
}

// releaseQueueLocked drops every queued chunk and returns its bytes to the
// aggregate budget.
func (e *StreamEngine) releaseQueueLocked(stream *muxStream) {
	for i := range stream.queue {
		stream.queue[i] = nil
	}
	stream.queue = nil
	if stream.bytes != 0 {
		e.aggregate -= stream.bytes
		stream.bytes = 0
	}
}

// queueOverflowsLocked reports whether one inbound chunk would exceed the
// per-stream chunk bound, the per-stream byte bound, or the negotiated
// aggregate byte bound.
func (e *StreamEngine) queueOverflowsLocked(stream *muxStream, n int) bool {
	return len(stream.queue) >= e.maxStreamFrames ||
		stream.bytes+n > e.maxStreamBytes ||
		e.aggregate+n > e.maxAggregateBytes
}

// enqueueLocked charges the chunk against both budgets and only then copies it
// into the queue, so a refused chunk is never allocated a queue-owned copy.
func (e *StreamEngine) enqueueLocked(stream *muxStream, chunk []byte) {
	n := len(chunk)
	e.aggregate += n
	stream.bytes += n
	owned := make([]byte, n)
	copy(owned, chunk)
	stream.queue = append(stream.queue, owned)
}

// closedLocked reports whether the physical connection already reached its
// terminal outcome. The caller holds the engine lock.
func (e *StreamEngine) closedLocked() bool {
	select {
	case <-e.physical.Done():
		return true
	default:
		return false
	}
}

// resetCause builds the stream-local terminal cause of one reset. A reset
// without a declared detail publishes no cause and lets terminalState
// substitute its failure sentinel.
func resetCause(msg Reset) error {
	if !msg.HasError {
		return nil
	}
	return ports.BrokerError{Code: msg.Error.Code, Text: msg.Error.Text}
}
