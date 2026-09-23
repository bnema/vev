// Outbound fair scheduling (P3.2).
//
// Scheduler is the outbound companion of the StreamEngine on one physical
// daemonmux connection. It owns one bounded, independently cancellable FIFO
// queue per admitted stream and orders the already-encoded application
// envelopes a future writer drains. It performs no I/O, owns no goroutine, and
// carries no framing, pump, socket, or listener: it only decides which stream's
// next immutable envelope is written next.
//
// A frame is enqueued only for a stream the engine has admitted and not yet
// terminalized (an opening or open stream), or, when no engine gate is
// installed, for any nonzero physical identity the caller already admitted. A
// closing stream (an orderly inbound Close with unread data) admits no new
// frame even though it is not yet terminal. Two exceptions are deliberate.
// Exactly one terminal Close or Reset is accepted for a known identity the
// engine has already settled, bypassing normal admission, so an overflow or
// peer-reset path can still signal the peer. And Refuse schedules exactly one
// Mux Refused for a fresh identity the engine rejected before recording it, so
// a refused Open is still answered; that refusal seals its own record. A
// duplicate terminal, a duplicate refusal, and data for a settled, unadmitted,
// or closing identity stay refused while the settling record is retained (see
// below). Each stream queues at most MaxMuxStreamQueueChunks (8) envelopes and
// MaxMuxStreamQueueBytes (32 MiB), and all streams together queue at most
// MaxMuxAggregateBytes (64 MiB) of application data. A frame is charged
// against both byte budgets before it is inserted, and a frame that would
// exceed the per-stream frame bound, the per-stream byte bound, or the
// aggregate byte bound discards exactly its own stream queue, frees that
// budget, and leaves every sibling untouched.
//
// An overflowed stream is not sealed outright: the discard settles its data
// but leaves its record open for exactly one terminal Close or Reset, so the
// caller can still signal the peer that the stream was aborted. That terminal
// is always retained: it is inserted without re-running the overflow check, so
// it may exceed the aggregate data budget by at most one bounded terminal
// envelope per live or retained overflowed stream. That bounded excess is
// peer notification, never application data: the data bounds above still hold
// for every non-terminal frame. Until that one terminal is enqueued the record
// refuses every non-terminal frame with ErrSchedulerFull; the terminal seals
// it, after which every later frame is refused (a duplicate terminal with
// ErrSchedulerSettled).
//
// Across active streams the scheduler runs byte-deficit round robin with a
// 64 KiB quantum (SchedulerQuantum): active streams are visited in turn, each
// visit tops up the stream's deficit by one quantum unconditionally and caps
// it at one quantum plus the largest envelope the codec admits, and its oldest
// frame is written once the head fits the accumulated deficit. A head frame
// larger than the quantum therefore accumulates deficit across visits instead
// of stalling the ring, so every admitted frame up to the 1 MiB application
// ceiling makes bounded progress (at most one quantum of accumulation per
// visit) and can never spin the scheduler. Per-stream order is always
// preserved: the scheduler only ever writes a stream's oldest queued frame, so
// a Close never overtakes earlier accepted data of its own stream. Control
// frames carry a small bounded priority (MaxMuxSchedulerControlBurst): they
// may be written ahead of data frames of other streams, but the burst is
// capped so control metadata can never starve data. A single hot stream
// therefore never starves the others, and the schedule is deterministic: the
// same enqueue sequence always yields the same dequeue order.
//
// Settled stream records are bounded: the scheduler retains at most
// MaxRetiredSchedulerRecords sealed-or-overflowed, empty records and evicts
// the oldest first, so a long-lived connection cannot accumulate an unbounded
// record table. The ErrSchedulerSettled refusals above are guaranteed within
// exactly that retention window. Once a record is evicted its identity falls
// back to the engine gate: an identity the engine gate no longer tracks fails
// that check and is reported unadmitted (ErrSchedulerUnadmitted), while a late
// terminal that is still accepted is harmlessly discarded by the peer's
// stream fencing, which ignores a Close or Reset for an already terminal
// stream. An overflowed record that accepts its one terminal after being
// listed keeps that queued frame past eviction, but draining the frame
// re-lists the record so a later eviction reclaims it and retention stays
// bounded.
//
// Reset settles one stream locally: it discards that stream's queued frames,
// frees their bytes immediately, and refuses every later frame for that
// identity. It never touches the physical connection or a sibling stream, so
// cancelling one stream is isolated. Every dequeued OutboundEnvelope carries
// the complete serialized directional envelope in Bytes, owned by the caller
// and never reused or mutated by the scheduler, together with the committed
// client or server message.
package daemonmux

import (
	"errors"
	"sync"
)

const (
	// SchedulerQuantum is the byte-deficit round-robin quantum: 64 KiB, the
	// largest opaque stream chunk the daemonmux codec admits. A stream with a
	// head frame at or below the quantum is served once per visit; a larger
	// frame accumulates one quantum per visit up to the overflow cap and is
	// served once its deficit covers it.
	SchedulerQuantum = int64(MaxMuxChunkBytes)

	// MaxRetiredSchedulerRecords bounds the settled scheduler records retained
	// so a duplicate terminal frame is still refused and a repeated Reset
	// stays idempotent; that refusal is guaranteed only within this retention
	// window, and an evicted identity may degrade to unadmitted. An overflowed,
	// not-yet-sealed record is retained the same way. It mirrors the negotiated
	// stream ceiling and is evicted oldest-first.
	MaxRetiredSchedulerRecords = 128

	// MaxMuxSchedulerControlBurst bounds how many control frames the
	// scheduler writes ahead of data before it forces a data frame. It keeps
	// control priority small and bounded so control metadata can never starve
	// data.
	MaxMuxSchedulerControlBurst = 4
)

// Outbound scheduling sentinels. They are typed so a caller can classify a
// refused frame without matching on message text.
var (
	// ErrSchedulerUnadmitted reports an outbound frame for a physical stream
	// the admission gate has not admitted or has already terminalized. An
	// identity whose settled scheduler record was evicted, and which the engine
	// gate no longer tracks, degrades to this refusal (see ErrSchedulerSettled).
	ErrSchedulerUnadmitted = errors.New("daemonmux: stream is not admitted for scheduling")
	// ErrSchedulerSettled reports an outbound frame for a stream the scheduler
	// already sealed: a queued terminal Close or Reset, a local Reset, or a
	// record that took the one terminal a queue overflow leaves open.
	//
	// The refusal is guaranteed only while the settled record is retained
	// (bounded by MaxRetiredSchedulerRecords). Once the record is evicted the
	// identity may instead be unadmitted (ErrSchedulerUnadmitted), or a late
	// terminal may be accepted once more and harmlessly discarded by the peer's
	// stream fencing.
	ErrSchedulerSettled = errors.New("daemonmux: stream is settled for scheduling")
	// ErrSchedulerFull reports an outbound frame that would exceed the
	// per-stream frame bound, the per-stream byte bound, or the aggregate byte
	// bound. The offending stream's queue is discarded and its budget
	// released. It also reports a non-terminal frame for a stream that already
	// overflowed and is awaiting its one terminal Close or Reset: the data was
	// discarded and the record is not yet sealed.
	ErrSchedulerFull = errors.New("daemonmux: scheduler queue overflow")
)

// FrameClass classifies one outbound frame for scheduling priority. Control
// frames may be written ahead of data frames under a bounded burst; data
// frames carry the opaque stream payload.
type FrameClass uint8

const (
	// FrameData is an opaque stream chunk: a daemonmux Data payload.
	FrameData FrameClass = iota + 1
	// FrameControl is connection or stream metadata: Open, Opened, Refused,
	// Close, or Reset.
	FrameControl
)

func (c FrameClass) String() string {
	switch c {
	case FrameData:
		return "data"
	case FrameControl:
		return "control"
	default:
		return "unknown"
	}
}

// EnvelopeDirection names the daemonmux direction of one outbound envelope.
// DirectionClient is broker-to-daemon (the codec's client direction);
// DirectionServer is daemon-to-broker (the codec's server direction).
type EnvelopeDirection uint8

const (
	// DirectionClient is the broker-to-daemon direction.
	DirectionClient EnvelopeDirection = iota + 1
	// DirectionServer is the daemon-to-broker direction.
	DirectionServer
)

func (d EnvelopeDirection) String() string {
	switch d {
	case DirectionClient:
		return "client"
	case DirectionServer:
		return "server"
	default:
		return "unknown"
	}
}

// OutboundEnvelope is one immutable outbound frame ready for a future writer.
// Bytes is the complete serialized directional envelope, owned by the caller
// and never reused or mutated by the scheduler. Exactly one of Client and
// Server is non-nil and matches Direction; it is a scheduler-owned committed
// copy, so a caller that mutates its own message after enqueue never changes a
// dequeued value. Class is the scheduling class the frame was enqueued under.
type OutboundEnvelope struct {
	Physical  PhysicalStreamID
	Direction EnvelopeDirection
	Class     FrameClass
	Bytes     []byte
	Client    ClientMessage
	Server    ServerMessage
}

// schedFrame is one queued outbound frame. Its encoded bytes are the
// immutable carriage the future writer drains; the committed message is
// retained for the caller that wants the typed value.
type schedFrame struct {
	direction EnvelopeDirection
	class     FrameClass
	bytes     []byte
	seals     bool
	client    ClientMessage
	server    ServerMessage
}

// schedStream is one tracked outbound stream. It is the scheduling view of a
// stream the engine admitted: the bounded FIFO queue, its charged bytes, the
// byte-deficit account, whether it still participates in the active ring, and
// whether it is sealed against further frames. Every field is guarded by the
// owning Scheduler's lock.
type schedStream struct {
	physical PhysicalStreamID
	queue    []schedFrame
	bytes    int
	deficit  int64
	sealed   bool
	// overflowed marks a stream whose queue overflowed and was discarded: it
	// is settled for data but not yet sealed, so exactly one terminal Close or
	// Reset may still be enqueued to signal the peer; that one terminal is
	// always retained and is the only frame that may carry the aggregate
	// charge past MaxMuxAggregateBytes (by at most one bounded envelope).
	overflowed bool
	active     bool
	// retired marks the record as listed for bounded eviction once it is
	// sealed and empty; it is set at most once.
	retired bool
}

// Scheduler is the outbound fair-scheduling authority of one physical
// daemonmux connection. It is safe for concurrent use and owns no I/O. The
// optional StreamEngine gate reads admission; a nil engine leaves admission to
// the caller.
type Scheduler struct {
	mu        sync.Mutex
	engine    *StreamEngine
	streams   map[PhysicalStreamID]*schedStream
	active    []*schedStream
	retired   []*schedStream
	aggregate int
	// controlBurst is the remaining number of control frames that may be
	// written ahead of data. It refills to MaxMuxSchedulerControlBurst after
	// every data frame.
	controlBurst int

	maxEnvelopeBytes uint64
	maxChunkBytes    uint64

	// Bounds and quantum are constructed from the package defaults and are
	// owned by the scheduler; they are fields so a test can tighten them.
	maxAggregateBytes int
	maxStreamBytes    int
	maxStreamFrames   int
	quantum           int64
}

// NewSchedulerWithCeilings returns an empty outbound scheduler whose envelope,
// chunk, and aggregate budgets enforce the given negotiated MuxCeilings. The
// value is validated and copied, so the ceilings a scheduler enforces are
// fixed for its lifetime. An invalid advertisement is refused with
// ErrInvalidCeilings.
func NewSchedulerWithCeilings(engine *StreamEngine, ceilings MuxCeilings) (*Scheduler, error) {
	if err := ceilings.Validate(); err != nil {
		return nil, err
	}
	return newScheduler(engine, ceilings), nil
}

// newScheduler builds a scheduler from already-validated ceilings. The
// exported constructors validate first; NewScheduler delegates with the
// package defaults, which are valid by construction.
func newScheduler(engine *StreamEngine, ceilings MuxCeilings) *Scheduler {
	return &Scheduler{
		engine:            engine,
		streams:           make(map[PhysicalStreamID]*schedStream, int(ceilings.MaxStreams)),
		maxEnvelopeBytes:  ceilings.MaxReceiveEnvelopeBytes,
		maxChunkBytes:     ceilings.StreamChunkLimit,
		maxAggregateBytes: int(ceilings.MaxAggregateBytes),
		maxStreamBytes:    MaxMuxStreamQueueBytes,
		maxStreamFrames:   MaxMuxStreamQueueChunks,
		quantum:           SchedulerQuantum,
		controlBurst:      MaxMuxSchedulerControlBurst,
	}
}

// EnqueueClient encodes and enqueues one broker-to-daemon envelope.
func (s *Scheduler) EnqueueClient(message ClientMessage) error {
	if s == nil || message == nil {
		return ErrInvalidMessage
	}
	physical, class, err := clientFrame(message)
	if err != nil {
		return err
	}
	raw, err := EncodeClient(message, s.maxEnvelopeBytes, s.maxChunkBytes)
	if err != nil {
		return err
	}
	return s.enqueue(schedFrame{
		direction: DirectionClient,
		class:     class,
		bytes:     raw,
		seals:     clientSeals(message),
		client:    cloneClientMessage(message),
	}, physical)
}

// Refuse enqueues exactly one Mux Refused for a fresh inbound Open the engine
// refused before it recorded any stream. It is the deliberate second admission
// exception, alongside the one terminal Close or Reset a settled identity
// accepts: a Refused is final for its physical identity (the daemon never sends
// another frame for a stream it never admitted), so the record is sealed and
// bounded by the same retired-record eviction as every other settled stream.
//
// The frame bypasses the engine gate because a rejected Open leaves no engine
// record to confirm; the caller is the pump, and it only reaches here for an
// identity above the engine's high-water mark that the engine refused. A
// duplicate Refused, or any frame after the refusal drained, is refused with
// ErrSchedulerSettled.
func (s *Scheduler) Refuse(message Refused) error {
	if s == nil {
		return ErrInvalidMessage
	}
	physical, class, err := serverFrame(message)
	if err != nil {
		return err
	}
	raw, err := EncodeServer(message, s.maxEnvelopeBytes, s.maxChunkBytes)
	if err != nil {
		return err
	}
	if err := physical.Validate(); err != nil {
		return err
	}
	frame := schedFrame{
		direction: DirectionServer,
		class:     class,
		bytes:     raw,
		seals:     true,
		server:    cloneServerMessage(message),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := s.streams[physical]
	if stream != nil {
		if stream.sealed || stream.overflowed {
			return ErrSchedulerSettled
		}
	} else {
		stream = &schedStream{physical: physical}
		s.streams[physical] = stream
	}
	if s.overflowLocked(stream, len(frame.bytes)) {
		s.overflowLockedStream(stream)
		return ErrSchedulerFull
	}
	s.insertLocked(stream, frame)
	return nil
}

// EnqueueServer encodes and enqueues one daemon-to-broker envelope.
func (s *Scheduler) EnqueueServer(message ServerMessage) error {
	if s == nil || message == nil {
		return ErrInvalidMessage
	}
	physical, class, err := serverFrame(message)
	if err != nil {
		return err
	}
	raw, err := EncodeServer(message, s.maxEnvelopeBytes, s.maxChunkBytes)
	if err != nil {
		return err
	}
	return s.enqueue(schedFrame{
		direction: DirectionServer,
		class:     class,
		bytes:     raw,
		seals:     serverSeals(message),
		server:    cloneServerMessage(message),
	}, physical)
}

// enqueue inserts one already-encoded frame under the scheduler lock. It
// confirms admission, then charges the frame against both byte budgets before
// the queue adopts it. A frame that would overflow any bound discards its own
// stream queue and frees that budget without touching a sibling. A terminal
// frame is the one exception to admission: it is accepted exactly once for a
// known identity, so an overflow or reset path can still signal the peer even
// after the engine settled the stream. Data for such an identity stays
// refused, and a duplicate terminal is refused with ErrSchedulerSettled.
func (s *Scheduler) enqueue(frame schedFrame, physical PhysicalStreamID) error {
	if err := physical.Validate(); err != nil {
		return ErrInvalidStream
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := s.streams[physical]
	if stream != nil {
		if stream.sealed {
			// A duplicate terminal, and every frame after a terminal, is
			// refused: the segment is already settled.
			return ErrSchedulerSettled
		}
		if stream.overflowed {
			// The queue overflowed and was discarded, but the record is not yet
			// sealed: exactly one terminal Close or Reset may still signal the
			// peer. Any other frame is refused with ErrSchedulerFull; the
			// terminal is inserted without re-running the overflow check because
			// the queue is already empty and its bytes released. It is therefore
			// the one frame that may carry the aggregate charge past
			// MaxMuxAggregateBytes, by at most one bounded terminal per
			// overflowed stream, so peer notification is never lost.
			if !frame.seals {
				return ErrSchedulerFull
			}
			s.insertLocked(stream, frame)
			return nil
		}
		if !frame.seals && !s.admittedLocked(physical) {
			return ErrSchedulerUnadmitted
		}
	} else {
		if !frame.seals {
			if !s.admittedLocked(physical) {
				return ErrSchedulerUnadmitted
			}
		} else if !s.knownLocked(physical) {
			// A terminal frame still needs a known identity: it may bypass a
			// settled admission, never a stream that was never admitted.
			return ErrSchedulerUnadmitted
		}
		stream = &schedStream{physical: physical}
		s.streams[physical] = stream
	}
	if s.overflowLocked(stream, len(frame.bytes)) {
		s.overflowLockedStream(stream)
		return ErrSchedulerFull
	}
	s.insertLocked(stream, frame)
	return nil
}

// insertLocked charges one already-admitted frame against both byte budgets
// and appends it to the stream's queue, sealing the record when the frame is a
// terminal Close or Reset and entering the stream into the active ring. The
// caller has already tested every bound, except for the one terminal an
// overflowed stream still retains: that frame is charged here deliberately, so
// peer notification survives the overflow at the cost of at most one bounded
// envelope above the aggregate data budget.
func (s *Scheduler) insertLocked(stream *schedStream, frame schedFrame) {
	n := len(frame.bytes)
	s.aggregate += n
	stream.bytes += n
	stream.queue = append(stream.queue, frame)
	if frame.seals {
		stream.sealed = true
	}
	if !stream.active {
		stream.active = true
		s.active = append(s.active, stream)
	}
}

// overflowLockedStream settles one stream whose frame exceeded a bound: it
// discards the queued frames, frees their budget, and marks the record as
// overflowed rather than sealed, so exactly one terminal Close or Reset may
// still be enqueued. The record is listed for bounded eviction while it stays
// empty and unsettled, so a long-lived connection cannot accumulate overflow
// records.
func (s *Scheduler) overflowLockedStream(stream *schedStream) {
	s.discardLocked(stream)
	stream.overflowed = true
	s.listRetiredLocked(stream)
}

// Dequeue removes and returns the next ready outbound envelope, or false when
// no stream has a queued frame. The returned Bytes are immutable and owned by
// the caller.
func (s *Scheduler) Dequeue() (OutboundEnvelope, bool) {
	if s == nil {
		return OutboundEnvelope{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.active) == 0 {
		return OutboundEnvelope{}, false
	}
	if s.controlBurst > 0 {
		if index := s.firstControlLocked(); index >= 0 {
			s.controlBurst--
			return s.serveIndexLocked(index), true
		}
	}
	envelope := s.serveDRRLocked()
	if envelope.Class == FrameData {
		s.controlBurst = MaxMuxSchedulerControlBurst
	}
	return envelope, true
}

// Reset settles one stream locally: it discards the stream's queued frames,
// frees their bytes immediately, and refuses every later frame for that
// identity. It reports whether it settled a stream that was not already
// settled, so a repeated cancel is idempotent. It never touches the physical
// connection or a sibling stream.
func (s *Scheduler) Reset(physical PhysicalStreamID) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := s.streams[physical]
	if stream == nil {
		if !s.admittedLocked(physical) {
			return false
		}
		stream = &schedStream{physical: physical, sealed: true}
		s.streams[physical] = stream
		s.listRetiredLocked(stream)
		return true
	}
	if stream.sealed && len(stream.queue) == 0 {
		return false
	}
	s.discardLocked(stream)
	stream.sealed = true
	s.listRetiredLocked(stream)
	return true
}

// AggregateBytes returns the outbound bytes currently charged across every
// stream. Application data never exceeds MaxMuxAggregateBytes; the only
// possible excess is the terminal an overflowed stream retains, at most one
// bounded envelope per live or retained overflowed stream, which is what keeps
// peer notification possible.
func (s *Scheduler) AggregateBytes() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aggregate
}

// QueuedFrames returns the number of frames queued for one stream.
func (s *Scheduler) QueuedFrames(physical PhysicalStreamID) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if stream := s.streams[physical]; stream != nil {
		return len(stream.queue)
	}
	return 0
}

// QueuedBytes returns the outbound bytes currently queued for one stream.
func (s *Scheduler) QueuedBytes(physical PhysicalStreamID) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if stream := s.streams[physical]; stream != nil {
		return stream.bytes
	}
	return 0
}

// Active returns the number of streams with a queued frame.
func (s *Scheduler) Active() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

// Pending reports whether any stream has a queued frame.
func (s *Scheduler) Pending() bool { return s.Active() > 0 }

// admittedLocked reports whether the admission gate permits scheduling for one
// physical stream: a nil engine admits any nonzero identity, and an installed
// engine admits a stream it tracks in opening or open state. A closing stream
// (an orderly inbound Close with unread data) accepts no new outbound frame,
// so it is not admitted; its one terminal Close or Reset still bypasses the
// gate through knownLocked.
func (s *Scheduler) admittedLocked(physical PhysicalStreamID) bool {
	if physical == 0 {
		return false
	}
	if s.engine == nil {
		return true
	}
	status, ok := s.engine.Status(physical)
	return ok && (status.State == StreamOpening || status.State == StreamOpen)
}

// knownLocked reports whether one physical stream identity is known to the
// admission gate at all: a nil engine trusts any nonzero identity, and an
// installed engine knows a stream it tracks in any state, including a settled
// one. It backs the single terminal-frame exception: a terminal Close or Reset
// may be enqueued once for a settled identity to signal the peer.
func (s *Scheduler) knownLocked(physical PhysicalStreamID) bool {
	if physical == 0 {
		return false
	}
	if s.engine == nil {
		return true
	}
	_, ok := s.engine.Status(physical)
	return ok
}

// listRetiredLocked records one settled or overflowed, empty stream for
// bounded eviction and evicts the oldest retired record past
// MaxRetiredSchedulerRecords. An evicted identity falls back to the engine
// gate for admission, so the ErrSchedulerSettled refusal is guaranteed only
// within this retention window and a late terminal may instead be accepted
// once more and harmlessly discarded by the peer's stream fencing. A record
// is listed at most once and only while it is
// sealed or overflowed and empty; a still-draining record stays until it
// empties, and an overflowed record that later takes its one terminal is
// sealed in place. A record that took that terminal after it was listed keeps
// its queued frame past eviction instead of being forgotten: eviction clears
// its retired flag, so draining its last frame re-lists it and a later
// eviction reclaims the record rather than orphaning it in the stream table.
func (s *Scheduler) listRetiredLocked(stream *schedStream) {
	if stream.retired || !(stream.sealed || stream.overflowed) || len(stream.queue) != 0 {
		return
	}
	stream.retired = true
	s.retired = append(s.retired, stream)
	for len(s.retired) > MaxRetiredSchedulerRecords {
		evicted := s.retired[0]
		s.retired = s.retired[1:]
		if (evicted.sealed || evicted.overflowed) && len(evicted.queue) == 0 {
			delete(s.streams, evicted.physical)
			continue
		}
		// The terminal this record accepted after it was listed is still
		// queued, so the record cannot be reclaimed yet. Clear the flag so
		// draining its last frame re-lists it for a later eviction.
		evicted.retired = false
	}
}

// overflowLocked reports whether one frame would exceed the per-stream frame
// bound, the per-stream byte bound, or the aggregate byte bound.
func (s *Scheduler) overflowLocked(stream *schedStream, n int) bool {
	return len(stream.queue) >= s.maxStreamFrames ||
		stream.bytes+n > s.maxStreamBytes ||
		s.aggregate+n > s.maxAggregateBytes
}

// discardLocked drops every queued frame of one stream and returns its bytes
// to the aggregate budget. The stream record stays so a repeat cancel is
// idempotent and a later frame is refused.
func (s *Scheduler) discardLocked(stream *schedStream) {
	s.aggregate -= stream.bytes
	stream.bytes = 0
	for i := range stream.queue {
		stream.queue[i] = schedFrame{}
	}
	stream.queue = nil
	if stream.active {
		s.removeActiveLocked(stream)
	}
}

// removeActiveLocked takes one stream out of the active ring.
func (s *Scheduler) removeActiveLocked(stream *schedStream) {
	for i, candidate := range s.active {
		if candidate != stream {
			continue
		}
		last := len(s.active) - 1
		copy(s.active[i:], s.active[i+1:])
		s.active[last] = nil
		s.active = s.active[:last]
		break
	}
	stream.active = false
}

// firstControlLocked returns the index of the first active stream whose oldest
// frame is control, or -1 when none is. It prefers the head position in the
// ring so the control priority stays round-robin.
func (s *Scheduler) firstControlLocked() int {
	for i, stream := range s.active {
		if stream.queue[0].class == FrameControl {
			return i
		}
	}
	return -1
}

// serveIndexLocked writes the oldest frame of the active stream at index. The
// stream's byte-deficit account is topped up and charged before the frame is
// removed, and the stream rotates to the back of the ring.
func (s *Scheduler) serveIndexLocked(index int) OutboundEnvelope {
	stream := s.active[index]
	s.topUpLocked(stream)
	stream.deficit -= int64(len(stream.queue[0].bytes))
	s.rotateToBackLocked(index)
	return s.popHeadLocked(stream)
}

// serveDRRLocked runs one byte-deficit round-robin step: it visits active
// streams in ring order, tops up and tests the head of each, and writes the
// first head that fits its accumulated deficit. When no head fits in a pass,
// every stream was topped up, so another pass is guaranteed to make progress.
func (s *Scheduler) serveDRRLocked() OutboundEnvelope {
	for {
		for i, stream := range s.active {
			s.topUpLocked(stream)
			size := int64(len(stream.queue[0].bytes))
			if size <= stream.deficit {
				stream.deficit -= size
				s.rotateToBackLocked(i)
				return s.popHeadLocked(stream)
			}
		}
	}
}

// topUpLocked adds one quantum to a stream's deficit unconditionally and caps
// the result at one quantum plus the largest frame the codec admits
// (MaxMuxApplicationBytes). Unconditional top-up is what guarantees progress:
// a head frame larger than the quantum accumulates one quantum per visit until
// it fits, while the cap bounds the deficit a stream can bank and keeps the
// schedule fair.
func (s *Scheduler) topUpLocked(stream *schedStream) {
	stream.deficit += s.quantum
	if maxDeficit := s.quantum + int64(min(s.maxEnvelopeBytes, MaxMuxApplicationBytes)); stream.deficit > maxDeficit {
		stream.deficit = maxDeficit
	}
}

// rotateToBackLocked moves the active stream at index to the back of the ring,
// preserving the relative order of every other stream and allocating nothing.
func (s *Scheduler) rotateToBackLocked(index int) {
	last := len(s.active) - 1
	if index == last {
		return
	}
	stream := s.active[index]
	copy(s.active[index:], s.active[index+1:])
	s.active[last] = stream
}

// popHeadLocked removes and returns the head frame of a stream that the caller
// already rotated to the back of the ring, releasing its bytes from both
// budgets. An emptied stream leaves the ring.
func (s *Scheduler) popHeadLocked(stream *schedStream) OutboundEnvelope {
	frame := stream.queue[0]
	stream.queue[0] = schedFrame{}
	stream.queue = stream.queue[1:]
	n := len(frame.bytes)
	stream.bytes -= n
	s.aggregate -= n
	if len(stream.queue) == 0 {
		last := len(s.active) - 1
		s.active[last] = nil
		s.active = s.active[:last]
		stream.active = false
		s.listRetiredLocked(stream)
	}
	return OutboundEnvelope{
		Physical:  stream.physical,
		Direction: frame.direction,
		Class:     frame.class,
		Bytes:     frame.bytes,
		Client:    frame.client,
		Server:    frame.server,
	}
}

// clientSeals reports whether a client message ends its stream for scheduling:
// a Close or Reset frame is the last frame the stream may enqueue, so a later
// frame is refused while the terminal frame drains in per-stream order.
func clientSeals(message ClientMessage) bool {
	switch message.(type) {
	case Close, *Close, Reset, *Reset:
		return true
	default:
		return false
	}
}

// serverSeals reports whether a server message ends its stream for scheduling.
func serverSeals(message ServerMessage) bool {
	switch message.(type) {
	case Close, *Close, Reset, *Reset:
		return true
	default:
		return false
	}
}

// cloneClientMessage returns a committed copy of one client message. Byte and
// string slices the caller still owns are copied so a dequeued envelope never
// aliases caller memory; every other field is a value type and is copied by
// assignment. Unknown concrete types are returned unchanged (they were already
// refused by the codec).
func cloneClientMessage(message ClientMessage) ClientMessage {
	switch m := message.(type) {
	case Data:
		return Data{Physical: m.Physical, Data: append([]byte(nil), m.Data...)}
	case *Data:
		if m == nil {
			return m
		}
		committed := *m
		committed.Data = append([]byte(nil), m.Data...)
		return &committed
	case Open:
		return m.clone()
	case *Open:
		if m == nil {
			return m
		}
		committed := m.clone()
		return &committed
	default:
		return message
	}
}

// clone returns an Open whose environment slice is owned by the scheduler.
func (m Open) clone() Open {
	m.Env = append([]string(nil), m.Env...)
	return m
}

// cloneServerMessage returns a committed copy of one server message under the
// same rule as cloneClientMessage: only the opaque data payload can alias
// caller memory.
func cloneServerMessage(message ServerMessage) ServerMessage {
	switch m := message.(type) {
	case Data:
		return Data{Physical: m.Physical, Data: append([]byte(nil), m.Data...)}
	case *Data:
		if m == nil {
			return m
		}
		committed := *m
		committed.Data = append([]byte(nil), m.Data...)
		return &committed
	default:
		return message
	}
}

// clientFrame resolves the physical stream identity and scheduling class of one
// broker-to-daemon message before it is encoded. Unknown concrete types fail
// closed with ErrWrongDirection, mirroring the codec.
func clientFrame(message ClientMessage) (PhysicalStreamID, FrameClass, error) {
	switch m := message.(type) {
	case Open:
		return m.Ref.Physical, FrameControl, nil
	case *Open:
		if m == nil {
			return 0, 0, ErrInvalidMessage
		}
		return clientFrame(*m)
	case Data:
		return m.Physical, FrameData, nil
	case *Data:
		if m == nil {
			return 0, 0, ErrInvalidMessage
		}
		return clientFrame(*m)
	case Close:
		return m.Physical, FrameControl, nil
	case *Close:
		if m == nil {
			return 0, 0, ErrInvalidMessage
		}
		return clientFrame(*m)
	case Reset:
		return m.Physical, FrameControl, nil
	case *Reset:
		if m == nil {
			return 0, 0, ErrInvalidMessage
		}
		return clientFrame(*m)
	default:
		return 0, 0, ErrWrongDirection
	}
}

// serverFrame resolves the physical stream identity and scheduling class of one
// daemon-to-broker message before it is encoded.
func serverFrame(message ServerMessage) (PhysicalStreamID, FrameClass, error) {
	switch m := message.(type) {
	case Opened:
		return m.Ref.Physical, FrameControl, nil
	case *Opened:
		if m == nil {
			return 0, 0, ErrInvalidMessage
		}
		return serverFrame(*m)
	case Refused:
		return m.Ref.Physical, FrameControl, nil
	case *Refused:
		if m == nil {
			return 0, 0, ErrInvalidMessage
		}
		return serverFrame(*m)
	case Data:
		return m.Physical, FrameData, nil
	case *Data:
		if m == nil {
			return 0, 0, ErrInvalidMessage
		}
		return serverFrame(*m)
	case Close:
		return m.Physical, FrameControl, nil
	case *Close:
		if m == nil {
			return 0, 0, ErrInvalidMessage
		}
		return serverFrame(*m)
	case Reset:
		return m.Physical, FrameControl, nil
	case *Reset:
		if m == nil {
			return 0, 0, ErrInvalidMessage
		}
		return serverFrame(*m)
	default:
		return 0, 0, ErrWrongDirection
	}
}
