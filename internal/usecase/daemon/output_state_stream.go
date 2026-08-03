package daemon

import (
	"errors"
	"sync/atomic"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

// outputStateStream owns the terminal-output dependency chain for one
// attached client. Each incremental output is rendered from the preceding
// emitted frame and therefore remains pipelineable without waiting for an
// ACK. Cumulative ACKs advance the bounded paint window.
// A reset frame starts a new epoch, clears the dependency chain, and advertises
// state 0 as its base.
//
// Callers serialize access with attachedClient.sendMu. A prepared transaction
// must be sent or committed as a no-op before the next transaction is prepared.
type outputStateStream struct {
	renderer             *renderer.Renderer
	epoch                uint64
	next                 uint64
	acked                uint64
	generation           uint64
	maxOutstanding       uint64
	outstandingAtomic    atomic.Uint64
	maxOutstandingAtomic atomic.Uint64
	forceSnapshot        bool
	initialized          bool
	attachment           *attachedClient
}

func newOutputStateStream(windowSize ...uint8) *outputStateStream {
	window := uint8(maxUnackedOutputStates)
	if len(windowSize) > 0 {
		window = normalizeOutputWindow(windowSize[0])
	}
	stream := &outputStateStream{
		renderer:       renderer.New(renderer.Capabilities{}),
		epoch:          1,
		maxOutstanding: uint64(window),
	}
	stream.maxOutstandingAtomic.Store(uint64(window))
	stream.publishOutstanding()
	return stream
}

type preparedOutput struct {
	stream               *outputStateStream
	draw                 renderer.PreparedDraw
	base                 uint64
	next                 uint64
	epoch                uint64
	generation           uint64
	connectionGeneration uint64
	viewRevision         uint64
	size                 domain.Size
	data                 []byte
	reset                bool
	attempted            bool
	sent                 bool
}

func (s *outputStateStream) prepare(frame renderer.Frame, damage []renderer.Damage, reset bool) (*preparedOutput, error) {
	if s.epoch == 0 {
		s.epoch = 1
	}
	if reset && s.initialized {
		s.rebase()
	}
	reset = reset || s.forceSnapshot || !s.initialized
	if s.next == ^uint64(0) {
		return nil, errors.New("output state number exhausted")
	}
	draw, err := s.renderer.Prepare(frame, damage, reset)
	if err != nil {
		return nil, err
	}
	s.initialized = true
	generation, epoch, viewRevision := s.fence()
	return &preparedOutput{
		stream:               s,
		draw:                 draw,
		base:                 s.next,
		next:                 s.next + 1,
		epoch:                epoch,
		generation:           s.generation,
		connectionGeneration: generation,
		viewRevision:         viewRevision,
		size:                 domain.Size{Cols: frame.Width, Rows: frame.Height},
		data:                 draw.Bytes(),
		reset:                reset,
	}, nil
}

func (p *preparedOutput) send(data []byte, echoAck uint64, send func(ports.Frame) error) error {
	if p == nil || p.attempted {
		return nil
	}
	p.attempted = true
	if !p.current() {
		return nil
	}
	if len(data) == 0 {
		p.commit()
		p.sent = true
		return nil
	}
	base := p.base
	if p.reset {
		base = 0
	}
	out := marshalOutputState(data, p.epoch, base, p.next, echoAck, p.viewRevision, p.size, p.reset)
	if err := send(out); err != nil {
		p.stream.forceSnapshot = true
		return err
	}
	p.commit()
	p.stream.next = p.next
	p.stream.publishOutstanding()
	p.sent = true
	return nil
}

func (p *preparedOutput) commitNoSend() {
	if p == nil || p.attempted {
		return
	}
	p.attempted = true
	if !p.current() || len(p.data) != 0 {
		return
	}
	p.commit()
	p.sent = true
}

func (p *preparedOutput) commit() {
	p.draw.Commit()
	p.stream.forceSnapshot = false
}

func (s *outputStateStream) currentEpoch() uint64 {
	if s.epoch == 0 {
		s.epoch = 1
	}
	return s.epoch
}

func (s *outputStateStream) fence() (uint64, uint64, uint64) {
	if s == nil {
		return 0, 1, 0
	}
	generation, revision := uint64(0), uint64(0)
	if s.attachment != nil {
		generation = s.attachment.connectionGeneration.Load()
		revision = s.attachment.viewSnapshot().revision
	}
	return generation, s.currentEpoch(), revision
}

func (s *outputStateStream) preparedCurrent(generation, epoch, viewRevision, base uint64) bool {
	if s == nil || s.currentEpoch() != epoch || s.generation != generation || s.next != base {
		return false
	}
	if s.attachment == nil {
		return true
	}
	return s.attachment.viewSnapshot().revision == viewRevision
}

func (p *preparedOutput) current() bool {
	return p != nil && p.stream != nil && p.stream.preparedCurrent(p.generation, p.epoch, p.viewRevision, p.base) &&
		(p.stream.attachment == nil || p.stream.attachment.connectionGeneration.Load() == p.connectionGeneration)
}

func marshalOutputState(data []byte, epoch, base, next, echoAck, viewRevision uint64, size domain.Size, full bool) ports.Frame {
	if !size.Valid() {
		size = domain.Size{Cols: 1, Rows: 1}
	}
	return ports.Frame{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{
		Epoch: epoch, Base: base, New: next, Echo: echoAck, ViewRevision: viewRevision,
		Size: size, Full: full, Data: data,
	})}
}

// sideEffect builds an output frame without advancing the state stream. When
// an attachment is present, callers must hold its sendMu; attachment-bound
// sends use sideEffectLocked after validating their transport incarnation.
func (s *outputStateStream) sideEffect(data []byte, echoAck uint64) ports.Frame {
	return s.sideEffectLocked(data, echoAck)
}

func (s *outputStateStream) sideEffectLocked(data []byte, echoAck uint64) ports.Frame {
	_, epoch, viewRevision := s.fence()
	size := domain.Size{Cols: 1, Rows: 1}
	if s != nil && s.attachment != nil && s.attachment.size.Valid() {
		size = s.attachment.size
	}
	return marshalOutputState(data, epoch, 0, 0, echoAck, viewRevision, size, false)
}

func (s *outputStateStream) ack(values ...uint64) {
	if s == nil {
		return
	}
	epoch, state := s.currentEpoch(), uint64(0)
	switch len(values) {
	case 1:
		state = values[0]
	case 2:
		epoch, state = values[0], values[1]
	default:
		return
	}
	if epoch != s.currentEpoch() || state > s.next || state <= s.acked {
		return
	}
	s.acked = state
	s.publishOutstanding()
}

func (s *outputStateStream) rebase() {
	if s == nil {
		return
	}
	epoch := s.currentEpoch()
	if epoch == ^uint64(0) {
		return
	}
	s.epoch = epoch + 1
	s.next = 0
	s.acked = 0
	s.generation++
	s.maxOutstandingAtomic.Store(s.maxOutstanding)
	s.publishOutstanding()
	s.renderer.Reset()
	s.forceSnapshot = true
	s.initialized = false
}

func (s *outputStateStream) publishOutstanding() {
	if s == nil {
		return
	}
	s.outstandingAtomic.Store(s.next - s.acked)
}

// syncCapacityLocked refreshes the lock-free readiness snapshot after a
// caller mutates the legacy fields under attachedClient.sendMu.
func (s *outputStateStream) syncCapacityLocked() {
	if s == nil {
		return
	}
	s.maxOutstandingAtomic.Store(s.maxOutstanding)
	s.publishOutstanding()
}

func (s *outputStateStream) atCapacity() bool {
	if s == nil {
		return false
	}
	return s.outstandingAtomic.Load() >= s.maxOutstandingAtomic.Load()
}

func (s *outputStateStream) outstanding() uint64 { return s.next - s.acked }
