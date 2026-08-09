package daemon

import (
	"errors"
	"sync"
	"sync/atomic"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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
	stateMu              sync.Mutex
}

// lockView serializes an attachment's view publication with every output
// state transition. It deliberately does not take sendMu: callers that hold
// architecture locks can publish a view without reversing the paint path's
// sendMu -> session lock order.
func (s *outputStateStream) lockView() {
	if s == nil {
		return
	}
	if s.attachment == nil {
		s.stateMu.Lock()
		return
	}
	s.attachment.viewMu.Lock()
}

func (s *outputStateStream) unlockView() {
	if s == nil {
		return
	}
	if s.attachment == nil {
		s.stateMu.Unlock()
		return
	}
	s.attachment.viewMu.Unlock()
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
	s.lockView()
	defer s.unlockView()
	if s.epoch == 0 {
		s.epoch = 1
	}
	if reset && s.initialized {
		s.rebaseLocked()
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
	generation, epoch, viewRevision := s.fenceLocked()
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
	if p == nil || p.stream == nil || p.attempted {
		return nil
	}
	p.stream.lockView()
	defer p.stream.unlockView()
	p.attempted = true
	if !p.currentLocked() {
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
	out, err := marshalOutputState(data, p.epoch, base, p.next, echoAck, p.viewRevision, p.size, p.reset)
	if err != nil {
		p.stream.forceSnapshot = true
		return err
	}
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
	if p == nil || p.stream == nil || p.attempted {
		return
	}
	p.stream.lockView()
	defer p.stream.unlockView()
	p.attempted = true
	if !p.currentLocked() || len(p.data) != 0 {
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
	s.lockView()
	defer s.unlockView()
	return s.currentEpochLocked()
}

func (s *outputStateStream) currentEpochLocked() uint64 {
	if s.epoch == 0 {
		s.epoch = 1
	}
	return s.epoch
}

func (s *outputStateStream) fenceLocked() (uint64, uint64, uint64) {
	if s == nil {
		return 0, 1, 0
	}
	generation, revision := uint64(0), uint64(0)
	if s.attachment != nil {
		generation = s.attachment.connectionGeneration.Load()
		revision = s.attachment.view.revision
	}
	return generation, s.currentEpochLocked(), revision
}

func (s *outputStateStream) preparedCurrentLocked(generation, epoch, viewRevision, base uint64) bool {
	if s == nil || s.currentEpochLocked() != epoch || s.generation != generation || s.next != base {
		return false
	}
	if s.attachment == nil {
		return true
	}
	return s.attachment.view.revision == viewRevision
}

func (p *preparedOutput) currentLocked() bool {
	return p != nil && p.stream != nil && p.stream.preparedCurrentLocked(p.generation, p.epoch, p.viewRevision, p.base) &&
		(p.stream.attachment == nil || p.stream.attachment.connectionGeneration.Load() == p.connectionGeneration)
}

func marshalOutputState(data []byte, epoch, base, next, echoAck, viewRevision uint64, size domain.Size, full bool) (ports.Frame, error) {
	payload, err := ports.MarshalOutput(ports.Output{
		Epoch: epoch, Base: base, New: next, Echo: echoAck, ViewRevision: viewRevision,
		Size: size, Full: full, Data: data,
	})
	if err != nil {
		return ports.Frame{}, err
	}
	return ports.Frame{Type: ports.MsgOutput, Payload: payload}, nil
}

// sideEffect builds an output frame without advancing the state stream. When
// an attachment is present, callers must hold its sendMu; attachment-bound
// sends use sideEffectLocked after validating their transport incarnation.
func (s *outputStateStream) sideEffect(data []byte, echoAck uint64) (ports.Frame, error) {
	s.lockView()
	defer s.unlockView()
	return s.sideEffectLocked(data, echoAck)
}

// sideEffectLocked requires the attachment view lock for both frame creation
// and transport emission. Keeping that lock across the send prevents a view
// publication from overtaking a frame that was fenced just before it.
func (s *outputStateStream) sideEffectLocked(data []byte, echoAck uint64) (ports.Frame, error) {
	_, epoch, viewRevision := s.fenceLocked()
	size := domain.Size{Cols: 1, Rows: 1}
	if s != nil && s.attachment != nil {
		size = s.attachment.sizeSnapshot()
	}
	return marshalOutputState(data, epoch, 0, 0, echoAck, viewRevision, size, false)
}

func (s *outputStateStream) ack(epoch, state uint64) bool {
	if s == nil {
		return false
	}
	s.lockView()
	defer s.unlockView()
	if epoch != s.currentEpochLocked() || state > s.next || state <= s.acked {
		return false
	}
	s.acked = state
	s.publishOutstanding()
	return true
}

func (s *outputStateStream) rebase() {
	if s == nil {
		return
	}
	s.lockView()
	defer s.unlockView()
	s.rebaseLocked()
}

// rebaseLocked is called while the attachment view lock is held. Publishing
// the next view revision takes this path before making that revision visible.
func (s *outputStateStream) rebaseLocked() {
	if s == nil {
		return
	}
	epoch := s.currentEpochLocked()
	if epoch == ^uint64(0) {
		return
	}
	s.epoch = epoch + 1
	s.next = 0
	s.acked = 0
	s.generation++
	s.publishOutstanding()
	s.maxOutstandingAtomic.Store(s.maxOutstanding)
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
	s.lockView()
	defer s.unlockView()
	s.maxOutstandingAtomic.Store(s.maxOutstanding)
	s.publishOutstanding()
}

func (s *outputStateStream) atCapacity() bool {
	if s == nil {
		return false
	}
	maxOutstanding := s.maxOutstandingAtomic.Load()
	if maxOutstanding == 0 {
		maxOutstanding = s.maxOutstanding
	}
	return s.outstandingAtomic.Load() >= maxOutstanding
}

func (s *outputStateStream) outstanding() uint64 {
	if s == nil {
		return 0
	}
	s.lockView()
	defer s.unlockView()
	return s.next - s.acked
}
