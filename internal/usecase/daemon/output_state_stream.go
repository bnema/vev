package daemon

import (
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

// outputStateStream owns the terminal-output dependency chain for one
// attached client. Each incremental output is rendered from the preceding
// emitted frame and therefore remains pipelineable without waiting for an
// ACK. Cumulative ACKs advance the bounded paint window.
// A reset frame is the only dependency-free output and advertises state 0 as
// its base.
//
// Callers serialize access with attachedClient.sendMu.
type outputStateStream struct {
	renderer       *renderer.Renderer
	next           uint64
	acked          uint64
	maxOutstanding uint64
	forceSnapshot  bool
}

func newOutputStateStream(windowSize ...uint8) *outputStateStream {
	window := uint8(maxUnackedOutputStates)
	if len(windowSize) > 0 {
		window = normalizeOutputWindow(windowSize[0])
	}
	return &outputStateStream{
		renderer:       renderer.New(renderer.Capabilities{}),
		maxOutstanding: uint64(window),
	}
}

type preparedOutput struct {
	stream    *outputStateStream
	draw      renderer.PreparedDraw
	next      uint64
	data      []byte
	reset     bool
	completed bool
}

func (s *outputStateStream) prepare(frame renderer.Frame, damage []renderer.Damage, reset bool) (*preparedOutput, error) {
	reset = reset || s.forceSnapshot
	draw, err := s.renderer.Prepare(frame, damage, reset)
	if err != nil {
		return nil, err
	}
	return &preparedOutput{
		stream: s,
		draw:   draw,
		next:   s.next + 1,
		data:   draw.Bytes(),
		reset:  reset,
	}, nil
}

func (p *preparedOutput) send(data []byte, echoAck uint64, send func(ports.Frame) error) error {
	if p.completed {
		return nil
	}
	p.completed = true
	base := p.stream.next
	if p.reset {
		base = 0
	}
	out := frameOutputState(data, base, p.next, echoAck)
	if err := send(out); err != nil {
		p.stream.forceSnapshot = true
		return err
	}
	p.commit()
	p.stream.next = p.next
	return nil
}

func (p *preparedOutput) commitNoSend() {
	if p.completed {
		return
	}
	p.completed = true
	p.commit()
}

func (p *preparedOutput) commit() {
	p.draw.Commit()
	if p.reset {
		p.stream.forceSnapshot = false
	}
}

func (s *outputStateStream) sideEffect(data []byte, echoAck uint64) ports.Frame {
	return frameOutputState(data, 0, 0, echoAck)
}

func (s *outputStateStream) ack(state uint64) {
	if state > s.next || state <= s.acked {
		return
	}
	s.acked = state
}

func (s *outputStateStream) rebase() {
	s.acked = s.next
	s.renderer.Reset()
	s.forceSnapshot = true
}

func (s *outputStateStream) atCapacity() bool {
	return s.outstanding() >= s.maxOutstanding
}

func (s *outputStateStream) outstanding() uint64 { return s.next - s.acked }
