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
	deferred       bool
	deferredReset  bool
	maxOutstanding uint64
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

func (s *outputStateStream) render(frame renderer.Frame, damage []renderer.Damage, reset bool) ([]byte, error) {
	if reset {
		s.renderer.Reset()
	}
	return s.renderer.Draw(frame, damage)
}

type preparedOutput struct {
	stream *outputStateStream
	next   uint64
	data   []byte
	reset  bool
}

func (s *outputStateStream) prepare(frame renderer.Frame, damage []renderer.Damage, reset bool) (*preparedOutput, error) {
	data, err := s.render(frame, damage, reset)
	if err != nil {
		return nil, err
	}
	return &preparedOutput{stream: s, next: s.next, data: data, reset: reset}, nil
}

func (p *preparedOutput) send(data []byte, echoAck uint64, send func(ports.Frame) error) error {
	out := p.stream.frame(data, p.reset, echoAck)
	if err := send(out); err != nil {
		p.stream.renderer.Reset()
		p.stream.next = p.next
		return err
	}
	return nil
}

func (s *outputStateStream) frame(data []byte, reset bool, echoAck uint64) ports.Frame {
	s.next++
	base := s.next - 1
	if reset {
		base = 0
	}
	frame := frameOutputState(data, base, s.next, echoAck)
	return frame
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
	s.clearDeferred()
}

func (s *outputStateStream) atCapacity() bool {
	return s.outstanding() >= s.maxOutstanding
}

func (s *outputStateStream) deferIfAtCapacity(reset bool) bool {
	if !s.atCapacity() {
		if reset {
			s.clearDeferred()
		}
		return false
	}
	s.deferred = true
	s.deferredReset = s.deferredReset || reset
	return true
}

func (s *outputStateStream) takeDeferred() (reset bool, ok bool) {
	if !s.deferred || s.atCapacity() {
		return false, false
	}
	reset = s.deferredReset
	s.clearDeferred()
	return reset, true
}

func (s *outputStateStream) clearDeferred() {
	s.deferred = false
	s.deferredReset = false
}

func (s *outputStateStream) outstanding() uint64 { return s.next - s.acked }
