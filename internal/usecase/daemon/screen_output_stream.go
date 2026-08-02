package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

// structuredOutputStream is the semantic counterpart of outputStateStream.
// The output state owns the shared state-number and ACK window; this stream
// owns only its planner shadow and the last committed absolute cursor.
// Callers serialize access with attachedClient.sendMu.
type structuredOutputStream struct {
	state *outputStateStream

	shadow    renderer.Frame
	cursorOut cursorOut

	forceSnapshot bool
}

// newStructuredOutputStream accepts the shared output state so ANSI and
// structured output use one dependency chain and one bounded ACK window.
func newStructuredOutputStream(state *outputStateStream) *structuredOutputStream {
	return &structuredOutputStream{state: state, forceSnapshot: true}
}

// prepare creates an owned structured payload without changing state.
func (s *structuredOutputStream) prepare(frame renderer.Frame, damage []renderer.Damage, cursor cursorOut, reset bool, echoAck uint64) (*preparedStructuredOutput, error) {
	if s == nil || s.state == nil {
		return nil, errors.New("structured output stream is nil")
	}
	if err := frame.Validate(); err != nil {
		return nil, err
	}

	reset = reset || s.forceSnapshot || s.state.next == 0 || s.shadow.Validate() != nil ||
		s.shadow.Width != frame.Width || s.shadow.Height != frame.Height
	candidate, err := renderer.PlanDelta(frame, damage, s.shadow, reset)
	if err != nil {
		return nil, err
	}

	plan := candidate.Plan
	snapshot := reset || plan.Snapshot
	cursorChanged := cursor != s.cursorOut
	if !snapshot && plan.Scroll.Height == 0 && len(plan.Spans) == 0 && !cursorChanged {
		return &preparedStructuredOutput{
			stream:    s,
			candidate: candidate,
			cursor:    cursor,
			echoAck:   echoAck,
		}, nil
	}
	if s.state.next == ^uint64(0) {
		return nil, errors.New("structured output state number exhausted")
	}

	wireCursor, err := wireScreenCursor(cursor, frame)
	if err != nil {
		return nil, err
	}
	update := ports.ScreenUpdate{
		BaseStateNum: s.state.next,
		NewStateNum:  s.state.next + 1,
		EchoAck:      echoAck,
		Kind:         ports.ScreenUpdateDelta,
		Size:         frameSize(frame),
		Cursor:       wireCursor,
	}
	if snapshot {
		update.BaseStateNum = 0
		update.Kind = ports.ScreenUpdateSnapshot
		update.Spans = make([]ports.ScreenSpan, frame.Height)
		for y := range frame.Height {
			update.Spans[y] = ports.ScreenSpan{Y: uint16(y), Cells: copyScreenCells(frame.Row(y))}
		}
	} else {
		if plan.Scroll.Height != 0 {
			if plan.Scroll.Y < 0 || plan.Scroll.Height <= 0 || plan.Scroll.Count <= 0 ||
				plan.Scroll.Y+plan.Scroll.Height > frame.Height || plan.Scroll.Count >= plan.Scroll.Height {
				return nil, errors.New("structured output: invalid planned scroll")
			}
			update.Scroll = &ports.ScreenScroll{
				Top:    uint16(plan.Scroll.Y),
				Height: uint16(plan.Scroll.Height),
				Count:  uint16(plan.Scroll.Count),
			}
		}
		update.Spans = make([]ports.ScreenSpan, 0, len(plan.Spans))
		for _, span := range plan.Spans {
			if span.Y < 0 || span.X < 0 || span.Width <= 0 || span.Y >= frame.Height || span.X+span.Width > frame.Width {
				return nil, errors.New("structured output: invalid planned span")
			}
			update.Spans = append(update.Spans, ports.ScreenSpan{
				Y:     uint16(span.Y),
				X:     uint16(span.X),
				Cells: copyScreenCells(frame.Row(span.Y)[span.X : span.X+span.Width]),
			})
		}
	}
	data, err := ports.MarshalScreenUpdate(update)
	if err != nil {
		return nil, err
	}
	return &preparedStructuredOutput{
		stream:    s,
		candidate: candidate,
		cursor:    cursor,
		update:    update,
		next:      update.NewStateNum,
		data:      data,
		echoAck:   echoAck,
		snapshot:  snapshot,
	}, nil
}

func frameSize(frame renderer.Frame) domain.Size {
	return domain.Size{Cols: frame.Width, Rows: frame.Height}
}

func copyScreenCells(cells []renderer.Cell) []renderer.Cell {
	return append([]renderer.Cell(nil), cells...)
}

func wireScreenCursor(cursor cursorOut, frame renderer.Frame) (ports.ScreenCursor, error) {
	out := ports.ScreenCursor{Visible: cursor.valid && !cursor.hidden}
	if !cursor.valid {
		return out, nil
	}
	if cursor.row < 0 || cursor.row >= frame.Height || cursor.col < 0 || cursor.col >= frame.Width {
		return ports.ScreenCursor{}, errors.New("structured output: cursor outside frame")
	}
	if cursor.hasStyle {
		if cursor.style < 0 || cursor.style > 6 {
			return ports.ScreenCursor{}, errors.New("structured output: cursor style out of range")
		}
		out.Style = uint8(cursor.style)
		out.StyleSet = true
	} else if cursor.style != 0 {
		return ports.ScreenCursor{}, errors.New("structured output: cursor style is not set")
	}
	out.Row = uint16(cursor.row)
	out.Col = uint16(cursor.col)
	return out, nil
}

type preparedStructuredOutput struct {
	stream    *structuredOutputStream
	candidate renderer.DeltaCandidate
	cursor    cursorOut
	update    ports.ScreenUpdate
	next      uint64
	echoAck   uint64
	data      []byte
	snapshot  bool
	completed bool
	committed bool
}

func (p *preparedStructuredOutput) send(sender func(ports.Frame) error) error {
	if p == nil || p.completed {
		return nil
	}
	if len(p.data) == 0 {
		p.completed = true
		p.commit()
		return nil
	}
	if sender == nil {
		p.completed = true
		p.stream.forceSnapshot = true
		return errors.New("structured output: nil sender")
	}
	if err := sender(ports.Frame{Type: ports.MsgScreenUpdate, Payload: p.data}); err != nil {
		p.completed = true
		p.stream.forceSnapshot = true
		return err
	}
	p.completed = true
	p.commit()
	p.stream.state.next = p.next
	return nil
}

// commitNoSend commits a prepared noop without adding a state-bearing frame.
// A failed send is already completed, so it cannot accidentally commit after
// the failure and thereby destroy the required snapshot retry.
func (p *preparedStructuredOutput) commitNoSend() {
	if p == nil || p.completed {
		return
	}
	p.completed = true
	p.commit()
}

func (p *preparedStructuredOutput) commit() {
	if p == nil || p.committed || p.stream == nil {
		return
	}
	p.candidate.Commit(&p.stream.shadow)
	p.stream.cursorOut = p.cursor
	p.stream.forceSnapshot = false
	p.committed = true
}

func (s *structuredOutputStream) ack(state uint64) {
	if s != nil && s.state != nil {
		s.state.ack(state)
	}
}

func (s *structuredOutputStream) atCapacity() bool {
	return s == nil || s.state == nil || s.state.atCapacity()
}

func (s *structuredOutputStream) outstanding() uint64 {
	if s == nil || s.state == nil {
		return 0
	}
	return s.state.outstanding()
}

// rebase discards only the local planner dependency. The shared state number
// is rebased once by attachedClient.rebaseOutput.
func (s *structuredOutputStream) rebase() {
	if s == nil {
		return
	}
	s.shadow = renderer.Frame{}
	s.cursorOut = cursorOut{}
	s.forceSnapshot = true
}
