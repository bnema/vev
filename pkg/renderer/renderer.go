package renderer

import (
	"bytes"
	"strconv"
	"sync"
)

const maxPooledBufferCap = 1 << 20

var bufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

type Capabilities struct {
	SynchronizedOutput bool
}

type Renderer struct {
	caps       Capabilities
	width      int
	height     int
	shadow     []Cell
	lineOffset []int
}

// PreparedDraw owns encoded output and its transactional delta until Commit.
// It is returned by value so ordinary prepared draws do not allocate.
type PreparedDraw struct {
	renderer  *Renderer
	candidate DeltaCandidate
	data      []byte
	committed bool
}

func New(caps Capabilities) *Renderer { return &Renderer{caps: caps} }

func (r *Renderer) Reset() {
	r.width = 0
	r.height = 0
	r.shadow = nil
	r.lineOffset = nil
}

// Bytes returns the prepared ANSI output. The returned bytes remain valid after
// another draw.
func (p PreparedDraw) Bytes() []byte { return p.data }

// Commit applies the prepared delta exactly once. Discarding it leaves the
// renderer's committed state unchanged.
func (p *PreparedDraw) Commit() {
	if p == nil || p.renderer == nil || p.committed {
		return
	}
	committed := p.renderer.committedFrame()
	p.candidate.Commit(&committed)
	p.renderer.setCommittedFrame(committed)
	p.committed = true
}

// Prepare plans and encodes a transactional draw. The renderer advances only
// when the returned draw is committed.
func (r *Renderer) Prepare(frame Frame, damage []Damage, reset bool) (PreparedDraw, error) {
	var candidate DeltaCandidate
	var err error
	if !reset && len(r.shadow) != 0 && r.width == frame.Width && r.height == frame.Height && len(damage) == 1 && (damage[0].Kind == DamageText || damage[0].Kind == DamageClear) {
		if err = frame.Validate(); err == nil {
			d := damage[0]
			x, y, width, height, ok := clampRect(frame, d.X, d.Y, d.Width, d.Height)
			plan := DeltaPlan{}
			if ok {
				if height > maxPlannedDamageSpans {
					plan.Snapshot = true
				} else {
					spans := make([]Span, height)
					for row := range height {
						spans[row] = Span{Y: y + row, X: x, Width: width}
					}
					if (frame.Height == 1 || len(spans) > 1) && deltaCostsSnapshot(frame, spans, false) {
						plan.Snapshot = true
					}
					plan.Spans = spans
				}
			}
			candidate = DeltaCandidate{Plan: plan, frame: frame}
		}
	} else {
		candidate, err = PlanDelta(frame, damage, r.committedFrame(), reset || len(r.shadow) == 0)
	}
	if err != nil {
		return PreparedDraw{}, err
	}
	prepared := PreparedDraw{renderer: r, candidate: candidate}
	plan := candidate.Plan
	if !plan.Snapshot && plan.Scroll.Height == 0 && len(plan.Spans) == 0 {
		return prepared, nil
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer putBuffer(buf)
	if r.caps.SynchronizedOutput {
		buf.WriteString(SyncStartCSI)
	}

	st := newDrawState()
	if plan.Snapshot {
		if len(plan.Spans) > 0 {
			r.emitDamageSpans(buf, frame, plan.Spans, &st)
		} else {
			r.writeFull(buf, frame, &st)
		}
	} else {
		if plan.Scroll.Height != 0 {
			scroll := plan.Scroll
			emitScrollUp(buf, Damage{Kind: DamageScrollUp, X: 0, Y: scroll.Y, Width: frame.Width, Height: scroll.Height, Count: scroll.Count})
		}
		for _, span := range plan.Spans {
			r.emitSpan(buf, frame, span.Y, span.X, span.Width, &st)
		}
		buf.WriteString("\x1b[0m")
	}
	if r.caps.SynchronizedOutput {
		buf.WriteString(SyncEndCSI)
	}
	prepared.data = copyBytes(buf)
	return prepared, nil
}

func (r *Renderer) Draw(frame Frame, damage []Damage) ([]byte, error) {
	prepared, err := r.Prepare(frame, damage, false)
	if err != nil {
		return nil, err
	}
	prepared.Commit()
	return prepared.Bytes(), nil
}

func (r *Renderer) committedFrame() Frame {
	return Frame{Width: r.width, Height: r.height, Cells: r.shadow, lineOffset: r.lineOffset}
}

func (r *Renderer) setCommittedFrame(frame Frame) {
	r.width = frame.Width
	r.height = frame.Height
	r.shadow = frame.Cells
	r.lineOffset = frame.lineOffset
}

// copyBytes copies the buffer contents into a fresh byte slice and is used
// to return output that is independent of the pooled scratch buffer.
func copyBytes(buf *bytes.Buffer) []byte {
	b := buf.Bytes()
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func putBuffer(buf *bytes.Buffer) {
	if buf.Cap() > maxPooledBufferCap {
		return
	}
	bufferPool.Put(buf)
}

func needsFull(damage []Damage) bool {
	if len(damage) == 0 {
		return false
	}
	for _, d := range damage {
		if d.Kind == DamageFullRedraw {
			return true
		}
	}
	return false
}

func hasScrollDamage(damage []Damage) bool {
	for _, d := range damage {
		if d.Kind == DamageScrollUp {
			return true
		}
	}
	return false
}

func (r *Renderer) writeFull(out *bytes.Buffer, frame Frame, st *drawState) {
	for y := range frame.Height {
		r.emitSpan(out, frame, y, 0, frame.Width, st)
	}
	out.WriteString("\x1b[0m")
}

func (r *Renderer) emitDamageSpans(out *bytes.Buffer, frame Frame, spans []Span, st *drawState) {
	for _, span := range spans {
		r.emitSpan(out, frame, span.Y, span.X, span.Width, st)
	}
	out.WriteString("\x1b[0m")
}
func clampRect(frame Frame, x, y, width, height int) (int, int, int, int, bool) {
	x, width, okX := clampRange(x, width, frame.Width)
	y, height, okY := clampRange(y, height, frame.Height)
	if !okX || !okY {
		return 0, 0, 0, 0, false
	}
	return x, y, width, height, true
}

// clampRange intersects [pos, pos+size) with [0, limit) without evaluating an
// overflowing endpoint from untrusted damage coordinates.
func clampRange(pos, size, limit int) (int, int, bool) {
	if size <= 0 || limit <= 0 || pos >= limit {
		return 0, 0, false
	}
	if pos < 0 {
		// -size is safe because size is positive. If pos is at or before that
		// point, the rectangle ends at or before zero.
		if pos <= -size {
			return 0, 0, false
		}
		end := pos + size // pos > -size proves this addition cannot overflow.
		if end > limit {
			end = limit
		}
		return 0, end, true
	}

	available := limit - pos
	return pos, min(size, available), true
}

// writeCursor emits a cursor-positioning CSI sequence without fmt.Fprintf
// allocations. It uses a stack-allocated buffer for integer formatting.
func writeCursor(out *bytes.Buffer, y, x int) {
	out.WriteString("\x1b[")
	var b [16]byte
	n := strconv.AppendInt(b[:0], int64(y+1), 10)
	out.Write(n)
	out.WriteByte(';')
	n = strconv.AppendInt(b[:0], int64(x+1), 10)
	out.Write(n)
	out.WriteByte('H')
}

// writeStyle emits SGR style parameters without fmt.Fprintf allocations.
func writeStyle(out *bytes.Buffer, style Style) {
	out.WriteString("\x1b[0")
	if style.Bold {
		out.WriteString(";1")
	}
	if style.Attrs&AttrDim != 0 {
		out.WriteString(";2")
	}
	if style.Italic {
		out.WriteString(";3")
	}
	if style.Attrs&AttrUnderline != 0 {
		switch style.UnderlineStyle {
		case UnderlineDouble:
			out.WriteString(";21")
		case UnderlineCurly:
			out.WriteString(";4:3")
		case UnderlineDotted:
			out.WriteString(";4:4")
		case UnderlineDashed:
			out.WriteString(";4:5")
		default:
			out.WriteString(";4")
		}
	}
	if style.Attrs&AttrBlink != 0 {
		out.WriteString(";5")
	}
	if style.Inverse {
		out.WriteString(";7")
	}
	if style.Attrs&AttrStrikethrough != 0 {
		out.WriteString(";9")
	}
	var b [16]byte
	if style.HasForegroundRGB {
		out.WriteString(";38;2;")
		writeRGB(out, &b, style.ForegroundRGB)
	} else if style.Foreground >= 0 {
		out.WriteString(";38;5;")
		n := strconv.AppendInt(b[:0], int64(style.Foreground), 10)
		out.Write(n)
	}
	if style.HasBackgroundRGB {
		out.WriteString(";48;2;")
		writeRGB(out, &b, style.BackgroundRGB)
	} else if style.Background >= 0 {
		out.WriteString(";48;5;")
		n := strconv.AppendInt(b[:0], int64(style.Background), 10)
		out.Write(n)
	}
	if style.HasUnderlineColorRGB {
		out.WriteString(";58;2;")
		writeRGB(out, &b, style.UnderlineColorRGB)
	} else if style.HasUnderlineColor {
		out.WriteString(";58;5;")
		n := strconv.AppendInt(b[:0], int64(style.UnderlineColor), 10)
		out.Write(n)
	}
	out.WriteByte('m')
}

func writeRGB(out *bytes.Buffer, b *[16]byte, rgb RGB) {
	n := strconv.AppendInt(b[:0], int64(rgb.R), 10)
	out.Write(n)
	out.WriteByte(';')
	n = strconv.AppendInt(b[:0], int64(rgb.G), 10)
	out.Write(n)
	out.WriteByte(';')
	n = strconv.AppendInt(b[:0], int64(rgb.B), 10)
	out.Write(n)
}

func sameDamage(a, b Damage) bool {
	return a.Kind == b.Kind && a.X == b.X && a.Y == b.Y && a.Width == b.Width && a.Height == b.Height && a.Count == b.Count
}
