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
	caps   Capabilities
	width  int
	height int
	shadow []Cell
}

func New(caps Capabilities) *Renderer { return &Renderer{caps: caps} }

func (r *Renderer) Reset() {
	r.width = 0
	r.height = 0
	r.shadow = nil
}

func (r *Renderer) Draw(frame Frame, damage []Damage) ([]byte, error) {
	if err := frame.Validate(); err != nil {
		return nil, err
	}

	knownSameDimensions := r.width == frame.Width && r.height == frame.Height && len(r.shadow) == len(frame.Cells)

	// Unchanged frame no-op: same dimensions, shadow populated, and either no
	// damage or a structural redraw request that can be satisfied from the
	// known terminal shadow.
	if knownSameDimensions {
		if len(damage) == 0 && !r.hasDirtyLines(frame) {
			return nil, nil
		}
		if needsFull(damage) && !r.hasDirtyLines(frame) {
			return nil, nil
		}
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer putBuffer(buf)

	if r.caps.SynchronizedOutput {
		buf.WriteString(SyncStartCSI)
	}

	// Per-Draw terminal state model shared by every emission path so cursor
	// tracking and the SGR pen persist across all rects within one Draw.
	st := newDrawState()

	if !knownSameDimensions {
		r.writeFull(buf, frame, &st)
		r.advanceShadow(frame)
		if r.caps.SynchronizedOutput {
			buf.WriteString(SyncEndCSI)
		}
		return copyBytes(buf), nil
	}

	if needsFull(damage) {
		r.writeDamage(buf, frame, nil, nil, &st)
		r.advanceShadow(frame)
		if r.caps.SynchronizedOutput {
			buf.WriteString(SyncEndCSI)
		}
		return copyBytes(buf), nil
	}

	if scroll, ok := findSafeScroll(frame, damage); ok && r.canApplyScroll(frame, scroll, damage) {
		emitScrollUp(buf, scroll)
		// emitScrollUp resets the SGR pen to default (matching st's initial
		// pen) but leaves the cursor wherever the DECSTBM restore put it —
		// terminal-dependent, so cursor tracking stays invalidated.
		r.applyScroll(scroll)
		r.writeDamage(buf, frame, damage, &scroll, &st)
		r.advanceDamage(frame, damage, &scroll)
		if r.caps.SynchronizedOutput {
			buf.WriteString(SyncEndCSI)
		}
		return copyBytes(buf), nil
	}

	// Scroll damage changes rows outside the explicit text/clear rectangles.
	// If the fast path is not safe, redraw the frame instead of applying
	// partial damage and leaving the terminal/shadow stale.
	if hasScrollDamage(damage) {
		r.writeFull(buf, frame, &st)
		r.advanceShadow(frame)
		if r.caps.SynchronizedOutput {
			buf.WriteString(SyncEndCSI)
		}
		return copyBytes(buf), nil
	}

	r.writeDamage(buf, frame, damage, nil, &st)
	r.advanceDamage(frame, damage, nil)
	if r.caps.SynchronizedOutput {
		buf.WriteString(SyncEndCSI)
	}
	return copyBytes(buf), nil
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

func (r *Renderer) hasDirtyLines(frame Frame) bool {
	for y := range frame.Height {
		if r.lineDirty(frame, y) {
			return true
		}
	}
	return false
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

func (r *Renderer) writeDamage(out *bytes.Buffer, frame Frame, damage []Damage, skip *Damage, st *drawState) {
	if len(damage) == 0 {
		for y := range frame.Height {
			if r.lineDirty(frame, y) {
				r.emitSpan(out, frame, y, 0, frame.Width, st)
			}
		}
		out.WriteString("\x1b[0m")
		return
	}
	for _, d := range damage {
		if skip != nil && sameDamage(d, *skip) {
			continue
		}
		switch d.Kind {
		case DamageText, DamageClear:
			x, y, w, h, ok := clampRect(frame, d.X, d.Y, d.Width, d.Height)
			if !ok {
				continue
			}
			for row := y; row < y+h; row++ {
				r.emitSpan(out, frame, row, x, w, st)
			}
		}
	}
	out.WriteString("\x1b[0m")
}

func (r *Renderer) lineDirty(frame Frame, y int) bool {
	// The shadow is stored in canonical (logical) row order; the frame is read
	// through its logical accessor so the frame's physical layout is invisible.
	start := y * frame.Width
	row := frame.Row(y)
	for i := range frame.Width {
		if !r.shadow[start+i].Equal(row[i]) {
			return true
		}
	}
	return false
}

func (r *Renderer) advanceShadow(frame Frame) {
	r.replaceShadow(frame)
}

func (r *Renderer) advanceDamage(frame Frame, damage []Damage, scroll *Damage) {
	r.syncDamage(frame, damage, scroll)
}

func (r *Renderer) replaceShadow(frame Frame) {
	r.width = frame.Width
	r.height = frame.Height
	n := len(frame.Cells)
	if cap(r.shadow) >= n {
		r.shadow = r.shadow[:n]
	} else {
		r.shadow = make([]Cell, n)
	}
	// Copy logical rows into canonical shadow order so the frame's line-offset
	// rotation never leaks into the renderer's own buffer.
	for y := range frame.Height {
		copy(r.shadow[y*frame.Width:], frame.Row(y))
	}
}

func (r *Renderer) syncDamage(frame Frame, damage []Damage, scroll *Damage) {
	if len(damage) == 0 {
		r.replaceShadow(frame)
		return
	}
	if scroll != nil {
		r.syncRect(frame, scroll.X, scroll.Y+scroll.Height-scroll.Count, scroll.Width, scroll.Count)
	}
	for _, d := range damage {
		switch d.Kind {
		case DamageText, DamageClear:
			r.syncRect(frame, d.X, d.Y, d.Width, d.Height)
		}
	}
}

func (r *Renderer) syncRect(frame Frame, x, y, width, height int) {
	x, y, width, height, ok := clampRect(frame, x, y, width, height)
	if !ok {
		return
	}
	for row := y; row < y+height; row++ {
		start := row*frame.Width + x
		frameRow := frame.Row(row)
		copy(r.shadow[start:start+width], frameRow[x:x+width])
	}
}

func clampRect(frame Frame, x, y, width, height int) (int, int, int, int, bool) {
	if width <= 0 || height <= 0 {
		return 0, 0, 0, 0, false
	}
	x0 := max(0, x)
	y0 := max(0, y)
	x1 := min(frame.Width, x+width)
	y1 := min(frame.Height, y+height)
	if x1 <= x0 || y1 <= y0 {
		return 0, 0, 0, 0, false
	}
	return x0, y0, x1 - x0, y1 - y0, true
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
