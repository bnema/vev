package renderer

import (
	"bytes"
	"strconv"
)

func findSafeScroll(frame Frame, damage []Damage) (Damage, bool) {
	for _, d := range damage {
		if d.Kind == DamageScrollUp && isSafeScroll(frame, d) {
			return d, true
		}
	}
	return Damage{}, false
}

func isSafeScroll(frame Frame, d Damage) bool {
	return d.X == 0 && d.Width == frame.Width && d.Y >= 0 && d.Height > 0 && d.Count > 0 && d.Count < d.Height && d.Y+d.Height <= frame.Height
}

func emitScrollUp(out *bytes.Buffer, d Damage) {
	out.WriteString("\x1b[0m")
	// \x1b[%d;%dr — set scroll region
	out.WriteString("\x1b[")
	var b [16]byte
	n := strconv.AppendInt(b[:0], int64(d.Y+1), 10)
	out.Write(n)
	out.WriteByte(';')
	n = strconv.AppendInt(b[:0], int64(d.Y+d.Height), 10)
	out.Write(n)
	out.WriteByte('r')
	// Position cursor at bottom of scroll region.
	writeCursor(out, d.Y+d.Height-1, 0)
	if d.Count == 1 {
		out.WriteByte('\n')
	} else {
		out.WriteString("\x1b[")
		n = strconv.AppendInt(b[:0], int64(d.Count), 10)
		out.Write(n)
		out.WriteByte('S')
	}
	out.WriteString("\x1b[r")
}

func (r *Renderer) canApplyScroll(frame Frame, scroll Damage, damage []Damage) bool {
	for y := scroll.Y; y < scroll.Y+scroll.Height-scroll.Count; y++ {
		frameStart := y*frame.Width + scroll.X
		shadowStart := (y+scroll.Count)*r.width + scroll.X
		for x := 0; x < scroll.Width; x++ {
			if r.shadow[shadowStart+x].Equal(frame.Cells[frameStart+x]) {
				continue
			}
			if !damageCoversCell(damage, scroll.X+x, y) {
				return false
			}
		}
	}
	return true
}

func (r *Renderer) applyScroll(scroll Damage) {
	for y := scroll.Y; y < scroll.Y+scroll.Height-scroll.Count; y++ {
		dst := y*r.width + scroll.X
		src := (y+scroll.Count)*r.width + scroll.X
		copy(r.shadow[dst:dst+scroll.Width], r.shadow[src:src+scroll.Width])
	}
}

func damageCoversCell(damage []Damage, x, y int) bool {
	for _, d := range damage {
		if (d.Kind == DamageText || d.Kind == DamageClear) && x >= d.X && x < d.X+d.Width && y >= d.Y && y < d.Y+d.Height {
			return true
		}
	}
	return false
}
