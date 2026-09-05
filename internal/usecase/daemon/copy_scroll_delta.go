package daemon

import (
	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
)

// Only successfully emitted composition caches carry this metadata forward.
// It describes the terminal shadow, not the unadorned live-pane cache.
type copyViewportState struct {
	document *scopy.Document
	target   domain.Rect
	top      int
	cursor   scopy.Pos
	frame    renderer.Frame
}

func captureCopyViewport(state capturedRenderState, content domain.Rect) copyViewportState {
	mode := state.overlays.copyMode
	if mode == nil || mode.Document() == nil || mode.Selection().Enabled || mode.SearchQuery != "" {
		return copyViewportState{}
	}
	return copyViewportState{document: mode.Document(), target: capturedCopyTarget(state, content), top: mode.ViewportTop, cursor: mode.Cursor()}
}

// A scroll hint is safe only for a whole-width, unchanged document viewport.
// The ANSI renderer independently verifies retained cells against its committed
// shadow and falls back to a snapshot if search or other adornments changed.
func copyViewportDamage(previous, next copyViewportState, frame renderer.Frame) []renderer.Damage {
	target := next.target
	if next.document == nil || previous.document != next.document || previous.target != target ||
		target.X != 0 || target.Width != frame.Width || target.Y < 0 || target.Height <= 0 ||
		target.Y >= frame.Height || target.Height >= frame.Height-target.Y ||
		target.Height != next.document.Height() || target.Width != next.document.Width() {
		return nil
	}
	delta := next.top - previous.top
	if delta == 0 || delta <= -target.Height || delta >= target.Height {
		return nil
	}
	kind, count, exposedY := renderer.DamageScrollUp, delta, target.Y+target.Height-delta
	if delta < 0 {
		kind, count, exposedY = renderer.DamageScrollDown, -delta, target.Y
	}
	damage := []renderer.Damage{
		{Kind: kind, Y: target.Y, Width: target.Width, Height: target.Height, Count: count},
		{Kind: renderer.DamageText, Y: exposedY, Width: frame.Width, Height: count},
	}
	// Refresh the old cursor after shifting it, and paint the new cursor. The
	// highlighted cursor need not occupy the same screen row after navigation.
	for _, cursor := range []scopy.Pos{previous.cursor, next.cursor} {
		y := cursor.Row - next.top
		if y >= 0 && y < target.Height {
			damage = append(damage, renderer.Damage{Kind: renderer.DamageText, Y: target.Y + y, Width: frame.Width, Height: 1})
		}
	}
	// Preserve all chrome and other panes above/below the scroll region, even
	// when live PTY output or status updates arrive together with the wheel.
	if target.Y > 0 {
		damage = append(damage, renderer.Damage{Kind: renderer.DamageText, Width: frame.Width, Height: target.Y})
	}
	bottom := target.Y + target.Height
	damage = append(damage, renderer.Damage{Kind: renderer.DamageText, Y: bottom, Width: frame.Width, Height: frame.Height - bottom})
	return damage
}

// Reuse retained compact rows instead of decoding and interning the whole
// immutable viewport. Both committed frames remain untouched until send succeeds.
func composeScrolledCopyViewport(state capturedRenderState, base renderer.Frame, previous, next copyViewportState, damage []renderer.Damage) renderer.Frame {
	frame := previous.frame.Clone()
	target := next.target
	scroll := damage[0]
	if scroll.Kind == renderer.DamageScrollDown {
		frame.ScrollDown(target.Y, target.Y+target.Height-1, scroll.Count)
	} else {
		frame.ScrollUp(target.Y, target.Y+target.Height-1, scroll.Count)
	}
	for y := range frame.Height {
		if y < target.Y || y >= target.Y+target.Height {
			frame.WriteRow(y, 0, base.Row(y))
		}
	}
	paint := func(y int, row []renderer.Cell) {
		if y == next.document.Height() {
			frame.WriteRow(frame.Height-1, 0, row)
		} else {
			frame.WriteRow(target.Y+y, 0, row)
		}
	}
	for _, d := range damage[1:] {
		start, end := max(d.Y-target.Y, 0), min(d.Y+d.Height-target.Y, target.Height)
		if start < end {
			state.overlays.copyMode.RenderRowsRange(start, end, paint, state.styles.CopyStatus, state.styles.Selection)
		}
	}
	return frame
}
