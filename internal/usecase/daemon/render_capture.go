package daemon

import (
	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

// damageCaptureMode makes damage ownership explicit. Primary active-session
// capture is destructive; picker preview observes the same VT state without
// acknowledging it.
type damageCaptureMode uint8

const (
	damageCapturePreview damageCaptureMode = iota
	damageCaptureConsume
)

// capturedRenderState is the immutable hand-off from authoritative daemon
// state to composition. Every field is a value or an owned bounded copy; the
// composer must not retain or consult session, tab, pane, or overlay owners.
type capturedRenderState struct {
	attachment         *attachedClient // identity only; never dereferenced by composition
	attachmentEpoch    uint64
	reset              bool
	layout             capturedTabLayout
	panes              []capturedPaneRenderState
	floating           capturedFloatingRenderState
	bars               barState
	theme              themeui.Theme
	overlays           capturedOverlayRenderState
	preview            picker.Preview
	cursor             capturedCursorInputs
	tabGeneration      uint64
	floatingGeneration uint64
}

type capturedTabLayout struct {
	root        *layout.Node
	area        domain.Rect
	focus       layout.PaneID
	placements  []layout.Placement
	fingerprint string
	valid       bool
}

type capturedPaneRenderState struct {
	id               layout.PaneID
	frame            renderer.Frame
	rawDamage        []renderer.Damage
	damage           []renderer.Damage
	damageGeneration uint64
	title            string
	titleGeneration  uint64
	placement        layout.Placement
	focused          bool
}

type capturedFloatingRenderState struct {
	visible         bool
	pane            capturedPaneRenderState
	geometry        floatingGeometry
	title           string
	generation      uint64
	titleGeneration uint64
}

type capturedOverlayRenderState struct {
	copyActive, copySearchActive, pickerActive, paletteActive, promptActive bool
	copyMode                                                                *scopy.Mode
	copySnapshot                                                            *scopy.Snapshot
	copyPaneID                                                              layout.PaneID
	copyFeedback                                                            string
	paletteGuidance                                                         string
	copySearch, picker, palette, prompt                                     capturedModal
}

type capturedModal struct {
	modal ui.Modal
	inner renderer.Frame
}

func (o capturedOverlayRenderState) active() bool {
	return o.copyActive || o.copySearchActive || o.pickerActive || o.paletteActive || o.promptActive
}

type capturedCursorInputs struct {
	row, col, style                                int
	hasStyle, visible, renderable, hiddenByOverlay bool
	content                                        domain.Rect
}

// capturePaneRenderStateLocked copies only the visible rectangle required by
// composition. It never copies scrollback/history and never lets the mutable
// VT frame's Cells or row-offset slices escape pane.mu.
func capturePaneRenderStateLocked(p *pane, visible domain.Rect, mode damageCaptureMode) capturedPaneRenderState {
	return capturePaneRenderStateLockedInto(p, visible, mode, capturedPaneRenderState{})
}

func capturePaneRenderStateLockedInto(p *pane, visible domain.Rect, mode damageCaptureMode, out capturedPaneRenderState) capturedPaneRenderState {
	out.id, out.title, out.titleGeneration = p.id, p.displayTitleLocked(), p.title.generation
	damage := p.screen.CaptureDamage()
	out.damageGeneration = damage.Generation
	out.rawDamage = append(out.rawDamage[:0], damage.Damage...)

	width := min(max(visible.Width, 0), p.screen.Frame.Width)
	height := min(max(visible.Height, 0), p.screen.Frame.Height)
	needsFrame := len(damage.Damage) > 0
	if out.frame.Width != width || out.frame.Height != height {
		out.frame = renderer.NewFrame(width, height)
		needsFrame = true
	}
	// A retained attachment snapshot is already an immutable copy of this
	// pane. With no VT damage, retain it rather than copying inactive panes.
	if needsFrame {
		for y := range height {
			copy(out.frame.Row(y), p.screen.Frame.Row(y)[:width])
		}
	}

	if uncertainDamage(damage.Damage, p.screen.Frame.Width, p.screen.Frame.Height) {
		out.damage = []renderer.Damage{renderer.FullRedraw()}
	} else {
		out.damage = append(out.damage[:0], damage.Damage...)
	}
	if mode == damageCaptureConsume {
		if !p.screen.AcknowledgeDamage(damage.Generation) {
			out.damage = []renderer.Damage{renderer.FullRedraw()}
		}
	}
	return out
}

func uncertainDamage(damage []renderer.Damage, width, height int) bool {
	for _, d := range damage {
		if d.Kind == renderer.DamageFullRedraw {
			if d != renderer.FullRedraw() {
				return true
			}
			continue
		}
		if d.Kind < renderer.DamageText || d.Kind > renderer.DamageScrollUp || d.X < 0 || d.Y < 0 || d.Width <= 0 || d.Height <= 0 || d.X+d.Width > width || d.Y+d.Height > height {
			return true
		}
		if d.Kind == renderer.DamageScrollUp && (d.Count <= 0 || d.Count > d.Height) {
			return true
		}
	}
	return false
}

// captureRenderState is the ownership boundary for a render transaction.
// Callers hold attachment sendMu; this function then follows session -> tab ->
// pane lock order. ACK-blocked primary capture returns before touching VT
// damage. Preview mode is always non-destructive.
func captureRenderState(sess *session, ac *attachedClient, bars barState, overlays capturedOverlayRenderState, preview picker.Preview, floatingCfg domain.FloatingConfig, reset bool, mode damageCaptureMode) (*capturedRenderState, bool) {
	if sess == nil || ac == nil || (mode == damageCaptureConsume && ac.output != nil && ac.output.atCapacity()) {
		return nil, false
	}
	sess.mu.Lock()
	if sess.client != ac || sess.active < 0 || sess.active >= len(sess.tabs) {
		sess.mu.Unlock()
		return nil, false
	}
	tb := sess.tabs[sess.active]
	epoch := ac.coordinatorEpoch.Load()
	sess.mu.Unlock()

	bars.status.tabs = append([]statusTab(nil), bars.status.tabs...)
	bars.mru = append([]recentSession(nil), bars.mru...)
	bars.rankedRecent = append([]rankedRecent(nil), bars.rankedRecent...)
	preview = clonePickerPreview(preview)

	tb.mu.Lock()
	defer tb.mu.Unlock()
	layoutSnap := solveTabLayoutLocked(tb)
	state := &capturedRenderState{
		attachment: ac, attachmentEpoch: epoch, reset: reset, bars: bars, theme: bars.theme,
		overlays: overlays, preview: preview,
		layout: capturedTabLayout{root: func() *layout.Node {
			if tb.tree == nil {
				return nil
			}
			return tb.tree.Clone().Root
		}(), area: layoutSnap.area, focus: layoutSnap.focus, placements: append([]layout.Placement(nil), layoutSnap.placements...), fingerprint: layoutSnap.fingerprint, valid: layoutSnap.ok},
		floatingGeneration: tb.floating.generation,
	}
	state.tabGeneration = uint64(len(layoutSnap.fingerprint))
	for _, placement := range layoutSnap.placements {
		p := tb.panes[placement.ID]
		if p == nil {
			continue
		}
		visible := placement.Content
		if placement.Collapsed {
			visible = domain.Rect{}
		}
		p.mu.Lock()
		if ac.captureFrames == nil {
			ac.captureFrames = make(map[layout.PaneID]capturedPaneRenderState)
		}
		captured := capturePaneRenderStateLockedInto(p, visible, mode, ac.captureFrames[p.id])
		captured.placement = placement
		captured.focused = placement.ID == layoutSnap.focus
		if captured.focused {
			state.cursor = captureCursorInputsLocked(p, placement.Content, overlays)
		}
		p.mu.Unlock()
		translated := make([]renderer.Damage, 0, len(captured.damage))
		for _, damage := range captured.damage {
			translated = append(translated, translatePaneDamage(damage, placement.Content, layoutSnap.area)...)
		}
		captured.damage = translated
		ac.captureFrames[captured.id] = captured
		state.panes = append(state.panes, captured)
	}
	if tb.floating.state == floatingVisible && tb.floating.pane != nil {
		p := tb.floating.pane
		p.mu.Lock()
		geometry := p.committedFloatingGeometryLocked(calculateContentFloatingGeometry(domain.Size{Cols: layoutSnap.area.Width, Rows: layoutSnap.area.Height}, floatingCfg))
		if ac.captureFrames == nil {
			ac.captureFrames = make(map[layout.PaneID]capturedPaneRenderState)
		}
		captured := capturePaneRenderStateLockedInto(p, geometry.Inner, mode, ac.captureFrames[p.id])
		ac.captureFrames[captured.id] = captured
		state.floating = capturedFloatingRenderState{visible: true, pane: captured, geometry: geometry, title: captured.title, generation: tb.floating.generation, titleGeneration: captured.titleGeneration}
		state.cursor = captureCursorInputsLocked(p, geometry.Inner, overlays)
		p.mu.Unlock()
	}
	return state, true
}

func captureCursorInputsLocked(p *pane, content domain.Rect, overlays capturedOverlayRenderState) capturedCursorInputs {
	style, hasStyle := p.screen.CursorStyle()
	hidden := overlays.copyActive || overlays.copySearchActive || overlays.pickerActive || overlays.paletteActive || overlays.promptActive
	return capturedCursorInputs{row: p.screen.CursorRow(), col: p.screen.CursorCol(), style: style, hasStyle: hasStyle, visible: p.screen.CursorVisible(), renderable: content.Width > 0 && content.Height > 0, hiddenByOverlay: hidden, content: content}
}

func clonePickerPreview(in picker.Preview) picker.Preview {
	out := picker.Preview{Width: in.Width, Height: in.Height, Rows: make([][]renderer.Cell, len(in.Rows))}
	for i := range in.Rows {
		out.Rows[i] = append([]renderer.Cell(nil), in.Rows[i]...)
	}
	return out
}
