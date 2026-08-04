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

// capturedRenderState is the immutable hand-off from authoritative daemon
// state to composition. Every field is a value or an owned bounded copy; the
// composer must not retain or consult session, tab, pane, or overlay owners.
// renderCaptureScratch owns bounded snapshots for one attachment. The complete
// capture/compose/emit transaction holds that attachment's sendMu, so these
// buffers cannot be observed or reused before emission completes.
type renderCaptureScratch struct {
	state             capturedRenderState
	panes             []capturedPaneRenderState
	placements        []layout.Placement
	dividers          []layout.Divider
	layoutTab         *tab
	layoutGeneration  uint64
	layoutArea        domain.Rect
	layoutFocus       layout.PaneID
	layoutFingerprint string
	layoutValid       bool
	statusTabs        []statusTab
	mru               []recentSession
	ranked            []rankedRecent
	titleIDs          []layout.PaneID
	receipts          []damageReceipt
}

type damageReceipt struct {
	pane       *pane
	generation uint64
}

type capturedRenderState struct {
	attachment         *attachedClient // identity only; never dereferenced by composition
	lease              *attachmentLease
	view               attachmentView
	window             domain.Size
	reset              bool
	layout             capturedTabLayout
	panes              []capturedPaneRenderState
	floating           capturedFloatingRenderState
	bars               barState
	theme              themeui.Theme
	styles             themeui.Styles
	styleGeneration    uint64
	overlays           capturedOverlayRenderState
	preview            picker.Preview
	cursor             capturedCursorInputs
	tabGeneration      uint64
	floatingGeneration uint64
	receipts           []damageReceipt // private capture receipts; compose must not inspect these
}

type capturedTabLayout struct {
	area        domain.Rect
	focus       layout.PaneID
	placements  []layout.Placement
	dividers    []layout.Divider
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
	focused         bool
	pane            capturedPaneRenderState
	geometry        floatingGeometry
	title           string
	generation      uint64
	titleGeneration uint64
}

type capturedOverlayRenderState struct {
	copyActive, copySearchActive, pickerActive, paletteActive, promptActive bool
	noticesOverlayActive, resizeActive                                      bool
	copyMode                                                                *scopy.Mode
	copyPaneID                                                              layout.PaneID
	statusFeedback                                                          string
	paletteGuidance                                                         string
	copySearch, picker, palette, prompt, noticesOverlay                     capturedModal
	notices                                                                 []domain.Notification
	noticeOverflow                                                          int
}

type capturedModal struct {
	active       bool
	title        string
	presentation ui.Presentation
	inner        renderer.Frame
	focused      bool
}

func (o capturedOverlayRenderState) active() bool {
	return o.copyActive || o.copySearchActive || o.pickerActive || o.paletteActive || o.promptActive || o.noticesOverlayActive
}

type capturedCursorInputs struct {
	row, col, style                                int
	hasStyle, visible, renderable, hiddenByOverlay bool
	content                                        domain.Rect
}

// capturePaneRenderStateLocked copies only the visible rectangle required by
// composition. It never copies scrollback/history and never lets the mutable
// VT frame's Cells or row-offset slices escape pane.mu.
func capturePaneRenderStateLocked(p *pane, visible domain.Rect) capturedPaneRenderState {
	return capturePaneRenderStateLockedInto(p, visible, capturedPaneRenderState{})
}

func capturePaneRenderStateLockedInto(p *pane, visible domain.Rect, out capturedPaneRenderState) capturedPaneRenderState {
	out.id, out.title, out.titleGeneration = p.id, p.displayTitleLocked(), p.title.generation
	damage := p.screen.CaptureDamage()
	out.damageGeneration = damage.Generation
	out.rawDamage = append(out.rawDamage[:0], damage.Damage...)

	width := min(max(visible.Width, 0), p.screen.Frame.Width)
	height := min(max(visible.Height, 0), p.screen.Frame.Height)
	needsFrame := len(damage.Damage) > 0
	frameChanged := out.frame.Width != width || out.frame.Height != height
	if frameChanged {
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

	if frameChanged || uncertainDamage(damage.Damage, p.screen.Frame.Width, p.screen.Frame.Height) {
		// A cache entry with a new frame has no terminal-shadow equivalent.
		// Redraw it even when the pane has no VT damage, such as an untouched
		// pane first made visible after a tab or session switch.
		out.damage = []renderer.Damage{renderer.FullRedraw()}
	} else {
		out.damage = append(out.damage[:0], damage.Damage...)
	}
	// Consumption is transactional: capture only snapshots damage. emitFrame
	// acknowledges this generation after preparation and transport success.
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

type renderCaptureRequest struct {
	bars            barState
	overlays        capturedOverlayRenderState
	preview         picker.Preview
	floatingCfg     domain.FloatingConfig
	styles          themeui.Styles
	styleGeneration uint64
	reset           bool
	lease           *attachmentLease
}

// captureLocalRenderState is the ownership boundary for a local attachment
// render transaction. Callers hold attachment sendMu; this function then follows
// session -> tab -> pane lock order. ACK-blocked capture returns before touching
// VT damage, and every captured pane records a receipt for successful emission.
func captureLocalRenderState(
	sess *session,
	ac *attachedClient,
	request renderCaptureRequest,
) (*capturedRenderState, bool) {
	bars := request.bars
	overlays := request.overlays
	preview := request.preview
	floatingCfg := request.floatingCfg
	reset := request.reset
	lease := request.lease
	if sess == nil || ac == nil || (ac.output != nil && ac.output.atCapacity()) {
		return nil, false
	}
	sess.mu.Lock()
	_, owned := sess.attachments[ac]
	sess.mu.Unlock()
	if !owned {
		return nil, false
	}
	tb, focusedPane := sess.paneForAttachment(ac)
	if tb == nil {
		tb = sess.tabForAttachment(ac)
	}
	if tb == nil {
		return nil, false
	}

	// A reset is an attachment-local capture boundary. The shared pane damage
	// receipt may have been acknowledged by another fan-out attachment before
	// this capture runs, so retaining this attachment's prior pane snapshots
	// would compose an old frame with a fresh reset epoch.
	if reset {
		ac.captureFrames = nil
	}
	scratch := &ac.renderScratch
	scratch.statusTabs = append(scratch.statusTabs[:0], bars.status.tabs...)
	bars.status.tabs = scratch.statusTabs
	scratch.mru = append(scratch.mru[:0], bars.mru...)
	bars.mru = scratch.mru
	if bars.rankedRecent != nil {
		scratch.ranked = copyRankedRecentInto(scratch.ranked, bars.rankedRecent)
		bars.rankedRecent = scratch.ranked
	}
	if len(preview.Rows) > 0 {
		preview = clonePickerPreview(preview)
	} else {
		preview = picker.Preview{Width: preview.Width, Height: preview.Height}
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()
	area := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	var focus layout.PaneID
	if focusedPane != nil && tb.panes[focusedPane.id] == focusedPane {
		focus = focusedPane.id
	}
	layoutSnap := tabLayoutSnapshot{
		area: area, focus: focus, placements: scratch.placements,
		dividers: scratch.dividers, fingerprint: scratch.layoutFingerprint, ok: scratch.layoutValid,
	}
	if scratch.layoutTab != tb || scratch.layoutGeneration != tb.layoutGeneration || scratch.layoutArea != area || scratch.layoutFocus != focus {
		layoutSnap = solveTabLayoutLocked(tb)
		layoutSnap.focus = focus
		scratch.placements = append(scratch.placements[:0], layoutSnap.placements...)
		scratch.dividers = append(scratch.dividers[:0], layoutSnap.dividers...)
		scratch.layoutTab = tb
		scratch.layoutGeneration = tb.layoutGeneration
		scratch.layoutArea = area
		scratch.layoutFocus = focus
		scratch.layoutFingerprint = layoutSnap.fingerprint
		scratch.layoutValid = layoutSnap.ok
		layoutSnap.placements = scratch.placements
		layoutSnap.dividers = scratch.dividers
	}
	state := &scratch.state
	view := ac.viewSnapshot()
	window := domain.Size{}
	if view.windowSet {
		window = ac.sizeSnapshot()
	}
	*state = capturedRenderState{
		attachment: ac, lease: lease, view: view, window: window,
		reset: reset, bars: bars, theme: bars.theme,
		styles: request.styles, styleGeneration: request.styleGeneration,
		overlays: overlays, preview: preview,
		layout:             capturedTabLayout{area: layoutSnap.area, focus: layoutSnap.focus, placements: scratch.placements, dividers: scratch.dividers, fingerprint: layoutSnap.fingerprint, valid: layoutSnap.ok},
		floatingGeneration: tb.floating.generation,
		receipts:           scratch.receipts[:0],
	}
	state.panes = scratch.panes[:0]
	state.tabGeneration = uint64(len(layoutSnap.fingerprint))
	if ac.captureFrames == nil {
		ac.captureFrames = make(map[*pane]capturedPaneRenderState)
	}
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
		captured := capturePaneRenderStateLockedInto(p, visible, ac.captureFrames[p])
		state.receipts = append(state.receipts, damageReceipt{pane: p, generation: captured.damageGeneration})
		captured.placement, captured.focused = placement, placement.ID == layoutSnap.focus
		if captured.focused {
			state.cursor = captureCursorInputsLocked(p, placement.Content, overlays)
		}
		p.mu.Unlock()
		translated := captured.rawDamage[:0]
		for _, damage := range captured.damage {
			translated = append(translated, translatePaneDamage(damage, placement.Content, layoutSnap.area)...)
		}
		captured.rawDamage, captured.damage = captured.damage, translated
		ac.captureFrames[p] = captured
		state.panes = append(state.panes, captured)
	}
	scratch.panes = state.panes
	if tb.floating.state == floatingVisible && tb.floating.pane != nil {
		p := tb.floating.pane
		p.mu.Lock()
		geometry := p.committedFloatingGeometryLocked(calculateContentFloatingGeometry(domain.Size{Cols: layoutSnap.area.Width, Rows: layoutSnap.area.Height}, floatingCfg))
		captured := capturePaneRenderStateLockedInto(p, geometry.Inner, ac.captureFrames[p])
		seen := false
		for _, receipt := range state.receipts {
			if receipt.pane == p {
				seen = true
				break
			}
		}
		if !seen {
			state.receipts = append(state.receipts, damageReceipt{pane: p, generation: captured.damageGeneration})
		}
		ac.captureFrames[p] = captured
		// A visible floating pane is the terminal input target, so its structural
		// border carries the focused semantic role independently of its content.
		state.floating = capturedFloatingRenderState{visible: true, focused: true, pane: captured, geometry: geometry, title: captured.title, generation: tb.floating.generation, titleGeneration: captured.titleGeneration}
		state.cursor = captureCursorInputsLocked(p, geometry.Inner, overlays)
		p.mu.Unlock()
	}
	scratch.receipts = state.receipts
	return state, true
}

// captureRenderState preserves the focused local test seam. Production
// rendering delegates through *session.captureRenderState.
func captureRenderState(sess *session, ac *attachedClient, request renderCaptureRequest) (*capturedRenderState, bool) {
	return captureLocalRenderState(sess, ac, request)
}

// copyRankedRecentInto preserves the non-nil empty slice that selects
// contextual recent-session mode while retaining scratch capacity for reuse.
func copyRankedRecentInto(dst, src []rankedRecent) []rankedRecent {
	if len(src) == 0 {
		if dst != nil {
			return dst[:0]
		}
		return []rankedRecent{}
	}
	return append(dst[:0], src...)
}

func captureCursorInputsLocked(p *pane, content domain.Rect, overlays capturedOverlayRenderState) capturedCursorInputs {
	style, hasStyle := p.screen.CursorStyle()
	hidden := overlays.copyActive || overlays.copySearchActive || overlays.pickerActive || overlays.paletteActive || overlays.promptActive || overlays.noticesOverlayActive
	return capturedCursorInputs{row: p.screen.CursorRow(), col: p.screen.CursorCol(), style: style, hasStyle: hasStyle, visible: p.screen.CursorVisible(), renderable: content.Width > 0 && content.Height > 0, hiddenByOverlay: hidden, content: content}
}

func clonePickerPreview(in picker.Preview) picker.Preview {
	out := picker.Preview{Width: in.Width, Height: in.Height, Rows: make([][]renderer.Cell, len(in.Rows))}
	for i := range in.Rows {
		out.Rows[i] = append([]renderer.Cell(nil), in.Rows[i]...)
	}
	return out
}

// appendStackPaneIDs returns exactly the panes whose solved placement can own a
// stack title bar. The caller holds tab.mu, and dst is attachment scratch.
func appendStackPaneIDs(dst []layout.PaneID, node *layout.Node) []layout.PaneID {
	if node == nil {
		return dst
	}
	if node.Kind == layout.Stack {
		for _, child := range node.Children {
			if child != nil && child.Kind == layout.Leaf {
				dst = append(dst, child.Leaf)
			}
		}
		return dst
	}
	for _, child := range node.Children {
		dst = appendStackPaneIDs(dst, child)
	}
	return dst
}
