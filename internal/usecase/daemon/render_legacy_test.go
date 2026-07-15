package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

// Legacy composition adapters are test-only. Production rendering is owned by
// capturePrimaryRenderState -> composeFrame -> emitFrame.
// copyTargetRectLocked maps a captured copy source into the already-composed
// client frame. The caller holds tb.mu, preserving the layout/floating
// snapshot while the rectangle is chosen.
func copyTargetRectLocked(layoutSnap tabLayoutSnapshot, contentArea domain.Rect, p, floating *pane, hasFloating bool, floatingFrameGeometry floatingGeometry) domain.Rect {
	if p == nil {
		return domain.Rect{}
	}
	if hasFloating && p == floating {
		return floatingFrameGeometry.Inner
	}
	for _, placement := range layoutSnap.placements {
		if placement.ID == p.id && !placement.Collapsed {
			r := placement.Content
			r.X += contentArea.X
			r.Y += contentArea.Y
			return r
		}
	}
	p.mu.Lock()
	width, height := p.screen.Frame.Width, p.screen.Frame.Height
	p.mu.Unlock()
	return domain.Rect{X: contentArea.X, Y: contentArea.Y, Width: min(width, contentArea.Width), Height: min(height, contentArea.Height)}
}

type composedFrameCache struct {
	frame                   renderer.Frame
	valid                   bool
	layoutSnap              tabLayoutSnapshot
	titleGenerations        map[layout.PaneID]uint64
	floating                *pane
	floatingFrame           renderer.Frame
	floatingGeneration      uint64
	floatingGeometry        floatingGeometry
	floatingTitleGeneration uint64
	floatingCaptured        capturedPaneRenderState
}

func composeClientFrame(sess *session, tb *tab, full bool, rightStatus string, caches ...*barCache) (renderer.Frame, []renderer.Damage) {
	return composeClientFrameWithState(barState{status: sess.statusSegments(true), copyFeedback: rightStatus}, tb, full, caches...)
}

func composeClientFrameWithState(bars barState, tb *tab, full bool, caches ...*barCache) (renderer.Frame, []renderer.Damage) {
	return composeClientFrameWithLayout(bars, tb, full, solveTabLayoutLocked(tb), caches...)
}

func composeClientFrameWithLayout(bars barState, tb *tab, full bool, layoutSnap tabLayoutSnapshot, caches ...*barCache) (renderer.Frame, []renderer.Damage) {
	var cache *barCache
	if len(caches) > 0 {
		cache = caches[0]
	}
	return composeClientFrameWithLayoutCached(bars, tb, full, layoutSnap, cache, nil)
}

func composeClientFrameWithLayoutCached(bars barState, tb *tab, full bool, layoutSnap tabLayoutSnapshot, cache *barCache, composed *composedFrameCache) (renderer.Frame, []renderer.Damage) {
	return composeClientFrameWithLayoutCachedOptions(bars, tb, full, layoutSnap, cache, composed, false)
}

func composeClientFrameWithLayoutCachedConsumeDamage(bars barState, tb *tab, full bool, layoutSnap tabLayoutSnapshot, cache *barCache, composed *composedFrameCache) (renderer.Frame, []renderer.Damage) {
	return composeClientFrameWithLayoutCachedOptions(bars, tb, full, layoutSnap, cache, composed, true)
}

func composeClientFrameWithLayoutCachedOptions(bars barState, tb *tab, full bool, layoutSnap tabLayoutSnapshot, cache *barCache, composed *composedFrameCache, consumeDamage bool) (renderer.Frame, []renderer.Damage) {
	p := tb.focusedPane()
	styles := newThemeStyles(bars.theme)
	if p == nil {
		width, screenRows := tb.size.Cols, tb.size.Rows
		if width <= 0 || screenRows <= 0 {
			return renderer.NewFrame(0, 0), nil
		}
		return renderer.NewFrame(width, screenRows+2), nil
	}
	p.mu.Lock()
	width, screenRows := p.screen.Frame.Width, p.screen.Frame.Height
	p.mu.Unlock()
	if tb.size.Valid() {
		width, screenRows = tb.size.Cols, tb.size.Rows
	}
	cacheValid := composed != nil && composed.valid && composed.frame.Width == width && composed.frame.Height == screenRows+2
	layoutSame := composed == nil || (cacheValid && sameTabLayoutSnapshot(composed.layoutSnap, layoutSnap))
	var frame renderer.Frame
	if cacheValid {
		frame = composed.frame
	} else {
		frame = renderer.NewFrame(width, screenRows+2)
		if composed != nil {
			full = true
		}
	}
	if !layoutSame {
		full = true
	}
	contentArea := domain.Rect{Y: 1, Width: width, Height: screenRows}
	if cacheValid && !layoutSame {
		clearFrameRect(frame, contentArea)
	}
	topBar := frame.Row(0)
	drawTopBarSnapshot(topBar, bars.status, bars.attentionFrame, bars.topRight, styles)
	var titleGenerations map[layout.PaneID]uint64
	if composed != nil {
		if composed.titleGenerations == nil {
			composed.titleGenerations = make(map[layout.PaneID]uint64)
		}
		titleGenerations = composed.titleGenerations
	}
	contentDamage := composeTabFrameIntoWithLayoutOptions(tb, frame, contentArea, bars.theme, layoutSnap, cacheValid && layoutSame, consumeDamage, titleGenerations)
	bottomBar := frame.Row(screenRows + 1)
	drawStatusBarState(bottomBar, bars, styles)
	if composed != nil {
		composed.frame = frame
		composed.valid = true
		composed.layoutSnap = layoutSnap
	}
	if full {
		if cache != nil {
			cache.capture(topBar, bottomBar)
		}
		return frame, []renderer.Damage{renderer.FullRedraw()}
	}
	damage := translateDamage(contentDamage, 0, 1)
	if cache == nil || !sameCells(cache.top, topBar) {
		damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 0, Width: width, Height: 1})
	}
	if cache == nil || !sameCells(cache.bottom, bottomBar) {
		damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: screenRows + 1, Width: width, Height: 1})
	}
	if cache != nil {
		cache.capture(topBar, bottomBar)
	}
	return frame, damage
}

func composeTabFrame(tb *tab, area domain.Rect, theme themeui.Theme) (renderer.Frame, []renderer.Damage) {
	return composeTabFrameWithLayout(tb, area, theme, tabLayoutSnapshot{})
}

func composeTabFrameWithLayout(tb *tab, area domain.Rect, theme themeui.Theme, layoutSnap tabLayoutSnapshot) (renderer.Frame, []renderer.Damage) {
	frame := renderer.NewFrame(area.Width, area.Height)
	damage := composeTabFrameIntoWithLayout(tb, frame, area, theme, layoutSnap, false)
	return frame, damage
}

func composeTabFrameIntoWithLayout(tb *tab, frame renderer.Frame, area domain.Rect, theme themeui.Theme, layoutSnap tabLayoutSnapshot, cacheValid bool) []renderer.Damage {
	return composeTabFrameIntoWithLayoutOptions(tb, frame, area, theme, layoutSnap, cacheValid, false, nil)
}

func composeTabFrameIntoWithLayoutOptions(tb *tab, frame renderer.Frame, area domain.Rect, theme themeui.Theme, layoutSnap tabLayoutSnapshot, cacheValid bool, consumeDamage bool, titleGenerations map[layout.PaneID]uint64) []renderer.Damage {
	contentArea := domain.Rect{Width: area.Width, Height: area.Height}
	root := tb.tree.Root
	placements, ok := layoutSnap.placements, layoutSnap.ok && layoutSnap.root == root && layoutSnap.area == contentArea
	if !ok {
		placements, ok = layout.Solve(root, contentArea)
	}
	var fallback *pane
	if !ok {
		fallback = tb.focusedPane()
		if fallback == nil {
			return nil
		}
		placements = []layout.Placement{{ID: fallback.id, Content: contentArea}}
	}
	// A valid cache has the same layout, so its title IDs remain valid. Reset
	// the existing cache only when rebuilding after layout or frame churn.
	if titleGenerations != nil && !cacheValid {
		clear(titleGenerations)
	}
	if ok && !cacheValid {
		drawDividers(frame, root, area, themeui.NewDimmer(theme).Dim(newThemeStyles(theme).border))
	}
	var damage []renderer.Damage
	for _, pl := range placements {
		p := tb.panes[pl.ID]
		if p == nil && fallback != nil && pl.ID == fallback.id {
			p = fallback
		}
		if p == nil {
			continue
		}
		focused := tb.tree.Focus == pl.ID
		pl = offsetPlacement(pl, area.X, area.Y)
		if pl.TitleBar.Height > 0 {
			generation := drawPaneTitleBar(frame, pl, p, focused, theme)
			if cacheValid && (titleGenerations == nil || titleGenerations[pl.ID] != generation) {
				titleDamage := pl.TitleBar
				titleDamage.X -= area.X
				titleDamage.Y -= area.Y
				damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: titleDamage.X, Y: titleDamage.Y, Width: titleDamage.Width, Height: titleDamage.Height})
			}
			if titleGenerations != nil {
				titleGenerations[pl.ID] = generation
			}
		}
		if pl.Collapsed || pl.Content.Width <= 0 || pl.Content.Height <= 0 {
			if consumeDamage {
				p.mu.Lock()
				captured := capturePaneRenderStateLocked(p, domain.Rect{})
				p.screen.AcknowledgeDamage(captured.damageGeneration)
				p.mu.Unlock()
			}
			continue
		}
		var paneFrame renderer.Frame
		var paneDamage []renderer.Damage
		if consumeDamage {
			p.mu.Lock()
			captured := capturePaneRenderStateLocked(p, pl.Content)
			p.screen.AcknowledgeDamage(captured.damageGeneration)
			p.mu.Unlock()
			paneFrame, paneDamage = captured.frame, captured.damage
		} else {
			p.mu.Lock()
			paneDamage = p.screen.Damage()
			if !cacheValid || len(paneDamage) > 0 {
				blitPaneFrame(frame, pl.Content, p.screen.Frame, !focused, themeui.NewDimmer(theme, themeui.WithForegroundDimming(inactivePaneForegroundDimming)))
			}
			for _, d := range paneDamage {
				localContent := pl.Content
				localContent.X -= area.X
				localContent.Y -= area.Y
				damage = append(damage, translatePaneDamage(d, localContent, contentArea)...)
			}
			p.mu.Unlock()
			continue
		}
		if !cacheValid || len(paneDamage) > 0 {
			blitPaneFrame(frame, pl.Content, paneFrame, !focused, themeui.NewDimmer(theme, themeui.WithForegroundDimming(inactivePaneForegroundDimming)))
		}
		for _, d := range paneDamage {
			localContent := pl.Content
			localContent.X -= area.X
			localContent.Y -= area.Y
			damage = append(damage, translatePaneDamage(d, localContent, contentArea)...)
		}
	}
	return damage
}

func sameTabLayoutSnapshot(a, b tabLayoutSnapshot) bool {
	return a.ok == b.ok && a.root == b.root && a.fingerprint == b.fingerprint && a.area == b.area && a.focus == b.focus
}

func clearFrameRect(frame renderer.Frame, r domain.Rect) {
	blank := renderer.BlankCell()
	for y := r.Y; y < r.Y+r.Height && y < frame.Height; y++ {
		for x := r.X; x < r.X+r.Width && x < frame.Width; x++ {
			frame.Set(x, y, blank)
		}
	}
}

func drawPaneTitleBar(frame renderer.Frame, pl layout.Placement, p *pane, focused bool, theme themeui.Theme) uint64 {
	styles := newThemeStyles(theme)
	style := styles.border
	if focused {
		style = styles.statusBar
	} else {
		style = themeui.NewDimmer(theme).Dim(style)
	}
	for x := pl.TitleBar.X; x < pl.TitleBar.X+pl.TitleBar.Width && x < frame.Width; x++ {
		frame.Set(x, pl.TitleBar.Y, renderer.Cell{Rune: ' ', Style: style})
	}
	p.mu.Lock()
	title := p.displayTitleLocked()
	generation := p.title.generation
	p.mu.Unlock()
	ui.DrawText(frame, pl.TitleBar.X, pl.TitleBar.Y, pl.TitleBar.X+pl.TitleBar.Width, title, style)
	return generation
}
