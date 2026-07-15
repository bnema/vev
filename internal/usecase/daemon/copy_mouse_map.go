package daemon

import (
	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/mouse"
)

// clientTopBarRows is deliberately kept at the client/frame boundary. Layout
// coordinates are content-relative and must cross this boundary exactly once.
const clientTopBarRows = 1

type copyMouseGeometry struct {
	pane    *pane
	content domain.Rect // absolute client-frame coordinates
}

type mappedCopyMouse struct {
	pane *pane
	pos  scopy.Pos
}

func copyContentToClientRect(content domain.Rect) domain.Rect {
	content.Y += clientTopBarRows
	return content
}

func mapCopyMouse(ev mouse.Event, geometry copyMouseGeometry, viewportTop int, document *scopy.Document, clamp bool) (mappedCopyMouse, bool) {
	if document == nil || geometry.pane == nil || geometry.content.Width <= 0 || geometry.content.Height <= 0 {
		return mappedCopyMouse{}, false
	}
	col, row := ev.Col, ev.Row
	if clamp {
		col = clampInt(col, geometry.content.X, geometry.content.X+geometry.content.Width-1)
		row = clampInt(row, geometry.content.Y, geometry.content.Y+geometry.content.Height-1)
	} else if !pointInRect(col, row, geometry.content) {
		return mappedCopyMouse{}, false
	}
	pos, ok := document.Normalize(scopy.Pos{Row: viewportTop + row - geometry.content.Y, Col: col - geometry.content.X})
	if !ok {
		return mappedCopyMouse{}, false
	}
	return mappedCopyMouse{pane: geometry.pane, pos: pos}, true
}

// hitTestCopyMouseGeometryLocked resolves a new press. Floating geometry is
// exclusive while visible; normal title bars and dividers are intentionally
// not copy content. Caller holds tb.mu.
func hitTestCopyMouseGeometryLocked(tb *tab, cfg domain.FloatingConfig, col, row int) (copyMouseGeometry, bool) {
	if tb == nil || row < clientTopBarRows {
		return copyMouseGeometry{}, false
	}
	if p, geometry, visible := tb.visibleFloatingSnapshotLocked(cfg); visible {
		content := copyContentToClientRect(geometry.Inner)
		if !pointInRect(col, row, content) {
			return copyMouseGeometry{}, false
		}
		return copyMouseGeometry{pane: p, content: content}, true
	}
	pl, ok := hitTestPlacementLocked(tb, col, row-clientTopBarRows)
	if !ok || pl.Collapsed || !pointInRect(col, row-clientTopBarRows, pl.Content) {
		return copyMouseGeometry{}, false
	}
	p := tb.panes[pl.ID]
	if p == nil {
		return copyMouseGeometry{}, false
	}
	return copyMouseGeometry{pane: p, content: copyContentToClientRect(pl.Content)}, true
}

// copyMouseGeometryForPaneLocked resolves geometry for an existing drag. It
// never hit-tests the new pointer, preventing cross-pane document switches.
func copyMouseGeometryForPaneLocked(tb *tab, cfg domain.FloatingConfig, target *pane) (copyMouseGeometry, bool) {
	if tb == nil || target == nil {
		return copyMouseGeometry{}, false
	}
	if p, geometry, visible := tb.visibleFloatingSnapshotLocked(cfg); visible {
		if p != target {
			return copyMouseGeometry{}, false
		}
		return copyMouseGeometry{pane: p, content: copyContentToClientRect(geometry.Inner)}, true
	}
	if tb.panes[target.id] != target {
		return copyMouseGeometry{}, false
	}
	placements, ok := solvedPlacementsLocked(tb)
	if !ok {
		return copyMouseGeometry{}, false
	}
	for _, pl := range placements {
		if pl.ID == target.id && !pl.Collapsed && pl.Content.Width > 0 && pl.Content.Height > 0 {
			return copyMouseGeometry{pane: target, content: copyContentToClientRect(pl.Content)}, true
		}
	}
	return copyMouseGeometry{}, false
}
