package ui

import "github.com/bnema/vev/internal/domain"

const (
	ResponsiveDrawerBreakpoint = 80
	DrawerContextRows          = 3
	DrawerBottomRows           = 1
)

// PresentationMode describes whether an overlay uses its preferred floating
// geometry or becomes a narrow-terminal drawer.
type PresentationMode uint8

const (
	PresentationFloating PresentationMode = iota
	PresentationDrawer
)

// BorderEdges identifies the edges drawn around a presentation.
type BorderEdges uint8

const (
	BorderTop BorderEdges = 1 << iota
	BorderRight
	BorderBottom
	BorderLeft
	BorderAll = BorderTop | BorderRight | BorderBottom | BorderLeft
)

// Presentation is the resolved outer and content geometry for an overlay.
type Presentation struct {
	Mode    PresentationMode
	Bounds  domain.Rect
	Inner   domain.Rect
	Borders BorderEdges
}

// ResolvePresentation preserves preferred geometry on complete frames at or
// above the responsive breakpoint. Narrow frames use a full-width bottom
// drawer while reserving frame rows 0-2 and the bottom bar.
func ResolvePresentation(base domain.Size, preferredBounds, preferredInner domain.Rect) Presentation {
	if !base.Valid() {
		return Presentation{}
	}
	if base.Cols >= ResponsiveDrawerBreakpoint {
		return Presentation{
			Mode:    PresentationFloating,
			Bounds:  preferredBounds,
			Inner:   preferredInner,
			Borders: BorderAll,
		}
	}

	available := max(0, base.Rows-DrawerContextRows-DrawerBottomRows)
	height := min(max(preferredBounds.Height, 0), available)
	bounds := domain.Rect{
		X:      0,
		Y:      base.Rows - DrawerBottomRows - height,
		Width:  base.Cols,
		Height: height,
	}
	inner := domain.Rect{
		X:      0,
		Y:      bounds.Y + min(1, height),
		Width:  base.Cols,
		Height: max(0, height-1),
	}
	return Presentation{
		Mode:    PresentationDrawer,
		Bounds:  bounds,
		Inner:   inner,
		Borders: BorderTop,
	}
}
