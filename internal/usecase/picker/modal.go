package picker

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
)

// This file owns the picker's presentation geometry. The serving daemon and the
// presenting client both need it: the daemon sizes the preview it captures from
// the same box the client draws, so the viewport always fits its pane.

// Modal is the picker's floating presentation: a centred box over the session,
// never a full-screen takeover. Both sides resolve it against the terminal size
// they are working with.
var Modal = ui.Modal{
	WidthPct: 80, HeightPct: 80, MinWidth: 24, MinHeight: 8,
	Title: " Sessions ", Anchor: domain.AnchorCenter, Margins: ui.Margins{},
}

// PreviewRect returns the preview pane inside the modal for one terminal size.
// An empty rectangle means this terminal has no room for a preview.
func PreviewRect(terminal domain.Size) domain.Rect {
	presentation := Modal.Resolve(terminal)
	return ChooseGeometry(Size(presentation.Inner)).Preview
}

// Size converts a rectangle to the dimensions a frame is built from.
func Size(rect domain.Rect) domain.Size {
	return domain.Size{Cols: rect.Width, Rows: rect.Height}
}
