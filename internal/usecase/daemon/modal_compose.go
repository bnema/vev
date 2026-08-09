package daemon

import (
	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

func composeModalClientFrame(base renderer.Frame, modal ui.Modal, styles themeui.Styles, renderInner func(domain.Size) renderer.Frame) (renderer.Frame, []renderer.Damage) {
	presentation := modal.Resolve(domain.Size{Cols: base.Width, Rows: base.Height})
	modal.CompositePresentation(base, presentation, styles.BorderMuted, styles.PickerBase)
	modalFrame := renderInner(rectSize(presentation.Inner))
	copyModalInner(base, presentation.Inner, modalFrame)
	return base, []renderer.Damage{renderer.FullRedraw()}
}
