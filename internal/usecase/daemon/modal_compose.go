package daemon

import (
	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

func composeModalClientFrame(base renderer.Frame, modal ui.Modal, styles themeui.Styles, renderInner func(domain.Size) renderer.Frame) (renderer.Frame, []renderer.Damage) {
	inner := modal.Composite(base, styles.BorderMuted, styles.PickerBase)
	modalFrame := renderInner(domain.Size{Cols: inner.Width, Rows: inner.Height})
	for y := range min(inner.Height, modalFrame.Height) {
		for x := range min(inner.Width, modalFrame.Width) {
			base.Set(inner.X+x, inner.Y+y, modalFrame.At(x, y))
		}
	}
	return base, []renderer.Damage{renderer.FullRedraw()}
}
