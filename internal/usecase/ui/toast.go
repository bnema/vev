package ui

import (
	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

const (
	defaultToastMargin   = 2
	defaultToastPaddingX = 1
	defaultToastMaxWidth = 60
)

// Toast describes a transient message rendered over a frame.
type Toast struct {
	Message string
	// Severity is carried for callers that style by it (client toast
	// borders); DrawToast does not read it.
	Severity domain.NoticeSeverity
	Anchor   domain.Anchor
	MinWidth int
	MaxWidth int
	PaddingX int
	PaddingY int
}

// ToastStyles contains styles used when drawing a toast.
type ToastStyles struct {
	Text renderer.Style
	Box  renderer.Style
}

// ToastBounds returns the toast rectangle positioned within base and clamped to base.
func ToastBounds(base domain.Size, toast Toast) domain.Rect {
	if base.Cols <= 0 || base.Rows <= 0 {
		return domain.Rect{}
	}

	paddingX := toast.PaddingX
	if paddingX == 0 {
		paddingX = defaultToastPaddingX
	}
	const borderWidth = 2
	const borderHeight = 2

	maxWidth := toast.MaxWidth
	if maxWidth <= 0 {
		maxWidth = defaultToastMaxWidth
	}
	width := textWidth(toast.Message) + paddingX*2 + borderWidth
	width = max(width, toast.MinWidth)
	width = clamp(width, 0, maxWidth)
	width = clamp(width, 0, base.Cols)

	height := 1 + toast.PaddingY*2 + borderHeight
	height = clamp(height, 0, base.Rows)

	return Place(base, domain.Size{Cols: width, Rows: height}, toast.Anchor, Margins{
		Top:    defaultToastMargin,
		Right:  defaultToastMargin,
		Bottom: defaultToastMargin,
		Left:   defaultToastMargin,
	})
}

// DrawToast draws one bordered toast at its anchor within frame.
func DrawToast(frame renderer.Frame, toast Toast, styles ToastStyles) {
	bounds := ToastBounds(domain.Size{Cols: frame.Width, Rows: frame.Height}, toast)
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return
	}

	DrawBox(frame, bounds, styles.Box)
	inner := domain.Rect{X: bounds.X + 1, Y: bounds.Y + 1, Width: max(0, bounds.Width-2), Height: max(0, bounds.Height-2)}
	if inner.Width <= 0 || inner.Height <= 0 {
		return
	}

	fill := renderer.BlankCell()
	fill.Style = styles.Text
	FillRect(frame, inner, fill)

	paddingX := toast.PaddingX
	if paddingX == 0 {
		paddingX = defaultToastPaddingX
	}
	textY := inner.Y + toast.PaddingY
	if textY >= inner.Y+inner.Height {
		return
	}
	textLeft := inner.X + paddingX
	textRight := inner.X + inner.Width - paddingX
	if textRight <= textLeft {
		return
	}
	DrawText(frame, textLeft, textY, textRight, truncateText(toast.Message, textRight-textLeft), styles.Text)
}

// TruncateText clips text to maxWidth cells, appending an ellipsis when cut.
func TruncateText(text string, maxWidth int) string { return truncateText(text, maxWidth) }

func truncateText(text string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	if textWidth(text) <= maxWidth {
		return text
	}
	ellipsis := "…"
	ellipsisWidth := textWidth(ellipsis)
	if maxWidth < ellipsisWidth {
		return ""
	}
	limit := maxWidth - ellipsisWidth
	out := make([]rune, 0, len(text))
	width := 0
	for _, r := range text {
		w := renderer.RuneWidth(r)
		if w == 0 {
			continue
		}
		if width+w > limit {
			break
		}
		out = append(out, r)
		width += w
	}
	return string(out) + ellipsis
}
