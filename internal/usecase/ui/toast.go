package ui

import (
	"strconv"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	defaultToastMargin   = 2
	defaultToastPaddingX = 1
	defaultToastMaxWidth = 60
)

// ToastAnchor describes where a toast is positioned within a frame.
type ToastAnchor int

const (
	ToastTopLeft ToastAnchor = iota
	ToastTopRight
	ToastBottomLeft
	ToastBottomRight
	ToastCenter
)

// Toast describes a transient message rendered over a frame.
type Toast struct {
	ID            string
	Message       string
	Anchor        ToastAnchor
	Duration      time.Duration
	DimBackground bool
	MinWidth      int
	MaxWidth      int
	PaddingX      int
	PaddingY      int
}

// ActiveToast is a toast currently tracked by a manager.
type ActiveToast struct {
	Toast
	ShownAt time.Time
}

// ToastManager tracks currently visible toasts.
type ToastManager struct {
	active map[string]ActiveToast
}

// ToastStyles contains styles used when drawing a toast.
type ToastStyles struct {
	Text renderer.Style
	Box  renderer.Style
}

// DimStyleFunc transforms an existing cell style for dimmed backgrounds.
type DimStyleFunc func(renderer.Style) renderer.Style

// NewToastManager creates an empty toast manager.
func NewToastManager() *ToastManager {
	return &ToastManager{active: make(map[string]ActiveToast)}
}

// Show records toast as active at now. Empty IDs are replaced with a stable key.
func (m *ToastManager) Show(now time.Time, toast Toast) {
	if m.active == nil {
		m.active = make(map[string]ActiveToast)
	}
	id := toast.ID
	if id == "" {
		id = toastAnchorID(toast.Anchor)
		toast.ID = id
	}
	m.active[id] = ActiveToast{Toast: toast, ShownAt: now}
}

func toastAnchorID(anchor ToastAnchor) string {
	return "anchor:" + strconv.Itoa(int(anchor))
}

// Dismiss removes a toast by ID.
func (m *ToastManager) Dismiss(id string) {
	delete(m.active, id)
}

// Clear removes all toasts.
func (m *ToastManager) Clear() {
	m.active = make(map[string]ActiveToast)
}

// Active returns non-expired toasts at now and prunes expired entries.
func (m *ToastManager) Active(now time.Time) []ActiveToast {
	if m.active == nil {
		return nil
	}
	active := make([]ActiveToast, 0, len(m.active))
	for id, toast := range m.active {
		if toast.Duration > 0 && !now.Before(toast.ShownAt.Add(toast.Duration)) {
			delete(m.active, id)
			continue
		}
		active = append(active, toast)
	}
	return active
}

// HasActive reports whether any toast is active at now.
func (m *ToastManager) HasActive(now time.Time) bool {
	return len(m.Active(now)) > 0
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

	x := 0
	y := 0
	switch toast.Anchor {
	case ToastTopLeft:
		x = defaultToastMargin
		y = defaultToastMargin
	case ToastTopRight:
		x = base.Cols - defaultToastMargin - width
		y = defaultToastMargin
	case ToastBottomLeft:
		x = defaultToastMargin
		y = base.Rows - defaultToastMargin - height
	case ToastBottomRight:
		x = base.Cols - defaultToastMargin - width
		y = base.Rows - defaultToastMargin - height
	case ToastCenter:
		x = (base.Cols - width) / 2
		y = (base.Rows - height) / 2
	}

	return domain.Rect{
		X:      clamp(x, 0, max(0, base.Cols-width)),
		Y:      clamp(y, 0, max(0, base.Rows-height)),
		Width:  width,
		Height: height,
	}
}

// CompositeToasts draws the latest toast per anchor over frame.
func CompositeToasts(frame renderer.Frame, toasts []ActiveToast, styles ToastStyles, dim DimStyleFunc) {
	visible := latestToastsByAnchor(toasts)
	if len(visible) == 0 {
		return
	}

	if dim != nil {
		shouldDim := false
		for _, toast := range visible {
			if toast.DimBackground {
				shouldDim = true
				break
			}
		}
		if shouldDim {
			for y := 0; y < frame.Height; y++ {
				for x := 0; x < frame.Width; x++ {
					cell := frame.At(x, y)
					cell.Style = dim(cell.Style)
					frame.Set(x, y, cell)
				}
			}
		}
	}

	for _, toast := range visible {
		drawToast(frame, toast.Toast, styles)
	}
}

func latestToastsByAnchor(toasts []ActiveToast) []ActiveToast {
	byAnchor := make(map[ToastAnchor]ActiveToast)
	for _, toast := range toasts {
		current, ok := byAnchor[toast.Anchor]
		if !ok || toast.ShownAt.After(current.ShownAt) || toast.ShownAt.Equal(current.ShownAt) {
			byAnchor[toast.Anchor] = toast
		}
	}
	anchors := []ToastAnchor{ToastTopLeft, ToastTopRight, ToastBottomLeft, ToastBottomRight, ToastCenter}
	visible := make([]ActiveToast, 0, len(byAnchor))
	for _, anchor := range anchors {
		if toast, ok := byAnchor[anchor]; ok {
			visible = append(visible, toast)
		}
	}
	return visible
}

func drawToast(frame renderer.Frame, toast Toast, styles ToastStyles) {
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
