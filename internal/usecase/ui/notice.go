package ui

import (
	"fmt"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	noticeMargin   = 1
	noticeMaxWidth = 60
	noticeMinWidth = 24
	noticeMaxLines = 3
)

// NoticeView is a plain rendering view of one notification. Severity mirrors
// domain.NoticeSeverity as a bare uint8 so this package never needs to depend
// on internal/usecase/daemon for its input type.
type NoticeView struct {
	Severity uint8
	Title    string
	Message  string
	Count    int
}

// NoticeStyles selects the box border style by severity; Text styles both the
// title (drawn on the border) and the wrapped message body.
type NoticeStyles struct {
	Text     renderer.Style
	BoxInfo  renderer.Style
	BoxWarn  renderer.Style
	BoxError renderer.Style
}

// ComposeNotices stacks notice boxes top-right of frame, newest first. Each
// box's width starts at frame.Width*2/5, is capped at noticeMaxWidth (60),
// then floored at noticeMinWidth (24), then clamped down to frame.Width so it
// never exceeds the frame — on frames narrower than the floor, that final
// clamp overrides the floor. Each box is titled with the notice's slug and an
// optional " ×N" count suffix, and its message is word-wrapped to at most 3
// lines. Boxes stack downward with one blank row between them; once the
// count behind overflow is nonzero, a right-aligned "+N more" line follows
// the last box.
func ComposeNotices(frame renderer.Frame, notices []NoticeView, overflow int, styles NoticeStyles) {
	if frame.Width <= 0 || frame.Height <= 0 || (len(notices) == 0 && overflow <= 0) {
		return
	}

	width := frame.Width * 2 / 5
	if width > noticeMaxWidth {
		width = noticeMaxWidth
	}
	if width < noticeMinWidth {
		width = noticeMinWidth
	}
	if width > frame.Width {
		width = frame.Width
	}
	if width <= 0 {
		return
	}
	x := frame.Width - noticeMargin - width
	y := noticeMargin

	for _, n := range notices {
		if y >= frame.Height {
			break
		}
		innerWidth := width - 2
		lines := wrapNoticeText(n.Message, innerWidth, noticeMaxLines)
		if len(lines) == 0 {
			lines = []string{""}
		}
		height := len(lines) + 2
		bounds := domain.Rect{X: x, Y: y, Width: width, Height: height}
		box := noticeBoxStyle(styles, n.Severity)
		DrawBox(frame, bounds, box)

		title := n.Title
		if n.Count > 1 {
			title = fmt.Sprintf("%s ×%d", n.Title, n.Count)
		}
		if title != "" && bounds.Width > 2 {
			left := bounds.X + 1
			right := bounds.X + bounds.Width - 1
			start := max(left, bounds.X+(bounds.Width-textWidth(title))/2)
			DrawText(frame, start, bounds.Y, right, title, box)
		}

		inner := domain.Rect{X: bounds.X + 1, Y: bounds.Y + 1, Width: max(0, bounds.Width-2), Height: max(0, bounds.Height-2)}
		if inner.Width > 0 && inner.Height > 0 {
			fill := renderer.BlankCell()
			fill.Style = styles.Text
			FillRect(frame, inner, fill)
			for i, line := range lines {
				if i >= inner.Height {
					break
				}
				DrawText(frame, inner.X, inner.Y+i, inner.X+inner.Width, line, styles.Text)
			}
		}

		y += height + 1
	}

	if overflow > 0 && y < frame.Height {
		text := fmt.Sprintf("+%d more", overflow)
		start := x + width - textWidth(text)
		if start < x {
			start = x
		}
		DrawText(frame, start, y, x+width, text, styles.Text)
	}
}

func noticeBoxStyle(styles NoticeStyles, severity uint8) renderer.Style {
	switch domain.NoticeSeverity(severity) {
	case domain.NoticeWarn:
		return styles.BoxWarn
	case domain.NoticeError:
		return styles.BoxError
	default:
		return styles.BoxInfo
	}
}

// wrapNoticeText greedily fills lines up to width, then caps at maxLines by
// collapsing any remainder into the last line and truncating it, so an
// overlong message is clipped visibly rather than silently dropped mid-word.
func wrapNoticeText(message string, width, maxLines int) []string {
	if width <= 0 || maxLines <= 0 {
		return nil
	}
	words := strings.Fields(message)
	if len(words) == 0 {
		return nil
	}

	lines := make([]string, 0, maxLines)
	current := words[0]
	for _, w := range words[1:] {
		candidate := current + " " + w
		if textWidth(candidate) <= width {
			current = candidate
			continue
		}
		lines = append(lines, current)
		current = w
	}
	lines = append(lines, current)

	if len(lines) <= maxLines {
		return lines
	}
	rest := strings.Join(lines[maxLines-1:], " ")
	lines = lines[:maxLines-1]
	return append(lines, TruncateText(rest, width))
}
