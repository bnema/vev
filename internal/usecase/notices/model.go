// Package notices renders the notification history overlay: a scrollable,
// newest-first list of the daemon's recorded notifications.
package notices

import (
	"fmt"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

// RenderStyles are the styles Render uses to draw list rows. The zero value
// is never used directly by daemon composition, which always supplies a
// theme-derived RenderStyles the way picker.RenderStyles is supplied.
type RenderStyles struct {
	Background     renderer.Style // fills unused interior rows
	Base           renderer.Style // non-selected row fill
	Selection      renderer.Style // selected row fill
	Text           renderer.Style // non-selected message segment
	SelectionText  renderer.Style // selected row message segment
	Muted          renderer.Style // non-selected severity/time/slug segment
	SelectionMuted renderer.Style // selected row severity/time/slug segment
}

// Model is the history overlay's list state: a snapshot of notifications,
// newest first, with a navigable selection. now anchors the relative
// timestamps rendered next to each row and is fixed at construction so the
// overlay's times don't drift while it stays open.
type Model struct {
	rows     []domain.Notification
	selected int
	now      time.Time
}

// New builds a Model from entries, expected newest-first as returned by the
// notice center's history(). now is the instant relative row times render
// against.
func New(entries []domain.Notification, now time.Time) *Model {
	m := &Model{rows: append([]domain.Notification(nil), entries...), now: now, selected: -1}
	if len(m.rows) > 0 {
		m.selected = 0
	}
	return m
}

func (m *Model) Up() {
	m.move(-1)
}

func (m *Model) Down() {
	m.move(1)
}

func (m *Model) move(delta int) {
	if m == nil || len(m.rows) == 0 {
		return
	}
	next := m.selected + delta
	if next < 0 {
		next = 0
	}
	if next >= len(m.rows) {
		next = len(m.rows) - 1
	}
	m.selected = next
}

// Selected returns the highlighted notification, or false when history is
// empty.
func (m *Model) Selected() (domain.Notification, bool) {
	if m == nil || m.selected < 0 || m.selected >= len(m.rows) {
		return domain.Notification{}, false
	}
	return m.rows[m.selected], true
}

// Clone returns an independent copy safe to render outside the lock that
// guards the live model.
func (m *Model) Clone() *Model {
	if m == nil {
		return nil
	}
	clone := *m
	clone.rows = append([]domain.Notification(nil), m.rows...)
	return &clone
}

// Render draws the history list into inner, scrolling to keep the selection
// visible. Degenerate dimensions draw nothing; an empty history draws a
// centered placeholder instead of a blank box.
func (m *Model) Render(inner domain.Size, styles RenderStyles) renderer.Frame {
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	if frame.Width <= 0 || frame.Height <= 0 {
		return frame
	}
	ui.FillRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, renderer.Cell{Rune: ' ', Style: styles.Background})
	if m == nil || len(m.rows) == 0 {
		text := "No notifications"
		x := max(0, (frame.Width-textWidth(text))/2)
		ui.DrawText(frame, x, frame.Height/2, frame.Width, text, styles.Text)
		return frame
	}
	offset := m.scrollOffset(frame.Height)
	for y := 0; y < frame.Height; y++ {
		idx := offset + y
		if idx >= len(m.rows) {
			break
		}
		n := m.rows[idx]
		base, mutedStyle, textStyle := styles.Base, styles.Muted, styles.Text
		if idx == m.selected {
			base, mutedStyle, textStyle = styles.Selection, styles.SelectionMuted, styles.SelectionText
		}
		ui.FillRect(frame, domain.Rect{X: 0, Y: y, Width: frame.Width, Height: 1}, renderer.Cell{Rune: ' ', Style: base})
		meta := fmt.Sprintf("[%c] %s  %s  ", severityLetter(n.Severity), relativeTime(m.now, n.Time), n.Code.String())
		x := ui.DrawText(frame, 0, y, frame.Width, meta, mutedStyle)
		ui.DrawText(frame, x, y, frame.Width, rowMessage(n), textStyle)
	}
	return frame
}

// rowMessage appends a "×N" coalesce count suffix. The format string matches
// the toast overlay's own count formatting (internal/usecase/ui.ComposeNotices),
// but the field it is appended to differs: the toast appends to Title, while
// this row appends to Message, matching this overlay's row layout.
func rowMessage(n domain.Notification) string {
	if n.Count > 1 {
		return fmt.Sprintf("%s ×%d", n.Message, n.Count)
	}
	return n.Message
}

func (m *Model) scrollOffset(visible int) int {
	if visible <= 0 || len(m.rows) <= visible || m.selected < 0 {
		return 0
	}
	if m.selected < visible {
		return 0
	}
	offset := m.selected - visible + 1
	return min(offset, len(m.rows)-visible)
}

func severityLetter(sev domain.NoticeSeverity) byte {
	switch sev {
	case domain.NoticeWarn:
		return 'W'
	case domain.NoticeError:
		return 'E'
	default:
		return 'I'
	}
}

// relativeTime renders how long ago t was, relative to now, in the coarsest
// unit that keeps the row compact. A non-positive delta (including clock
// skew that puts t in the future) reads as "just now" rather than negative.
func relativeTime(now, t time.Time) string {
	d := now.Sub(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d/time.Minute))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d/time.Hour))
	default:
		return fmt.Sprintf("%dd ago", int(d/(24*time.Hour)))
	}
}

func textWidth(text string) int {
	width := 0
	for _, r := range text {
		width += renderer.RuneWidth(r)
	}
	return width
}
