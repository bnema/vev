package copy

import (
	"encoding/base64"
	"strings"

	"github.com/bnema/vev/pkg/renderer"
)

// OSC52MaxPayloadBytes caps clipboard payloads while vev intentionally emits
// exactly one OSC 52 sequence. Splitting one clipboard copy across multiple OSC
// 52 sequences replaces the clipboard repeatedly in common terminals, corrupting
// the intended result, so oversized selections are deferred until a terminal-
// specific continuation protocol is supported.
const OSC52MaxPayloadBytes = 75_000

// Snapshot is the copy-mode document: scrollback followed by the live screen.
type Snapshot struct {
	Rows   [][]renderer.Cell
	Width  int
	Height int
}

// NewSnapshot copies rows from scrollback and then the live screen frame.
func NewSnapshot(sb *Scrollback, screen renderer.Frame) Snapshot {
	scrollRows := 0
	if sb != nil {
		scrollRows = sb.Len()
	}
	rows := make([][]renderer.Cell, 0, scrollRows+screen.Height)
	if sb != nil {
		for i := range sb.Len() {
			rows = append(rows, sb.Row(i))
		}
	}
	for y := range screen.Height {
		rows = append(rows, append([]renderer.Cell(nil), screen.Row(y)...))
	}
	return Snapshot{Rows: rows, Width: screen.Width, Height: screen.Height}
}

// Mode stores per-client copy-mode viewport and line-selection state.
type Mode struct {
	ViewportTop int
	Cursor      int
	Anchor      int
	Selecting   bool
}

func NewMode(s Snapshot) *Mode {
	m := &Mode{Anchor: -1}
	m.ViewportTop = max(len(s.Rows)-s.Height, 0)
	m.Cursor = max(len(s.Rows)-1, 0)
	m.clamp(s)
	return m
}

func (m *Mode) Move(s Snapshot, delta int) { m.setCursor(s, m.Cursor+delta) }
func (m *Mode) Page(s Snapshot, pages int) { m.setCursor(s, m.Cursor+pages*max(s.Height, 1)) }
func (m *Mode) Top(s Snapshot)             { m.setCursor(s, 0) }
func (m *Mode) Bottom(s Snapshot)          { m.setCursor(s, len(s.Rows)-1) }

func (m *Mode) ToggleSelection() {
	if m.Selecting {
		m.Selecting = false
		m.Anchor = -1
		return
	}
	m.Selecting = true
	m.Anchor = m.Cursor
}

func (m *Mode) setCursor(s Snapshot, cursor int) {
	m.Cursor = cursor
	m.clamp(s)
	if m.Cursor < m.ViewportTop {
		m.ViewportTop = m.Cursor
	}
	if bottom := m.ViewportTop + max(s.Height, 1) - 1; m.Cursor > bottom {
		m.ViewportTop = m.Cursor - max(s.Height, 1) + 1
	}
	m.clamp(s)
}

func (m *Mode) clamp(s Snapshot) {
	total := len(s.Rows)
	if total == 0 {
		m.Cursor, m.ViewportTop = 0, 0
		return
	}
	m.Cursor = min(max(m.Cursor, 0), total-1)
	maxTop := max(total-max(s.Height, 1), 0)
	m.ViewportTop = min(max(m.ViewportTop, 0), maxTop)
}

func (m *Mode) SelectedBounds() (int, int, bool) {
	if !m.Selecting || m.Anchor < 0 {
		return 0, 0, false
	}
	lo, hi := m.Anchor, m.Cursor
	if lo > hi {
		lo, hi = hi, lo
	}
	return lo, hi, true
}

func (m *Mode) SelectedText(s Snapshot) string {
	lo, hi, ok := m.SelectedBounds()
	if !ok || len(s.Rows) == 0 {
		return ""
	}
	lo = max(lo, 0)
	hi = min(hi, len(s.Rows)-1)
	lines := make([]string, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		lines = append(lines, rowString(s.Rows[i]))
	}
	return strings.Join(lines, "\n")
}

func (m *Mode) Render(s Snapshot) renderer.Frame {
	m.clamp(s)
	frame := renderer.NewFrame(s.Width, s.Height+1)
	lo, hi, selected := m.SelectedBounds()
	for y := range s.Height {
		src := m.ViewportTop + y
		if src >= len(s.Rows) {
			break
		}
		row := frame.Row(y)
		copy(row, s.Rows[src])
		if selected && src >= lo && src <= hi {
			for x := range row {
				row[x].Style.Inverse = !row[x].Style.Inverse
			}
		}
	}
	drawCopyStatus(frame.Row(s.Height), m, len(s.Rows))
	return frame
}

func rowString(row []renderer.Cell) string {
	runes := make([]rune, 0, len(row))
	for _, c := range row {
		if c.Continuation {
			continue
		}
		r := c.Rune
		if r == 0 {
			r = ' '
		}
		runes = append(runes, r)
	}
	return strings.TrimRight(string(runes), " ")
}

func drawCopyStatus(row []renderer.Cell, m *Mode, total int) {
	for i := range row {
		row[i] = renderer.BlankCell()
	}
	text := " [COPY] "
	if total > 0 {
		text += strconvItoa(m.Cursor+1) + "/" + strconvItoa(total) + " "
	} else {
		text += "0/0 "
	}
	style := renderer.DefaultStyle()
	style.Inverse = true
	for i, r := range text {
		if i >= len(row) {
			break
		}
		row[i] = renderer.Cell{Rune: r, Style: style}
	}
}

// OSC52 encodes text for clipboard transfer as one complete OSC 52 sequence.
// Oversized payloads return no sequence rather than emitting corrupting
// multi-sequence replacements.
func OSC52(text string) [][]byte {
	if len([]byte(text)) > OSC52MaxPayloadBytes {
		return nil
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(text))
	return [][]byte{[]byte("\x1b]52;c;" + encoded + "\x07")}
}

func strconvItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
