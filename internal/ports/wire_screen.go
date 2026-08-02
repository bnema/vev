package ports

import (
	"encoding/binary"
	"errors"
	"math"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

// ScreenUpdateKind selects whether an update replaces the complete screen or
// applies a delta to the previously committed screen.
type ScreenUpdateKind uint8

const (
	ScreenUpdateSnapshot ScreenUpdateKind = 1
	ScreenUpdateDelta    ScreenUpdateKind = 2
)

// ScreenCursor is the absolute cursor state carried by each screen update.
type ScreenCursor struct {
	Row, Col uint16
	Style    uint8
	Visible  bool
	StyleSet bool
}

// ScreenScroll describes a full-width upward scroll applied before spans.
type ScreenScroll struct {
	Top, Height, Count uint16
}

// ScreenSpan identifies owned cells beginning at a horizontal position.
type ScreenSpan struct {
	Y, X  uint16
	Cells []renderer.Cell
}

// ScreenUpdate is the semantic structured screen message exchanged by proxy
// daemons. Wire style tables and run IDs remain private to its codec.
type ScreenUpdate struct {
	BaseStateNum uint64
	NewStateNum  uint64
	EchoAck      uint64
	Kind         ScreenUpdateKind
	Size         domain.Size
	Cursor       ScreenCursor
	Scroll       *ScreenScroll
	Spans        []ScreenSpan
}

var (
	// ErrInvalidScreenUpdate reports a malformed or non-canonical screen
	// update. It is deliberately shared by marshal and unmarshal validation.
	ErrInvalidScreenUpdate = errors.New("ports: invalid screen update")
	// ErrScreenUpdateTooLarge reports a payload that cannot fit in a frame.
	ErrScreenUpdateTooLarge = errors.New("ports: screen update too large")
)

const (
	maxScreenCells = 1 << 18

	screenHeaderLen = 40
	screenStyleLen  = 18
	screenSpanLimit = 4096

	// The frame length includes MsgScreenUpdate's type byte. The codec takes
	// only the payload, so one byte is reserved here.
	screenPayloadLimit = MaxFrameLen - 1

	screenFlagCursorVisible  = 1 << 0
	screenFlagCursorStyleSet = 1 << 1
	screenKnownCursorFlags   = screenFlagCursorVisible | screenFlagCursorStyleSet

	styleBoldBit              = 1 << 0
	styleItalicBit            = 1 << 1
	styleInverseBit           = 1 << 2
	styleDimBit               = 1 << 3
	styleUnderlineBit         = 1 << 4
	styleBlinkBit             = 1 << 5
	styleStrikethroughBit     = 1 << 6
	styleForegroundRGBBit     = 1 << 7
	styleBackgroundRGBBit     = 1 << 8
	styleUnderlineColorBit    = 1 << 9
	styleUnderlineColorRGBBit = 1 << 10
	styleKnownBits            = (1 << 11) - 1
)

// MarshalScreenUpdate encodes a canonical structured screen update.
func MarshalScreenUpdate(m ScreenUpdate) ([]byte, error) {
	if err := validateScreenUpdate(m); err != nil {
		return nil, err
	}

	styles := make([]renderer.Style, 0)
	styleIDs := make(map[renderer.Style]uint16)
	for _, span := range m.Spans {
		for _, cell := range span.Cells {
			style := canonicalScreenStyle(cell.Style)
			if _, ok := styleIDs[style]; ok {
				continue
			}
			if len(styles) == math.MaxUint16 {
				return nil, ErrInvalidScreenUpdate
			}
			styleIDs[style] = uint16(len(styles))
			styles = append(styles, style)
		}
	}
	w := screenWriter{b: make([]byte, 0, screenHeaderLen+len(styles)*screenStyleLen)}
	w.u64(m.BaseStateNum)
	w.u64(m.NewStateNum)
	w.u64(m.EchoAck)
	w.u8(uint8(m.Kind))
	w.u16(uint16(m.Size.Cols))
	w.u16(uint16(m.Size.Rows))
	w.u16(m.Cursor.Row)
	w.u16(m.Cursor.Col)
	w.u8(m.Cursor.Style)
	var cursorFlags uint8
	if m.Cursor.Visible {
		cursorFlags |= screenFlagCursorVisible
	}
	if m.Cursor.StyleSet {
		cursorFlags |= screenFlagCursorStyleSet
	}
	w.u8(cursorFlags)
	if m.Scroll != nil {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.u16(uint16(len(styles)))
	w.u16(uint16(len(m.Spans)))
	if m.Scroll != nil {
		w.u16(m.Scroll.Top)
		w.u16(m.Scroll.Height)
		w.u16(m.Scroll.Count)
	}
	for _, style := range styles {
		appendScreenStyle(&w, style)
	}
	for _, span := range m.Spans {
		w.u16(span.Y)
		w.u16(span.X)
		w.u16(uint16(len(span.Cells)))
		runs := screenRunCount(span.Cells)
		w.u16(uint16(runs))
		for start := 0; start < len(span.Cells); {
			style := canonicalScreenStyle(span.Cells[start].Style)
			id := styleIDs[style]
			end := start + 1
			for end < len(span.Cells) && styleIDs[canonicalScreenStyle(span.Cells[end].Style)] == id {
				end++
			}
			w.u16(id)
			w.u16(uint16(end - start))
			for _, cell := range span.Cells[start:end] {
				if cell.Continuation {
					w.uvarint(0)
				} else {
					w.uvarint(uint64(cell.Rune) + 1)
				}
			}
			start = end
		}
	}
	if w.tooLarge {
		return nil, ErrScreenUpdateTooLarge
	}
	return w.b, nil
}

// UnmarshalScreenUpdate strictly decodes one screen-update payload. All
// decoded slices own their data; no input-backed slice is retained.
func UnmarshalScreenUpdate(data []byte) (ScreenUpdate, error) {
	if len(data) > screenPayloadLimit {
		return ScreenUpdate{}, ErrScreenUpdateTooLarge
	}
	if len(data) < screenHeaderLen {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	r := screenReader{b: data}
	var m ScreenUpdate
	var err error
	if m.BaseStateNum, err = r.u64(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if m.NewStateNum, err = r.u64(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if m.EchoAck, err = r.u64(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	kind, err := r.u8()
	if err != nil || (ScreenUpdateKind(kind) != ScreenUpdateSnapshot && ScreenUpdateKind(kind) != ScreenUpdateDelta) {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	m.Kind = ScreenUpdateKind(kind)
	cols, err := r.u16()
	if err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	rows, err := r.u16()
	if err != nil || cols == 0 || rows == 0 || !screenAreaWithinLimit(domain.Size{Cols: int(cols), Rows: int(rows)}) {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	m.Size = domain.Size{Cols: int(cols), Rows: int(rows)}
	if m.Cursor.Row, err = r.u16(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if m.Cursor.Col, err = r.u16(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if m.Cursor.Style, err = r.u8(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	cursorFlags, err := r.u8()
	if err != nil || cursorFlags&^uint8(screenKnownCursorFlags) != 0 || m.Cursor.Row >= rows || m.Cursor.Col >= cols {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	m.Cursor.Visible = cursorFlags&screenFlagCursorVisible != 0
	m.Cursor.StyleSet = cursorFlags&screenFlagCursorStyleSet != 0
	if !m.Cursor.StyleSet && m.Cursor.Style != 0 || m.Cursor.StyleSet && m.Cursor.Style > 6 {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	scrollPresent, err := r.u8()
	if err != nil || scrollPresent > 1 {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	styleCount, err := r.u16()
	if err != nil || uint64(styleCount) > uint64(len(r.b))/screenStyleLen {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	spanCount, err := r.u16()
	if err != nil || spanCount > screenSpanLimit || uint64(spanCount) > uint64(len(r.b))/8 {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if scrollPresent != 0 {
		if m.Kind != ScreenUpdateDelta || len(r.b) < 6 {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		m.Scroll = new(ScreenScroll)
		if m.Scroll.Top, err = r.u16(); err != nil {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if m.Scroll.Height, err = r.u16(); err != nil {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if m.Scroll.Count, err = r.u16(); err != nil {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if !validScreenScroll(*m.Scroll, rows) {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
	} else {
		m.Scroll = nil
	}
	// styleCount was checked against the bytes still available before this
	// allocation. Each style is copied field-by-field below.
	styles := make([]renderer.Style, styleCount)
	seenStyles := make(map[renderer.Style]struct{}, styleCount)
	for i := range styles {
		style, ok := readScreenStyle(&r)
		if !ok {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if _, duplicate := seenStyles[style]; duplicate {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		seenStyles[style] = struct{}{}
		styles[i] = style
	}
	m.Spans = make([]ScreenSpan, spanCount)
	usedStyles := make([]bool, styleCount)
	nextStyleID := uint16(0)
	var previousEnd uint32
	var previousY uint16
	for i := range m.Spans {
		y, errY := r.u16()
		x, errX := r.u16()
		cellCount, errCells := r.u16()
		runCount, errRuns := r.u16()
		if errY != nil || errX != nil || errCells != nil || errRuns != nil || cellCount == 0 || runCount == 0 || runCount > cellCount || uint64(runCount) > uint64(len(r.b))/4 {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if y >= rows || x >= cols || uint64(x)+uint64(cellCount) > uint64(cols) {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if i > 0 {
			if y < previousY || (y == previousY && uint32(x) < previousEnd) {
				return ScreenUpdate{}, ErrInvalidScreenUpdate
			}
		}
		// Ordered non-overlapping spans have at most one cell per screen cell,
		// so the area check above bounds aggregate decoded cells before each
		// per-span allocation. Every cell has at least one token and every run
		// has a four-byte descriptor; check both before allocating the slice.
		minimum := uint64(runCount)*4 + uint64(cellCount)
		if minimum > uint64(len(r.b)) {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		cells := make([]renderer.Cell, cellCount)
		filled := 0
		previousStyleID := uint16(math.MaxUint16)
		for range runCount {
			styleID, e1 := r.u16()
			runCells, e2 := r.u16()
			if e1 != nil || e2 != nil || runCells == 0 || styleID >= styleCount || styleID == previousStyleID || uint32(filled)+uint32(runCells) > uint32(cellCount) {
				return ScreenUpdate{}, ErrInvalidScreenUpdate
			}
			if !usedStyles[styleID] {
				if styleID != nextStyleID {
					return ScreenUpdate{}, ErrInvalidScreenUpdate
				}
				nextStyleID++
				usedStyles[styleID] = true
			}
			previousStyleID = styleID
			for range runCells {
				token, ok := r.uvarint()
				if !ok {
					return ScreenUpdate{}, ErrInvalidScreenUpdate
				}
				if token == 0 {
					cells[filled] = renderer.Cell{Style: styles[styleID], Continuation: true}
				} else {
					runeValue, valid := screenRune(token)
					if !valid {
						return ScreenUpdate{}, ErrInvalidScreenUpdate
					}
					cells[filled] = renderer.Cell{Rune: runeValue, Style: styles[styleID]}
				}
				filled++
			}
		}
		if filled != int(cellCount) {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		m.Spans[i] = ScreenSpan{Y: y, X: x, Cells: cells}
		previousY = y
		previousEnd = uint32(x) + uint32(cellCount)
	}
	if len(r.b) != 0 || nextStyleID != styleCount {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if err := validateScreenUpdate(m); err != nil {
		return ScreenUpdate{}, err
	}
	return m, nil
}

func validateScreenUpdate(m ScreenUpdate) error {
	if m.Kind == ScreenUpdateSnapshot {
		if m.BaseStateNum != 0 || m.NewStateNum == 0 {
			return ErrInvalidScreenUpdate
		}
	} else if m.Kind == ScreenUpdateDelta {
		if m.BaseStateNum == 0 || m.BaseStateNum == math.MaxUint64 || m.NewStateNum != m.BaseStateNum+1 {
			return ErrInvalidScreenUpdate
		}
	} else {
		return ErrInvalidScreenUpdate
	}
	if (m.Kind != ScreenUpdateSnapshot && m.Kind != ScreenUpdateDelta) ||
		m.Size.Cols <= 0 || m.Size.Rows <= 0 || m.Size.Cols > math.MaxUint16 || m.Size.Rows > math.MaxUint16 ||
		!screenAreaWithinLimit(m.Size) || m.Cursor.Row >= uint16(m.Size.Rows) || m.Cursor.Col >= uint16(m.Size.Cols) {
		return ErrInvalidScreenUpdate
	}
	if !m.Cursor.StyleSet && m.Cursor.Style != 0 || m.Cursor.StyleSet && m.Cursor.Style > 6 {
		return ErrInvalidScreenUpdate
	}
	if m.Kind == ScreenUpdateSnapshot && m.Scroll != nil {
		return ErrInvalidScreenUpdate
	}
	if m.Scroll != nil && !validScreenScroll(*m.Scroll, uint16(m.Size.Rows)) {
		return ErrInvalidScreenUpdate
	}
	if len(m.Spans) > screenSpanLimit || len(m.Spans) > math.MaxUint16 || (m.Kind == ScreenUpdateSnapshot && len(m.Spans) == 0) {
		return ErrInvalidScreenUpdate
	}
	return validateScreenSpans(m)
}

func screenAreaWithinLimit(size domain.Size) bool {
	return size.Cols > 0 && size.Rows > 0 && size.Rows <= maxScreenCells/size.Cols
}

func validScreenScroll(scroll ScreenScroll, rows uint16) bool {
	return scroll.Height != 0 && scroll.Count != 0 && scroll.Count < scroll.Height &&
		scroll.Top < rows && uint64(scroll.Top)+uint64(scroll.Height) <= uint64(rows)
}

func validateScreenSpans(m ScreenUpdate) error {
	cols, rows := uint32(m.Size.Cols), uint32(m.Size.Rows)
	var previousY uint16
	var previousEnd uint32
	for i, span := range m.Spans {
		if len(span.Cells) == 0 || len(span.Cells) > math.MaxUint16 || uint32(span.Y) >= rows || uint32(span.X) >= cols || uint64(span.X)+uint64(len(span.Cells)) > uint64(cols) {
			return ErrInvalidScreenUpdate
		}
		if i > 0 && (span.Y < previousY || (span.Y == previousY && uint32(span.X) < previousEnd)) {
			return ErrInvalidScreenUpdate
		}
		for _, cell := range span.Cells {
			if cell.Continuation && cell.Rune != 0 || !cell.Continuation && !utf8.ValidRune(cell.Rune) {
				return ErrInvalidScreenUpdate
			}
			if err := validateScreenStyle(cell.Style); err != nil {
				return err
			}
		}
		previousY = span.Y
		previousEnd = uint32(span.X) + uint32(len(span.Cells))
	}
	if m.Kind == ScreenUpdateSnapshot {
		if len(m.Spans) != m.Size.Rows {
			return ErrInvalidScreenUpdate
		}
		for y, span := range m.Spans {
			if int(span.Y) != y || span.X != 0 || len(span.Cells) != m.Size.Cols {
				return ErrInvalidScreenUpdate
			}
		}
	}
	return nil
}

func validateScreenStyle(style renderer.Style) error {
	if style.Attrs&^(renderer.AttrDim|renderer.AttrUnderline|renderer.AttrBlink|renderer.AttrStrikethrough) != 0 || style.UnderlineStyle > renderer.UnderlineDashed {
		return ErrInvalidScreenUpdate
	}
	if !style.HasForegroundRGB && (style.Foreground < -1 || style.Foreground > math.MaxUint8 || style.ForegroundRGB != (renderer.RGB{})) {
		return ErrInvalidScreenUpdate
	}
	if !style.HasBackgroundRGB && (style.Background < -1 || style.Background > math.MaxUint8 || style.BackgroundRGB != (renderer.RGB{})) {
		return ErrInvalidScreenUpdate
	}
	if style.HasUnderlineColorRGB && style.HasUnderlineColor {
		return ErrInvalidScreenUpdate
	} else if style.HasUnderlineColorRGB {
		// The indexed value is inactive when RGB is selected.
	} else if style.HasUnderlineColor {
		if style.UnderlineColor < 0 || style.UnderlineColor > math.MaxUint8 || style.UnderlineColorRGB != (renderer.RGB{}) {
			return ErrInvalidScreenUpdate
		}
	} else if style.UnderlineColorRGB != (renderer.RGB{}) {
		return ErrInvalidScreenUpdate
	}
	return nil
}

func canonicalScreenStyle(style renderer.Style) renderer.Style {
	if style.HasForegroundRGB {
		style.Foreground = -1
	} else {
		style.ForegroundRGB = renderer.RGB{}
	}
	if style.HasBackgroundRGB {
		style.Background = -1
	} else {
		style.BackgroundRGB = renderer.RGB{}
	}
	if style.HasUnderlineColorRGB {
		style.UnderlineColor = -1
	} else if style.HasUnderlineColor {
		style.UnderlineColorRGB = renderer.RGB{}
	} else {
		style.UnderlineColor = -1
		style.UnderlineColorRGB = renderer.RGB{}
	}
	return style
}

func screenStyleBits(style renderer.Style) uint16 {
	var bits uint16
	if style.Bold {
		bits |= styleBoldBit
	}
	if style.Italic {
		bits |= styleItalicBit
	}
	if style.Inverse {
		bits |= styleInverseBit
	}
	if style.Attrs&renderer.AttrDim != 0 {
		bits |= styleDimBit
	}
	if style.Attrs&renderer.AttrUnderline != 0 {
		bits |= styleUnderlineBit
	}
	if style.Attrs&renderer.AttrBlink != 0 {
		bits |= styleBlinkBit
	}
	if style.Attrs&renderer.AttrStrikethrough != 0 {
		bits |= styleStrikethroughBit
	}
	if style.HasForegroundRGB {
		bits |= styleForegroundRGBBit
	}
	if style.HasBackgroundRGB {
		bits |= styleBackgroundRGBBit
	}
	if style.HasUnderlineColor {
		bits |= styleUnderlineColorBit
	}
	if style.HasUnderlineColorRGB {
		bits |= styleUnderlineColorRGBBit
	}
	return bits
}

func appendScreenStyle(w *screenWriter, style renderer.Style) {
	style = canonicalScreenStyle(style)
	w.u16(screenStyleBits(style))
	w.u16(screenColorIndex(style.Foreground))
	w.u16(screenColorIndex(style.Background))
	w.u16(screenColorIndex(style.UnderlineColor))
	w.u8(style.ForegroundRGB.R)
	w.u8(style.ForegroundRGB.G)
	w.u8(style.ForegroundRGB.B)
	w.u8(style.BackgroundRGB.R)
	w.u8(style.BackgroundRGB.G)
	w.u8(style.BackgroundRGB.B)
	w.u8(style.UnderlineColorRGB.R)
	w.u8(style.UnderlineColorRGB.G)
	w.u8(style.UnderlineColorRGB.B)
	w.u8(uint8(style.UnderlineStyle))
}

func screenColorIndex(index int) uint16 {
	if index < 0 {
		return math.MaxUint16
	}
	return uint16(index)
}

func screenColorValue(index uint16) int {
	if index == math.MaxUint16 {
		return -1
	}
	return int(index)
}

func readScreenStyle(r *screenReader) (renderer.Style, bool) {
	bits, ok := r.u16ok()
	if !ok || bits&^uint16(styleKnownBits) != 0 {
		return renderer.Style{}, false
	}
	foregroundIndex, ok := r.u16ok()
	if !ok {
		return renderer.Style{}, false
	}
	backgroundIndex, ok := r.u16ok()
	if !ok {
		return renderer.Style{}, false
	}
	underlineIndex, ok := r.u16ok()
	if !ok {
		return renderer.Style{}, false
	}
	fgRGB, ok := r.rgb()
	if !ok {
		return renderer.Style{}, false
	}
	bgRGB, ok := r.rgb()
	if !ok {
		return renderer.Style{}, false
	}
	underlineRGB, ok := r.rgb()
	if !ok {
		return renderer.Style{}, false
	}
	underlineStyle, ok := r.u8ok()
	if !ok || underlineStyle > uint8(renderer.UnderlineDashed) {
		return renderer.Style{}, false
	}
	style := renderer.Style{
		Bold:                 bits&styleBoldBit != 0,
		Italic:               bits&styleItalicBit != 0,
		Inverse:              bits&styleInverseBit != 0,
		UnderlineStyle:       renderer.UnderlineStyle(underlineStyle),
		Foreground:           screenColorValue(foregroundIndex),
		Background:           screenColorValue(backgroundIndex),
		HasForegroundRGB:     bits&styleForegroundRGBBit != 0,
		ForegroundRGB:        fgRGB,
		HasBackgroundRGB:     bits&styleBackgroundRGBBit != 0,
		BackgroundRGB:        bgRGB,
		HasUnderlineColor:    bits&styleUnderlineColorBit != 0,
		UnderlineColor:       screenColorValue(underlineIndex),
		HasUnderlineColorRGB: bits&styleUnderlineColorRGBBit != 0,
		UnderlineColorRGB:    underlineRGB,
	}
	if style.HasUnderlineColor && style.HasUnderlineColorRGB {
		return renderer.Style{}, false
	}
	style.Attrs = 0
	if bits&styleDimBit != 0 {
		style.Attrs |= renderer.AttrDim
	}
	if bits&styleUnderlineBit != 0 {
		style.Attrs |= renderer.AttrUnderline
	}
	if bits&styleBlinkBit != 0 {
		style.Attrs |= renderer.AttrBlink
	}
	if bits&styleStrikethroughBit != 0 {
		style.Attrs |= renderer.AttrStrikethrough
	}
	if style.HasForegroundRGB {
		if foregroundIndex != math.MaxUint16 {
			return renderer.Style{}, false
		}
		style.Foreground = -1
	} else if foregroundIndex != math.MaxUint16 && foregroundIndex > math.MaxUint8 || fgRGB != (renderer.RGB{}) {
		return renderer.Style{}, false
	}
	if style.HasBackgroundRGB {
		if backgroundIndex != math.MaxUint16 {
			return renderer.Style{}, false
		}
		style.Background = -1
	} else if backgroundIndex != math.MaxUint16 && backgroundIndex > math.MaxUint8 || bgRGB != (renderer.RGB{}) {
		return renderer.Style{}, false
	}
	if style.HasUnderlineColorRGB {
		if underlineIndex != math.MaxUint16 {
			return renderer.Style{}, false
		}
		style.UnderlineColor = -1
	} else if style.HasUnderlineColor {
		if underlineIndex == math.MaxUint16 || underlineIndex > math.MaxUint8 || underlineRGB != (renderer.RGB{}) {
			return renderer.Style{}, false
		}
	} else if underlineIndex != math.MaxUint16 || underlineRGB != (renderer.RGB{}) {
		return renderer.Style{}, false
	}
	return style, true
}

func screenRunCount(cells []renderer.Cell) int {
	if len(cells) == 0 {
		return 0
	}
	n := 1
	previous := canonicalScreenStyle(cells[0].Style)
	for _, cell := range cells[1:] {
		style := canonicalScreenStyle(cell.Style)
		if style != previous {
			n++
			previous = style
		}
	}
	return n
}

func screenRune(token uint64) (rune, bool) {
	if token == 0 || token > uint64(utf8.MaxRune)+1 {
		return 0, false
	}
	r := rune(token - 1)
	return r, utf8.ValidRune(r)
}

type screenWriter struct {
	b        []byte
	tooLarge bool
}

func (w *screenWriter) append(p ...byte) {
	if w.tooLarge || len(w.b) > screenPayloadLimit-len(p) {
		w.tooLarge = true
		return
	}
	w.b = append(w.b, p...)
}
func (w *screenWriter) u8(v uint8)   { w.append(v) }
func (w *screenWriter) u16(v uint16) { w.append(byte(v>>8), byte(v)) }
func (w *screenWriter) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.append(b[:]...)
}
func (w *screenWriter) uvarint(v uint64) {
	var b [10]byte
	n := binary.PutUvarint(b[:], v)
	w.append(b[:n]...)
}

type screenReader struct{ b []byte }

func (r *screenReader) take(n int) ([]byte, bool) {
	if n < 0 || len(r.b) < n {
		return nil, false
	}
	p := r.b[:n]
	r.b = r.b[n:]
	return p, true
}
func (r *screenReader) u8() (uint8, error) {
	v, ok := r.u8ok()
	if !ok {
		return 0, ErrInvalidScreenUpdate
	}
	return v, nil
}
func (r *screenReader) u8ok() (uint8, bool) {
	p, ok := r.take(1)
	if !ok {
		return 0, false
	}
	return p[0], true
}
func (r *screenReader) u16() (uint16, error) {
	p, ok := r.take(2)
	if !ok {
		return 0, ErrInvalidScreenUpdate
	}
	return binary.BigEndian.Uint16(p), nil
}
func (r *screenReader) u16ok() (uint16, bool) {
	p, ok := r.take(2)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint16(p), true
}
func (r *screenReader) u64() (uint64, error) {
	p, ok := r.take(8)
	if !ok {
		return 0, ErrInvalidScreenUpdate
	}
	return binary.BigEndian.Uint64(p), nil
}
func (r *screenReader) rgb() (renderer.RGB, bool) {
	p, ok := r.take(3)
	if !ok {
		return renderer.RGB{}, false
	}
	return renderer.RGB{R: p[0], G: p[1], B: p[2]}, true
}
func (r *screenReader) uvarint() (uint64, bool) {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		return 0, false
	}
	var canonical [binary.MaxVarintLen64]byte
	if binary.PutUvarint(canonical[:], v) != n {
		return 0, false
	}
	r.b = r.b[n:]
	return v, true
}
