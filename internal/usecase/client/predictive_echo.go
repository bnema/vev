package client

import (
	"bytes"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// Predictive local echo, after mosh's PredictionEngine
// (src/frontend/terminaloverlay.cc).
//
// On a slow link the client guesses the echo of printable keys and backspace
// on the cursor row and draws it before the daemon confirms it. The guess is
// checked against a local mirror of the daemon's composed output once the
// daemon acknowledges the input in Output.Echo. Anything the engine cannot
// model (control bytes, escape sequences, wide runes, the last column) makes
// later guesses tentative: they stay hidden until a correct guess of the same
// epoch confirms it. A wrong tentative guess kills its epoch; a wrong
// confirmed guess resets the engine.
//
// The attached loop is the only caller, so the engine holds no lock.

// Adaptive display thresholds from mosh. They apply to the send interval,
// half the smoothed round trip clamped to [20ms, 250ms].
const (
	echoSRTTTriggerLow  = 20 * time.Millisecond
	echoSRTTTriggerHigh = 30 * time.Millisecond
	echoFlagTriggerLow  = 50 * time.Millisecond
	echoFlagTriggerHigh = 80 * time.Millisecond

	echoSendIntervalMin = 20 * time.Millisecond
	echoSendIntervalMax = 250 * time.Millisecond

	// echoGlitchThreshold shows predictions when one is pending this long.
	echoGlitchThreshold = 250 * time.Millisecond
	// echoGlitchFlagThreshold also underlines them.
	echoGlitchFlagThreshold = 5000 * time.Millisecond
	// echoGlitchRepairCount quick confirmations, each at least
	// echoGlitchRepairMinInterval apart, cure a glitch.
	echoGlitchRepairCount       = 10
	echoGlitchRepairMinInterval = 150 * time.Millisecond

	// echoServerDelay is removed from each round-trip sample so the
	// thresholds compare network time, as mosh's SRTT does.
	echoServerDelay = protocol.EchoAckDelay

	// maxEchoSent bounds the send-time history used for round-trip samples.
	maxEchoSent = 256
	// maxEchoOriginals bounds the overwritten contents kept per cell.
	maxEchoOriginals = 8
)

type echoValidity uint8

const (
	echoInactive echoValidity = iota
	echoPending
	echoCorrect
	echoCorrectNoCredit
	echoIncorrect
)

// echoCell is one predicted cell of the prediction row.
type echoCell struct {
	active      bool
	unknown     bool
	expires     uint64
	epoch       uint64
	predictedAt time.Time
	replacement vt.Cell
	originals   []rune
}

func (c *echoCell) tentative(confirmed uint64) bool { return c.epoch > confirmed }

func (c *echoCell) reset() { *c = echoCell{originals: c.originals[:0]} }

// resetWithOriginal keeps a known replacement as overwritten content, so a
// later guess that merely restores it earns no credit.
func (c *echoCell) resetWithOriginal() {
	if !c.active || c.unknown {
		c.reset()
		return
	}
	originals := append(c.originals, c.replacement.Rune)
	if len(originals) > maxEchoOriginals {
		originals = originals[len(originals)-maxEchoOriginals:]
	}
	*c = echoCell{originals: originals}
}

func (c *echoCell) arm(seq, epoch uint64, now time.Time) {
	c.active = true
	c.epoch = epoch
	c.expires = seq
	c.predictedAt = now
}

// echoCursor is the predicted cursor position.
type echoCursor struct {
	active  bool
	row     int
	col     int
	expires uint64
	epoch   uint64
}

type echoSent struct {
	seq uint64
	at  time.Time
}

// echoPredictor is the prediction engine of one attachment run.
type echoPredictor struct {
	mode   domain.EchoPredictMode
	screen *vt.Screen

	sent       []echoSent
	echo       uint64
	srtt       time.Duration
	srttKnown  bool
	srttOn     bool
	flagging   bool
	glitch     int
	lastQuick  time.Time
	predEpoch  uint64
	confEpoch  uint64
	holdUntil  uint64
	row        int
	cells      []echoCell
	cursor     echoCursor
	drawnRow   int
	drawn      []int
	cursorMove bool
	apc        apcFilter
}

func newEchoPredictor(mode domain.EchoPredictMode) *echoPredictor {
	return &echoPredictor{mode: mode, predEpoch: 1, screen: vt.NewScreen(1, 1)}
}

// applyOutput feeds one applied Output to the mirror, samples the round trip
// when Echo advances, and validates pending guesses.
func (p *echoPredictor) applyOutput(output protocol.Output, now time.Time) {
	// Side effects (New == 0) may carry a placeholder size; only state
	// frames describe the grid.
	if output.New != 0 && output.Size.Valid() && (output.Size.Cols != p.screen.Columns() || output.Size.Rows != p.screen.Rows()) {
		p.screen.Resize(output.Size.Cols, output.Size.Rows)
		// The resized frame repaints the whole terminal.
		p.forget()
	}
	if output.Full {
		// A full frame repaints everything: guesses placed against the old
		// frame would land on unrelated content.
		p.forget()
	}
	if data := p.apc.strip(output.Data); len(data) > 0 {
		p.screen.Write(data)
	}
	if output.Echo > p.echo {
		p.sample(output.Echo, now)
		p.echo = output.Echo
	}
	p.cull(now)
}

// sample updates the smoothed round trip from the send time of seq.
func (p *echoPredictor) sample(seq uint64, now time.Time) {
	keep := p.sent[:0]
	for _, sent := range p.sent {
		if sent.seq == seq {
			rtt := max(now.Sub(sent.at)-echoServerDelay, 0)
			if p.srttKnown {
				p.srtt = (7*p.srtt + rtt) / 8
			} else {
				p.srtt, p.srttKnown = rtt, true
			}
		}
		if sent.seq > seq {
			keep = append(keep, sent)
		}
	}
	p.sent = keep
}

func (p *echoPredictor) sendInterval() time.Duration {
	interval := (p.srtt + 1) / 2
	return min(max(interval, echoSendIntervalMin), echoSendIntervalMax)
}

// enabled reports whether the mirror describes a screen the engine can
// predict on: a visible cursor on a usable grid. The mirror holds the
// daemon's composed frame, which never enters the alternate screen, so
// full-screen pane programs rely on epoch validation, as in mosh.
func (p *echoPredictor) enabled() bool {
	return p.mode != domain.EchoPredictNever && p.screen.CursorVisible() &&
		p.screen.Columns() > 1 && p.screen.Rows() > 0
}

func (p *echoPredictor) active() bool {
	if p.cursor.active {
		return true
	}
	for i := range p.cells {
		if p.cells[i].active {
			return true
		}
	}
	return false
}

func (p *echoPredictor) displaying() bool {
	switch p.mode {
	case domain.EchoPredictNever:
		return false
	case domain.EchoPredictAlways:
		return true
	}
	return p.srttOn || p.glitch > 0
}

func (p *echoPredictor) becomeTentative() { p.predEpoch++ }

// reset drops every guess. Drawn cells stay recorded so render repaints them.
func (p *echoPredictor) reset() {
	for i := range p.cells {
		p.cells[i].reset()
	}
	p.cursor = echoCursor{}
	p.becomeTentative()
}

// forget drops every guess and the drawn record, for when something else
// repaints or owns the terminal (a full frame, an overlay).
func (p *echoPredictor) forget() {
	p.reset()
	p.drawn = p.drawn[:0]
	p.cursorMove = false
}

func (p *echoPredictor) killEpoch(epoch uint64) {
	for i := range p.cells {
		if p.cells[i].active && p.cells[i].epoch >= epoch {
			p.cells[i].reset()
		}
	}
	if p.cursor.active && p.cursor.epoch >= epoch {
		p.cursor = echoCursor{}
	}
	p.becomeTentative()
}

// cull validates guesses against the mirror and updates the display
// triggers, as mosh's PredictionEngine::cull.
func (p *echoPredictor) cull(now time.Time) {
	if p.mode == domain.EchoPredictNever {
		return
	}
	if !p.enabled() {
		p.reset()
		return
	}
	if p.srttKnown {
		interval := p.sendInterval()
		if interval > echoSRTTTriggerHigh {
			p.srttOn = true
		} else if p.srttOn && interval <= echoSRTTTriggerLow && !p.active() {
			p.srttOn = false
		}
		if interval > echoFlagTriggerHigh {
			p.flagging = true
		} else if interval <= echoFlagTriggerLow {
			p.flagging = false
		}
	}
	if p.glitch > echoGlitchRepairCount {
		p.flagging = true
	}
	if p.row >= p.screen.Rows() || len(p.cells) != p.screen.Columns() {
		p.cells = nil
	}
	for col := range p.cells {
		cell := &p.cells[col]
		switch p.cellValidity(col) {
		case echoIncorrect:
			if cell.tentative(p.confEpoch) {
				p.killEpoch(cell.epoch)
				continue
			}
			p.reset()
			return
		case echoCorrect:
			p.confEpoch = max(p.confEpoch, cell.epoch)
			if now.Sub(cell.predictedAt) < echoGlitchThreshold && p.glitch > 0 && now.Sub(p.lastQuick) >= echoGlitchRepairMinInterval {
				p.glitch--
				p.lastQuick = now
			}
			// Later guesses take the confirmed style.
			style := p.screen.Cell(col, p.row).Style
			for k := col; k < len(p.cells); k++ {
				p.cells[k].replacement.Style = style
			}
			cell.reset()
		case echoCorrectNoCredit:
			cell.reset()
		case echoPending:
			pending := now.Sub(cell.predictedAt)
			if pending >= echoGlitchFlagThreshold {
				p.glitch = 2 * echoGlitchRepairCount
			} else if pending >= echoGlitchThreshold && p.glitch < echoGlitchRepairCount {
				p.glitch = echoGlitchRepairCount
			}
		}
	}
	if p.cursor.active && p.echo >= p.cursor.expires {
		if p.screen.CursorRow() != p.cursor.row || p.screen.CursorCol() != p.cursor.col {
			p.reset()
			return
		}
		p.cursor = echoCursor{}
	}
}

func (p *echoPredictor) cellValidity(col int) echoValidity {
	cell := &p.cells[col]
	if !cell.active {
		return echoInactive
	}
	if p.echo < cell.expires {
		return echoPending
	}
	if cell.unknown || isBlankRune(cell.replacement.Rune) {
		return echoCorrectNoCredit
	}
	current := p.screen.Cell(col, p.row)
	if current.Rune != cell.replacement.Rune {
		return echoIncorrect
	}
	for _, original := range cell.originals {
		if original == cell.replacement.Rune {
			return echoCorrectNoCredit
		}
	}
	return echoCorrect
}

func isBlankRune(r rune) bool { return r == 0 || r == ' ' }

// isBorderRune reports a box-drawing rune: a pane border or bar the shell
// line never shifts across.
func isBorderRune(r rune) bool { return r >= 0x2500 && r <= 0x257f }

// isPlainCell reports a cell the engine can move and repaint exactly: one
// column, no grapheme or hyperlink payload.
func isPlainCell(cell vt.Cell) bool {
	return !cell.Continuation && cell.Payload.Empty() && (cell.Rune == 0 || vt.RuneWidth(cell.Rune) == 1)
}

// unknownKey gives up on the cursor row until the daemon answers this input.
func (p *echoPredictor) unknownKey(seq uint64) {
	p.becomeTentative()
	p.cursor = echoCursor{}
	p.holdUntil = seq
}

// input records the Input numbered seq and predicts its bytes.
func (p *echoPredictor) input(seq uint64, data []byte, now time.Time) {
	if seq == 0 || p.mode == domain.EchoPredictNever {
		return
	}
	if len(p.sent) == maxEchoSent {
		p.sent = append(p.sent[:0], p.sent[1:]...)
	}
	p.sent = append(p.sent, echoSent{seq: seq, at: now})
	p.cull(now)
	// A daemon that never acknowledges echo (Echo stays 0) could never
	// confirm or refute a guess, so nothing is predicted before the first
	// acknowledgement.
	if !p.enabled() || !p.srttKnown {
		return
	}
	// A bracketed paste arrives as one Input: it is not typing.
	if bytes.Contains(data, []byte("\x1b[200~")) {
		p.becomeTentative()
		return
	}
	for len(data) > 0 {
		if data[0] == 0x1b {
			data = data[escapeLength(data):]
			p.becomeTentative()
			continue
		}
		r, size := utf8.DecodeRune(data)
		data = data[size:]
		switch {
		case r == utf8.RuneError && size == 1:
			p.becomeTentative()
		case r == 0x7f:
			p.backspace(seq, now)
		case r == '\r':
			// The next row's state is only known once the daemon answers.
			p.unknownKey(seq)
		case vt.RuneWidth(r) != 1:
			p.becomeTentative()
		default:
			p.print(r, seq, now)
		}
	}
}

// escapeLength is the length of the escape sequence at the start of data, or
// all of data when it is incomplete.
func escapeLength(data []byte) int {
	if len(data) < 2 {
		return len(data)
	}
	switch data[1] {
	case '[':
		for i := 2; i < len(data); i++ {
			if data[i] >= 0x40 && data[i] <= 0x7e {
				return i + 1
			}
		}
		return len(data)
	case 'O':
		return min(3, len(data))
	case ']', 'P', '_', '^', 'X':
		// OSC, DCS, APC, PM, SOS: up to BEL or ST.
		for i := 2; i < len(data); i++ {
			if data[i] == 0x07 {
				return i + 1
			}
			if data[i] == 0x1b && i+1 < len(data) && data[i+1] == '\\' {
				return i + 2
			}
		}
		return len(data)
	}
	return 2
}

// initCursor starts the cursor prediction from the mirror, and the cell row
// with it. It reports false while the cursor row is not known yet.
func (p *echoPredictor) initCursor() bool {
	if p.echo < p.holdUntil {
		return false
	}
	if !p.cursor.active {
		row, col := p.screen.CursorRow(), p.screen.CursorCol()
		if row < 0 || row >= p.screen.Rows() || col < 0 || col >= p.screen.Columns() {
			return false
		}
		p.cursor = echoCursor{active: true, row: row, col: col, epoch: p.predEpoch}
	}
	if p.cells == nil || p.row != p.cursor.row || len(p.cells) != p.screen.Columns() {
		p.row = p.cursor.row
		p.cells = make([]echoCell, p.screen.Columns())
	}
	return true
}

// effective is the cell the user will see at col: a known guess, else the
// mirror.
func (p *echoPredictor) effective(col int) (vt.Cell, bool) {
	cell := &p.cells[col]
	if cell.active {
		if cell.unknown {
			return vt.Cell{}, false
		}
		return cell.replacement, true
	}
	return p.screen.Cell(col, p.row), true
}

func (p *echoPredictor) moveCursor(col int, seq uint64) {
	p.cursor.col = col
	p.cursor.expires = seq
	p.cursor.epoch = p.predEpoch
}

// print predicts one width-1 rune in insert mode. The shift stops at the
// first blank, so a split layout's borders to the right never move.
func (p *echoPredictor) print(r rune, seq uint64, now time.Time) {
	if !p.initCursor() {
		p.becomeTentative()
		return
	}
	width := len(p.cells)
	col := p.cursor.col
	if col+1 >= width {
		// mosh: the last column is tricky (wrap markers, shells differ).
		p.becomeTentative()
	}
	end := col
	for end < width-1 {
		cell, known := p.effective(end)
		if known && (!isPlainCell(cell) || isBorderRune(cell.Rune)) {
			// Wide or payload text cannot be shifted exactly, and a border
			// would move with the line.
			p.unknownKey(seq)
			return
		}
		if known && isBlankRune(cell.Rune) {
			break
		}
		end++
	}
	for i := end; i > col; i-- {
		cell := &p.cells[i]
		prev, known := p.effective(i - 1)
		original := p.screen.Cell(i, p.row).Rune
		cell.resetWithOriginal()
		cell.arm(seq, p.predEpoch, now)
		cell.originals = append(cell.originals, original)
		if i == width-1 || !known {
			cell.unknown = true
		} else {
			cell.replacement = prev
		}
	}
	style := vt.DefaultStyle()
	if col > 0 {
		if left, known := p.effective(col - 1); known {
			style = left.Style
		}
	}
	cell := &p.cells[col]
	original := p.screen.Cell(col, p.row).Rune
	cell.resetWithOriginal()
	cell.arm(seq, p.predEpoch, now)
	cell.replacement = vt.Cell{Rune: r, Style: style}
	cell.originals = append(cell.originals, original)
	if col < width-1 {
		p.moveCursor(col+1, seq)
	} else {
		p.becomeTentative()
		p.moveCursor(col, seq)
	}
}

// backspace predicts DEL: the cursor moves left and the text after it moves
// left up to the first blank.
func (p *echoPredictor) backspace(seq uint64, now time.Time) {
	if !p.initCursor() {
		p.becomeTentative()
		return
	}
	if p.cursor.col == 0 {
		return
	}
	width := len(p.cells)
	col := p.cursor.col - 1
	if left, known := p.effective(col); known && isBorderRune(left.Rune) {
		// The input starts after a border (a modal box or pane divider):
		// backspace cannot erase it.
		p.becomeTentative()
		return
	}
	for i := col; i < width; i++ {
		cell, known := p.effective(i)
		if known && !isPlainCell(cell) {
			p.unknownKey(seq)
			return
		}
		if known && (isBlankRune(cell.Rune) || isBorderRune(cell.Rune)) {
			break
		}
	}
	p.moveCursor(col, seq)
	for i := col; i < width; i++ {
		cell := &p.cells[i]
		original := p.screen.Cell(i, p.row).Rune
		var next vt.Cell
		known := false
		if i+2 < width {
			next, known = p.effective(i + 1)
		}
		blank := known && isBlankRune(next.Rune)
		if known && isBorderRune(next.Rune) {
			// The text ends at a border: what fills this cell is unknown.
			known, blank = false, true
		}
		cell.resetWithOriginal()
		cell.arm(seq, p.predEpoch, now)
		cell.originals = append(cell.originals, original)
		if !known {
			cell.unknown = true
		} else {
			cell.replacement = next
		}
		if blank {
			break
		}
	}
}

// needsTick reports whether a pending guess still waits on a timing trigger
// (mosh's timing_tests_necessary), so the caller re-culls without output.
func (p *echoPredictor) needsTick() bool {
	return p.mode == domain.EchoPredictAdaptive && p.active() && !(p.glitch > 0 && p.flagging)
}

// undraw returns the bytes that repaint every drawn guess from the mirror, so
// a daemon frame lands on the screen the daemon believes it has. A frame that
// scrolls would otherwise move a guess where nothing ever repairs it. The
// guesses stay pending and render draws them again after the frame.
func (p *echoPredictor) undraw() []byte {
	if len(p.drawn) == 0 {
		return nil
	}
	var out bytes.Buffer
	if p.drawnRow < p.screen.Rows() {
		for _, col := range p.drawn {
			if col < p.screen.Columns() {
				p.restoreCell(&out, p.drawnRow, col)
			}
		}
	}
	p.drawn = p.drawn[:0]
	if out.Len() > 0 {
		out.WriteString("\x1b[0m")
		writeEchoCUP(&out, p.screen.CursorRow(), p.screen.CursorCol())
	}
	return out.Bytes()
}

// render returns the terminal bytes that bring the screen from the last
// render to the current guesses: drawn cells that no longer show a guess are
// repainted from the mirror, shown guesses are drawn again (a daemon frame
// may have overwritten them), and the cursor goes to the predicted position
// or back to the real one.
func (p *echoPredictor) render() []byte {
	var out bytes.Buffer
	show := p.displaying() && p.enabled()
	var want []int
	// Until the daemon answers an unknown key (Enter), the guesses' row may
	// have moved, so none is drawn.
	if show && p.cells != nil && p.row < p.screen.Rows() && p.echo >= p.holdUntil {
		for col := range p.cells {
			cell := &p.cells[col]
			if !cell.active || cell.unknown || cell.tentative(p.confEpoch) {
				continue
			}
			if p.screen.Cell(col, p.row).Equal(cell.replacement) && !p.flagging {
				continue
			}
			want = append(want, col)
		}
	}
	for _, col := range p.drawn {
		if p.drawnRow == p.row && slices.Contains(want, col) {
			continue
		}
		if p.drawnRow < p.screen.Rows() && col < p.screen.Columns() {
			p.restoreCell(&out, p.drawnRow, col)
		}
	}
	for _, col := range want {
		cell := p.cells[col].replacement
		flag := p.flagging && !(isBlankRune(cell.Rune) && isBlankRune(p.screen.Cell(col, p.row).Rune))
		writeEchoCell(&out, p.row, col, cell, flag)
	}
	p.drawn = append(p.drawn[:0], want...)
	p.drawnRow = p.row

	row, col := p.screen.CursorRow(), p.screen.CursorCol()
	moved := false
	if show && p.cursor.active && p.cursor.epoch <= p.confEpoch {
		moved = p.cursor.row != row || p.cursor.col != col
		row, col = p.cursor.row, p.cursor.col
	}
	if out.Len() > 0 || moved || p.cursorMove {
		out.WriteString("\x1b[0m")
		writeEchoCUP(&out, row, col)
	}
	p.cursorMove = moved
	return out.Bytes()
}

func writeEchoCUP(out *bytes.Buffer, row, col int) {
	out.WriteString("\x1b[")
	out.WriteString(strconv.Itoa(row + 1))
	out.WriteByte(';')
	out.WriteString(strconv.Itoa(col + 1))
	out.WriteByte('H')
}

// restoreCell repaints the mirror's cell at (row, col). The right half of a
// wide glyph is repainted from its left cell, which covers both columns.
func (p *echoPredictor) restoreCell(out *bytes.Buffer, row, col int) {
	cell := p.screen.Cell(col, row)
	if cell.Continuation && col > 0 {
		col--
		cell = p.screen.Cell(col, row)
	}
	writeEchoCell(out, row, col, cell, false)
}

// writeEchoCell draws one cell. Mirror styles come from daemon output already
// projected to this terminal's color profile, so they are written as is.
func writeEchoCell(out *bytes.Buffer, row, col int, cell vt.Cell, underline bool) {
	writeEchoCUP(out, row, col)
	out.WriteString(echoSGR(cell.Style, underline))
	switch {
	case cell.Continuation || cell.Rune == 0:
		out.WriteByte(' ')
	case cell.Payload.Grapheme() != "":
		out.WriteString(cell.Payload.Grapheme())
	default:
		out.WriteRune(cell.Rune)
	}
}

func echoSGR(style vt.Style, underline bool) string {
	var b strings.Builder
	b.WriteString("\x1b[0")
	if style.Bold {
		b.WriteString(";1")
	}
	if style.Attrs&vt.AttrDim != 0 {
		b.WriteString(";2")
	}
	if style.Italic {
		b.WriteString(";3")
	}
	switch {
	case underline:
		b.WriteString(";4")
	case style.Attrs&vt.AttrUnderline != 0:
		switch style.UnderlineStyle {
		case vt.UnderlineDouble:
			b.WriteString(";21")
		case vt.UnderlineCurly:
			b.WriteString(";4:3")
		case vt.UnderlineDotted:
			b.WriteString(";4:4")
		case vt.UnderlineDashed:
			b.WriteString(";4:5")
		default:
			b.WriteString(";4")
		}
	}
	if style.Attrs&vt.AttrBlink != 0 {
		b.WriteString(";5")
	}
	if style.Inverse {
		b.WriteString(";7")
	}
	if style.Attrs&vt.AttrStrikethrough != 0 {
		b.WriteString(";9")
	}
	writeEchoColor(&b, style.HasForegroundRGB, style.ForegroundRGB, style.Foreground, 38, 30, 90)
	writeEchoColor(&b, style.HasBackgroundRGB, style.BackgroundRGB, style.Background, 48, 40, 100)
	switch {
	case style.HasUnderlineColorRGB:
		b.WriteString(";58;2;" + strconv.Itoa(int(style.UnderlineColorRGB.R)) + ";" + strconv.Itoa(int(style.UnderlineColorRGB.G)) + ";" + strconv.Itoa(int(style.UnderlineColorRGB.B)))
	case style.HasUnderlineColor:
		b.WriteString(";58;5;" + strconv.Itoa(style.UnderlineColor))
	}
	b.WriteByte('m')
	return b.String()
}

func writeEchoColor(b *strings.Builder, hasRGB bool, rgb vt.RGB, index, extended, normal, bright int) {
	switch {
	case hasRGB:
		b.WriteString(";" + strconv.Itoa(extended) + ";2;" + strconv.Itoa(int(rgb.R)) + ";" + strconv.Itoa(int(rgb.G)) + ";" + strconv.Itoa(int(rgb.B)))
	case index < 0:
	case index < 8:
		b.WriteString(";" + strconv.Itoa(normal+index))
	case index < 16:
		b.WriteString(";" + strconv.Itoa(bright+index-8))
	case index < 256:
		b.WriteString(";" + strconv.Itoa(extended) + ";5;" + strconv.Itoa(index))
	}
}

// apcFilter drops APC strings (ESC _ ... ST), which carry kitty graphics, from
// the mirror's input: the engine needs text only, and decoding images would
// cost the client CPU and memory. Its state spans Output boundaries.
type apcFilter struct {
	state uint8
}

const (
	apcText uint8 = iota
	apcEscape
	apcBody
	apcBodyEscape
)

func (f *apcFilter) strip(data []byte) []byte {
	if f.state == apcText && bytes.IndexByte(data, 0x1b) < 0 {
		return data
	}
	out := make([]byte, 0, len(data))
	for _, b := range data {
		switch f.state {
		case apcText:
			if b == 0x1b {
				f.state = apcEscape
				continue
			}
			out = append(out, b)
		case apcEscape:
			if b == '_' {
				f.state = apcBody
				continue
			}
			out = append(out, 0x1b, b)
			f.state = apcText
			if b == 0x1b {
				out = out[:len(out)-1]
				f.state = apcEscape
			}
		case apcBody:
			if b == 0x1b {
				f.state = apcBodyEscape
			}
		case apcBodyEscape:
			switch b {
			case '\\':
				f.state = apcText
			case 0x1b:
			default:
				f.state = apcBody
			}
		}
	}
	return out
}
