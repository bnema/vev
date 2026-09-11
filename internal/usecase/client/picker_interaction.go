package client

import (
	"time"

	ansirenderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
)

// This file owns the client side of the picker interaction: source admission,
// local model ownership, and typed commit/cancel. It mirrors the
// inventoryRelay admission discipline (navigation_inventory.go) but never
// canonicalizes line order: picker order is semantically significant. The
// loop consumes terminal-pump records, never raw reads; commit sends a typed
// PickerSelection on the ordered control channel, never picker bytes toward
// the PTY.

// pickerInteraction tracks one picker namespace: the admitted interaction ID
// and the newest interaction this attachment retired. A retired interaction is
// never resurrected by a late snapshot.
type pickerInteraction struct {
	open        bool
	interaction uint64
	retired     uint64
	// intent is the intent the serving daemon published for this interaction.
	// Move intents present destination rows instead of navigation rows.
	intent protocol.PickerIntent
	// sourceRevisions tracks the newest admitted revision per source. Every
	// source publishes its own revision, so one source's refresh never
	// invalidates a selection displayed from another.
	sourceRevisions map[string]uint64
}

// setOpen starts or stops the interaction namespace. Superseding an open
// interaction retires it too: a late snapshot for the replaced ID must not
// take presentation back. Closing retires the namespace for good.
func (p *pickerInteraction) setOpen(open bool, interaction uint64) {
	if p == nil {
		return
	}
	if open && interaction != p.interaction && p.open && p.interaction > p.retired {
		p.retired = p.interaction
	}
	p.open = open
	if open && interaction != p.interaction {
		p.interaction = interaction
		p.sourceRevisions = make(map[string]uint64)
	}
	if !open && interaction > p.retired {
		p.retired = interaction
	}
	if !open {
		p.intent = 0
		p.sourceRevisions = nil
	}
}

// setIntent records the intent published with the offer that opened the
// interaction.
func (p *pickerInteraction) setIntent(interaction uint64, intent protocol.PickerIntent) {
	if p == nil || !p.open || p.interaction != interaction {
		return
	}
	p.intent = intent
}

// admitSnapshot validates one source publication against the open interaction
// and reports whether it carries a newer revision worth displaying. Retired
// interactions, foreign interactions, and older or duplicate source revisions
// discard.
func (p *pickerInteraction) admitSnapshot(snapshot protocol.PickerSnapshot, wantsEvent bool) bool {
	if p == nil {
		return false
	}
	if protocol.ValidatePickerSnapshot(snapshot) != nil {
		return false
	}
	if snapshot.InteractionID <= p.retired {
		return false
	}
	if !p.open || snapshot.InteractionID != p.interaction {
		return false
	}
	if snapshot.SourceRevision <= p.sourceRevisions[snapshot.SourceID] {
		return false
	}
	if wantsEvent && p.intent == 0 {
		// The offer carries the intent; a snapshot that arrives first is
		// still admitted so a lost offer cannot wedge the interaction.
	}
	p.sourceRevisions[snapshot.SourceID] = snapshot.SourceRevision
	return true
}

// pickerLoop owns one admitted *picker.Model plus the source publication it
// was built from. Cursor, search, and sort are local-only presentation;
// commit and cancel cross the wire as typed messages. The daemon revalidates
// the committed source revision and key, so the client never keeps a second
// copy of the row set.
type pickerLoop struct {
	model          *picker.Model
	interaction    uint64
	intent         protocol.PickerIntent
	sourceID       string
	sourceRevision uint64
}

// pickerLoopFromSnapshot builds the local model from one admitted source
// publication. Rows are rendered exactly as published: the daemon owns
// eligibility and per-row actions, the client owns order, cursor, and search.
func pickerLoopFromSnapshot(snapshot protocol.PickerSnapshot, intent protocol.PickerIntent, sort picker.SortMode) *pickerLoop {
	model := picker.New(snapshot.Lines, picker.Config{Intent: intent, Cursor: snapshot.Cursor, Sort: sort})
	return &pickerLoop{
		model: model, interaction: snapshot.InteractionID, intent: intent,
		sourceID: snapshot.SourceID, sourceRevision: snapshot.SourceRevision,
	}
}

// replaceLines applies a newer publication of the already displayed source
// while retaining the local search editor and cursor key.
func (l *pickerLoop) replaceLines(snapshot protocol.PickerSnapshot) {
	if l == nil || l.model == nil || snapshot.SourceID != l.sourceID {
		return
	}
	l.model.ReplaceLines(snapshot.Lines, snapshot.Cursor)
	l.sourceRevision = snapshot.SourceRevision
}

// up moves the cursor one focusable row up.
func (l *pickerLoop) up() {
	if l == nil || l.model == nil {
		return
	}
	l.model.Up()
}

// down moves the cursor one focusable row down.
func (l *pickerLoop) down() {
	if l == nil || l.model == nil {
		return
	}
	l.model.Down()
}

// enterSearch begins search mode; text inserts through insert().
func (l *pickerLoop) enterSearch() {
	if l == nil || l.model == nil {
		return
	}
	if !l.model.SearchActive() {
		l.model.EnterSearch()
	}
}

// insert types one rune into the active search, entering search first.
func (l *pickerLoop) insert(r rune) {
	if l == nil || l.model == nil {
		return
	}
	l.enterSearch()
	l.model.InsertSearch(r)
}

// backspace deletes one search rune; Escape handling distinguishes
// search-exit from picker-close at the call site.
func (l *pickerLoop) backspace() {
	if l == nil || l.model == nil {
		return
	}
	l.model.BackspaceSearch()
}

// escape handles one Escape press: exit search when searching, else report
// picker-close. The boolean is true when the caller must send PickerClose.
func (l *pickerLoop) escape() bool {
	if l == nil || l.model == nil {
		return true
	}
	if l.model.SearchActive() {
		l.model.ExitSearch()
		return false
	}
	return true
}

// toggleSort flips the local ordering between recency and grouped and reports
// whether the display changed.
func (l *pickerLoop) toggleSort() bool {
	if l == nil || l.model == nil {
		return false
	}
	next := picker.SortGrouped
	if l.model.SortMode() == picker.SortGrouped {
		next = picker.SortRecent
	}
	l.model.SetSort(next)
	return true
}

// selectedAction reports the action the displayed row authorises: navigation
// rows navigate, move rows move. The daemon publishes the bits, so the client
// never invents authority.
func (l *pickerLoop) selectedAction() (protocol.PickerAction, bool) {
	if l == nil || l.model == nil {
		return 0, false
	}
	line, ok := l.model.Selected()
	if !ok {
		return 0, false
	}
	switch {
	case line.Actions&protocol.PickerCanNavigate != 0:
		return protocol.PickerActionNavigate, true
	case line.Actions&protocol.PickerCanMove != 0:
		return protocol.PickerActionMove, true
	default:
		return 0, false
	}
}

// killAction reports whether the cursor row authorises destruction.
func (l *pickerLoop) killAction() bool {
	if l == nil || l.model == nil {
		return false
	}
	line, ok := l.model.Selected()
	return ok && line.Actions&protocol.PickerCanKill != 0
}

// defaultPickerSort is the client-local initial ordering mode.
func defaultPickerSort() picker.SortMode { return picker.SortRecent }

// emptyPickerPreview is the zero preview used until the daemon publishes one.
func emptyPickerPreview() picker.Preview { return picker.Preview{} }

// cursorKey names the row the modal currently displays. The daemon revalidates
// it against the interaction it published, so it stays opaque here.
func (l *pickerLoop) cursorKey() string {
	if l == nil || l.model == nil {
		return ""
	}
	line, ok := l.model.Selected()
	if !ok {
		return ""
	}
	return line.Key
}

// pickerPreviewDebounce bounds how long the cursor may rest before the client
// asks the serving daemon for that row's preview. The daemon keeps publishing
// the row afterwards, so this only throttles cursor movement.
const pickerPreviewDebounce = 80 * time.Millisecond

// pickerPreviewClient owns the client half of the row preview: the row the
// modal displays, the row whose request is on the wire, and the viewport the
// daemon last published for it. The client never captures a viewport itself.
type pickerPreviewClient struct {
	interaction uint64
	sent        string
	frame       picker.Preview
}

// resetFor drops the preview state of one retired interaction. A released
// interaction never shows the previous row's viewport again.
func (p *pickerPreviewClient) resetFor() {
	if p == nil {
		return
	}
	*p = pickerPreviewClient{}
}

// needsRequest reports whether the displayed row still needs a request: a new
// interaction, a moved cursor, or a row the daemon has not been asked for yet.
func (p *pickerPreviewClient) needsRequest(interaction uint64, key string) bool {
	if p == nil || key == "" || interaction == 0 {
		return false
	}
	return p.interaction != interaction || p.sent != key
}

// requestFor builds the request for one displayed row. The client asks for
// exactly the viewport it can display, bounded by the preview protocol.
func (p *pickerPreviewClient) requestFor(interaction uint64, key string, size domain.Size) (protocol.PickerPreviewRequest, bool) {
	request := protocol.PickerPreviewRequest{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: interaction,
		SourceID: protocol.PickerServingSourceID, Key: key,
		Width:  clampPreviewDimension(size.Cols, protocol.PickerPreviewMaxWidth),
		Height: clampPreviewDimension(size.Rows, protocol.PickerPreviewMaxHeight),
	}
	if protocol.ValidatePickerPreviewRequest(request) != nil {
		return protocol.PickerPreviewRequest{}, false
	}
	return request, true
}

// markSent records the row whose request crossed the wire. The next debounce
// fires only when the cursor moved again.
func (p *pickerPreviewClient) markSent(interaction uint64, key string) {
	if p == nil {
		return
	}
	p.interaction, p.sent = interaction, key
}

// accept applies one published preview and reports whether it describes the
// displayed row. A late answer for a previous row leaves the current viewport
// untouched, and a status-only answer clears it.
func (p *pickerPreviewClient) accept(preview protocol.PickerPreview, interaction uint64, key string) bool {
	if p == nil || key == "" || preview.InteractionID != interaction || preview.Key != key {
		return false
	}
	if preview.Status != protocol.PickerPreviewOK {
		p.frame = emptyPickerPreview()
		return true
	}
	rows := preview.FrameRows()
	if rows == nil {
		return false
	}
	p.frame = picker.Preview{Rows: rows, Width: int(preview.Width), Height: int(preview.Height)}
	return true
}

// clampPreviewDimension bounds one requested viewport dimension.
func clampPreviewDimension(value int, maxValue uint16) uint16 {
	if value <= 0 {
		return 1
	}
	if value > int(maxValue) {
		return maxValue
	}
	return uint16(value)
}

// pickerRenderer encodes one picker frame to terminal bytes. It owns a
// dedicated ANSI renderer shadow (never the daemon output shadow): the
// client-picker frame is a local presentation overlay, not daemon output,
// so its bytes never enter the output ACK window.
type pickerRenderer struct {
	renderer *ansirenderer.Renderer
	size     domain.Size
	// pasteMode records whether bracketed paste was enabled on the terminal
	// for this interaction, so it is enabled once and disabled exactly once.
	pasteMode bool
}

func newPickerRenderer() *pickerRenderer {
	return &pickerRenderer{}
}

// Bracketed-paste mode is enabled while the picker owns the terminal: the
// terminal then wraps pasted text in markers, which the input decoder drops
// as a unit. Without it, a paste arrives as ordinary bytes and only the
// event ordering distinguishes it from fast typing.
const (
	bracketedPasteEnable  = "\x1b[?2004h"
	bracketedPasteDisable = "\x1b[?2004l"
)

// disableBracketedPaste returns the mode reset once, or nil when nothing was
// enabled. The caller writes it through the terminal's sole writer.
func (r *pickerRenderer) disableBracketedPaste() []byte {
	if r == nil || !r.pasteMode {
		return nil
	}
	r.pasteMode = false
	return []byte(bracketedPasteDisable)
}

// render composes the loop model into terminal bytes for one display
// refresh. Previews arrive as source data (picker_preview.go) and are attached
// to the model by the caller; a full redraw every refresh keeps the shadow
// trivially consistent: the frame is small and modal, never a PTY stream.
func (r *pickerRenderer) render(loop *pickerLoop, size domain.Size, preview picker.Preview) []byte {
	if r == nil || loop == nil || loop.model == nil {
		return nil
	}
	if size.Cols <= 0 || size.Rows <= 0 {
		return nil
	}
	if r.renderer == nil || r.size != size {
		r.renderer = ansirenderer.New(ansirenderer.Capabilities{})
		r.size = size
	}
	frame := loop.model.Render(size, preview)
	data, err := r.renderer.Draw(frame, []ansirenderer.Damage{ansirenderer.FullRedraw()})
	if err != nil {
		return nil
	}
	if !r.pasteMode {
		// Enable bracketed paste with the first frame so the terminal marks
		// every paste from the moment the picker owns the screen.
		r.pasteMode = true
		return append([]byte(bracketedPasteEnable), data...)
	}
	return data
}

// commitSelection builds the typed commit for the row under the cursor at the
// displayed source revision. Zero picker bytes travel toward the PTY: the
// opaque key plus revision ride a PickerSelection on the ordered control
// channel and the owning source resolves them.
func commitSelection(loop *pickerLoop, action protocol.PickerAction, causeActionID uint64) (protocol.PickerSelection, bool) {
	if loop == nil || loop.model == nil {
		return protocol.PickerSelection{}, false
	}
	line, ok := loop.model.Selected()
	if !ok {
		return protocol.PickerSelection{}, false
	}
	selection := protocol.PickerSelection{
		CauseActionID: causeActionID, InteractionID: loop.interaction,
		SourceID: loop.sourceID, SourceRevision: loop.sourceRevision,
		Key: line.Key, Action: action,
	}
	if protocol.ValidatePickerSelection(selection) != nil {
		return protocol.PickerSelection{}, false
	}
	return selection, true
}

// killSelection builds the typed kill for the row under the cursor. It is a
// distinct commit so the source can verify the row still authorises
// destruction.
func killSelection(loop *pickerLoop, causeActionID uint64) (protocol.PickerSelection, bool) {
	if loop == nil || loop.model == nil {
		return protocol.PickerSelection{}, false
	}
	line, ok := loop.model.Selected()
	if !ok || line.Actions&protocol.PickerCanKill == 0 {
		return protocol.PickerSelection{}, false
	}
	selection := protocol.PickerSelection{
		CauseActionID: causeActionID, InteractionID: loop.interaction,
		SourceID: loop.sourceID, SourceRevision: loop.sourceRevision,
		Key: line.Key, Action: protocol.PickerActionKill,
	}
	if protocol.ValidatePickerSelection(selection) != nil {
		return protocol.PickerSelection{}, false
	}
	return selection, true
}

// pickerOp is the presentation decision one driver operation asks the attach
// loop to carry out. Close wins over commit when both land in one op,
// mirroring the overlay exit short-circuit.
type pickerOp struct {
	commit bool
	kill   bool
	close  bool
}

// pickerDriverOp is one ui-driver operation applied to an open loop. Keys
// drive cursor/search/exit: arrows and j/k move in normal mode (j/k are
// literal in search), `/` enters search, `s` reorders locally, `x` asks to
// destroy the cursor row, `q`/Ctrl-C/Escape close in normal mode (Escape
// exits search first), Backspace edits the query. Text runes insert only
// while search is active; normal-mode typing is ignored exactly like the
// overlay.
func pickerDriverOp(loop *pickerLoop, keys []string, text string) (op pickerOp, changed bool) {
	if loop == nil || loop.model == nil {
		return pickerOp{}, false
	}
	insertLit := func(r rune) {
		if loop.model.SearchActive() {
			loop.insert(r)
		}
	}
	for _, key := range keys {
		active := loop.model.SearchActive()
		beforeIndex, beforeSearch := loop.model.SelectedIndex(), active
		beforeQuery := loop.model.Query()
		switch key {
		case "Up":
			loop.up()
		case "Down":
			loop.down()
		case "j":
			if active {
				loop.insert('j')
			} else {
				loop.down()
			}
		case "k":
			if active {
				loop.insert('k')
			} else {
				loop.up()
			}
		case "Enter":
			// Commit is typed at the call site via commitSelection; the
			// key itself sends zero bytes toward the PTY.
			op.commit = true
		case "Escape":
			if loop.escape() {
				return pickerOp{close: true}, changed
			}
		case "Backspace":
			if active {
				loop.backspace()
			}
		case "/":
			if active {
				loop.insert('/')
			} else {
				loop.enterSearch()
			}
		case "s":
			if active {
				loop.insert('s')
			} else {
				changed = loop.toggleSort() || changed
			}
		case "x":
			if active {
				loop.insert('x')
			} else {
				op.kill = true
			}
		case "q":
			if active {
				loop.insert('q')
			} else {
				return pickerOp{close: true}, changed
			}
		case "Ctrl+C":
			// The overlay exits on Ctrl-C in both modes; the byte
			// itself never reaches the query.
			return pickerOp{close: true}, changed
		default:
			if len(key) == 1 {
				insertLit(rune(key[0]))
			}
		}
		if loop.model.SelectedIndex() != beforeIndex || loop.model.SearchActive() != beforeSearch || loop.model.Query() != beforeQuery {
			changed = true
		}
	}
	for _, r := range text {
		if loop.model.SearchActive() {
			loop.insert(r)
			changed = true
		}
	}
	return op, changed
}

// pickerInputBatch is one decoded batch: ordered input events.
type pickerInputBatch struct {
	events []pickerEvent
}

// pickerConsumeOutcome is one decoded operation reported by the stdin
// pump to the attach loop. The pump decodes and identifies; the attach
// loop is the only writer of the picker model, so the outcome carries the
// ordered events and their interaction identity, never a mutation or a
// message. actionID echoes the admitted record so the loop completes the
// right ui-driver action.
type pickerConsumeOutcome struct {
	consumed    bool
	actionID    uint64
	generation  uint64
	interaction uint64
	events      []pickerEvent
}

// acceptOutcome reports whether this operation still belongs to the
// interaction the attachment currently presents. Retired generations and
// interactions drop.
func (o pickerConsumeOutcome) acceptOutcome(interaction, generation uint64) bool {
	return o.consumed && o.interaction != 0 && o.interaction == interaction && o.generation == generation
}

// applyPickerBatch applies one decoded batch in arrival order. Every event
// is applied: a terminal read can legitimately carry several keystrokes, so
// batch size never decides whether input is a command. Paste protection is
// the decoder's paste state, not a heuristic here: a bracketed paste's
// content never reaches this function.
func applyPickerBatch(loop *pickerLoop, events []pickerEvent) (op pickerOp, changed bool) {
	if loop == nil || loop.model == nil || len(events) == 0 {
		return pickerOp{}, false
	}
	for _, event := range events {
		if event.kind == pickerEventRune {
			if loop.model.SearchActive() {
				loop.insert(event.r)
				changed = true
			}
			continue
		}
		eventOp, eventChanged := pickerDriverOp(loop, []string{event.key}, "")
		changed = changed || eventChanged
		if eventOp.close {
			return pickerOp{close: true}, changed
		}
		op.commit = op.commit || eventOp.commit
		op.kill = op.kill || eventOp.kill
	}
	return op, changed
}
