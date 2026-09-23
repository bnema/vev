package client

import (
	"fmt"

	renderer "github.com/bnema/vev-vt"
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
	model       *picker.Model
	interaction uint64
	intent      protocol.PickerIntent
	sources     map[string]protocol.PickerSnapshot
	order       []string
	rows        map[string]pickerRowIdentity
}

type pickerRowIdentity struct {
	sourceID       string
	sourceRevision uint64
	key            string
}

// pickerLoopFromSnapshot builds the local model from the first admitted source.
// Source-owned opaque keys are namespaced only inside the presentation model;
// commits translate them back to the exact source identity published on wire.
func pickerLoopFromSnapshot(snapshot protocol.PickerSnapshot, intent protocol.PickerIntent, sort picker.SortMode) *pickerLoop {
	snapshot = protocol.NormalizePickerSnapshot(snapshot)
	loop := &pickerLoop{interaction: snapshot.InteractionID, intent: intent, sources: make(map[string]protocol.PickerSnapshot)}
	loop.sources[snapshot.SourceID] = snapshot
	loop.order = append(loop.order, snapshot.SourceID)
	projection := pickerSnapshotProjection(snapshot, sort)
	cursor := projection.Cursor
	if cursor.Key != "" {
		cursor.Key = fmt.Sprintf("%d:%s:%s", len(snapshot.SourceID), snapshot.SourceID, cursor.Key)
	}
	loop.rebuild(cursor, sort)
	return loop
}

// replaceLines applies one source publication while retaining every other
// source. Independent source revisions therefore cannot invalidate each other.
func (l *pickerLoop) replaceLines(snapshot protocol.PickerSnapshot) {
	snapshot = protocol.NormalizePickerSnapshot(snapshot)
	if l == nil || l.model == nil || snapshot.InteractionID != l.interaction {
		return
	}
	if _, exists := l.sources[snapshot.SourceID]; !exists {
		l.order = append(l.order, snapshot.SourceID)
	}
	selectedKey := ""
	if selected, ok := l.model.Selected(); ok {
		selectedKey = selected.Key
	}
	l.sources[snapshot.SourceID] = snapshot
	l.rebuild(protocol.PickerCursor{Key: selectedKey, Index: -1}, l.model.SortMode(), true)
}

func (l *pickerLoop) rebuild(cursor protocol.PickerCursor, sort picker.SortMode, preserveEditor ...bool) {
	lines := make([]protocol.PickerLine, 0)
	rows := make(map[string]pickerRowIdentity)
	cursorKey := ""
	for _, sourceID := range l.order {
		snapshot, ok := l.sources[sourceID]
		if !ok {
			continue
		}
		projection := pickerSnapshotProjection(snapshot, sort)
		for index, published := range projection.Lines {
			line := published
			if line.Key != "" {
				presented := fmt.Sprintf("%d:%s:%s", len(sourceID), sourceID, line.Key)
				rows[presented] = pickerRowIdentity{sourceID: sourceID, sourceRevision: snapshot.SourceRevision, key: line.Key}
				if presented == cursor.Key || cursor.Key == "" && sourceID == snapshot.SourceID && index == projection.Cursor.Index {
					cursorKey = presented
				}
				line.Key = presented
			}
			lines = append(lines, line)
		}
	}
	l.rows = rows
	modelCursor := protocol.PickerCursor{Key: cursorKey, Index: -1}
	if len(preserveEditor) != 0 && preserveEditor[0] && l.model != nil {
		l.model.ReplaceProjection(lines, modelCursor, sort)
		return
	}
	l.model = picker.New(lines, picker.Config{Intent: l.intent, Cursor: modelCursor, Sort: sort})
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
	selected := protocol.PickerCursor{Index: -1}
	if line, ok := l.model.Selected(); ok {
		selected.Key = line.Key
	}
	l.rebuild(selected, next, true)
	return true
}

func pickerSnapshotProjection(snapshot protocol.PickerSnapshot, sort picker.SortMode) protocol.PickerProjection {
	if sort == picker.SortGrouped {
		return snapshot.Grouped
	}
	return snapshot.Recent
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

// pickerPreviewSize is the viewport the modal can display for one terminal: the
// preview pane inside the shared picker geometry.
func pickerPreviewSize(terminal domain.Size) domain.Size {
	return picker.Size(picker.PreviewRect(terminal))
}

// emptyPickerPreview is the zero preview used until the daemon publishes one.
func emptyPickerPreview() picker.Preview { return picker.Preview{} }

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
	renderer     *ansirenderer.Renderer
	profile      ansirenderer.ColorProfile
	renderStyles []picker.RenderStyles
	size         domain.Size
	// prevBounds is the box drawn last: when it moves or resizes, its previous
	// cells are blanked together with the new ones, and the rest of the screen
	// stays untouched.
	prevBounds *domain.Rect
	// primed records that the renderer shadow was seeded with the screen as it
	// is, so the first box is diffed against it instead of replacing the screen.
	primed bool
}

func newPickerRenderer(profile ansirenderer.ColorProfile) *pickerRenderer {
	return &pickerRenderer{profile: profile}
}

// pickerColorProfile maps the terminal's detected color capability to the
// picker renderer profile. A terminal without truecolor gets indexed colors,
// so RGB surfaces are quantized instead of dropped (#280).
func pickerColorProfile(trueColor bool) ansirenderer.ColorProfile {
	if trueColor {
		return ansirenderer.ColorProfileTrueColor
	}
	return ansirenderer.ColorProfileANSI256
}

// render composes the loop model into terminal bytes for one display
// refresh. Previews arrive as source data (picker_preview.go) and are attached
// to the model by the caller; a full redraw every refresh keeps the shadow
// trivially consistent: the frame is small and modal, never a PTY stream.
// render composes one floating modal over the session: the picker never takes
// the whole screen. The box is resolved from the shared picker geometry, its
// inner content is rendered into that rectangle, and only the box's own cells
// are written, so the session stays visible around it until the daemon's
// authoritative repaint after the close.
func (r *pickerRenderer) render(loop *pickerLoop, size domain.Size, preview picker.Preview) []byte {
	if r == nil || loop == nil || loop.model == nil {
		return nil
	}
	if size.Cols <= 0 || size.Rows <= 0 {
		return nil
	}
	if r.renderer == nil || r.size != size {
		r.renderer = ansirenderer.NewWithColorProfile(ansirenderer.Capabilities{}, r.profile)
		r.size = size
		// Keep prevBounds across a resize. The new renderer is primed with an
		// empty shadow below, so damaging the old rectangle explicitly clears
		// the part of the previous modal that remains visible at the new size.
		r.primed = false
	}
	// The picker is a floating box over a screen the client does not own: a
	// fresh renderer would repaint the whole terminal from its empty shadow and
	// wipe the session. Seed the shadow with the empty screen instead, without
	// emitting anything, so only the box's own cells are ever written.
	if !r.primed {
		if prepared, err := r.renderer.Prepare(renderer.NewFrame(size.Cols, size.Rows), nil, false); err == nil {
			prepared.Commit()
			r.primed = true
		}
	}
	presentation := picker.Modal.Resolve(size)
	base := renderer.NewFrame(size.Cols, size.Rows)
	border := renderer.DefaultStyle()
	border.Attrs |= renderer.AttrDim
	picker.Modal.CompositePresentation(base, presentation, border, renderer.DefaultStyle())
	inner := loop.model.Render(picker.Size(presentation.Inner), preview, r.renderStyles...)
	copyFrameRect(base, presentation.Inner, inner)

	// Only the box is written: the session stays on screen around it. A box
	// that moved (a resize, or a wider title) blanks the cells it used to own.
	var damage []ansirenderer.Damage
	if r.prevBounds != nil && *r.prevBounds != presentation.Bounds {
		damage = append(damage, damageRect(*r.prevBounds))
	}
	damage = append(damage, damageRect(presentation.Bounds))
	data, err := r.renderer.Draw(base, damage)
	if err != nil {
		return nil
	}
	bounds := presentation.Bounds
	r.prevBounds = &bounds
	return data
}

// invalidate forgets everything the renderer believes is on screen. The next
// render re-primes an empty shadow and writes the whole box again: someone
// else (an attachment's output or its authoritative repaint) owned the
// terminal since the last picker frame, so a diff against the old shadow would
// leave parts of the box missing.
func (r *pickerRenderer) invalidate() {
	if r == nil {
		return
	}
	r.renderer = nil
	r.primed = false
	r.prevBounds = nil
}

// damageRect is the damage one rectangle of a composed frame produces.
func damageRect(rect domain.Rect) ansirenderer.Damage {
	return ansirenderer.Damage{
		Kind: renderer.DamageText,
		X:    rect.X, Y: rect.Y,
		Width: rect.Width, Height: rect.Height, Count: 1,
	}
}

// copyFrameRect blits src into dst at the target rectangle, clipped to both.
func copyFrameRect(dst renderer.Frame, target domain.Rect, src renderer.Frame) {
	left := max(target.X, 0)
	top := max(target.Y, 0)
	right := min(target.X+target.Width, dst.Width)
	bottom := min(target.Y+target.Height, dst.Height)
	if left >= right || top >= bottom {
		return
	}
	sourceX := left - target.X
	sourceY := top - target.Y
	width := min(right-left, src.Width-sourceX)
	height := min(bottom-top, src.Height-sourceY)
	if width <= 0 || height <= 0 {
		return
	}
	for y := range height {
		for x := range width {
			dst.Set(left+x, top+y, src.Cell(sourceX+x, sourceY+y))
		}
	}
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
	identity, ok := loop.rows[line.Key]
	if !ok {
		return protocol.PickerSelection{}, false
	}
	selection := protocol.PickerSelection{
		CauseActionID: causeActionID, InteractionID: loop.interaction,
		SourceID: identity.sourceID, SourceRevision: identity.sourceRevision,
		Key: identity.key, Action: action,
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
	identity, ok := loop.rows[line.Key]
	if !ok {
		return protocol.PickerSelection{}, false
	}
	selection := protocol.PickerSelection{
		CauseActionID: causeActionID, InteractionID: loop.interaction,
		SourceID: identity.sourceID, SourceRevision: identity.sourceRevision,
		Key: identity.key, Action: protocol.PickerActionKill,
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
	// exit marks the explicit exit key (Ctrl+C). It always travels with close:
	// over a live attachment it cancels back to that attachment like any close,
	// and only a picker with no attachment to return to ends the process on it.
	exit bool
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
			// itself never reaches the query. It is the explicit exit key.
			return pickerOp{close: true, exit: true}, changed
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
			return pickerOp{close: true, exit: eventOp.exit}, changed
		}
		op.commit = op.commit || eventOp.commit
		op.kill = op.kill || eventOp.kill
	}
	return op, changed
}
